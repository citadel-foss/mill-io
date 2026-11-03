use crate::{
    error::Result, reactor::Reactor, ComputePoolMetrics, EventHandler, TaskPriority, TimerId,
};
use mio::{Interest, Token};
use std::{
    cell::RefCell,
    error::Error,
    fmt,
    ops::Deref,
    sync::{atomic::Ordering, Arc, Weak},
    time::Duration,
};

thread_local! {
    /// Reactors this thread holds an [`Upgraded`] of, by address, once per upgrade.
    static UPGRADED: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) };
}

/// A `Handle`'s strong reference to its reactor, held for one call. Counted in
/// the reactor's `admitted`, which `EventLoop::drop` waits on, and per thread so
/// that an `EventLoop` dropped during the call, on the same thread (a cancelled
/// timer or a rejected handler owning the last `Arc<EventLoop>`), does not wait
/// for this call to end.
pub(crate) struct Upgraded(Arc<Reactor>);

impl Upgraded {
    /// `None` once the loop has started dropping.
    fn admit(reactor: Arc<Reactor>) -> Option<Self> {
        // SeqCst pairs with `EventLoop::drop`, which sets `closing` before reading
        // `admitted`: either the drop waits for this call, or this call sees `closing`.
        reactor.admitted.fetch_add(1, Ordering::SeqCst);
        if reactor.closing.load(Ordering::SeqCst) {
            reactor.admitted.fetch_sub(1, Ordering::SeqCst);
            return None;
        }
        let address = Arc::as_ptr(&reactor) as usize;
        let _ = UPGRADED.try_with(|held| held.borrow_mut().push(address));
        Some(Self(reactor))
    }
}

impl Deref for Upgraded {
    type Target = Reactor;

    fn deref(&self) -> &Reactor {
        &self.0
    }
}

impl Drop for Upgraded {
    fn drop(&mut self) {
        let address = Arc::as_ptr(&self.0) as usize;
        let _ = UPGRADED.try_with(|held| {
            let mut held = held.borrow_mut();
            if let Some(position) = held.iter().rposition(|a| *a == address) {
                held.swap_remove(position);
            }
        });
        self.0.admitted.fetch_sub(1, Ordering::SeqCst);
    }
}

/// How many [`Upgraded`] references to `reactor` this thread holds.
pub(crate) fn upgrades_held_here(reactor: &Arc<Reactor>) -> usize {
    let address = Arc::as_ptr(reactor) as usize;
    UPGRADED
        .try_with(|held| held.borrow().iter().filter(|a| **a == address).count())
        .unwrap_or(0)
}

/// The error a [`Handle`] returns once its [`EventLoop`](crate::EventLoop) is dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoopGone;

impl fmt::Display for LoopGone {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("mill-io event loop is gone")
    }
}

impl Error for LoopGone {}

/// A cheap, cloneable reference to an [`EventLoop`](crate::EventLoop) that does
/// not keep it alive.
///
/// It has the loop's methods except [`run`](crate::EventLoop::run). Once the
/// loop is dropped they return `Err(LoopGone)`,
/// [`cancel_timer`](Self::cancel_timer) and
/// [`is_reactor_thread`](Self::is_reactor_thread) return `false`, and
/// [`stop`](Self::stop) does nothing. Because it
/// holds no strong reference, handlers and timer callbacks stored in the loop
/// can capture one without keeping the loop from being freed.
///
/// ```rust
/// use mill_io::{EventLoop, LoopGone};
/// use std::time::Duration;
///
/// let event_loop = EventLoop::default();
/// let handle = event_loop.handle();
/// let repeat = handle.clone();
/// handle.schedule_repeating(Duration::from_secs(1), move || {
///     let _ = repeat.post(|| println!("tick"));
/// })?;
///
/// drop(event_loop);
/// assert_eq!(handle.spawn_compute(|| {}), Err(LoopGone));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Debug)]
pub struct Handle {
    reactor: Weak<Reactor>,
}

impl PartialEq for Handle {
    /// Two handles are equal when they refer to the same loop.
    fn eq(&self, other: &Self) -> bool {
        self.reactor.ptr_eq(&other.reactor)
    }
}

impl Eq for Handle {}

impl Handle {
    pub(crate) fn new(reactor: &Arc<Reactor>) -> Self {
        Self {
            reactor: Arc::downgrade(reactor),
        }
    }

    pub(crate) fn reactor(&self) -> std::result::Result<Upgraded, LoopGone> {
        self.reactor
            .upgrade()
            .and_then(Upgraded::admit)
            .ok_or(LoopGone)
    }

    /// See [`EventLoop::register`](crate::EventLoop::register). The outer
    /// error means the loop is gone; the inner result is the registration's.
    pub fn register<H, S>(
        &self,
        source: &mut S,
        token: Token,
        interests: Interest,
        handler: H,
    ) -> std::result::Result<Result<()>, LoopGone>
    where
        H: EventHandler + Send + Sync + 'static,
        S: mio::event::Source + ?Sized,
    {
        Ok(self
            .reactor()?
            .poll_handle
            .register(source, token, interests, handler))
    }

    /// See [`EventLoop::reregister`](crate::EventLoop::reregister). The outer
    /// error means the loop is gone; the inner result is the reregistration's.
    pub fn reregister<S>(
        &self,
        source: &mut S,
        token: Token,
        interests: Interest,
    ) -> std::result::Result<Result<()>, LoopGone>
    where
        S: mio::event::Source + ?Sized,
    {
        Ok(self
            .reactor()?
            .poll_handle
            .reregister(source, token, interests))
    }

    /// See [`EventLoop::deregister`](crate::EventLoop::deregister). The outer
    /// error means the loop is gone; the inner result is the deregistration's.
    pub fn deregister<S>(
        &self,
        source: &mut S,
        token: Token,
    ) -> std::result::Result<Result<()>, LoopGone>
    where
        S: mio::event::Source + ?Sized,
    {
        Ok(self.reactor()?.poll_handle.deregister(source, token))
    }

    /// See [`EventLoop::alloc_token`](crate::EventLoop::alloc_token).
    pub fn alloc_token(&self) -> std::result::Result<Token, LoopGone> {
        Ok(self.reactor()?.poll_handle.alloc_token())
    }

    /// See [`EventLoop::spawn_compute`](crate::EventLoop::spawn_compute).
    pub fn spawn_compute<F>(&self, task: F) -> std::result::Result<(), LoopGone>
    where
        F: FnOnce() + Send + 'static,
    {
        self.reactor()?.spawn_compute(task, TaskPriority::Normal);
        Ok(())
    }

    /// See [`EventLoop::spawn_compute_with_priority`](crate::EventLoop::spawn_compute_with_priority).
    pub fn spawn_compute_with_priority<F>(
        &self,
        task: F,
        priority: TaskPriority,
    ) -> std::result::Result<(), LoopGone>
    where
        F: FnOnce() + Send + 'static,
    {
        self.reactor()?.spawn_compute(task, priority);
        Ok(())
    }

    /// See [`EventLoop::schedule_once`](crate::EventLoop::schedule_once).
    pub fn schedule_once<F>(
        &self,
        delay: Duration,
        callback: F,
    ) -> std::result::Result<TimerId, LoopGone>
    where
        F: FnOnce() + Send + 'static,
    {
        Ok(self.reactor()?.schedule_once(delay, callback))
    }

    /// See [`EventLoop::schedule_repeating`](crate::EventLoop::schedule_repeating).
    pub fn schedule_repeating<F>(
        &self,
        interval: Duration,
        callback: F,
    ) -> std::result::Result<TimerId, LoopGone>
    where
        F: Fn() + Send + Sync + 'static,
    {
        Ok(self.reactor()?.schedule_repeating(interval, callback))
    }

    /// See [`EventLoop::cancel_timer`](crate::EventLoop::cancel_timer).
    /// `false` once the loop is dropped.
    pub fn cancel_timer(&self, timer_id: TimerId) -> bool {
        self.reactor()
            .is_ok_and(|reactor| reactor.timer_wheel.cancel(timer_id))
    }

    /// See [`EventLoop::pending_timers`](crate::EventLoop::pending_timers).
    pub fn pending_timers(&self) -> std::result::Result<usize, LoopGone> {
        Ok(self.reactor()?.timer_wheel.pending_count())
    }

    /// See [`EventLoop::post`](crate::EventLoop::post). A failed wake-up is
    /// not reported: `f` is still queued and runs by the next poll timeout.
    pub fn post<F>(&self, f: F) -> std::result::Result<(), LoopGone>
    where
        F: FnOnce() + Send + 'static,
    {
        let _ = self.reactor()?.post(f);
        Ok(())
    }

    /// See [`EventLoop::handler_panics`](crate::EventLoop::handler_panics).
    pub fn handler_panics(&self) -> std::result::Result<u64, LoopGone> {
        Ok(self.reactor()?.handler_panics())
    }

    /// See [`EventLoop::timer_panics`](crate::EventLoop::timer_panics).
    pub fn timer_panics(&self) -> std::result::Result<u64, LoopGone> {
        Ok(self.reactor()?.timer_wheel.panics())
    }

    /// See [`EventLoop::get_compute_metrics`](crate::EventLoop::get_compute_metrics).
    pub fn get_compute_metrics(&self) -> std::result::Result<Arc<ComputePoolMetrics>, LoopGone> {
        Ok(self.reactor()?.get_compute_metrics())
    }

    /// See [`EventLoop::is_reactor_thread`](crate::EventLoop::is_reactor_thread).
    /// `false` once the loop is dropped.
    pub fn is_reactor_thread(&self) -> bool {
        self.reactor().is_ok_and(|r| r.is_reactor_thread())
    }

    /// See [`EventLoop::stop`](crate::EventLoop::stop). Does nothing once the
    /// loop is dropped.
    pub fn stop(&self) {
        if let Ok(reactor) = self.reactor() {
            reactor.get_shutdown_handle().shutdown();
        }
    }
}
