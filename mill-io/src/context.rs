use crate::{
    handle::{Handle, Upgraded},
    TimerId,
};
use std::{
    cell::{Cell, RefCell},
    marker::PhantomData,
    time::Duration,
};

thread_local! {
    /// Entered loops, innermost last, each tagged with the id of the guard that
    /// entered it so a guard dropped out of order removes only its own entry.
    static CONTEXT: RefCell<Vec<(u64, Handle)>> = const { RefCell::new(Vec::new()) };
    static NEXT_GUARD: Cell<u64> = const { Cell::new(0) };
}

/// Keeps a loop current on this thread; created by
/// [`EventLoop::enter`](crate::EventLoop::enter) and [`Handle::enter`].
///
/// Guards nest: dropping one makes the loop that was current before it current
/// again. A guard dropped out of order removes only its own loop, so the
/// innermost guard still alive decides the current loop. Not `Send`: the
/// context belongs to the thread that entered it.
#[must_use = "the loop stops being current when the guard is dropped"]
#[derive(Debug)]
pub struct EnterGuard<'a> {
    id: u64,
    _not_send: PhantomData<*const ()>,
    _handle: PhantomData<&'a Handle>,
}

impl EnterGuard<'_> {
    pub(crate) fn new(handle: Handle) -> Self {
        let id = NEXT_GUARD.with(|next| {
            let id = next.get();
            next.set(id + 1);
            id
        });
        CONTEXT.with(|context| context.borrow_mut().push((id, handle)));
        Self {
            id,
            _not_send: PhantomData,
            _handle: PhantomData,
        }
    }
}

impl Drop for EnterGuard<'_> {
    fn drop(&mut self) {
        // `try_with`: a guard owned by a thread-local may drop during thread teardown.
        let removed = CONTEXT.try_with(|context| {
            let mut context = context.borrow_mut();
            let position = context.iter().rposition(|(id, _)| *id == self.id);
            position.map(|position| context.remove(position))
        });
        drop(removed);
    }
}

#[track_caller]
fn outside(function: &str) -> ! {
    panic!(
        "mill_io::{function} called outside a mill-io context; \
         use EventLoop::enter() or #[mill_io::main]"
    )
}

/// The current loop's reactor, or `Err` if that loop was dropped. Panics outside a context.
#[track_caller]
pub(crate) fn current_reactor(function: &str) -> std::result::Result<Upgraded, crate::LoopGone> {
    let reactor = CONTEXT
        .try_with(|context| context.borrow().last().map(|(_, handle)| handle.reactor()))
        .ok()
        .flatten();
    match reactor {
        Some(reactor) => reactor,
        None => outside(function),
    }
}

/// Returns the current thread's loop, or `None` outside a mill-io context.
///
/// A context is set on the thread inside [`EventLoop::run`](crate::EventLoop::run),
/// on the loop's I/O and compute workers, and on any thread holding an
/// [`EnterGuard`].
///
/// ```rust
/// use mill_io::EventLoop;
///
/// assert!(mill_io::try_current().is_none());
/// let event_loop = EventLoop::default();
/// let _guard = event_loop.enter();
/// assert_eq!(mill_io::try_current(), Some(event_loop.handle()));
/// ```
pub fn try_current() -> Option<Handle> {
    CONTEXT
        .try_with(|context| context.borrow().last().map(|(_, handle)| handle.clone()))
        .ok()
        .flatten()
}

/// Returns the current thread's loop.
///
/// Code that runs on a loop's threads sees that loop, including callbacks a
/// library runs on a private loop of its own: a function that must reach your
/// loop should take a [`Handle`] rather than rely on `current()`.
///
/// ## Panics
///
/// Outside a mill-io context; see [`try_current`].
///
/// ```rust
/// use mill_io::EventLoop;
/// use std::thread;
///
/// let event_loop = EventLoop::default();
/// let _guard = event_loop.enter();
/// let handle = mill_io::current();
/// thread::spawn(move || {
///     let _guard = handle.enter();
///     mill_io::post(|| println!("runs on the reactor thread"));
/// })
/// .join()
/// .unwrap();
/// ```
#[track_caller]
pub fn current() -> Handle {
    match try_current() {
        Some(handle) => handle,
        None => outside("current"),
    }
}

/// Runs `f` on the current loop's reactor thread; see
/// [`EventLoop::post`](crate::EventLoop::post). If the loop was dropped, `f` is
/// dropped without running.
///
/// ## Panics
///
/// Outside a mill-io context.
///
/// ```rust
/// use mill_io::EventLoop;
///
/// let event_loop = EventLoop::default();
/// let _guard = event_loop.enter();
/// let stopper = mill_io::current();
/// mill_io::post(move || stopper.stop());
/// event_loop.run()?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[track_caller]
pub fn post<F>(f: F)
where
    F: FnOnce() + Send + 'static,
{
    if let Ok(reactor) = current_reactor("post") {
        // An error means only that the wake-up failed; `f` is still queued.
        let _ = reactor.post(f);
    }
}

/// Schedules `f` to run once on the current loop's reactor thread after
/// `delay`; see [`EventLoop::schedule_once`](crate::EventLoop::schedule_once).
/// If the loop was dropped, `f` is dropped and the returned id cancels nothing.
///
/// ## Panics
///
/// Outside a mill-io context.
///
/// ```rust
/// use mill_io::EventLoop;
/// use std::time::Duration;
///
/// let event_loop = EventLoop::default();
/// let _guard = event_loop.enter();
/// let stopper = mill_io::current();
/// mill_io::after(Duration::from_millis(10), move || stopper.stop());
/// event_loop.run()?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[track_caller]
pub fn after<F>(delay: Duration, f: F) -> TimerId
where
    F: FnOnce() + Send + 'static,
{
    match current_reactor("after") {
        Ok(reactor) => reactor.schedule_once(delay, f),
        Err(_) => TimerId::NONE,
    }
}

/// Schedules `f` to run on the current loop's reactor thread every
/// `interval`; see [`EventLoop::schedule_repeating`](crate::EventLoop::schedule_repeating).
/// If the loop was dropped, `f` is dropped and the returned id cancels nothing.
///
/// ## Panics
///
/// Outside a mill-io context.
///
/// ```rust
/// use mill_io::EventLoop;
/// use std::{
///     sync::atomic::{AtomicUsize, Ordering},
///     time::Duration,
/// };
///
/// let event_loop = EventLoop::default();
/// let _guard = event_loop.enter();
/// let ticks = AtomicUsize::new(0);
/// let stopper = mill_io::current();
/// mill_io::every(Duration::from_millis(5), move || {
///     if ticks.fetch_add(1, Ordering::SeqCst) == 2 {
///         stopper.stop();
///     }
/// });
/// event_loop.run()?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[track_caller]
pub fn every<F>(interval: Duration, f: F) -> TimerId
where
    F: Fn() + Send + Sync + 'static,
{
    match current_reactor("every") {
        Ok(reactor) => reactor.schedule_repeating(interval, f),
        Err(_) => TimerId::NONE,
    }
}

/// Cancels a timer of the current loop; see
/// [`EventLoop::cancel_timer`](crate::EventLoop::cancel_timer). `false` if the
/// loop was dropped.
///
/// ## Panics
///
/// Outside a mill-io context.
///
/// ```rust
/// use mill_io::EventLoop;
/// use std::time::Duration;
///
/// let event_loop = EventLoop::default();
/// let _guard = event_loop.enter();
/// let id = mill_io::after(Duration::from_secs(60), || unreachable!());
/// assert!(mill_io::cancel_timer(id));
/// assert!(!mill_io::cancel_timer(id));
/// ```
#[track_caller]
pub fn cancel_timer(id: TimerId) -> bool {
    match current_reactor("cancel_timer") {
        Ok(reactor) => reactor.timer_wheel.cancel(id),
        Err(_) => false,
    }
}
