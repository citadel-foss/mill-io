use crate::{
    context::current_reactor,
    reactor::{current_reactor_id, Reactor},
    thread_pool::{JoinWatch, SharedTask},
    Handle, TaskPriority,
};
use parking_lot::{Condvar, Mutex};
use std::{
    any::Any,
    error::Error,
    fmt,
    panic::{catch_unwind, resume_unwind, AssertUnwindSafe},
    sync::Arc,
    time::{Duration, Instant},
};

/// Why a task spawned with [`spawn`] produced no value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinError {
    /// The task panicked; this is the panic message.
    Panicked(String),
    /// The task never ran: its loop was gone when it was spawned, or the task
    /// was dropped without running.
    LoopStopped,
}

impl fmt::Display for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinError::Panicked(message) => write!(f, "mill-io task panicked: {message}"),
            JoinError::LoopStopped => f.write_str("mill-io task never ran: its loop was dropped"),
        }
    }
}

impl Error for JoinError {}

/// Returned by [`JoinHandle::join_timeout`] when the task has no value to give
/// within the timeout. Holds the handle, so the task can still be joined.
pub struct JoinTimeout<T>(pub JoinHandle<T>);

impl<T> fmt::Debug for JoinTimeout<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("JoinTimeout").field(&self.0).finish()
    }
}

impl<T> fmt::Display for JoinTimeout<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("mill-io task did not finish in time")
    }
}

impl<T> Error for JoinTimeout<T> {}

enum Slot<F, T> {
    Queued(F),
    Running,
    /// The error is boxed to keep the packet, allocated per task, small.
    Done(Result<T, Box<JoinError>>),
    Taken,
}

struct State<F, T> {
    slot: Slot<F, T>,
    /// Set while a worker of the task's own pool waits in `join` and is counted
    /// in its pool's `JoinWatch::blocked`.
    #[cfg(debug_assertions)]
    counted_joiner: bool,
}

/// The task and its result in one allocation, shared by the compute pool's
/// queue and the [`JoinHandle`].
struct Packet<F, T> {
    state: Mutex<State<F, T>>,
    done: Condvar,
    #[cfg(debug_assertions)]
    join_watch: Option<Arc<JoinWatch>>,
}

impl<F, T> Packet<F, T> {
    fn new(slot: Slot<F, T>, join_watch: Option<&Arc<JoinWatch>>) -> Self {
        // Release builds skip the deadlock check, and the shared counter it needs.
        #[cfg(not(debug_assertions))]
        let _ = join_watch;
        Self {
            state: Mutex::new(State {
                slot,
                #[cfg(debug_assertions)]
                counted_joiner: false,
            }),
            done: Condvar::new(),
            #[cfg(debug_assertions)]
            join_watch: join_watch.cloned(),
        }
    }

    fn finish(&self, result: Result<T, JoinError>) {
        let mut state = self.state.lock();
        state.slot = Slot::Done(result.map_err(Box::new));
        #[cfg(debug_assertions)]
        if state.counted_joiner {
            state.counted_joiner = false;
            if let Some(watch) = &self.join_watch {
                *watch.blocked.lock() -= 1;
            }
        }
        drop(state);
        self.done.notify_all();
    }

    #[cfg(debug_assertions)]
    #[track_caller]
    fn check_join_deadlock(&self, state: &mut State<F, T>) {
        let Some(watch) = self.join_watch.as_ref().filter(|w| w.on_worker()) else {
            return;
        };
        if matches!(state.slot, Slot::Done(_) | Slot::Taken) {
            return;
        }
        let mut blocked = watch.blocked.lock();
        if matches!(state.slot, Slot::Queued(_)) && *blocked + 1 >= watch.workers {
            drop(blocked);
            panic!(
                "mill_io::JoinHandle::join called on a compute worker while every \
                 other worker of the pool waits in join and the task has not \
                 started; this would deadlock"
            );
        }
        *blocked += 1;
        state.counted_joiner = true;
    }
}

impl<F, T> SharedTask for Packet<F, T>
where
    F: FnOnce() -> T + Send,
    T: Send,
{
    fn run(&self) {
        let f = {
            let mut state = self.state.lock();
            match std::mem::replace(&mut state.slot, Slot::Running) {
                Slot::Queued(f) => f,
                other => {
                    state.slot = other;
                    return;
                }
            }
        };
        match catch_unwind(AssertUnwindSafe(f)) {
            Ok(value) => self.finish(Ok(value)),
            Err(payload) => {
                self.finish(Err(JoinError::Panicked(panic_message(&*payload))));
                // Re-raised so the pool counts it in `tasks_failed`, as for `spawn_compute`.
                resume_unwind(payload)
            }
        }
    }

    fn abandon(&self) {
        let unrun = {
            let mut state = self.state.lock();
            match std::mem::replace(&mut state.slot, Slot::Running) {
                Slot::Queued(f) => Some(f),
                other => {
                    state.slot = other;
                    None
                }
            }
        };
        if unrun.is_some() {
            drop(unrun);
            self.finish(Err(JoinError::LoopStopped));
        }
    }
}

/// What a [`JoinHandle`] needs from its packet, without the task's type.
trait Outcome<T>: Send + Sync {
    fn is_finished(&self) -> bool;
    /// Waits until the task finishes or `deadline` passes, then takes the
    /// result if there is one and `only_ok` doesn't reject it.
    fn take(&self, deadline: Option<Instant>, only_ok: bool) -> Option<Result<T, JoinError>>;
}

impl<F, T> Outcome<T> for Packet<F, T>
where
    F: Send,
    T: Send,
{
    fn is_finished(&self) -> bool {
        !matches!(self.state.lock().slot, Slot::Queued(_) | Slot::Running)
    }

    #[track_caller]
    fn take(&self, deadline: Option<Instant>, only_ok: bool) -> Option<Result<T, JoinError>> {
        let mut state = self.state.lock();
        #[cfg(debug_assertions)]
        if deadline.is_none() {
            self.check_join_deadlock(&mut state);
        }
        while matches!(state.slot, Slot::Queued(_) | Slot::Running) {
            match deadline {
                Some(deadline) => {
                    if self.done.wait_until(&mut state, deadline).timed_out() {
                        return None;
                    }
                }
                None => self.done.wait(&mut state),
            }
        }
        if only_ok && !matches!(state.slot, Slot::Done(Ok(_))) {
            return None;
        }
        match std::mem::replace(&mut state.slot, Slot::Taken) {
            Slot::Done(result) => Some(result.map_err(|e| *e)),
            _ => Some(Err(JoinError::LoopStopped)),
        }
    }
}

fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

/// Owns the result of a task started with [`spawn`]. Dropping it detaches the
/// task, which still runs.
pub struct JoinHandle<T> {
    packet: Arc<dyn Outcome<T>>,
    /// 0 for a task that was never queued.
    reactor_id: usize,
}

impl<T> fmt::Debug for JoinHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JoinHandle")
            .field("finished", &self.is_finished())
            .finish()
    }
}

impl<T: Send + 'static> JoinHandle<T> {
    fn loop_stopped() -> Self {
        let packet: Packet<fn() -> T, T> =
            Packet::new(Slot::Done(Err(Box::new(JoinError::LoopStopped))), None);
        Self {
            packet: Arc::new(packet),
            reactor_id: 0,
        }
    }
}

impl<T> JoinHandle<T> {
    /// Waits for the task and returns its value, or why there is none.
    ///
    /// ## Panics
    ///
    /// On the reactor thread of the task's loop, where waiting would stall the
    /// loop and deadlock any task that needs it; use
    /// [`join_timeout`](Self::join_timeout) or [`is_finished`](Self::is_finished)
    /// there, or [`post`](crate::post) the result back from the task.
    ///
    /// A compute worker that joins a task of its own pool waits for a free
    /// worker to run it, so a full pool of such joins deadlocks. Debug builds
    /// panic instead when every other worker of the pool is already waiting in
    /// `join` and the task has not started.
    #[track_caller]
    pub fn join(self) -> Result<T, JoinError> {
        if self.reactor_id != 0 && self.reactor_id == current_reactor_id() {
            panic!(
                "mill_io::JoinHandle::join called on its loop's reactor thread; \
                 waiting there would deadlock the loop. Use join_timeout, \
                 is_finished or post() instead"
            );
        }
        self.packet
            .take(None, false)
            .unwrap_or(Err(JoinError::LoopStopped))
    }

    /// Waits up to `d` for the task's value.
    ///
    /// Returns the handle in [`JoinTimeout`] when there is no value by then:
    /// the task is still running, or it finished without a value, in which
    /// case [`join`](Self::join) on the returned handle reports why at once.
    /// Unlike `join`, it may be called on the reactor thread.
    pub fn join_timeout(self, d: Duration) -> Result<T, JoinTimeout<T>> {
        let deadline = Instant::now()
            .checked_add(d)
            .unwrap_or_else(|| Instant::now() + Duration::from_secs(u32::MAX.into()));
        match self.packet.take(Some(deadline), true) {
            Some(Ok(value)) => Ok(value),
            _ => Err(JoinTimeout(self)),
        }
    }

    /// True once the task returned, panicked, or is known never to run.
    pub fn is_finished(&self) -> bool {
        self.packet.is_finished()
    }
}

pub(crate) fn spawn_on<F, T>(reactor: &Reactor, priority: TaskPriority, f: F) -> JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let compute_pool = reactor.compute_pool();
    let packet = Arc::new(Packet::new(
        Slot::Queued(f),
        Some(compute_pool.join_watch()),
    ));
    compute_pool.spawn_shared(packet.clone(), priority);
    JoinHandle {
        packet,
        reactor_id: reactor.id(),
    }
}

impl Handle {
    /// Runs `f` on this loop's compute pool with normal priority; see [`spawn`].
    pub fn spawn<F, T>(&self, f: F) -> JoinHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.spawn_with_priority(TaskPriority::Normal, f)
    }

    /// Runs `f` on this loop's compute pool with priority `p`; see
    /// [`spawn_with_priority`].
    pub fn spawn_with_priority<F, T>(&self, p: TaskPriority, f: F) -> JoinHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        match self.reactor() {
            Ok(reactor) => spawn_on(&reactor, p, f),
            Err(_) => JoinHandle::loop_stopped(),
        }
    }
}

/// Runs `f` on the current loop's compute pool with normal priority and
/// returns a handle to its result.
///
/// A panic in `f` is caught and returned by [`JoinHandle::join`] as
/// [`JoinError::Panicked`]. If the current loop was dropped, `f` is dropped
/// and `join` returns [`JoinError::LoopStopped`]. Tasks already queued when
/// the loop is dropped still run.
///
/// `T` must be `Send`, so a task cannot return
/// [`mill_io::error::Result`](crate::error::Result): its `Box<dyn Error>` is
/// not `Send`. Return a `Send` error type instead, for example
/// `Result<T, String>` or `Box<dyn Error + Send + Sync>`.
///
/// ## Panics
///
/// Outside a mill-io context.
///
/// ```rust
/// use mill_io::{EventLoop, JoinError};
///
/// let event_loop = EventLoop::default();
/// let _guard = event_loop.enter();
///
/// let sum = mill_io::spawn(|| (1..=10).sum::<u32>());
/// assert_eq!(sum.join(), Ok(55));
///
/// let failed = mill_io::spawn(|| -> u32 { panic!("bad input") });
/// assert_eq!(failed.join(), Err(JoinError::Panicked("bad input".into())));
/// ```
#[track_caller]
pub fn spawn<F, T>(f: F) -> JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    spawn_named("spawn", TaskPriority::Normal, f)
}

/// [`spawn`] with a [`TaskPriority`].
///
/// ## Panics
///
/// Outside a mill-io context.
///
/// ```rust
/// use mill_io::{EventLoop, TaskPriority};
///
/// let event_loop = EventLoop::default();
/// let _guard = event_loop.enter();
/// let urgent = mill_io::spawn_with_priority(TaskPriority::Critical, || "first");
/// assert_eq!(urgent.join(), Ok("first"));
/// ```
#[track_caller]
pub fn spawn_with_priority<F, T>(p: TaskPriority, f: F) -> JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    spawn_named("spawn_with_priority", p, f)
}

#[track_caller]
fn spawn_named<F, T>(function: &str, p: TaskPriority, f: F) -> JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    match current_reactor(function) {
        Ok(reactor) => spawn_on(&reactor, p, f),
        Err(_) => JoinHandle::loop_stopped(),
    }
}

/// Spawns each closure on the current loop's compute pool, waits for all of
/// them, and returns their results as a tuple of `Result<T, JoinError>` in
/// argument order.
///
/// It blocks the calling thread; it is not a futures combinator. The closures
/// run at the same time when the pool has enough free workers. A panic in one
/// closure is returned as its [`JoinError::Panicked`]; the others still return.
///
/// ## Panics
///
/// Outside a mill-io context, and wherever [`JoinHandle::join`] panics.
///
/// ```rust
/// use mill_io::EventLoop;
///
/// let event_loop = EventLoop::default();
/// let _guard = event_loop.enter();
/// let (sum, text) = mill_io::join!(|| 2 + 2, || "four".to_string());
/// assert_eq!(sum, Ok(4));
/// assert_eq!(text.as_deref(), Ok("four"));
/// ```
#[macro_export]
macro_rules! join {
    ($($task:expr),+ $(,)?) => {
        $crate::__join_spawn!([] $($task,)+)
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __join_spawn {
    ([$($handle:ident)*] $task:expr, $($rest:expr,)*) => {{
        // Each expansion's `handle` is a distinct binding (macro hygiene).
        let handle = $crate::spawn($task);
        $crate::__join_spawn!([$($handle)* handle] $($rest,)*)
    }};
    ([$($handle:ident)*]) => {
        ($($handle.join(),)*)
    };
}
