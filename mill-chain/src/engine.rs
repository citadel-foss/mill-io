//! The request engine: one connection per backend, replies correlated by id, deadlines on
//! the loop's timer wheel, retries of transport failures, and reconnect with backoff.
//!
//! Threading invariants:
//! - `State` is only locked on the event loop thread (posted closures, timers and mill-net
//!   callbacks in direct mode), so the mutex is never contended and never held across a
//!   blocking wait.
//! - mill-net may run `on_disconnect` and `on_error` synchronously inside a send or a drop
//!   made under that lock, so those two callbacks only post to the loop; every other
//!   callback is entered from the reactor with the lock free.
//! - A caller's reply channel is completed only after the call left `State` and the
//!   `pending` gauge was updated, so `stats()` read after a call returns includes it.

use crate::{ChainError, ChainEvent};
use crossbeam_channel::{Receiver, Sender};
use mill_io::{EventLoop, TimerId};
use mill_net::errors::{NetworkError, Result as NetResult};
use mill_net::tcp::{ConnSender, ConnectionId, SendError, ServerContext};
use mill_net::tcp::{NetworkHandler, TcpClient, TcpClientConfig};
use serde_json::Value;
use std::collections::hash_map::RandomState;
use std::collections::{HashMap, VecDeque};
use std::hash::{BuildHasher, Hasher};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError, RwLock, Weak};
use std::thread::{self, ThreadId};
use std::time::{Duration, Instant};

/// Tuning for the request engine. The defaults suit a local backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineOptions {
    /// Deadline for calls on a handle that does not set one. Default 30 s.
    pub default_deadline: Duration,
    /// Retries for calls on a handle that does not set them. Default 0.
    pub default_retries: u8,
    /// Wait before the first reconnect attempt. Default 100 ms.
    pub backoff_initial: Duration,
    /// Longest wait between reconnect attempts. Default 5 s. The wait doubles on each
    /// failed attempt, with ±20% jitter, and never exceeds this.
    pub backoff_max: Duration,
    /// Most calls held while the connection is down; further calls fail at once with
    /// [`ChainError::Transport`]. Default 1024.
    pub reconnect_queue_limit: usize,
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            default_deadline: Duration::from_secs(30),
            default_retries: 0,
            backoff_initial: Duration::from_millis(100),
            backoff_max: Duration::from_secs(5),
            reconnect_queue_limit: 1024,
        }
    }
}

/// Counters describing a client, read without a round trip to the event loop.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ClientStats {
    /// Calls written and not yet answered, plus calls queued for a connection.
    pub pending_requests: usize,
    /// Requests written to a connection, retries included.
    pub requests_sent: u64,
    /// Successful connections after the first.
    pub reconnects: u64,
    /// [`ChainEvent::Resync`] events put on the events channel.
    pub resyncs: u64,
    /// Open backend connections.
    pub connections_open: usize,
}

/// One decoded reply.
pub(crate) struct Reply {
    pub(crate) id: u64,
    pub(crate) result: Result<Value, ChainError>,
}

/// Turns calls into bytes and bytes into replies for one connection.
pub(crate) trait Codec: Send {
    fn encode(
        &mut self,
        id: u64,
        method: &str,
        params: &Value,
        out: &mut Vec<u8>,
    ) -> Result<(), ChainError>;
    fn decode(&mut self, data: &[u8], replies: &mut Vec<Reply>);
    /// Forgets partial input; called when a connection is replaced.
    fn reset(&mut self);
}

pub(crate) type ReplyTx = Sender<Result<Value, ChainError>>;

/// A blocking caller's request, as handed to the loop.
pub(crate) struct Call {
    pub(crate) method: String,
    pub(crate) params: Value,
    pub(crate) deadline: Option<Instant>,
    pub(crate) retries: u8,
    pub(crate) cancel: Option<Arc<AtomicBool>>,
    pub(crate) reply: ReplyTx,
}

impl Call {
    fn is_cancelled(&self) -> bool {
        self.cancel
            .as_ref()
            .is_some_and(|flag| flag.load(Ordering::SeqCst))
    }
}

struct Pending {
    call: Call,
    timer: Option<TimerId>,
    /// Set while the call is written and unanswered; `None` while it is queued.
    wire_id: Option<u64>,
}

enum Link {
    Idle,
    Connecting {
        _client: TcpClient<Events>,
        id: ConnectionId,
    },
    Connected {
        _client: TcpClient<Events>,
        sender: ConnSender,
        id: ConnectionId,
    },
    Backoff(TimerId),
}

impl Link {
    fn id(&self) -> Option<ConnectionId> {
        match self {
            Link::Connecting { id, .. } | Link::Connected { id, .. } => Some(*id),
            Link::Idle | Link::Backoff(_) => None,
        }
    }
}

pub(crate) struct State {
    closed: bool,
    link: Link,
    codec: Box<dyn Codec>,
    calls: HashMap<u64, Pending>,
    /// Wire id to call id, for calls written to the current connection.
    in_flight: HashMap<u64, u64>,
    /// Calls waiting for a writable connection, oldest first.
    queue: VecDeque<u64>,
    /// A send hit the write queue's high watermark; writing resumes on `on_drained`.
    blocked: bool,
    next_wire_id: u64,
    /// Reconnect attempts since a connection last proved healthy: it answered a call, or
    /// stayed up for `backoff_max`.
    attempt: u32,
    connected_at: Option<Instant>,
    jitter: f64,
    rng: u64,
    connected_before: bool,
    events: Option<Sender<ChainEvent>>,
    scratch: Vec<u8>,
}

#[derive(Default)]
struct Stats {
    pending: AtomicUsize,
    sent: AtomicU64,
    reconnects: AtomicU64,
    resyncs: AtomicU64,
    open: AtomicUsize,
}

/// Where and how to connect.
pub(crate) struct Endpoint {
    pub(crate) addr: SocketAddr,
    pub(crate) codec: Box<dyn Codec>,
    pub(crate) events_capacity: usize,
}

pub(crate) struct Engine {
    me: Weak<Engine>,
    addr: SocketAddr,
    pub(crate) options: EngineOptions,
    /// Taken out at shutdown, after the loop thread is joined, so the loop and its worker
    /// threads are dropped by the thread that shuts down.
    event_loop: RwLock<Option<Arc<EventLoop>>>,
    loop_thread: OnceLock<ThreadId>,
    closed: AtomicBool,
    next_call_id: AtomicU64,
    state: Mutex<State>,
    stats: Stats,
}

/// The connection callbacks.
pub(crate) struct Events {
    engine: Weak<Engine>,
}

impl NetworkHandler for Events {
    fn on_connect(&self, _ctx: &ServerContext, conn: ConnectionId) -> NetResult<()> {
        if let Some(engine) = self.engine.upgrade() {
            engine.on_connected(conn);
        }
        Ok(())
    }

    fn on_data(&self, _ctx: &ServerContext, conn: ConnectionId, data: &[u8]) -> NetResult<()> {
        if let Some(engine) = self.engine.upgrade() {
            engine.on_data(conn, data);
        }
        Ok(())
    }

    fn on_drained(&self, _ctx: &ServerContext, conn: ConnectionId) -> NetResult<()> {
        if let Some(engine) = self.engine.upgrade() {
            engine.on_drained(conn);
        }
        Ok(())
    }

    fn on_disconnect(&self, _ctx: &ServerContext, conn: ConnectionId) -> NetResult<()> {
        if let Some(engine) = self.engine.upgrade() {
            engine.post_lost(conn, "connection closed".to_string());
        }
        Ok(())
    }

    fn on_error(&self, _ctx: &ServerContext, conn: Option<ConnectionId>, error: NetworkError) {
        match (conn, &error) {
            (Some(conn), NetworkError::Connect(_) | NetworkError::Io(_)) => {
                if let Some(engine) = self.engine.upgrade() {
                    engine.post_lost(conn, error.to_string());
                }
            }
            _ => log::debug!("backend connection: {}", error),
        }
    }
}

impl Engine {
    pub(crate) fn new(
        endpoint: Endpoint,
        options: EngineOptions,
        event_loop: Arc<EventLoop>,
        events: Sender<ChainEvent>,
    ) -> Arc<Self> {
        let seed = RandomState::new().build_hasher().finish() | 1;
        Arc::new_cyclic(|me| Engine {
            me: me.clone(),
            addr: endpoint.addr,
            options,
            event_loop: RwLock::new(Some(event_loop)),
            loop_thread: OnceLock::new(),
            closed: AtomicBool::new(false),
            next_call_id: AtomicU64::new(1),
            state: Mutex::new(State {
                closed: false,
                link: Link::Idle,
                codec: endpoint.codec,
                calls: HashMap::new(),
                in_flight: HashMap::new(),
                queue: VecDeque::new(),
                blocked: false,
                next_wire_id: 1,
                attempt: 0,
                connected_at: None,
                jitter: 1.0,
                rng: seed,
                connected_before: false,
                events: Some(events),
                scratch: Vec::new(),
            }),
            stats: Stats::default(),
        })
    }

    /// Sends of `events` beyond `capacity` turn into one `Resync`, which needs a slot.
    pub(crate) fn events_channel(capacity: usize) -> (Sender<ChainEvent>, Receiver<ChainEvent>) {
        crossbeam_channel::bounded(capacity.saturating_add(1))
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn event_loop(&self) -> Option<Arc<EventLoop>> {
        self.event_loop
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Runs `f` on the loop thread. Fails with `Shutdown` once the loop is gone.
    pub(crate) fn post(&self, f: impl FnOnce() + Send + 'static) -> Result<(), ChainError> {
        let event_loop = self.event_loop().ok_or(ChainError::Shutdown)?;
        // A failed wake-up still leaves `f` queued; it runs by the next poll timeout.
        let _ = event_loop.post(f);
        Ok(())
    }

    pub(crate) fn set_loop_thread(&self) {
        let _ = self.loop_thread.set(thread::current().id());
    }

    pub(crate) fn is_loop_thread(&self) -> bool {
        self.loop_thread.get() == Some(&thread::current().id())
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    pub(crate) fn stats(&self) -> ClientStats {
        ClientStats {
            pending_requests: self.stats.pending.load(Ordering::SeqCst),
            requests_sent: self.stats.sent.load(Ordering::SeqCst),
            reconnects: self.stats.reconnects.load(Ordering::SeqCst),
            resyncs: self.stats.resyncs.load(Ordering::SeqCst),
            connections_open: self.stats.open.load(Ordering::SeqCst),
        }
    }

    /// Hands `call` to the loop; returns the id to cancel it by.
    pub(crate) fn submit(&self, call: Call) -> Result<u64, ChainError> {
        let id = self.next_call_id.fetch_add(1, Ordering::Relaxed);
        let engine = self.me.upgrade().ok_or(ChainError::Shutdown)?;
        self.post(move || engine.on_submit(id, call))?;
        Ok(id)
    }

    /// Ends call `id` with `error` if it is still pending.
    pub(crate) fn abandon(&self, id: u64, error: ChainError) {
        if let Some(engine) = self.me.upgrade() {
            let _ = self.post(move || engine.on_abandon(id, error));
        }
    }

    /// Asks the loop to stop; calls made from now on fail with `Shutdown`.
    pub(crate) fn stop(&self) {
        self.closed.store(true, Ordering::SeqCst);
        if let Some(event_loop) = self.event_loop() {
            event_loop.stop();
        }
    }

    /// Drops this engine's reference to the loop.
    pub(crate) fn release_loop(&self) -> Option<Arc<EventLoop>> {
        self.event_loop
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    pub(crate) fn start(&self) {
        let Some(event_loop) = self.event_loop() else {
            return;
        };
        let mut st = self.lock();
        if matches!(st.link, Link::Idle) && !st.closed {
            self.connect(&mut st, &event_loop);
        }
    }

    /// Runs on the loop thread once `run` returned: every call still pending gets
    /// `Shutdown`, the connection closes and the events channel disconnects.
    pub(crate) fn close_all(&self) {
        self.closed.store(true, Ordering::SeqCst);
        let event_loop = self.event_loop();
        let mut st = self.lock();
        st.closed = true;
        if let (Link::Backoff(timer), Some(event_loop)) = (&st.link, &event_loop) {
            event_loop.cancel_timer(*timer);
        }
        st.link = Link::Idle;
        self.stats.open.store(0, Ordering::SeqCst);
        st.queue.clear();
        st.in_flight.clear();
        let calls: Vec<Pending> = st.calls.drain().map(|(_, pending)| pending).collect();
        self.stats.pending.store(0, Ordering::SeqCst);
        st.events = None;
        drop(st);
        for pending in calls {
            if let (Some(timer), Some(event_loop)) = (pending.timer, &event_loop) {
                event_loop.cancel_timer(timer);
            }
            let _ = pending.call.reply.send(Err(ChainError::Shutdown));
        }
    }

    fn on_submit(&self, id: u64, call: Call) {
        let Some(event_loop) = self.event_loop() else {
            let _ = call.reply.send(Err(ChainError::Shutdown));
            return;
        };
        let mut st = self.lock();
        if st.closed {
            let _ = call.reply.send(Err(ChainError::Shutdown));
            return;
        }
        if call.is_cancelled() {
            let _ = call.reply.send(Err(ChainError::Cancelled));
            return;
        }
        let writable = matches!(st.link, Link::Connected { .. }) && !st.blocked;
        if !writable && st.queue.len() >= self.options.reconnect_queue_limit {
            let _ = call
                .reply
                .send(Err(ChainError::Transport("reconnect queue full".into())));
            return;
        }
        let timer = match call.deadline {
            None => None,
            Some(deadline) => match self.arm_deadline(&event_loop, id, deadline) {
                Some(timer) => Some(timer),
                None => {
                    let _ = call.reply.send(Err(ChainError::Timeout));
                    return;
                }
            },
        };
        st.calls.insert(
            id,
            Pending {
                call,
                timer,
                wire_id: None,
            },
        );
        self.stats.pending.store(st.calls.len(), Ordering::SeqCst);
        st.queue.push_back(id);
        self.flush(&mut st, &event_loop);
    }

    /// Schedules the deadline check for call `id`; `None` if `deadline` has passed.
    fn arm_deadline(&self, event_loop: &EventLoop, id: u64, deadline: Instant) -> Option<TimerId> {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return None;
        }
        let engine = self.me.clone();
        // The wheel truncates delays to whole ticks; rounding up keeps it from firing early.
        Some(event_loop.schedule_once(round_up_to_ms(left), move || {
            if let Some(engine) = engine.upgrade() {
                engine.on_deadline(id);
            }
        }))
    }

    fn on_deadline(&self, id: u64) {
        let Some(event_loop) = self.event_loop() else {
            return;
        };
        let mut st = self.lock();
        let Some(pending) = st.calls.get_mut(&id) else {
            return;
        };
        let rearmed = pending
            .call
            .deadline
            .and_then(|deadline| self.arm_deadline(&event_loop, id, deadline));
        match rearmed {
            Some(timer) => pending.timer = Some(timer),
            None => self.finish(&mut st, &event_loop, id, Err(ChainError::Timeout)),
        }
    }

    fn on_abandon(&self, id: u64, error: ChainError) {
        if let Some(event_loop) = self.event_loop() {
            let mut st = self.lock();
            self.finish(&mut st, &event_loop, id, Err(error));
        }
    }

    fn on_connected(&self, conn: ConnectionId) {
        let Some(event_loop) = self.event_loop() else {
            return;
        };
        let mut st = self.lock();
        if st.closed || st.link.id() != Some(conn) {
            return;
        }
        let Link::Connecting {
            _client: client,
            id,
        } = std::mem::replace(&mut st.link, Link::Idle)
        else {
            return;
        };
        let sender = client.sender();
        st.link = Link::Connected {
            _client: client,
            sender,
            id,
        };
        if st.connected_before {
            self.stats.reconnects.fetch_add(1, Ordering::SeqCst);
        }
        st.connected_before = true;
        st.connected_at = Some(Instant::now());
        self.stats.open.store(1, Ordering::SeqCst);
        self.flush(&mut st, &event_loop);
    }

    fn on_data(&self, conn: ConnectionId, data: &[u8]) {
        let Some(event_loop) = self.event_loop() else {
            return;
        };
        let mut st = self.lock();
        if st.link.id() != Some(conn) {
            return;
        }
        let mut replies = Vec::new();
        st.codec.decode(data, &mut replies);
        for reply in replies {
            let Some(&call) = st.in_flight.get(&reply.id) else {
                log::debug!("ignoring a reply for unknown id {}", reply.id);
                continue;
            };
            st.attempt = 0;
            let deadline = st.calls.get(&call).and_then(|p| p.call.deadline);
            let result = if expired(deadline, Instant::now()) {
                Err(ChainError::Timeout)
            } else {
                reply.result
            };
            self.finish(&mut st, &event_loop, call, result);
        }
    }

    fn on_drained(&self, conn: ConnectionId) {
        let Some(event_loop) = self.event_loop() else {
            return;
        };
        let mut st = self.lock();
        if st.link.id() == Some(conn) {
            st.blocked = false;
            self.flush(&mut st, &event_loop);
        }
    }

    fn post_lost(&self, conn: ConnectionId, reason: String) {
        if let Some(engine) = self.me.upgrade() {
            let _ = self.post(move || engine.on_lost(conn, reason));
        }
    }

    fn on_lost(&self, conn: ConnectionId, reason: String) {
        if let Some(event_loop) = self.event_loop() {
            let mut st = self.lock();
            self.lost(&mut st, &event_loop, conn, &reason);
        }
    }

    fn on_backoff_elapsed(&self) {
        let Some(event_loop) = self.event_loop() else {
            return;
        };
        let mut st = self.lock();
        if matches!(st.link, Link::Backoff(_)) && !st.closed {
            self.connect(&mut st, &event_loop);
        }
    }

    /// Writes queued calls while the connection takes them.
    fn flush(&self, st: &mut State, event_loop: &Arc<EventLoop>) {
        let (sender, conn) = match &st.link {
            Link::Connected { sender, id, .. } => (sender.clone(), *id),
            _ => return,
        };
        let mut buf = std::mem::take(&mut st.scratch);
        while !st.blocked {
            let Some(id) = st.queue.pop_front() else {
                break;
            };
            let Some(pending) = st.calls.get(&id) else {
                continue;
            };
            if pending.call.is_cancelled() {
                self.finish(st, event_loop, id, Err(ChainError::Cancelled));
                continue;
            }
            let wire_id = st.next_wire_id;
            st.next_wire_id += 1;
            buf.clear();
            if let Err(e) = st.codec.encode(
                wire_id,
                &pending.call.method,
                &pending.call.params,
                &mut buf,
            ) {
                self.finish(st, event_loop, id, Err(e));
                continue;
            }
            match sender.send(&buf) {
                Ok(()) => {
                    self.stats.sent.fetch_add(1, Ordering::SeqCst);
                    self.mark_written(st, id, wire_id);
                }
                Err(SendError::Backpressure) => {
                    st.queue.push_front(id);
                    st.blocked = true;
                }
                Err(e) => {
                    self.mark_written(st, id, wire_id);
                    let reason = format!("write failed: {}", e);
                    self.lost(st, event_loop, conn, &reason);
                    break;
                }
            }
        }
        st.scratch = buf;
    }

    fn mark_written(&self, st: &mut State, id: u64, wire_id: u64) {
        if let Some(pending) = st.calls.get_mut(&id) {
            pending.wire_id = Some(wire_id);
            st.in_flight.insert(wire_id, id);
        }
    }

    /// Removes call `id` and sends its result.
    fn finish(
        &self,
        st: &mut State,
        event_loop: &EventLoop,
        id: u64,
        result: Result<Value, ChainError>,
    ) {
        let Some(pending) = st.calls.remove(&id) else {
            return;
        };
        if let Some(timer) = pending.timer {
            event_loop.cancel_timer(timer);
        }
        match pending.wire_id {
            Some(wire_id) => {
                st.in_flight.remove(&wire_id);
            }
            None => st.queue.retain(|&queued| queued != id),
        }
        self.stats.pending.store(st.calls.len(), Ordering::SeqCst);
        let _ = pending.call.reply.send(result);
    }

    /// Connection `conn` is gone: calls written to it are retried or fail with
    /// `Transport`, and a reconnect is scheduled.
    fn lost(&self, st: &mut State, event_loop: &Arc<EventLoop>, conn: ConnectionId, reason: &str) {
        if st.link.id() != Some(conn) {
            return;
        }
        log::debug!("backend connection lost: {}", reason);
        if st
            .connected_at
            .take()
            .is_some_and(|at| at.elapsed() >= self.options.backoff_max)
        {
            st.attempt = 0;
        }
        st.link = Link::Idle;
        st.blocked = false;
        st.codec.reset();
        self.stats.open.store(0, Ordering::SeqCst);

        let mut written: Vec<(u64, u64)> = st.in_flight.drain().collect();
        written.sort_unstable();
        let mut retry = Vec::new();
        for (_, id) in written {
            let Some(pending) = st.calls.get_mut(&id) else {
                continue;
            };
            pending.wire_id = None;
            if pending.call.retries > 0 {
                pending.call.retries -= 1;
                retry.push(id);
            } else {
                self.finish(
                    st,
                    event_loop,
                    id,
                    Err(ChainError::Transport(reason.to_string())),
                );
            }
        }
        for id in retry.into_iter().rev() {
            st.queue.push_front(id);
        }
        self.schedule_reconnect(st, event_loop);
    }

    fn connect(&self, st: &mut State, event_loop: &Arc<EventLoop>) {
        let config = TcpClientConfig {
            keep_alive: Some(Duration::from_secs(30)),
            ..TcpClientConfig::default()
        };
        let handler = Events {
            engine: self.me.clone(),
        };
        let started = TcpClient::connect(self.addr, config, handler)
            .and_then(|mut client| client.start(event_loop).map(|()| client))
            .map_err(|e| e.to_string());
        match started {
            Ok(client) => {
                st.link = Link::Connecting {
                    id: client.id(),
                    _client: client,
                };
            }
            Err(e) => {
                log::debug!("connecting to {} failed: {}", self.addr, e);
                self.schedule_reconnect(st, event_loop);
            }
        }
    }

    fn schedule_reconnect(&self, st: &mut State, event_loop: &EventLoop) {
        if st.closed {
            st.link = Link::Idle;
            return;
        }
        // One jitter factor per outage keeps the doubling visible while spreading out
        // clients that lost the same server at the same moment.
        if st.attempt == 0 {
            st.jitter = jitter_factor(&mut st.rng);
        }
        let delay = backoff_delay(
            self.options.backoff_initial,
            self.options.backoff_max,
            st.attempt,
            st.jitter,
        );
        st.attempt = st.attempt.saturating_add(1);
        let engine = self.me.clone();
        let timer = event_loop.schedule_once(delay, move || {
            if let Some(engine) = engine.upgrade() {
                engine.on_backoff_elapsed();
            }
        });
        st.link = Link::Backoff(timer);
    }
}

/// True once `deadline` has passed. A loop that runs late can handle a reply and its call's
/// overdue deadline in one turn, I/O first; the deadline came first, so it wins.
fn expired(deadline: Option<Instant>, now: Instant) -> bool {
    deadline.is_some_and(|deadline| now >= deadline)
}

fn round_up_to_ms(d: Duration) -> Duration {
    Duration::from_millis(u64::try_from(d.as_nanos().div_ceil(1_000_000)).unwrap_or(u64::MAX))
}

pub(crate) fn backoff_delay(
    initial: Duration,
    max: Duration,
    attempt: u32,
    jitter: f64,
) -> Duration {
    let factor = 1u32.checked_shl(attempt).unwrap_or(u32::MAX);
    let base = initial.saturating_mul(factor).min(max);
    Duration::try_from_secs_f64(base.as_secs_f64() * jitter)
        .unwrap_or(max)
        .min(max)
}

/// A factor in [0.8, 1.2) from a xorshift generator.
fn jitter_factor(rng: &mut u64) -> f64 {
    let mut x = *rng;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *rng = x;
    0.8 + 0.4 * ((x >> 11) as f64 / (1u64 << 53) as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: Duration = Duration::from_millis(1);

    #[test]
    fn backoff_doubles_up_to_the_cap() {
        let delays: Vec<Duration> = (0..8)
            .map(|attempt| backoff_delay(100 * MS, 5000 * MS, attempt, 1.0))
            .collect();
        assert_eq!(
            delays,
            [100, 200, 400, 800, 1600, 3200, 5000, 5000].map(|ms| ms * MS)
        );
        assert_eq!(backoff_delay(100 * MS, 5000 * MS, 40, 1.0), 5000 * MS);
    }

    #[test]
    fn backoff_jitter_scales_but_never_exceeds_the_cap() {
        assert_eq!(backoff_delay(100 * MS, 5000 * MS, 1, 0.8), 160 * MS);
        assert_eq!(backoff_delay(100 * MS, 5000 * MS, 1, 1.2), 240 * MS);
        assert_eq!(backoff_delay(100 * MS, 5000 * MS, 5, 1.2), 3840 * MS);
        assert_eq!(backoff_delay(100 * MS, 5000 * MS, 6, 1.2), 5000 * MS);
        assert_eq!(backoff_delay(100 * MS, 5000 * MS, 6, 0.8), 4000 * MS);
        assert_eq!(
            backoff_delay(Duration::MAX, Duration::MAX, 3, 1.2),
            Duration::MAX
        );
    }

    #[test]
    fn deadlines_round_up_to_whole_ticks() {
        assert_eq!(round_up_to_ms(Duration::ZERO), Duration::ZERO);
        assert_eq!(round_up_to_ms(Duration::from_micros(1)), MS);
        assert_eq!(round_up_to_ms(Duration::from_micros(199_950)), 200 * MS);
        assert_eq!(round_up_to_ms(200 * MS), 200 * MS);
        assert_eq!(round_up_to_ms(Duration::from_nanos(200_000_001)), 201 * MS);
    }

    #[test]
    fn a_reply_after_the_deadline_is_a_timeout() {
        let now = Instant::now();
        assert!(!expired(None, now));
        assert!(!expired(Some(now + MS), now));
        assert!(expired(Some(now), now));
        assert!(expired(Some(now), now + MS));
    }

    #[test]
    fn jitter_stays_within_twenty_percent_and_varies() {
        let mut rng = 0x9e37_79b9_7f4a_7c15;
        let factors: Vec<f64> = (0..10_000).map(|_| jitter_factor(&mut rng)).collect();
        assert!(factors.iter().all(|f| (0.8..1.2).contains(f)));
        assert!(factors.iter().any(|&f| f < 0.85));
        assert!(factors.iter().any(|&f| f > 1.15));
    }

    #[test]
    fn default_options_match_the_documented_values() {
        let options = EngineOptions::default();
        assert_eq!(options.default_deadline, Duration::from_secs(30));
        assert_eq!(options.default_retries, 0);
        assert_eq!(options.backoff_initial, 100 * MS);
        assert_eq!(options.backoff_max, Duration::from_secs(5));
        assert_eq!(options.reconnect_queue_limit, 1024);
    }
}
