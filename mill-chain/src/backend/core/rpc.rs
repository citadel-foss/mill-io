//! The Bitcoin Core runtime: a pool of HTTP/1.1 keep-alive connections, Basic or cookie
//! auth, `/` and `/wallet/<name>` routing, and a long-call lane.
//!
//! Threading follows the request engine: `State` is only locked on the event loop thread,
//! `on_disconnect` and `on_error` only post to the loop, and a caller's reply is sent only
//! after its call left `State` and the `pending` gauge was updated.
//!
//! HTTP/1.1 has no request ids, so a connection carries one request at a time and a
//! response belongs to the request in flight on it. A call that ends while its request is
//! in flight (deadline, cancel) closes that connection, or the late response would be
//! read as the answer to the next request.

use super::{CoreAuth, CoreConfig, CoreShared};
use crate::engine::{backoff_delay, expired, jitter_factor, round_up_to_ms};
use crate::engine::{Call, ClientStats, EngineOptions, EventSink, Route, Runtime, Stats};
use crate::layer::{Dialer, Hop};
use crate::proto::http::{self, Response, ResponseParser};
use crate::proto::jsonrpc;
use crate::ChainError;
use mill_io::{EventLoop, TaskPriority, TimerId};
use mill_net::errors::{NetworkError, Result as NetResult};
use mill_net::tcp::{ConnSender, ConnectionId, NetworkHandler, ServerContext};
use mill_net::tcp::{TcpClient, TcpClientConfig};
use serde_json::Value;
use std::collections::hash_map::RandomState;
use std::collections::{HashMap, VecDeque};
use std::hash::{BuildHasher, Hasher};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError, RwLock, Weak};
use std::thread::{self, ThreadId};
use std::time::{Duration, Instant};

/// Methods that can run for hours. They take the long-call lane: a connection of their
/// own, no `request_timeout`, and never a resend, which would start the job again.
const LONG_METHODS: [&str; 2] = ["importdescriptors", "rescanblockchain"];
/// An idle connection older than this is closed instead of reused, so a request does not
/// race the node closing it at `-rpcservertimeout` (30 s by default).
const IDLE_REUSE: Duration = Duration::from_secs(15);
/// Waits before resending a call the node refused with 503 because its work queue was
/// full. The request never ran, so resending is safe.
const BUSY_INITIAL: Duration = Duration::from_millis(5);
const BUSY_MAX: Duration = Duration::from_millis(500);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lane {
    Normal,
    Long,
}

impl Lane {
    const ALL: [Lane; 2] = [Lane::Normal, Lane::Long];

    fn of(call: &Call) -> Lane {
        let is_long = |method: &str| LONG_METHODS.contains(&method);
        let long = match call.route {
            Route::Chain | Route::Wallet => is_long(&call.method),
            Route::ChainBatch => call.params.as_array().is_some_and(|requests| {
                requests
                    .iter()
                    .any(|r| r.get("method").and_then(Value::as_str).is_some_and(is_long))
            }),
        };
        if long {
            Lane::Long
        } else {
            Lane::Normal
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

enum Auth {
    Basic(String),
    Cookie {
        path: PathBuf,
        /// The `Authorization` value from the cookie last read.
        header: Option<String>,
        /// Bumped whenever the cookie read differs from the one before.
        generation: u64,
    },
}

impl Auth {
    fn new(auth: CoreAuth) -> Self {
        match auth {
            CoreAuth::UserPass { user, pass } => {
                Auth::Basic(http::basic_auth(&format!("{}:{}", user, pass)))
            }
            CoreAuth::Cookie(path) => Auth::Cookie {
                path,
                header: None,
                generation: 0,
            },
        }
    }

    fn generation(&self) -> u64 {
        match self {
            Auth::Basic(_) => 0,
            Auth::Cookie { generation, .. } => *generation,
        }
    }

    /// The `Authorization` value, reading the cookie if none was read yet.
    fn header(&mut self) -> Result<String, ChainError> {
        if let Auth::Cookie { header: None, .. } = self {
            self.reload()?;
        }
        match self {
            Auth::Basic(header) => Ok(header.clone()),
            Auth::Cookie { header, .. } => header.clone().ok_or(ChainError::Auth),
        }
    }

    /// Reads the cookie file again; true if it changed. The file is a few dozen bytes on
    /// local disk, read rarely enough to do on the loop thread.
    fn reload(&mut self) -> Result<bool, ChainError> {
        let Auth::Cookie {
            path,
            header,
            generation,
        } = self
        else {
            return Ok(false);
        };
        let cookie = std::fs::read_to_string(&*path).map_err(|e| {
            log::warn!("cannot read the RPC cookie {}: {}", path.display(), e);
            ChainError::Auth
        })?;
        let fresh = http::basic_auth(cookie.trim_end());
        if header.as_deref() == Some(fresh.as_str()) {
            return Ok(false);
        }
        *header = Some(fresh);
        *generation += 1;
        Ok(true)
    }
}

struct Pending {
    call: Call,
    lane: Lane,
    timer: Option<TimerId>,
    /// Set while the call waits out a 503.
    wake: Option<TimerId>,
    /// Set while its request is in flight on this connection.
    conn: Option<ConnectionId>,
    /// The JSON-RPC id of the request in flight; a batch numbers on from it.
    wire_id: u64,
    /// The auth generation the request in flight was sent with.
    auth_generation: u64,
    /// Already resent once after a 401 with a fresh cookie.
    reauthed: bool,
    busy: u32,
}

struct Conn {
    lane: Lane,
    client: TcpClient<Events>,
    /// Set once the TCP handshake completed.
    sender: Option<ConnSender>,
    parser: ResponseParser,
    call: Option<u64>,
    idle_since: Instant,
    /// Has answered at least once.
    served: bool,
}

struct State {
    closed: bool,
    auth: Auth,
    calls: HashMap<u64, Pending>,
    /// Calls waiting for a connection, per lane, oldest first.
    queues: [VecDeque<u64>; 2],
    conns: HashMap<ConnectionId, Conn>,
    next_wire_id: u64,
    /// While set, no new connection is opened.
    backoff: Option<TimerId>,
    /// Failed connects since the node last answered.
    attempt: u32,
    jitter: f64,
    rng: u64,
    /// A connection failed or dropped a request; the next one up counts as a reconnect.
    reconnect_pending: bool,
    events: EventSink,
    body: Vec<u8>,
    request: Vec<u8>,
}

pub(crate) struct CoreRpc {
    me: Weak<CoreRpc>,
    /// The first hop: the proxy if one is set, else the node.
    addr: SocketAddr,
    dialer: Dialer,
    host: String,
    wallet_path: Option<String>,
    options: EngineOptions,
    rpc_connections: usize,
    request_timeout: Duration,
    max_body_bytes: usize,
    /// Taken out at shutdown, after the loop thread is joined.
    event_loop: RwLock<Option<Arc<EventLoop>>>,
    loop_thread: OnceLock<ThreadId>,
    closed: AtomicBool,
    next_call_id: AtomicU64,
    state: Mutex<State>,
    stats: Stats,
    shared: CoreShared,
}

pub(crate) struct Events {
    rpc: Weak<CoreRpc>,
}

impl NetworkHandler for Events {
    fn on_connect(&self, _ctx: &ServerContext, conn: ConnectionId) -> NetResult<()> {
        if let Some(rpc) = self.rpc.upgrade() {
            rpc.on_connected(conn);
        }
        Ok(())
    }

    fn on_data(&self, _ctx: &ServerContext, conn: ConnectionId, data: &[u8]) -> NetResult<()> {
        if let Some(rpc) = self.rpc.upgrade() {
            rpc.on_data(conn, data);
        }
        Ok(())
    }

    fn on_disconnect(&self, _ctx: &ServerContext, conn: ConnectionId) -> NetResult<()> {
        if let Some(rpc) = self.rpc.upgrade() {
            rpc.post_lost(conn, "connection closed".to_string());
        }
        Ok(())
    }

    fn on_error(&self, _ctx: &ServerContext, conn: Option<ConnectionId>, error: NetworkError) {
        match (conn, &error) {
            (Some(conn), NetworkError::Connect(_) | NetworkError::Io(_)) => {
                if let Some(rpc) = self.rpc.upgrade() {
                    rpc.post_lost(conn, error.to_string());
                }
            }
            _ => log::debug!("Core RPC connection: {}", error),
        }
    }
}

/// Splits `http://host:port` into the `Host` header value, the host and the port.
fn parse_url(url: &str) -> Result<(String, String, u16), ChainError> {
    let scheme_ok = url
        .get(..7)
        .is_some_and(|s| s.eq_ignore_ascii_case("http://"));
    if !scheme_ok {
        return Err(ChainError::Unsupported("rpc_url must start with http://"));
    }
    let authority = url[7..].strip_suffix('/').unwrap_or(&url[7..]);
    if authority.is_empty() || authority.contains(['/', '@', '?', '#']) {
        return Err(ChainError::Unsupported(
            "rpc_url must be http://host:port, without a path or credentials",
        ));
    }
    let has_port = authority.rsplit_once(':').is_some_and(|(host, port)| {
        !port.is_empty() && (!host.contains(':') || host.ends_with(']'))
    });
    if !has_port {
        return Err(ChainError::Unsupported("rpc_url must name a port"));
    }
    let (host, port) = authority
        .rsplit_once(':')
        .ok_or(ChainError::Unsupported("rpc_url must name a port"))?;
    let port = port
        .parse()
        .map_err(|_| ChainError::Transport(format!("invalid port in {}", authority)))?;
    Ok((authority.to_string(), host.to_string(), port))
}

/// Percent-encodes a wallet name for use as a path segment.
fn wallet_path(name: &str) -> String {
    let mut path = String::from("/wallet/");
    for byte in name.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            path.push(char::from(byte));
        } else {
            path.push_str(&format!("%{:02X}", byte));
        }
    }
    path
}

fn earliest(a: Option<Instant>, b: Option<Instant>) -> Option<Instant> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

impl CoreRpc {
    pub(crate) fn new(
        config: CoreConfig,
        options: EngineOptions,
        event_loop: Arc<EventLoop>,
        events: EventSink,
    ) -> Result<Arc<Self>, ChainError> {
        let (authority, host, port) = parse_url(&config.rpc_url)?;
        let dialer = Dialer::new(&host, port, &config.transport)?;
        // Resolved here, on the caller's thread: through a proxy only the proxy's name is.
        let addr = match dialer.hop() {
            Hop::Connect(addr) => addr,
            Hop::Resolve(name, port) => (dialer.resolver())(&name, port)
                .map_err(|e| ChainError::Transport(format!("cannot resolve {}: {}", name, e)))?
                .into_iter()
                .next()
                .ok_or_else(|| ChainError::Transport(format!("{} has no address", name)))?,
        };
        let seed = RandomState::new().build_hasher().finish() | 1;
        Ok(Arc::new_cyclic(|me| CoreRpc {
            me: me.clone(),
            addr,
            dialer,
            host: authority,
            wallet_path: config.wallet.as_deref().map(wallet_path),
            options,
            rpc_connections: config.rpc_connections.max(1),
            request_timeout: config.request_timeout,
            max_body_bytes: config.max_body_bytes,
            event_loop: RwLock::new(Some(event_loop)),
            loop_thread: OnceLock::new(),
            closed: AtomicBool::new(false),
            next_call_id: AtomicU64::new(1),
            shared: CoreShared::new(host.clone(), config.max_body_bytes),
            state: Mutex::new(State {
                closed: false,
                auth: Auth::new(config.auth),
                calls: HashMap::new(),
                queues: [VecDeque::new(), VecDeque::new()],
                conns: HashMap::new(),
                next_wire_id: 1,
                backoff: None,
                attempt: 0,
                jitter: 1.0,
                rng: seed,
                reconnect_pending: false,
                events,
                body: Vec::new(),
                request: Vec::new(),
            }),
            stats: Stats::default(),
        }))
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

    /// Runs `f` with the state on the loop thread, if the loop still runs.
    fn on_loop(&self, f: impl FnOnce(&Self, &mut State, &Arc<EventLoop>) + Send + 'static) {
        let Some(rpc) = self.me.upgrade() else {
            return;
        };
        let _ = Runtime::post(
            self,
            Box::new(move || {
                if let Some(event_loop) = rpc.event_loop() {
                    let mut st = rpc.lock();
                    f(&rpc, &mut st, &event_loop);
                }
            }),
        );
    }

    fn on_submit(&self, id: u64, mut call: Call) {
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
        if call.route == Route::Wallet && self.wallet_path.is_none() {
            let _ = call.reply.send(Err(ChainError::Unsupported(
                "wallet calls need CoreConfig::wallet",
            )));
            return;
        }
        let lane = Lane::of(&call);
        if st.queues[lane.index()].len() >= self.options.reconnect_queue_limit {
            let _ = call
                .reply
                .send(Err(ChainError::Transport("request queue full".into())));
            return;
        }
        if lane == Lane::Normal {
            let timeout = Instant::now().checked_add(self.request_timeout);
            call.deadline = earliest(call.deadline, timeout);
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
                lane,
                timer,
                wake: None,
                conn: None,
                wire_id: 0,
                auth_generation: 0,
                reauthed: false,
                busy: 0,
            },
        );
        self.stats.pending.store(st.calls.len(), Ordering::SeqCst);
        st.queues[lane.index()].push_back(id);
        self.dispatch(&mut st, &event_loop);
    }

    /// Schedules the deadline check for call `id`; `None` if `deadline` has passed.
    fn arm_deadline(&self, event_loop: &EventLoop, id: u64, deadline: Instant) -> Option<TimerId> {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return None;
        }
        let rpc = self.me.clone();
        // The wheel truncates delays to whole ticks; rounding up keeps it from firing early.
        Some(event_loop.schedule_once(round_up_to_ms(left), move || {
            if let Some(rpc) = rpc.upgrade() {
                rpc.on_deadline(id);
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
            None => {
                self.finish(&mut st, &event_loop, id, Err(ChainError::Timeout));
                self.drop_unwanted_handshakes(&mut st);
                self.dispatch(&mut st, &event_loop);
            }
        }
    }

    /// A layer handshake gets as long as the calls waiting on it: once a lane has no call
    /// left, its connections still handshaking are closed.
    fn drop_unwanted_handshakes(&self, st: &mut State) {
        if self.dialer.handshake_timeout().is_none() {
            return;
        }
        for lane in Lane::ALL {
            if !st.queues[lane.index()].is_empty() {
                continue;
            }
            let stalled: Vec<ConnectionId> = st
                .conns
                .iter()
                .filter(|(_, c)| c.lane == lane && c.sender.is_none())
                .map(|(&id, _)| id)
                .collect();
            for id in stalled {
                self.drop_conn(st, id);
            }
        }
    }

    /// Writes queued calls to idle connections, and opens connections for the rest.
    fn dispatch(&self, st: &mut State, event_loop: &Arc<EventLoop>) {
        if st.closed {
            return;
        }
        for lane in Lane::ALL {
            while let Some(&id) = st.queues[lane.index()].front() {
                let Some(conn) = self.idle_conn(st, lane) else {
                    self.grow(st, event_loop, lane);
                    break;
                };
                st.queues[lane.index()].pop_front();
                self.write(st, event_loop, id, conn);
            }
        }
    }

    /// An idle connection of `lane` to reuse. Closes the ones idle too long.
    fn idle_conn(&self, st: &mut State, lane: Lane) -> Option<ConnectionId> {
        let mut fresh = None;
        let mut stale = Vec::new();
        for (&id, conn) in &st.conns {
            if conn.lane != lane || conn.sender.is_none() || conn.call.is_some() {
                continue;
            }
            if conn.idle_since.elapsed() >= IDLE_REUSE {
                stale.push(id);
            } else if fresh.is_none() {
                fresh = Some(id);
            }
        }
        for id in stale {
            self.drop_conn(st, id);
        }
        fresh
    }

    /// Opens connections for `lane` up to its limit, one per waiting call.
    fn grow(&self, st: &mut State, event_loop: &Arc<EventLoop>, lane: Lane) {
        let limit = match lane {
            Lane::Normal => self.rpc_connections,
            Lane::Long => 1,
        };
        let waiting = st.queues[lane.index()].len();
        let in_lane = st.conns.values().filter(|c| c.lane == lane);
        let (mut open, mut connecting) = in_lane.fold((0, 0), |(open, connecting), c| {
            (open + 1, connecting + usize::from(c.sender.is_none()))
        });
        while st.backoff.is_none() && open < limit && connecting < waiting {
            if !self.connect(st, event_loop, lane) {
                break;
            }
            open += 1;
            connecting += 1;
        }
    }

    fn connect(&self, st: &mut State, event_loop: &Arc<EventLoop>, lane: Lane) -> bool {
        let config = TcpClientConfig {
            keep_alive: Some(Duration::from_secs(30)),
            ..TcpClientConfig::default()
        };
        let handler = Events {
            rpc: self.me.clone(),
        };
        let started = TcpClient::connect(self.addr, config, handler)
            .and_then(|client| self.dialer.add_layers(client))
            .and_then(|mut client| client.start(event_loop).map(|()| client));
        match started {
            Ok(client) => {
                let conn = Conn {
                    lane,
                    client,
                    sender: None,
                    parser: ResponseParser::new(self.max_body_bytes),
                    call: None,
                    idle_since: Instant::now(),
                    served: false,
                };
                st.conns.insert(conn.client.id(), conn);
                true
            }
            Err(e) => {
                self.connect_failed(st, event_loop, lane, &e.to_string());
                false
            }
        }
    }

    /// A connect for `lane` failed. Backs off, and fails the lane's waiting calls if no
    /// connection is left to serve them, retrying those that have retries left.
    fn connect_failed(&self, st: &mut State, event_loop: &Arc<EventLoop>, lane: Lane, why: &str) {
        log::debug!("connecting to {} failed: {}", self.addr, why);
        st.reconnect_pending = true;
        self.schedule_backoff(st, event_loop);
        if st.conns.values().any(|c| c.lane == lane) {
            return;
        }
        let waiting: Vec<u64> = st.queues[lane.index()].iter().copied().collect();
        for id in waiting {
            let Some(pending) = st.calls.get_mut(&id) else {
                continue;
            };
            if pending.lane == Lane::Normal && pending.call.retries > 0 {
                pending.call.retries -= 1;
            } else {
                let error = ChainError::Transport(format!("cannot connect: {}", why));
                self.finish(st, event_loop, id, Err(error));
            }
        }
    }

    fn schedule_backoff(&self, st: &mut State, event_loop: &EventLoop) {
        if st.backoff.is_some() || st.closed {
            return;
        }
        // One jitter factor per outage keeps the doubling visible while spreading out
        // clients that lost the same node at the same moment.
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
        let rpc = self.me.clone();
        st.backoff = Some(event_loop.schedule_once(delay, move || {
            if let Some(rpc) = rpc.upgrade() {
                rpc.on_loop(|rpc, st, event_loop| {
                    st.backoff = None;
                    rpc.dispatch(st, event_loop);
                });
            }
        }));
    }

    /// Sends call `id` on idle connection `conn`.
    fn write(&self, st: &mut State, event_loop: &Arc<EventLoop>, id: u64, conn: ConnectionId) {
        let State {
            calls,
            auth,
            conns,
            next_wire_id,
            body,
            request,
            ..
        } = st;
        let Some(pending) = calls.get_mut(&id) else {
            return;
        };
        let Some((target, sender)) = conns
            .get_mut(&conn)
            .and_then(|target| target.sender.clone().map(|sender| (target, sender)))
        else {
            st.queues[pending.lane.index()].push_front(id);
            return;
        };
        if pending.call.is_cancelled() {
            self.finish(st, event_loop, id, Err(ChainError::Cancelled));
            return;
        }
        // A long call is never resent after a 401, so it starts from the current cookie.
        let refreshed = match pending.lane {
            Lane::Long => auth.reload().map(drop),
            Lane::Normal => Ok(()),
        };
        let header = match refreshed.and_then(|()| auth.header()) {
            Ok(header) => header,
            Err(e) => {
                self.finish(st, event_loop, id, Err(e));
                return;
            }
        };
        let wire_id = *next_wire_id;
        body.clear();
        let encoded = match pending.call.route {
            Route::ChainBatch => {
                let requests = pending
                    .call
                    .params
                    .as_array()
                    .map_or(&[][..], Vec::as_slice);
                *next_wire_id += requests.len() as u64;
                jsonrpc::encode_batch(body, wire_id, requests)
            }
            Route::Chain | Route::Wallet => {
                *next_wire_id += 1;
                jsonrpc::encode_request(body, wire_id, &pending.call.method, &pending.call.params)
            }
        };
        if let Err(e) = encoded {
            self.finish(st, event_loop, id, Err(e));
            return;
        }
        let path = match (pending.call.route, &self.wallet_path) {
            (Route::Wallet, Some(path)) => path.as_str(),
            _ => "/",
        };
        request.clear();
        http::write_post(request, path, &self.host, Some(&header), body);
        match sender.send(request) {
            Ok(()) => {
                self.stats.sent.fetch_add(1, Ordering::SeqCst);
                pending.conn = Some(conn);
                pending.wire_id = wire_id;
                pending.auth_generation = auth.generation();
                target.call = Some(id);
            }
            Err(e) => {
                self.drop_conn(st, conn);
                self.transport_failed(st, event_loop, id, &format!("write failed: {}", e));
            }
        }
    }

    fn on_connected(&self, conn: ConnectionId) {
        let Some(event_loop) = self.event_loop() else {
            return;
        };
        let mut st = self.lock();
        let Some(target) = st.conns.get_mut(&conn) else {
            return;
        };
        target.sender = Some(target.client.sender());
        target.idle_since = Instant::now();
        if std::mem::take(&mut st.reconnect_pending) {
            self.stats.reconnects.fetch_add(1, Ordering::SeqCst);
        }
        self.update_open(&st);
        self.dispatch(&mut st, &event_loop);
    }

    fn on_data(&self, conn: ConnectionId, data: &[u8]) {
        let Some(event_loop) = self.event_loop() else {
            return;
        };
        let mut st = self.lock();
        let Some(target) = st.conns.get_mut(&conn) else {
            return;
        };
        let mut responses = Vec::new();
        let parsed = target.parser.push(data, &mut responses);
        let call = target.call;
        let mut responses = responses.into_iter();
        let response = responses.next();
        let extra = responses.next().is_some();
        let unsolicited = call.is_none() && !data.is_empty();
        let close =
            parsed.is_err() || extra || unsolicited || response.as_ref().is_some_and(|r| r.close);
        if response.is_some() {
            target.call = None;
            target.idle_since = Instant::now();
            target.served = true;
        }
        if close {
            self.drop_conn(&mut st, conn);
        }
        match (call, response, parsed) {
            (Some(id), Some(response), _) => self.on_response(&mut st, &event_loop, id, response),
            (Some(id), None, Err(e)) => {
                let error = ChainError::Protocol(e.to_string());
                self.finish(&mut st, &event_loop, id, Err(error));
            }
            _ => {}
        }
        self.dispatch(&mut st, &event_loop);
    }

    fn on_response(&self, st: &mut State, event_loop: &Arc<EventLoop>, id: u64, resp: Response) {
        st.attempt = 0;
        let Some(pending) = st.calls.get_mut(&id) else {
            return;
        };
        pending.conn = None;
        if expired(pending.call.deadline, Instant::now()) {
            self.finish(st, event_loop, id, Err(ChainError::Timeout));
            return;
        }
        match resp.status {
            401 => self.unauthorized(st, event_loop, id),
            403 => self.finish(st, event_loop, id, Err(ChainError::Auth)),
            503 if pending.lane == Lane::Normal => self.busy(st, event_loop, id),
            status => {
                let result = match pending.call.route {
                    Route::ChainBatch => {
                        let n = pending.call.params.as_array().map_or(0, Vec::len);
                        jsonrpc::decode_batch(&resp.body, pending.wire_id, n)
                    }
                    Route::Chain | Route::Wallet => {
                        jsonrpc::decode_reply(&resp.body, pending.wire_id)
                    }
                };
                let result = match result {
                    Err(ChainError::Protocol(_)) if status != 200 => Err(status_error(&resp)),
                    result => result,
                };
                self.finish(st, event_loop, id, result);
            }
        }
    }

    /// A 401: resend once if the cookie changed since the request went out, else `Auth`.
    fn unauthorized(&self, st: &mut State, event_loop: &Arc<EventLoop>, id: u64) {
        let State { calls, auth, .. } = &mut *st;
        let Some(pending) = calls.get_mut(&id) else {
            return;
        };
        let resend = matches!(auth, Auth::Cookie { .. })
            && pending.lane == Lane::Normal
            && !pending.reauthed
            && (pending.auth_generation != auth.generation() || auth.reload().unwrap_or(false));
        if resend {
            pending.reauthed = true;
            st.queues[Lane::Normal.index()].push_front(id);
        } else {
            self.finish(st, event_loop, id, Err(ChainError::Auth));
        }
    }

    /// A 503 on the normal lane: the node's work queue was full and the request never ran.
    /// Sends it again after a short, growing wait.
    fn busy(&self, st: &mut State, event_loop: &EventLoop, id: u64) {
        let jitter = jitter_factor(&mut st.rng);
        let Some(pending) = st.calls.get_mut(&id) else {
            return;
        };
        let delay = backoff_delay(BUSY_INITIAL, BUSY_MAX, pending.busy, jitter);
        pending.busy = pending.busy.saturating_add(1);
        let rpc = self.me.clone();
        pending.wake = Some(event_loop.schedule_once(delay, move || {
            if let Some(rpc) = rpc.upgrade() {
                rpc.on_loop(move |rpc, st, event_loop| {
                    let Some(pending) = st.calls.get_mut(&id) else {
                        return;
                    };
                    pending.wake = None;
                    st.queues[pending.lane.index()].push_front(id);
                    rpc.dispatch(st, event_loop);
                });
            }
        }));
    }

    /// The request of call `id` was lost with its connection: resend it if it is on the
    /// normal lane and has retries left, else fail it with `Transport`.
    fn transport_failed(&self, st: &mut State, event_loop: &Arc<EventLoop>, id: u64, why: &str) {
        let Some(pending) = st.calls.get_mut(&id) else {
            return;
        };
        pending.conn = None;
        if pending.lane == Lane::Normal && pending.call.retries > 0 {
            pending.call.retries -= 1;
            st.queues[Lane::Normal.index()].push_front(id);
        } else {
            self.finish(
                st,
                event_loop,
                id,
                Err(ChainError::Transport(why.to_string())),
            );
        }
    }

    fn post_lost(&self, conn: ConnectionId, why: String) {
        self.on_loop(move |rpc, st, event_loop| rpc.lost(st, event_loop, conn, &why));
    }

    fn lost(&self, st: &mut State, event_loop: &Arc<EventLoop>, conn: ConnectionId, why: &str) {
        let Some(mut lost) = st.conns.remove(&conn) else {
            return;
        };
        self.update_open(st);
        if lost.sender.is_none() {
            self.connect_failed(st, event_loop, lost.lane, why);
        } else if let Some(id) = lost.call {
            match lost.parser.finish() {
                Ok(Some(response)) => self.on_response(st, event_loop, id, response),
                _ => {
                    st.reconnect_pending = true;
                    self.transport_failed(st, event_loop, id, why);
                }
            }
        }
        // A node that accepts and closes without answering is backed off from like one
        // that refuses connections, or the pool would reconnect in a tight loop.
        if lost.sender.is_some() && !lost.served {
            st.reconnect_pending = true;
            self.schedule_backoff(st, event_loop);
        }
        self.dispatch(st, event_loop);
    }

    /// Removes call `id` and sends its result. A request still in flight takes its
    /// connection with it.
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
        for timer in [pending.timer, pending.wake].into_iter().flatten() {
            event_loop.cancel_timer(timer);
        }
        match pending.conn {
            Some(conn) => {
                self.drop_conn(st, conn);
            }
            None => st.queues[pending.lane.index()].retain(|&queued| queued != id),
        }
        self.stats.pending.store(st.calls.len(), Ordering::SeqCst);
        let _ = pending.call.reply.send(result);
    }

    /// Closes connection `conn`; returns the call whose request was in flight on it.
    fn drop_conn(&self, st: &mut State, conn: ConnectionId) -> Option<u64> {
        let dropped = st.conns.remove(&conn)?;
        self.update_open(st);
        dropped.call
    }

    fn update_open(&self, st: &State) {
        let open = st.conns.values().filter(|c| c.sender.is_some()).count();
        self.stats.open.store(open, Ordering::SeqCst);
    }
}

/// The error for a non-200 response whose body is not a JSON-RPC reply.
fn status_error(resp: &Response) -> ChainError {
    let text = String::from_utf8_lossy(&resp.body[..resp.body.len().min(200)]);
    let message = format!("HTTP {}: {}", resp.status, text.trim());
    if resp.status == 503 {
        ChainError::Transport(message)
    } else {
        ChainError::Protocol(message)
    }
}

impl Runtime for CoreRpc {
    fn options(&self) -> &EngineOptions {
        &self.options
    }

    fn post(&self, f: Box<dyn FnOnce() + Send>) -> Result<(), ChainError> {
        let event_loop = self.event_loop().ok_or(ChainError::Shutdown)?;
        // A failed wake-up still leaves `f` queued; it runs by the next poll timeout.
        let _ = event_loop.post(f);
        Ok(())
    }

    fn set_loop_thread(&self) {
        let _ = self.loop_thread.set(thread::current().id());
    }

    fn is_loop_thread(&self) -> bool {
        self.loop_thread.get() == Some(&thread::current().id())
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    fn stats(&self) -> ClientStats {
        self.stats.snapshot()
    }

    fn submit(&self, call: Call) -> Result<u64, ChainError> {
        let id = self.next_call_id.fetch_add(1, Ordering::Relaxed);
        let rpc = self.me.upgrade().ok_or(ChainError::Shutdown)?;
        Runtime::post(self, Box::new(move || rpc.on_submit(id, call)))?;
        Ok(id)
    }

    fn abandon(&self, id: u64, error: ChainError) {
        self.on_loop(move |rpc, st, event_loop| {
            rpc.finish(st, event_loop, id, Err(error));
            rpc.dispatch(st, event_loop);
        });
    }

    fn start(&self) {}

    fn stop(&self) {
        self.closed.store(true, Ordering::SeqCst);
        if let Some(event_loop) = self.event_loop() {
            event_loop.stop();
        }
    }

    fn close_all(&self) {
        self.closed.store(true, Ordering::SeqCst);
        let event_loop = self.event_loop();
        let mut st = self.lock();
        st.closed = true;
        if let (Some(timer), Some(event_loop)) = (st.backoff.take(), &event_loop) {
            event_loop.cancel_timer(timer);
        }
        st.conns.clear();
        self.stats.open.store(0, Ordering::SeqCst);
        for queue in &mut st.queues {
            queue.clear();
        }
        let calls: Vec<Pending> = st.calls.drain().map(|(_, pending)| pending).collect();
        self.stats.pending.store(0, Ordering::SeqCst);
        st.events.close();
        drop(st);
        for pending in calls {
            if let Some(event_loop) = &event_loop {
                for timer in [pending.timer, pending.wake].into_iter().flatten() {
                    event_loop.cancel_timer(timer);
                }
            }
            let _ = pending.call.reply.send(Err(ChainError::Shutdown));
        }
    }

    fn release_loop(&self) -> Option<Arc<EventLoop>> {
        self.event_loop
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    fn spawn_compute(&self, task: Box<dyn FnOnce() + Send>) -> Result<(), ChainError> {
        let event_loop = self.event_loop().ok_or(ChainError::Shutdown)?;
        event_loop.spawn_compute_with_priority(task, TaskPriority::High);
        Ok(())
    }

    fn core(&self) -> Option<&CoreShared> {
        Some(&self.shared)
    }

    #[cfg(feature = "testing")]
    fn inject_events(&self, n: usize) {
        self.lock().events.inject(n, &self.stats.resyncs);
    }

    #[cfg(feature = "testing")]
    fn dns_lookups(&self) -> u64 {
        self.dialer.dns_lookups()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls_need_http_a_host_and_a_port() {
        let (authority, host, port) = parse_url("http://127.0.0.1:18443").unwrap();
        assert_eq!(authority, "127.0.0.1:18443");
        assert_eq!((host.as_str(), port), ("127.0.0.1", 18443));
        assert_eq!(parse_url("HTTP://127.0.0.1:1/").unwrap().0, "127.0.0.1:1");
        let (authority, host, port) = parse_url("http://[::1]:8332").unwrap();
        assert_eq!(authority, "[::1]:8332");
        assert_eq!((host.as_str(), port), ("[::1]", 8332));
        assert_eq!(
            parse_url("http://localhost:8332").unwrap().0,
            "localhost:8332"
        );
        for bad in [
            "127.0.0.1:8332",
            "https://127.0.0.1:8332",
            "http://",
            "http://127.0.0.1",
            "http://127.0.0.1:",
            "http://[::1]",
            "http://127.0.0.1:8332/wallet/w",
            "http://user:pass@127.0.0.1:8332",
            "http://127.0.0.1:8332?x",
        ] {
            assert!(
                matches!(parse_url(bad), Err(ChainError::Unsupported(_))),
                "{}",
                bad
            );
        }
        assert!(matches!(
            parse_url("http://127.0.0.1:99999"),
            Err(ChainError::Transport(_))
        ));
    }

    #[test]
    fn wallet_names_are_percent_encoded() {
        assert_eq!(wallet_path("w-1.a_b~"), "/wallet/w-1.a_b~");
        assert_eq!(wallet_path("my wallet/é"), "/wallet/my%20wallet%2F%C3%A9");
        assert_eq!(wallet_path(""), "/wallet/");
    }

    #[test]
    fn long_methods_take_the_long_lane() {
        let call = |method: &str, params: Value, route| {
            let (reply, _) = crossbeam_channel::bounded(1);
            Call {
                method: method.into(),
                params,
                route,
                deadline: None,
                retries: 0,
                cancel: None,
                reply,
            }
        };
        let lane = |c: Call| Lane::of(&c);
        assert_eq!(
            lane(call("importdescriptors", Value::Null, Route::Wallet)),
            Lane::Long
        );
        assert_eq!(
            lane(call("rescanblockchain", Value::Null, Route::Chain)),
            Lane::Long
        );
        assert_eq!(
            lane(call("getblockcount", Value::Null, Route::Chain)),
            Lane::Normal
        );
        let batch =
            serde_json::json!([{"method": "getblockcount"}, {"method": "rescanblockchain"}]);
        assert_eq!(lane(call("", batch, Route::ChainBatch)), Lane::Long);
        let batch = serde_json::json!([{"method": "getblockcount"}]);
        assert_eq!(lane(call("", batch, Route::ChainBatch)), Lane::Normal);
    }

    /// Calls `on_response` for a call written with `deadline`, as a late loop would after
    /// reading the reply in the same turn the deadline passed.
    fn answer_with_deadline(deadline: Instant, status: u16) -> Result<Value, ChainError> {
        let event_loop = Arc::new(EventLoop::new_low_latency(16, 10).unwrap());
        let (events, _receiver) = EventSink::channel(4);
        let config = CoreConfig::new(
            "http://127.0.0.1:1",
            CoreAuth::Cookie("/nonexistent".into()),
        );
        let rpc = CoreRpc::new(
            config,
            EngineOptions::default(),
            Arc::clone(&event_loop),
            events,
        )
        .unwrap();
        let (reply, replies) = crossbeam_channel::bounded(1);
        let call = Call {
            method: "getblockcount".into(),
            params: Value::Null,
            route: Route::Chain,
            deadline: Some(deadline),
            retries: 0,
            cancel: None,
            reply,
        };
        let mut st = rpc.lock();
        st.calls.insert(
            7,
            Pending {
                call,
                lane: Lane::Normal,
                timer: None,
                wake: None,
                conn: None,
                wire_id: 3,
                auth_generation: 0,
                reauthed: false,
                busy: 0,
            },
        );
        let body = br#"{"result":5,"error":null,"id":3}"#.to_vec();
        let response = Response {
            status,
            close: false,
            body,
        };
        rpc.on_response(&mut st, &event_loop, 7, response);
        assert!(st.calls.is_empty() && st.queues.iter().all(VecDeque::is_empty));
        drop(st);
        replies.try_recv().unwrap()
    }

    #[test]
    fn a_reply_after_the_deadline_is_a_timeout() {
        let later = Instant::now() + Duration::from_secs(60);
        assert_eq!(answer_with_deadline(later, 200), Ok(serde_json::json!(5)));
        assert_eq!(answer_with_deadline(later, 401), Err(ChainError::Auth));
        for status in [200, 401, 503] {
            assert_eq!(
                answer_with_deadline(Instant::now(), status),
                Err(ChainError::Timeout),
                "{}",
                status
            );
        }
    }

    #[test]
    fn earliest_deadline_wins() {
        let now = Instant::now();
        let later = now + Duration::from_secs(1);
        assert_eq!(earliest(Some(now), Some(later)), Some(now));
        assert_eq!(earliest(Some(later), Some(now)), Some(now));
        assert_eq!(earliest(None, Some(now)), Some(now));
        assert_eq!(earliest(Some(now), None), Some(now));
        assert_eq!(earliest(None, None), None);
    }

    #[test]
    fn cookies_are_read_lazily_and_reloaded_on_change() {
        let dir = std::env::temp_dir().join(format!("mill-chain-cookie-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(".cookie");
        let _ = std::fs::remove_file(&path);
        let mut auth = Auth::new(CoreAuth::Cookie(path.clone()));
        assert_eq!(auth.header(), Err(ChainError::Auth));
        std::fs::write(&path, "__cookie__:one\n").unwrap();
        assert_eq!(auth.header().unwrap(), http::basic_auth("__cookie__:one"));
        assert_eq!(auth.generation(), 1);
        assert_eq!(auth.reload(), Ok(false));
        std::fs::write(&path, "__cookie__:two").unwrap();
        assert_eq!(auth.reload(), Ok(true));
        assert_eq!(auth.generation(), 2);
        assert_eq!(auth.header().unwrap(), http::basic_auth("__cookie__:two"));
        std::fs::remove_file(&path).unwrap();
        assert_eq!(auth.reload(), Err(ChainError::Auth));
        assert_eq!(auth.header().unwrap(), http::basic_auth("__cookie__:two"));
        let _ = std::fs::remove_dir(&dir);

        let mut basic = Auth::new(CoreAuth::UserPass {
            user: "u".into(),
            pass: "p".into(),
        });
        assert_eq!(basic.header().unwrap(), http::basic_auth("u:p"));
        assert_eq!(basic.reload(), Ok(false));
        assert_eq!(basic.generation(), 0);
    }
}
