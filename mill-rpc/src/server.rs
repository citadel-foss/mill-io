//! RPC Server built on mill-net's TcpServer.
//!
//! The server accepts TCP connections, parses Mill-RPC frames, dispatches
//! requests to registered services, and sends back responses.

use crate::{Codec, RpcContext, RpcError, ServiceDispatch};
use mill_io::{EventLoop, TimerId};
use mill_net::errors::{NetworkError, Result};
use mill_net::tcp::config::TcpServerConfig;
use mill_net::tcp::traits::{ConnectionId, NetworkHandler};
use mill_net::tcp::{ConnSender, SendError, ServerContext, TcpServer};
use mill_rpc_core::protocol::{self, Frame, MessageType};
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard, RwLock, Weak};
use std::time::Duration;

/// Default for [`RpcServerBuilder::slow_reader_timeout`].
pub const DEFAULT_SLOW_READER_TIMEOUT: Duration = Duration::from_secs(60);

/// A registered service with its dispatch implementation.
struct RegisteredService {
    dispatcher: Box<dyn ServiceDispatch>,
}

/// RPC Server that hosts one or more services.
pub struct RpcServer {
    tcp_server: Arc<TcpServer<RpcServerHandler>>,
}

impl RpcServer {
    /// Create a builder for configuring the RPC server.
    pub fn builder() -> RpcServerBuilder {
        RpcServerBuilder::new()
    }

    /// The address the server is listening on, with the real port when it bound port 0.
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.tcp_server.local_addr()
    }
}

/// Builder for constructing an RpcServer.
pub struct RpcServerBuilder {
    address: Option<SocketAddr>,
    codec: Codec,
    max_connections: Option<usize>,
    services: Vec<(u16, Box<dyn ServiceDispatch>)>,
    next_service_id: u16,
    slow_reader_timeout: Duration,
}

impl RpcServerBuilder {
    fn new() -> Self {
        Self {
            address: None,
            codec: Codec::bincode(),
            max_connections: None,
            services: Vec::new(),
            next_service_id: 0,
            slow_reader_timeout: DEFAULT_SLOW_READER_TIMEOUT,
        }
    }

    /// How long a connection may leave its replies unread before it is closed.
    ///
    /// When a client stops reading and its reply queue fills, the server stops reading
    /// and dispatching that connection's requests until the queue drains; replies are
    /// kept, never dropped. Defaults to [`DEFAULT_SLOW_READER_TIMEOUT`].
    pub fn slow_reader_timeout(mut self, timeout: Duration) -> Self {
        self.slow_reader_timeout = timeout;
        self
    }

    /// Set the address to bind to.
    pub fn bind(mut self, addr: SocketAddr) -> Self {
        self.address = Some(addr);
        self
    }

    /// Set the codec for serialization.
    pub fn codec(mut self, codec: Codec) -> Self {
        self.codec = codec;
        self
    }

    /// Set the maximum number of connections.
    pub fn max_connections(mut self, max: usize) -> Self {
        self.max_connections = Some(max);
        self
    }

    /// Register a service implementation wrapped in its dispatcher.
    ///
    /// The service must implement `ServiceDispatch` - typically via the
    /// generated `{Name}Dispatcher` wrapper:
    ///
    /// ```ignore
    /// .service(CalculatorDispatcher(MyCalculator))
    /// ```
    pub fn service<S: ServiceDispatch>(mut self, svc: S) -> Self {
        let id = self.next_service_id;
        self.next_service_id += 1;
        self.services.push((id, Box::new(svc)));
        self
    }

    /// Register a service with an explicit service ID.
    pub fn service_with_id<S: ServiceDispatch>(mut self, id: u16, svc: S) -> Self {
        self.services.push((id, Box::new(svc)));
        self
    }

    /// Build and start the RPC server on the given event loop.
    pub fn build(self, event_loop: &Arc<EventLoop>) -> Result<RpcServer> {
        let addr = self
            .address
            .unwrap_or_else(|| "127.0.0.1:9000".parse().unwrap());

        let mut service_map = HashMap::new();
        for (id, dispatcher) in self.services {
            service_map.insert(id, RegisteredService { dispatcher });
        }

        let handler = RpcServerHandler(Arc::new(Inner {
            services: RwLock::new(service_map),
            codec: self.codec,
            slow_reader_timeout: self.slow_reader_timeout,
            event_loop: Arc::downgrade(event_loop),
            conns: RwLock::new(HashMap::new()),
        }));

        let mut config_builder = TcpServerConfig::builder().address(addr);
        if let Some(max) = self.max_connections {
            config_builder = config_builder.max_connections(max);
        }
        let config = config_builder.build();

        let tcp_server = Arc::new(TcpServer::new(config, handler)?);
        tcp_server
            .clone()
            .start(event_loop, event_loop.alloc_token())?;

        log::info!("Mill-RPC server listening on {}", tcp_server.local_addr()?);

        Ok(RpcServer { tcp_server })
    }
}

/// Internal handler that bridges mill-net's NetworkHandler to RPC dispatch.
struct RpcServerHandler(Arc<Inner>);

/// Shared so a connection's work can continue on a compute worker.
struct Inner {
    services: RwLock<HashMap<u16, RegisteredService>>,
    codec: Codec,
    slow_reader_timeout: Duration,
    event_loop: Weak<EventLoop>,
    conns: RwLock<HashMap<ConnectionId, Arc<Mutex<ConnState>>>>,
}

/// One connection's requests and replies.
///
/// mill-net runs one `on_data` at a time per connection, so `buf` only sees that
/// connection's bytes, in order. `on_drained` can run on another thread, so dispatching
/// and sending go through `drive`, which one thread at a time owns (`driving`); that keeps
/// replies in request order.
#[derive(Default)]
struct ConnState {
    /// Bytes not yet parsed into frames.
    buf: Vec<u8>,
    /// Requests not yet dispatched, oldest first.
    requests: VecDeque<Frame>,
    /// Encoded replies the connection has not accepted yet, oldest first.
    replies: VecDeque<Vec<u8>>,
    /// A reply was refused with `Backpressure`; reads and dispatch wait for `on_drained`.
    stalled: bool,
    /// Closes the connection if the stall outlasts `slow_reader_timeout`.
    stall_timer: Option<TimerId>,
    /// Tells a stall's timer whether the stall it was armed for is still going.
    stall_gen: u64,
    driving: bool,
    /// Work arrived while another thread was driving.
    again: bool,
    /// The loop runs handlers on workers, so requests must not be dispatched on the
    /// reactor thread, where mill-net's re-posted reads and drains also run.
    pool_mode: bool,
    /// Requests were handed to a compute worker; reads wait until it has dispatched them,
    /// as they would behind a dispatch done in place.
    backlog: bool,
    /// What mill-net was last told: reads are paused while `stalled` or `backlog`.
    reads_paused: bool,
}

impl ConnState {
    fn sync_reads(&mut self, sender: &ConnSender) {
        let paused = self.stalled || self.backlog;
        if paused != self.reads_paused {
            self.reads_paused = paused;
            if paused {
                sender.pause_reads();
            } else {
                sender.resume_reads();
            }
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

fn read_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|e| e.into_inner())
}

fn write_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(|e| e.into_inner())
}

impl NetworkHandler for RpcServerHandler {
    fn on_connect(&self, _ctx: &ServerContext, conn_id: ConnectionId) -> Result<()> {
        log::debug!("RPC connection established: {:?}", conn_id);
        // Accepted connections run on_connect on a worker in pool mode and on the reactor
        // thread in direct mode.
        let state = ConnState {
            pool_mode: !self.0.on_reactor_thread(),
            ..ConnState::default()
        };
        write_lock(&self.0.conns).insert(conn_id, Arc::new(Mutex::new(state)));
        Ok(())
    }

    fn on_data(&self, ctx: &ServerContext, conn_id: ConnectionId, data: &[u8]) -> Result<()> {
        let Some(state) = self.0.state(conn_id) else {
            return Ok(());
        };
        {
            let mut st = lock(&state);
            st.buf.extend_from_slice(data);
            // A bad frame leaves the stream unsynchronised; failing closes the connection.
            let (frames, consumed) = protocol::parse_frames(&st.buf)
                .map_err(|e| format!("frame parse error from {:?}: {}", conn_id, e))?;
            st.buf.drain(..consumed);
            st.requests.extend(frames);
        }
        Inner::drive(&self.0, ctx, conn_id, state);
        Ok(())
    }

    fn on_drained(&self, ctx: &ServerContext, conn_id: ConnectionId) -> Result<()> {
        if let Some(state) = self.0.state(conn_id) {
            Inner::drive(&self.0, ctx, conn_id, state);
        }
        Ok(())
    }

    fn on_disconnect(&self, _ctx: &ServerContext, conn_id: ConnectionId) -> Result<()> {
        log::debug!("RPC connection closed: {:?}", conn_id);
        if let Some(state) = write_lock(&self.0.conns).remove(&conn_id) {
            let timer = lock(&state).stall_timer.take();
            self.0.cancel(timer);
        }
        Ok(())
    }

    fn on_error(&self, _ctx: &ServerContext, conn_id: Option<ConnectionId>, error: NetworkError) {
        log::error!("RPC network error (conn={:?}): {}", conn_id, error);
    }
}

impl Inner {
    fn state(&self, conn_id: ConnectionId) -> Option<Arc<Mutex<ConnState>>> {
        read_lock(&self.conns).get(&conn_id).cloned()
    }

    fn on_reactor_thread(&self) -> bool {
        self.event_loop
            .upgrade()
            .is_some_and(|event_loop| event_loop.is_reactor_thread())
    }

    fn cancel(&self, timer: Option<TimerId>) {
        if let (Some(timer), Some(event_loop)) = (timer, self.event_loop.upgrade()) {
            event_loop.cancel_timer(timer);
        }
    }

    /// Sends queued replies, then dispatches queued requests, until a reply is refused or
    /// nothing is left. Only one thread drives a connection at a time.
    fn drive(
        this: &Arc<Self>,
        ctx: &ServerContext,
        conn_id: ConnectionId,
        state: Arc<Mutex<ConnState>>,
    ) {
        {
            let mut st = lock(&state);
            if st.driving {
                st.again = true;
                return;
            }
            st.driving = true;
        }
        let Some(sender) = ctx.sender(conn_id) else {
            let mut st = lock(&state);
            st.requests.clear();
            st.replies.clear();
            st.driving = false;
            return;
        };
        Self::run(this, sender, conn_id, state);
    }

    /// The body of `drive`, entered with the driver role held. No lock is held while
    /// sending or dispatching.
    fn run(
        this: &Arc<Self>,
        sender: ConnSender,
        conn_id: ConnectionId,
        state: Arc<Mutex<ConnState>>,
    ) {
        loop {
            let blocked = this.send_replies(&sender, &state);
            if !blocked {
                let (timer, pending, pool_mode) = {
                    let mut st = lock(&state);
                    let timer = std::mem::take(&mut st.stalled).then(|| st.stall_timer.take());
                    (timer, !st.requests.is_empty(), st.pool_mode)
                };
                if let Some(timer) = timer {
                    this.cancel(timer);
                }
                if pending && pool_mode {
                    if let Some(event_loop) = this.event_loop.upgrade() {
                        if event_loop.is_reactor_thread() {
                            let mut st = lock(&state);
                            st.backlog = true;
                            st.sync_reads(&sender);
                            drop(st);
                            // The driver role moves with the closure, so order is kept.
                            let this = Arc::clone(this);
                            event_loop
                                .spawn_compute(move || Self::run(&this, sender, conn_id, state));
                            return;
                        }
                    }
                }
                let next = lock(&state).requests.pop_front();
                if let Some(frame) = next {
                    if let Some(reply) = this.handle_frame(conn_id, frame) {
                        lock(&state).replies.push_back(reply.encode());
                    }
                    continue;
                }
            }
            let mut st = lock(&state);
            if !blocked {
                st.backlog = false;
            }
            st.sync_reads(&sender);
            if std::mem::take(&mut st.again) {
                continue;
            }
            st.driving = false;
            return;
        }
    }

    /// Sends queued replies in order. True if one was refused and the connection stalled.
    fn send_replies(&self, sender: &ConnSender, state: &Arc<Mutex<ConnState>>) -> bool {
        loop {
            let Some(reply) = lock(state).replies.pop_front() else {
                return false;
            };
            match sender.send(&reply) {
                Ok(()) => {}
                Err(SendError::Backpressure) => {
                    let mut st = lock(state);
                    st.replies.push_front(reply);
                    if !st.stalled {
                        st.stalled = true;
                        st.stall_gen += 1;
                        st.stall_timer = self.arm_stall_timer(sender, state, st.stall_gen);
                        st.sync_reads(sender);
                    }
                    return true;
                }
                Err(e) => {
                    log::debug!("Dropping replies to {:?}: {}", sender.id(), e);
                    let mut st = lock(state);
                    st.replies.clear();
                    st.requests.clear();
                    return true;
                }
            }
        }
    }

    fn arm_stall_timer(
        &self,
        sender: &ConnSender,
        state: &Arc<Mutex<ConnState>>,
        gen: u64,
    ) -> Option<TimerId> {
        let event_loop = self.event_loop.upgrade()?;
        let sender = sender.clone();
        let state = Arc::downgrade(state);
        Some(event_loop.schedule_once(self.slow_reader_timeout, move || {
            let Some(state) = state.upgrade() else {
                return;
            };
            let expired = {
                let st = lock(&state);
                st.stalled && st.stall_gen == gen
            };
            if expired {
                log::error!(
                    "{:?} left its replies unread too long; closing",
                    sender.id()
                );
                sender.close();
            }
        }))
    }

    /// Dispatches one frame and returns the reply to send, if any.
    fn handle_frame(&self, conn_id: ConnectionId, frame: Frame) -> Option<Frame> {
        match frame.header.message_type {
            MessageType::Request => self.handle_request(conn_id, frame),
            MessageType::Ping => Some(Frame::pong()),
            MessageType::Cancel => {
                log::debug!("Cancel received from {:?} (not yet supported)", conn_id);
                None
            }
            other => {
                log::warn!("Unexpected message type {:?} from {:?}", other, conn_id);
                None
            }
        }
    }

    fn handle_request(&self, conn_id: ConnectionId, frame: Frame) -> Option<Frame> {
        let (request_id, service_id, method_id, args) = match frame.parse_request_payload() {
            Ok(parsed) => parsed,
            Err(e) => {
                log::error!("Invalid request payload from {:?}: {}", conn_id, e);
                return None;
            }
        };

        let is_one_way = frame.header.flags.is_one_way();

        let rpc_ctx = RpcContext::new(request_id, service_id, method_id);

        // Look up the service and dispatch.
        let result = {
            let services = read_lock(&self.services);
            match services.get(&service_id) {
                Some(svc) => svc
                    .dispatcher
                    .dispatch(&rpc_ctx, method_id, args, &self.codec),
                None => Err(RpcError::service_not_found(service_id)),
            }
        };

        // Don't send a response for one-way calls.
        if is_one_way {
            if let Err(e) = &result {
                log::error!("One-way request {}.{} failed: {}", service_id, method_id, e);
            }
            return None;
        }

        match result {
            Ok(resp_bytes) => Some(Frame::response(request_id, resp_bytes)),
            Err(rpc_err) => match self.codec.serialize(&rpc_err) {
                Ok(err_bytes) => Some(Frame::error(request_id, err_bytes)),
                Err(e) => {
                    log::error!("Failed to serialize error: {}", e);
                    None
                }
            },
        }
    }
}
