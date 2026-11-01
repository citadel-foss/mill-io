//! TCP server and client implementations with lockfree connection management.
//!
//! This module provides high-performance TCP networking built on Mill-IO's event loop.
//!
//! Each connection gets a [`ConnectionId`] that is unique in the process, and is
//! registered with the loop under a token from [`EventLoop::alloc_token`].
//!
//! ## Event handling
//!
//! - **Readable:** the connection reads until WouldBlock, handing each chunk to
//!   `on_data`. After [`MAX_READ_PER_EVENT`] bytes it re-posts the rest with
//!   [`EventLoop::post`], so one busy connection cannot starve the others.
//! - **Writes:** [`ConnSender::send`] queues a whole frame and writes what the socket takes;
//!   each writable event drains the queue until it is empty or the socket would block.
//!   Above the high watermark `send` returns [`SendError::Backpressure`], and
//!   `on_drained` fires once the queue falls to the low watermark.
//! - **Close:** EOF, an I/O error, a failing `on_data`, or [`ConnSender::close`]
//!   deregisters the socket, closes it, and fires `on_disconnect` once.
//!
//! ## Configuration
//!
//! TcpServerConfig uses the builder pattern for ergonomic configuration:
//!
//! ```rust
//! use mill_net::tcp::config::TcpServerConfig;
//! # use std::sync::Arc;
//!
//! let config = TcpServerConfig::builder()
//!     .address("0.0.0.0:8080".parse().unwrap())
//!     .buffer_size(16384)              // Larger buffers for high throughput
//!     .max_connections(1000)           // Limit concurrent connections
//!     .no_delay(true)                  // Disable Nagle's algorithm
//!     .build();
//! ```
//!
//! ## Handler Implementation
//!
//! Your handler must implement NetworkHandler trait.
//!
//! ```rust
//! use mill_net::tcp::{traits::{NetworkHandler, ConnectionId}, ServerContext};
//! use mill_net::errors::Result;
//!
//! struct MyHandler;
//!
//! impl NetworkHandler for MyHandler {
//!     fn on_data(&self, ctx: &ServerContext, conn_id: ConnectionId, data: &[u8]) -> Result<()> {
//!         println!("Received {} bytes from {:?}", data.len(), conn_id);
//!         ctx.send_to(conn_id, b"some response")?;
//!         Ok(())
//!     }
//! }
//! ```

pub mod config;
mod conn;
pub mod layer;
mod sockopt;
pub mod traits;

pub use config::{TcpClientConfig, TcpServerConfig};
pub use conn::{ConnSender, SendError, MAX_READ_PER_EVENT};
pub use layer::Layer;
pub use traits::*;

use crate::errors::Result;
use crate::errors::{NetworkError, NetworkEvent};
use conn::{next_conn_id, Conn, ConnectOpts, ContextOps, Shared, Watermarks};
use mill_io::{EventHandler, EventLoop, ObjectPool};
use mio::event::Event;
use mio::net::{TcpListener, TcpStream};
use mio::{Interest, Token};
use parking_lot::Mutex;
#[cfg(test)]
use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

/// Builds one layer for each accepted connection.
type LayerFactory = Arc<dyn Fn() -> Box<dyn Layer> + Send + Sync>;

/// How long the listener waits to accept again after the process ran out of descriptors.
const ACCEPT_RETRY: Duration = Duration::from_millis(100);

/// Context for network handlers to interact with the server or client that called them.
pub struct ServerContext {
    ops: Weak<dyn ContextOps>,
}

impl ServerContext {
    /// Send data to a specific connection; see [`ConnSender::send`].
    ///
    /// Fails with [`NetworkError::ConnectionNotFound`] for an unknown connection, or with
    /// the [`SendError`] that `send` returned.
    pub fn send_to(&self, conn_id: ConnectionId, data: &[u8]) -> Result<()> {
        match self.sender(conn_id) {
            Some(sender) => sender.send(data).map_err(|e| e.into()),
            None => Err(Box::new(NetworkError::ConnectionNotFound(conn_id))),
        }
    }

    /// Send data to every connection. Per-connection failures go to `on_error`.
    pub fn broadcast(&self, data: &[u8]) -> Result<()> {
        if let Some(ops) = self.ops.upgrade() {
            ops.broadcast(data);
        }
        Ok(())
    }

    /// Close a specific connection; see [`ConnSender::close`].
    pub fn close_connection(&self, conn_id: ConnectionId) -> Result<()> {
        if let Some(sender) = self.sender(conn_id) {
            sender.close();
        }
        Ok(())
    }

    /// A sender for `conn`, usable from any thread; `None` if it is not open.
    pub fn sender(&self, conn: ConnectionId) -> Option<ConnSender> {
        self.ops.upgrade()?.sender(conn)
    }
}

/// High-level TCP server
///
/// A started server runs until [`stop`](Self::stop), or until its event loop is gone;
/// dropping the handle passed to [`start`](Self::start) does not stop it.
///
/// Connections the peer aborted or reset before they were accepted are skipped. If the
/// process runs out of file descriptors, the server reports it once through
/// `on_error(None, NetworkError::Accept(..))` and tries again every 100 ms.
pub struct TcpServer<H: NetworkHandler> {
    shared: Arc<Shared<H>>,
    listener: Arc<Mutex<Option<TcpListener>>>,
    listener_token: Mutex<Option<Token>>,
    local_addr: SocketAddr,
    config: TcpServerConfig,
    layers: Vec<LayerFactory>,
    #[cfg(test)]
    accept_faults: Arc<Mutex<VecDeque<io::Error>>>,
}

impl<H: NetworkHandler> TcpServer<H> {
    pub fn new(config: TcpServerConfig, handler: H) -> Result<Self> {
        let listener = TcpListener::bind(config.address)?;
        let buffer_size = config.buffer_size;
        let shared = Shared::new(handler, ObjectPool::new(20, move || vec![0; buffer_size]));

        Ok(Self {
            shared,
            local_addr: listener.local_addr()?,
            listener: Arc::new(Mutex::new(Some(listener))),
            listener_token: Mutex::new(None),
            config,
            layers: Vec::new(),
            #[cfg(test)]
            accept_faults: Arc::default(),
        })
    }

    /// Adds a [`Layer`] above any added before, built by `factory` for each accepted
    /// connection. Call before [`start`](Self::start).
    pub fn with_layer<F>(mut self, factory: F) -> Self
    where
        F: Fn() -> Box<dyn Layer> + Send + Sync + 'static,
    {
        self.layers.push(Arc::new(factory));
        self
    }

    /// Get the local address the server is bound to
    pub fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.local_addr)
    }

    /// Start the server by registering with the event loop
    pub fn start(
        self: Arc<Self>,
        event_loop: &Arc<EventLoop>,
        listener_token: Token,
    ) -> Result<()> {
        self.shared.bind(event_loop)?;

        let listener_handler = ListenerEvents(Arc::new(Listener {
            shared: self.shared.clone(),
            listener: self.listener.clone(),
            config: self.config.clone(),
            layers: self.layers.clone(),
            out_of_fds_reported: AtomicBool::new(false),
            retry_pending: AtomicBool::new(false),
            #[cfg(test)]
            faults: self.accept_faults.clone(),
        }));

        let mut listener = self.listener.lock();
        let Some(listener) = listener.as_mut() else {
            return Err(Box::new(NetworkError::Configuration(
                "server was stopped".to_string(),
            )));
        };
        event_loop.register(
            listener,
            listener_token,
            Interest::READABLE,
            listener_handler,
        )?;
        *self.listener_token.lock() = Some(listener_token);

        Ok(())
    }

    /// Stops accepting, closes the listening socket, and closes every connection, firing
    /// `on_disconnect` for each. Later calls do nothing.
    pub fn stop(&self) -> Result<()> {
        // Set before the connections are collected; `accept` checks it after inserting,
        // so a connection accepted concurrently is closed by one side or the other.
        self.shared.stopped.store(true, Ordering::SeqCst);

        let listener = self.listener.lock().take();
        let token = self.listener_token.lock().take();
        if let (Some(mut listener), Some(token)) = (listener, token) {
            if let Some(event_loop) = self.shared.event_loop() {
                let _ = event_loop.deregister(&mut listener, token);
            }
        }

        let conns: Vec<_> = self
            .shared
            .conns
            .iter()
            .map(|entry| entry.val().clone())
            .collect();
        for conn in conns {
            conn.shutdown(true);
        }
        Ok(())
    }

    /// Get active connection count
    pub fn connection_count(&self) -> usize {
        self.shared.count.load(Ordering::SeqCst)
    }
}

/// Registered with the loop for the listening socket.
struct ListenerEvents<H: NetworkHandler>(Arc<Listener<H>>);

impl<H: NetworkHandler> EventHandler for ListenerEvents<H> {
    fn handle_event(&self, event: &Event) {
        if event.is_readable() {
            Listener::accept_backlog(&self.0);
        }
    }
}

/// The listening socket and its accept state, shared with the accept retry timer.
struct Listener<H: NetworkHandler> {
    shared: Arc<Shared<H>>,
    listener: Arc<Mutex<Option<TcpListener>>>,
    config: TcpServerConfig,
    layers: Vec<LayerFactory>,
    /// Set from the first out-of-descriptors error until the backlog drains again.
    out_of_fds_reported: AtomicBool,
    /// An `ACCEPT_RETRY` timer is scheduled.
    retry_pending: AtomicBool,
    #[cfg(test)]
    faults: Arc<Mutex<VecDeque<io::Error>>>,
}

/// Errors that end one pending connection, not the listener: accept the next one.
fn is_transient(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::Interrupted
    )
}

/// EMFILE and ENFILE, which have the same values on Linux, macOS and the BSDs.
#[cfg(unix)]
const OUT_OF_FDS: &[i32] = &[24, 23];
/// WSAEMFILE.
#[cfg(windows)]
const OUT_OF_FDS: &[i32] = &[10024];
#[cfg(not(any(unix, windows)))]
const OUT_OF_FDS: &[i32] = &[];

fn is_out_of_fds(e: &io::Error) -> bool {
    e.raw_os_error()
        .is_some_and(|code| OUT_OF_FDS.contains(&code))
}

impl<H: NetworkHandler> Listener<H> {
    /// Accepts until `WouldBlock`. Under edge triggering a backlog left behind raises no new
    /// event, so the loop only stops early when a timer will resume it, or on an error it
    /// cannot get past.
    fn accept_backlog(this: &Arc<Self>) {
        loop {
            let Some(accepted) = this.accept_one() else {
                return;
            };
            match accepted {
                Ok((stream, peer_addr)) => this.accept(stream, peer_addr),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) if is_transient(&e) => {}
                Err(e) if is_out_of_fds(&e) => {
                    if !this.out_of_fds_reported.swap(true, Ordering::SeqCst) {
                        this.shared.report(None, NetworkError::Accept(Box::new(e)));
                    }
                    Self::retry_later(this);
                    return;
                }
                Err(e) => {
                    this.shared.report(None, NetworkError::Accept(Box::new(e)));
                    return;
                }
            }
        }
        this.out_of_fds_reported.store(false, Ordering::SeqCst);
    }

    /// Accepting again right away would spin until a descriptor is freed, so a timer does
    /// it; at most one is pending.
    fn retry_later(this: &Arc<Self>) {
        if this.retry_pending.swap(true, Ordering::SeqCst) {
            return;
        }
        let Some(event_loop) = this.shared.event_loop() else {
            return;
        };
        let listener = Arc::downgrade(this);
        event_loop.schedule_once(ACCEPT_RETRY, move || {
            if let Some(listener) = listener.upgrade() {
                listener.retry_pending.store(false, Ordering::SeqCst);
                Self::accept_backlog(&listener);
            }
        });
    }

    fn accept_one(&self) -> Option<io::Result<(TcpStream, SocketAddr)>> {
        #[cfg(test)]
        if let Some(fault) = self.faults.lock().pop_front() {
            return Some(Err(fault));
        }
        self.listener.lock().as_ref().map(TcpListener::accept)
    }

    /// Takes a slot under `max_connections`, or reports the peer and returns false.
    fn reserve_slot(&self, peer_addr: SocketAddr) -> bool {
        let count = &self.shared.count;
        let Some(max) = self.config.max_connections else {
            count.fetch_add(1, Ordering::SeqCst);
            return true;
        };
        let mut current = count.load(Ordering::SeqCst);
        let reserved = loop {
            if current >= max {
                break false;
            }
            match count.compare_exchange(current, current + 1, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => break true,
                Err(actual) => current = actual,
            }
        };
        if !reserved {
            self.shared
                .report(None, NetworkError::MaxConnectionsReached(peer_addr));
        }
        reserved
    }

    fn accept(&self, stream: TcpStream, peer_addr: SocketAddr) {
        let shared = &self.shared;
        if !self.reserve_slot(peer_addr) {
            return;
        }
        let Some(event_loop) = shared.event_loop() else {
            shared.count.fetch_sub(1, Ordering::SeqCst);
            shared.report(None, NetworkError::EventLoopGone);
            return;
        };

        let conn_id = next_conn_id();
        if let Err(e) = stream.set_nodelay(self.config.no_delay) {
            shared.report(
                Some(conn_id),
                NetworkError::Configuration(format!("Failed to set TCP_NODELAY: {}", e)),
            );
        }

        let marks = Watermarks {
            high: self.config.high_watermark,
            low: self.config.low_watermark,
        };
        if let Some(idle) = self.config.keep_alive {
            if let Err(e) = sockopt::set_keepalive(&stream, Some(idle)) {
                shared.report(
                    Some(conn_id),
                    NetworkError::Configuration(format!("Failed to set SO_KEEPALIVE: {}", e)),
                );
            }
        }

        let layers = self.layers.iter().map(|factory| factory()).collect();
        let conn = Conn::new(
            conn_id,
            stream,
            Arc::downgrade(shared),
            marks,
            false,
            layers,
        );
        shared.conns.insert(conn_id, conn.clone());
        if shared.stopped.load(Ordering::SeqCst) {
            conn.shutdown(false);
            return;
        }

        let _ = shared.handler.on_event(
            &shared.ctx,
            NetworkEvent::ConnectionEstablished(conn_id, peer_addr),
        );

        // `on_connect` runs before the socket is registered, so it precedes any `on_data`.
        // With a handshake to do, the read path runs it once the layers are ready.
        if Conn::start_layers(&conn) && !conn.connect_handler(shared) {
            return;
        }

        let token = event_loop.alloc_token();
        if let Err(e) = Conn::register(
            &conn,
            &event_loop,
            token,
            Interest::READABLE | Interest::WRITABLE,
        ) {
            shared.report(Some(conn_id), NetworkError::Io(e));
            conn.shutdown(true);
        }
    }
}

/// High-level TCP client: one outbound connection.
///
/// Dropping it closes the connection, as [`disconnect`](Self::disconnect) does.
pub struct TcpClient<H: NetworkHandler> {
    shared: Arc<Shared<H>>,
    conn: Arc<Conn<H>>,
    config: TcpClientConfig,
    started: bool,
}

impl<H: NetworkHandler> TcpClient<H> {
    /// Starts a non-blocking connect to `addr`.
    ///
    /// Fails only on errors detected at once. A refused or timed-out connect is reported
    /// later through `on_error(Some(id), ..)`, without `on_connect` or `on_disconnect`.
    pub fn connect(addr: SocketAddr, config: TcpClientConfig, handler: H) -> Result<Self> {
        let stream = TcpStream::connect(addr)?;
        let shared = Shared::new(handler, ObjectPool::new(2, || vec![0; 8192]));
        let conn_id = next_conn_id();
        let marks = Watermarks {
            high: config.high_watermark,
            low: config.low_watermark,
        };
        let conn = Conn::new(
            conn_id,
            stream,
            Arc::downgrade(&shared),
            marks,
            true,
            Vec::new(),
        );
        shared.conns.insert(conn_id, conn.clone());
        shared.count.fetch_add(1, Ordering::SeqCst);

        Ok(Self {
            shared,
            conn,
            config,
            started: false,
        })
    }

    /// Adds a [`Layer`] above any added before.
    ///
    /// Fails with [`NetworkError::Configuration`] after [`start`](Self::start) or the
    /// first send, which the new layer would have missed.
    pub fn with_layer<L: Layer>(self, layer: L) -> Result<Self> {
        if self.started || !self.conn.add_layer(Box::new(layer)) {
            return Err(Box::new(NetworkError::Configuration(
                "layers must be added before start() and the first send".to_string(),
            )));
        }
        Ok(self)
    }

    /// Registers the connection with `event_loop` under a token from
    /// [`EventLoop::alloc_token`] and starts the connect deadline.
    ///
    /// `on_connect` fires on the loop once the handshake completes.
    pub fn start(&mut self, event_loop: &Arc<EventLoop>) -> Result<()> {
        self.shared.bind(event_loop)?;
        self.started = true;
        if self.conn.is_closed() {
            return Err(Box::new(NetworkError::ConnectionNotFound(self.conn.id())));
        }

        self.conn.set_connect_opts(ConnectOpts {
            nodelay: self.config.nodelay,
            keep_alive: self.config.keep_alive,
        });
        Conn::arm_connect_timer(&self.conn, event_loop, self.config.connect_timeout);
        let token = event_loop.alloc_token();
        if let Err(e) = Conn::register(
            &self.conn,
            event_loop,
            token,
            Interest::READABLE | Interest::WRITABLE,
        ) {
            self.conn.shutdown(false);
            return Err(e);
        }

        Ok(())
    }

    /// The id handlers see for this connection.
    pub fn id(&self) -> ConnectionId {
        self.conn.id()
    }

    /// A sender for this connection, usable from any thread.
    pub fn sender(&self) -> ConnSender {
        Conn::sender(&self.conn)
    }

    /// Same as `self.sender().send(data)`.
    pub fn send(&self, data: &[u8]) -> std::result::Result<(), SendError> {
        self.sender().send(data)
    }

    /// Same as `self.sender().close()`.
    pub fn disconnect(&self) -> Result<()> {
        self.conn.shutdown(true);
        Ok(())
    }
}

impl<H: NetworkHandler> Drop for TcpClient<H> {
    fn drop(&mut self) {
        self.conn.shutdown(true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mill_io::EventLoop;
    use std::sync::{Arc, Condvar, Mutex};
    use std::thread;
    use std::time::Duration;

    struct TestHandler {
        on_connect_cb: Option<Box<dyn Fn() + Send + Sync>>,
        #[allow(clippy::type_complexity)]
        on_data_cb: Option<Box<dyn Fn(&ServerContext, ConnectionId, &[u8]) + Send + Sync>>,
    }

    impl TestHandler {
        fn new() -> Self {
            Self {
                on_connect_cb: None,
                on_data_cb: None,
            }
        }

        fn with_on_connect<F>(mut self, f: F) -> Self
        where
            F: Fn() + Send + Sync + 'static,
        {
            self.on_connect_cb = Some(Box::new(f));
            self
        }

        fn with_on_data<F>(mut self, f: F) -> Self
        where
            F: Fn(&ServerContext, ConnectionId, &[u8]) + Send + Sync + 'static,
        {
            self.on_data_cb = Some(Box::new(f));
            self
        }
    }

    impl NetworkHandler for TestHandler {
        fn on_connect(&self, _ctx: &ServerContext, _conn_id: ConnectionId) -> Result<()> {
            if let Some(cb) = &self.on_connect_cb {
                cb();
            }
            Ok(())
        }

        fn on_data(&self, ctx: &ServerContext, conn_id: ConnectionId, data: &[u8]) -> Result<()> {
            if let Some(cb) = &self.on_data_cb {
                cb(ctx, conn_id, data);
            }
            Ok(())
        }
    }

    #[test]
    fn test_tcp_server_client_echo() {
        let event_loop = Arc::new(EventLoop::new(2, 1024, 100).unwrap());

        // synchronization primitives
        let server_connected = Arc::new((Mutex::new(false), Condvar::new()));
        let client_received = Arc::new((Mutex::new(false), Condvar::new()));
        let received_data = Arc::new(Mutex::new(Vec::new()));

        // server handler
        let sc = server_connected.clone();
        let server_handler = TestHandler::new()
            .with_on_connect(move || {
                let (lock, cvar) = &*sc;
                let mut started = lock.lock().unwrap();
                *started = true;
                cvar.notify_all();
            })
            .with_on_data(|ctx, conn_id, data| {
                ctx.send_to(conn_id, data).unwrap();
            });

        // setup server
        let config = TcpServerConfig::builder()
            .address("127.0.0.1:0".parse().unwrap())
            .build();
        let server = Arc::new(TcpServer::new(config, server_handler).unwrap());
        let server_addr = server.local_addr().unwrap();

        server.clone().start(&event_loop, Token(1)).unwrap();

        // client handler
        let cr = client_received.clone();
        let rd = received_data.clone();
        let client_handler = TestHandler::new().with_on_data(move |_, _, data| {
            let mut r_data = rd.lock().unwrap();
            r_data.extend_from_slice(data);
            let (lock, cvar) = &*cr;
            let mut received = lock.lock().unwrap();
            *received = true;
            cvar.notify_all();
        });

        let mut client =
            TcpClient::connect(server_addr, TcpClientConfig::default(), client_handler).unwrap();
        client.start(&event_loop).unwrap();

        let el_clone = event_loop.clone();
        thread::spawn(move || {
            el_clone.run().unwrap();
        });

        {
            let (lock, cvar) = &*server_connected;
            let mut started = lock.lock().unwrap();
            while !*started {
                let result = cvar.wait_timeout(started, Duration::from_secs(2)).unwrap();
                if result.1.timed_out() {
                    panic!("Server did not accept connection in time");
                }
                started = result.0;
            }
        }

        let msg = b"Hello, World!";
        client.send(msg).unwrap();

        {
            let (lock, cvar) = &*client_received;
            let mut received = lock.lock().unwrap();
            while !*received {
                let result = cvar.wait_timeout(received, Duration::from_secs(2)).unwrap();
                if result.1.timed_out() {
                    panic!("Client did not receive data in time");
                }
                received = result.0;
            }
        }

        let data = received_data.lock().unwrap();
        assert_eq!(*data, msg);

        event_loop.stop();
    }

    #[test]
    fn nodelay_is_set_on_clients_and_accepted_connections() {
        for event_loop in [
            Arc::new(EventLoop::new(2, 1024, 10).unwrap()),
            Arc::new(EventLoop::new_low_latency(1024, 10).unwrap()),
        ] {
            let connected = Arc::new((Mutex::new(0), Condvar::new()));
            let on_connect = |connected: &Arc<(Mutex<i32>, Condvar)>| {
                let connected = Arc::clone(connected);
                move || {
                    let (count, cvar) = &*connected;
                    *count.lock().unwrap() += 1;
                    cvar.notify_all();
                }
            };
            let config = TcpServerConfig::builder()
                .address("127.0.0.1:0".parse().unwrap())
                .build();
            let server = Arc::new(
                TcpServer::new(
                    config,
                    TestHandler::new().with_on_connect(on_connect(&connected)),
                )
                .unwrap(),
            );
            Arc::clone(&server)
                .start(&event_loop, event_loop.alloc_token())
                .unwrap();
            let mut client = TcpClient::connect(
                server.local_addr().unwrap(),
                TcpClientConfig::default(),
                TestHandler::new().with_on_connect(on_connect(&connected)),
            )
            .unwrap();
            client.start(&event_loop).unwrap();
            let el = Arc::clone(&event_loop);
            let runner = thread::spawn(move || el.run().unwrap());

            let (count, cvar) = &*connected;
            let guard = cvar
                .wait_timeout_while(count.lock().unwrap(), Duration::from_secs(5), |n| *n < 2)
                .unwrap()
                .0;
            assert_eq!(*guard, 2, "client and server did not both connect");
            drop(guard);

            assert_eq!(client.conn.nodelay(), Some(true), "client");
            let accepted: Vec<_> = server
                .shared
                .conns
                .iter()
                .map(|e| e.val().nodelay())
                .collect();
            assert_eq!(accepted, vec![Some(true)], "accepted connection");

            event_loop.stop();
            runner.join().unwrap();
        }
    }

    type ErrorLog = Arc<Mutex<Vec<(Option<ConnectionId>, String)>>>;

    #[derive(Clone, Default)]
    struct AcceptLog {
        connects: Arc<std::sync::atomic::AtomicUsize>,
        errors: ErrorLog,
    }

    impl NetworkHandler for AcceptLog {
        fn on_connect(&self, _ctx: &ServerContext, _conn: ConnectionId) -> Result<()> {
            self.connects.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn on_data(&self, _ctx: &ServerContext, _conn: ConnectionId, _data: &[u8]) -> Result<()> {
            Ok(())
        }

        fn on_error(&self, _ctx: &ServerContext, conn: Option<ConnectionId>, error: NetworkError) {
            self.errors.lock().unwrap().push((conn, error.to_string()));
        }
    }

    fn wait_for(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        while !cond() {
            if std::time::Instant::now() >= deadline {
                return false;
            }
            thread::sleep(Duration::from_millis(1));
        }
        true
    }

    /// A server whose listener fails with `faults`, in order, before its real accepts.
    /// Clients connect before it starts, so the whole backlog raises a single edge.
    fn server_with_backlog(
        event_loop: &Arc<EventLoop>,
        faults: Vec<io::Error>,
        clients: usize,
    ) -> (
        Arc<TcpServer<AcceptLog>>,
        AcceptLog,
        Vec<std::net::TcpStream>,
    ) {
        let log = AcceptLog::default();
        let config = TcpServerConfig::builder()
            .address("127.0.0.1:0".parse().unwrap())
            .build();
        let server = Arc::new(TcpServer::new(config, log.clone()).unwrap());
        server.accept_faults.lock().extend(faults);
        let addr = server.local_addr().unwrap();
        let peers = (0..clients)
            .map(|_| std::net::TcpStream::connect(addr).unwrap())
            .collect();
        Arc::clone(&server)
            .start(event_loop, event_loop.alloc_token())
            .unwrap();
        (server, log, peers)
    }

    fn out_of_fds() -> io::Error {
        io::Error::from_raw_os_error(OUT_OF_FDS[0])
    }

    #[test]
    fn transient_accept_errors_do_not_strand_the_backlog() {
        let event_loop = Arc::new(EventLoop::new(2, 1024, 10).unwrap());
        let faults = vec![
            io::ErrorKind::ConnectionAborted.into(),
            io::ErrorKind::ConnectionReset.into(),
            io::ErrorKind::Interrupted.into(),
        ];
        let (server, log, peers) = server_with_backlog(&event_loop, faults, 3);
        let runner = {
            let event_loop = Arc::clone(&event_loop);
            thread::spawn(move || event_loop.run().unwrap())
        };

        assert!(
            wait_for(Duration::from_secs(5), || log
                .connects
                .load(Ordering::SeqCst)
                == 3),
            "{} of 3 queued connections accepted",
            log.connects.load(Ordering::SeqCst)
        );
        assert!(log.errors.lock().unwrap().is_empty(), "{:?}", log.errors);

        event_loop.stop();
        runner.join().unwrap();
        drop((server, peers));
    }

    #[test]
    fn running_out_of_fds_is_reported_once_and_retried_on_a_timer() {
        let event_loop = Arc::new(EventLoop::new(2, 1024, 10).unwrap());
        let faults = vec![out_of_fds(), out_of_fds(), out_of_fds()];
        let (server, log, mut peers) = server_with_backlog(&event_loop, faults, 2);
        let started = std::time::Instant::now();
        let runner = {
            let event_loop = Arc::clone(&event_loop);
            thread::spawn(move || event_loop.run().unwrap())
        };

        assert!(
            wait_for(Duration::from_secs(5), || log
                .connects
                .load(Ordering::SeqCst)
                == 2),
            "{} of 2 queued connections accepted",
            log.connects.load(Ordering::SeqCst)
        );
        // Three failures, each retried after ACCEPT_RETRY rather than at once.
        assert!(
            started.elapsed() >= 2 * ACCEPT_RETRY,
            "{:?}",
            started.elapsed()
        );
        {
            let errors = log.errors.lock().unwrap();
            assert_eq!(errors.len(), 1, "{errors:?}");
            assert_eq!(errors[0].0, None);
            assert!(errors[0].1.starts_with("Accept Error"), "{errors:?}");
        }

        // The last retry timer drained the backlog on the reactor thread, so a closure posted
        // there runs after it has finished.
        let (done, finished) = std::sync::mpsc::channel();
        event_loop
            .post(move || {
                let _ = done.send(());
            })
            .unwrap();
        finished.recv_timeout(Duration::from_secs(5)).unwrap();

        // Running out again after the backlog drained is a new episode, reported again.
        server.accept_faults.lock().push_back(out_of_fds());
        peers.push(std::net::TcpStream::connect(server.local_addr().unwrap()).unwrap());
        assert!(wait_for(Duration::from_secs(5), || log
            .connects
            .load(Ordering::SeqCst)
            == 3));
        assert_eq!(log.errors.lock().unwrap().len(), 2);

        event_loop.stop();
        runner.join().unwrap();
        drop((server, peers));
    }
}
