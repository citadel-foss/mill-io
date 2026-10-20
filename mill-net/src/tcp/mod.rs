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
use std::io;
use std::net::SocketAddr;
use std::sync::{atomic::Ordering, Arc, Weak};

/// Builds one layer for each accepted connection.
type LayerFactory = Arc<dyn Fn() -> Box<dyn Layer> + Send + Sync>;

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
pub struct TcpServer<H: NetworkHandler> {
    shared: Arc<Shared<H>>,
    listener: Arc<Mutex<Option<TcpListener>>>,
    listener_token: Mutex<Option<Token>>,
    local_addr: SocketAddr,
    config: TcpServerConfig,
    layers: Vec<LayerFactory>,
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

        let listener_handler = ListenerEvents {
            shared: self.shared.clone(),
            listener: self.listener.clone(),
            config: self.config.clone(),
            layers: self.layers.clone(),
        };

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
struct ListenerEvents<H: NetworkHandler> {
    shared: Arc<Shared<H>>,
    listener: Arc<Mutex<Option<TcpListener>>>,
    config: TcpServerConfig,
    layers: Vec<LayerFactory>,
}

impl<H: NetworkHandler> EventHandler for ListenerEvents<H> {
    fn handle_event(&self, event: &Event) {
        if !event.is_readable() {
            return;
        }

        loop {
            let accepted = match self.listener.lock().as_ref() {
                Some(listener) => listener.accept(),
                None => return,
            };
            match accepted {
                Ok((stream, peer_addr)) => self.accept(stream, peer_addr),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => {
                    self.shared.report(None, NetworkError::Accept(Box::new(e)));
                    break;
                }
            }
        }
    }
}

impl<H: NetworkHandler> ListenerEvents<H> {
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

        if let Err(e) = stream.set_nodelay(self.config.no_delay) {
            shared.report(
                None,
                NetworkError::Configuration(format!("Failed to set TCP_NODELAY: {}", e)),
            );
        }

        let conn_id = next_conn_id();
        let marks = Watermarks {
            high: self.config.high_watermark,
            low: self.config.low_watermark,
        };
        if let Some(idle) = self.config.keep_alive {
            if let Err(e) = sockopt::set_keepalive(&stream, Some(idle)) {
                shared.report(
                    None,
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
}
