//! TCP server and client implementations with lockfree connection management.
//!
//! This module provides high-performance TCP networking built on Mill-IO's event loop.
//!
//! Each TCP connection is assigned a unique ConnectionId generated atomically.
//! The connection state is stored in a lockfree map, allowing concurrent access
//! from multiple worker threads without blocking.
//!
//! ```text
//! Connection Storage:
//!   LockfreeMap<u64, TcpConnection>
//!        │
//!        ├--> ConnId(1) --> TcpConnection { stream, token, addr }
//!        ├--> ConnId(2) --> TcpConnection { stream, token, addr }
//!        └--> ConnId(N) --> TcpConnection { stream, token, addr }
//! ```
//!
//! ## Event Handling Pipeline
//!
//! ```text
//! 1. Listener Events:
//!    New Connection --> TcpListenerHandler::handle_event()
//!        - accept() --> Create ConnectionId
//!        - Register TcpConnectionHandler with EventLoop
//!        - Insert into LockfreeMap
//!        - Call handler.on_connect()
//!
//! 2. Connection Events:
//!    Readable Event --> TcpConnectionHandler::handle_event()
//!        - Read from stream into pooled buffer
//!        - Call handler.on_data()
//!        - If EOF: disconnect()
//!
//!    Writable Event --> TcpConnectionHandler::handle_event()
//!        - Call handler.on_writable()
//!
//! 3. Disconnection:
//!    disconnect() --> Remove from LockfreeMap
//!        - Deregister from EventLoop
//!        - Call handler.on_disconnect()
//! ```
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
pub mod traits;

pub use config::TcpServerConfig;
pub use traits::*;

use crate::errors::Result;
use crate::errors::{NetworkError, NetworkEvent};
use lock_freedom::map::Map as LockfreeMap;
use mill_io::{EventHandler, EventLoop, ObjectPool, PooledObject};
use mio::event::Event;
use mio::net::{TcpListener, TcpStream};
use mio::{Interest, Token};
use parking_lot::Mutex;
use std::io;
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    Arc, RwLock, Weak,
};

/// Most bytes one readable event reads before the rest is re-posted to the loop.
pub const MAX_READ_PER_EVENT: usize = 64 * 1024;

/// What one read pass ended on.
enum ReadPass {
    /// The socket returned WouldBlock.
    Drained,
    /// `MAX_READ_PER_EVENT` bytes were read; the socket may hold more.
    Capped,
    /// The peer closed its side.
    Eof,
    /// The stream is gone or the handler asked to stop.
    Stopped,
    Failed(io::Error),
}

/// Makes one thread at a time the reader of a connection without blocking the others.
///
/// Under edge-triggered polling a readiness edge that finds the socket busy must not be
/// dropped: it is recorded in `pending`, and the current reader runs another pass for it.
#[derive(Default)]
struct ReadDrive {
    active: AtomicBool,
    pending: AtomicBool,
}

impl ReadDrive {
    /// Records an edge; true if the caller became the reader.
    fn begin(&self) -> bool {
        self.pending.store(true, Ordering::SeqCst);
        self.try_own()
    }

    /// Gives up the reader role; true if a recorded edge made the caller the reader again.
    fn release(&self) -> bool {
        self.active.store(false, Ordering::SeqCst);
        self.pending.load(Ordering::SeqCst) && self.try_own()
    }

    /// Gives up the reader role without looking for recorded edges.
    fn yield_now(&self) {
        self.active.store(false, Ordering::SeqCst);
    }

    fn try_own(&self) -> bool {
        loop {
            if self
                .active
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
            {
                return false;
            }
            if self.pending.swap(false, Ordering::SeqCst) {
                return true;
            }
            self.active.store(false, Ordering::SeqCst);
            if !self.pending.load(Ordering::SeqCst) {
                return false;
            }
        }
    }
}

/// Reads until WouldBlock, EOF or `MAX_READ_PER_EVENT` bytes, passing each chunk to `deliver`.
///
/// Edge-triggered polling raises no new event for bytes left in the socket, so a pass that
/// stops early ends in `Capped`, and the caller must re-post the rest.
fn read_pass(
    buffer: &mut [u8],
    mut read: impl FnMut(&mut [u8]) -> Option<io::Result<usize>>,
    mut deliver: impl FnMut(&[u8]) -> bool,
) -> ReadPass {
    let mut total = 0;
    loop {
        let want = buffer.len().min(MAX_READ_PER_EVENT - total);
        match read(&mut buffer[..want]) {
            None => return ReadPass::Stopped,
            Some(Ok(0)) => return ReadPass::Eof,
            Some(Ok(n)) => {
                if !deliver(&buffer[..n]) {
                    return ReadPass::Stopped;
                }
                total += n;
                if total >= MAX_READ_PER_EVENT {
                    return ReadPass::Capped;
                }
            }
            Some(Err(e)) if e.kind() == io::ErrorKind::WouldBlock => return ReadPass::Drained,
            Some(Err(e)) if e.kind() == io::ErrorKind::Interrupted => {}
            Some(Err(e)) => return ReadPass::Failed(e),
        }
    }
}

/// Context for network handlers to interact with the server.
pub struct ServerContext {
    server: RwLock<Option<Arc<dyn ServerOperations>>>,
    event_loop: RwLock<Option<Arc<EventLoop>>>,
}

impl ServerContext {
    /// Send data to a specific connection.
    pub fn send_to(&self, conn_id: ConnectionId, data: &[u8]) -> Result<()> {
        if let Some(server) = self.server.read().unwrap().as_ref() {
            server.send_to(conn_id, data)
        } else {
            Ok(())
        }
    }

    /// Broadcast data to all connections.
    pub fn broadcast(&self, data: &[u8]) -> Result<()> {
        if let Some(server) = self.server.read().unwrap().as_ref() {
            server.broadcast(data)
        } else {
            Ok(())
        }
    }

    /// Close a specific connection.
    pub fn close_connection(&self, conn_id: ConnectionId) -> Result<()> {
        let server_guard = self.server.read().unwrap();
        if let Some(server) = server_guard.as_ref() {
            if let Some(event_loop) = self.event_loop.read().unwrap().as_ref() {
                server.close_connection(event_loop, conn_id)
            } else {
                Ok(())
            }
        } else {
            Ok(())
        }
    }
}

/// Trait defining server operations, allowing for a weak reference from the context.
trait ServerOperations: Send + Sync {
    fn send_to(&self, conn_id: ConnectionId, data: &[u8]) -> Result<()>;
    fn broadcast(&self, data: &[u8]) -> Result<()>;
    fn close_connection(&self, event_loop: &EventLoop, conn_id: ConnectionId) -> Result<()>;
}

/// High-level TCP server
pub struct TcpServer<H: NetworkHandler> {
    listener: Arc<Mutex<TcpListener>>,
    connections: Arc<LockfreeMap<ConnectionId, TcpConnection>>,
    handler: Arc<H>,
    config: TcpServerConfig,
    buffer_pool: ObjectPool<Vec<u8>>,
    next_conn_id: Arc<AtomicU64>,
    connection_counter: Arc<AtomicUsize>,
    context: Arc<ServerContext>,
}

impl<H: NetworkHandler> TcpServer<H> {
    pub fn new(config: TcpServerConfig, handler: H) -> Result<Self> {
        let listener = TcpListener::bind(config.address)?;

        Ok(Self {
            listener: Arc::new(Mutex::new(listener)),
            connections: Arc::new(LockfreeMap::new()),
            handler: Arc::new(handler),
            buffer_pool: ObjectPool::new(20, move || vec![0; config.buffer_size]),
            next_conn_id: Arc::new(AtomicU64::new(1)),
            connection_counter: Arc::new(AtomicUsize::new(0)),
            config,
            context: Arc::new(ServerContext {
                server: RwLock::new(None),
                event_loop: RwLock::new(None),
            }),
        })
    }

    /// Get the local address the server is bound to
    pub fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.listener.lock().local_addr()?)
    }

    /// Start the server by registering with the event loop
    pub fn start(
        self: Arc<Self>,
        event_loop: &Arc<EventLoop>,
        listener_token: Token,
    ) -> Result<()> {
        // Update the context.
        *self.context.server.write().unwrap() = Some(self.clone());
        *self.context.event_loop.write().unwrap() = Some(event_loop.clone());

        let listener_handler = TcpListenerHandler {
            listener: self.listener.clone(),
            connections: self.connections.clone(),
            handler: self.handler.clone(),
            config: self.config.clone(),
            buffer_pool: self.buffer_pool.clone(),
            next_conn_id: self.next_conn_id.clone(),
            event_loop: Arc::downgrade(event_loop),
            connection_counter: self.connection_counter.clone(),
            context: self.context.clone(),
        };

        event_loop.register(
            &mut *self.listener.lock(),
            listener_token,
            Interest::READABLE,
            listener_handler,
        )?;

        Ok(())
    }

    /// Get active connection count
    pub fn connection_count(&self) -> usize {
        self.connection_counter.load(Ordering::SeqCst)
    }
}

impl<H: NetworkHandler> ServerOperations for TcpServer<H> {
    /// Send data to a specific connection
    fn send_to(&self, conn_id: ConnectionId, data: &[u8]) -> Result<()> {
        if let Some(conn) = self.connections.get(&conn_id) {
            let mut stream = conn.val().stream.lock();
            stream.write_all(data)?;
        } else {
            return Err(Box::new(NetworkError::ConnectionNotFound(conn_id)));
        }
        Ok(())
    }

    /// Close a specific connection
    fn close_connection(&self, event_loop: &EventLoop, conn_id: ConnectionId) -> Result<()> {
        if let Some(conn) = self.connections.remove(&conn_id) {
            let mut stream = conn.val().stream.lock();

            let _ = event_loop.deregister(&mut *stream, conn.val().token);
            let _ = stream.shutdown(std::net::Shutdown::Both);

            if let Err(e) = self.handler.on_disconnect(&self.context, conn_id) {
                self.handler.on_error(
                    &self.context,
                    Some(conn_id),
                    NetworkError::HandlerError(format!("on_disconnect: {}", e)),
                );
            }

            let _ = self
                .handler
                .on_event(&self.context, NetworkEvent::ConnectionClosed(conn_id));
        }
        Ok(())
    }

    /// Broadcast data to all connections
    fn broadcast(&self, data: &[u8]) -> Result<()> {
        for conn in self.connections.iter() {
            let mut stream = conn.val().stream.lock();
            if let Err(e) = stream.write_all(data) {
                self.handler.on_error(
                    &self.context,
                    Some(*conn.key()),
                    NetworkError::Io(Box::new(e)),
                );
            }
        }
        Ok(())
    }
}

/// Internal TCP connection representation
struct TcpConnection {
    stream: Arc<Mutex<TcpStream>>,
    token: Token,
    #[allow(dead_code)]
    peer_addr: SocketAddr,
}

/// Handler for accepting new connections
struct TcpListenerHandler<H: NetworkHandler> {
    listener: Arc<Mutex<TcpListener>>,
    connections: Arc<LockfreeMap<ConnectionId, TcpConnection>>,
    handler: Arc<H>,
    config: TcpServerConfig,
    buffer_pool: ObjectPool<Vec<u8>>,
    next_conn_id: Arc<AtomicU64>,
    event_loop: Weak<EventLoop>,
    connection_counter: Arc<AtomicUsize>,
    context: Arc<ServerContext>,
}

// Safety: We ensure event_loop_ref is valid for the lifetime of the handler
unsafe impl<H: NetworkHandler> Send for TcpListenerHandler<H> {}
unsafe impl<H: NetworkHandler> Sync for TcpListenerHandler<H> {}

impl<H: NetworkHandler> EventHandler for TcpListenerHandler<H> {
    fn handle_event(&self, event: &Event) {
        if !event.is_readable() {
            return;
        }

        loop {
            let listener = self.listener.lock();

            match listener.accept() {
                Ok((stream, peer_addr)) => {
                    // Atomically check and increment the connection count.
                    if let Some(max) = self.config.max_connections {
                        let mut accepted = false;
                        loop {
                            let current = self.connection_counter.load(Ordering::SeqCst);
                            if current >= max {
                                self.handler.on_error(
                                    &self.context,
                                    None,
                                    NetworkError::MaxConnectionsReached(peer_addr),
                                );
                                break;
                            }
                            match self.connection_counter.compare_exchange(
                                current,
                                current + 1,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                            ) {
                                Ok(_) => {
                                    accepted = true;
                                    break;
                                }
                                Err(_) => continue,
                            }
                        }
                        if !accepted {
                            continue;
                        }
                    } else {
                        self.connection_counter.fetch_add(1, Ordering::SeqCst);
                    }

                    if let Err(e) = stream.set_nodelay(self.config.no_delay) {
                        self.handler.on_error(
                            &self.context,
                            None,
                            NetworkError::Configuration(format!(
                                "Failed to set TCP_NODELAY: {}",
                                e
                            )),
                        );
                    }

                    let conn_id = ConnectionId(self.next_conn_id.fetch_add(1, Ordering::SeqCst));
                    let token = Token(conn_id.as_u64() as usize);

                    let stream_arc = Arc::new(Mutex::new(stream));

                    let conn_handler = ConnectionEvents(Arc::new(TcpConnectionHandler {
                        conn_id,
                        stream: stream_arc.clone(),
                        connections: self.connections.clone(),
                        handler: self.handler.clone(),
                        buffer_pool: self.buffer_pool.clone(),
                        event_loop: self.event_loop.clone(),
                        connection_counter: self.connection_counter.clone(),
                        context: self.context.clone(),
                        drive: ReadDrive::default(),
                    }));

                    let event_loop = if let Some(arc) = self.event_loop.upgrade() {
                        arc
                    } else {
                        self.handler
                            .on_error(&self.context, None, NetworkError::EventLoopGone);
                        self.connection_counter.fetch_sub(1, Ordering::SeqCst);
                        continue;
                    };

                    if let Err(e) = event_loop.register(
                        &mut *stream_arc.lock(),
                        token,
                        Interest::READABLE | Interest::WRITABLE,
                        conn_handler,
                    ) {
                        self.handler
                            .on_error(&self.context, Some(conn_id), NetworkError::Io(e));
                        self.connection_counter.fetch_sub(1, Ordering::SeqCst);
                        continue;
                    }

                    let conn = TcpConnection {
                        stream: stream_arc,
                        token,
                        peer_addr,
                    };
                    self.connections.insert(conn_id, conn);

                    let _ = self.handler.on_event(
                        &self.context,
                        NetworkEvent::ConnectionEstablished(conn_id, peer_addr),
                    );

                    if let Err(e) = self.handler.on_connect(&self.context, conn_id) {
                        self.handler.on_error(
                            &self.context,
                            Some(conn_id),
                            NetworkError::HandlerError(format!("on_connect: {}", e)),
                        );

                        // Remove the connection from the map and deregister from the event loop.
                        if let Some(conn) = self.connections.remove(&conn_id) {
                            let mut stream = conn.val().stream.lock();
                            let _ = event_loop.deregister(&mut *stream, conn.val().token);
                        }
                        self.connection_counter.fetch_sub(1, Ordering::SeqCst);
                        continue;
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    break;
                }
                Err(e) => {
                    self.handler
                        .on_error(&self.context, None, NetworkError::Accept(Box::new(e)));
                    break;
                }
            }
        }
    }
}

/// Handler for individual TCP connections
struct TcpConnectionHandler<H: NetworkHandler> {
    conn_id: ConnectionId,
    stream: Arc<Mutex<TcpStream>>,
    connections: Arc<LockfreeMap<ConnectionId, TcpConnection>>,
    handler: Arc<H>,
    buffer_pool: ObjectPool<Vec<u8>>,
    event_loop: Weak<EventLoop>,
    connection_counter: Arc<AtomicUsize>,
    context: Arc<ServerContext>,
    drive: ReadDrive,
}

unsafe impl<H: NetworkHandler> Send for TcpConnectionHandler<H> {}
unsafe impl<H: NetworkHandler> Sync for TcpConnectionHandler<H> {}

/// Registered with the loop; shared so a capped read can re-post itself.
struct ConnectionEvents<H: NetworkHandler>(Arc<TcpConnectionHandler<H>>);

impl<H: NetworkHandler> EventHandler for ConnectionEvents<H> {
    fn handle_event(&self, event: &Event) {
        let conn = &self.0;

        if event.is_readable() {
            TcpConnectionHandler::handle_read(conn);
        }

        if event.is_writable() {
            if let Err(e) = conn.handler.on_writable(&conn.context, conn.conn_id) {
                conn.handler.on_error(
                    &conn.context,
                    Some(conn.conn_id),
                    NetworkError::HandlerError(format!("on_writable: {}", e)),
                );
            }
        }
    }
}

impl<H: NetworkHandler> TcpConnectionHandler<H> {
    fn handle_read(this: &Arc<Self>) {
        if !this.drive.begin() {
            return;
        }
        loop {
            match this.read_pass() {
                ReadPass::Drained => {
                    if !this.drive.release() {
                        return;
                    }
                }
                ReadPass::Capped => {
                    this.drive.yield_now();
                    let Some(event_loop) = this.event_loop.upgrade() else {
                        return;
                    };
                    let conn = Arc::clone(this);
                    // A failed wake-up still leaves the closure queued for the next poll.
                    let _ = event_loop.post(move || Self::handle_read(&conn));
                    return;
                }
                ReadPass::Eof | ReadPass::Stopped => {
                    this.disconnect();
                    return;
                }
                ReadPass::Failed(e) => {
                    this.handler.on_error(
                        &this.context,
                        Some(this.conn_id),
                        NetworkError::Io(Box::new(e)),
                    );
                    this.disconnect();
                    return;
                }
            }
        }
    }

    fn read_pass(&self) -> ReadPass {
        let mut buffer: PooledObject<Vec<u8>> = self.buffer_pool.acquire();
        read_pass(
            buffer.as_mut(),
            |buf| Some(self.stream.lock().read(buf)),
            |data| match self.handler.on_data(&self.context, self.conn_id, data) {
                Ok(()) => true,
                Err(e) => {
                    self.handler.on_error(
                        &self.context,
                        Some(self.conn_id),
                        NetworkError::HandlerError(format!("on_data: {}", e)),
                    );
                    false
                }
            },
        )
    }

    fn disconnect(&self) {
        if let Some(conn) = self.connections.remove(&self.conn_id) {
            self.connection_counter.fetch_sub(1, Ordering::SeqCst);
            if let Some(event_loop) = self.event_loop.upgrade() {
                let mut stream = conn.val().stream.lock();
                let _ = event_loop.deregister(&mut *stream, conn.val().token);
            }

            if let Err(e) = self.handler.on_disconnect(&self.context, self.conn_id) {
                self.handler.on_error(
                    &self.context,
                    Some(self.conn_id),
                    NetworkError::HandlerError(format!("on_disconnect: {}", e)),
                );
            }

            let _ = self
                .handler
                .on_event(&self.context, NetworkEvent::ConnectionClosed(self.conn_id));
        }
    }
}

/// High-level TCP client
#[derive(Clone)]
pub struct TcpClient<H: NetworkHandler> {
    stream: Arc<Mutex<Option<TcpStream>>>,
    handler: Arc<H>,
    buffer_pool: ObjectPool<Vec<u8>>,
    conn_id: ConnectionId,
    context: Arc<ServerContext>,
}

impl<H: NetworkHandler> TcpClient<H> {
    pub fn connect(addr: SocketAddr, handler: H) -> Result<Self> {
        let stream = TcpStream::connect(addr)?;

        Ok(Self {
            stream: Arc::new(Mutex::new(Some(stream))),
            handler: Arc::new(handler),
            buffer_pool: ObjectPool::new(5, || vec![0; 8192]),
            conn_id: ConnectionId::new(1),
            context: Arc::new(ServerContext {
                server: RwLock::new(None),
                event_loop: RwLock::new(None),
            }),
        })
    }

    pub fn start(&mut self, event_loop: &Arc<EventLoop>, token: Token) -> Result<()> {
        *self.context.event_loop.write().unwrap() = Some(event_loop.clone());
        *self.context.server.write().unwrap() = None;

        let handler = ClientEvents(Arc::new(TcpClientHandler {
            conn_id: self.conn_id,
            stream: self.stream.clone(),
            handler: self.handler.clone(),
            buffer_pool: self.buffer_pool.clone(),
            event_loop: Arc::downgrade(event_loop),
            context: self.context.clone(),
            drive: ReadDrive::default(),
        }));

        let mut stream_guard = self.stream.lock();
        let stream = stream_guard
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "TCP stream is None"))?;

        event_loop.register(
            stream,
            token,
            Interest::READABLE | Interest::WRITABLE,
            handler,
        )?;

        self.handler.on_connect(&self.context, self.conn_id)?;

        Ok(())
    }

    pub fn send(&self, data: &[u8]) -> Result<()> {
        let mut stream_guard = self.stream.lock();
        if let Some(stream) = stream_guard.as_mut() {
            stream.write_all(data)?;
        }
        Ok(())
    }

    pub fn disconnect(&self) -> Result<()> {
        *self.stream.lock() = None;
        self.handler.on_disconnect(&self.context, self.conn_id)?;
        Ok(())
    }
}

struct TcpClientHandler<H: NetworkHandler> {
    conn_id: ConnectionId,
    stream: Arc<Mutex<Option<TcpStream>>>,
    handler: Arc<H>,
    buffer_pool: ObjectPool<Vec<u8>>,
    event_loop: Weak<EventLoop>,
    context: Arc<ServerContext>,
    drive: ReadDrive,
}

/// Registered with the loop; shared so a capped read can re-post itself.
struct ClientEvents<H: NetworkHandler>(Arc<TcpClientHandler<H>>);

impl<H: NetworkHandler> EventHandler for ClientEvents<H> {
    fn handle_event(&self, event: &Event) {
        let client = &self.0;
        if event.is_readable() {
            TcpClientHandler::handle_read(client);
        }
        if event.is_writable() {
            if let Err(e) = client.handler.on_writable(&client.context, client.conn_id) {
                client.handler.on_error(
                    &client.context,
                    Some(client.conn_id),
                    NetworkError::HandlerError(format!("on_writable: {}", e)),
                );
            }
        }
    }
}

impl<H: NetworkHandler> TcpClientHandler<H> {
    fn handle_read(this: &Arc<Self>) {
        if !this.drive.begin() {
            return;
        }
        loop {
            match this.read_pass() {
                ReadPass::Drained => {
                    if !this.drive.release() {
                        return;
                    }
                }
                ReadPass::Capped => {
                    this.drive.yield_now();
                    let Some(event_loop) = this.event_loop.upgrade() else {
                        return;
                    };
                    let client = Arc::clone(this);
                    // A failed wake-up still leaves the closure queued for the next poll.
                    let _ = event_loop.post(move || Self::handle_read(&client));
                    return;
                }
                ReadPass::Eof => {
                    let _ = this
                        .handler
                        .on_event(&this.context, NetworkEvent::ConnectionClosed(this.conn_id));
                    this.disconnect();
                    return;
                }
                ReadPass::Stopped => {
                    this.disconnect();
                    return;
                }
                ReadPass::Failed(e) => {
                    this.handler.on_error(
                        &this.context,
                        Some(this.conn_id),
                        NetworkError::Io(Box::new(e)),
                    );
                    this.disconnect();
                    return;
                }
            }
        }
    }

    fn read_pass(&self) -> ReadPass {
        let mut buffer: PooledObject<Vec<u8>> = self.buffer_pool.acquire();
        read_pass(
            buffer.as_mut(),
            |buf| self.stream.lock().as_mut().map(|stream| stream.read(buf)),
            |data| match self.handler.on_data(&self.context, self.conn_id, data) {
                Ok(()) => true,
                Err(e) => {
                    self.handler.on_error(
                        &self.context,
                        Some(self.conn_id),
                        NetworkError::HandlerError(format!("on_data: {}", e)),
                    );
                    false
                }
            },
        )
    }

    fn disconnect(&self) {
        let mut stream_guard = self.stream.lock();

        let Some(mut stream) = stream_guard.take() else {
            return;
        };
        if let Some(event_loop) = self.event_loop.upgrade() {
            let _ = event_loop.deregister(&mut stream, Token(self.conn_id.as_u64() as usize));
        }
        drop(stream);
        drop(stream_guard);

        if let Err(e) = self.handler.on_disconnect(&self.context, self.conn_id) {
            self.handler.on_error(
                &self.context,
                Some(self.conn_id),
                NetworkError::HandlerError(format!("on_disconnect: {}", e)),
            );
        }
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

        let mut client = TcpClient::connect(server_addr, client_handler).unwrap();
        client.start(&event_loop, Token(2)).unwrap();

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
}
