//! The connection core shared by [`TcpServer`](super::TcpServer) and
//! [`TcpClient`](super::TcpClient): read drain, write queue and close.
//!
//! Lock order, where more than one is held: `write` before `stream`. No lock is held while
//! a handler callback runs, and `stream` is only held across non-blocking syscalls.

use super::{ConnectionId, NetworkHandler, ServerContext};
use crate::errors::{NetworkError, NetworkEvent};
use lock_freedom::map::Map as LockfreeMap;
use mill_io::{EventHandler, EventLoop, ObjectPool, PooledObject, TimerId};
use mio::event::Event;
use mio::net::TcpStream;
use mio::Token;
use parking_lot::{Mutex, RwLock};
use std::collections::VecDeque;
use std::fmt;
use std::io::{self, IoSlice, Read, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

/// Most bytes one readable event reads before the rest is re-posted to the loop.
pub const MAX_READ_PER_EVENT: usize = 64 * 1024;

const MAX_WRITE_SLICES: usize = 64;

static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

/// Ids come from one process-wide counter, so they never repeat on a loop.
pub(crate) fn next_conn_id() -> ConnectionId {
    ConnectionId(NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed))
}

/// Why [`ConnSender::send`] did not queue a frame.
#[non_exhaustive]
#[derive(Debug)]
pub enum SendError {
    /// The queue is at or above its high watermark; the frame was not queued.
    /// [`NetworkHandler::on_drained`] fires once the queue falls to the low watermark.
    Backpressure,
    /// The connection is closed, failed to connect, or was never established.
    Closed,
    /// Writing failed; the connection has been closed.
    Io(io::Error),
}

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendError::Backpressure => write!(f, "send queue is above its high watermark"),
            SendError::Closed => write!(f, "connection is closed"),
            SendError::Io(e) => write!(f, "write failed: {}", e),
        }
    }
}

impl std::error::Error for SendError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SendError::Io(e) => Some(e),
            _ => None,
        }
    }
}

/// A handle that sends to one connection from any thread: handlers, timers, compute tasks.
///
/// Cloning is cheap. A frame is queued whole or not at all, and frames from different
/// threads never interleave on the wire.
#[derive(Clone)]
pub struct ConnSender {
    conn: Arc<dyn ConnOps>,
}

impl ConnSender {
    pub(crate) fn new(conn: Arc<dyn ConnOps>) -> Self {
        Self { conn }
    }

    /// Queues `frame` and writes as much as the socket takes now.
    ///
    /// Accepted while the queued bytes are below the high watermark, even if `frame` takes
    /// the queue past it. An empty frame returns `Ok(())` and writes nothing.
    pub fn send(&self, frame: &[u8]) -> Result<(), SendError> {
        self.conn.send(frame)
    }

    /// Discards queued data, closes the socket and fires `on_disconnect` once.
    /// Callable from any thread; later calls do nothing.
    pub fn close(&self) {
        self.conn.close()
    }

    /// Stops reading from the socket once the chunk being delivered is done, so the
    /// peer is pushed back by TCP flow control. EOF and read errors go unseen until
    /// [`resume_reads`](Self::resume_reads).
    pub fn pause_reads(&self) {
        self.conn.pause_reads()
    }

    /// Undoes [`pause_reads`](Self::pause_reads); bytes that arrived meanwhile are read
    /// on the loop.
    pub fn resume_reads(&self) {
        self.conn.resume_reads()
    }

    /// The connection this sender writes to.
    pub fn id(&self) -> ConnectionId {
        self.conn.id()
    }
}

impl fmt::Debug for ConnSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnSender")
            .field("id", &self.conn.id())
            .finish()
    }
}

pub(crate) trait ConnOps: Send + Sync {
    fn send(&self, frame: &[u8]) -> Result<(), SendError>;
    fn close(&self);
    fn id(&self) -> ConnectionId;
    fn pause_reads(&self);
    fn resume_reads(&self);
}

/// What a [`ServerContext`] reaches through its weak reference.
pub(crate) trait ContextOps: Send + Sync {
    fn sender(&self, conn: ConnectionId) -> Option<ConnSender>;
    fn broadcast(&self, data: &[u8]);
}

/// State shared by a server's (or a client's) connections.
pub(crate) struct Shared<H: NetworkHandler> {
    pub(crate) handler: H,
    pub(crate) ctx: ServerContext,
    pub(crate) conns: LockfreeMap<ConnectionId, Arc<Conn<H>>>,
    pub(crate) count: AtomicUsize,
    /// Set by `TcpServer::stop`; no connection may be added after it.
    pub(crate) stopped: AtomicBool,
    pub(crate) buffer_pool: ObjectPool<Vec<u8>>,
    pub(crate) event_loop: OnceLock<Weak<EventLoop>>,
}

impl<H: NetworkHandler> Shared<H> {
    pub(crate) fn new(handler: H, buffer_pool: ObjectPool<Vec<u8>>) -> Arc<Self> {
        Arc::new_cyclic(|weak: &Weak<Self>| {
            let ops: Weak<dyn ContextOps> = weak.clone();
            Shared {
                handler,
                ctx: ServerContext { ops },
                conns: LockfreeMap::new(),
                count: AtomicUsize::new(0),
                stopped: AtomicBool::new(false),
                buffer_pool,
                event_loop: OnceLock::new(),
            }
        })
    }

    pub(crate) fn event_loop(&self) -> Option<Arc<EventLoop>> {
        self.event_loop.get().and_then(Weak::upgrade)
    }

    /// Binds to `event_loop`; fails if already bound to another loop.
    pub(crate) fn bind(&self, event_loop: &Arc<EventLoop>) -> io::Result<()> {
        let bound = self.event_loop.get_or_init(|| Arc::downgrade(event_loop));
        if Weak::ptr_eq(bound, &Arc::downgrade(event_loop)) {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "already started on another event loop",
            ))
        }
    }

    pub(crate) fn report(&self, conn: Option<ConnectionId>, error: NetworkError) {
        self.handler.on_error(&self.ctx, conn, error);
    }
}

impl<H: NetworkHandler> ContextOps for Shared<H> {
    fn sender(&self, conn: ConnectionId) -> Option<ConnSender> {
        let conn: Arc<dyn ConnOps> = self.conns.get(&conn)?.val().clone();
        Some(ConnSender::new(conn))
    }

    fn broadcast(&self, data: &[u8]) {
        for entry in self.conns.iter() {
            if let Err(e) = ConnOps::send(&**entry.val(), data) {
                self.report(Some(*entry.key()), NetworkError::Send(e));
            }
        }
    }
}

/// One connection's outbound bytes, oldest first.
struct WriteState {
    queue: VecDeque<Vec<u8>>,
    /// Bytes of `queue.front()` already written.
    offset: usize,
    /// Bytes queued and not yet written.
    queued: usize,
    /// False until the socket may be written to.
    can_write: bool,
    /// Set when a send was refused; cleared when `on_drained` is due.
    backpressured: bool,
}

impl WriteState {
    fn push(&mut self, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        self.queued += bytes.len();
        self.queue.push_back(bytes.to_vec());
    }

    fn clear(&mut self) {
        self.queue.clear();
        self.offset = 0;
        self.queued = 0;
        self.can_write = false;
    }

    fn advance(&mut self, mut written: usize) {
        self.queued -= written;
        while written > 0 {
            let Some(front) = self.queue.front() else {
                return;
            };
            let left = front.len() - self.offset;
            if written < left {
                self.offset += written;
                return;
            }
            written -= left;
            self.offset = 0;
            self.queue.pop_front();
        }
    }

    /// Writes queued bytes until the queue is empty or the socket would block.
    fn flush(&mut self, mut stream: &TcpStream) -> io::Result<()> {
        while !self.queue.is_empty() {
            let mut slices = [IoSlice::new(&[]); MAX_WRITE_SLICES];
            let mut count = 0;
            for (i, chunk) in self.queue.iter().take(MAX_WRITE_SLICES).enumerate() {
                let start = if i == 0 { self.offset } else { 0 };
                slices[i] = IoSlice::new(&chunk[start..]);
                count += 1;
            }
            match stream.write_vectored(&slices[..count]) {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(n) => self.advance(n),
                Err(e) if is_would_block(&e) => return Ok(()),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// True once if a refused send is followed by the queue falling to `low`.
    fn take_drained(&mut self, low: usize) -> bool {
        if self.backpressured && self.queued <= low {
            self.backpressured = false;
            true
        } else {
            false
        }
    }
}

fn is_would_block(e: &io::Error) -> bool {
    e.kind() == io::ErrorKind::WouldBlock
}

/// What one read pass ended on.
enum ReadPass {
    /// The socket returned WouldBlock.
    Drained,
    /// `MAX_READ_PER_EVENT` bytes were read; the socket may hold more.
    Capped,
    /// The peer closed its side.
    Eof,
    /// The connection is closed or the handler failed.
    Stopped,
    /// Reads are paused.
    Paused,
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

/// Closes the connection without callbacks if a handler panics during a read pass,
/// so the connection is not left with its reader role taken forever.
struct CloseOnUnwind<'a, H: NetworkHandler>(&'a Conn<H>);

impl<H: NetworkHandler> Drop for CloseOnUnwind<'_, H> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.shutdown(false);
        }
    }
}

/// Watermarks for one connection's outbound queue.
#[derive(Clone, Copy)]
pub(crate) struct Watermarks {
    pub(crate) high: usize,
    pub(crate) low: usize,
}

pub(crate) struct Conn<H: NetworkHandler> {
    id: ConnectionId,
    token: AtomicUsize,
    stream: RwLock<Option<TcpStream>>,
    /// Set and read under the `stream` write lock.
    registered: AtomicBool,
    write: Mutex<WriteState>,
    marks: Watermarks,
    drive: ReadDrive,
    reads_paused: AtomicBool,
    closed: AtomicBool,
    /// `on_connect` has run, so `on_disconnect` is owed.
    connected: AtomicBool,
    /// An outbound connect that has not completed yet.
    connecting: AtomicBool,
    connect_timer: Mutex<Option<TimerId>>,
    connect_opts: Mutex<Option<ConnectOpts>>,
    shared: Weak<Shared<H>>,
    me: Weak<Self>,
}

impl<H: NetworkHandler> Conn<H> {
    pub(crate) fn new(
        id: ConnectionId,
        stream: TcpStream,
        shared: Weak<Shared<H>>,
        marks: Watermarks,
        connecting: bool,
    ) -> Arc<Self> {
        Arc::new_cyclic(|me| Self {
            id,
            token: AtomicUsize::new(0),
            stream: RwLock::new(Some(stream)),
            registered: AtomicBool::new(false),
            write: Mutex::new(WriteState {
                queue: VecDeque::new(),
                offset: 0,
                queued: 0,
                can_write: !connecting,
                backpressured: false,
            }),
            marks,
            drive: ReadDrive::default(),
            reads_paused: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            connected: AtomicBool::new(false),
            connecting: AtomicBool::new(connecting),
            connect_timer: Mutex::new(None),
            connect_opts: Mutex::new(None),
            shared,
            me: me.clone(),
        })
    }

    pub(crate) fn id(&self) -> ConnectionId {
        self.id
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    pub(crate) fn sender(this: &Arc<Self>) -> ConnSender {
        let conn: Arc<dyn ConnOps> = this.clone();
        ConnSender::new(conn)
    }

    /// Registers the socket under `token`; does nothing once closed.
    pub(crate) fn register(
        this: &Arc<Self>,
        event_loop: &EventLoop,
        token: Token,
        interest: mio::Interest,
    ) -> mill_io::error::Result<()> {
        let mut stream = this.stream.write();
        let Some(socket) = stream.as_mut() else {
            return Ok(());
        };
        this.token.store(token.0, Ordering::SeqCst);
        event_loop.register(socket, token, interest, ConnEvents(Arc::clone(this)))?;
        this.registered.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Runs `on_connect`; closes without `on_disconnect` if it fails.
    pub(crate) fn connect_handler(&self, shared: &Shared<H>) -> bool {
        self.connected.store(true, Ordering::SeqCst);
        match shared.handler.on_connect(&shared.ctx, self.id) {
            Ok(()) => true,
            Err(e) => {
                shared.report(
                    Some(self.id),
                    NetworkError::HandlerError(format!("on_connect: {}", e)),
                );
                self.connected.store(false, Ordering::SeqCst);
                self.shutdown(true);
                false
            }
        }
    }

    /// Closes once: discards queued data, deregisters, closes the socket and, if `notify`
    /// and `on_connect` ran, fires `on_disconnect`.
    pub(crate) fn shutdown(&self, notify: bool) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        self.write.lock().clear();

        let shared = self.shared.upgrade();
        if let Some(event_loop) = shared.as_ref().and_then(|s| s.event_loop()) {
            self.cancel_connect_timer(&event_loop);
        }
        let socket = {
            let mut stream = self.stream.write();
            let socket = stream.take();
            if let (Some(mut socket), true) = (socket, self.registered.load(Ordering::SeqCst)) {
                if let Some(event_loop) = shared.as_ref().and_then(|s| s.event_loop()) {
                    let token = Token(self.token.load(Ordering::SeqCst));
                    let _ = event_loop.deregister(&mut socket, token);
                }
                Some(socket)
            } else {
                None
            }
        };
        drop(socket);

        let Some(shared) = shared else {
            return;
        };
        if shared.conns.remove(&self.id).is_some() {
            shared.count.fetch_sub(1, Ordering::SeqCst);
        }
        if notify && self.connected.load(Ordering::SeqCst) {
            if let Err(e) = shared.handler.on_disconnect(&shared.ctx, self.id) {
                shared.report(
                    Some(self.id),
                    NetworkError::HandlerError(format!("on_disconnect: {}", e)),
                );
            }
            let _ = shared
                .handler
                .on_event(&shared.ctx, NetworkEvent::ConnectionClosed(self.id));
        }
    }

    /// Starts the connect deadline. Call before registering, so a connect that completes
    /// at once finds the timer to cancel.
    pub(crate) fn arm_connect_timer(this: &Arc<Self>, event_loop: &EventLoop, timeout: Duration) {
        let conn = Arc::downgrade(this);
        let id = event_loop.schedule_once(timeout, move || {
            if let Some(conn) = conn.upgrade() {
                conn.connect_failed(io::ErrorKind::TimedOut.into());
            }
        });
        *this.connect_timer.lock() = Some(id);
    }

    fn cancel_connect_timer(&self, event_loop: &EventLoop) {
        if let Some(id) = self.connect_timer.lock().take() {
            event_loop.cancel_timer(id);
        }
    }

    /// Ends a pending connect with `error`: `on_error`, and neither `on_connect` nor
    /// `on_disconnect`.
    fn connect_failed(&self, error: io::Error) {
        if self
            .connecting
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        // Closed before reporting, so a handler that sees the error gets `Closed` from
        // its next send.
        self.shutdown(false);
        if let Some(shared) = self.shared.upgrade() {
            shared.report(Some(self.id), NetworkError::Connect(Box::new(error)));
        }
    }

    /// Checks a pending connect on a readiness event. Under edge triggering the first
    /// writable (or error) event is the only signal that the handshake finished.
    fn poll_connect(this: &Arc<Self>, event: &Event) {
        // kqueue reports a failed connect as EV_EOF, and `peer_addr` then says
        // NotConnected just as it does mid-handshake, so the event decides.
        let closed = event.is_error() || event.is_read_closed() || event.is_write_closed();
        let outcome = this.with_stream(|stream| match stream.take_error() {
            Ok(Some(e)) | Err(e) => Err(e),
            Ok(None) => match stream.peer_addr() {
                Ok(_) => Ok(true),
                Err(e) if e.kind() == io::ErrorKind::NotConnected && closed => {
                    Err(connect_error(stream))
                }
                Err(e) if e.kind() == io::ErrorKind::NotConnected => Ok(false),
                Err(e) => Err(e),
            },
        });
        match outcome {
            None | Some(Ok(false)) => {}
            Some(Err(e)) => this.connect_failed(e),
            Some(Ok(true)) => Self::connected(this),
        }
    }

    fn connected(this: &Arc<Self>) {
        if this
            .connecting
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        let Some(shared) = this.shared.upgrade() else {
            return;
        };
        if let Some(event_loop) = shared.event_loop() {
            this.cancel_connect_timer(&event_loop);
        }
        this.apply_connect_opts(&shared);
        this.write.lock().can_write = true;
        if !this.connect_handler(&shared) {
            return;
        }
        Self::flush(this, true);
        Self::handle_read(this);
    }

    /// Socket options for a client, applied once its connect has completed: macOS fails
    /// them with EINVAL on a socket whose connect was already refused.
    pub(crate) fn set_connect_opts(&self, opts: ConnectOpts) {
        *self.connect_opts.lock() = Some(opts);
    }

    fn apply_connect_opts(&self, shared: &Shared<H>) {
        let Some(opts) = self.connect_opts.lock().take() else {
            return;
        };
        if let Err(e) = self.set_nodelay(opts.nodelay) {
            shared.report(
                Some(self.id),
                NetworkError::Configuration(format!("Failed to set TCP_NODELAY: {}", e)),
            );
        }
        if let Some(idle) = opts.keep_alive {
            if let Err(e) = self.set_keepalive(idle) {
                shared.report(
                    Some(self.id),
                    NetworkError::Configuration(format!("Failed to set SO_KEEPALIVE: {}", e)),
                );
            }
        }
    }

    fn fail(&self, error: io::Error) {
        if let Some(shared) = self.shared.upgrade() {
            shared.report(Some(self.id), NetworkError::Io(Box::new(error)));
        }
        self.shutdown(true);
    }

    pub(crate) fn set_keepalive(&self, idle: Duration) -> io::Result<()> {
        self.with_stream(|stream| super::sockopt::set_keepalive(stream, Some(idle)))
            .unwrap_or(Ok(()))
    }

    pub(crate) fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        self.with_stream(|stream| stream.set_nodelay(nodelay))
            .unwrap_or(Ok(()))
    }

    #[cfg(test)]
    pub(crate) fn nodelay(&self) -> Option<bool> {
        self.with_stream(|stream| stream.nodelay().ok()).flatten()
    }

    fn with_stream<T>(&self, f: impl FnOnce(&TcpStream) -> T) -> Option<T> {
        self.stream.read().as_ref().map(f)
    }

    /// Writes what the queue holds. `on_drained` runs here if `direct`, else on the loop.
    fn flush(this: &Arc<Self>, direct: bool) {
        let (result, drained) = {
            let mut write = this.write.lock();
            if !write.can_write {
                return;
            }
            let result = this
                .with_stream(|stream| write.flush(stream))
                .unwrap_or(Ok(()));
            (result, write.take_drained(this.marks.low))
        };
        if let Err(e) = result {
            this.fail(e);
            return;
        }
        if drained {
            Self::drained(this, direct);
        }
    }

    fn drained(this: &Arc<Self>, direct: bool) {
        let Some(shared) = this.shared.upgrade() else {
            return;
        };
        if direct {
            this.run_drained(&shared);
        } else if let Some(event_loop) = shared.event_loop() {
            let conn = Arc::clone(this);
            // A failed wake-up still leaves the closure queued for the next poll.
            let _ = event_loop.post(move || {
                if let Some(shared) = conn.shared.upgrade() {
                    conn.run_drained(&shared);
                }
            });
        }
    }

    fn run_drained(&self, shared: &Shared<H>) {
        if self.is_closed() {
            return;
        }
        if let Err(e) = shared.handler.on_drained(&shared.ctx, self.id) {
            shared.report(
                Some(self.id),
                NetworkError::HandlerError(format!("on_drained: {}", e)),
            );
        }
    }

    fn send_frame(this: &Arc<Self>, frame: &[u8]) -> Result<(), SendError> {
        if this.is_closed() {
            return Err(SendError::Closed);
        }
        if frame.is_empty() {
            return Ok(());
        }
        let (result, drained) = {
            let mut write = this.write.lock();
            // `shutdown` sets `closed` before it takes this lock to clear the queue.
            if this.is_closed() {
                return Err(SendError::Closed);
            }
            if write.queued >= this.marks.high {
                write.backpressured = true;
                return Err(SendError::Backpressure);
            }
            let result = if write.can_write && write.queue.is_empty() {
                this.with_stream(|stream| write_now(stream, frame))
                    .unwrap_or(Ok(0))
                    .map(|written| write.push(&frame[written..]))
            } else {
                write.push(frame);
                if write.can_write {
                    this.with_stream(|stream| write.flush(stream))
                        .unwrap_or(Ok(()))
                } else {
                    Ok(())
                }
            };
            (result, write.take_drained(this.marks.low))
        };
        match result {
            Ok(()) => {
                if drained {
                    Self::drained(this, false);
                }
                Ok(())
            }
            Err(e) => {
                let returned = io::Error::new(e.kind(), e.to_string());
                this.fail(e);
                Err(SendError::Io(returned))
            }
        }
    }

    fn handle_read(this: &Arc<Self>) {
        if !this.drive.begin() {
            return;
        }
        let _guard = CloseOnUnwind(this);
        loop {
            match this.read_pass() {
                // A pause leaves the reader role through `release` so that an edge recorded
                // by a racing `resume_reads` is not lost.
                ReadPass::Drained | ReadPass::Paused => {
                    if !this.drive.release() {
                        return;
                    }
                }
                ReadPass::Capped => {
                    this.drive.yield_now();
                    let Some(event_loop) = this.shared.upgrade().and_then(|s| s.event_loop())
                    else {
                        return;
                    };
                    let conn = Arc::clone(this);
                    // A failed wake-up still leaves the closure queued for the next poll.
                    let _ = event_loop.post(move || Self::handle_read(&conn));
                    return;
                }
                ReadPass::Eof | ReadPass::Stopped => {
                    this.shutdown(true);
                    return;
                }
                ReadPass::Failed(e) => {
                    this.fail(e);
                    return;
                }
            }
        }
    }

    /// Reads until WouldBlock, EOF or `MAX_READ_PER_EVENT` bytes, handing each chunk to
    /// `on_data`.
    ///
    /// Edge-triggered polling raises no new event for bytes left in the socket, so a pass
    /// that stops early ends in `Capped`, and the caller must re-post the rest.
    fn read_pass(&self) -> ReadPass {
        let Some(shared) = self.shared.upgrade() else {
            return ReadPass::Stopped;
        };
        let mut buffer: PooledObject<Vec<u8>> = shared.buffer_pool.acquire();
        let buffer = buffer.as_mut();
        let mut total = 0;
        loop {
            if self.reads_paused.load(Ordering::SeqCst) {
                return ReadPass::Paused;
            }
            let want = buffer.len().min(MAX_READ_PER_EVENT - total);
            let read = self.with_stream(|mut stream| stream.read(&mut buffer[..want]));
            match read {
                None => return ReadPass::Stopped,
                Some(Ok(0)) => return ReadPass::Eof,
                Some(Ok(n)) => {
                    if let Err(e) = shared.handler.on_data(&shared.ctx, self.id, &buffer[..n]) {
                        shared.report(
                            Some(self.id),
                            NetworkError::HandlerError(format!("on_data: {}", e)),
                        );
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

    fn handle_writable(this: &Arc<Self>) {
        Self::flush(this, true);
        if this.is_closed() {
            return;
        }
        let Some(shared) = this.shared.upgrade() else {
            return;
        };
        if let Err(e) = shared.handler.on_writable(&shared.ctx, this.id) {
            shared.report(
                Some(this.id),
                NetworkError::HandlerError(format!("on_writable: {}", e)),
            );
        }
    }
}

/// Client socket options that wait for the connect to complete.
pub(crate) struct ConnectOpts {
    pub(crate) nodelay: bool,
    pub(crate) keep_alive: Option<Duration>,
}

/// The error behind a connect that ended in EOF without `SO_ERROR` set.
fn connect_error(mut stream: &TcpStream) -> io::Error {
    match stream.read(&mut [0u8; 1]) {
        Err(e) if e.kind() != io::ErrorKind::WouldBlock => e,
        _ => io::ErrorKind::ConnectionRefused.into(),
    }
}

/// Writes as much of `frame` as the socket takes now; returns how much that was.
fn write_now(mut stream: &TcpStream, frame: &[u8]) -> io::Result<usize> {
    let mut written = 0;
    while written < frame.len() {
        match stream.write(&frame[written..]) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(n) => written += n,
            Err(e) if is_would_block(&e) => break,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(written)
}

impl<H: NetworkHandler> ConnOps for Conn<H> {
    fn send(&self, frame: &[u8]) -> Result<(), SendError> {
        match self.me.upgrade() {
            Some(this) => Self::send_frame(&this, frame),
            None => Err(SendError::Closed),
        }
    }

    fn close(&self) {
        self.shutdown(true);
    }

    fn id(&self) -> ConnectionId {
        self.id
    }

    fn pause_reads(&self) {
        self.reads_paused.store(true, Ordering::SeqCst);
    }

    fn resume_reads(&self) {
        if !self.reads_paused.swap(false, Ordering::SeqCst) {
            return;
        }
        // Edge triggering raises no new event for bytes that arrived while paused.
        let (Some(this), Some(event_loop)) = (
            self.me.upgrade(),
            self.shared.upgrade().and_then(|s| s.event_loop()),
        ) else {
            return;
        };
        // A failed wake-up still leaves the closure queued for the next poll.
        let _ = event_loop.post(move || Self::handle_read(&this));
    }
}

/// Registered with the loop for one connection.
struct ConnEvents<H: NetworkHandler>(Arc<Conn<H>>);

impl<H: NetworkHandler> EventHandler for ConnEvents<H> {
    fn handle_event(&self, event: &Event) {
        if self.0.connecting.load(Ordering::SeqCst) {
            Conn::poll_connect(&self.0, event);
            return;
        }
        if event.is_readable() {
            Conn::handle_read(&self.0);
        }
        if event.is_writable() {
            Conn::handle_writable(&self.0);
        }
    }
}
