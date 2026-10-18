//! Test tools: a scriptable line-delimited JSON-RPC server, and hooks that run code on a
//! client's event loop thread.

use crate::ChainClient;
use serde_json::{json, Value};
use std::cmp::Ordering as CmpOrdering;
use std::collections::BinaryHeap;
use std::fmt;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// What a [`MockServer`] does with one request.
#[derive(Debug, Clone, PartialEq)]
pub enum MockReply {
    /// Reply at once with this `result`.
    Result(Value),
    /// Reply with this `result` after the delay, serving other requests meanwhile.
    After(Duration, Value),
    /// Reply at once with a JSON-RPC error object.
    Error {
        /// The error's `code`.
        code: i64,
        /// The error's `message`.
        message: String,
    },
    /// Record the request and never answer it.
    Never,
    /// Close this connection, dropping replies still due on it.
    Close,
    /// Write this text verbatim plus `"\n"`; it may hold several lines.
    Raw(String),
}

type ScriptFn = dyn Fn(&Value) -> MockReply + Send + Sync;

/// How a [`MockServer`] answers.
#[derive(Clone)]
pub struct MockScript {
    f: Arc<ScriptFn>,
}

impl MockScript {
    /// `f` runs once per request line received, on any connection, with the parsed request.
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(&Value) -> MockReply + Send + Sync + 'static,
    {
        Self { f: Arc::new(f) }
    }
}

impl fmt::Debug for MockScript {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MockScript").finish_non_exhaustive()
    }
}

/// Scriptable line-delimited JSON-RPC server.
///
/// It listens on 127.0.0.1 and accepts any number of connections. On one connection,
/// replies go out in arrival order, except [`MockReply::After`]. Dropping it closes every
/// connection and joins its threads.
pub struct MockServer {
    addr: SocketAddr,
    shared: Arc<ServerShared>,
    acceptor: Option<JoinHandle<()>>,
    scheduler: Option<JoinHandle<()>>,
}

struct ServerShared {
    script: MockScript,
    stopping: AtomicBool,
    requests: Mutex<Vec<Value>>,
    connection_times: Mutex<Vec<Instant>>,
    conns: Mutex<Vec<Arc<MockConn>>>,
    readers: Mutex<Vec<JoinHandle<()>>>,
    delayed: Mutex<BinaryHeap<Delayed>>,
    delayed_ready: Condvar,
    next_seq: AtomicU64,
}

struct MockConn {
    stream: TcpStream,
    write: Mutex<()>,
    closed: AtomicBool,
}

/// A reply due at `due`; the heap pops the earliest first.
struct Delayed {
    due: Instant,
    seq: u64,
    conn: Arc<MockConn>,
    bytes: Vec<u8>,
}

impl PartialEq for Delayed {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == CmpOrdering::Equal
    }
}

impl Eq for Delayed {}

impl PartialOrd for Delayed {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for Delayed {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        (other.due, other.seq).cmp(&(self.due, self.seq))
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl MockConn {
    fn send(&self, bytes: &[u8]) {
        let _write = lock(&self.write);
        if !self.closed.load(Ordering::SeqCst) {
            let _ = (&self.stream).write_all(bytes);
        }
    }

    fn close(&self) {
        let _write = lock(&self.write);
        self.closed.store(true, Ordering::SeqCst);
        let _ = self.stream.shutdown(Shutdown::Both);
    }
}

struct ConnReader(Arc<MockConn>);

impl Read for ConnReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        (&self.0.stream).read(buf)
    }
}

fn reply_line(id: &Value, body: (&str, Value)) -> Vec<u8> {
    let mut reply = json!({"jsonrpc": "2.0", "id": id});
    reply[body.0] = body.1;
    let mut bytes = reply.to_string().into_bytes();
    bytes.push(b'\n');
    bytes
}

impl MockServer {
    /// Starts a server on an ephemeral 127.0.0.1 port.
    pub fn start(script: MockScript) -> io::Result<Self> {
        Self::start_on(SocketAddr::from(([127, 0, 0, 1], 0)), script)
    }

    /// Starts a server on `addr`, for example to bring back a server at an address a
    /// client is already reconnecting to.
    pub fn start_on(addr: SocketAddr, script: MockScript) -> io::Result<Self> {
        let listener = TcpListener::bind(addr)?;
        let addr = listener.local_addr()?;
        let shared = Arc::new(ServerShared {
            script,
            stopping: AtomicBool::new(false),
            requests: Mutex::new(Vec::new()),
            connection_times: Mutex::new(Vec::new()),
            conns: Mutex::new(Vec::new()),
            readers: Mutex::new(Vec::new()),
            delayed: Mutex::new(BinaryHeap::new()),
            delayed_ready: Condvar::new(),
            next_seq: AtomicU64::new(0),
        });
        let accept_shared = Arc::clone(&shared);
        let acceptor = thread::Builder::new()
            .name("mock-server-accept".to_string())
            .spawn(move || accept_loop(&listener, &accept_shared))?;
        let delay_shared = Arc::clone(&shared);
        let scheduler = thread::Builder::new()
            .name("mock-server-delay".to_string())
            .spawn(move || delay_loop(&delay_shared))?;
        Ok(Self {
            addr,
            shared,
            acceptor: Some(acceptor),
            scheduler: Some(scheduler),
        })
    }

    /// The address it listens on.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Every parsed request line so far, in arrival order, recorded before its reply was
    /// applied.
    pub fn requests_seen(&self) -> Vec<Value> {
        lock(&self.shared.requests).clone()
    }

    /// When each connection was accepted, one entry per connection.
    pub fn connection_times(&self) -> Vec<Instant> {
        lock(&self.shared.connection_times).clone()
    }

    /// Closes every connection, dropping replies still due on them, and keeps listening
    /// on the same address.
    pub fn restart(&self) {
        let conns: Vec<_> = lock(&self.shared.conns).drain(..).collect();
        for conn in conns {
            conn.close();
        }
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.shared.stopping.store(true, Ordering::SeqCst);
        // Wakes the blocking accept so it sees `stopping`.
        let _ = TcpStream::connect(self.addr);
        if let Some(acceptor) = self.acceptor.take() {
            let _ = acceptor.join();
        }
        self.restart();
        let readers: Vec<_> = lock(&self.shared.readers).drain(..).collect();
        for reader in readers {
            let _ = reader.join();
        }
        {
            let _delayed = lock(&self.shared.delayed);
            self.shared.delayed_ready.notify_all();
        }
        if let Some(scheduler) = self.scheduler.take() {
            let _ = scheduler.join();
        }
    }
}

impl fmt::Debug for MockServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MockServer")
            .field("addr", &self.addr)
            .finish_non_exhaustive()
    }
}

fn accept_loop(listener: &TcpListener, shared: &Arc<ServerShared>) {
    for stream in listener.incoming() {
        if shared.stopping.load(Ordering::SeqCst) {
            return;
        }
        let Ok(stream) = stream else {
            continue;
        };
        lock(&shared.connection_times).push(Instant::now());
        let _ = stream.set_nodelay(true);
        let conn = Arc::new(MockConn {
            stream,
            write: Mutex::new(()),
            closed: AtomicBool::new(false),
        });
        lock(&shared.conns).push(Arc::clone(&conn));
        let reader_shared = Arc::clone(shared);
        let spawned = thread::Builder::new()
            .name("mock-server-conn".to_string())
            .spawn(move || serve(&conn, &reader_shared));
        if let Ok(reader) = spawned {
            lock(&shared.readers).push(reader);
        }
    }
}

fn serve(conn: &Arc<MockConn>, shared: &ServerShared) {
    let mut reader = BufReader::new(ConnReader(Arc::clone(conn)));
    let mut line = Vec::new();
    loop {
        line.clear();
        match reader.read_until(b'\n', &mut line) {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
        if conn.closed.load(Ordering::SeqCst) {
            break;
        }
        let Ok(request) = serde_json::from_slice::<Value>(&line) else {
            continue;
        };
        lock(&shared.requests).push(request.clone());
        let id = request.get("id").cloned().unwrap_or(Value::Null);
        match (shared.script.f)(&request) {
            MockReply::Result(result) => conn.send(&reply_line(&id, ("result", result))),
            MockReply::After(delay, result) => {
                let delayed = Delayed {
                    due: Instant::now() + delay,
                    seq: shared.next_seq.fetch_add(1, Ordering::Relaxed),
                    conn: Arc::clone(conn),
                    bytes: reply_line(&id, ("result", result)),
                };
                lock(&shared.delayed).push(delayed);
                shared.delayed_ready.notify_one();
            }
            MockReply::Error { code, message } => conn.send(&reply_line(
                &id,
                ("error", json!({"code": code, "message": message})),
            )),
            MockReply::Never => {}
            MockReply::Close => break,
            MockReply::Raw(text) => conn.send(format!("{}\n", text).as_bytes()),
        }
    }
    conn.close();
    lock(&shared.conns).retain(|other| !Arc::ptr_eq(other, conn));
}

fn delay_loop(shared: &ServerShared) {
    let mut delayed = lock(&shared.delayed);
    while !shared.stopping.load(Ordering::SeqCst) {
        let now = Instant::now();
        match delayed.peek().map(|next| next.due) {
            Some(due) if due <= now => {
                if let Some(reply) = delayed.pop() {
                    drop(delayed);
                    reply.conn.send(&reply.bytes);
                    delayed = lock(&shared.delayed);
                }
            }
            Some(due) => {
                delayed = shared
                    .delayed_ready
                    .wait_timeout(delayed, due - now)
                    .unwrap_or_else(PoisonError::into_inner)
                    .0;
            }
            None => {
                delayed = shared
                    .delayed_ready
                    .wait(delayed)
                    .unwrap_or_else(PoisonError::into_inner);
            }
        }
    }
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "closure panicked on the event loop thread".to_string()
    }
}

/// Runs `f` on `c`'s event loop thread and returns its result.
///
/// A panic in `f` is counted in mill-io's `handler_panics()`, the loop keeps running, and
/// the panic is raised again on the calling thread.
///
/// # Panics
///
/// If `f` panics, or the client is shut down before `f` runs.
pub fn on_reactor<R: Send + 'static>(c: &ChainClient, f: impl FnOnce() -> R + Send + 'static) -> R {
    let engine = c.engine();
    if engine.is_loop_thread() {
        return f();
    }
    let (tx, rx) = crossbeam_channel::bounded(1);
    let posted = engine.post(move || match panic::catch_unwind(AssertUnwindSafe(f)) {
        Ok(result) => {
            let _ = tx.send(Ok(result));
        }
        Err(payload) => {
            let _ = tx.send(Err(panic_message(&*payload)));
            panic::resume_unwind(payload);
        }
    });
    if posted.is_err() {
        panic!("on_reactor: the client is shut down");
    }
    match rx.recv() {
        Ok(Ok(result)) => result,
        Ok(Err(message)) => panic!("{}", message),
        Err(_) => panic!("on_reactor: the client shut down before the closure ran"),
    }
}

/// Emits `n` placeholder events from `c`'s event loop thread, as a backend would, and
/// returns once they are on the events channel (or replaced by a `Resync`). Does nothing
/// once the client is shut down.
pub fn inject_events(c: &ChainClient, n: usize) {
    let engine = c.engine();
    if engine.is_loop_thread() {
        engine.inject_events(n);
        return;
    }
    let (tx, rx) = crossbeam_channel::bounded(1);
    let loop_engine = Arc::clone(engine);
    let posted = engine.post(move || {
        loop_engine.inject_events(n);
        let _ = tx.send(());
    });
    if posted.is_ok() {
        let _ = rx.recv();
    }
}
