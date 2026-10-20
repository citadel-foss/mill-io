//! Test tools: scriptable line-delimited JSON-RPC and HTTP JSON-RPC servers, and hooks that
//! run code on a client's event loop thread.

use crate::{ChainClient, ChainError, ChainHandle};
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
    let posted = engine.post(Box::new(move || {
        match panic::catch_unwind(AssertUnwindSafe(f)) {
            Ok(result) => {
                let _ = tx.send(Ok(result));
            }
            Err(payload) => {
                let _ = tx.send(Err(panic_message(&*payload)));
                panic::resume_unwind(payload);
            }
        }
    }));
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
    let posted = engine.post(Box::new(move || {
        loop_engine.inject_events(n);
        let _ = tx.send(());
    }));
    if posted.is_ok() {
        let _ = rx.recv();
    }
}

/// One request a [`MockHttpServer`] received.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRequest {
    /// Which connection it came on, numbered from 0 in accept order.
    pub conn: usize,
    /// The request target, such as `/` or `/wallet/w`.
    pub path: String,
    /// The headers, in order, as sent.
    pub headers: Vec<(String, String)>,
    /// The body.
    pub body: Vec<u8>,
}

impl HttpRequest {
    /// The first header named `name`, ignoring case.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    /// The body as JSON.
    pub fn json(&self) -> Option<Value> {
        serde_json::from_slice(&self.body).ok()
    }

    /// The JSON-RPC method of a single request.
    pub fn method(&self) -> Option<String> {
        self.json()?.get("method")?.as_str().map(str::to_string)
    }

    /// The user and password of a Basic `Authorization` header.
    pub fn basic_auth(&self) -> Option<(String, String)> {
        use base64::Engine as _;
        let encoded = self.header("authorization")?.strip_prefix("Basic ")?;
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .ok()?;
        let decoded = String::from_utf8(decoded).ok()?;
        let (user, pass) = decoded.split_once(':')?;
        Some((user.to_string(), pass.to_string()))
    }
}

/// What a [`MockHttpServer`] answers one request with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpReply {
    /// The status code.
    pub status: u16,
    /// Extra headers. `Content-Length` or `Transfer-Encoding` is added to match the body.
    pub headers: Vec<(String, String)>,
    /// The body.
    pub body: Vec<u8>,
    /// Send the body chunked, in chunks of at most this many bytes.
    pub chunk_size: Option<usize>,
    /// Wait this long before answering. Other connections are served meanwhile.
    pub delay: Duration,
    /// Send `Connection: close` and close the connection after the reply.
    pub close: bool,
    /// Close the connection without answering.
    pub drop_connection: bool,
}

impl HttpReply {
    /// A reply with `status` and `body`, sent at once with a `Content-Length`.
    pub fn new(status: u16, body: impl Into<Vec<u8>>) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: body.into(),
            chunk_size: None,
            delay: Duration::ZERO,
            close: false,
            drop_connection: false,
        }
    }

    /// A JSON body with `status`.
    pub fn json(status: u16, body: &Value) -> Self {
        let mut reply = Self::new(status, body.to_string());
        reply
            .headers
            .push(("Content-Type".into(), "application/json".into()));
        reply
    }

    /// A 200 JSON-RPC reply to `request` carrying `result`.
    pub fn result(request: &HttpRequest, result: Value) -> Self {
        let id = request_id(request);
        Self::json(200, &json!({"result": result, "error": null, "id": id}))
    }

    /// A JSON-RPC error reply to `request`, with status 500 as Bitcoin Core sends it.
    pub fn error(request: &HttpRequest, code: i64, message: &str) -> Self {
        let id = request_id(request);
        let error = json!({"code": code, "message": message});
        Self::json(500, &json!({"result": null, "error": error, "id": id}))
    }

    /// An empty body with `status`.
    pub fn status(status: u16) -> Self {
        Self::new(status, Vec::new())
    }

    /// Closes the connection without answering.
    pub fn drop_connection() -> Self {
        Self {
            drop_connection: true,
            ..Self::status(0)
        }
    }

    /// Sends the body chunked, `size` bytes at most per chunk.
    pub fn chunked(mut self, size: usize) -> Self {
        self.chunk_size = Some(size.max(1));
        self
    }

    /// Answers after `delay`.
    pub fn after(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    /// Closes the connection after the reply, saying so in a `Connection: close` header.
    pub fn closing(mut self) -> Self {
        self.close = true;
        self
    }

    /// Adds a header.
    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut out = format!("HTTP/1.1 {} Mock\r\n", self.status).into_bytes();
        for (name, value) in &self.headers {
            out.extend_from_slice(format!("{}: {}\r\n", name, value).as_bytes());
        }
        if self.close {
            out.extend_from_slice(b"Connection: close\r\n");
        }
        match self.chunk_size {
            None => {
                let length = format!("Content-Length: {}\r\n\r\n", self.body.len());
                out.extend_from_slice(length.as_bytes());
                out.extend_from_slice(&self.body);
            }
            Some(size) => {
                out.extend_from_slice(b"Transfer-Encoding: chunked\r\n\r\n");
                for chunk in self.body.chunks(size) {
                    out.extend_from_slice(format!("{:x}\r\n", chunk.len()).as_bytes());
                    out.extend_from_slice(chunk);
                    out.extend_from_slice(b"\r\n");
                }
                out.extend_from_slice(b"0\r\n\r\n");
            }
        }
        out
    }
}

fn request_id(request: &HttpRequest) -> Value {
    request
        .json()
        .and_then(|body| body.get("id").cloned())
        .unwrap_or(Value::Null)
}

type HttpScriptFn = dyn Fn(&HttpRequest) -> HttpReply + Send + Sync;

/// Scriptable JSON-RPC-over-HTTP/1.1 server, standing in for Bitcoin Core.
///
/// It listens on 127.0.0.1, accepts any number of keep-alive connections and answers
/// the requests on each one in order. Dropping it closes every connection and joins its
/// threads.
pub struct MockHttpServer {
    addr: SocketAddr,
    shared: Arc<HttpShared>,
    acceptor: Option<JoinHandle<()>>,
}

struct HttpShared {
    script: Box<HttpScriptFn>,
    stopping: Mutex<bool>,
    stopped: Condvar,
    requests: Mutex<Vec<HttpRequest>>,
    conns: Mutex<Vec<Arc<TcpStream>>>,
    accepted: AtomicU64,
    workers: Mutex<Vec<JoinHandle<()>>>,
}

impl MockHttpServer {
    /// Starts a server on an ephemeral 127.0.0.1 port. `script` runs once per request,
    /// after the request is logged.
    pub fn start<F>(script: F) -> io::Result<Self>
    where
        F: Fn(&HttpRequest) -> HttpReply + Send + Sync + 'static,
    {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))?;
        let addr = listener.local_addr()?;
        let shared = Arc::new(HttpShared {
            script: Box::new(script),
            stopping: Mutex::new(false),
            stopped: Condvar::new(),
            requests: Mutex::new(Vec::new()),
            conns: Mutex::new(Vec::new()),
            accepted: AtomicU64::new(0),
            workers: Mutex::new(Vec::new()),
        });
        let accept_shared = Arc::clone(&shared);
        let acceptor = thread::Builder::new()
            .name("mock-http-accept".to_string())
            .spawn(move || http_accept_loop(&listener, &accept_shared))?;
        Ok(Self {
            addr,
            shared,
            acceptor: Some(acceptor),
        })
    }

    /// The address it listens on.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The `rpc_url` to reach it.
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Every request so far, in arrival order.
    pub fn requests_seen(&self) -> Vec<HttpRequest> {
        lock(&self.shared.requests).clone()
    }

    /// Connections accepted so far.
    pub fn connection_count(&self) -> usize {
        self.shared.accepted.load(Ordering::SeqCst) as usize
    }

    /// Closes every open connection, dropping replies not yet sent, and keeps listening.
    pub fn restart(&self) {
        for conn in lock(&self.shared.conns).drain(..) {
            let _ = conn.shutdown(Shutdown::Both);
        }
    }
}

impl Drop for MockHttpServer {
    fn drop(&mut self) {
        *lock(&self.shared.stopping) = true;
        self.shared.stopped.notify_all();
        let _ = TcpStream::connect(self.addr);
        if let Some(acceptor) = self.acceptor.take() {
            let _ = acceptor.join();
        }
        self.restart();
        for worker in lock(&self.shared.workers).drain(..).collect::<Vec<_>>() {
            let _ = worker.join();
        }
    }
}

impl fmt::Debug for MockHttpServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MockHttpServer")
            .field("addr", &self.addr)
            .finish_non_exhaustive()
    }
}

fn http_accept_loop(listener: &TcpListener, shared: &Arc<HttpShared>) {
    for stream in listener.incoming() {
        if *lock(&shared.stopping) {
            return;
        }
        let Ok(stream) = stream else {
            continue;
        };
        let _ = stream.set_nodelay(true);
        let stream = Arc::new(stream);
        let conn = shared.accepted.fetch_add(1, Ordering::SeqCst) as usize;
        lock(&shared.conns).push(Arc::clone(&stream));
        let worker_shared = Arc::clone(shared);
        let spawned = thread::Builder::new()
            .name("mock-http-conn".to_string())
            .spawn(move || http_serve(conn, &stream, &worker_shared));
        if let Ok(worker) = spawned {
            lock(&shared.workers).push(worker);
        }
    }
}

fn http_serve(conn: usize, stream: &Arc<TcpStream>, shared: &HttpShared) {
    let mut reader = BufReader::new(&**stream);
    while let Some(request) = read_http_request(conn, &mut reader) {
        lock(&shared.requests).push(request.clone());
        let reply = (shared.script)(&request);
        if reply.drop_connection {
            break;
        }
        if !reply.delay.is_zero() && wait_stopping(shared, reply.delay) {
            break;
        }
        if (&**stream).write_all(&reply.to_bytes()).is_err() || reply.close {
            break;
        }
    }
    let _ = stream.shutdown(Shutdown::Both);
    lock(&shared.conns).retain(|other| !Arc::ptr_eq(other, stream));
}

/// Waits `delay` or until the server stops; true if it stopped.
fn wait_stopping(shared: &HttpShared, delay: Duration) -> bool {
    let deadline = Instant::now() + delay;
    let mut stopping = lock(&shared.stopping);
    while !*stopping {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            break;
        }
        stopping = shared
            .stopped
            .wait_timeout(stopping, left)
            .unwrap_or_else(PoisonError::into_inner)
            .0;
    }
    *stopping
}

fn read_http_request(conn: usize, reader: &mut impl BufRead) -> Option<HttpRequest> {
    let mut line = String::new();
    if reader.read_line(&mut line).ok()? == 0 {
        return None;
    }
    let path = line.split_whitespace().nth(1)?.to_string();
    let mut headers = Vec::new();
    loop {
        line.clear();
        if reader.read_line(&mut line).ok()? == 0 {
            return None;
        }
        let header = line.trim_end();
        if header.is_empty() {
            break;
        }
        let (name, value) = header.split_once(':')?;
        headers.push((name.to_string(), value.trim().to_string()));
    }
    let length = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.parse().ok())
        .unwrap_or(0);
    let mut body = vec![0; length];
    reader.read_exact(&mut body).ok()?;
    Some(HttpRequest {
        conn,
        path,
        headers,
        body,
    })
}

/// Parses one HTTP response from `reads`, fed to mill-chain's parser one read at a time,
/// with the connection closing after the last. Returns its status and body.
pub fn parse_http_response<'a>(
    reads: impl IntoIterator<Item = &'a [u8]>,
    max_body_bytes: usize,
) -> Result<(u16, Vec<u8>), String> {
    let mut parser = crate::proto::http::ResponseParser::new(max_body_bytes);
    let mut responses = Vec::new();
    for read in reads {
        parser
            .push(read, &mut responses)
            .map_err(|e| e.to_string())?;
        if !responses.is_empty() {
            let response = responses.swap_remove(0);
            return Ok((response.status, response.body));
        }
    }
    match parser.finish() {
        Ok(Some(response)) => Ok((response.status, response.body)),
        Ok(None) => Err("no response".into()),
        Err(e) => Err(e.to_string()),
    }
}

/// Sends `calls` as one JSON-RPC batch through `handle`, as typed queries do, and returns
/// one result per call in request order. Bitcoin Core only.
pub fn batch_raw(
    handle: &ChainHandle,
    calls: &[(&str, Value)],
) -> Result<Vec<Result<Value, ChainError>>, ChainError> {
    handle.batch_raw(calls)
}
