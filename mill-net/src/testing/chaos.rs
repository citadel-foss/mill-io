use parking_lot::{Condvar, Mutex};
use socket2::SockRef;
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const BUF_SIZE: usize = 64 * 1024;
const UPSTREAM_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const WRITE_POLL: Duration = Duration::from_millis(50);
const ACCEPT_BACKOFF: Duration = Duration::from_millis(10);

/// What a [`ChaosProxy`] does to the bytes it forwards; every knob is off by default.
///
/// "Client" is whoever connects to [`ChaosProxy::addr`], "upstream" the server behind it.
#[derive(Clone, Debug, Default)]
pub struct ChaosConfig {
    /// Both directions: forward in writes of at most N bytes; `Some(0)` is rejected.
    /// A reader that falls behind can still see several writes coalesced into one read.
    pub split_writes: Option<usize>,
    /// Client to upstream only: read from the client at most N bytes per second.
    pub read_rate_bps: Option<u64>,
    /// How long `read_rate_bps` lasts from accept; `None` means for the whole connection.
    pub throttle_for: Option<Duration>,
    /// Each direction on its own: stop forwarding after N bytes, keeping the sockets open.
    pub stall_after: Option<usize>,
    /// Reset both sockets (`SO_LINGER` 0, then close) once either direction forwarded N bytes.
    pub reset_after: Option<usize>,
}

/// A TCP proxy on `127.0.0.1` that forwards to `upstream` as its [`ChaosConfig`] says.
///
/// Half-closes pass through and an error on either side resets both. Dropping the proxy closes
/// every socket it opened and joins every thread it started.
pub struct ChaosProxy {
    addr: SocketAddr,
    shared: Arc<Shared>,
    acceptor: Option<JoinHandle<()>>,
}

impl ChaosProxy {
    /// Listens on a free port of `127.0.0.1` and forwards each connection to `upstream`.
    pub fn start(upstream: SocketAddr, cfg: ChaosConfig) -> io::Result<Self> {
        if cfg.split_writes == Some(0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "split_writes must be at least 1",
            ));
        }

        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let shared = Arc::new(Shared {
            upstream,
            cfg,
            connections_seen: AtomicUsize::new(0),
            registry: Mutex::new(Registry {
                stopping: false,
                conns: Vec::new(),
            }),
        });

        let acceptor = {
            let shared = Arc::clone(&shared);
            thread::Builder::new()
                .name("chaos-accept".into())
                .spawn(move || accept_loop(listener, shared))?
        };

        Ok(Self {
            addr,
            shared,
            acceptor: Some(acceptor),
        })
    }

    /// The address clients connect to instead of `upstream`.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Client connections accepted so far, including ones whose upstream connect failed.
    pub fn connections_seen(&self) -> usize {
        self.shared.connections_seen.load(Ordering::SeqCst)
    }
}

impl Drop for ChaosProxy {
    fn drop(&mut self) {
        // `stopping` is set under the registry lock, so the acceptor either registered a
        // connection before this (and it is closed below) or sees `stopping` and drops it.
        let conns = {
            let mut registry = self.shared.registry.lock();
            registry.stopping = true;
            std::mem::take(&mut registry.conns)
        };
        for (conn, _) in &conns {
            if let Some(conn) = conn.upgrade() {
                conn.close();
            }
        }

        // Without this wake-up connection the acceptor stays blocked in `accept`, so it is
        // only joined if the connection went through.
        let woken = TcpStream::connect_timeout(&self.addr, Duration::from_secs(1)).is_ok();
        if let Some(acceptor) = self.acceptor.take() {
            if woken {
                let _ = acceptor.join();
            }
        }

        for (_, handle) in conns {
            let _ = handle.join();
        }
    }
}

impl std::fmt::Debug for ChaosProxy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChaosProxy")
            .field("addr", &self.addr)
            .field("upstream", &self.shared.upstream)
            .field("cfg", &self.shared.cfg)
            .field("connections_seen", &self.connections_seen())
            .finish()
    }
}

struct Shared {
    upstream: SocketAddr,
    cfg: ChaosConfig,
    connections_seen: AtomicUsize,
    registry: Mutex<Registry>,
}

struct Registry {
    stopping: bool,
    conns: Vec<(Weak<Conn>, JoinHandle<()>)>,
}

fn accept_loop(listener: TcpListener, shared: Arc<Shared>) {
    loop {
        let accepted = listener.accept();

        let mut registry = shared.registry.lock();
        if registry.stopping {
            return;
        }
        let client = match accepted {
            Ok((client, _)) => client,
            Err(_) => {
                drop(registry);
                thread::sleep(ACCEPT_BACKOFF);
                continue;
            }
        };
        shared.connections_seen.fetch_add(1, Ordering::SeqCst);
        registry.conns.retain(|(_, handle)| !handle.is_finished());

        let conn = Arc::new(Conn::new(client));
        let spawned = {
            let shared = Arc::clone(&shared);
            let conn = Arc::clone(&conn);
            thread::Builder::new()
                .name("chaos-conn".into())
                .spawn(move || run_connection(&shared, conn))
        };
        match spawned {
            Ok(handle) => registry.conns.push((Arc::downgrade(&conn), handle)),
            Err(_) => conn.close(),
        }
    }
}

fn run_connection(shared: &Shared, conn: Arc<Conn>) {
    let upstream = match TcpStream::connect_timeout(&shared.upstream, UPSTREAM_CONNECT_TIMEOUT) {
        Ok(upstream) => upstream,
        Err(_) => {
            conn.close();
            return;
        }
    };
    configure(&upstream);
    let _ = conn.upstream.set(upstream);
    // `close` may have run before `upstream` was set and so missed shutting it down.
    if conn.is_closing() {
        conn.close();
        return;
    }

    let back = {
        let conn = Arc::clone(&conn);
        let cfg = shared.cfg.clone();
        thread::Builder::new()
            .name("chaos-pump".into())
            .spawn(move || pump(&conn, Direction::ToClient, &cfg))
    };
    match back {
        Ok(back) => {
            pump(&conn, Direction::ToUpstream, &shared.cfg);
            let _ = back.join();
        }
        Err(_) => conn.close(),
    }
}

fn configure(stream: &TcpStream) {
    let _ = stream.set_nodelay(true);
    let _ = stream.set_write_timeout(Some(WRITE_POLL));
}

/// The sockets close when the last `Arc<Conn>` is dropped, which is after both pumps return.
struct Conn {
    client: TcpStream,
    upstream: OnceLock<TcpStream>,
    accepted_at: Instant,
    closing: Mutex<bool>,
    closing_cv: Condvar,
}

impl Conn {
    fn new(client: TcpStream) -> Self {
        configure(&client);
        Self {
            client,
            upstream: OnceLock::new(),
            accepted_at: Instant::now(),
            closing: Mutex::new(false),
            closing_cv: Condvar::new(),
        }
    }

    fn is_closing(&self) -> bool {
        *self.closing.lock()
    }

    // Pumps never hold `closing` across I/O. Shutting the sockets down wakes pumps blocked in
    // `read`; pumps blocked in `write` see `closing` when their write timeout fires.
    fn close(&self) {
        *self.closing.lock() = true;
        self.closing_cv.notify_all();
        let _ = self.client.shutdown(Shutdown::Both);
        if let Some(upstream) = self.upstream.get() {
            let _ = upstream.shutdown(Shutdown::Both);
        }
    }

    fn reset(&self) {
        *self.closing.lock() = true;
        self.closing_cv.notify_all();
        let upstream = self.upstream.get();
        for stream in std::iter::once(&self.client).chain(upstream) {
            let _ = SockRef::from(stream).set_linger(Some(Duration::ZERO));
            // Unlike `Shutdown::Write` this sends nothing, so the peer's first sign of the
            // reset is the RST sent when the last `Arc<Conn>` closes the socket.
            let _ = stream.shutdown(Shutdown::Read);
        }
    }

    fn fail(&self) {
        if !self.is_closing() {
            self.reset();
        }
    }

    fn wait_closing(&self, timeout: Option<Duration>) -> bool {
        let mut closing = self.closing.lock();
        match timeout {
            Some(timeout) => {
                let deadline = Instant::now() + timeout;
                while !*closing {
                    if self
                        .closing_cv
                        .wait_until(&mut closing, deadline)
                        .timed_out()
                    {
                        break;
                    }
                }
            }
            None => {
                while !*closing {
                    self.closing_cv.wait(&mut closing);
                }
            }
        }
        *closing
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Direction {
    ToUpstream,
    ToClient,
}

fn pump(conn: &Conn, dir: Direction, cfg: &ChaosConfig) {
    let Some(upstream) = conn.upstream.get() else {
        return;
    };
    let (mut src, dst) = match dir {
        Direction::ToUpstream => (&conn.client, upstream),
        Direction::ToClient => (upstream, &conn.client),
    };
    let mut throttle = match (dir, cfg.read_rate_bps) {
        (Direction::ToUpstream, Some(rate)) => Some(Throttle::new(
            rate,
            cfg.throttle_for.map(|d| conn.accepted_at + d),
        )),
        _ => None,
    };
    let limit = match (cfg.stall_after, cfg.reset_after) {
        (Some(stall), Some(reset)) => Some(stall.min(reset)),
        (stall, reset) => stall.or(reset),
    };
    let mut forwarded = 0usize;
    let split = cfg.split_writes.is_some();
    let chunk = cfg.split_writes.unwrap_or(BUF_SIZE).max(1);
    let mut buf = vec![0u8; BUF_SIZE];

    loop {
        if cfg.reset_after == Some(forwarded) {
            conn.reset();
            return;
        }
        if cfg.stall_after == Some(forwarded) {
            conn.wait_closing(None);
            return;
        }

        let mut want = buf.len();
        if let Some(limit) = limit {
            want = want.min(limit - forwarded);
        }
        if let Some(t) = throttle.as_mut() {
            match t.admit(conn) {
                Admit::Upto(n) => want = want.min(n),
                Admit::Unthrottled => throttle = None,
                Admit::Closing => return,
            }
        }

        let n = match src.read(&mut buf[..want]) {
            Ok(0) => {
                if !conn.is_closing() {
                    let _ = dst.shutdown(Shutdown::Write);
                }
                return;
            }
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => {
                conn.fail();
                return;
            }
        };
        if let Some(t) = throttle.as_mut() {
            t.consume(n);
        }

        for piece in buf[..n].chunks(chunk) {
            if write_fully(conn, dst, piece).is_err() {
                conn.fail();
                return;
            }
            if split {
                // Lets a reader that keeps up see each chunk on its own before the kernel
                // coalesces it with the next one.
                thread::yield_now();
            }
        }
        forwarded += n;
    }
}

fn write_fully(conn: &Conn, mut dst: &TcpStream, mut data: &[u8]) -> io::Result<()> {
    while !data.is_empty() {
        match dst.write(data) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(n) => data = &data[n..],
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock
                        | io::ErrorKind::TimedOut
                        | io::ErrorKind::Interrupted
                ) =>
            {
                if conn.is_closing() {
                    return Err(e);
                }
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

enum Admit {
    Upto(usize),
    Unthrottled,
    Closing,
}

/// Token bucket: tokens accrue at `rate` up to 100 ms worth, and reads wait for 10 ms worth.
struct Throttle {
    rate: f64,
    until: Option<Instant>,
    tokens: f64,
    burst: f64,
    quantum: f64,
    refilled_at: Instant,
}

impl Throttle {
    fn new(rate: u64, until: Option<Instant>) -> Self {
        let rate = rate as f64;
        let burst = (rate / 10.0).max(1.0);
        Self {
            rate,
            until,
            tokens: 0.0,
            burst,
            quantum: (rate / 100.0).clamp(1.0, burst),
            refilled_at: Instant::now(),
        }
    }

    fn admit(&mut self, conn: &Conn) -> Admit {
        loop {
            let now = Instant::now();
            if self.until.is_some_and(|until| now >= until) {
                return Admit::Unthrottled;
            }

            let elapsed = now.duration_since(self.refilled_at).as_secs_f64();
            self.tokens = (self.tokens + elapsed * self.rate).min(self.burst);
            self.refilled_at = now;
            if self.tokens >= self.quantum {
                return Admit::Upto(self.tokens as usize);
            }

            let refill = (self.rate > 0.0)
                .then(|| Duration::from_secs_f64((self.quantum - self.tokens) / self.rate));
            let wait = match (refill, self.until) {
                (Some(refill), Some(until)) => Some(refill.min(until - now)),
                (Some(refill), None) => Some(refill),
                (None, Some(until)) => Some(until - now),
                (None, None) => None,
            };
            if conn.wait_closing(wait) {
                return Admit::Closing;
            }
        }
    }

    fn consume(&mut self, n: usize) {
        self.tokens = (self.tokens - n as f64).max(0.0);
    }
}
