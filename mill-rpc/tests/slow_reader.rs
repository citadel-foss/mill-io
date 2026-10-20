use mill_io::EventLoop;
use mill_rpc::prelude::*;
use mill_rpc::{Frame, FrameHeader};
use mill_rpc_core::protocol::HEADER_SIZE;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

mill_rpc::service! {
    service Bytes {
        fn echo(data: Vec<u8>) -> Vec<u8>;
    }
}

struct Echo;

impl bytes::Service for Echo {
    fn echo(&self, _ctx: &RpcContext, data: Vec<u8>) -> Vec<u8> {
        data
    }
}

const REQUESTS: usize = 64;
const PAYLOAD: usize = 256 * 1024;

struct Harness {
    event_loop: Arc<EventLoop>,
    runner: Option<JoinHandle<()>>,
    addr: SocketAddr,
    _server: RpcServer,
}

/// Echoes, and counts calls dispatched on the reactor thread.
struct ThreadCheck {
    event_loop: Weak<EventLoop>,
    calls: Arc<AtomicUsize>,
    on_reactor: Arc<AtomicUsize>,
}

impl bytes::Service for ThreadCheck {
    fn echo(&self, _ctx: &RpcContext, data: Vec<u8>) -> Vec<u8> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(event_loop) = self.event_loop.upgrade() {
            if event_loop.is_reactor_thread() {
                self.on_reactor.fetch_add(1, Ordering::SeqCst);
            }
        }
        data
    }
}

impl Harness {
    fn start(slow_reader_timeout: Duration) -> Self {
        Self::start_with(slow_reader_timeout, |_| Echo)
    }

    fn start_with<S: bytes::Service>(
        slow_reader_timeout: Duration,
        service: impl FnOnce(&Arc<EventLoop>) -> S,
    ) -> Self {
        let event_loop = Arc::new(EventLoop::new(4, 1024, 10).unwrap());
        let addr = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let server = RpcServer::builder()
            .bind(addr)
            .slow_reader_timeout(slow_reader_timeout)
            .service(bytes::server(service(&event_loop)))
            .build(&event_loop)
            .unwrap();
        let runner = {
            let event_loop = Arc::clone(&event_loop);
            thread::spawn(move || event_loop.run().unwrap())
        };
        Self {
            event_loop,
            runner: Some(runner),
            addr,
            _server: server,
        }
    }

    /// A well-behaved client on the same server answers promptly.
    fn assert_others_unaffected(&self, what: &str) {
        let transport = RpcClient::connect(self.addr, &self.event_loop).unwrap();
        transport.set_timeout(Duration::from_secs(5));
        let client = bytes::Client::new(transport, Codec::bincode(), 0);
        let started = Instant::now();
        for i in 0..50u8 {
            assert_eq!(client.echo(vec![i; 100]).unwrap(), vec![i; 100], "{what}");
        }
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "{what}: 50 calls took {:?}",
            started.elapsed()
        );
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.event_loop.stop();
        if let Some(runner) = self.runner.take() {
            let _ = runner.join();
        }
    }
}

/// A client whose receive buffer is fixed small, so replies it doesn't read back up into
/// the server instead of disappearing into an autotuned kernel buffer.
fn small_window_client(addr: SocketAddr) -> TcpStream {
    use socket2::{Domain, Protocol, Socket, Type};
    let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP)).unwrap();
    socket.set_recv_buffer_size(64 * 1024).unwrap();
    // Set before connecting: macOS rejects socket options once the peer has reset it.
    socket
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    socket.connect(&addr.into()).unwrap();
    socket.into()
}

fn payload(id: usize) -> Vec<u8> {
    (0..PAYLOAD).map(|i| (i * 7 + id) as u8).collect()
}

fn request(id: usize) -> Vec<u8> {
    // The generated `EchoRequest { data }` encodes like the 1-tuple `(data,)` in bincode.
    let args = Codec::bincode().serialize(&(payload(id),)).unwrap();
    Frame::request(id as u64, 0, bytes::methods::ECHO, args, false).encode()
}

/// Writes every request, built up front, on its own thread; counts the ones fully written.
fn spawn_writer(stream: &TcpStream) -> (JoinHandle<()>, Arc<AtomicUsize>) {
    let requests: Vec<Vec<u8>> = (1..=REQUESTS).map(request).collect();
    let mut stream = stream.try_clone().unwrap();
    let written = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&written);
    let writer = thread::spawn(move || {
        for request in &requests {
            if stream.write_all(request).is_err() {
                return;
            }
            count.fetch_add(1, Ordering::SeqCst);
        }
    });
    (writer, written)
}

/// True once `written` stops moving for `quiet` before reaching `REQUESTS`.
fn writer_stalls(written: &AtomicUsize, quiet: Duration, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    let mut last = written.load(Ordering::SeqCst);
    let mut since = Instant::now();
    while Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
        let now = written.load(Ordering::SeqCst);
        if now == REQUESTS {
            return false;
        }
        if now != last {
            last = now;
            since = Instant::now();
        } else if since.elapsed() >= quiet {
            return true;
        }
    }
    false
}

fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while !cond() {
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(1));
    }
    true
}

fn read_reply(stream: &mut TcpStream) -> io::Result<(u64, Vec<u8>)> {
    let mut header = [0u8; HEADER_SIZE];
    stream.read_exact(&mut header)?;
    let header = FrameHeader::decode(&header).map_err(|e| io::Error::other(e.to_string()))?;
    let mut payload = vec![0u8; header.payload_len as usize];
    stream.read_exact(&mut payload)?;
    let frame = Frame { header, payload };
    let (id, data) = frame
        .parse_response_payload()
        .map_err(|e| io::Error::other(e.to_string()))?;
    let data: Vec<u8> = Codec::bincode()
        .deserialize(data)
        .map_err(|e| io::Error::other(e.to_string()))?;
    Ok((id, data))
}

#[test]
fn slow_reader_gets_every_reply_in_order() {
    let harness = Harness::start(Duration::from_secs(60));
    let mut stream = small_window_client(harness.addr);
    let (writer, written) = spawn_writer(&stream);

    // Not reading: once the replies back up, the server stops reading requests too, so
    // the writer stalls well short of the end.
    assert!(
        writer_stalls(
            &written,
            Duration::from_millis(500),
            Duration::from_secs(20)
        ),
        "the server kept reading requests from a client that reads no replies"
    );
    let stalled_at = written.load(Ordering::SeqCst);
    harness.assert_others_unaffected("during the stall");

    for id in 1..=REQUESTS {
        let (reply_id, data) = read_reply(&mut stream)
            .unwrap_or_else(|e| panic!("reply {id} (writer stalled at {stalled_at}): {e}"));
        assert_eq!(reply_id, id as u64, "replies out of order");
        assert!(data == payload(id), "reply {id} has the wrong payload");
    }
    writer.join().unwrap();
    assert_eq!(written.load(Ordering::SeqCst), REQUESTS);
}

#[test]
fn stuck_reader_is_disconnected() {
    let harness = Harness::start(Duration::from_millis(500));
    let mut stream = small_window_client(harness.addr);
    let (writer, written) = spawn_writer(&stream);

    // Never reads; the server must give up on it, which also cuts the writer off.
    assert!(
        wait_until(Duration::from_secs(20), || writer.is_finished()),
        "the server never closed a client that stopped reading"
    );
    assert!(
        written.load(Ordering::SeqCst) < REQUESTS,
        "every request went through, so the reader never stalled"
    );
    harness.assert_others_unaffected("after a reader was stuck");

    let mut replies = 0;
    let end = loop {
        match read_reply(&mut stream) {
            Ok(_) => replies += 1,
            Err(e) => break e,
        }
    };
    assert!(
        matches!(
            end.kind(),
            io::ErrorKind::UnexpectedEof
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::ConnectionAborted
        ),
        "expected the server to close the connection, got {end} after {replies} replies"
    );
    assert!(
        replies < REQUESTS,
        "every reply arrived; nothing was cut off"
    );
    writer.join().unwrap();
}

#[test]
fn requests_after_a_stall_are_not_dispatched_on_the_reactor() {
    let calls = Arc::new(AtomicUsize::new(0));
    let on_reactor = Arc::new(AtomicUsize::new(0));
    let harness = Harness::start_with(Duration::from_secs(60), |event_loop| ThreadCheck {
        event_loop: Arc::downgrade(event_loop),
        calls: Arc::clone(&calls),
        on_reactor: Arc::clone(&on_reactor),
    });
    let mut stream = small_window_client(harness.addr);
    let (writer, written) = spawn_writer(&stream);
    assert!(
        writer_stalls(
            &written,
            Duration::from_millis(500),
            Duration::from_secs(20)
        ),
        "the connection never stalled"
    );
    let dispatched_before_drain = calls.load(Ordering::SeqCst);

    for id in 1..=REQUESTS {
        let (reply_id, _) = read_reply(&mut stream).unwrap();
        assert_eq!(reply_id, id as u64, "replies out of order");
    }
    writer.join().unwrap();

    assert_eq!(calls.load(Ordering::SeqCst), REQUESTS);
    assert!(
        dispatched_before_drain < REQUESTS,
        "every request ran before the stall cleared"
    );
    assert_eq!(
        on_reactor.load(Ordering::SeqCst),
        0,
        "requests were dispatched on the reactor thread in pool mode"
    );
}
