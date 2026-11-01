use mill_net::errors::{NetworkError, Result};
use mill_net::tcp::config::TcpServerConfig;
use mill_net::tcp::{
    ConnectionId, NetworkHandler, SendError, ServerContext, TcpClient, TcpClientConfig, TcpServer,
};
use parking_lot::Mutex;
use std::io;
use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mill_io::EventLoop;

mod common;

use common::wait_until;

type ErrorLog = Arc<Mutex<Vec<(Option<ConnectionId>, String, Option<io::ErrorKind>, Instant)>>>;

#[derive(Clone, Default)]
struct Recorder {
    connects: Arc<AtomicUsize>,
    disconnects: Arc<AtomicUsize>,
    errors: ErrorLog,
    greeting: Option<&'static [u8]>,
    received: Arc<Mutex<Vec<u8>>>,
}

impl NetworkHandler for Recorder {
    fn on_connect(&self, ctx: &ServerContext, conn: ConnectionId) -> Result<()> {
        self.connects.fetch_add(1, Ordering::SeqCst);
        if let Some(greeting) = self.greeting {
            ctx.send_to(conn, greeting)?;
        }
        Ok(())
    }

    fn on_data(&self, _ctx: &ServerContext, _conn: ConnectionId, data: &[u8]) -> Result<()> {
        self.received.lock().extend_from_slice(data);
        Ok(())
    }

    fn on_disconnect(&self, _ctx: &ServerContext, _conn: ConnectionId) -> Result<()> {
        self.disconnects.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn on_error(&self, _ctx: &ServerContext, conn: Option<ConnectionId>, error: NetworkError) {
        let kind = match &error {
            NetworkError::Connect(e) => e.downcast_ref::<io::Error>().map(io::Error::kind),
            _ => None,
        };
        self.errors
            .lock()
            .push((conn, error.to_string(), kind, Instant::now()));
    }
}

#[cfg(unix)]
#[mill_io::test(modes = "both", workers = 2, poll_timeout_ms = 10)]
fn connect_timeout_is_enforced(event_loop: &Arc<EventLoop>) {
    use std::net::TcpStream;

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    // Calling `listen` again on a listening socket changes its backlog. 1, not 0: some
    // systems read 0 as "the default".
    socket2::SockRef::from(&listener).listen(1).unwrap();
    let addr = listener.local_addr().unwrap();
    // Fill the queue until the kernel starts dropping SYNs, however it counts the
    // backlog; then the client's SYN is dropped too.
    let mut fillers = Vec::new();
    let mut black_hole = false;
    while fillers.len() < 200 {
        match TcpStream::connect_timeout(&addr, Duration::from_millis(200)) {
            Ok(stream) => fillers.push(stream),
            Err(e) => {
                black_hole = e.kind() == io::ErrorKind::TimedOut;
                break;
            }
        }
    }
    if !black_hole && !cfg!(target_os = "linux") {
        eprintln!(
            "skipped: after {} connections the listener still answered SYNs, \
             so this OS gives no way to stall a connect",
            fillers.len()
        );
        return;
    }
    assert!(black_hole, "a full backlog did not drop SYNs");

    let recorder = Recorder::default();
    let errors = Arc::clone(&recorder.errors);
    let connects = Arc::clone(&recorder.connects);
    let disconnects = Arc::clone(&recorder.disconnects);
    let config = TcpClientConfig {
        connect_timeout: Duration::from_millis(500),
        ..TcpClientConfig::default()
    };
    let mut client = TcpClient::connect(addr, config, recorder).unwrap();
    let id = client.id();
    let started = Instant::now();
    client.start(event_loop).unwrap();

    assert!(
        wait_until(Duration::from_secs(2), || !errors.lock().is_empty()),
        "no connect error within 2 s"
    );
    let (conn, message, kind, at) = errors.lock()[0].clone();
    let elapsed = at - started;
    assert!(
        elapsed <= Duration::from_millis(700),
        "timed out after {elapsed:?}"
    );
    assert!(
        elapsed >= Duration::from_millis(450),
        "failed early after {elapsed:?}: {message}"
    );
    assert_eq!(conn, Some(id));
    assert_eq!(kind, Some(io::ErrorKind::TimedOut), "{message}");
    assert_eq!(connects.load(Ordering::SeqCst), 0, "on_connect fired");
    assert_eq!(disconnects.load(Ordering::SeqCst), 0);
    assert!(matches!(client.send(b"x"), Err(SendError::Closed)));
    assert_eq!(event_loop.pending_timers(), 0, "timer left behind");
}

#[mill_io::test(modes = "both", workers = 2, poll_timeout_ms = 10)]
fn connect_refused_is_an_error(event_loop: &Arc<EventLoop>) {
    let addr = {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap()
    };

    let recorder = Recorder::default();
    let errors = Arc::clone(&recorder.errors);
    let connects = Arc::clone(&recorder.connects);
    let disconnects = Arc::clone(&recorder.disconnects);
    let _client = match TcpClient::connect(addr, TcpClientConfig::default(), recorder) {
        Ok(mut client) => {
            client.start(event_loop).unwrap();
            let connect_error = || {
                errors
                    .lock()
                    .iter()
                    .find(|(_, _, kind, _)| kind.is_some())
                    .cloned()
            };
            assert!(
                wait_until(Duration::from_secs(5), || connect_error().is_some()),
                "refused connect reported no connect error: {:?}",
                errors.lock()
            );
            let (conn, message, _, _) = connect_error().unwrap();
            assert_eq!(conn, Some(client.id()), "{message}");
            assert!(matches!(client.send(b"x"), Err(SendError::Closed)));
            Some(client)
        }
        Err(_) => None,
    };

    assert_eq!(connects.load(Ordering::SeqCst), 0, "on_connect fired");
    assert_eq!(disconnects.load(Ordering::SeqCst), 0);
    assert_eq!(event_loop.pending_timers(), 0, "timer left behind");
}

#[mill_io::test(modes = "both", workers = 2, poll_timeout_ms = 10)]
fn on_connect_waits_for_the_handshake_and_ctx_sends(event_loop: &Arc<EventLoop>) {
    let server_recorder = Recorder::default();
    let server_received = Arc::clone(&server_recorder.received);
    let config = TcpServerConfig::builder()
        .address("127.0.0.1:0".parse().unwrap())
        .build();
    let server = Arc::new(TcpServer::new(config, server_recorder).unwrap());
    Arc::clone(&server)
        .start(event_loop, event_loop.alloc_token())
        .unwrap();

    let recorder = Recorder {
        greeting: Some(b"hello from on_connect"),
        ..Recorder::default()
    };
    let connects = Arc::clone(&recorder.connects);
    let errors = Arc::clone(&recorder.errors);
    let mut client = TcpClient::connect(
        server.local_addr().unwrap(),
        TcpClientConfig::default(),
        recorder,
    )
    .unwrap();
    client.start(event_loop).unwrap();
    assert_eq!(
        connects.load(Ordering::SeqCst),
        0,
        "on_connect fired inside start()"
    );

    assert!(
        wait_until(Duration::from_secs(5), || {
            server_received.lock().as_slice() == b"hello from on_connect"
        }),
        "server got {:?}, client errors {:?}",
        server_received.lock(),
        errors.lock()
    );
    assert_eq!(connects.load(Ordering::SeqCst), 1);
    assert_eq!(
        event_loop.pending_timers(),
        0,
        "connect timer not cancelled"
    );
}

#[mill_io::test(modes = "both", workers = 2, poll_timeout_ms = 10)]
fn sends_queued_while_connecting_are_flushed(event_loop: &Arc<EventLoop>) {
    let server_recorder = Recorder::default();
    let server_received = Arc::clone(&server_recorder.received);
    let config = TcpServerConfig::builder()
        .address("127.0.0.1:0".parse().unwrap())
        .build();
    let server = Arc::new(TcpServer::new(config, server_recorder).unwrap());
    Arc::clone(&server)
        .start(event_loop, event_loop.alloc_token())
        .unwrap();

    let mut client = TcpClient::connect(
        server.local_addr().unwrap(),
        TcpClientConfig::default(),
        Recorder::default(),
    )
    .unwrap();
    client.send(b"early ").unwrap();
    client.start(event_loop).unwrap();
    client.send(b"bird").unwrap();

    assert!(
        wait_until(Duration::from_secs(5), || {
            server_received.lock().as_slice() == b"early bird"
        }),
        "server got {:?}",
        server_received.lock()
    );
}
