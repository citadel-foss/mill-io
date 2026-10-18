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

mod common;

use common::{pool_and_direct, spawn_loop, stop_loop, wait_until};

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
mod backlog {
    use std::net::TcpListener;
    use std::os::raw::c_int;
    use std::os::unix::io::AsRawFd;

    extern "C" {
        fn listen(socket: c_int, backlog: c_int) -> c_int;
    }

    /// Calling `listen` again on a listening socket changes its backlog.
    pub fn shrink_to_zero(listener: &TcpListener) {
        // SAFETY: the fd belongs to `listener`, which is open for the whole call.
        let rc = unsafe { listen(listener.as_raw_fd(), 0) };
        assert_eq!(rc, 0, "listen(fd, 0) failed");
    }
}

#[cfg(unix)]
#[test]
fn connect_timeout_is_enforced() {
    use std::net::TcpStream;

    for (mode, event_loop) in pool_and_direct() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        backlog::shrink_to_zero(&listener);
        let addr = listener.local_addr().unwrap();
        let mut fillers = Vec::new();
        for _ in 0..64 {
            match TcpStream::connect_timeout(&addr, Duration::from_millis(200)) {
                Ok(stream) => fillers.push(stream),
                Err(_) => break,
            }
        }
        assert!(!fillers.is_empty(), "{mode}: no filler connection got in");

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
        let runner = spawn_loop(&event_loop);
        let started = Instant::now();
        client.start(&event_loop).unwrap();

        assert!(
            wait_until(Duration::from_secs(2), || !errors.lock().is_empty()),
            "{mode}: no connect error within 2 s"
        );
        let (conn, message, kind, at) = errors.lock()[0].clone();
        let elapsed = at - started;
        assert!(
            elapsed <= Duration::from_millis(700),
            "{mode}: timed out after {elapsed:?}"
        );
        assert!(
            elapsed >= Duration::from_millis(450),
            "{mode}: failed early after {elapsed:?}: {message}"
        );
        assert_eq!(conn, Some(id), "{mode}");
        assert_eq!(kind, Some(io::ErrorKind::TimedOut), "{mode}: {message}");
        assert_eq!(
            connects.load(Ordering::SeqCst),
            0,
            "{mode}: on_connect fired"
        );
        assert_eq!(disconnects.load(Ordering::SeqCst), 0, "{mode}");
        assert!(matches!(client.send(b"x"), Err(SendError::Closed)));
        assert_eq!(event_loop.pending_timers(), 0, "{mode}: timer left behind");

        stop_loop(&event_loop, runner);
        drop(fillers);
    }
}

#[test]
fn connect_refused_is_an_error() {
    for (mode, event_loop) in pool_and_direct() {
        let addr = {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap()
        };

        let recorder = Recorder::default();
        let errors = Arc::clone(&recorder.errors);
        let connects = Arc::clone(&recorder.connects);
        let disconnects = Arc::clone(&recorder.disconnects);
        let runner = spawn_loop(&event_loop);
        let client = match TcpClient::connect(addr, TcpClientConfig::default(), recorder) {
            Ok(mut client) => {
                client.start(&event_loop).unwrap();
                assert!(
                    wait_until(Duration::from_secs(2), || !errors.lock().is_empty()),
                    "{mode}: refused connect reported no error"
                );
                let (conn, message, _, _) = errors.lock()[0].clone();
                assert_eq!(conn, Some(client.id()), "{mode}: {message}");
                assert!(matches!(client.send(b"x"), Err(SendError::Closed)));
                Some(client)
            }
            Err(_) => None,
        };

        assert_eq!(
            connects.load(Ordering::SeqCst),
            0,
            "{mode}: on_connect fired"
        );
        assert_eq!(disconnects.load(Ordering::SeqCst), 0, "{mode}");
        assert_eq!(event_loop.pending_timers(), 0, "{mode}: timer left behind");
        stop_loop(&event_loop, runner);
        drop(client);
    }
}

#[test]
fn on_connect_waits_for_the_handshake_and_ctx_sends() {
    for (mode, event_loop) in pool_and_direct() {
        let server_recorder = Recorder::default();
        let server_received = Arc::clone(&server_recorder.received);
        let config = TcpServerConfig::builder()
            .address("127.0.0.1:0".parse().unwrap())
            .build();
        let server = Arc::new(TcpServer::new(config, server_recorder).unwrap());
        Arc::clone(&server)
            .start(&event_loop, event_loop.alloc_token())
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
        client.start(&event_loop).unwrap();
        assert_eq!(
            connects.load(Ordering::SeqCst),
            0,
            "{mode}: on_connect fired inside start()"
        );
        let runner = spawn_loop(&event_loop);

        assert!(
            wait_until(Duration::from_secs(5), || {
                server_received.lock().as_slice() == b"hello from on_connect"
            }),
            "{mode}: server got {:?}, client errors {:?}",
            server_received.lock(),
            errors.lock()
        );
        assert_eq!(connects.load(Ordering::SeqCst), 1, "{mode}");
        assert_eq!(
            event_loop.pending_timers(),
            0,
            "{mode}: connect timer not cancelled"
        );

        stop_loop(&event_loop, runner);
        drop((client, server));
    }
}

#[test]
fn sends_queued_while_connecting_are_flushed() {
    for (mode, event_loop) in pool_and_direct() {
        let server_recorder = Recorder::default();
        let server_received = Arc::clone(&server_recorder.received);
        let config = TcpServerConfig::builder()
            .address("127.0.0.1:0".parse().unwrap())
            .build();
        let server = Arc::new(TcpServer::new(config, server_recorder).unwrap());
        Arc::clone(&server)
            .start(&event_loop, event_loop.alloc_token())
            .unwrap();

        let mut client = TcpClient::connect(
            server.local_addr().unwrap(),
            TcpClientConfig::default(),
            Recorder::default(),
        )
        .unwrap();
        client.send(b"early ").unwrap();
        client.start(&event_loop).unwrap();
        client.send(b"bird").unwrap();
        let runner = spawn_loop(&event_loop);

        assert!(
            wait_until(Duration::from_secs(5), || {
                server_received.lock().as_slice() == b"early bird"
            }),
            "{mode}: server got {:?}",
            server_received.lock()
        );

        stop_loop(&event_loop, runner);
        drop((client, server));
    }
}
