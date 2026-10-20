use mill_net::errors::Result;
use mill_net::tcp::config::TcpServerConfig;
use mill_net::tcp::{
    ConnectionId, Layer, NetworkHandler, SendError, ServerContext, TcpClient, TcpClientConfig,
    TcpServer,
};
use mill_net::testing::{ChaosConfig, ChaosProxy};
use parking_lot::Mutex;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

mod common;

use common::{pool_and_direct, spawn_loop, stop_loop, wait_until};

/// Test-only layer: each side sends a one-byte key first, then XORs what it sends with its
/// own key and what it receives with the peer's.
struct XorLayer {
    key: u8,
    peer_key: Option<u8>,
}

impl XorLayer {
    fn new(key: u8) -> Self {
        Self {
            key,
            peer_key: None,
        }
    }
}

impl Layer for XorLayer {
    fn start(&mut self, out: &mut Vec<u8>) -> io::Result<()> {
        out.push(self.key);
        Ok(())
    }

    fn decode(&mut self, input: &[u8], up: &mut Vec<u8>, _out: &mut Vec<u8>) -> io::Result<()> {
        let mut input = input;
        let key = match self.peer_key {
            Some(key) => key,
            None => {
                let key = input[0];
                self.peer_key = Some(key);
                input = &input[1..];
                key
            }
        };
        up.extend(input.iter().map(|b| b ^ key));
        Ok(())
    }

    fn encode(&mut self, frame: &[u8], out: &mut Vec<u8>) -> io::Result<()> {
        out.extend(frame.iter().map(|b| b ^ self.key));
        Ok(())
    }

    fn is_ready(&self) -> bool {
        self.peer_key.is_some()
    }
}

#[derive(Clone, Default)]
struct Echo {
    connects: Arc<AtomicUsize>,
    received: Arc<Mutex<Vec<u8>>>,
}

impl NetworkHandler for Echo {
    fn on_connect(&self, _ctx: &ServerContext, _conn: ConnectionId) -> Result<()> {
        self.connects.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn on_data(&self, ctx: &ServerContext, conn: ConnectionId, data: &[u8]) -> Result<()> {
        self.received.lock().extend_from_slice(data);
        ctx.send_to(conn, data)
    }
}

#[derive(Clone, Default)]
struct Collector {
    connects: Arc<AtomicUsize>,
    received: Arc<Mutex<Vec<u8>>>,
}

impl NetworkHandler for Collector {
    fn on_connect(&self, _ctx: &ServerContext, _conn: ConnectionId) -> Result<()> {
        self.connects.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn on_data(&self, _ctx: &ServerContext, _conn: ConnectionId, data: &[u8]) -> Result<()> {
        self.received.lock().extend_from_slice(data);
        Ok(())
    }
}

fn xor_echo_server(event_loop: &Arc<mill_io::EventLoop>) -> (Arc<TcpServer<Echo>>, Echo) {
    let echo = Echo::default();
    let config = TcpServerConfig::builder()
        .address("127.0.0.1:0".parse().unwrap())
        .high_watermark(64 * 1024 * 1024)
        .build();
    let server = TcpServer::new(config, echo.clone())
        .unwrap()
        .with_layer(|| Box::new(XorLayer::new(0x5a)));
    let server = Arc::new(server);
    Arc::clone(&server)
        .start(event_loop, event_loop.alloc_token())
        .unwrap();
    (server, echo)
}

#[test]
fn xor_layer_round_trip() {
    // Every byte crosses the proxy as its own segment, which direct mode, with one thread
    // for both ends, handles too slowly for the full 1 MB.
    let [pool, direct] = pool_and_direct();
    for ((mode, event_loop), len) in [(pool, 1024 * 1024), (direct, 64 * 1024)] {
        let (server, echo) = xor_echo_server(&event_loop);
        let proxy = ChaosProxy::start(
            server.local_addr().unwrap(),
            ChaosConfig {
                split_writes: Some(1),
                ..ChaosConfig::default()
            },
        )
        .unwrap();

        let collector = Collector::default();
        let received = Arc::clone(&collector.received);
        let connects = Arc::clone(&collector.connects);
        let config = TcpClientConfig {
            high_watermark: 64 * 1024 * 1024,
            ..TcpClientConfig::default()
        };
        let mut client = TcpClient::connect(proxy.addr(), config, collector)
            .unwrap()
            .with_layer(XorLayer::new(0xa5))
            .unwrap();
        let data: Vec<u8> = (0..len).map(|i| (i * 7 % 256) as u8).collect();
        // Sent before the handshake: held, then encoded once both keys are known.
        client.send(&data[..1000]).unwrap();
        client.start(&event_loop).unwrap();
        let runner = spawn_loop(&event_loop);

        for chunk in data[1000..].chunks(16 * 1024) {
            match client.send(chunk) {
                Ok(()) => {}
                Err(SendError::Backpressure) => panic!("{mode}: unexpected backpressure"),
                Err(e) => panic!("{mode}: {e}"),
            }
        }

        assert!(
            wait_until(Duration::from_secs(300), || received.lock().len() >= len),
            "{mode}: {} of {len} bytes came back",
            received.lock().len()
        );
        assert!(
            *echo.received.lock() == data,
            "{mode}: server saw other bytes"
        );
        assert!(*received.lock() == data, "{mode}: echo differs");
        assert_eq!(connects.load(Ordering::SeqCst), 1, "{mode}");
        assert_eq!(echo.connects.load(Ordering::SeqCst), 1, "{mode}");

        stop_loop(&event_loop, runner);
        drop((client, proxy, server));
    }
}

#[test]
fn layer_handshake_gates_on_connect_and_transforms_bytes() {
    for (mode, event_loop) in pool_and_direct() {
        let (server, echo) = xor_echo_server(&event_loop);
        let runner = spawn_loop(&event_loop);

        let mut raw = TcpStream::connect(server.local_addr().unwrap()).unwrap();
        raw.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut key = [0; 1];
        raw.read_exact(&mut key).unwrap();
        assert_eq!(key, [0x5a], "{mode}: the server's layer did not start");
        assert!(
            !wait_until(Duration::from_millis(100), || echo
                .connects
                .load(Ordering::SeqCst)
                > 0),
            "{mode}: on_connect fired before the handshake finished"
        );

        raw.write_all(&[0x11]).unwrap();
        raw.write_all(&[b'h' ^ 0x11, b'i' ^ 0x11]).unwrap();
        let mut reply = [0; 2];
        raw.read_exact(&mut reply).unwrap();
        assert_eq!(reply, [b'h' ^ 0x5a, b'i' ^ 0x5a], "{mode}");
        assert_eq!(echo.received.lock().as_slice(), b"hi", "{mode}");
        assert_eq!(echo.connects.load(Ordering::SeqCst), 1, "{mode}");

        stop_loop(&event_loop, runner);
        drop(server);
    }
}

#[test]
fn with_layer_after_a_send_or_start_is_refused() {
    use mill_net::errors::NetworkError;

    let event_loop = Arc::new(mill_io::EventLoop::new(2, 1024, 10).unwrap());
    let (server, _echo) = xor_echo_server(&event_loop);
    let addr = server.local_addr().unwrap();
    let refused = |result: Result<TcpClient<Collector>>| match result {
        Ok(_) => panic!("a layer was added too late"),
        Err(e) => assert!(
            matches!(
                e.downcast_ref::<NetworkError>(),
                Some(NetworkError::Configuration(_))
            ),
            "unexpected error: {e}"
        ),
    };

    let client = TcpClient::connect(addr, TcpClientConfig::default(), Collector::default())
        .unwrap()
        .with_layer(XorLayer::new(1))
        .unwrap();
    client.send(b"queued before start").unwrap();
    refused(client.with_layer(XorLayer::new(2)));

    let mut client =
        TcpClient::connect(addr, TcpClientConfig::default(), Collector::default()).unwrap();
    client.start(&event_loop).unwrap();
    refused(client.with_layer(XorLayer::new(3)));

    let client = TcpClient::connect(addr, TcpClientConfig::default(), Collector::default())
        .unwrap()
        .with_layer(XorLayer::new(4))
        .unwrap()
        .with_layer(XorLayer::new(5));
    assert!(
        client.is_ok(),
        "two layers before start and send were refused"
    );
}

type ErrorLog = Arc<Mutex<Vec<(Option<ConnectionId>, Option<io::ErrorKind>)>>>;

#[derive(Clone, Default)]
struct Watcher {
    connects: Arc<AtomicUsize>,
    disconnects: Arc<AtomicUsize>,
    errors: ErrorLog,
}

impl NetworkHandler for Watcher {
    fn on_connect(&self, _ctx: &ServerContext, _conn: ConnectionId) -> Result<()> {
        self.connects.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn on_data(&self, _ctx: &ServerContext, _conn: ConnectionId, _data: &[u8]) -> Result<()> {
        Ok(())
    }

    fn on_disconnect(&self, _ctx: &ServerContext, _conn: ConnectionId) -> Result<()> {
        self.disconnects.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn on_error(
        &self,
        _ctx: &ServerContext,
        conn: Option<ConnectionId>,
        error: mill_net::errors::NetworkError,
    ) {
        let kind = match &error {
            mill_net::errors::NetworkError::Io(e) => {
                e.downcast_ref::<io::Error>().map(io::Error::kind)
            }
            _ => None,
        };
        self.errors.lock().push((conn, kind));
    }
}

/// The peer went away: EOF, or a reset if it closed with our handshake bytes unread.
fn is_peer_gone(kind: Option<io::ErrorKind>) -> bool {
    matches!(
        kind,
        Some(
            io::ErrorKind::UnexpectedEof
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::ConnectionAborted
                | io::ErrorKind::BrokenPipe
        )
    )
}

#[test]
fn peer_close_during_the_handshake_is_an_error() {
    for (mode, event_loop) in pool_and_direct() {
        let runner = spawn_loop(&event_loop);

        // Client side: the peer accepts and closes before sending its key.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let watcher = Watcher::default();
        let mut client = TcpClient::connect(
            listener.local_addr().unwrap(),
            TcpClientConfig::default(),
            watcher.clone(),
        )
        .unwrap()
        .with_layer(XorLayer::new(0x33))
        .unwrap();
        client.start(&event_loop).unwrap();
        drop(listener.accept().unwrap());
        assert!(
            wait_until(Duration::from_secs(2), || !watcher.errors.lock().is_empty()),
            "{mode}: client: the close during the handshake was not reported"
        );
        let (conn, kind) = watcher.errors.lock()[0];
        assert_eq!(conn, Some(client.id()), "{mode}");
        assert!(is_peer_gone(kind), "{mode}: client: {kind:?}");
        assert!(
            wait_until(Duration::from_secs(2), || matches!(
                client.send(b"x"),
                Err(SendError::Closed)
            )),
            "{mode}: client: the connection was not cleaned up"
        );
        assert_eq!(watcher.connects.load(Ordering::SeqCst), 0, "{mode}");
        assert_eq!(watcher.disconnects.load(Ordering::SeqCst), 0, "{mode}");

        // Server side: a raw peer connects and closes before sending its key.
        let watcher = Watcher::default();
        let config = TcpServerConfig::builder()
            .address("127.0.0.1:0".parse().unwrap())
            .build();
        let server = Arc::new(
            TcpServer::new(config, watcher.clone())
                .unwrap()
                .with_layer(|| Box::new(XorLayer::new(0x44))),
        );
        Arc::clone(&server)
            .start(&event_loop, event_loop.alloc_token())
            .unwrap();
        drop(TcpStream::connect(server.local_addr().unwrap()).unwrap());
        assert!(
            wait_until(Duration::from_secs(2), || !watcher.errors.lock().is_empty()),
            "{mode}: server: the close during the handshake was not reported"
        );
        let (conn, kind) = watcher.errors.lock()[0];
        assert!(conn.is_some(), "{mode}");
        assert!(is_peer_gone(kind), "{mode}: server: {kind:?}");
        assert!(
            wait_until(Duration::from_secs(2), || server.connection_count() == 0),
            "{mode}: server: the connection was not cleaned up"
        );
        assert_eq!(watcher.connects.load(Ordering::SeqCst), 0, "{mode}");
        assert_eq!(watcher.disconnects.load(Ordering::SeqCst), 0, "{mode}");

        stop_loop(&event_loop, runner);
        drop((client, server));
    }
}
