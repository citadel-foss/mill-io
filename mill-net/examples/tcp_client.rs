//! `TcpClient` against a local echo server: `connect` with a `TcpClientConfig`, a send
//! queued before the handshake, `on_connect` once the handshake is done, and a refused
//! connect reported through `on_error` with no `on_connect` or `on_disconnect`.
//!
//! Run with `cargo run -p mill-net --example tcp_client`.

use mill_io::EventLoop;
use mill_net::errors::{NetworkError, Result};
use mill_net::tcp::{
    ConnectionId, NetworkHandler, ServerContext, TcpClient, TcpClientConfig, TcpServer,
    TcpServerConfig,
};
use std::net::{SocketAddr, TcpListener};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

struct Echo;

impl NetworkHandler for Echo {
    fn on_data(&self, ctx: &ServerContext, conn: ConnectionId, data: &[u8]) -> Result<()> {
        ctx.send_to(conn, data)
    }
}

#[derive(Debug)]
enum ClientEvent {
    Connected(ConnectionId),
    Data(String),
    Disconnected(ConnectionId),
    Failed(Option<ConnectionId>, String),
}

struct Reporter {
    name: &'static str,
    events: mpsc::Sender<ClientEvent>,
}

impl Reporter {
    fn report(&self, event: ClientEvent) {
        println!("[{}] {event:?}", self.name);
        let _ = self.events.send(event);
    }
}

impl NetworkHandler for Reporter {
    fn on_connect(&self, _ctx: &ServerContext, conn: ConnectionId) -> Result<()> {
        self.report(ClientEvent::Connected(conn));
        Ok(())
    }

    fn on_data(&self, _ctx: &ServerContext, _conn: ConnectionId, data: &[u8]) -> Result<()> {
        self.report(ClientEvent::Data(
            String::from_utf8_lossy(data).into_owned(),
        ));
        Ok(())
    }

    fn on_disconnect(&self, _ctx: &ServerContext, conn: ConnectionId) -> Result<()> {
        self.report(ClientEvent::Disconnected(conn));
        Ok(())
    }

    fn on_error(&self, _ctx: &ServerContext, conn: Option<ConnectionId>, error: NetworkError) {
        self.report(ClientEvent::Failed(conn, error.to_string()));
    }
}

fn next_event(events: &mpsc::Receiver<ClientEvent>) -> ClientEvent {
    events
        .recv_timeout(Duration::from_secs(5))
        .expect("no client event within 5 s")
}

fn closed_port() -> std::io::Result<SocketAddr> {
    TcpListener::bind("127.0.0.1:0")?.local_addr()
}

fn main() -> Result<()> {
    let event_loop = Arc::new(EventLoop::default());
    let server = Arc::new(TcpServer::new(
        TcpServerConfig::builder()
            .address("127.0.0.1:0".parse()?)
            .build(),
        Echo,
    )?);
    Arc::clone(&server).start(&event_loop, event_loop.alloc_token())?;
    let server_addr = server.local_addr()?;
    println!("echo server on {server_addr}");

    let runner = {
        let event_loop = Arc::clone(&event_loop);
        thread::spawn(move || event_loop.run().expect("event loop failed"))
    };

    let config = TcpClientConfig {
        connect_timeout: Duration::from_secs(2),
        ..TcpClientConfig::default()
    };

    let (events, received) = mpsc::channel();
    let mut client = TcpClient::connect(
        server_addr,
        config.clone(),
        Reporter {
            name: "client",
            events: events.clone(),
        },
    )?;
    client.send(b"queued before the handshake")?;
    client.start(&event_loop)?;
    println!(
        "[client] started {:?}; on_connect waits for the handshake",
        client.id()
    );

    assert!(matches!(next_event(&received), ClientEvent::Connected(id) if id == client.id()));
    match next_event(&received) {
        ClientEvent::Data(echo) => assert_eq!(echo, "queued before the handshake"),
        other => panic!("expected the echo, got {other:?}"),
    }
    let client_id = client.id();
    drop(client);
    assert!(matches!(next_event(&received), ClientEvent::Disconnected(id) if id == client_id));

    let refused_addr = closed_port()?;
    let mut refused = TcpClient::connect(
        refused_addr,
        config,
        Reporter {
            name: "refused",
            events,
        },
    )?;
    refused.start(&event_loop)?;
    println!("[refused] connecting to {refused_addr}, where nothing listens");
    match next_event(&received) {
        ClientEvent::Failed(Some(id), error) => {
            assert_eq!(id, refused.id());
            assert!(error.starts_with("Connect Error"), "{error}");
        }
        other => panic!("expected a connect error, got {other:?}"),
    }
    assert!(
        received.recv_timeout(Duration::from_millis(200)).is_err(),
        "a failed connect gets neither on_connect nor on_disconnect"
    );
    drop(refused);

    server.stop()?;
    event_loop.stop();
    runner.join().expect("event loop thread panicked");
    println!("done");
    Ok(())
}
