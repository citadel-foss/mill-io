//! Shutting a server down cleanly: `TcpServer::stop` closes the listener and every
//! connection, firing `on_disconnect` for each, and the clients see the close. Then the
//! event loop is stopped and `run()` returns on the main thread.
//!
//! Run with `cargo run -p mill-net --example graceful_shutdown`.

use mill_io::EventLoop;
use mill_net::errors::{NetworkError, Result};
use mill_net::tcp::{
    ConnectionId, NetworkHandler, ServerContext, TcpClient, TcpClientConfig, TcpServer,
    TcpServerConfig,
};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

const CLIENTS: usize = 3;
const WAIT: Duration = Duration::from_secs(5);

#[derive(PartialEq)]
enum Lifecycle {
    Connected,
    Disconnected,
    Failed,
}

struct Tracker {
    side: &'static str,
    events: mpsc::Sender<Lifecycle>,
}

impl Tracker {
    fn record(&self, event: Lifecycle, what: String) {
        println!("[{}] {what}", self.side);
        let _ = self.events.send(event);
    }
}

impl NetworkHandler for Tracker {
    fn on_connect(&self, _ctx: &ServerContext, conn: ConnectionId) -> Result<()> {
        self.record(Lifecycle::Connected, format!("on_connect {conn:?}"));
        Ok(())
    }

    fn on_data(&self, _ctx: &ServerContext, _conn: ConnectionId, _data: &[u8]) -> Result<()> {
        Ok(())
    }

    fn on_disconnect(&self, _ctx: &ServerContext, conn: ConnectionId) -> Result<()> {
        self.record(Lifecycle::Disconnected, format!("on_disconnect {conn:?}"));
        Ok(())
    }

    fn on_error(&self, _ctx: &ServerContext, _conn: Option<ConnectionId>, error: NetworkError) {
        self.record(Lifecycle::Failed, format!("on_error {error}"));
    }
}

fn wait_for(events: &mpsc::Receiver<Lifecycle>, count: usize, wanted: Lifecycle) {
    let mut seen = 0;
    while seen < count {
        let event = events.recv_timeout(WAIT).expect("lifecycle event missing");
        if event == wanted {
            seen += 1;
        }
    }
}

fn main() -> Result<()> {
    let event_loop = Arc::new(EventLoop::default());
    let (events, lifecycle) = mpsc::channel();
    let server = Arc::new(TcpServer::new(
        TcpServerConfig::builder()
            .address("127.0.0.1:0".parse()?)
            .build(),
        Tracker {
            side: "server",
            events: events.clone(),
        },
    )?);
    Arc::clone(&server).start(&event_loop, event_loop.alloc_token())?;
    let addr = server.local_addr()?;

    let mut clients = Vec::new();
    for _ in 0..CLIENTS {
        let mut client = TcpClient::connect(
            addr,
            TcpClientConfig::default(),
            Tracker {
                side: "client",
                events: events.clone(),
            },
        )?;
        client.start(&event_loop)?;
        clients.push(client);
    }

    let shutdown = {
        let event_loop = Arc::clone(&event_loop);
        let server = Arc::clone(&server);
        thread::spawn(move || {
            wait_for(&lifecycle, 2 * CLIENTS, Lifecycle::Connected);
            println!(
                "shutting down with {} open connections",
                server.connection_count()
            );
            server.stop().expect("stop the server");
            println!(
                "server stopped, {} connections left",
                server.connection_count()
            );
            wait_for(&lifecycle, 2 * CLIENTS, Lifecycle::Disconnected);

            let mut late = TcpClient::connect(
                addr,
                TcpClientConfig::default(),
                Tracker {
                    side: "late client",
                    events,
                },
            )
            .expect("start a non-blocking connect");
            late.start(&event_loop).expect("register the late client");
            wait_for(&lifecycle, 1, Lifecycle::Failed);

            println!("all connections closed, stopping the loop");
            event_loop.stop();
        })
    };

    event_loop.run()?;
    shutdown.join().expect("shutdown thread panicked");
    println!(
        "event loop returned, {} connections",
        server.connection_count()
    );
    assert_eq!(server.connection_count(), 0);
    drop(clients);
    Ok(())
}
