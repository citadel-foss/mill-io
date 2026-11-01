//! `testing::ChaosProxy` between a client and an echo server: it forwards in writes of at
//! most 16 bytes and throttles the client's upload to 8 KiB/s. The echo still arrives
//! whole, but over many `on_data` calls on both sides and only as fast as the throttle allows.
//!
//! Run with `cargo run -p mill-net --example chaos_proxy --features testing`.

use mill_io::EventLoop;
use mill_net::errors::Result;
use mill_net::tcp::{
    ConnectionId, NetworkHandler, ServerContext, TcpClient, TcpClientConfig, TcpServer,
    TcpServerConfig,
};
use mill_net::testing::{ChaosConfig, ChaosProxy};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const MESSAGE_BYTES: usize = 4096;

struct Echo {
    reads: Arc<AtomicUsize>,
}

impl NetworkHandler for Echo {
    fn on_data(&self, ctx: &ServerContext, conn: ConnectionId, data: &[u8]) -> Result<()> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        ctx.send_to(conn, data)
    }
}

struct Collector {
    reads: AtomicUsize,
    received: Mutex<Vec<u8>>,
    complete: mpsc::Sender<(usize, Vec<u8>)>,
}

impl NetworkHandler for Collector {
    fn on_data(&self, _ctx: &ServerContext, _conn: ConnectionId, data: &[u8]) -> Result<()> {
        let reads = self.reads.fetch_add(1, Ordering::SeqCst) + 1;
        let mut received = self.received.lock().unwrap();
        received.extend_from_slice(data);
        if received.len() == MESSAGE_BYTES {
            let _ = self.complete.send((reads, std::mem::take(&mut received)));
        }
        Ok(())
    }
}

fn main() -> Result<()> {
    let event_loop = Arc::new(EventLoop::default());
    let server_reads = Arc::new(AtomicUsize::new(0));
    let server = Arc::new(TcpServer::new(
        TcpServerConfig::builder()
            .address("127.0.0.1:0".parse()?)
            .build(),
        Echo {
            reads: Arc::clone(&server_reads),
        },
    )?);
    Arc::clone(&server).start(&event_loop, event_loop.alloc_token())?;

    let proxy = ChaosProxy::start(
        server.local_addr()?,
        ChaosConfig {
            split_writes: Some(16),
            read_rate_bps: Some(8 * 1024),
            ..ChaosConfig::default()
        },
    )?;
    println!(
        "echo server on {}, chaos proxy on {}",
        server.local_addr()?,
        proxy.addr()
    );

    let runner = {
        let event_loop = Arc::clone(&event_loop);
        thread::spawn(move || event_loop.run().expect("event loop failed"))
    };

    let (complete, echoed) = mpsc::channel();
    let mut client = TcpClient::connect(
        proxy.addr(),
        TcpClientConfig::default(),
        Collector {
            reads: AtomicUsize::new(0),
            received: Mutex::new(Vec::new()),
            complete,
        },
    )?;
    client.start(&event_loop)?;

    let message: Vec<u8> = (0..MESSAGE_BYTES).map(|i| (i % 251) as u8).collect();
    let started = Instant::now();
    client.send(&message)?;
    let (client_reads, echo) = echoed.recv_timeout(Duration::from_secs(10))?;
    let elapsed = started.elapsed();

    println!(
        "sent {MESSAGE_BYTES} bytes in one send; the echo took {:.2} s",
        elapsed.as_secs_f64()
    );
    println!(
        "server on_data calls: {}, client on_data calls: {client_reads}",
        server_reads.load(Ordering::SeqCst)
    );
    println!("proxy connections seen: {}", proxy.connections_seen());
    assert_eq!(echo, message);
    assert!(client_reads > 1, "the proxy should have split the echo");

    drop(client);
    drop(proxy);
    server.stop()?;
    event_loop.stop();
    runner.join().expect("event loop thread panicked");
    Ok(())
}
