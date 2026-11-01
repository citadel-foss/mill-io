//! A server that pushes data from outside its handlers with `ConnSender`: a repeating
//! timer broadcasts ticks to subscribers, and a producer thread streams a download.
//!
//! The downloading client pauses its reads, so the server's queue reaches the high
//! watermark and `send` returns `SendError::Backpressure`. The producer waits for
//! `on_drained`, ticks keep flowing to the other client meanwhile, and the download
//! finishes once the client resumes reading.
//!
//! Run with `cargo run -p mill-net --example broadcast_sender`.

use mill_io::EventLoop;
use mill_net::errors::{NetworkError, Result};
use mill_net::tcp::{
    ConnSender, ConnectionId, NetworkHandler, SendError, ServerContext, TcpClient, TcpClientConfig,
    TcpServer, TcpServerConfig,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;

const DOWNLOAD_BYTES: usize = 32 * 1024 * 1024;
const CHUNK_BYTES: usize = 64 * 1024;
const HIGH_WATERMARK: usize = 256 * 1024;
const LOW_WATERMARK: usize = 64 * 1024;
const WAIT: Duration = Duration::from_secs(10);

type Subscribers = Arc<Mutex<Vec<ConnSender>>>;

struct Hub {
    subscribers: Subscribers,
    downloads: mpsc::Sender<ConnSender>,
    drained: mpsc::Sender<ConnectionId>,
}

impl NetworkHandler for Hub {
    fn on_data(&self, ctx: &ServerContext, conn: ConnectionId, data: &[u8]) -> Result<()> {
        let Some(sender) = ctx.sender(conn) else {
            return Ok(());
        };
        match data {
            b"subscribe" => self.subscribers.lock().unwrap().push(sender),
            b"download" => {
                let _ = self.downloads.send(sender);
            }
            other => println!("[server] ignoring {:?}", String::from_utf8_lossy(other)),
        }
        Ok(())
    }

    fn on_drained(&self, _ctx: &ServerContext, conn: ConnectionId) -> Result<()> {
        let _ = self.drained.send(conn);
        Ok(())
    }

    fn on_disconnect(&self, _ctx: &ServerContext, conn: ConnectionId) -> Result<()> {
        self.subscribers
            .lock()
            .unwrap()
            .retain(|sender| sender.id() != conn);
        Ok(())
    }

    fn on_error(&self, _ctx: &ServerContext, conn: Option<ConnectionId>, error: NetworkError) {
        eprintln!("[server] error on {conn:?}: {error}");
    }
}

fn broadcast_tick(subscribers: &Subscribers, tick: usize) {
    let line = format!("tick {tick}\n");
    subscribers
        .lock()
        .unwrap()
        .retain(|sender| match sender.send(line.as_bytes()) {
            Ok(()) => true,
            Err(SendError::Backpressure) => {
                println!("[server] {:?} is behind, skipping tick {tick}", sender.id());
                true
            }
            Err(_) => false,
        });
}

struct Download {
    sent: usize,
    refusals: usize,
}

fn stream_download(
    sender: ConnSender,
    drained: mpsc::Receiver<ConnectionId>,
    first_refusal: mpsc::Sender<usize>,
) -> Download {
    let chunk = vec![0x42; CHUNK_BYTES];
    let mut download = Download {
        sent: 0,
        refusals: 0,
    };
    while download.sent < DOWNLOAD_BYTES {
        match sender.send(&chunk) {
            Ok(()) => download.sent += chunk.len(),
            Err(SendError::Backpressure) => {
                download.refusals += 1;
                if download.refusals == 1 {
                    let _ = first_refusal.send(download.sent);
                }
                drained.recv_timeout(WAIT).expect("on_drained did not fire");
            }
            Err(e) => panic!("download failed: {e}"),
        }
    }
    download
}

struct Subscriber {
    lines: mpsc::Sender<String>,
}

impl NetworkHandler for Subscriber {
    fn on_connect(&self, ctx: &ServerContext, conn: ConnectionId) -> Result<()> {
        ctx.send_to(conn, b"subscribe")
    }

    fn on_data(&self, _ctx: &ServerContext, _conn: ConnectionId, data: &[u8]) -> Result<()> {
        for line in String::from_utf8_lossy(data).lines() {
            println!("[subscriber] {line}");
            let _ = self.lines.send(line.to_owned());
        }
        Ok(())
    }
}

struct Downloader {
    received: Arc<AtomicUsize>,
    complete: mpsc::Sender<()>,
}

impl NetworkHandler for Downloader {
    fn on_connect(&self, ctx: &ServerContext, conn: ConnectionId) -> Result<()> {
        if let Some(sender) = ctx.sender(conn) {
            sender.pause_reads();
        }
        ctx.send_to(conn, b"download")
    }

    fn on_data(&self, _ctx: &ServerContext, _conn: ConnectionId, data: &[u8]) -> Result<()> {
        let total = self.received.fetch_add(data.len(), Ordering::SeqCst) + data.len();
        if total == DOWNLOAD_BYTES {
            let _ = self.complete.send(());
        }
        Ok(())
    }
}

fn main() -> Result<()> {
    let event_loop = Arc::new(EventLoop::default());
    let subscribers = Subscribers::default();
    let (downloads, download_requests) = mpsc::channel();
    let (drained, drained_events) = mpsc::channel();

    let config = TcpServerConfig::builder()
        .address("127.0.0.1:0".parse()?)
        .high_watermark(HIGH_WATERMARK)
        .low_watermark(LOW_WATERMARK)
        .build();
    let server = Arc::new(TcpServer::new(
        config,
        Hub {
            subscribers: Arc::clone(&subscribers),
            downloads,
            drained,
        },
    )?);
    Arc::clone(&server).start(&event_loop, event_loop.alloc_token())?;
    let addr = server.local_addr()?;
    println!(
        "[server] on {addr}, watermarks: high {} KiB, low {} KiB",
        HIGH_WATERMARK / 1024,
        LOW_WATERMARK / 1024
    );

    let ticks = AtomicUsize::new(0);
    let ticker = {
        let subscribers = Arc::clone(&subscribers);
        event_loop.schedule_repeating(Duration::from_millis(20), move || {
            broadcast_tick(&subscribers, ticks.fetch_add(1, Ordering::SeqCst) + 1);
        })
    };

    let runner = {
        let event_loop = Arc::clone(&event_loop);
        thread::spawn(move || event_loop.run().expect("event loop failed"))
    };

    let (lines, tick_lines) = mpsc::channel();
    let mut subscriber =
        TcpClient::connect(addr, TcpClientConfig::default(), Subscriber { lines })?;
    subscriber.start(&event_loop)?;

    let received = Arc::new(AtomicUsize::new(0));
    let (complete, download_complete) = mpsc::channel();
    let mut downloader = TcpClient::connect(
        addr,
        TcpClientConfig::default(),
        Downloader {
            received: Arc::clone(&received),
            complete,
        },
    )?;
    downloader.start(&event_loop)?;

    let (first_refusal, refused_at) = mpsc::channel();
    let producer = {
        let sender = download_requests.recv_timeout(WAIT)?;
        thread::spawn(move || stream_download(sender, drained_events, first_refusal))
    };

    let refused_after = refused_at.recv_timeout(WAIT)?;
    println!(
        "[producer] Backpressure after {} KiB while the downloader's reads are paused",
        refused_after / 1024
    );
    for _ in 0..3 {
        tick_lines.recv_timeout(WAIT)?;
    }
    println!("[downloader] resuming reads");
    downloader.sender().resume_reads();

    download_complete.recv_timeout(WAIT)?;
    let download = producer.join().expect("producer panicked");
    println!(
        "[producer] sent {} MiB, refused {} times, resumed after each on_drained",
        download.sent / (1024 * 1024),
        download.refusals
    );
    println!(
        "[downloader] received {} MiB",
        received.load(Ordering::SeqCst) / (1024 * 1024)
    );

    event_loop.cancel_timer(ticker);
    drop(subscriber);
    drop(downloader);
    server.stop()?;
    event_loop.stop();
    runner.join().expect("event loop thread panicked");

    assert!(download.refusals >= 1);
    assert_eq!(received.load(Ordering::SeqCst), DOWNLOAD_BYTES);
    Ok(())
}
