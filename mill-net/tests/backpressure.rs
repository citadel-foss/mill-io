use mill_net::errors::Result;
use mill_net::tcp::config::TcpServerConfig;
use mill_net::tcp::{
    ConnectionId, NetworkHandler, SendError, ServerContext, TcpClient, TcpClientConfig, TcpServer,
};
use mill_net::testing::{ChaosConfig, ChaosProxy};
use parking_lot::{Condvar, Mutex};
use sha2::{Digest, Sha256};
use std::io::Read;
use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

mod common;

use common::{pool_and_direct, spawn_loop, stop_loop, wait_until};

#[derive(Default)]
struct Drains {
    count: Mutex<usize>,
    changed: Condvar,
}

impl Drains {
    fn get(&self) -> usize {
        *self.count.lock()
    }

    /// Waits until more than `seen` drains happened; false at `deadline`.
    fn wait_past(&self, seen: usize, deadline: Instant) -> bool {
        let mut count = self.count.lock();
        while *count <= seen {
            if self.changed.wait_until(&mut count, deadline).timed_out() {
                return *count > seen;
            }
        }
        true
    }
}

#[derive(Clone, Default)]
struct DrainCounter {
    drains: Arc<Drains>,
    connected: Arc<AtomicUsize>,
}

impl NetworkHandler for DrainCounter {
    fn on_connect(&self, _ctx: &ServerContext, _conn: ConnectionId) -> Result<()> {
        self.connected.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn on_data(&self, _ctx: &ServerContext, _conn: ConnectionId, _data: &[u8]) -> Result<()> {
        Ok(())
    }

    fn on_drained(&self, _ctx: &ServerContext, _conn: ConnectionId) -> Result<()> {
        *self.drains.count.lock() += 1;
        self.drains.changed.notify_all();
        Ok(())
    }
}

#[derive(Clone, Default)]
struct Hasher {
    state: Arc<Mutex<(Sha256, usize)>>,
}

impl NetworkHandler for Hasher {
    fn on_data(&self, _ctx: &ServerContext, _conn: ConnectionId, data: &[u8]) -> Result<()> {
        let mut state = self.state.lock();
        state.0.update(data);
        state.1 += data.len();
        Ok(())
    }
}

fn payload(len: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

#[test]
fn throttled_reader_receives_10mb_intact() {
    const TOTAL: usize = 10 * 1024 * 1024;
    const CHUNK: usize = 64 * 1024;

    for (mode, event_loop) in pool_and_direct() {
        let hasher = Hasher::default();
        let received = Arc::clone(&hasher.state);
        let config = TcpServerConfig::builder()
            .address("127.0.0.1:0".parse().unwrap())
            .build();
        let server = Arc::new(TcpServer::new(config, hasher).unwrap());
        Arc::clone(&server)
            .start(&event_loop, event_loop.alloc_token())
            .unwrap();

        let proxy = ChaosProxy::start(
            server.local_addr().unwrap(),
            ChaosConfig {
                read_rate_bps: Some(1024),
                throttle_for: Some(Duration::from_secs(5)),
                ..ChaosConfig::default()
            },
        )
        .unwrap();

        let counter = DrainCounter::default();
        let drains = Arc::clone(&counter.drains);
        let connected = Arc::clone(&counter.connected);
        let client_config = TcpClientConfig {
            high_watermark: 256 * 1024,
            low_watermark: 64 * 1024,
            ..TcpClientConfig::default()
        };
        let mut client = TcpClient::connect(proxy.addr(), client_config, counter).unwrap();
        client.start(&event_loop).unwrap();
        let runner = spawn_loop(&event_loop);
        assert!(
            wait_until(Duration::from_secs(5), || connected.load(Ordering::SeqCst)
                == 1),
            "{mode}: client did not connect"
        );

        let data = payload(TOTAL, 0x5eed);
        let expected = Sha256::digest(&data);
        let sender = client.sender();
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut backpressured = 0;
        for chunk in data.chunks(CHUNK) {
            loop {
                let seen = drains.get();
                match sender.send(chunk) {
                    Ok(()) => break,
                    Err(SendError::Backpressure) => {
                        backpressured += 1;
                        assert!(
                            drains.wait_past(seen, deadline),
                            "{mode}: on_drained never fired after a Backpressure"
                        );
                    }
                    Err(e) => panic!("{mode}: send failed: {e}"),
                }
            }
        }

        assert!(
            wait_until(Duration::from_secs(60), || received.lock().1 >= TOTAL),
            "{mode}: server received {} of {TOTAL} bytes",
            received.lock().1
        );
        let (digest, len) = {
            let state = received.lock();
            (state.0.clone().finalize(), state.1)
        };
        assert_eq!(len, TOTAL, "{mode}: byte count");
        assert_eq!(digest, expected, "{mode}: SHA-256 differs");
        assert!(backpressured >= 1, "{mode}: no Backpressure seen");
        assert!(drains.get() >= 1, "{mode}: no on_drained seen");

        stop_loop(&event_loop, runner);
        drop((client, proxy, server));
    }
}

#[test]
fn backpressure_drained_backpressure_cycle() {
    const BIG: usize = 32 * 1024 * 1024;

    for (mode, event_loop) in pool_and_direct() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let counter = DrainCounter::default();
        let drains = Arc::clone(&counter.drains);
        let connected = Arc::clone(&counter.connected);
        let config = TcpClientConfig {
            high_watermark: 64 * 1024,
            low_watermark: 16 * 1024,
            ..TcpClientConfig::default()
        };
        let mut client =
            TcpClient::connect(listener.local_addr().unwrap(), config, counter).unwrap();
        client.start(&event_loop).unwrap();
        let runner = spawn_loop(&event_loop);
        let (mut peer, _) = listener.accept().unwrap();
        assert!(wait_until(Duration::from_secs(5), || connected
            .load(Ordering::SeqCst)
            == 1));

        let sender = client.sender();
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut read_total = 0;
        let mut buf = vec![0; 1024 * 1024];
        for round in 0..2 {
            // Accepted although it is far above the high watermark: the queue was below it.
            sender.send(&vec![round as u8; BIG]).unwrap();
            let mut sent = BIG;
            // The kernel may take the whole frame (Windows accepts any send while its buffer
            // is below SO_SNDBUF), so keep sending until the queue itself refuses.
            let chunk = vec![round as u8; 64 * 1024];
            loop {
                match sender.send(&chunk) {
                    Ok(()) => sent += chunk.len(),
                    Err(SendError::Backpressure) => break,
                    Err(e) => panic!("{mode}: round {round}: {e}"),
                }
                assert!(
                    sent < 1024 * 1024 * 1024,
                    "{mode}: round {round}: 1 GiB accepted without a Backpressure"
                );
            }
            assert!(
                matches!(sender.send(b"x"), Err(SendError::Backpressure)),
                "{mode}: round {round}: a send above the high watermark was queued"
            );
            assert_eq!(drains.get(), round, "{mode}: drained before the peer read");

            let target = read_total + sent;
            while read_total < target {
                read_total += peer.read(&mut buf).unwrap();
            }
            assert!(
                drains.wait_past(round, deadline),
                "{mode}: round {round}: no on_drained"
            );
        }
        assert_eq!(
            drains.get(),
            2,
            "{mode}: on_drained fired more than once per refusal"
        );

        sender.send(b"tail").unwrap();
        let mut tail = [0; 4];
        peer.read_exact(&mut tail).unwrap();
        assert_eq!(&tail, b"tail", "{mode}: the refused byte was sent");

        stop_loop(&event_loop, runner);
        drop(client);
    }
}
