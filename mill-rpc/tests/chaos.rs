use mill_io::EventLoop;
use mill_net::testing::{ChaosConfig, ChaosProxy};
use mill_rpc::prelude::*;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

mill_rpc::service! {
    service Bytes {
        fn echo(data: Vec<u8>) -> Vec<u8>;
        fn slow(millis: u64) -> u64;
    }
}

#[derive(Clone, Default)]
struct BytesImpl {
    calls: Arc<AtomicUsize>,
}

impl bytes::Service for BytesImpl {
    fn echo(&self, _ctx: &RpcContext, data: Vec<u8>) -> Vec<u8> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        data
    }

    fn slow(&self, _ctx: &RpcContext, millis: u64) -> u64 {
        self.calls.fetch_add(1, Ordering::SeqCst);
        thread::sleep(Duration::from_millis(millis));
        millis
    }
}

struct Harness {
    event_loop: Arc<EventLoop>,
    runner: Option<JoinHandle<()>>,
    addr: SocketAddr,
    calls: Arc<AtomicUsize>,
    _server: RpcServer,
}

impl Harness {
    fn start() -> Self {
        let event_loop = Arc::new(EventLoop::new(4, 1024, 10).unwrap());
        let service = BytesImpl::default();
        let calls = Arc::clone(&service.calls);
        let server = RpcServer::builder()
            .bind("127.0.0.1:0".parse().unwrap())
            .service(bytes::server(service))
            .build(&event_loop)
            .unwrap();
        let addr = server.local_addr().unwrap();
        let runner = {
            let event_loop = Arc::clone(&event_loop);
            thread::spawn(move || event_loop.run().unwrap())
        };
        Self {
            event_loop,
            runner: Some(runner),
            addr,
            calls,
            _server: server,
        }
    }

    fn client(&self, addr: SocketAddr) -> Arc<bytes::Client> {
        let transport = RpcClient::connect(addr, &self.event_loop).unwrap();
        transport.set_timeout(Duration::from_secs(10));
        Arc::new(bytes::Client::new(transport, Codec::bincode(), 0))
    }

    fn proxy(&self, cfg: ChaosConfig) -> ChaosProxy {
        ChaosProxy::start(self.addr, cfg).unwrap()
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

fn split(n: usize) -> ChaosConfig {
    ChaosConfig {
        split_writes: Some(n),
        ..ChaosConfig::default()
    }
}

#[test]
fn frames_survive_one_byte_splits() {
    let harness = Harness::start();
    let proxy = harness.proxy(split(1));
    let client = harness.client(proxy.addr());

    for len in [0, 1, 2, 9, 10, 255, 1000, 4096, 16 * 1024] {
        let data: Vec<u8> = (0..len).map(|i| (i * 31 % 251) as u8).collect();
        let reply = client
            .echo(data.clone())
            .unwrap_or_else(|e| panic!("{len}-byte echo failed: {e}"));
        assert!(reply == data, "{len}-byte echo came back different");
    }
    assert_eq!(proxy.connections_seen(), 1);
}

#[test]
fn random_split_patterns() {
    const PATTERNS: u64 = 1000;
    let seed = std::env::var("MILL_RPC_CHAOS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0x6d69_6c6c_u64);

    let harness = Harness::start();
    for pattern in 0..PATTERNS {
        let pattern_seed = seed.wrapping_add(pattern);
        let mut rng = StdRng::seed_from_u64(pattern_seed);
        let chunk = rng.gen_range(1..=64);
        let proxy = harness.proxy(split(chunk));
        let client = harness.client(proxy.addr());
        for call in 0..rng.gen_range(1..=3) {
            let len = rng.gen_range(0..=1500);
            let data: Vec<u8> = (0..len).map(|_| rng.gen()).collect();
            match client.echo(data.clone()) {
                Ok(reply) => assert!(
                    reply == data,
                    "seed {pattern_seed} (MILL_RPC_CHAOS_SEED={seed}, pattern {pattern}): \
                     call {call}, {len} bytes in {chunk}-byte splits came back different"
                ),
                Err(e) => panic!(
                    "seed {pattern_seed} (MILL_RPC_CHAOS_SEED={seed}, pattern {pattern}): \
                     call {call}, {len} bytes in {chunk}-byte splits failed: {e}"
                ),
            }
        }
    }
}

#[test]
fn pending_calls_fail_when_the_connection_drops() {
    let harness = Harness::start();
    let proxy = harness.proxy(ChaosConfig::default());
    let client = harness.client(proxy.addr());
    assert_eq!(client.echo(vec![1, 2, 3]).unwrap(), vec![1, 2, 3]);

    let caller = {
        let client = Arc::clone(&client);
        thread::spawn(move || {
            let result = client.slow(3_000);
            (result, Instant::now())
        })
    };
    let calls = Arc::clone(&harness.calls);
    let deadline = Instant::now() + Duration::from_secs(5);
    while calls.load(Ordering::SeqCst) < 2 {
        assert!(
            Instant::now() < deadline,
            "the slow call never reached the server"
        );
        thread::sleep(Duration::from_millis(1));
    }

    let dropped_at = Instant::now();
    drop(proxy);
    let (result, failed_at) = caller.join().unwrap();
    let err = result.expect_err("the call outlived its connection");
    assert_eq!(err.status, RpcStatus::Unavailable, "{err}");
    assert!(
        failed_at - dropped_at < Duration::from_secs(1),
        "the call failed {:?} after the drop",
        failed_at - dropped_at
    );

    let err = client
        .echo(vec![4])
        .expect_err("a call on a closed connection");
    assert_eq!(err.status, RpcStatus::Unavailable, "{err}");
}

#[test]
fn reset_mid_frame_fails_the_call() {
    let harness = Harness::start();
    let proxy = harness.proxy(ChaosConfig {
        reset_after: Some(100),
        ..ChaosConfig::default()
    });
    let client = harness.client(proxy.addr());

    let started = Instant::now();
    let err = client
        .echo(vec![7; 10_000])
        .expect_err("a call cut off mid-frame succeeded");
    assert_eq!(err.status, RpcStatus::Unavailable, "{err}");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(
        harness.calls.load(Ordering::SeqCst),
        0,
        "the server dispatched a partial frame"
    );
}

#[test]
fn stalled_connection_hits_the_callers_deadline() {
    let harness = Harness::start();
    let proxy = harness.proxy(ChaosConfig {
        stall_after: Some(50),
        ..ChaosConfig::default()
    });
    let transport = RpcClient::connect(proxy.addr(), &harness.event_loop).unwrap();
    transport.set_timeout(Duration::from_millis(300));
    let client = bytes::Client::new(transport, Codec::bincode(), 0);

    let started = Instant::now();
    let err = client
        .echo(vec![9; 1000])
        .expect_err("a call through a stalled connection succeeded");
    let elapsed = started.elapsed();
    assert_eq!(err.status, RpcStatus::DeadlineExceeded, "{err}");
    assert!(
        elapsed >= Duration::from_millis(250) && elapsed < Duration::from_secs(2),
        "{elapsed:?}"
    );
}
