use crossbeam_channel::TryRecvError;
use mill_chain::testing::{inject_events, on_reactor, MockReply, MockScript, MockServer};
use mill_chain::{Chain, ChainClient, ChainConfig, ChainError, ChainEvent, EngineOptions};
use serde_json::json;
use std::collections::HashSet;
use std::io::Read;
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::thread;
use std::time::{Duration, Instant};

/// Tests that count the process's threads and fds hold it exclusively.
static PROCESS: RwLock<()> = RwLock::new(());

fn shared() -> RwLockReadGuard<'static, ()> {
    PROCESS.read().unwrap_or_else(|e| e.into_inner())
}

fn exclusive() -> RwLockWriteGuard<'static, ()> {
    PROCESS.write().unwrap_or_else(|e| e.into_inner())
}

const MS: Duration = Duration::from_millis(1);

fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if cond() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(2 * MS);
    }
}

fn spawn(addr: SocketAddr, options: EngineOptions, events_capacity: usize) -> ChainClient {
    let config = ChainConfig::RawLineJsonRpc {
        addr,
        events_capacity,
    };
    Chain::spawn_with(config, options).unwrap()
}

fn connect(server: &MockServer) -> ChainClient {
    spawn(server.addr(), EngineOptions::default(), 64)
}

fn echo() -> MockScript {
    MockScript::new(|request| MockReply::Result(request["params"].clone()))
}

fn methods_seen(server: &MockServer) -> Vec<String> {
    server
        .requests_seen()
        .iter()
        .map(|r| r["method"].as_str().unwrap_or_default().to_string())
        .collect()
}

/// An address nothing listens on.
fn unused_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

#[test]
fn concurrent_requests_correlate() {
    let _gate = shared();
    const THREADS: u64 = 32;
    const CALLS: u64 = 10_000;
    let server = MockServer::start(MockScript::new(|request| {
        let params = request["params"].clone();
        let r = mix(params[0].as_u64().unwrap() * CALLS + params[1].as_u64().unwrap());
        if r % 4 == 0 {
            MockReply::Result(params)
        } else {
            MockReply::After(Duration::from_micros(r % 500), params)
        }
    }))
    .unwrap();
    let client = connect(&server);

    let workers: Vec<_> = (0..THREADS)
        .map(|t| {
            let handle = client.handle();
            thread::spawn(move || {
                for i in 0..CALLS {
                    let params = json!([t, i]);
                    assert_eq!(handle.call_raw("echo", params.clone()), Ok(params));
                }
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }

    let seen = server.requests_seen();
    let ids: HashSet<u64> = seen.iter().map(|r| r["id"].as_u64().unwrap()).collect();
    assert_eq!(seen.len() as u64, THREADS * CALLS);
    assert_eq!(ids.len(), seen.len());
    let stats = client.stats();
    assert_eq!(stats.pending_requests, 0);
    assert_eq!(stats.requests_sent, THREADS * CALLS);
    assert_eq!(server.connection_times().len(), 1);
    client.shutdown().unwrap();
}

#[test]
fn deadline_fires_on_time() {
    let _gate = shared();
    let server = MockServer::start(MockScript::new(|_| MockReply::Never)).unwrap();
    let client = connect(&server);
    let handle = client.handle().deadline(200 * MS);

    let start = Instant::now();
    let result = handle.call_raw("hang", json!([]));
    let elapsed = start.elapsed();

    assert_eq!(result, Err(ChainError::Timeout));
    // One timer tick, the loop's 100 ms poll timeout and 50 ms of scheduling slack.
    assert!(elapsed >= 200 * MS, "{:?}", elapsed);
    assert!(elapsed <= (200 + 1 + 100 + 50) * MS, "{:?}", elapsed);
    assert_eq!(client.stats().pending_requests, 0);
    client.shutdown().unwrap();
}

#[test]
fn cancel_from_another_thread() {
    let _gate = shared();
    let server = MockServer::start(MockScript::new(|request| {
        match request["method"].as_str() {
            Some("slow") => MockReply::After(300 * MS, json!("late")),
            _ => MockReply::After(400 * MS, json!("done")),
        }
    }))
    .unwrap();
    let client = connect(&server);
    let flag = Arc::new(AtomicBool::new(false));
    let handle = client.handle().cancel_on(Arc::clone(&flag));

    let setter = {
        let flag = Arc::clone(&flag);
        thread::spawn(move || {
            thread::sleep(100 * MS);
            flag.store(true, Ordering::SeqCst);
        })
    };
    let start = Instant::now();
    let result = handle.call_raw("slow", json!([]));
    let elapsed = start.elapsed();
    setter.join().unwrap();

    assert_eq!(result, Err(ChainError::Cancelled));
    assert!(elapsed <= 200 * MS, "{:?}", elapsed);
    assert_eq!(client.stats().pending_requests, 0);

    // Its reply goes out 100 ms before this one's, on the same connection, so once this
    // returns the late reply has been read and dropped.
    assert_eq!(
        client.handle().call_raw("next", json!([])),
        Ok(json!("done"))
    );
    assert_eq!(client.stats().pending_requests, 0);
    assert_eq!(methods_seen(&server), ["slow", "next"]);
    client.shutdown().unwrap();
}

#[test]
fn blocking_call_on_reactor_is_refused() {
    let _gate = shared();
    let server = MockServer::start(echo()).unwrap();
    let client = connect(&server);
    let handle = client.handle();

    let outcome = panic::catch_unwind(AssertUnwindSafe(|| {
        on_reactor(&client, move || handle.call_raw("blocked", json!([])))
    }));
    if cfg!(debug_assertions) {
        let payload = outcome.unwrap_err();
        let message = payload
            .downcast_ref::<String>()
            .cloned()
            .unwrap_or_default();
        assert!(message.contains("WouldDeadlock"), "{}", message);
    } else {
        assert_eq!(outcome.unwrap(), Err(ChainError::WouldDeadlock));
    }

    assert_eq!(client.handle().call_raw("ping", json!([1])), Ok(json!([1])));
    assert_eq!(methods_seen(&server), ["ping"]);
    client.shutdown().unwrap();
}

#[test]
fn event_overflow_yields_single_resync() {
    let _gate = shared();
    let server = MockServer::start(echo()).unwrap();
    let client = spawn(server.addr(), EngineOptions::default(), 16);
    let events = client.events();

    inject_events(&client, 10_000);
    let drained: Vec<ChainEvent> = events.try_iter().collect();
    assert_eq!(events.try_recv(), Err(TryRecvError::Empty));

    let (last, before) = drained.split_last().unwrap();
    assert!(matches!(last, ChainEvent::Resync { .. }), "{:?}", last);
    assert!(before.len() <= 16, "{}", before.len());
    for (i, event) in before.iter().enumerate() {
        assert!(
            matches!(event, ChainEvent::BlockDisconnected { height, .. } if *height == i as u64),
            "{:?}",
            event
        );
    }
    assert_eq!(client.stats().resyncs, 1);

    inject_events(&client, 3);
    let after: Vec<ChainEvent> = events.try_iter().collect();
    assert_eq!(after.len(), 3);
    assert!(after
        .iter()
        .all(|e| matches!(e, ChainEvent::BlockDisconnected { .. })));
    assert_eq!(client.stats().resyncs, 1);
    client.shutdown().unwrap();
}

fn thread_count() -> Option<usize> {
    std::fs::read_dir("/proc/self/task").ok().map(|d| d.count())
}

fn fd_count() -> Option<usize> {
    std::fs::read_dir("/proc/self/fd").ok().map(|d| d.count())
}

#[test]
fn shutdown_joins_everything() {
    let _gate = exclusive();
    let server = MockServer::start(MockScript::new(|_| MockReply::Never)).unwrap();
    let (threads_before, fds_before) = (thread_count(), fd_count());

    let client = connect(&server);
    let callers: Vec<_> = (0..100)
        .map(|i| {
            let handle = client.handle();
            thread::spawn(move || handle.call_raw("hang", json!([i])))
        })
        .collect();
    assert!(wait_until(5 * 1000 * MS, || server.requests_seen().len() == 100));

    let start = Instant::now();
    client.shutdown().unwrap();
    assert!(start.elapsed() < 1000 * MS, "{:?}", start.elapsed());
    for caller in callers {
        assert_eq!(caller.join().unwrap(), Err(ChainError::Shutdown));
    }

    if let (Some(threads), Some(fds)) = (threads_before, fds_before) {
        let settled = wait_until(5 * 1000 * MS, || {
            thread_count().unwrap() <= threads + 1 && fd_count().unwrap() <= fds + 1
        });
        assert!(
            settled,
            "threads {} -> {:?}, fds {} -> {:?}",
            threads,
            thread_count(),
            fds,
            fd_count()
        );
    }
}

#[test]
fn only_not_found_is_absence() {
    let _gate = shared();
    let every_variant = [
        ChainError::NotFound,
        ChainError::Transport("reset".into()),
        ChainError::Timeout,
        ChainError::Cancelled,
        ChainError::Shutdown,
        ChainError::Auth,
        ChainError::Server {
            code: -5,
            message: "No such mempool or blockchain transaction".into(),
        },
        ChainError::Protocol("bad".into()),
        ChainError::Unsupported("block_at_height"),
        ChainError::WouldDeadlock,
    ];
    for error in &every_variant {
        assert_eq!(
            error.is_absence(),
            *error == ChainError::NotFound,
            "{:?}",
            error
        );
    }

    let server = MockServer::start(MockScript::new(|request| {
        match request["method"].as_str() {
            Some("drop") => MockReply::Close,
            _ => MockReply::Error {
                code: -5,
                message: "No such mempool or blockchain transaction".into(),
            },
        }
    }))
    .unwrap();
    let client = connect(&server);
    let handle = client.handle();

    let dropped = handle.call_raw("drop", json!([])).unwrap_err();
    assert!(matches!(dropped, ChainError::Transport(_)), "{:?}", dropped);
    assert!(!dropped.is_absence());

    let missing = handle.call_raw("getrawtransaction", json!([])).unwrap_err();
    assert_eq!(
        missing,
        ChainError::Server {
            code: -5,
            message: "No such mempool or blockchain transaction".into()
        }
    );
    assert!(!missing.is_absence());
    client.shutdown().unwrap();
}

/// Accepts connections and closes them at once. Records when each was accepted and when the
/// client let go of it (dropped its end), which is when the client's backoff starts.
struct ClosingListener {
    accepts: Arc<Mutex<Vec<Instant>>>,
    released: Arc<Mutex<Vec<Instant>>>,
    stop: Arc<AtomicBool>,
    addr: SocketAddr,
    thread: Option<thread::JoinHandle<()>>,
}

/// Closes our side of `stream` and waits until the peer closes its side.
fn release(mut stream: TcpStream) -> Instant {
    let _ = stream.shutdown(Shutdown::Write);
    let _ = stream.set_read_timeout(Some(5 * 1000 * MS));
    let mut buf = [0u8; 256];
    while matches!(stream.read(&mut buf), Ok(n) if n > 0) {}
    Instant::now()
}

impl ClosingListener {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let accepts = Arc::new(Mutex::new(Vec::new()));
        let released = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let thread = {
            let (accepts, released) = (Arc::clone(&accepts), Arc::clone(&released));
            let stop = Arc::clone(&stop);
            thread::spawn(move || loop {
                let Ok((stream, _)) = listener.accept() else {
                    continue;
                };
                accepts.lock().unwrap().push(Instant::now());
                if stop.load(Ordering::SeqCst) {
                    // Close the port before the client can see this connection end, so its
                    // next attempt is refused.
                    drop(listener);
                    released.lock().unwrap().push(release(stream));
                    return;
                }
                released.lock().unwrap().push(release(stream));
            })
        };
        Self {
            accepts,
            released,
            stop,
            addr,
            thread: Some(thread),
        }
    }

    fn accepts(&self) -> Vec<Instant> {
        self.accepts.lock().unwrap().clone()
    }

    fn released(&self) -> Vec<Instant> {
        self.released.lock().unwrap().clone()
    }

    /// Takes the client's next attempt, closes the port, then waits until the client has let
    /// go of that last connection. Connects are refused from then on.
    fn stop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}

#[test]
fn reconnect_backoff_and_queued_requests() {
    let _gate = shared();
    let cap = 400 * MS;
    let options = EngineOptions {
        backoff_initial: 50 * MS,
        backoff_max: cap,
        ..EngineOptions::default()
    };
    let mut down = ClosingListener::start();
    let addr = down.addr;
    let client = spawn(addr, options, 16);

    assert!(wait_until(10 * 1000 * MS, || down.accepts().len() >= 7));
    // Each gap runs from the client dropping a connection, when its backoff timer starts, to
    // its next attempt, so the time the loop took to notice the drop is not counted.
    let (accepts, released) = (down.accepts(), down.released());
    let gaps: Vec<Duration> = released
        .iter()
        .zip(&accepts[1..])
        .map(|(released, next)| next.saturating_duration_since(*released))
        .collect();
    assert!(gaps.len() >= 6, "{:?}", gaps);
    // 50, 100, 200 ms, then the 400 ms cap, all scaled by one jitter factor in [0.8, 1.2).
    assert!(gaps[0] >= 40 * MS, "{:?}", gaps);
    for pair in gaps[..3].windows(2) {
        assert!(
            pair[1].as_secs_f64() >= pair[0].as_secs_f64() * 1.5 * 0.8,
            "{:?}",
            gaps
        );
    }
    for gap in &gaps {
        assert!(*gap <= cap + 60 * MS, "{:?}", gaps);
    }
    for gap in &gaps[3..] {
        assert!(*gap >= cap.mul_f64(0.8) - 10 * MS, "{:?}", gaps);
    }

    down.stop();
    assert!(wait_until(5 * 1000 * MS, || client
        .stats()
        .connections_open
        == 0));
    let callers: Vec<_> = (0..5)
        .map(|i| {
            let handle = client.handle().deadline(10 * 1000 * MS);
            thread::spawn(move || handle.call_raw("queued", json!([i])))
        })
        .collect();
    assert!(wait_until(5 * 1000 * MS, || client
        .stats()
        .pending_requests
        == 5));
    assert_eq!(client.stats().connections_open, 0);

    let server = MockServer::start_on(addr, echo()).unwrap();
    for (i, caller) in callers.into_iter().enumerate() {
        assert_eq!(caller.join().unwrap(), Ok(json!([i])));
    }
    assert_eq!(server.requests_seen().len(), 5);
    let stats = client.stats();
    assert_eq!(stats.pending_requests, 0);
    assert_eq!(stats.connections_open, 1);
    assert!(stats.reconnects >= 1);
    client.shutdown().unwrap();
}

/// Closes the connection on the first `drops` requests, then answers "ok".
fn flaky(drops: usize) -> (MockServer, Arc<AtomicUsize>) {
    let seen = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&seen);
    let server = MockServer::start(MockScript::new(move |request| {
        match request["method"].as_str() {
            Some("fail") => MockReply::Error {
                code: -1,
                message: "nope".into(),
            },
            _ if counter.fetch_add(1, Ordering::SeqCst) < drops => MockReply::Close,
            _ => MockReply::Result(json!("ok")),
        }
    }))
    .unwrap();
    (server, seen)
}

#[test]
fn retry_policy_is_respected() {
    let _gate = shared();
    let (server, seen) = flaky(2);
    let client = connect(&server);
    assert_eq!(
        client.handle().retries(2).call_raw("flaky", json!([])),
        Ok(json!("ok"))
    );
    assert_eq!(seen.load(Ordering::SeqCst), 3);
    assert_eq!(client.stats().requests_sent, 3);
    assert_eq!(server.connection_times().len(), 3);

    let server_error = client.handle().retries(5).call_raw("fail", json!([]));
    assert!(matches!(
        server_error,
        Err(ChainError::Server { code: -1, .. })
    ));
    assert_eq!(
        methods_seen(&server)
            .iter()
            .filter(|m| *m == "fail")
            .count(),
        1
    );
    client.shutdown().unwrap();

    let (server, seen) = flaky(1);
    let client = connect(&server);
    let result = client.handle().retries(0).call_raw("flaky", json!([]));
    assert!(
        matches!(result, Err(ChainError::Transport(_))),
        "{:?}",
        result
    );
    assert_eq!(seen.load(Ordering::SeqCst), 1);

    let (server2, _) = flaky(3);
    let client2 = spawn(server2.addr(), EngineOptions::default(), 16);
    let exhausted = client2.handle().retries(2).call_raw("flaky", json!([]));
    assert!(
        matches!(exhausted, Err(ChainError::Transport(_))),
        "{:?}",
        exhausted
    );
    assert_eq!(server2.requests_seen().len(), 3);
    client.shutdown().unwrap();
    client2.shutdown().unwrap();
}

#[test]
fn reply_for_unknown_id_is_ignored() {
    let _gate = shared();
    let server = MockServer::start(MockScript::new(|request| {
        MockReply::Raw(format!(
            "{}\n{}",
            json!({"jsonrpc": "2.0", "id": 987_654_321, "result": "stray"}),
            json!({"jsonrpc": "2.0", "id": request["id"], "result": "mine"})
        ))
    }))
    .unwrap();
    let client = connect(&server);
    assert_eq!(client.handle().call_raw("a", json!([])), Ok(json!("mine")));
    assert_eq!(client.handle().call_raw("b", json!([])), Ok(json!("mine")));
    assert_eq!(client.stats().pending_requests, 0);
    client.shutdown().unwrap();
}

#[test]
fn malformed_replies_fail_only_their_request() {
    let _gate = shared();
    let server = MockServer::start(MockScript::new(|request| {
        match request["method"].as_str() {
            Some("garbage") => MockReply::Raw("{this is not json".into()),
            Some("shape") => MockReply::Raw(json!({"id": request["id"], "x": 1}).to_string()),
            Some("bad_error") => {
                MockReply::Raw(json!({"id": request["id"], "error": "boom"}).to_string())
            }
            _ => MockReply::After(200 * MS, request["params"].clone()),
        }
    }))
    .unwrap();
    let client = connect(&server);

    let good = {
        let handle = client.handle();
        thread::spawn(move || handle.call_raw("good", json!([7])))
    };
    assert!(wait_until(5 * 1000 * MS, || server.requests_seen().len() == 1));
    let handle = client.handle().deadline(150 * MS);
    assert_eq!(
        handle.call_raw("garbage", json!([])),
        Err(ChainError::Timeout)
    );
    assert!(matches!(
        handle.call_raw("shape", json!([])),
        Err(ChainError::Protocol(_))
    ));
    assert!(matches!(
        handle.call_raw("bad_error", json!([])),
        Err(ChainError::Protocol(_))
    ));
    assert_eq!(good.join().unwrap(), Ok(json!([7])));
    assert_eq!(server.connection_times().len(), 1);
    assert_eq!(client.stats().reconnects, 0);
    client.shutdown().unwrap();
}

#[test]
fn deadline_and_cancel_race() {
    let _gate = shared();
    let server = MockServer::start(MockScript::new(|_| MockReply::Never)).unwrap();
    let client = connect(&server);
    for _ in 0..50 {
        let flag = Arc::new(AtomicBool::new(false));
        let handle = client
            .handle()
            .deadline(20 * MS)
            .cancel_on(Arc::clone(&flag));
        let setter = thread::spawn(move || {
            thread::sleep(20 * MS);
            flag.store(true, Ordering::SeqCst);
        });
        let result = handle.call_raw("race", json!([]));
        setter.join().unwrap();
        assert!(
            matches!(
                result,
                Err(ChainError::Timeout) | Err(ChainError::Cancelled)
            ),
            "{:?}",
            result
        );
    }
    assert!(wait_until(1000 * MS, || client.stats().pending_requests == 0));
    client.shutdown().unwrap();
}

#[test]
fn thousand_handles_share_one_connection() {
    let _gate = shared();
    let server = MockServer::start(MockScript::new(|request| {
        MockReply::After(100 * MS, request["params"].clone())
    }))
    .unwrap();
    let client = connect(&server);
    let callers: Vec<_> = (0..1000u64)
        .map(|i| {
            let deadline = if i % 2 == 0 {
                Duration::from_millis(10 + i % 30)
            } else {
                Duration::from_millis(5000 + i)
            };
            let handle = client.handle().deadline(deadline);
            thread::Builder::new()
                .stack_size(128 * 1024)
                .spawn(move || (i, handle.call_raw("echo", json!([i]))))
                .unwrap()
        })
        .collect();
    for caller in callers {
        let (i, result) = caller.join().unwrap();
        if i % 2 == 0 {
            assert_eq!(result, Err(ChainError::Timeout));
        } else {
            assert_eq!(result, Ok(json!([i])));
        }
    }
    assert_eq!(server.connection_times().len(), 1);
    assert_eq!(client.stats().pending_requests, 0);
    client.shutdown().unwrap();
}

#[test]
fn shutdown_from_a_clone_while_a_call_blocks() {
    let _gate = shared();
    let server = MockServer::start(MockScript::new(|_| MockReply::Never)).unwrap();
    let client = connect(&server);
    let events = client.events();
    let handle = client.handle();
    let blocked = thread::spawn(move || handle.call_raw("hang", json!([])));
    assert!(wait_until(5 * 1000 * MS, || server.requests_seen().len() == 1));

    client.clone().shutdown().unwrap();
    assert_eq!(blocked.join().unwrap(), Err(ChainError::Shutdown));
    assert_eq!(
        client.handle().call_raw("after", json!([])),
        Err(ChainError::Shutdown)
    );
    assert!(events.recv().is_err());
    client.shutdown().unwrap();
}

#[test]
fn preset_cancel_flag_never_reaches_the_wire() {
    let _gate = shared();
    let server = MockServer::start(echo()).unwrap();
    let client = connect(&server);
    let flag = Arc::new(AtomicBool::new(true));
    let cancelled = client.handle().cancel_on(Arc::clone(&flag));
    assert_eq!(
        cancelled.call_raw("never", json!([])),
        Err(ChainError::Cancelled)
    );
    assert_eq!(
        client
            .handle()
            .deadline(Duration::ZERO)
            .call_raw("never", json!([])),
        Err(ChainError::Timeout)
    );
    flag.store(false, Ordering::SeqCst);
    assert_eq!(cancelled.call_raw("sent", json!([1])), Ok(json!([1])));
    assert_eq!(methods_seen(&server), ["sent"]);
    client.shutdown().unwrap();
}

#[test]
fn unreachable_server_queues_then_fails() {
    let _gate = shared();
    let options = EngineOptions {
        reconnect_queue_limit: 2,
        ..EngineOptions::default()
    };
    let client = spawn(unused_addr(), options, 16);
    let queued: Vec<_> = (0..2)
        .map(|_| {
            let handle = client.handle().deadline(300 * MS);
            thread::spawn(move || handle.call_raw("queued", json!([])))
        })
        .collect();
    assert!(wait_until(1000 * MS, || client.stats().pending_requests == 2));

    let start = Instant::now();
    let full = client.handle().call_raw("third", json!([]));
    assert_eq!(
        full,
        Err(ChainError::Transport("reconnect queue full".into()))
    );
    assert!(start.elapsed() < 100 * MS, "{:?}", start.elapsed());

    for caller in queued {
        assert_eq!(caller.join().unwrap(), Err(ChainError::Timeout));
    }
    let stats = client.stats();
    assert_eq!(stats.pending_requests, 0);
    assert_eq!(stats.requests_sent, 0);
    assert_eq!(stats.connections_open, 0);
    client.shutdown().unwrap();
}

#[test]
fn restart_drops_replies_and_reconnects() {
    let _gate = shared();
    let server = MockServer::start(MockScript::new(|request| {
        match request["method"].as_str() {
            Some("slow") => MockReply::After(300 * MS, json!("late")),
            _ => MockReply::Result(request["params"].clone()),
        }
    }))
    .unwrap();
    let client = connect(&server);
    let slow = {
        let handle = client.handle();
        thread::spawn(move || handle.call_raw("slow", json!([])))
    };
    assert!(wait_until(5 * 1000 * MS, || server.requests_seen().len() == 1));
    server.restart();

    let result = slow.join().unwrap();
    assert!(
        matches!(result, Err(ChainError::Transport(_))),
        "{:?}",
        result
    );
    assert_eq!(client.handle().call_raw("ok", json!([2])), Ok(json!([2])));
    assert_eq!(server.connection_times().len(), 2);
    assert_eq!(client.stats().reconnects, 1);
    client.shutdown().unwrap();
}

#[test]
fn slow_reader_backpressure_delays_but_delivers() {
    let _gate = shared();
    let stalled = Arc::new(AtomicBool::new(false));
    let first = Arc::clone(&stalled);
    let server = MockServer::start(MockScript::new(move |request| {
        // Stalls the server's reader so the client's write queue backs up.
        if !first.swap(true, Ordering::SeqCst) {
            thread::sleep(500 * MS);
        }
        MockReply::Result(json!(request["params"][0].as_str().map(str::len)))
    }))
    .unwrap();
    let client = connect(&server);
    let big = "x".repeat(256 * 1024);
    let callers: Vec<_> = (0..40)
        .map(|_| {
            let (handle, big) = (client.handle(), big.clone());
            thread::spawn(move || handle.call_raw("big", json!([big])))
        })
        .collect();
    for caller in callers {
        assert_eq!(caller.join().unwrap(), Ok(json!(256 * 1024)));
    }
    assert!(stalled.load(Ordering::SeqCst));
    assert_eq!(client.stats().requests_sent, 40);
    assert_eq!(server.connection_times().len(), 1);
    client.shutdown().unwrap();
}

#[test]
fn backoff_resets_once_a_connection_answers() {
    let _gate = shared();
    let server = MockServer::start(MockScript::new(|request| {
        match request["method"].as_str() {
            Some("drop") => MockReply::Close,
            _ => MockReply::Result(json!("ok")),
        }
    }))
    .unwrap();
    let options = EngineOptions {
        backoff_initial: 100 * MS,
        backoff_max: 5000 * MS,
        ..EngineOptions::default()
    };
    let client = spawn(server.addr(), options, 16);
    let handle = client.handle();
    for _ in 0..3 {
        assert!(matches!(
            handle.call_raw("drop", json!([])),
            Err(ChainError::Transport(_))
        ));
    }
    // Three drops without an answer: the next wait is 800 ms, scaled by the jitter.
    assert_eq!(handle.call_raw("ok", json!([])), Ok(json!("ok")));

    assert!(matches!(
        handle.call_raw("drop", json!([])),
        Err(ChainError::Transport(_))
    ));
    let dropped_at = Instant::now();
    let connections = server.connection_times().len();
    assert!(wait_until(5000 * MS, || server.connection_times().len() > connections));
    let waited = server.connection_times()[connections] - dropped_at;
    assert!(waited < 250 * MS, "{:?}", waited);
    client.shutdown().unwrap();
}
