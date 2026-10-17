#![cfg(feature = "testing")]

use mill_net::testing::{ChaosConfig, ChaosProxy};
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::{Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const DEADLINE: Duration = Duration::from_secs(10);

// `drop_stops_threads` counts the process's threads and fds, which other tests disturb when
// they share its process (`cargo test`; nextest runs each test in its own).
fn serial() -> MutexGuard<'static, ()> {
    static SERIAL: Mutex<()> = Mutex::new(());
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

fn wait_until(timeout: Duration, what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(5));
    }
}

fn pattern(len: usize) -> Vec<u8> {
    let mut x: u32 = 0x9e37_79b9;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            x as u8
        })
        .collect()
}

fn serve<T, F>(conns: usize, handler: F) -> (SocketAddr, JoinHandle<Vec<T>>)
where
    T: Send + 'static,
    F: Fn(TcpStream) -> T + Send + Sync + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = thread::spawn(move || {
        let handler = &handler;
        thread::scope(|scope| {
            let workers: Vec<_> = (0..conns)
                .map(|_| {
                    let (stream, _) = listener.accept().unwrap();
                    stream.set_read_timeout(Some(DEADLINE)).unwrap();
                    scope.spawn(move || handler(stream))
                })
                .collect();
            workers.into_iter().map(|w| w.join().unwrap()).collect()
        })
    });
    (addr, handle)
}

fn echo(mut stream: TcpStream) {
    let mut buf = [0u8; 16 * 1024];
    loop {
        match stream.read(&mut buf) {
            Ok(0) | Err(_) => return,
            Ok(n) => {
                if stream.write_all(&buf[..n]).is_err() {
                    return;
                }
            }
        }
    }
}

fn read_sizes(stream: &mut TcpStream, len: usize) -> (Vec<u8>, Vec<usize>) {
    let mut data = Vec::with_capacity(len);
    let mut sizes = Vec::new();
    let mut buf = vec![0u8; 64 * 1024];
    while data.len() < len {
        let want = buf.len().min(len - data.len());
        let n = stream.read(&mut buf[..want]).unwrap();
        assert_ne!(n, 0, "EOF after {} of {len} bytes", data.len());
        data.extend_from_slice(&buf[..n]);
        sizes.push(n);
    }
    (data, sizes)
}

fn connect(proxy: &ChaosProxy) -> TcpStream {
    let stream = TcpStream::connect(proxy.addr()).unwrap();
    stream.set_read_timeout(Some(DEADLINE)).unwrap();
    stream
}

fn read_until_closed(stream: &mut TcpStream) {
    let mut buf = [0u8; 4096];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => return,
            Ok(_) => continue,
            Err(e) if e.kind() == io::ErrorKind::ConnectionReset => return,
            Err(e) => panic!("expected the connection to close, got {e}"),
        }
    }
}

#[cfg(target_os = "linux")]
fn thread_names() -> Vec<String> {
    std::fs::read_dir("/proc/self/task")
        .unwrap()
        .filter_map(|task| std::fs::read_to_string(task.ok()?.path().join("comm")).ok())
        .map(|name| name.trim_end().to_owned())
        .collect()
}

#[cfg(target_os = "linux")]
fn proxy_threads() -> Vec<String> {
    thread_names()
        .into_iter()
        .filter(|name| name.starts_with("chaos-"))
        .collect()
}

#[cfg(unix)]
fn open_fds() -> usize {
    std::fs::read_dir("/dev/fd").unwrap().count()
}

#[test]
fn forwards_bytes_unchanged() {
    let _serial = serial();
    const LEN: usize = 1024 * 1024;
    let (upstream, server) = serve(1, echo);
    let proxy = ChaosProxy::start(upstream, ChaosConfig::default()).unwrap();

    let mut client = connect(&proxy);
    let sent = pattern(LEN);
    let writer = {
        let mut client = client.try_clone().unwrap();
        let sent = sent.clone();
        thread::spawn(move || {
            client.write_all(&sent).unwrap();
            client.shutdown(Shutdown::Write).unwrap();
        })
    };
    let (received, _) = read_sizes(&mut client, LEN);
    writer.join().unwrap();

    assert!(received == sent, "1 MB came back changed");
    assert_eq!(client.read(&mut [0u8; 1]).unwrap(), 0);
    server.join().unwrap();
}

/// (stress)
#[test]
fn splits_writes_into_chunks() {
    let _serial = serial();
    const LEN: usize = 16 * 1024;
    const SPLIT: usize = 1;
    let sent = pattern(LEN);

    let (upstream, server) = {
        let sent = sent.clone();
        serve(1, move |mut stream| {
            let received = read_sizes(&mut stream, LEN);
            stream.write_all(&sent).unwrap();
            received
        })
    };
    let proxy = ChaosProxy::start(
        upstream,
        ChaosConfig {
            split_writes: Some(SPLIT),
            ..ChaosConfig::default()
        },
    )
    .unwrap();

    let mut client = connect(&proxy);
    client.write_all(&sent).unwrap();
    let (to_client, to_client_sizes) = read_sizes(&mut client, LEN);
    let (to_server, to_server_sizes) = server.join().unwrap().pop().unwrap();

    assert!(to_server == sent, "client-to-server bytes changed");
    assert!(to_client == sent, "server-to-client bytes changed");
    // TCP keeps no write boundaries, so a receiver that falls behind reads several chunks at
    // once and `max <= SPLIT` can't hold every time. Unsplit, LEN arrives in one or two reads.
    const MEDIAN_MAX: usize = 4 * SPLIT;
    for (dir, mut sizes) in [
        ("client to server", to_server_sizes),
        ("server to client", to_client_sizes),
    ] {
        sizes.sort_unstable();
        let median = sizes[sizes.len() / 2];
        assert!(
            median <= MEDIAN_MAX,
            "{dir}: median read {median} > {MEDIAN_MAX} over {} reads",
            sizes.len()
        );
        assert!(
            sizes.len() >= LEN / 16,
            "{dir}: only {} reads for {LEN} bytes",
            sizes.len()
        );
    }
}

#[test]
fn split_writes_of_zero_are_rejected() {
    let _serial = serial();
    let upstream: SocketAddr = "127.0.0.1:9".parse().unwrap();
    let err = ChaosProxy::start(
        upstream,
        ChaosConfig {
            split_writes: Some(0),
            ..ChaosConfig::default()
        },
    )
    .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
}

/// (stress)
#[test]
fn throttles_then_unthrottles() {
    let _serial = serial();
    const RATE: u64 = 10_000;
    const THROTTLE_FOR: Duration = Duration::from_secs(1);
    const SLOW: usize = 5_000;
    const FAST: usize = 1024 * 1024;

    let (upstream, server) = serve(1, |mut stream| {
        let writer = {
            let mut stream = stream.try_clone().unwrap();
            thread::spawn(move || stream.write_all(&pattern(FAST)).unwrap())
        };
        let (slow, _) = read_sizes(&mut stream, SLOW);
        let slow_at = Instant::now();
        let (fast, _) = read_sizes(&mut stream, FAST);
        let fast_at = Instant::now();
        writer.join().unwrap();
        ((slow, slow_at), (fast, fast_at))
    });
    let proxy = ChaosProxy::start(
        upstream,
        ChaosConfig {
            read_rate_bps: Some(RATE),
            throttle_for: Some(THROTTLE_FOR),
            ..ChaosConfig::default()
        },
    )
    .unwrap();

    let start = Instant::now();
    let mut client = connect(&proxy);
    let sent = pattern(SLOW + FAST);
    let writer = {
        let mut client = client.try_clone().unwrap();
        let sent = sent.clone();
        thread::spawn(move || client.write_all(&sent).unwrap())
    };

    let (reply, _) = read_sizes(&mut client, FAST);
    let reply_took = start.elapsed();
    assert!(reply == pattern(FAST), "reply changed");
    assert!(
        reply_took < THROTTLE_FOR,
        "upstream to client was slowed: {reply_took:?}"
    );

    writer.join().unwrap();
    let ((slow, slow_at), (fast, fast_at)) = server.join().unwrap().pop().unwrap();
    assert!(
        slow == sent[..SLOW] && fast == sent[SLOW..],
        "bytes changed"
    );

    // The proxy's throttle clock starts after `start`, so a working throttle can't beat this.
    let slow_took = slow_at - start;
    assert!(
        slow_took >= Duration::from_millis(400),
        "first {SLOW} bytes took only {slow_took:?} at {RATE} B/s"
    );
    assert!(
        slow_took < Duration::from_secs(3),
        "first {SLOW} bytes took {slow_took:?} at {RATE} B/s"
    );
    let all_took = fast_at - start;
    assert!(
        all_took >= THROTTLE_FOR - Duration::from_millis(100),
        "throttle ended early: everything took {all_took:?}"
    );
    assert!(
        all_took < THROTTLE_FOR + Duration::from_secs(4),
        "not unthrottled: everything took {all_took:?}"
    );
}

#[test]
fn counts_connections() {
    let _serial = serial();
    let (upstream, server) = serve(3, echo);
    let proxy = ChaosProxy::start(upstream, ChaosConfig::default()).unwrap();
    assert_eq!(proxy.connections_seen(), 0);

    for i in 1..=3u8 {
        let mut client = connect(&proxy);
        client.write_all(&[i]).unwrap();
        let mut byte = [0u8; 1];
        client.read_exact(&mut byte).unwrap();
        assert_eq!(byte, [i]);
        assert_eq!(proxy.connections_seen(), usize::from(i));
    }
    server.join().unwrap();

    let refused = TcpListener::bind("127.0.0.1:0").unwrap();
    let refused_addr = refused.local_addr().unwrap();
    drop(refused);
    let proxy = ChaosProxy::start(refused_addr, ChaosConfig::default()).unwrap();
    let mut client = connect(&proxy);
    read_until_closed(&mut client);
    assert_eq!(proxy.connections_seen(), 1);
}

/// (stress)
#[test]
fn drop_stops_threads() {
    let _serial = serial();
    #[cfg(unix)]
    let fds_before = open_fds();

    let (upstream, server) = serve(2, echo);
    let proxy = ChaosProxy::start(
        upstream,
        ChaosConfig {
            read_rate_bps: Some(1),
            ..ChaosConfig::default()
        },
    )
    .unwrap();

    let mut clients: Vec<_> = (0..2).map(|_| connect(&proxy)).collect();
    for client in &mut clients {
        client.write_all(&[0u8; 4096]).unwrap();
    }
    wait_until(DEADLINE, "both connections to be accepted", || {
        proxy.connections_seen() == 2
    });
    #[cfg(target_os = "linux")]
    wait_until(DEADLINE, "the proxy's threads to start", || {
        proxy_threads().len() == 5
    });

    drop(proxy);

    // A joined thread can linger in /proc/self/task for a moment after `join` returns.
    #[cfg(target_os = "linux")]
    wait_until(
        Duration::from_millis(500),
        "the proxy's threads to exit",
        || proxy_threads().is_empty(),
    );
    for client in &mut clients {
        read_until_closed(client);
    }
    server.join().unwrap();

    drop(clients);
    #[cfg(unix)]
    wait_until(DEADLINE, "file descriptors to be released", || {
        open_fds() == fds_before
    });
}
