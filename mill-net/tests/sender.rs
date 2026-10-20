use mill_net::errors::{NetworkError, Result};
use mill_net::tcp::config::TcpServerConfig;
use mill_net::tcp::{
    ConnSender, ConnectionId, NetworkHandler, SendError, ServerContext, TcpClient, TcpClientConfig,
    TcpServer,
};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::io::Read;
use std::net::TcpListener;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

mod common;

use common::{pool_and_direct, spawn_loop, stop_loop, wait_until};

const HEADER: usize = 12;

fn payload_len(producer: u32, seq: u32) -> usize {
    ((seq as usize * 97 + producer as usize * 13) % 3000) + 1
}

fn pattern(producer: u32, seq: u32, i: usize) -> u8 {
    (producer as usize * 31 + seq as usize * 7 + i) as u8
}

fn frame(producer: u32, seq: u32) -> Vec<u8> {
    let len = payload_len(producer, seq);
    let mut frame = Vec::with_capacity(HEADER + len);
    frame.extend_from_slice(&(len as u32).to_le_bytes());
    frame.extend_from_slice(&producer.to_le_bytes());
    frame.extend_from_slice(&seq.to_le_bytes());
    frame.extend((0..len).map(|i| pattern(producer, seq, i)));
    frame
}

/// Parses the frames above and records every torn, interleaved or reordered one.
#[derive(Clone, Default)]
struct FrameChecker {
    state: Arc<Mutex<CheckState>>,
    frames: Arc<AtomicUsize>,
}

#[derive(Default)]
struct CheckState {
    buffer: Vec<u8>,
    next_seq: HashMap<u32, u32>,
    errors: Vec<String>,
}

impl NetworkHandler for FrameChecker {
    fn on_data(&self, _ctx: &ServerContext, _conn: ConnectionId, data: &[u8]) -> Result<()> {
        let mut state = self.state.lock();
        state.buffer.extend_from_slice(data);
        let mut at = 0;
        while state.buffer.len() - at >= HEADER {
            let field = |i: usize| {
                let b = &state.buffer[at + i * 4..at + i * 4 + 4];
                u32::from_le_bytes([b[0], b[1], b[2], b[3]])
            };
            let (len, producer, seq) = (field(0) as usize, field(1), field(2));
            if state.buffer.len() - at < HEADER + len {
                break;
            }
            let body = &state.buffer[at + HEADER..at + HEADER + len];
            let intact = len == payload_len(producer, seq)
                && body
                    .iter()
                    .enumerate()
                    .all(|(i, b)| *b == pattern(producer, seq, i));
            let mut error = None;
            if !intact {
                error = Some(format!(
                    "torn frame: producer {producer} seq {seq} len {len}"
                ));
            }
            let expected = state.next_seq.get(&producer).copied().unwrap_or(0);
            if seq != expected {
                error = Some(format!(
                    "producer {producer}: got seq {seq}, expected {expected}"
                ));
            }
            if let Some(error) = error {
                state.errors.push(error);
            }
            state.next_seq.insert(producer, seq + 1);
            at += HEADER + len;
            self.frames.fetch_add(1, Ordering::SeqCst);
        }
        state.buffer.drain(..at);
        Ok(())
    }
}

/// A sending side's handler: counts `on_drained` so refused senders can wait for it.
#[derive(Clone, Default)]
struct Drains {
    count: Arc<(Mutex<u64>, parking_lot::Condvar)>,
}

impl NetworkHandler for Drains {
    fn on_data(&self, _ctx: &ServerContext, _conn: ConnectionId, _data: &[u8]) -> Result<()> {
        Ok(())
    }

    fn on_drained(&self, _ctx: &ServerContext, _conn: ConnectionId) -> Result<()> {
        let (count, changed) = &*self.count;
        *count.lock() += 1;
        changed.notify_all();
        Ok(())
    }
}

/// Retries a refused frame once the queue drains. Spinning instead starves the threads
/// that drain it of CPU on a busy machine.
fn send_retrying(sender: &ConnSender, frame: &[u8], drains: &Drains) {
    let (count, changed) = &*drains.count;
    loop {
        let seen = *count.lock();
        match sender.send(frame) {
            Ok(()) => return,
            Err(SendError::Backpressure) => {
                let mut count = count.lock();
                if *count == seen {
                    // Bounded, in case the drain landed between the send and this lock.
                    changed.wait_for(&mut count, Duration::from_millis(10));
                }
            }
            Err(e) => panic!("send failed: {e}"),
        }
    }
}

/// The kernel's view of the test's connection (`ss -tnim`), for a failure message.
#[cfg(target_os = "linux")]
fn socket_report(port: u16) -> String {
    let Ok(out) = std::process::Command::new("ss")
        .args(["-tnimH", "state", "established"])
        .output()
    else {
        return "ss unavailable".to_string();
    };
    let text = String::from_utf8_lossy(&out.stdout);
    let mut report = String::new();
    let mut keep = false;
    for line in text.lines() {
        if !line.starts_with(char::is_whitespace) {
            let port = format!(":{port}");
            keep = line
                .split_whitespace()
                .any(|column| column.ends_with(&port));
        }
        if keep {
            report.push_str(line.trim());
            report.push('\n');
        }
    }
    report
}

#[cfg(not(target_os = "linux"))]
fn socket_report(_port: u16) -> String {
    String::new()
}

#[test]
fn conn_sender_from_threads_and_timers() {
    const THREADS: u32 = 8;
    const PER_THREAD: u32 = 2000;
    const TIMER_FRAMES: u32 = 200;
    const COMPUTE_FRAMES: u32 = 500;
    const TOTAL: usize = (THREADS * PER_THREAD + TIMER_FRAMES + COMPUTE_FRAMES) as usize;

    for (mode, event_loop) in pool_and_direct() {
        let checker = FrameChecker::default();
        let state = Arc::clone(&checker.state);
        let frames = Arc::clone(&checker.frames);
        let config = TcpServerConfig::builder()
            .address("127.0.0.1:0".parse().unwrap())
            .build();
        let server = Arc::new(TcpServer::new(config, checker).unwrap());
        Arc::clone(&server)
            .start(&event_loop, event_loop.alloc_token())
            .unwrap();

        let drains = Drains::default();
        let mut client = TcpClient::connect(
            server.local_addr().unwrap(),
            TcpClientConfig::default(),
            drains.clone(),
        )
        .unwrap();
        client.start(&event_loop).unwrap();
        let runner = spawn_loop(&event_loop);

        let timer_sender = client.sender();
        let timer_seq = Arc::new(AtomicU32::new(0));
        let timer = {
            let timer_seq = Arc::clone(&timer_seq);
            event_loop.schedule_repeating(Duration::from_millis(1), move || {
                let seq = timer_seq.load(Ordering::SeqCst);
                if seq < TIMER_FRAMES && timer_sender.send(&frame(THREADS, seq)).is_ok() {
                    timer_seq.store(seq + 1, Ordering::SeqCst);
                }
            })
        };

        let compute_sender = client.sender();
        let compute_drains = drains.clone();
        event_loop.spawn_compute(move || {
            for seq in 0..COMPUTE_FRAMES {
                send_retrying(&compute_sender, &frame(THREADS + 1, seq), &compute_drains);
            }
        });

        let producers: Vec<_> = (0..THREADS)
            .map(|producer| {
                let sender = client.sender();
                let drains = drains.clone();
                thread::spawn(move || {
                    for seq in 0..PER_THREAD {
                        send_retrying(&sender, &frame(producer, seq), &drains);
                    }
                })
            })
            .collect();
        for producer in producers {
            producer.join().unwrap();
        }

        let all_arrived = wait_until(Duration::from_secs(30), || {
            frames.load(Ordering::SeqCst) >= TOTAL
        });
        let stalled_at = (!all_arrived).then(|| {
            // Tells a slow machine from a stall in the failure message.
            let at_deadline = frames.load(Ordering::SeqCst);
            let sockets = socket_report(server.local_addr().unwrap().port());
            wait_until(Duration::from_secs(30), || {
                frames.load(Ordering::SeqCst) >= TOTAL
            });
            (at_deadline, frames.load(Ordering::SeqCst), sockets)
        });
        event_loop.cancel_timer(timer);
        let state = state.lock();
        assert!(
            state.errors.is_empty(),
            "{mode}: {} bad frames, first: {}",
            state.errors.len(),
            state.errors[0]
        );
        if let Some((at_deadline, later, sockets)) = stalled_at {
            panic!(
                "{mode}: {at_deadline} of {TOTAL} frames arrived within 30 s, {later} after 30 s more ({}); sockets at the deadline:\n{sockets}",
                if later == at_deadline { "stalled" } else { "still moving" }
            );
        }
        assert_eq!(timer_seq.load(Ordering::SeqCst), TIMER_FRAMES);
        drop(state);

        stop_loop(&event_loop, runner);
        drop((client, server));
    }
}

#[derive(Clone, Default)]
struct Lifecycle {
    connects: Arc<AtomicUsize>,
    disconnects: Arc<AtomicUsize>,
    close_on_data: bool,
    misbehaved: Arc<AtomicUsize>,
    errors: Arc<Mutex<Vec<Option<ConnectionId>>>>,
    received: Arc<AtomicUsize>,
}

impl NetworkHandler for Lifecycle {
    fn on_connect(&self, _ctx: &ServerContext, _conn: ConnectionId) -> Result<()> {
        self.connects.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn on_data(&self, ctx: &ServerContext, conn: ConnectionId, data: &[u8]) -> Result<()> {
        self.received.fetch_add(data.len(), Ordering::SeqCst);
        if self.close_on_data {
            let sender = ctx.sender(conn).unwrap();
            sender.close();
            sender.close();
            ctx.close_connection(conn)?;
            if !matches!(sender.send(b"late"), Err(SendError::Closed)) || ctx.sender(conn).is_some()
            {
                self.misbehaved.fetch_add(1, Ordering::SeqCst);
            }
        }
        Ok(())
    }

    fn on_disconnect(&self, _ctx: &ServerContext, _conn: ConnectionId) -> Result<()> {
        self.disconnects.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn on_error(&self, _ctx: &ServerContext, conn: Option<ConnectionId>, _error: NetworkError) {
        self.errors.lock().push(conn);
    }
}

#[test]
fn close_from_handler_and_threads_fires_on_disconnect_once() {
    for (mode, event_loop) in pool_and_direct() {
        let server_handler = Lifecycle {
            close_on_data: true,
            ..Lifecycle::default()
        };
        let server_disconnects = Arc::clone(&server_handler.disconnects);
        let misbehaved = Arc::clone(&server_handler.misbehaved);
        let config = TcpServerConfig::builder()
            .address("127.0.0.1:0".parse().unwrap())
            .build();
        let server = Arc::new(TcpServer::new(config, server_handler).unwrap());
        Arc::clone(&server)
            .start(&event_loop, event_loop.alloc_token())
            .unwrap();

        let client_handler = Lifecycle::default();
        let client_disconnects = Arc::clone(&client_handler.disconnects);
        let mut client = TcpClient::connect(
            server.local_addr().unwrap(),
            TcpClientConfig::default(),
            client_handler,
        )
        .unwrap();
        client.start(&event_loop).unwrap();
        let runner = spawn_loop(&event_loop);

        client.send(b"close me").unwrap();
        assert!(
            wait_until(Duration::from_secs(5), || {
                server_disconnects.load(Ordering::SeqCst) == 1
                    && client_disconnects.load(Ordering::SeqCst) == 1
            }),
            "{mode}: server {} / client {} disconnects",
            server_disconnects.load(Ordering::SeqCst),
            client_disconnects.load(Ordering::SeqCst)
        );
        assert_eq!(server.connection_count(), 0, "{mode}");
        assert_eq!(
            misbehaved.load(Ordering::SeqCst),
            0,
            "{mode}: a closed connection still took sends or had a sender"
        );

        let closers: Vec<_> = (0..4)
            .map(|_| {
                let sender = client.sender();
                thread::spawn(move || sender.close())
            })
            .collect();
        for closer in closers {
            closer.join().unwrap();
        }
        client.disconnect().unwrap();
        assert!(matches!(client.send(b"x"), Err(SendError::Closed)));
        assert!(matches!(client.send(b""), Err(SendError::Closed)));

        stop_loop(&event_loop, runner);
        assert_eq!(server_disconnects.load(Ordering::SeqCst), 1, "{mode}");
        assert_eq!(client_disconnects.load(Ordering::SeqCst), 1, "{mode}");
        drop((client, server));
    }
}

#[test]
fn zero_byte_send_writes_nothing() {
    for (mode, event_loop) in pool_and_direct() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let handler = Lifecycle::default();
        let connects = Arc::clone(&handler.connects);
        let disconnects = Arc::clone(&handler.disconnects);
        let mut client = TcpClient::connect(
            listener.local_addr().unwrap(),
            TcpClientConfig::default(),
            handler,
        )
        .unwrap();
        client.start(&event_loop).unwrap();
        let runner = spawn_loop(&event_loop);
        let (mut peer, _) = listener.accept().unwrap();
        assert!(wait_until(Duration::from_secs(5), || connects
            .load(Ordering::SeqCst)
            == 1));

        let sender = client.sender();
        sender.send(b"").unwrap();
        sender.send(b"a").unwrap();
        sender.send(b"").unwrap();
        let mut first = [0; 1];
        peer.read_exact(&mut first).unwrap();
        sender.close();
        assert!(matches!(sender.send(b""), Err(SendError::Closed)), "{mode}");

        let mut rest = Vec::new();
        peer.read_to_end(&mut rest).unwrap();
        assert_eq!(&first, b"a", "{mode}");
        assert!(rest.is_empty(), "{mode}: {} extra bytes", rest.len());
        assert_eq!(disconnects.load(Ordering::SeqCst), 1, "{mode}");

        stop_loop(&event_loop, runner);
    }
}

#[test]
fn peer_close_with_queued_data_disconnects_once() {
    for (mode, event_loop) in pool_and_direct() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let handler = Lifecycle::default();
        let connects = Arc::clone(&handler.connects);
        let disconnects = Arc::clone(&handler.disconnects);
        let config = TcpClientConfig {
            high_watermark: 64 * 1024 * 1024,
            ..TcpClientConfig::default()
        };
        let mut client =
            TcpClient::connect(listener.local_addr().unwrap(), config, handler).unwrap();
        client.start(&event_loop).unwrap();
        let runner = spawn_loop(&event_loop);
        let (peer, _) = listener.accept().unwrap();
        assert!(wait_until(Duration::from_secs(5), || connects
            .load(Ordering::SeqCst)
            == 1));

        client.send(&vec![3; 32 * 1024 * 1024]).unwrap();
        drop(peer);

        assert!(
            wait_until(Duration::from_secs(5), || disconnects
                .load(Ordering::SeqCst)
                == 1),
            "{mode}: the peer's close went unnoticed"
        );
        assert!(
            wait_until(Duration::from_secs(5), || matches!(
                client.send(b"x"),
                Err(SendError::Closed)
            )),
            "{mode}: sends still accepted after the peer closed"
        );
        assert!(
            !wait_until(Duration::from_millis(100), || disconnects
                .load(Ordering::SeqCst)
                > 1),
            "{mode}: on_disconnect fired twice"
        );

        stop_loop(&event_loop, runner);
    }
}

#[test]
fn reset_mid_frame_is_an_error() {
    use mill_net::testing::{ChaosConfig, ChaosProxy};
    use std::io::Write;
    use std::net::TcpStream;

    for (mode, event_loop) in pool_and_direct() {
        let handler = Lifecycle::default();
        let disconnects = Arc::clone(&handler.disconnects);
        let errors = Arc::clone(&handler.errors);
        let received = Arc::clone(&handler.received);
        let connected = Arc::clone(&handler.connects);
        let config = TcpServerConfig::builder()
            .address("127.0.0.1:0".parse().unwrap())
            .build();
        let server = Arc::new(TcpServer::new(config, handler).unwrap());
        Arc::clone(&server)
            .start(&event_loop, event_loop.alloc_token())
            .unwrap();
        let runner = spawn_loop(&event_loop);
        let proxy = ChaosProxy::start(
            server.local_addr().unwrap(),
            ChaosConfig {
                reset_after: Some(1000),
                ..ChaosConfig::default()
            },
        )
        .unwrap();

        let mut peer = TcpStream::connect(proxy.addr()).unwrap();
        let _ = peer.write_all(&[5; 100_000]);

        assert!(
            wait_until(Duration::from_secs(5), || disconnects
                .load(Ordering::SeqCst)
                == 1),
            "{mode}: the reset went unnoticed"
        );
        assert_eq!(connected.load(Ordering::SeqCst), 1, "{mode}");
        let errors = errors.lock().clone();
        assert!(
            !errors.is_empty() && errors.iter().all(Option::is_some),
            "{mode}: the reset was not reported as a connection error: {errors:?}"
        );
        assert!(received.load(Ordering::SeqCst) <= 1000, "{mode}");
        assert_eq!(server.connection_count(), 0, "{mode}");

        stop_loop(&event_loop, runner);
        drop((proxy, server));
    }
}

#[derive(Clone, Default)]
struct Pauser {
    sender: Arc<Mutex<Option<ConnSender>>>,
    received: Arc<Mutex<Vec<u8>>>,
}

impl NetworkHandler for Pauser {
    fn on_connect(&self, ctx: &ServerContext, conn: ConnectionId) -> Result<()> {
        let sender = ctx.sender(conn).unwrap();
        sender.pause_reads();
        *self.sender.lock() = Some(sender);
        Ok(())
    }

    fn on_data(&self, _ctx: &ServerContext, _conn: ConnectionId, data: &[u8]) -> Result<()> {
        self.received.lock().extend_from_slice(data);
        Ok(())
    }
}

#[test]
fn paused_reads_push_back_on_the_peer() {
    const TOTAL: usize = 64 * 1024 * 1024;
    const CHUNK: usize = 64 * 1024;

    for (mode, event_loop) in pool_and_direct() {
        let pauser = Pauser::default();
        let (paused, received) = (Arc::clone(&pauser.sender), Arc::clone(&pauser.received));
        let config = TcpServerConfig::builder()
            .address("127.0.0.1:0".parse().unwrap())
            .build();
        let server = Arc::new(TcpServer::new(config, pauser).unwrap());
        Arc::clone(&server)
            .start(&event_loop, event_loop.alloc_token())
            .unwrap();
        let mut client = TcpClient::connect(
            server.local_addr().unwrap(),
            TcpClientConfig::default(),
            Lifecycle::default(),
        )
        .unwrap();
        client.start(&event_loop).unwrap();
        let runner = spawn_loop(&event_loop);
        assert!(wait_until(Duration::from_secs(5), || paused
            .lock()
            .is_some()));

        let data: Vec<u8> = (0..TOTAL).map(|i| (i % 253) as u8).collect();
        let chunks: Vec<&[u8]> = data.chunks(CHUNK).collect();
        let mut next = 0;
        while next < chunks.len() {
            match client.send(chunks[next]) {
                Ok(()) => next += 1,
                Err(SendError::Backpressure) => break,
                Err(e) => panic!("{mode}: {e}"),
            }
        }
        assert!(
            next < chunks.len(),
            "{mode}: the sender never felt the paused reader"
        );
        assert!(
            received.lock().is_empty(),
            "{mode}: paused reads still delivered data"
        );

        paused.lock().as_ref().unwrap().resume_reads();
        for chunk in &chunks[next..] {
            assert!(
                wait_until(Duration::from_secs(10), || client.send(chunk).is_ok()),
                "{mode}: the queue never drained after resume_reads"
            );
        }
        assert!(
            wait_until(Duration::from_secs(30), || received.lock().len() == TOTAL),
            "{mode}: {} of {TOTAL} bytes after resume_reads",
            received.lock().len()
        );
        assert!(*received.lock() == data, "{mode}: bytes reordered or lost");

        stop_loop(&event_loop, runner);
        drop((client, server));
    }
}
