#![cfg(unix)]

use mill_io::{EventHandler, EventLoop};
use mio::{event::Event, net::UnixStream, Interest, Token};
use std::{
    io::{ErrorKind, Read, Write},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if cond() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(1));
    }
}

fn spawn_loop(event_loop: &Arc<EventLoop>) -> JoinHandle<Result<(), String>> {
    let event_loop = Arc::clone(event_loop);
    thread::spawn(move || event_loop.run().map_err(|e| e.to_string()))
}

struct PanicOnce {
    fired: AtomicBool,
}

impl EventHandler for PanicOnce {
    fn handle_event(&self, _event: &Event) {
        if !self.fired.swap(true, Ordering::SeqCst) {
            panic!("intentional handler panic");
        }
    }
}

struct CountBytes {
    stream: Arc<Mutex<UnixStream>>,
    bytes: Arc<AtomicUsize>,
}

impl EventHandler for CountBytes {
    fn handle_event(&self, _event: &Event) {
        let mut stream = self.stream.lock().unwrap();
        let mut buf = [0u8; 64];
        loop {
            match stream.read(&mut buf) {
                Ok(0) => return,
                Ok(n) => {
                    self.bytes.fetch_add(n, Ordering::SeqCst);
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => return,
                Err(e) => panic!("read failed: {e}"),
            }
        }
    }
}

struct Counted {
    peer: UnixStream,
    bytes: Arc<AtomicUsize>,
    _stream: Arc<Mutex<UnixStream>>,
}

fn register_counted(event_loop: &EventLoop, token: Token) -> Counted {
    let (stream, peer) = UnixStream::pair().unwrap();
    let stream = Arc::new(Mutex::new(stream));
    let bytes = Arc::new(AtomicUsize::new(0));
    let handler = CountBytes {
        stream: Arc::clone(&stream),
        bytes: Arc::clone(&bytes),
    };
    event_loop
        .register(
            &mut *stream.lock().unwrap(),
            token,
            Interest::READABLE,
            handler,
        )
        .unwrap();
    Counted {
        peer,
        bytes,
        _stream: stream,
    }
}

fn check_panic_keeps_loop_running(event_loop: Arc<EventLoop>) {
    let (mut panicking, mut panicking_peer) = UnixStream::pair().unwrap();
    event_loop
        .register(
            &mut panicking,
            Token(1),
            Interest::READABLE,
            PanicOnce {
                fired: AtomicBool::new(false),
            },
        )
        .unwrap();
    // Token(3) shares a worker with Token(1) under per-token routing.
    let mut others = [
        register_counted(&event_loop, Token(2)),
        register_counted(&event_loop, Token(3)),
    ];

    let runner = spawn_loop(&event_loop);

    panicking_peer.write_all(b"x").unwrap();
    assert!(
        wait_until(Duration::from_secs(1), || event_loop.handler_panics() == 1),
        "the handler panic was not counted (handler_panics = {})",
        event_loop.handler_panics()
    );

    for round in 1..=20 {
        for (i, other) in others.iter_mut().enumerate() {
            other.peer.write_all(b"y").unwrap();
            assert!(
                wait_until(Duration::from_secs(1), || {
                    other.bytes.load(Ordering::SeqCst) == round
                }),
                "source {i} got no event within 1 s in round {round} \
                 (run() finished: {})",
                runner.is_finished()
            );
        }
    }
    assert!(
        !runner.is_finished(),
        "run() returned while it should be running"
    );
    assert_eq!(event_loop.handler_panics(), 1);

    event_loop.stop();
    let result = runner.join().expect("the reactor thread panicked");
    assert_eq!(result, Ok(()));
}

#[test]
fn handler_panic_keeps_loop_running_pool() {
    let event_loop = Arc::new(EventLoop::new(2, 1024, 10).unwrap());
    check_panic_keeps_loop_running(event_loop);
}

#[test]
fn handler_panic_keeps_loop_running_direct() {
    let event_loop = Arc::new(EventLoop::new_low_latency(1024, 10).unwrap());
    check_panic_keeps_loop_running(event_loop);
}

#[test]
fn stop_never_panics() {
    for event_loop in [
        Arc::new(EventLoop::new(2, 1024, 10).unwrap()),
        Arc::new(EventLoop::new_low_latency(1024, 10).unwrap()),
    ] {
        event_loop.stop();
        event_loop.stop();
        assert_eq!(
            run_within(&event_loop, Duration::from_secs(1)),
            Some(Ok(()))
        );

        let started = Arc::new(AtomicBool::new(false));
        {
            let started = Arc::clone(&started);
            event_loop
                .post(move || started.store(true, Ordering::SeqCst))
                .unwrap();
        }
        let runner = spawn_loop(&event_loop);
        assert!(
            wait_until(Duration::from_secs(1), || started.load(Ordering::SeqCst)),
            "the loop did not start"
        );
        event_loop.stop();
        event_loop.stop();
        {
            let el = Arc::clone(&event_loop);
            // The loop may already have stopped; a queued closure is fine.
            let _ = event_loop.post(move || el.stop());
        }
        assert_eq!(runner.join().expect("run() panicked"), Ok(()));

        event_loop.stop();
        event_loop.stop();
        assert_eq!(
            run_within(&event_loop, Duration::from_secs(1)),
            Some(Ok(()))
        );
    }
}

/// Runs `run()` on a helper thread and waits up to `timeout` for it to return.
/// On timeout it stops the loop again so the helper can exit, and returns None.
fn run_within(event_loop: &Arc<EventLoop>, timeout: Duration) -> Option<Result<(), String>> {
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let el = Arc::clone(event_loop);
    let runner = thread::spawn(move || {
        let _ = done_tx.send(el.run().map_err(|e| e.to_string()));
    });
    let result = done_rx.recv_timeout(timeout).ok();
    if result.is_none() {
        event_loop.stop();
    }
    runner.join().expect("run() panicked");
    result
}

#[test]
fn stop_before_run_returns_promptly() {
    for event_loop in [
        Arc::new(EventLoop::new(2, 1024, 10).unwrap()),
        Arc::new(EventLoop::new_low_latency(1024, 10).unwrap()),
    ] {
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let el = Arc::clone(&event_loop);
        let runner = thread::spawn(move || {
            el.stop();
            let _ = done_tx.send(el.run().map_err(|e| e.to_string()));
        });
        let result = done_rx.recv_timeout(Duration::from_secs(1));
        if result.is_err() {
            event_loop.stop();
        }
        runner.join().expect("run() panicked");
        assert_eq!(
            result.ok(),
            Some(Ok(())),
            "run() after stop() on the same thread did not return within 1 s"
        );
    }
}

#[test]
fn stop_right_after_spawn_never_hangs() {
    const ROUNDS: usize = 500;

    for (mode, event_loop) in [
        ("pool", Arc::new(EventLoop::new(2, 1024, 10).unwrap())),
        (
            "direct",
            Arc::new(EventLoop::new_low_latency(1024, 10).unwrap()),
        ),
    ] {
        let mut hung = 0;
        for _ in 0..ROUNDS {
            let (done_tx, done_rx) = std::sync::mpsc::channel();
            let el = Arc::clone(&event_loop);
            let runner = thread::spawn(move || {
                let _ = done_tx.send(el.run().map_err(|e| e.to_string()));
            });
            event_loop.stop();
            if done_rx.recv_timeout(Duration::from_secs(1)).is_err() {
                hung += 1;
                event_loop.stop();
            }
            runner.join().expect("run() panicked");
        }
        assert_eq!(
            hung, 0,
            "{mode}: {hung} of {ROUNDS} runs ignored a stop() made right after spawning"
        );
    }
}

#[test]
fn run_after_consumed_stop_runs_again() {
    for event_loop in [
        Arc::new(EventLoop::new(2, 1024, 10).unwrap()),
        Arc::new(EventLoop::new_low_latency(1024, 10).unwrap()),
    ] {
        event_loop.stop();
        assert_eq!(
            run_within(&event_loop, Duration::from_secs(1)),
            Some(Ok(()))
        );

        let ticks = Arc::new(AtomicUsize::new(0));
        let runner = spawn_loop(&event_loop);
        {
            let ticks = Arc::clone(&ticks);
            event_loop.schedule_repeating(Duration::from_millis(1), move || {
                ticks.fetch_add(1, Ordering::SeqCst);
            });
        }
        assert!(
            wait_until(Duration::from_secs(1), || ticks.load(Ordering::SeqCst) >= 5),
            "the second run() did not keep running"
        );
        assert!(
            !runner.is_finished(),
            "the second run() returned without a new stop()"
        );
        event_loop.stop();
        assert_eq!(runner.join().expect("run() panicked"), Ok(()));
    }
}

#[test]
fn timer_panic_keeps_loop_running() {
    for event_loop in [
        Arc::new(EventLoop::new(2, 1024, 10).unwrap()),
        Arc::new(EventLoop::new_low_latency(1024, 10).unwrap()),
    ] {
        let runner = spawn_loop(&event_loop);
        event_loop.schedule_once(Duration::from_millis(5), || {
            panic!("intentional timer panic")
        });
        let later = Arc::new(AtomicUsize::new(0));
        {
            let later = Arc::clone(&later);
            event_loop.schedule_once(Duration::from_millis(30), move || {
                later.fetch_add(1, Ordering::SeqCst);
            });
        }
        assert!(
            wait_until(Duration::from_secs(1), || later.load(Ordering::SeqCst) == 1),
            "a timer due after the panicking one did not fire"
        );
        assert_eq!(event_loop.timer_panics(), 1);

        let after = Arc::new(AtomicUsize::new(0));
        {
            let after = Arc::clone(&after);
            event_loop.schedule_once(Duration::from_millis(1), move || {
                after.fetch_add(1, Ordering::SeqCst);
            });
        }
        assert!(
            wait_until(Duration::from_secs(1), || after.load(Ordering::SeqCst) == 1),
            "a timer scheduled after the panic did not fire"
        );
        assert!(!runner.is_finished(), "run() returned after a timer panic");
        assert_eq!(event_loop.timer_panics(), 1);
        assert_eq!(event_loop.handler_panics(), 0);

        event_loop.stop();
        assert_eq!(runner.join().expect("run() panicked"), Ok(()));
    }
}
