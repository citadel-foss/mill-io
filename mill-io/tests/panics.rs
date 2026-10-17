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
