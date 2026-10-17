use mill_io::{EventHandler, EventLoop};
use mio::{event::Event, net::TcpListener, Interest, Token};
use std::{
    collections::HashSet,
    net::TcpStream as StdTcpStream,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Barrier,
    },
    thread,
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

struct CountEvents(Arc<AtomicUsize>);

impl EventHandler for CountEvents {
    fn handle_event(&self, _event: &Event) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn register_rejects_waker_token() {
    for event_loop in [
        Arc::new(EventLoop::new(2, 1024, 10).unwrap()),
        Arc::new(EventLoop::new_low_latency(1024, 10).unwrap()),
    ] {
        let mut listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();
        let waker_hits = Arc::new(AtomicUsize::new(0));

        let result = event_loop.register(
            &mut listener,
            Token(0),
            Interest::READABLE,
            CountEvents(Arc::clone(&waker_hits)),
        );
        assert!(result.is_err(), "register with Token(0) must fail");

        // The rejected call must not have reached the OS registration.
        let accepts = Arc::new(AtomicUsize::new(0));
        let token = event_loop.alloc_token();
        event_loop
            .register(
                &mut listener,
                token,
                Interest::READABLE,
                CountEvents(Arc::clone(&accepts)),
            )
            .expect("the source should still be registrable");

        let runner = {
            let event_loop = Arc::clone(&event_loop);
            thread::spawn(move || event_loop.run().map_err(|e| e.to_string()))
        };

        // Timers wake the loop, producing waker-token events.
        for _ in 0..5 {
            event_loop.schedule_once(Duration::from_millis(1), || {});
        }
        let _client = StdTcpStream::connect(addr).unwrap();
        assert!(
            wait_until(Duration::from_secs(1), || accepts.load(Ordering::SeqCst)
                > 0),
            "the listener got no event under its allocated token"
        );
        assert!(
            wait_until(Duration::from_secs(1), || event_loop.pending_timers() == 0),
            "timers did not fire"
        );

        event_loop.stop();
        assert_eq!(runner.join().unwrap(), Ok(()));
        assert_eq!(
            waker_hits.load(Ordering::SeqCst),
            0,
            "the rejected handler was called"
        );
    }
}

#[test]
fn alloc_token_is_unique() {
    const THREADS: usize = 8;
    const TOTAL: usize = 1_000_000;
    const PER_THREAD: usize = TOTAL / THREADS;

    let event_loop = Arc::new(EventLoop::new(1, 64, 10).unwrap());
    let start = Arc::new(Barrier::new(THREADS));

    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let event_loop = Arc::clone(&event_loop);
            let start = Arc::clone(&start);
            thread::spawn(move || {
                start.wait();
                (0..PER_THREAD)
                    .map(|_| event_loop.alloc_token())
                    .collect::<Vec<Token>>()
            })
        })
        .collect();

    let mut seen = HashSet::with_capacity(TOTAL);
    for handle in handles {
        for token in handle.join().unwrap() {
            assert_ne!(token, Token(0), "alloc_token returned the waker token");
            // mill-net numbers each server's and client's connections from 1.
            assert!(
                token.0 >= usize::MAX / 2,
                "{token:?} is in the range mill-net uses for connections"
            );
            assert!(seen.insert(token), "alloc_token returned {token:?} twice");
        }
    }
    assert_eq!(seen.len(), TOTAL);
}

fn tcp_pair() -> (mio::net::TcpStream, StdTcpStream) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let peer = StdTcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (accepted, _) = listener.accept().unwrap();
    accepted.set_nonblocking(true).unwrap();
    (mio::net::TcpStream::from_std(accepted), peer)
}

struct CountKinds {
    readable: Arc<AtomicUsize>,
    writable: Arc<AtomicUsize>,
}

impl EventHandler for CountKinds {
    fn handle_event(&self, event: &Event) {
        if event.is_readable() {
            self.readable.fetch_add(1, Ordering::SeqCst);
        }
        if event.is_writable() {
            self.writable.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[test]
fn reregister_adds_writable_interest() {
    for event_loop in [
        Arc::new(EventLoop::new(2, 1024, 10).unwrap()),
        Arc::new(EventLoop::new_low_latency(1024, 10).unwrap()),
    ] {
        let (mut stream, mut peer) = tcp_pair();
        let readable = Arc::new(AtomicUsize::new(0));
        let writable = Arc::new(AtomicUsize::new(0));
        let token = event_loop.alloc_token();
        event_loop
            .register(
                &mut stream,
                token,
                Interest::READABLE,
                CountKinds {
                    readable: Arc::clone(&readable),
                    writable: Arc::clone(&writable),
                },
            )
            .unwrap();
        let runner = {
            let event_loop = Arc::clone(&event_loop);
            thread::spawn(move || event_loop.run().map_err(|e| e.to_string()))
        };

        std::io::Write::write_all(&mut peer, b"a").unwrap();
        assert!(wait_until(Duration::from_secs(1), || readable
            .load(Ordering::SeqCst)
            > 0));
        assert_eq!(writable.load(Ordering::SeqCst), 0, "READABLE only so far");

        // The socket is writable, so asking for WRITABLE yields an event.
        event_loop
            .reregister(&mut stream, token, Interest::READABLE | Interest::WRITABLE)
            .unwrap();
        assert!(
            wait_until(Duration::from_secs(1), || writable.load(Ordering::SeqCst)
                > 0),
            "no writable event after reregister"
        );

        let before = readable.load(Ordering::SeqCst);
        std::io::Write::write_all(&mut peer, b"b").unwrap();
        assert!(
            wait_until(Duration::from_secs(1), || readable.load(Ordering::SeqCst)
                > before),
            "no readable event after reregister"
        );

        event_loop.stop();
        assert_eq!(runner.join().unwrap(), Ok(()));
    }
}

#[test]
fn reregister_rejects_unknown_and_waker_tokens() {
    let event_loop = EventLoop::new(1, 64, 10).unwrap();
    let (mut stream, _peer) = tcp_pair();

    let unknown = event_loop.alloc_token();
    assert!(event_loop
        .reregister(&mut stream, unknown, Interest::READABLE)
        .is_err());

    let token = event_loop.alloc_token();
    event_loop
        .register(
            &mut stream,
            token,
            Interest::READABLE,
            CountEvents(Arc::new(AtomicUsize::new(0))),
        )
        .unwrap();
    assert!(event_loop
        .reregister(&mut stream, Token(0), Interest::WRITABLE)
        .is_err());

    event_loop.deregister(&mut stream, token).unwrap();
    assert!(event_loop
        .reregister(&mut stream, token, Interest::READABLE)
        .is_err());
}

#[test]
fn token_is_reusable_after_deregister() {
    let event_loop = Arc::new(EventLoop::new(2, 1024, 10).unwrap());
    let token = event_loop.alloc_token();

    let (mut first, mut first_peer) = tcp_pair();
    let first_hits = Arc::new(AtomicUsize::new(0));
    event_loop
        .register(
            &mut first,
            token,
            Interest::READABLE,
            CountEvents(Arc::clone(&first_hits)),
        )
        .unwrap();
    event_loop.deregister(&mut first, token).unwrap();

    let (mut second, mut second_peer) = tcp_pair();
    let second_hits = Arc::new(AtomicUsize::new(0));
    event_loop
        .register(
            &mut second,
            token,
            Interest::READABLE,
            CountEvents(Arc::clone(&second_hits)),
        )
        .unwrap();

    let runner = {
        let event_loop = Arc::clone(&event_loop);
        thread::spawn(move || event_loop.run().map_err(|e| e.to_string()))
    };
    std::io::Write::write_all(&mut first_peer, b"x").unwrap();
    std::io::Write::write_all(&mut second_peer, b"y").unwrap();
    assert!(
        wait_until(Duration::from_secs(1), || second_hits
            .load(Ordering::SeqCst)
            > 0),
        "the new source got no event under the reused token"
    );
    event_loop.stop();
    assert_eq!(runner.join().unwrap(), Ok(()));
    assert_eq!(
        first_hits.load(Ordering::SeqCst),
        0,
        "the deregistered handler was called"
    );
}

#[test]
fn register_with_pending_data_gets_event() {
    const ROUNDS: usize = 300;

    for (mode, event_loop) in [
        ("pool", Arc::new(EventLoop::new(4, 1024, 10).unwrap())),
        (
            "direct",
            Arc::new(EventLoop::new_low_latency(1024, 10).unwrap()),
        ),
    ] {
        let runner = {
            let event_loop = Arc::clone(&event_loop);
            thread::spawn(move || event_loop.run().map_err(|e| e.to_string()))
        };
        for round in 0..ROUNDS {
            let (mut stream, mut peer) = tcp_pair();
            std::io::Write::write_all(&mut peer, b"ready").unwrap();
            let hits = Arc::new(AtomicUsize::new(0));
            let token = event_loop.alloc_token();
            event_loop
                .register(
                    &mut stream,
                    token,
                    Interest::READABLE,
                    CountEvents(Arc::clone(&hits)),
                )
                .unwrap();
            // Edge-triggered: the event raised by registering a ready socket
            // is the only one this source will ever get.
            assert!(
                wait_until(Duration::from_secs(1), || hits.load(Ordering::SeqCst) > 0),
                "{mode}: round {round}: the readiness present at register() was lost"
            );
            event_loop.deregister(&mut stream, token).unwrap();
        }
        event_loop.stop();
        assert_eq!(runner.join().unwrap(), Ok(()));
    }
}

#[test]
fn reregister_to_writable_never_loses_the_edge() {
    const ROUNDS: usize = 200;

    for (mode, event_loop) in [
        ("pool", Arc::new(EventLoop::new(4, 1024, 10).unwrap())),
        (
            "direct",
            Arc::new(EventLoop::new_low_latency(1024, 10).unwrap()),
        ),
    ] {
        let runner = {
            let event_loop = Arc::clone(&event_loop);
            thread::spawn(move || event_loop.run().map_err(|e| e.to_string()))
        };
        for round in 0..ROUNDS {
            let (mut stream, _peer) = tcp_pair();
            let readable = Arc::new(AtomicUsize::new(0));
            let writable = Arc::new(AtomicUsize::new(0));
            let token = event_loop.alloc_token();
            event_loop
                .register(
                    &mut stream,
                    token,
                    Interest::READABLE,
                    CountKinds {
                        readable: Arc::clone(&readable),
                        writable: Arc::clone(&writable),
                    },
                )
                .unwrap();
            // The socket is already writable, so this raises the only writable
            // edge it will ever get.
            event_loop
                .reregister(&mut stream, token, Interest::READABLE | Interest::WRITABLE)
                .unwrap();
            assert!(
                wait_until(Duration::from_secs(1), || writable.load(Ordering::SeqCst)
                    > 0),
                "{mode}: round {round}: the writable edge raised by reregister was lost"
            );
            event_loop.deregister(&mut stream, token).unwrap();
        }
        event_loop.stop();
        assert_eq!(runner.join().unwrap(), Ok(()));
    }
}
