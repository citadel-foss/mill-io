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
