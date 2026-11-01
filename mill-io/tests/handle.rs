use mill_io::{EventHandler, EventLoop, Handle, LoopGone, TaskPriority};
use mio::{event::Event, net::UdpSocket, Interest};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc, Arc,
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

struct Probe(Arc<AtomicUsize>);

impl Drop for Probe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct ProbedHandler {
    handle: Handle,
    _probe: Probe,
}

impl EventHandler for ProbedHandler {
    fn handle_event(&self, _: &Event) {
        let _ = self.handle.post(|| {});
    }
}

#[test]
fn handle_is_send_sync_and_clone() {
    fn assert_traits<T: Send + Sync + Clone + 'static>() {}
    assert_traits::<Handle>();
}

#[test]
fn handle_forwards_to_its_loop() {
    let event_loop = Arc::new(EventLoop::new(2, 64, 10).unwrap());
    let handle = event_loop.handle();
    assert_eq!(handle, event_loop.handle());
    assert_ne!(handle, EventLoop::new(1, 8, 10).unwrap().handle());
    let runner = spawn_loop(&event_loop);

    let (tx, rx) = mpsc::channel();
    let on_reactor = handle.clone();
    let posted = tx.clone();
    handle
        .post(move || {
            posted
                .send(("post", on_reactor.is_reactor_thread()))
                .unwrap()
        })
        .unwrap();
    let fired = tx.clone();
    handle
        .schedule_once(Duration::from_millis(5), move || {
            fired.send(("once", true)).unwrap()
        })
        .unwrap();
    let computed = tx.clone();
    handle
        .spawn_compute(move || computed.send(("compute", true)).unwrap())
        .unwrap();
    let prioritised = tx.clone();
    handle
        .spawn_compute_with_priority(
            move || prioritised.send(("priority", true)).unwrap(),
            TaskPriority::High,
        )
        .unwrap();
    let mut seen: Vec<_> = (0..4)
        .map(|_| rx.recv_timeout(Duration::from_secs(5)).unwrap())
        .collect();
    seen.sort();
    assert_eq!(
        seen,
        [
            ("compute", true),
            ("once", true),
            ("post", true),
            ("priority", true)
        ]
    );
    assert!(!handle.is_reactor_thread());

    let far = handle
        .schedule_repeating(Duration::from_secs(3600), || {})
        .unwrap();
    assert_eq!(handle.pending_timers(), Ok(1));
    assert!(handle.cancel_timer(far));
    assert!(!handle.cancel_timer(far));

    let mut socket = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let token = handle.alloc_token().unwrap();
    let events = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&events);
    struct Count(Arc<AtomicUsize>);
    impl EventHandler for Count {
        fn handle_event(&self, _: &Event) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    handle
        .register(&mut socket, token, Interest::WRITABLE, Count(counted))
        .unwrap()
        .unwrap();
    assert!(wait_until(Duration::from_secs(5), || {
        events.load(Ordering::SeqCst) > 0
    }));
    handle
        .reregister(&mut socket, token, Interest::READABLE)
        .unwrap()
        .unwrap();
    handle.deregister(&mut socket, token).unwrap().unwrap();
    assert!(handle
        .register(
            &mut socket,
            mio::Token(0),
            Interest::READABLE,
            Count(events)
        )
        .unwrap()
        .is_err());
    assert_eq!(handle.handler_panics(), Ok(0));
    assert_eq!(handle.timer_panics(), Ok(0));
    assert!(handle.get_compute_metrics().unwrap().tasks_completed() >= 2);

    handle.stop();
    runner.join().unwrap().unwrap();
}

#[test]
fn handle_reports_loop_gone_after_drop() {
    let event_loop = EventLoop::new(1, 8, 10).unwrap();
    let handle = event_loop.handle();
    let far_timer = event_loop.schedule_once(Duration::from_secs(3600), || {});
    drop(event_loop);

    let mut socket = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let token = mio::Token(7);
    struct Nothing;
    impl EventHandler for Nothing {
        fn handle_event(&self, _: &Event) {}
    }
    assert!(matches!(
        handle.register(&mut socket, token, Interest::READABLE, Nothing),
        Err(LoopGone)
    ));
    assert!(matches!(
        handle.reregister(&mut socket, token, Interest::READABLE),
        Err(LoopGone)
    ));
    assert!(matches!(
        handle.deregister(&mut socket, token),
        Err(LoopGone)
    ));
    assert_eq!(handle.post(|| {}), Err(LoopGone));
    assert!(!handle.cancel_timer(far_timer));
    assert_eq!(handle.alloc_token(), Err(LoopGone));
    assert_eq!(handle.spawn_compute(|| {}), Err(LoopGone));
    assert_eq!(
        handle.spawn_compute_with_priority(|| {}, TaskPriority::Low),
        Err(LoopGone)
    );
    assert_eq!(handle.schedule_once(Duration::ZERO, || {}), Err(LoopGone));
    assert_eq!(
        handle.schedule_repeating(Duration::from_millis(1), || {}),
        Err(LoopGone)
    );
    assert_eq!(handle.pending_timers(), Err(LoopGone));
    assert_eq!(handle.handler_panics(), Err(LoopGone));
    assert_eq!(handle.timer_panics(), Err(LoopGone));
    assert!(handle.get_compute_metrics().is_err());
    assert!(!handle.is_reactor_thread());
    handle.stop();
    assert_eq!(LoopGone.to_string(), "mill-io event loop is gone");
    assert!(format!("{LoopGone:?}").contains("LoopGone"));
    let boxed: Box<dyn std::error::Error> = Box::new(LoopGone);
    assert!(boxed.is::<LoopGone>());
}

fn arm_probes(event_loop: &EventLoop, dropped: &Arc<AtomicUsize>) -> Arc<AtomicUsize> {
    let handle = event_loop.handle();
    let ticks = Arc::new(AtomicUsize::new(0));

    let (h, p, t) = (
        handle.clone(),
        Probe(Arc::clone(dropped)),
        Arc::clone(&ticks),
    );
    event_loop.schedule_repeating(Duration::from_millis(1), move || {
        let _ = &p;
        t.fetch_add(1, Ordering::SeqCst);
        let _ = h.post(|| {});
    });

    let (h, p) = (handle.clone(), Probe(Arc::clone(dropped)));
    event_loop.schedule_once(Duration::from_secs(3600), move || {
        let _ = (&h, &p);
    });

    let mut socket = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    event_loop
        .register(
            &mut socket,
            event_loop.alloc_token(),
            Interest::READABLE,
            ProbedHandler {
                handle: handle.clone(),
                _probe: Probe(Arc::clone(dropped)),
            },
        )
        .unwrap();
    // The registry keeps the handler; the socket can go.
    drop(socket);
    ticks
}

#[test]
fn dropped_loop_frees_timers_and_handlers_that_hold_handles() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let event_loop = EventLoop::new(2, 64, 10).unwrap();
    let handle = event_loop.handle();
    arm_probes(&event_loop, &dropped);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    drop(event_loop);
    assert_eq!(dropped.load(Ordering::SeqCst), 3);
    assert_eq!(handle.post(|| {}), Err(LoopGone));
}

#[test]
fn dropped_loop_frees_probes_after_running() {
    for direct in [false, true] {
        let dropped = Arc::new(AtomicUsize::new(0));
        let event_loop = Arc::new(if direct {
            EventLoop::new_low_latency(64, 10).unwrap()
        } else {
            EventLoop::new(2, 64, 10).unwrap()
        });
        let handle = event_loop.handle();
        let ticks = arm_probes(&event_loop, &dropped);
        let runner = spawn_loop(&event_loop);
        assert!(wait_until(Duration::from_secs(5), || {
            ticks.load(Ordering::SeqCst) >= 3
        }));
        handle.stop();
        runner.join().unwrap().unwrap();
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        drop(event_loop);
        assert_eq!(dropped.load(Ordering::SeqCst), 3, "direct = {direct}");
    }
}

#[test]
fn drop_while_handles_are_in_use_never_hangs() {
    let (done_tx, done_rx) = mpsc::channel();
    thread::spawn(move || {
        for _ in 0..50 {
            let event_loop = Arc::new(EventLoop::new(2, 64, 10).unwrap());
            let runner = spawn_loop(&event_loop);
            let started = Arc::new(AtomicBool::new(false));
            let users: Vec<_> = (0..4)
                .map(|i| {
                    let handle = event_loop.handle();
                    let started = Arc::clone(&started);
                    thread::spawn(move || loop {
                        started.store(true, Ordering::SeqCst);
                        let inner = handle.clone();
                        let alive = match i % 3 {
                            0 => handle.post(|| {}).is_ok(),
                            1 => handle
                                .spawn_compute(move || {
                                    let _ = inner.spawn_compute(|| {});
                                })
                                .is_ok(),
                            _ => handle
                                .schedule_once(Duration::from_millis(1), move || {
                                    let _ = inner.post(|| {});
                                })
                                .is_ok(),
                        };
                        if !alive {
                            return;
                        }
                    })
                })
                .collect();
            while !started.load(Ordering::SeqCst) {
                thread::yield_now();
            }
            event_loop.stop();
            runner.join().unwrap().unwrap();
            drop(event_loop);
            for user in users {
                user.join().unwrap();
            }
        }
        let _ = done_tx.send(());
    });
    assert!(
        done_rx.recv_timeout(Duration::from_secs(60)).is_ok(),
        "dropping a loop while handles were in use hung"
    );
}

struct OwnsLoop {
    _event_loop: Arc<EventLoop>,
}

impl EventHandler for OwnsLoop {
    fn handle_event(&self, _: &Event) {}
}

/// Runs `call` on a helper thread and fails if it does not return in time.
fn returns_within(timeout: Duration, call: impl FnOnce() + Send + 'static) -> bool {
    let (done, finished) = mpsc::channel();
    thread::spawn(move || {
        call();
        let _ = done.send(());
    });
    finished.recv_timeout(timeout).is_ok()
}

#[test]
fn dropping_the_last_loop_inside_a_handle_call_does_not_hang() {
    // A cancelled timer drops its callback, which owns the last Arc<EventLoop>.
    let event_loop = Arc::new(EventLoop::new(1, 8, 10).unwrap());
    let handle = event_loop.handle();
    let owner = OwnsLoop {
        _event_loop: event_loop,
    };
    let timer = handle
        .schedule_once(Duration::from_secs(3600), move || drop(owner))
        .unwrap();
    let cancelling = handle.clone();
    assert!(
        returns_within(Duration::from_secs(10), move || {
            assert!(cancelling.cancel_timer(timer));
        }),
        "cancel_timer hung dropping the loop it was called on"
    );
    assert_eq!(handle.post(|| {}), Err(LoopGone));

    // A rejected registration drops its handler, which owns the last Arc<EventLoop>.
    let event_loop = Arc::new(EventLoop::new(1, 8, 10).unwrap());
    let handle = event_loop.handle();
    let registering = handle.clone();
    assert!(
        returns_within(Duration::from_secs(10), move || {
            let mut socket = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
            let handler = OwnsLoop {
                _event_loop: event_loop,
            };
            let rejected = registering
                .register(&mut socket, mio::Token(0), Interest::READABLE, handler)
                .unwrap();
            assert!(rejected.is_err());
        }),
        "register hung dropping the loop it was called on"
    );
    assert_eq!(handle.post(|| {}), Err(LoopGone));
}
