use mill_io::{EventHandler, EventLoop, Handle};
use mio::{event::Event, net::UdpSocket, Interest};
use std::{
    panic::{catch_unwind, UnwindSafe},
    sync::{
        atomic::{AtomicUsize, Ordering},
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

fn stop_loop(event_loop: &EventLoop, runner: JoinHandle<Result<(), String>>) {
    event_loop.stop();
    runner.join().unwrap().unwrap();
}

fn pool_and_direct() -> [(&'static str, Arc<EventLoop>); 2] {
    [
        ("pool", Arc::new(EventLoop::new(2, 64, 10).unwrap())),
        (
            "direct",
            Arc::new(EventLoop::new_low_latency(64, 10).unwrap()),
        ),
    ]
}

fn panic_message(f: impl FnOnce() + UnwindSafe) -> String {
    let payload = catch_unwind(f).expect_err("expected a panic");
    if let Some(s) = payload.downcast_ref::<&str>() {
        s.to_string()
    } else {
        payload
            .downcast_ref::<String>()
            .cloned()
            .unwrap_or_default()
    }
}

fn assert_outside_message(function: &str, f: impl FnOnce() + UnwindSafe) {
    assert_eq!(
        panic_message(f),
        format!(
            "mill_io::{function} called outside a mill-io context; \
             use EventLoop::enter() or #[mill_io::main]"
        )
    );
}

#[derive(Debug, PartialEq)]
struct Seen {
    current: Option<Handle>,
    on_reactor: bool,
}

fn seen(event_loop: &Handle) -> Seen {
    Seen {
        current: mill_io::try_current(),
        on_reactor: event_loop.is_reactor_thread(),
    }
}

struct Report {
    handle: Handle,
    tx: mpsc::Sender<Seen>,
}

impl EventHandler for Report {
    fn handle_event(&self, _: &Event) {
        let _ = self.tx.send(seen(&self.handle));
    }
}

#[test]
fn context_is_set_on_reactor_io_and_compute_threads() {
    for (mode, event_loop) in pool_and_direct() {
        let handle = event_loop.handle();
        let runner = spawn_loop(&event_loop);
        let (tx, rx) = mpsc::channel();

        let (h, t) = (handle.clone(), tx.clone());
        event_loop.post(move || t.send(seen(&h)).unwrap()).unwrap();
        let reactor = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(reactor.current.as_ref(), Some(&handle), "{mode}: post");
        assert!(reactor.on_reactor, "{mode}: post");

        let (h, t) = (handle.clone(), tx.clone());
        event_loop.schedule_once(Duration::from_millis(1), move || t.send(seen(&h)).unwrap());
        let timer = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(timer, reactor, "{mode}: timer");

        let (h, t) = (handle.clone(), tx.clone());
        event_loop.spawn_compute(move || t.send(seen(&h)).unwrap());
        let compute = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(compute.current.as_ref(), Some(&handle), "{mode}: compute");
        assert!(!compute.on_reactor, "{mode}: compute");

        let mut socket = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let report = Report {
            handle: handle.clone(),
            tx: tx.clone(),
        };
        event_loop
            .register(
                &mut socket,
                event_loop.alloc_token(),
                Interest::READABLE,
                report,
            )
            .unwrap();
        let peer = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        peer.send_to(b"x", socket.local_addr().unwrap()).unwrap();
        let io = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(io.current.as_ref(), Some(&handle), "{mode}: handler");
        assert_eq!(io.on_reactor, mode == "direct", "{mode}: handler");

        stop_loop(&event_loop, runner);
    }
}

#[test]
fn context_ends_with_run() {
    let event_loop = Arc::new(EventLoop::new(1, 8, 10).unwrap());
    let runner = Arc::clone(&event_loop);
    let thread = thread::spawn(move || {
        let stopper = runner.handle();
        runner.post(move || stopper.stop()).unwrap();
        runner.run().unwrap();
        mill_io::try_current()
    });
    assert_eq!(thread.join().unwrap(), None);
}

#[test]
fn context_absent_on_unrelated_threads() {
    let event_loop = Arc::new(EventLoop::new(1, 8, 10).unwrap());
    let runner = spawn_loop(&event_loop);
    let _guard = event_loop.enter();
    assert!(mill_io::try_current().is_some());
    assert_eq!(
        thread::spawn(mill_io::try_current).join().unwrap(),
        None,
        "a new thread does not inherit the context"
    );
    stop_loop(&event_loop, runner);
}

#[test]
fn nested_guards_restore_the_outer_loop() {
    let a = EventLoop::new(1, 8, 10).unwrap();
    let b = EventLoop::new(1, 8, 10).unwrap();
    assert_eq!(mill_io::try_current(), None);
    {
        let _a = a.enter();
        assert_eq!(mill_io::current(), a.handle());
        {
            let b_handle = b.handle();
            let _b = b_handle.enter();
            assert_eq!(mill_io::current(), b.handle());
            {
                let _a_again = a.enter();
                assert_eq!(mill_io::current(), a.handle());
            }
            assert_eq!(mill_io::current(), b.handle());
        }
        assert_eq!(mill_io::current(), a.handle());
    }
    assert_eq!(mill_io::try_current(), None);
}

#[test]
fn guards_dropped_out_of_order_keep_the_context_consistent() {
    let a = EventLoop::new(1, 8, 10).unwrap();
    let b = EventLoop::new(1, 8, 10).unwrap();
    let c = EventLoop::new(1, 8, 10).unwrap();

    let guard_a = a.enter();
    let guard_b = b.enter();
    drop(guard_a);
    assert_eq!(mill_io::current(), b.handle());
    drop(guard_b);
    assert_eq!(mill_io::try_current(), None);

    let guard_a = a.enter();
    let guard_b = b.enter();
    let guard_c = c.enter();
    drop(guard_b);
    assert_eq!(mill_io::current(), c.handle());
    drop(guard_c);
    assert_eq!(mill_io::current(), a.handle());
    drop(guard_a);
    assert_eq!(mill_io::try_current(), None);
}

#[test]
fn after_from_a_plain_thread_inside_enter() {
    let event_loop = Arc::new(EventLoop::new(1, 8, 10).unwrap());
    let runner = spawn_loop(&event_loop);
    let handle = event_loop.handle();
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _guard = handle.enter();
        let on_reactor = mill_io::current();
        mill_io::after(Duration::from_millis(5), move || {
            tx.send(on_reactor.is_reactor_thread()).unwrap()
        });
    })
    .join()
    .unwrap();
    assert_eq!(rx.recv_timeout(Duration::from_secs(5)), Ok(true));
    stop_loop(&event_loop, runner);
}

#[test]
fn post_from_a_compute_task_runs_on_the_reactor() {
    for (mode, event_loop) in pool_and_direct() {
        let runner = spawn_loop(&event_loop);
        let (tx, rx) = mpsc::channel();
        event_loop.spawn_compute(move || {
            let handle = mill_io::current();
            mill_io::post(move || tx.send(handle.is_reactor_thread()).unwrap());
        });
        assert_eq!(rx.recv_timeout(Duration::from_secs(5)), Ok(true), "{mode}");
        stop_loop(&event_loop, runner);
    }
}

#[test]
fn free_timer_functions_act_on_the_current_loop() {
    let event_loop = Arc::new(EventLoop::new(1, 8, 10).unwrap());
    let runner = spawn_loop(&event_loop);
    let _guard = event_loop.enter();

    let ticks = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&ticks);
    let repeating = mill_io::every(Duration::from_millis(2), move || {
        counted.fetch_add(1, Ordering::SeqCst);
    });
    assert!(wait_until(Duration::from_secs(5), || {
        ticks.load(Ordering::SeqCst) >= 3
    }));
    assert!(mill_io::cancel_timer(repeating));
    assert!(!mill_io::cancel_timer(repeating));

    let never = mill_io::after(Duration::from_secs(3600), || {});
    assert_eq!(event_loop.pending_timers(), 1);
    assert!(mill_io::cancel_timer(never));
    assert_eq!(event_loop.pending_timers(), 0);

    let (tx, rx) = mpsc::channel();
    mill_io::after(Duration::from_millis(1), move || {
        let inner = tx.clone();
        mill_io::after(Duration::from_millis(1), move || inner.send(()).unwrap());
    });
    assert_eq!(rx.recv_timeout(Duration::from_secs(5)), Ok(()));
    stop_loop(&event_loop, runner);
}

#[test]
fn two_loops_never_see_each_others_context() {
    const ROUNDS: usize = 200;
    let loops = [
        Arc::new(EventLoop::new(2, 64, 10).unwrap()),
        Arc::new(EventLoop::new_low_latency(64, 10).unwrap()),
    ];
    let runners: Vec<_> = loops.iter().map(spawn_loop).collect();
    let mismatches = Arc::new(AtomicUsize::new(0));
    let checks = Arc::new(AtomicUsize::new(0));

    let check = |own: &Handle| {
        let own = own.clone();
        let mismatches = Arc::clone(&mismatches);
        let checks = Arc::clone(&checks);
        move || {
            if mill_io::try_current().as_ref() != Some(&own) {
                mismatches.fetch_add(1, Ordering::SeqCst);
            }
            checks.fetch_add(1, Ordering::SeqCst);
        }
    };

    for _ in 0..ROUNDS {
        for event_loop in &loops {
            let own = event_loop.handle();
            event_loop.post(check(&own)).unwrap();
            event_loop.spawn_compute(check(&own));
            event_loop.schedule_once(Duration::ZERO, check(&own));
        }
    }
    assert!(wait_until(Duration::from_secs(10), || {
        checks.load(Ordering::SeqCst) == ROUNDS * 2 * 3
    }));
    assert_eq!(mismatches.load(Ordering::SeqCst), 0);
    for (event_loop, runner) in loops.iter().zip(runners) {
        stop_loop(event_loop, runner);
    }
}

#[test]
fn free_functions_on_a_dropped_loop_do_nothing() {
    let event_loop = EventLoop::new(1, 8, 10).unwrap();
    let handle = event_loop.handle();
    drop(event_loop);
    let _guard = handle.enter();
    assert_eq!(mill_io::current(), handle);
    mill_io::post(|| unreachable!());
    let id = mill_io::after(Duration::ZERO, || unreachable!());
    assert!(!mill_io::cancel_timer(id));
    let id = mill_io::every(Duration::from_millis(1), || unreachable!());
    assert!(!mill_io::cancel_timer(id));
}

#[test]
fn outside_context_try_current_is_none() {
    assert_eq!(mill_io::try_current(), None);
}

#[test]
fn outside_context_current_panics() {
    assert_outside_message("current", || {
        mill_io::current();
    });
}

#[test]
fn outside_context_post_panics() {
    assert_outside_message("post", || mill_io::post(|| {}));
}

#[test]
fn outside_context_after_panics() {
    assert_outside_message("after", || {
        mill_io::after(Duration::ZERO, || {});
    });
}

#[test]
fn outside_context_every_panics() {
    assert_outside_message("every", || {
        mill_io::every(Duration::from_millis(1), || {});
    });
}

#[test]
fn outside_context_cancel_timer_panics() {
    let event_loop = EventLoop::new(1, 8, 10).unwrap();
    let id = event_loop.schedule_once(Duration::from_secs(60), || {});
    assert_outside_message("cancel_timer", move || {
        mill_io::cancel_timer(id);
    });
}

#[test]
fn outside_context_spawn_panics() {
    assert_outside_message("spawn", || {
        mill_io::spawn(|| 1);
    });
}

#[test]
fn outside_context_spawn_with_priority_panics() {
    assert_outside_message("spawn_with_priority", || {
        mill_io::spawn_with_priority(mill_io::TaskPriority::Low, || 1);
    });
}

#[test]
fn outside_context_join_macro_panics() {
    assert_outside_message("spawn", || {
        let _ = mill_io::join!(|| 1, || 2);
    });
}
