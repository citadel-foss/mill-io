use mill_io::{EventLoop, JoinError, JoinHandle, JoinTimeout, TaskPriority};
use std::{
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc, Mutex,
    },
    thread::{self, JoinHandle as ThreadHandle},
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

fn spawn_loop(event_loop: &Arc<EventLoop>) -> ThreadHandle<Result<(), String>> {
    let event_loop = Arc::clone(event_loop);
    thread::spawn(move || event_loop.run().map_err(|e| e.to_string()))
}

fn stop_loop(event_loop: &EventLoop, runner: ThreadHandle<Result<(), String>>) {
    event_loop.stop();
    runner.join().unwrap().unwrap();
}

fn compute_loop(workers: usize) -> EventLoop {
    EventLoop::builder()
        .io_workers(1)
        .compute_workers(workers)
        .build()
        .unwrap()
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        s.to_string()
    } else {
        payload
            .downcast_ref::<String>()
            .cloned()
            .unwrap_or_default()
    }
}

/// A task that runs until the returned sender sends or is dropped.
fn gated(event_loop: &EventLoop) -> (mpsc::Sender<()>, JoinHandle<&'static str>) {
    let (release, gate) = mpsc::channel::<()>();
    let _guard = event_loop.enter();
    let task = mill_io::spawn(move || {
        let _ = gate.recv();
        "released"
    });
    (release, task)
}

#[test]
fn join_handle_is_send_and_sync() {
    fn assert_traits<T: Send + Sync>() {}
    assert_traits::<JoinHandle<String>>();
    assert_traits::<JoinTimeout<String>>();
    assert_traits::<JoinError>();
}

#[test]
fn join_returns_the_task_value() {
    let event_loop = compute_loop(2);
    let _guard = event_loop.enter();
    assert_eq!(mill_io::spawn(|| 6 * 7).join(), Ok(42));
    let owned = mill_io::spawn(|| vec![String::from("a"), String::from("b")]);
    assert_eq!(owned.join().unwrap(), ["a", "b"]);
}

#[test]
fn join_reports_the_panic_message() {
    let event_loop = compute_loop(1);
    let _guard = event_loop.enter();
    let literal = mill_io::spawn(|| -> u8 { panic!("static message") });
    assert_eq!(
        literal.join(),
        Err(JoinError::Panicked("static message".to_string()))
    );
    let formatted = mill_io::spawn(|| -> u8 { panic!("formatted {}", 7) });
    assert_eq!(
        formatted.join(),
        Err(JoinError::Panicked("formatted 7".to_string()))
    );
    let opaque = mill_io::spawn(|| -> u8 { std::panic::panic_any(5_u32) });
    assert!(matches!(opaque.join(), Err(JoinError::Panicked(_))));

    let metrics = event_loop.get_compute_metrics();
    assert!(wait_until(Duration::from_secs(5), || metrics
        .tasks_failed()
        == 3));
    assert_eq!(mill_io::spawn(|| 1).join(), Ok(1));
}

#[test]
fn join_timeout_gives_back_a_handle_that_joins_later() {
    let event_loop = compute_loop(1);
    let (release, task) = gated(&event_loop);
    let started = Instant::now();
    let JoinTimeout(task) = task.join_timeout(Duration::from_millis(30)).unwrap_err();
    assert!(started.elapsed() >= Duration::from_millis(30));
    assert!(!task.is_finished());
    release.send(()).unwrap();
    assert_eq!(task.join(), Ok("released"));
}

#[test]
fn join_timeout_returns_the_value_when_ready() {
    let event_loop = compute_loop(1);
    let (release, task) = gated(&event_loop);
    release.send(()).unwrap();
    assert_eq!(
        task.join_timeout(Duration::from_secs(5)).ok(),
        Some("released")
    );

    let _guard = event_loop.enter();
    let huge = mill_io::spawn(|| 3);
    assert_eq!(huge.join_timeout(Duration::MAX).ok(), Some(3));
}

#[test]
fn join_timeout_on_a_failed_task_returns_the_handle_with_the_error() {
    let event_loop = compute_loop(1);
    let _guard = event_loop.enter();
    let task = mill_io::spawn(|| -> u8 { panic!("failed") });
    let JoinTimeout(task) = task.join_timeout(Duration::from_secs(5)).unwrap_err();
    assert!(task.is_finished());
    assert_eq!(task.join(), Err(JoinError::Panicked("failed".to_string())));
}

#[test]
fn is_finished_turns_true_when_the_task_returns() {
    let event_loop = compute_loop(1);
    let (release, task) = gated(&event_loop);
    assert!(!task.is_finished());
    release.send(()).unwrap();
    assert!(wait_until(Duration::from_secs(5), || task.is_finished()));
    assert_eq!(task.join(), Ok("released"));
}

#[test]
fn detached_tasks_still_run() {
    let event_loop = compute_loop(2);
    let _guard = event_loop.enter();
    let ran = Arc::new(AtomicUsize::new(0));
    for _ in 0..20 {
        let ran = Arc::clone(&ran);
        drop(mill_io::spawn(move || ran.fetch_add(1, Ordering::SeqCst)));
    }
    assert!(wait_until(Duration::from_secs(5), || {
        ran.load(Ordering::SeqCst) == 20
    }));
}

#[test]
fn join_on_the_reactor_thread_panics_instead_of_deadlocking() {
    for direct in [false, true] {
        let event_loop = Arc::new(if direct {
            EventLoop::new_low_latency(64, 10).unwrap()
        } else {
            EventLoop::new(1, 64, 10).unwrap()
        });
        let runner = spawn_loop(&event_loop);
        let (tx, rx) = mpsc::channel();

        let from_post = tx.clone();
        event_loop
            .post(move || {
                let task = mill_io::spawn(|| 1);
                let result = catch_unwind(AssertUnwindSafe(|| task.join()));
                from_post.send(result.map_err(panic_message)).unwrap();
            })
            .unwrap();
        let from_timer = tx.clone();
        event_loop.schedule_once(Duration::from_millis(1), move || {
            let finished = mill_io::spawn(|| 2);
            let _ = wait_until(Duration::from_secs(5), || finished.is_finished());
            let result = catch_unwind(AssertUnwindSafe(|| finished.join()));
            from_timer.send(result.map_err(panic_message)).unwrap();
        });

        for _ in 0..2 {
            let message = rx
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap_err();
            assert!(message.contains("would deadlock"), "{message}");
        }
        stop_loop(&event_loop, runner);
    }
}

#[test]
fn join_timeout_is_allowed_on_the_reactor_thread() {
    let event_loop = Arc::new(EventLoop::new(1, 64, 10).unwrap());
    let runner = spawn_loop(&event_loop);
    let (tx, rx) = mpsc::channel();
    event_loop
        .post(move || {
            let task = mill_io::spawn(|| 9);
            tx.send(task.join_timeout(Duration::from_secs(5)).ok())
                .unwrap();
        })
        .unwrap();
    assert_eq!(rx.recv_timeout(Duration::from_secs(5)), Ok(Some(9)));
    stop_loop(&event_loop, runner);
}

#[test]
fn join_on_another_loops_reactor_thread_is_allowed() {
    let reactor = Arc::new(EventLoop::new(1, 64, 10).unwrap());
    let compute = compute_loop(1);
    let other = compute.handle();
    let runner = spawn_loop(&reactor);
    let (tx, rx) = mpsc::channel();
    reactor
        .post(move || tx.send(other.spawn(|| 5).join()).unwrap())
        .unwrap();
    assert_eq!(rx.recv_timeout(Duration::from_secs(5)), Ok(Ok(5)));
    stop_loop(&reactor, runner);
}

#[test]
fn spawn_on_a_dropped_loop_is_loop_stopped() {
    let event_loop = compute_loop(1);
    let handle = event_loop.handle();
    drop(event_loop);
    let task = handle.spawn(|| 1);
    assert!(task.is_finished());
    assert_eq!(task.join(), Err(JoinError::LoopStopped));

    let _guard = handle.enter();
    assert_eq!(mill_io::spawn(|| 1).join(), Err(JoinError::LoopStopped));
    assert_eq!(
        mill_io::spawn_with_priority(TaskPriority::High, || 1).join(),
        Err(JoinError::LoopStopped)
    );
}

#[test]
fn spawn_while_the_loop_is_being_dropped_is_loop_stopped() {
    let event_loop = compute_loop(1);
    let handle = event_loop.handle();
    let (tx, rx) = mpsc::channel();
    let (started_tx, started) = mpsc::channel();
    event_loop.spawn_compute(move || {
        started_tx.send(()).unwrap();
        // The pool runs this task to completion while the loop drops, after
        // its last strong reference is gone.
        while handle.alloc_token().is_ok() {
            thread::yield_now();
        }
        tx.send(mill_io::spawn(|| 1).join()).unwrap();
    });
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    drop(event_loop);
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(5)),
        Ok(Err(JoinError::LoopStopped))
    );
}

#[test]
fn stop_while_join_handles_are_pending() {
    let event_loop = Arc::new(compute_loop(1));
    let runner = spawn_loop(&event_loop);
    let (release, blocker) = gated(&event_loop);
    let _guard = event_loop.enter();
    let queued: Vec<_> = (0..5).map(|i| mill_io::spawn(move || i * 10)).collect();
    stop_loop(&event_loop, runner);
    assert!(queued.iter().all(|task| !task.is_finished()));

    release.send(()).unwrap();
    assert_eq!(blocker.join(), Ok("released"));
    let results: Vec<_> = queued.into_iter().map(JoinHandle::join).collect();
    assert_eq!(results, [Ok(0), Ok(10), Ok(20), Ok(30), Ok(40)]);
}

#[test]
fn tasks_queued_when_the_loop_is_dropped_still_finish() {
    let event_loop = compute_loop(1);
    let (release, blocker) = gated(&event_loop);
    let queued: Vec<_> = {
        let _guard = event_loop.enter();
        (0..3).map(|i| mill_io::spawn(move || i)).collect()
    };
    let dropper = thread::spawn(move || drop(event_loop));
    release.send(()).unwrap();
    dropper.join().unwrap();
    assert_eq!(blocker.join(), Ok("released"));
    let results: Vec<_> = queued.into_iter().map(JoinHandle::join).collect();
    assert_eq!(results, [Ok(0), Ok(1), Ok(2)]);
}

#[test]
fn spawn_from_a_timer_callback_and_from_a_compute_task() {
    let event_loop = Arc::new(compute_loop(2));
    let runner = spawn_loop(&event_loop);
    let (tx, rx) = mpsc::channel();
    let from_timer = tx.clone();
    event_loop.schedule_once(Duration::from_millis(1), move || {
        from_timer.send(mill_io::spawn(|| "timer")).unwrap();
    });
    let from_compute = tx.clone();
    event_loop.spawn_compute(move || {
        let nested = mill_io::spawn(|| "nested");
        let joined = mill_io::spawn(move || nested.join().unwrap())
            .join()
            .unwrap();
        from_compute.send(mill_io::spawn(move || joined)).unwrap();
    });
    let mut got: Vec<_> = (0..2)
        .map(|_| {
            let task: JoinHandle<&str> = rx.recv_timeout(Duration::from_secs(5)).unwrap();
            task.join().unwrap()
        })
        .collect();
    got.sort();
    assert_eq!(got, ["nested", "timer"]);
    stop_loop(&event_loop, runner);
}

#[test]
fn spawn_with_priority_runs_higher_priority_first() {
    let event_loop = compute_loop(1);
    let (release, blocker) = gated(&event_loop);
    let _guard = event_loop.enter();
    let order = Arc::new(Mutex::new(Vec::new()));
    let tasks: Vec<_> = [
        (TaskPriority::Low, "low"),
        (TaskPriority::Critical, "critical"),
        (TaskPriority::Normal, "normal"),
    ]
    .into_iter()
    .map(|(priority, name)| {
        let order = Arc::clone(&order);
        mill_io::spawn_with_priority(priority, move || order.lock().unwrap().push(name))
    })
    .collect();
    release.send(()).unwrap();
    blocker.join().unwrap();
    for task in tasks {
        task.join().unwrap();
    }
    assert_eq!(*order.lock().unwrap(), ["critical", "normal", "low"]);
}

#[test]
fn join_from_a_compute_worker_with_a_free_worker_succeeds() {
    let event_loop = compute_loop(2);
    let _guard = event_loop.enter();
    let outer = mill_io::spawn(|| mill_io::spawn(|| 21).join().map(|v| v * 2));
    assert_eq!(outer.join(), Ok(Ok(42)));
}

#[cfg(debug_assertions)]
#[test]
fn join_from_the_last_free_compute_worker_fails_fast_in_debug() {
    for workers in [1, 2, 3] {
        let event_loop = compute_loop(workers);
        let _guard = event_loop.enter();
        let all_busy = Arc::new(std::sync::Barrier::new(workers));
        let outers: Vec<_> = (0..workers)
            .map(|_| {
                let all_busy = Arc::clone(&all_busy);
                mill_io::spawn(move || {
                    all_busy.wait();
                    let inner = mill_io::spawn(|| 1);
                    catch_unwind(AssertUnwindSafe(|| inner.join())).map_err(panic_message)
                })
            })
            .collect();
        let results: Vec<_> = outers
            .into_iter()
            .map(|outer| outer.join().unwrap())
            .collect();
        let failed: Vec<_> = results.iter().filter_map(|r| r.as_ref().err()).collect();
        assert_eq!(failed.len(), 1, "{workers} workers: {results:?}");
        assert!(failed[0].contains("would deadlock"), "{}", failed[0]);
        assert_eq!(
            results.iter().filter(|r| **r == Ok(Ok(1))).count(),
            workers - 1
        );
    }
}

#[test]
fn join_macro_with_one_closure() {
    let event_loop = compute_loop(1);
    let _guard = event_loop.enter();
    let (only,) = mill_io::join!(|| "one");
    assert_eq!(only, Ok("one"));
}

#[test]
fn join_macro_with_two_closures_of_different_types() {
    let event_loop = compute_loop(2);
    let _guard = event_loop.enter();
    let (number, text) = mill_io::join!(|| 2_u64.pow(10), || String::from("text"),);
    assert_eq!(number, Ok(1024));
    assert_eq!(text, Ok(String::from("text")));
}

#[test]
fn join_macro_with_eight_closures_keeps_argument_order() {
    let event_loop = compute_loop(3);
    let _guard = event_loop.enter();
    let slow = |value: u32| {
        move || {
            thread::sleep(Duration::from_millis(u64::from(8 - value)));
            value
        }
    };
    let results = mill_io::join!(
        slow(0),
        slow(1),
        slow(2),
        slow(3),
        slow(4),
        slow(5),
        slow(6),
        slow(7)
    );
    assert_eq!(
        results,
        (Ok(0), Ok(1), Ok(2), Ok(3), Ok(4), Ok(5), Ok(6), Ok(7))
    );
}

#[test]
fn join_macro_runs_closures_concurrently() {
    const TASKS: usize = 4;
    let event_loop = compute_loop(TASKS);
    let _guard = event_loop.enter();
    let arrived = Arc::new(AtomicUsize::new(0));
    let meet = || {
        let arrived = Arc::clone(&arrived);
        move || {
            arrived.fetch_add(1, Ordering::SeqCst);
            let all_met = wait_until(Duration::from_secs(5), || {
                arrived.load(Ordering::SeqCst) == TASKS
            });
            thread::sleep(Duration::from_millis(100));
            all_met
        }
    };
    let started = Instant::now();
    let results = mill_io::join!(meet(), meet(), meet(), meet());
    let elapsed = started.elapsed();
    assert_eq!(results, (Ok(true), Ok(true), Ok(true), Ok(true)));
    assert!(
        elapsed < Duration::from_millis(350),
        "four 100 ms closures took {elapsed:?}"
    );
}

#[test]
fn join_macro_returns_the_others_when_one_panics() {
    let event_loop = compute_loop(2);
    let _guard = event_loop.enter();
    let (first, second, third) = mill_io::join!(|| 1, || -> i32 { panic!("second failed") }, || 3);
    assert_eq!(first, Ok(1));
    assert_eq!(
        second,
        Err(JoinError::Panicked("second failed".to_string()))
    );
    assert_eq!(third, Ok(3));
}

#[test]
fn join_error_and_timeout_display() {
    assert_eq!(
        JoinError::Panicked("x".into()).to_string(),
        "mill-io task panicked: x"
    );
    assert_eq!(
        JoinError::LoopStopped.to_string(),
        "mill-io task never ran: its loop was dropped"
    );
    let event_loop = compute_loop(1);
    let (release, task) = gated(&event_loop);
    let timeout = task.join_timeout(Duration::ZERO).unwrap_err();
    assert_eq!(timeout.to_string(), "mill-io task did not finish in time");
    assert!(format!("{timeout:?}").contains("finished: false"));
    release.send(()).unwrap();
    assert_eq!(timeout.0.join(), Ok("released"));
}
