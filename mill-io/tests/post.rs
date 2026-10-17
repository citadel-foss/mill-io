use mill_io::EventLoop;
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
        Arc, Barrier,
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

fn pool_and_direct() -> [(&'static str, Arc<EventLoop>); 2] {
    [
        ("pool", Arc::new(EventLoop::new(2, 1024, 10).unwrap())),
        (
            "direct",
            Arc::new(EventLoop::new_low_latency(1024, 10).unwrap()),
        ),
    ]
}

#[test]
fn post_runs_each_closure_once_on_reactor() {
    const THREADS: usize = 16;
    const PER_THREAD: usize = 10_000;
    const TOTAL: usize = THREADS * PER_THREAD;

    for (mode, event_loop) in pool_and_direct() {
        let runner = spawn_loop(&event_loop);

        let run_counts: Arc<Vec<AtomicU8>> =
            Arc::new((0..TOTAL).map(|_| AtomicU8::new(0)).collect());
        let last_index_by_producer: Arc<Vec<AtomicUsize>> =
            Arc::new((0..THREADS).map(|_| AtomicUsize::new(0)).collect());
        let total = Arc::new(AtomicUsize::new(0));
        let off_reactor = Arc::new(AtomicUsize::new(0));
        let out_of_order = Arc::new(AtomicUsize::new(0));
        let start = Arc::new(Barrier::new(THREADS));

        let producers: Vec<_> = (0..THREADS)
            .map(|t| {
                let event_loop = Arc::clone(&event_loop);
                let run_counts = Arc::clone(&run_counts);
                let last_index_by_producer = Arc::clone(&last_index_by_producer);
                let total = Arc::clone(&total);
                let off_reactor = Arc::clone(&off_reactor);
                let out_of_order = Arc::clone(&out_of_order);
                let start = Arc::clone(&start);
                thread::spawn(move || {
                    assert!(!event_loop.is_reactor_thread());
                    start.wait();
                    for i in 0..PER_THREAD {
                        let el = Arc::clone(&event_loop);
                        let run_counts = Arc::clone(&run_counts);
                        let last_index_by_producer = Arc::clone(&last_index_by_producer);
                        let total = Arc::clone(&total);
                        let off_reactor = Arc::clone(&off_reactor);
                        let out_of_order = Arc::clone(&out_of_order);
                        event_loop
                            .post(move || {
                                if !el.is_reactor_thread() {
                                    off_reactor.fetch_add(1, Ordering::SeqCst);
                                }
                                // Indices are stored +1 so 0 means "none yet".
                                let prev = last_index_by_producer[t].swap(i + 1, Ordering::SeqCst);
                                if prev != i {
                                    out_of_order.fetch_add(1, Ordering::SeqCst);
                                }
                                run_counts[t * PER_THREAD + i].fetch_add(1, Ordering::SeqCst);
                                total.fetch_add(1, Ordering::SeqCst);
                            })
                            .expect("post failed");
                    }
                })
            })
            .collect();
        for producer in producers {
            producer.join().unwrap();
        }

        let all_ran = wait_until(Duration::from_secs(30), || {
            total.load(Ordering::SeqCst) >= TOTAL
        });
        assert!(
            !event_loop.is_reactor_thread(),
            "the test thread is not the reactor"
        );
        event_loop.stop();
        assert_eq!(runner.join().unwrap(), Ok(()), "{mode}: run() failed");

        assert!(
            all_ran,
            "{mode}: only {} of {TOTAL} closures ran within 30 s",
            total.load(Ordering::SeqCst)
        );
        assert_eq!(total.load(Ordering::SeqCst), TOTAL, "{mode}: total runs");
        let wrong: Vec<usize> = (0..TOTAL)
            .filter(|&id| run_counts[id].load(Ordering::SeqCst) != 1)
            .take(5)
            .collect();
        assert!(
            wrong.is_empty(),
            "{mode}: closures not run exactly once: {wrong:?}"
        );
        assert_eq!(
            off_reactor.load(Ordering::SeqCst),
            0,
            "{mode}: closures ran off the reactor thread"
        );
        assert_eq!(
            out_of_order.load(Ordering::SeqCst),
            0,
            "{mode}: one producer's closures ran out of order"
        );
    }
}

#[test]
fn post_before_run_waits_for_run() {
    let event_loop = Arc::new(EventLoop::new(1, 64, 10).unwrap());
    let ran_on_reactor = Arc::new(AtomicBool::new(false));
    {
        let el = Arc::clone(&event_loop);
        let ran_on_reactor = Arc::clone(&ran_on_reactor);
        event_loop
            .post(move || ran_on_reactor.store(el.is_reactor_thread(), Ordering::SeqCst))
            .unwrap();
    }
    assert!(!ran_on_reactor.load(Ordering::SeqCst));

    let runner = spawn_loop(&event_loop);
    assert!(
        wait_until(Duration::from_secs(1), || ran_on_reactor
            .load(Ordering::SeqCst)),
        "a closure posted before run() did not run on the reactor thread"
    );
    event_loop.stop();
    assert_eq!(runner.join().unwrap(), Ok(()));
}

#[test]
fn post_panic_is_caught_and_counted() {
    for (mode, event_loop) in pool_and_direct() {
        let runner = spawn_loop(&event_loop);
        event_loop
            .post(|| panic!("intentional panic in a posted closure"))
            .unwrap();
        let after = Arc::new(AtomicBool::new(false));
        {
            let after = Arc::clone(&after);
            event_loop
                .post(move || after.store(true, Ordering::SeqCst))
                .unwrap();
        }
        assert!(
            wait_until(Duration::from_secs(1), || after.load(Ordering::SeqCst)),
            "{mode}: the closure after the panicking one did not run"
        );
        assert_eq!(event_loop.handler_panics(), 1, "{mode}");
        assert!(!runner.is_finished(), "{mode}: run() returned");
        event_loop.stop();
        assert_eq!(runner.join().unwrap(), Ok(()), "{mode}");
    }
}

#[test]
fn closure_posted_from_reactor_runs_later() {
    let event_loop = Arc::new(EventLoop::new_low_latency(64, 10).unwrap());
    let runner = spawn_loop(&event_loop);
    let inner_ran = Arc::new(AtomicBool::new(false));
    {
        let el = Arc::clone(&event_loop);
        let inner_ran = Arc::clone(&inner_ran);
        event_loop
            .post(move || {
                let el2 = Arc::clone(&el);
                el.post(move || inner_ran.store(el2.is_reactor_thread(), Ordering::SeqCst))
                    .unwrap();
            })
            .unwrap();
    }
    assert!(
        wait_until(Duration::from_secs(1), || inner_ran.load(Ordering::SeqCst)),
        "a closure posted from the reactor thread did not run"
    );
    event_loop.stop();
    assert_eq!(runner.join().unwrap(), Ok(()));
}
