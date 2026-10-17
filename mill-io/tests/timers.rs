use mill_io::timer_wheel::TimerWheel;
use mill_io::EventLoop;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

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

type RunResult = Result<(), String>;

fn spawn_loop() -> (Arc<EventLoop>, JoinHandle<RunResult>) {
    let event_loop = Arc::new(EventLoop::new(2, 64, 50).expect("create event loop"));
    let runner = event_loop.clone();
    let handle = thread::spawn(move || runner.run().map_err(|e| e.to_string()));
    (event_loop, handle)
}

fn stop_loop(event_loop: &EventLoop, handle: JoinHandle<RunResult>) {
    event_loop.stop();
    let result = handle.join().expect("run() thread panicked");
    assert!(result.is_ok(), "run() returned an error: {result:?}");
}

fn counter() -> Arc<AtomicUsize> {
    Arc::new(AtomicUsize::new(0))
}

fn move_wheel_clock_past_tick_zero(event_loop: &EventLoop) {
    let fired = counter();
    let f = fired.clone();
    event_loop.schedule_once(Duration::from_millis(5), move || {
        f.fetch_add(1, Ordering::SeqCst);
    });
    assert!(
        wait_until(Duration::from_secs(5), || fired.load(Ordering::SeqCst) == 1),
        "warm-up timer did not fire"
    );
}

#[test]
fn max_delay_saturates() {
    let (event_loop, handle) = spawn_loop();
    move_wheel_clock_past_tick_zero(&event_loop);

    let fired = counter();
    let f1 = fired.clone();
    let f2 = fired.clone();
    let once = event_loop.schedule_once(Duration::MAX, move || {
        f1.fetch_add(1, Ordering::SeqCst);
    });
    let repeating = event_loop.schedule_repeating(Duration::MAX, move || {
        f2.fetch_add(1, Ordering::SeqCst);
    });
    assert_eq!(event_loop.pending_timers(), 2);

    // A deadline that wrapped to just behind the cursor fires only when the
    // 256 ms level-0 rotation comes round, so the sentinel is due after that.
    let sentinel = counter();
    let s = sentinel.clone();
    event_loop.schedule_once(Duration::from_millis(300), move || {
        s.fetch_add(1, Ordering::SeqCst);
    });
    assert!(
        wait_until(Duration::from_secs(5), || sentinel.load(Ordering::SeqCst)
            == 1),
        "sentinel timer did not fire"
    );

    assert_eq!(fired.load(Ordering::SeqCst), 0, "Duration::MAX timer fired");
    assert_eq!(event_loop.pending_timers(), 2);

    assert!(event_loop.cancel_timer(once));
    assert!(event_loop.cancel_timer(repeating));
    assert_eq!(event_loop.pending_timers(), 0);
    assert!(!event_loop.cancel_timer(once), "double cancel succeeded");

    stop_loop(&event_loop, handle);
    assert_eq!(fired.load(Ordering::SeqCst), 0);
}

#[test]
fn huge_delays_saturate_on_the_wheel() {
    let wheel = TimerWheel::new();

    let past_tick_zero = counter();
    let p = past_tick_zero.clone();
    wheel.schedule_once(Duration::from_millis(1), move || {
        p.fetch_add(1, Ordering::SeqCst);
    });
    assert!(wait_until(Duration::from_secs(5), || {
        wheel.tick();
        past_tick_zero.load(Ordering::SeqCst) == 1
    }));

    let delays = [
        Duration::MAX,
        Duration::from_secs(u64::MAX),
        Duration::from_millis(u64::MAX),
        Duration::from_millis(u64::MAX - 1),
        Duration::MAX - Duration::from_millis(1),
    ];
    let fired = counter();
    let mut ids = Vec::new();
    for delay in delays {
        let f = fired.clone();
        ids.push(wheel.schedule_once(delay, move || {
            f.fetch_add(1, Ordering::SeqCst);
        }));
        let f = fired.clone();
        ids.push(wheel.schedule_repeating(delay, move || {
            f.fetch_add(1, Ordering::SeqCst);
        }));
    }
    assert_eq!(wheel.pending_count(), ids.len());

    let next = wheel.time_until_next().expect("timers pending");
    assert!(next > Duration::from_secs(365 * 24 * 3600), "{next:?}");

    // Due after one 256 ms level-0 rotation, when a wrapped deadline fires.
    let sentinel = counter();
    let s = sentinel.clone();
    wheel.schedule_once(Duration::from_millis(300), move || {
        s.fetch_add(1, Ordering::SeqCst);
    });
    assert!(wait_until(Duration::from_secs(5), || {
        wheel.tick();
        sentinel.load(Ordering::SeqCst) == 1
    }));
    assert_eq!(fired.load(Ordering::SeqCst), 0, "a saturated timer fired");
    assert_eq!(wheel.pending_count(), ids.len());

    for id in ids {
        assert!(wheel.cancel(id));
    }
    assert_eq!(wheel.pending_count(), 0);
    assert!(wheel.time_until_next().is_none());
}

#[test]
fn zero_interval_is_clamped() {
    let (event_loop, handle) = spawn_loop();

    let fired = counter();
    let f = fired.clone();
    let id = event_loop.schedule_repeating(Duration::ZERO, move || {
        f.fetch_add(1, Ordering::SeqCst);
    });
    assert!(
        wait_until(Duration::from_secs(5), || fired.load(Ordering::SeqCst) >= 1),
        "zero-interval timer never fired"
    );

    let start_count = fired.load(Ordering::SeqCst);
    let started = Instant::now();
    thread::sleep(Duration::from_secs(1));
    let fires = fired.load(Ordering::SeqCst) - start_count;
    let elapsed = started.elapsed().as_secs_f64();

    assert!(event_loop.cancel_timer(id));
    stop_loop(&event_loop, handle);

    let per_second = fires as f64 / elapsed;
    assert!(
        per_second <= 1100.0,
        "zero-interval timer fired {fires} times in {elapsed:.3}s ({per_second:.0}/s)"
    );
    assert!(
        fires >= 10,
        "zero-interval timer stopped repeating: {fires}"
    );
}

#[test]
fn sub_tick_interval_is_clamped_under_tight_ticking() {
    let wheel = TimerWheel::new();
    let fired = counter();
    let f1 = fired.clone();
    let f2 = fired.clone();
    wheel.schedule_repeating(Duration::ZERO, move || {
        f1.fetch_add(1, Ordering::SeqCst);
    });
    wheel.schedule_repeating(Duration::from_micros(10), move || {
        f2.fetch_add(1, Ordering::SeqCst);
    });

    let started = Instant::now();
    while started.elapsed() < Duration::from_millis(500) {
        wheel.tick();
    }
    let elapsed = started.elapsed().as_secs_f64();
    let per_timer_per_second = fired.load(Ordering::SeqCst) as f64 / 2.0 / elapsed;

    assert!(
        per_timer_per_second <= 1100.0,
        "sub-tick timers fired {per_timer_per_second:.0}/s each"
    );
    assert_eq!(wheel.pending_count(), 2);
}

#[test]
fn zero_interval_does_not_spin() {
    let wheel = TimerWheel::new();
    let fired = counter();
    let f = fired.clone();
    wheel.schedule_repeating(Duration::ZERO, move || {
        f.fetch_add(1, Ordering::SeqCst);
    });

    let mut zero_timeouts = 0u64;
    let mut wasted = 0u64;
    let started = Instant::now();
    while started.elapsed() < Duration::from_millis(200) {
        let next = wheel.time_until_next().expect("timer pending");
        let before = fired.load(Ordering::SeqCst);
        wheel.tick();
        if next == Duration::ZERO {
            zero_timeouts += 1;
            if fired.load(Ordering::SeqCst) == before {
                wasted += 1;
            }
        } else {
            assert!(next <= Duration::from_millis(1), "{next:?}");
        }
    }

    assert_eq!(
        wasted, 0,
        "{wasted} of {zero_timeouts} zero poll timeouts fired nothing"
    );
    assert!(fired.load(Ordering::SeqCst) >= 10);
}

#[test]
fn timer_panic_keeps_loop_running() {
    let (event_loop, handle) = spawn_loop();

    event_loop.schedule_once(Duration::from_millis(5), || panic!("one-shot timer panic"));

    let repeat_fires = counter();
    let r = repeat_fires.clone();
    let repeating = event_loop.schedule_repeating(Duration::from_millis(5), move || {
        r.fetch_add(1, Ordering::SeqCst);
        panic!("repeating timer panic");
    });

    let later = counter();
    let l = later.clone();
    event_loop.schedule_once(Duration::from_millis(40), move || {
        l.fetch_add(1, Ordering::SeqCst);
    });

    assert!(
        wait_until(Duration::from_secs(5), || later.load(Ordering::SeqCst) == 1),
        "timer due after the panics did not fire"
    );
    assert!(
        wait_until(Duration::from_secs(5), || repeat_fires
            .load(Ordering::SeqCst)
            >= 3),
        "panicking repeating timer was not re-armed"
    );
    assert!(!handle.is_finished(), "run() exited after a timer panic");

    assert!(event_loop.cancel_timer(repeating));
    let after = counter();
    let a = after.clone();
    event_loop.schedule_once(Duration::from_millis(5), move || {
        a.fetch_add(1, Ordering::SeqCst);
    });
    assert!(
        wait_until(Duration::from_secs(5), || after.load(Ordering::SeqCst) == 1),
        "timer scheduled after the panics did not fire"
    );
    assert_eq!(event_loop.pending_timers(), 0);

    stop_loop(&event_loop, handle);
}
