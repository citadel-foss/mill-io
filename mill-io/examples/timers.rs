//! Timers on the event loop: one-shot and repeating timers, cancelling them, counting
//! pending ones, and a panicking callback that the loop survives.
//!
//! Run with `cargo run -p mill-io --example timers`. The loop stops itself after ~150 ms.

use mill_io::{error::Result, EventLoop};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn main() -> Result<()> {
    let event_loop = Arc::new(EventLoop::default());
    let started = Instant::now();
    let elapsed_ms = move || started.elapsed().as_millis();

    let heartbeats = Arc::new(AtomicUsize::new(0));
    let heartbeat = {
        let heartbeats = Arc::clone(&heartbeats);
        event_loop.schedule_repeating(Duration::from_millis(25), move || {
            let n = heartbeats.fetch_add(1, Ordering::SeqCst) + 1;
            println!("[{:>3} ms] heartbeat #{n}", elapsed_ms());
        })
    };

    let reminder = event_loop.schedule_once(Duration::from_millis(60), || {
        panic!("this timer was cancelled and must never fire");
    });

    event_loop.schedule_once(Duration::from_millis(40), move || {
        println!("[{:>3} ms] one-shot timer fired", elapsed_ms());
    });

    event_loop.schedule_once(Duration::from_millis(70), move || {
        println!(
            "[{:>3} ms] a timer callback is about to panic",
            elapsed_ms()
        );
        panic!("timer callback failed");
    });

    let weak_loop = Arc::downgrade(&event_loop);
    event_loop.schedule_once(Duration::from_millis(150), move || {
        let Some(event_loop) = weak_loop.upgrade() else {
            return;
        };
        let cancelled = event_loop.cancel_timer(heartbeat);
        println!(
            "[{:>3} ms] heartbeat cancelled: {cancelled}, stopping the loop",
            elapsed_ms()
        );
        event_loop.stop();
    });

    println!(
        "pending timers after scheduling: {}",
        event_loop.pending_timers()
    );
    println!("cancel the reminder: {}", event_loop.cancel_timer(reminder));
    println!("cancel it again:     {}", event_loop.cancel_timer(reminder));
    println!("pending timers now:  {}", event_loop.pending_timers());

    event_loop.run()?;

    let beats = heartbeats.load(Ordering::SeqCst);
    println!("loop stopped after {} ms", started.elapsed().as_millis());
    println!("heartbeats: {beats}");
    println!("timer panics caught: {}", event_loop.timer_panics());
    println!("pending timers left: {}", event_loop.pending_timers());

    assert!(beats >= 1, "the repeating timer never fired");
    assert_eq!(event_loop.timer_panics(), 1);
    assert_eq!(event_loop.pending_timers(), 0);
    Ok(())
}
