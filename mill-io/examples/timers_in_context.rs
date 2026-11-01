//! The free functions from inside the loop: `every`, `after`, `cancel_timer` and `post`
//! called from timer callbacks and compute tasks, and `current()` handed to a plain std
//! thread that joins the loop with `enter()`.
//!
//! Run with `cargo run -p mill-io --example timers_in_context`. It exits after about 150 ms.

use mill_io::TimerId;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc, OnceLock,
    },
    thread,
    time::{Duration, Instant},
};

fn checksum(len: u64) -> u64 {
    (0..len).fold(0, |sum, i| sum.wrapping_mul(31).wrapping_add(i))
}

#[mill_io::main]
fn main() {
    let started = Instant::now();
    let ms = move || started.elapsed().as_millis();
    let ticks = Arc::new(AtomicUsize::new(0));
    let heartbeat_id = Arc::new(OnceLock::<TimerId>::new());
    // A timer callback can't be joined, so it reports the end over a channel.
    let (finished, done) = mpsc::channel::<()>();

    let heartbeat = {
        let (ticks, heartbeat_id) = (Arc::clone(&ticks), Arc::clone(&heartbeat_id));
        mill_io::every(Duration::from_millis(20), move || {
            let n = ticks.fetch_add(1, Ordering::SeqCst) + 1;
            println!("[{:>3} ms] heartbeat {n}", ms());
            if n == 2 {
                // Blocking work leaves the reactor thread; its result comes back with post().
                mill_io::spawn(move || {
                    let sum = checksum(5_000_000);
                    mill_io::post(move || {
                        let on_reactor = mill_io::current().is_reactor_thread();
                        println!(
                            "[{:>3} ms] checksum {sum:#x} posted back, on reactor: {on_reactor}",
                            ms()
                        );
                    });
                    mill_io::after(Duration::from_millis(5), move || {
                        println!("[{:>3} ms] timer armed by a compute task fired", ms());
                    });
                });
            }
            if n == 5 {
                if let Some(id) = heartbeat_id.get() {
                    println!(
                        "[{:>3} ms] heartbeat cancels itself: {}",
                        ms(),
                        mill_io::cancel_timer(*id)
                    );
                }
                let finished = finished.clone();
                mill_io::after(Duration::from_millis(10), move || {
                    let _ = finished.send(());
                });
            }
        })
    };
    let _ = heartbeat_id.set(heartbeat);

    let reminder = mill_io::after(Duration::from_millis(500), || {
        unreachable!("cancelled before it was due")
    });
    println!(
        "cancel the 500 ms reminder: {}",
        mill_io::cancel_timer(reminder)
    );

    let handle = mill_io::current();
    thread::spawn(move || {
        let _guard = handle.enter();
        mill_io::after(Duration::from_millis(30), move || {
            println!(
                "[{:>3} ms] timer set from a std thread fired on the reactor: {}",
                ms(),
                mill_io::current().is_reactor_thread()
            );
        });
    })
    .join()
    .expect("the helper thread panicked");

    done.recv().expect("the heartbeat never finished");
    println!(
        "{} heartbeats, {} timers left",
        ticks.load(Ordering::SeqCst),
        mill_io::current().pending_timers().unwrap_or(0)
    );
}
