//! Compute work through the current loop: `spawn` with `JoinHandle::join`, a panicking
//! task surfacing as `JoinError::Panicked`, `join_timeout`, and `join!` over several
//! closures.
//!
//! Run with `cargo run -p mill-io --example spawn_join`. It exits after about 300 ms.

use mill_io::{JoinError, JoinTimeout};
use std::{
    error::Error,
    thread,
    time::{Duration, Instant},
};

fn is_prime(n: u64) -> bool {
    n >= 2 && (2..).take_while(|d| d * d <= n).all(|d| n % d != 0)
}

fn slow_square(n: u64, delay: Duration) -> u64 {
    thread::sleep(delay);
    n * n
}

#[mill_io::main(compute_workers = 4)]
fn main() -> Result<(), Box<dyn Error>> {
    let primes = mill_io::spawn(|| (2..50_000).filter(|&n| is_prime(n)).count());
    println!("primes below 50 000: {}", primes.join()?);

    let failing = mill_io::spawn(|| -> u64 {
        let readings: Vec<u64> = Vec::new();
        readings[3]
    });
    match failing.join() {
        Err(JoinError::Panicked(message)) => println!("the task panicked: {message}"),
        other => return Err(format!("expected a panic, got {other:?}").into()),
    }

    let slow = mill_io::spawn(|| slow_square(12, Duration::from_millis(200)));
    let slow = match slow.join_timeout(Duration::from_millis(20)) {
        Ok(value) => return Err(format!("{value} arrived too early").into()),
        Err(JoinTimeout(handle)) => {
            println!(
                "no value after 20 ms; finished: {}, joining again",
                handle.is_finished()
            );
            handle
        }
    };
    println!("the slow task returned {}", slow.join()?);

    let started = Instant::now();
    let (a, b, c, d) = mill_io::join!(
        || slow_square(3, Duration::from_millis(50)),
        || slow_square(4, Duration::from_millis(50)),
        || is_prime(1_000_000_007),
        || -> u64 { panic!("one closure fails, the others still return") },
    );
    println!(
        "join! gave {:?}, {:?}, {:?} and {:?} in {} ms (the two 50 ms sleeps overlapped)",
        a,
        b,
        c,
        d.map_err(|e| e.to_string()),
        started.elapsed().as_millis()
    );
    assert_eq!((a?, b?, c?), (9, 16, true));
    Ok(())
}
