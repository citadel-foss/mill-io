//! One client shared by many workers: every handle clone shares one connection pool, and
//! each sets its own deadline, retries and cancel flag. A handle works from any thread, std
//! or a pool; here the workers are tasks on the program's own mill-io compute pool.
//!
//! ```text
//! cargo run -p mill-chain --example shared_handle [workers]
//! ```
//!
//! `workers` patient tasks (by default, two fewer than the pool has threads, so all of
//! them and the two below run at once) each read the headers of the last 20 blocks. One
//! more task uses a deadline too short to succeed, and another is cancelled by a timer
//! while it works. The client's counters are printed at the end. The node is set by
//! `BITCOIN_RPC_URL` and `BITCOIN_RPC_AUTH` or `BITCOIN_RPC_COOKIE`.

mod support;

use mill_chain::{Chain, ChainConfig, ChainError, ChainHandle};
use mill_io::EventLoop;
use std::error::Error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

type Report = (String, Result<usize, ChainError>);

fn read_recent_headers(chain: &ChainHandle) -> Result<usize, ChainError> {
    let tip = chain.tip()?.height;
    let mut read = 0;
    for height in tip.saturating_sub(19)..=tip {
        chain.header(height)?;
        read += 1;
    }
    Ok(read)
}

fn main() -> Result<(), Box<dyn Error>> {
    let pool_threads = thread::available_parallelism().map_or(4, |n| n.get());
    let default_workers = pool_threads.saturating_sub(2).max(1);
    let workers: usize = std::env::args()
        .nth(1)
        .map_or(Ok(default_workers), |n| n.parse())?;
    if workers + 2 > pool_threads {
        println!(
            "{} tasks on a pool of {} threads: at most {} run at once",
            workers + 2,
            pool_threads,
            pool_threads
        );
    }

    let client = Chain::spawn(ChainConfig::Core(support::core_config(None)))?;
    let app = Arc::new(EventLoop::new_low_latency(64, 100)?);
    let running = {
        let app = Arc::clone(&app);
        thread::spawn(move || app.run().map_err(|e| e.to_string()))
    };
    let (report, reports) = mpsc::channel::<Report>();
    let started = Instant::now();

    let patient = client.handle().deadline(Duration::from_secs(10)).retries(2);
    for i in 0..workers {
        let (chain, report) = (patient.clone(), report.clone());
        app.spawn_compute(move || {
            let _ = report.send((format!("worker {}", i), read_recent_headers(&chain)));
        });
    }

    let hasty = client.handle().deadline(Duration::from_micros(50));
    let hasty_report = report.clone();
    app.spawn_compute(move || {
        let _ = hasty_report.send(("50 µs deadline".into(), read_recent_headers(&hasty)));
    });

    let stop = Arc::new(AtomicBool::new(false));
    let stoppable = client
        .handle()
        .deadline(Duration::from_secs(10))
        .cancel_on(Arc::clone(&stop));
    app.spawn_compute(move || {
        let mut read = 0;
        let result = loop {
            match read_recent_headers(&stoppable) {
                Ok(n) if read < 1_000_000 => read += n,
                other => break other,
            }
        };
        let _ = report.send((format!("cancelled after {} headers", read), result));
    });
    app.schedule_once(Duration::from_millis(200), move || {
        stop.store(true, Ordering::SeqCst)
    });

    for (who, result) in reports.iter().take(workers + 2) {
        println!("{}: {:?}", who, result);
    }
    println!("{} tasks in {:?}", workers + 2, started.elapsed());
    app.stop();
    running.join().map_err(|_| "event loop panicked")??;
    println!("{:?}", client.stats());
    client.shutdown()?;
    Ok(())
}
