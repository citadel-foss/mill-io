#![allow(dead_code)]

use mill_io::EventLoop;
use std::fmt;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

pub fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
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

pub fn pool_and_direct() -> [(&'static str, Arc<EventLoop>); 2] {
    [
        ("pool", Arc::new(EventLoop::new(2, 1024, 10).unwrap())),
        (
            "direct",
            Arc::new(EventLoop::new_low_latency(1024, 10).unwrap()),
        ),
    ]
}

pub fn spawn_loop(event_loop: &Arc<EventLoop>) -> JoinHandle<()> {
    let event_loop = Arc::clone(event_loop);
    thread::spawn(move || event_loop.run().unwrap())
}

pub fn stop_loop(event_loop: &EventLoop, runner: JoinHandle<()>) {
    event_loop.stop();
    assert!(
        wait_until(Duration::from_secs(5), || runner.is_finished()),
        "event loop did not stop"
    );
    runner.join().unwrap();
}

/// How long a transfer may go without progress before a test calls it stalled.
pub const STALL_AFTER: Duration = Duration::from_secs(20);
/// The most a transfer may take in all.
pub const TRANSFER_CAP: Duration = Duration::from_secs(300);

/// Where a transfer stopped.
#[derive(Debug)]
pub struct Stall {
    pub reached: usize,
    pub target: usize,
    pub elapsed: Duration,
    pub idle: Duration,
}

impl fmt::Display for Stall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} of {} after {:?}, nothing new for {:?}",
            self.reached, self.target, self.elapsed, self.idle
        )
    }
}

/// Waits until `progress()` reaches `target`, failing only on a stall: `STALL_AFTER` with
/// no change, or `TRANSFER_CAP` in all. A fixed deadline also fails a transfer that is just
/// slow, as on a loaded runner, or on Linux 6.17, which can shrink a loopback receive
/// buffer to a few KB.
pub fn wait_for_progress(target: usize, mut progress: impl FnMut() -> usize) -> Result<(), Stall> {
    let started = Instant::now();
    let (mut seen, mut moved) = (0, started);
    loop {
        let reached = progress();
        if reached >= target {
            return Ok(());
        }
        if reached != seen {
            (seen, moved) = (reached, Instant::now());
        }
        if moved.elapsed() >= STALL_AFTER || started.elapsed() >= TRANSFER_CAP {
            return Err(Stall {
                reached,
                target,
                elapsed: started.elapsed(),
                idle: moved.elapsed(),
            });
        }
        thread::sleep(Duration::from_millis(10));
    }
}

/// The kernel's view of the connections on `port` (`ss -tnim`), for a failure message.
#[cfg(target_os = "linux")]
pub fn socket_report(port: u16) -> String {
    let Ok(out) = std::process::Command::new("ss")
        .args(["-tnimH", "state", "established"])
        .output()
    else {
        return "ss unavailable".to_string();
    };
    let text = String::from_utf8_lossy(&out.stdout);
    let mut report = String::new();
    let mut keep = false;
    for line in text.lines() {
        if !line.starts_with(char::is_whitespace) {
            let port = format!(":{port}");
            keep = line
                .split_whitespace()
                .any(|column| column.ends_with(&port));
        }
        if keep {
            report.push_str(line.trim());
            report.push('\n');
        }
    }
    report
}

#[cfg(not(target_os = "linux"))]
pub fn socket_report(_port: u16) -> String {
    String::new()
}
