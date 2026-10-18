#![allow(dead_code)]

use mill_io::EventLoop;
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
