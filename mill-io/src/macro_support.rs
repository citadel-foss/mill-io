use crate::{EventLoop, EventLoopBuilder};
use std::{
    any::Any,
    fmt::Debug,
    mem,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{mpsc, Arc},
    thread,
    time::Duration,
};

/// The loop running on a `mill-io-reactor` thread; stopping and joining it on
/// drop covers a body that returns early or panics.
struct LoopRunner {
    event_loop: Arc<EventLoop>,
    thread: Option<thread::JoinHandle<Result<(), String>>>,
}

impl LoopRunner {
    fn start(event_loop: &Arc<EventLoop>) -> Self {
        let runner = Arc::clone(event_loop);
        let thread = thread::Builder::new()
            .name("mill-io-reactor".to_string())
            .spawn(move || runner.run().map_err(|e| e.to_string()))
            .unwrap_or_else(|e| panic!("mill-io: cannot spawn the reactor thread: {e}"));
        Self {
            event_loop: Arc::clone(event_loop),
            thread: Some(thread),
        }
    }

    fn stop_and_join(&mut self) -> Result<(), String> {
        self.event_loop.stop();
        match self.thread.take().map(thread::JoinHandle::join) {
            None | Some(Ok(Ok(()))) => Ok(()),
            Some(Ok(Err(e))) => Err(e),
            Some(Err(_)) => Err("the reactor thread panicked".to_string()),
        }
    }

    /// Stops the loop without waiting for it, for a body that never returned.
    fn abandon(mut self) {
        self.event_loop.stop();
        drop(self.thread.take());
        // Keeps the loop alive: dropping it could wait on tasks the hung body is part of.
        mem::forget(self);
    }
}

impl Drop for LoopRunner {
    fn drop(&mut self) {
        let _ = self.stop_and_join();
    }
}

fn build(builder: EventLoopBuilder, attribute: &str) -> Arc<EventLoop> {
    match builder.build() {
        Ok(event_loop) => Arc::new(event_loop),
        Err(e) => panic!("#[mill_io::{attribute}]: cannot build the event loop: {e}"),
    }
}

/// Runs `body` with the loop running on a background thread and entered on
/// this one; stops and joins the loop afterwards, also when `body` panics.
pub fn run_main<R>(builder: EventLoopBuilder, body: impl FnOnce(&Arc<EventLoop>) -> R) -> R {
    run_body(builder, "main", body)
}

fn run_body<R>(
    builder: EventLoopBuilder,
    attribute: &str,
    body: impl FnOnce(&Arc<EventLoop>) -> R,
) -> R {
    let event_loop = build(builder, attribute);
    let mut runner = LoopRunner::start(&event_loop);
    let result = {
        let _context = event_loop.enter();
        body(&event_loop)
    };
    if let Err(e) = runner.stop_and_join() {
        panic!("#[mill_io::{attribute}]: the event loop failed: {e}");
    }
    drop(runner);
    drop(event_loop);
    result
}

/// How `#[mill_io::test]` dispatches handlers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// On the I/O worker pool.
    Pool,
    /// On the reactor thread ([`EventLoopBuilder::low_latency`]).
    Direct,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Mode::Pool => "pool",
            Mode::Direct => "direct",
        }
    }
}

/// What a `#[mill_io::test]` body may return.
pub trait TestResult {
    /// `Err` with the failure to report.
    fn into_test_result(self) -> Result<(), String>;
}

impl TestResult for () {
    fn into_test_result(self) -> Result<(), String> {
        Ok(())
    }
}

impl<E: Debug> TestResult for Result<(), E> {
    fn into_test_result(self) -> Result<(), String> {
        self.map_err(|e| format!("returned Err({e:?})"))
    }
}

fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

/// Runs `body` once per mode on a fresh loop, as [`run_main`] does, and panics
/// with the mode's name on the first failure. With a `timeout`, a body still
/// running then fails the test; its thread and loop are leaked.
pub fn run_test<F>(
    base: fn() -> EventLoopBuilder,
    modes: &[Mode],
    timeout: Option<Duration>,
    body: F,
) where
    F: Fn(&Arc<EventLoop>) -> Result<(), String> + Send + Sync + Copy + 'static,
{
    for &mode in modes {
        let builder = match mode {
            Mode::Pool => base(),
            Mode::Direct => base().low_latency(),
        };
        let outcome = match timeout {
            None => catch_unwind(AssertUnwindSafe(|| run_body(builder, "test", body)))
                .unwrap_or_else(|payload| Err(format!("panicked: {}", panic_message(&*payload)))),
            Some(timeout) => run_with_timeout(builder, body, timeout),
        };
        if let Err(failure) = outcome {
            panic!("#[mill_io::test] mode `{}` {failure}", mode.name());
        }
    }
}

fn run_with_timeout<F>(builder: EventLoopBuilder, body: F, timeout: Duration) -> Result<(), String>
where
    F: Fn(&Arc<EventLoop>) -> Result<(), String> + Send + 'static,
{
    let event_loop = build(builder, "test");
    let mut runner = LoopRunner::start(&event_loop);
    let (done, outcome) = mpsc::channel();
    let body_loop = Arc::clone(&event_loop);
    thread::Builder::new()
        .name("mill-io-test-body".to_string())
        .spawn(move || {
            let result = catch_unwind(AssertUnwindSafe(|| {
                let _context = body_loop.enter();
                body(&body_loop)
            }));
            drop(body_loop);
            let _ = done.send(result);
        })
        .unwrap_or_else(|e| panic!("mill-io: cannot spawn the test body thread: {e}"));
    match outcome.recv_timeout(timeout) {
        Ok(result) => {
            let result = result
                .unwrap_or_else(|payload| Err(format!("panicked: {}", panic_message(&*payload))));
            let stopped = runner.stop_and_join();
            drop(runner);
            drop(event_loop);
            result?;
            stopped.map_err(|e| format!("event loop failed: {e}"))
        }
        Err(_) => {
            runner.abandon();
            Err(format!("timed out after {timeout:?}"))
        }
    }
}
