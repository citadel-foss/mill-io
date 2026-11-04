use mill_io::{EventHandler, EventLoop, Handle, LoopGone};
use mio::{event::Event, net::UdpSocket, Interest};
use std::{
    collections::HashSet,
    panic::catch_unwind,
    process::{Command, Stdio},
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc, Mutex,
    },
    thread,
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

struct Probe(&'static AtomicUsize);

impl Drop for Probe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct ProbedHandler(#[allow(dead_code)] Probe);

impl EventHandler for ProbedHandler {
    fn handle_event(&self, _: &Event) {}
}

/// Leaves three probes in the current loop: a timer, a handler and a posted closure.
fn arm_probes(dropped: &'static AtomicUsize) -> Handle {
    let handle = mill_io::current();
    let probe = Probe(dropped);
    mill_io::after(Duration::from_secs(3600), move || drop(probe));
    let mut socket = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    handle
        .register(
            &mut socket,
            handle.alloc_token().unwrap(),
            Interest::READABLE,
            ProbedHandler(Probe(dropped)),
        )
        .unwrap()
        .unwrap();
    let probe = Probe(dropped);
    mill_io::every(Duration::from_secs(3600), move || {
        let _ = &probe;
    });
    handle
}

fn reactor_thread_name() -> Option<String> {
    let (tx, rx) = mpsc::channel();
    mill_io::post(move || {
        let _ = tx.send(thread::current().name().map(str::to_string));
    });
    rx.recv_timeout(Duration::from_secs(5)).unwrap()
}

#[mill_io::main]
fn body_with_context() -> (Option<Handle>, Option<String>) {
    (mill_io::try_current(), reactor_thread_name())
}

#[test]
fn main_body_sees_a_context_and_a_running_loop() {
    assert_eq!(mill_io::try_current(), None);
    let (handle, reactor) = body_with_context();
    assert_eq!(mill_io::try_current(), None);
    assert_eq!(reactor.as_deref(), Some("mill-io-reactor"));
    assert_eq!(handle.unwrap().alloc_token(), Err(LoopGone));
}

static UNIT_RAN: AtomicUsize = AtomicUsize::new(0);

#[mill_io::main]
fn unit_body() {
    let answer = mill_io::spawn(|| 6 * 7).join();
    assert_eq!(answer, Ok(42));
    UNIT_RAN.fetch_add(1, Ordering::SeqCst);
}

#[test]
fn main_with_unit_return() {
    unit_body();
    assert_eq!(UNIT_RAN.load(Ordering::SeqCst), 1);
}

static OK_PROBES: AtomicUsize = AtomicUsize::new(0);

#[mill_io::main]
fn returns_ok() -> Result<Handle, String> {
    Ok(arm_probes(&OK_PROBES))
}

#[test]
fn main_stops_and_joins_the_loop_on_ok() {
    let handle = returns_ok().unwrap();
    assert_eq!(OK_PROBES.load(Ordering::SeqCst), 3);
    assert_eq!(handle.spawn_compute(|| {}), Err(LoopGone));
}

static ERR_PROBES: AtomicUsize = AtomicUsize::new(0);

#[mill_io::main]
fn returns_err() -> Result<(), Handle> {
    Err(arm_probes(&ERR_PROBES))
}

#[test]
fn main_stops_and_joins_the_loop_on_err() {
    let handle = returns_err().unwrap_err();
    assert_eq!(ERR_PROBES.load(Ordering::SeqCst), 3);
    assert_eq!(handle.spawn_compute(|| {}), Err(LoopGone));
}

static PANIC_PROBES: AtomicUsize = AtomicUsize::new(0);
static PANIC_HANDLE: Mutex<Option<Handle>> = Mutex::new(None);

#[mill_io::main]
fn panics() {
    *PANIC_HANDLE.lock().unwrap() = Some(arm_probes(&PANIC_PROBES));
    panic!("body panicked");
}

#[test]
fn main_stops_and_joins_the_loop_on_panic() {
    let payload = catch_unwind(panics).unwrap_err();
    assert_eq!(payload.downcast_ref::<&str>(), Some(&"body panicked"));
    assert_eq!(PANIC_PROBES.load(Ordering::SeqCst), 3);
    let handle = PANIC_HANDLE.lock().unwrap().take().unwrap();
    assert_eq!(handle.spawn_compute(|| {}), Err(LoopGone));
    assert_eq!(mill_io::try_current(), None);
}

const CHILD_ENV: &str = "MILL_IO_MACROS_PANIC_CHILD";

struct LoudProbe;

impl Drop for LoudProbe {
    fn drop(&mut self) {
        println!("loop resources dropped");
    }
}

#[mill_io::main(compute_workers = 1)]
fn child_main() {
    let probe = LoudProbe;
    mill_io::after(Duration::from_secs(3600), move || drop(probe));
    panic!("child body panicked");
}

#[test]
fn main_panic_child_process() {
    if std::env::var_os(CHILD_ENV).is_some() {
        child_main();
    }
}

#[test]
fn main_panic_ends_the_process_after_stopping_the_loop() {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "main_panic_child_process",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD_ENV, "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    assert!(
        wait_until(Duration::from_secs(30), || child
            .try_wait()
            .unwrap()
            .is_some()),
        "the child process did not exit"
    );
    let output = child.wait_with_output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "{stdout}\n{stderr}");
    assert!(
        stdout.contains("loop resources dropped"),
        "{stdout}\n{stderr}"
    );
    assert!(
        format!("{stdout}{stderr}").contains("child body panicked"),
        "{stdout}\n{stderr}"
    );
}

#[mill_io::main]
fn takes_a_ref_to_the_arc(event_loop: &Arc<EventLoop>) -> bool {
    event_loop.handle() == mill_io::current()
}

#[mill_io::main]
fn takes_a_ref_to_the_loop(event_loop: &EventLoop) -> bool {
    event_loop.handle() == mill_io::current()
}

#[mill_io::main]
fn takes_the_arc(event_loop: Arc<EventLoop>) -> bool {
    event_loop.handle() == mill_io::current()
}

#[test]
fn main_passes_the_loop_to_a_parameter() {
    assert!(takes_a_ref_to_the_arc());
    assert!(takes_a_ref_to_the_loop());
    assert!(takes_the_arc());
}

fn peak_compute_concurrency(tasks: usize) -> usize {
    let arrived = Arc::new(AtomicUsize::new(0));
    let running = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let handles: Vec<_> = (0..tasks)
        .map(|_| {
            let (arrived, running, peak) = (
                Arc::clone(&arrived),
                Arc::clone(&running),
                Arc::clone(&peak),
            );
            mill_io::spawn(move || {
                let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                arrived.fetch_add(1, Ordering::SeqCst);
                wait_until(Duration::from_millis(300), || {
                    arrived.load(Ordering::SeqCst) == tasks
                });
                running.fetch_sub(1, Ordering::SeqCst);
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }
    peak.load(Ordering::SeqCst)
}

#[mill_io::main(compute_workers = 2)]
fn two_compute_workers() -> usize {
    peak_compute_concurrency(3)
}

struct ThreadName(mpsc::Sender<(Option<String>, bool)>);

impl EventHandler for ThreadName {
    fn handle_event(&self, _: &Event) {
        let name = thread::current().name().map(str::to_string);
        let _ = self.0.send((name, mill_io::current().is_reactor_thread()));
    }
}

fn handler_threads(sockets: usize) -> Vec<(Option<String>, bool)> {
    let handle = mill_io::current();
    let (tx, rx) = mpsc::channel();
    let peer = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let mut kept = Vec::new();
    for _ in 0..sockets {
        let mut socket = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        handle
            .register(
                &mut socket,
                handle.alloc_token().unwrap(),
                Interest::READABLE,
                ThreadName(tx.clone()),
            )
            .unwrap()
            .unwrap();
        peer.send_to(b"x", socket.local_addr().unwrap()).unwrap();
        kept.push(socket);
    }
    (0..sockets)
        .map(|_| rx.recv_timeout(Duration::from_secs(5)).unwrap())
        .collect()
}

#[mill_io::main(workers = 3)]
fn three_io_workers() -> usize {
    let seen = handler_threads(6);
    assert!(seen.iter().all(|(_, on_reactor)| !on_reactor));
    seen.into_iter()
        .map(|(name, _)| name)
        .collect::<HashSet<_>>()
        .len()
}

#[mill_io::main(low_latency)]
fn low_latency_handlers() -> Vec<(Option<String>, bool)> {
    handler_threads(2)
}

struct Record {
    log: Arc<Mutex<Vec<&'static str>>>,
}

impl EventHandler for Record {
    fn handle_event(&self, _: &Event) {
        let mut log = self.log.lock().unwrap();
        if log.is_empty() {
            let posted = Arc::clone(&self.log);
            mill_io::post(move || posted.lock().unwrap().push("posted"));
        }
        log.push("handler");
    }
}

/// Two sources become ready in the same poll. Closures posted by a handler run
/// after that poll's events, so they come between the two handlers only when
/// the loop takes one event per poll.
fn posted_between_handlers() -> Vec<&'static str> {
    let handle = mill_io::current();
    let log = Arc::new(Mutex::new(Vec::new()));
    let peer = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let mut sockets = Vec::new();
    for _ in 0..2 {
        let socket = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        peer.send_to(b"x", socket.local_addr().unwrap()).unwrap();
        sockets.push(socket);
    }
    let (paused_tx, paused) = mpsc::channel();
    let (resume, resumed) = mpsc::channel::<()>();
    mill_io::post(move || {
        paused_tx.send(()).unwrap();
        let _ = resumed.recv();
    });
    paused.recv_timeout(Duration::from_secs(5)).unwrap();
    for socket in &mut sockets {
        handle
            .register(
                socket,
                handle.alloc_token().unwrap(),
                Interest::READABLE,
                Record {
                    log: Arc::clone(&log),
                },
            )
            .unwrap()
            .unwrap();
    }
    resume.send(()).unwrap();
    assert!(wait_until(Duration::from_secs(5), || {
        log.lock().unwrap().len() == 3
    }));
    let order = log.lock().unwrap().clone();
    order
}

#[mill_io::main(low_latency, events = 1)]
fn one_event_per_poll() -> Vec<&'static str> {
    posted_between_handlers()
}

#[mill_io::main(low_latency, events = 64)]
fn many_events_per_poll() -> Vec<&'static str> {
    posted_between_handlers()
}

#[mill_io::main(poll_timeout_ms = 5, workers = 1, compute_workers = 1, events = 8)]
fn every_option_at_once() -> usize {
    peak_compute_concurrency(2)
}

#[test]
fn main_applies_every_option() {
    assert_eq!(two_compute_workers(), 2);
    assert_eq!(three_io_workers(), 3);
    let direct = low_latency_handlers();
    assert!(
        direct
            .iter()
            .all(|(name, on_reactor)| *on_reactor && name.as_deref() == Some("mill-io-reactor")),
        "{direct:?}"
    );
    assert_eq!(one_event_per_poll(), ["handler", "posted", "handler"]);
    assert_eq!(many_events_per_poll(), ["handler", "handler", "posted"]);
    assert_eq!(every_option_at_once(), 1);
}

mod app {
    #[mill_io::main]
    pub fn main() -> std::thread::ThreadId {
        assert!(mill_io::try_current().is_some());
        std::thread::current().id()
    }
}

#[test]
fn main_inside_a_module_runs_on_the_calling_thread() {
    assert_eq!(app::main(), thread::current().id());
    assert_eq!(app::main(), thread::current().id());
}
