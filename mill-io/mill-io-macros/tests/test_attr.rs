use mill_io::{EventHandler, EventLoop};
use mio::{event::Event, net::UdpSocket, Interest};
use std::{
    cell::RefCell,
    panic::catch_unwind,
    sync::{mpsc, Arc},
    thread,
    time::{Duration, Instant},
};

struct OnReactor(mpsc::Sender<bool>);

impl EventHandler for OnReactor {
    fn handle_event(&self, _: &Event) {
        let _ = self.0.send(mill_io::current().is_reactor_thread());
    }
}

/// True when the current loop dispatches handlers on its reactor thread.
fn handlers_run_on_reactor() -> bool {
    let handle = mill_io::current();
    let (tx, rx) = mpsc::channel();
    let mut socket = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let token = handle.alloc_token().unwrap();
    handle
        .register(&mut socket, token, Interest::READABLE, OnReactor(tx))
        .unwrap()
        .unwrap();
    let peer = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    peer.send_to(b"x", socket.local_addr().unwrap()).unwrap();
    let on_reactor = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    handle.deregister(&mut socket, token).unwrap().unwrap();
    on_reactor
}

thread_local! {
    static MODES_SEEN: RefCell<Vec<bool>> = const { RefCell::new(Vec::new()) };
}

#[mill_io::test(modes = "both")]
fn records_its_mode() {
    let on_reactor = handlers_run_on_reactor();
    MODES_SEEN.with(|seen| seen.borrow_mut().push(on_reactor));
}

#[test]
fn both_runs_the_body_once_per_mode() {
    MODES_SEEN.with(|seen| seen.borrow_mut().clear());
    records_its_mode();
    assert_eq!(MODES_SEEN.with(|seen| seen.borrow().clone()), [false, true]);
}

#[mill_io::test]
fn default_mode_is_pool() {
    assert!(!handlers_run_on_reactor());
}

#[mill_io::test(modes = "direct")]
fn direct_mode_runs_handlers_on_the_reactor() {
    assert!(handlers_run_on_reactor());
}

#[mill_io::test(low_latency)]
fn low_latency_is_direct_mode() {
    assert!(handlers_run_on_reactor());
}

#[mill_io::test(modes = "both")]
#[should_panic(expected = "mode `direct` panicked: fails only in direct mode")]
fn failure_in_direct_mode_names_it() {
    if handlers_run_on_reactor() {
        panic!("fails only in direct mode");
    }
}

#[mill_io::test(modes = "both")]
#[should_panic(expected = "mode `pool` panicked: fails only in pool mode")]
fn failure_in_pool_mode_names_it() {
    if !handlers_run_on_reactor() {
        panic!("fails only in pool mode");
    }
}

#[mill_io::test(modes = "both")]
#[should_panic(expected = "mode `pool` returned Err(\"refused\")")]
fn err_return_fails_and_names_the_mode() -> Result<(), String> {
    Err("refused".to_string())
}

#[mill_io::test(modes = "both", timeout = "5s")]
fn ok_return_passes() -> Result<(), String> {
    let value = mill_io::spawn(|| 2).join().map_err(|e| e.to_string())?;
    assert_eq!(value, 2);
    Ok(())
}

#[mill_io::test(modes = "both")]
fn body_can_take_the_loop(event_loop: &EventLoop) {
    assert_eq!(event_loop.handle(), mill_io::current());
}

#[mill_io::test(modes = "both", timeout = "5s")]
fn body_with_a_timeout_takes_the_loop(event_loop: &Arc<EventLoop>) {
    assert_eq!(event_loop.handle(), mill_io::current());
    assert_eq!(
        thread::current().name(),
        Some("mill-io-test-body"),
        "a timed body runs on its own thread"
    );
}

#[mill_io::test(compute_workers = 1, timeout = "5s")]
fn options_reach_the_loop() {
    let (first, second) = mill_io::join!(|| thread::current().name().map(str::to_string), || {
        thread::current().name().map(str::to_string)
    });
    assert_eq!(first.unwrap().as_deref(), Some("compute-worker-0"));
    assert_eq!(second.unwrap().as_deref(), Some("compute-worker-0"));
}

#[mill_io::test(timeout = "1s")]
#[should_panic(expected = "mode `pool` timed out after 1s")]
fn hung_body_times_out() {
    loop {
        thread::park();
    }
}

#[test]
fn timeout_fails_within_a_second_of_the_deadline() {
    let started = Instant::now();
    assert!(catch_unwind(hung_body_times_out).is_err());
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_secs(1) && elapsed < Duration::from_secs(2),
        "{elapsed:?}"
    );
}
