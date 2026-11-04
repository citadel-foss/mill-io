use mill_io::{EventHandler, EventLoop};
use mio::{event::Event, net::UdpSocket, Interest};
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, Weak,
    },
    thread::{self, JoinHandle},
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

fn spawn_loop(event_loop: &Arc<EventLoop>) -> JoinHandle<Result<(), String>> {
    let event_loop = Arc::clone(event_loop);
    thread::spawn(move || event_loop.run().map_err(|e| e.to_string()))
}

fn stop_loop(event_loop: &EventLoop, runner: JoinHandle<Result<(), String>>) {
    event_loop.stop();
    runner.join().unwrap().unwrap();
}

fn peak_concurrency(event_loop: &EventLoop, tasks: usize) -> usize {
    let arrived = Arc::new(AtomicUsize::new(0));
    let running = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let finished = Arc::new(AtomicUsize::new(0));
    for _ in 0..tasks {
        let (arrived, running, peak, finished) = (
            Arc::clone(&arrived),
            Arc::clone(&running),
            Arc::clone(&peak),
            Arc::clone(&finished),
        );
        event_loop.spawn_compute(move || {
            let now = running.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(now, Ordering::SeqCst);
            arrived.fetch_add(1, Ordering::SeqCst);
            // Every task waits for all of them, so only as many overlap as there are workers.
            wait_until(Duration::from_millis(300), || {
                arrived.load(Ordering::SeqCst) == tasks
            });
            running.fetch_sub(1, Ordering::SeqCst);
            finished.fetch_add(1, Ordering::SeqCst);
        });
    }
    assert!(wait_until(Duration::from_secs(10), || {
        finished.load(Ordering::SeqCst) == tasks
    }));
    peak.load(Ordering::SeqCst)
}

#[test]
fn builder_compute_workers_sets_pool_size() {
    for workers in [1, 2, 3] {
        let event_loop = EventLoop::builder()
            .compute_workers(workers)
            .build()
            .unwrap();
        assert_eq!(peak_concurrency(&event_loop, workers + 1), workers);
    }
}

#[test]
fn builder_rejects_zero_sizes() {
    let zero_io = EventLoop::builder().io_workers(0).build();
    let zero_compute = EventLoop::builder().compute_workers(0).build();
    let zero_events = EventLoop::builder().events_capacity(0).build();
    for (what, result) in [
        ("io_workers", zero_io),
        ("compute_workers", zero_compute),
        ("events_capacity", zero_events),
    ] {
        let err = result.err().expect("a zero size must be rejected");
        assert!(err.to_string().contains(what), "{what}: {err}");
    }
}

#[test]
fn existing_constructors_still_accept_their_old_arguments() {
    let pool = EventLoop::new(2, 64, 10).unwrap();
    let direct = EventLoop::new_low_latency(64, 10).unwrap();
    let default = EventLoop::default();
    for event_loop in [pool, direct, default] {
        assert!(peak_concurrency(&event_loop, 1) >= 1);
    }
}

struct Recorder {
    on_reactor: Arc<Mutex<Vec<bool>>>,
    threads: Arc<Mutex<HashSet<String>>>,
    event_loop: Weak<EventLoop>,
}

impl EventHandler for Recorder {
    fn handle_event(&self, _: &Event) {
        self.on_reactor
            .lock()
            .unwrap()
            .push(self.event_loop.upgrade().unwrap().is_reactor_thread());
        let name = thread::current().name().unwrap_or("").to_string();
        self.threads.lock().unwrap().insert(name);
    }
}

fn fire_sockets(event_loop: &Arc<EventLoop>, count: usize) -> (Vec<bool>, HashSet<String>) {
    let on_reactor = Arc::new(Mutex::new(Vec::new()));
    let threads = Arc::new(Mutex::new(HashSet::new()));
    let mut sockets = Vec::new();
    for _ in 0..count {
        let mut socket = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let handler = Recorder {
            on_reactor: Arc::clone(&on_reactor),
            threads: Arc::clone(&threads),
            event_loop: Arc::downgrade(event_loop),
        };
        event_loop
            .register(
                &mut socket,
                event_loop.alloc_token(),
                Interest::READABLE,
                handler,
            )
            .unwrap();
        sockets.push(socket);
    }
    let runner = spawn_loop(event_loop);
    let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    for socket in &sockets {
        sender.send_to(b"x", socket.local_addr().unwrap()).unwrap();
    }
    assert!(wait_until(Duration::from_secs(5), || {
        on_reactor.lock().unwrap().len() >= count
    }));
    stop_loop(event_loop, runner);
    let on_reactor = on_reactor.lock().unwrap().clone();
    let threads = threads.lock().unwrap().clone();
    (on_reactor, threads)
}

#[test]
fn builder_io_workers_sets_handler_pool_size() {
    let event_loop = Arc::new(EventLoop::builder().io_workers(3).build().unwrap());
    let (on_reactor, threads) = fire_sockets(&event_loop, 6);
    assert!(on_reactor.iter().all(|on| !on));
    assert_eq!(threads.len(), 3, "{threads:?}");
}

#[test]
fn builder_low_latency_runs_handlers_on_reactor_thread() {
    let event_loop = Arc::new(EventLoop::builder().low_latency().build().unwrap());
    let (on_reactor, _) = fire_sockets(&event_loop, 2);
    assert!(on_reactor.iter().all(|on| *on));
}
