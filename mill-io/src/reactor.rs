use crate::{
    error::Result,
    poll::{PollHandle, WAKER_TOKEN},
    thread_pool::{ComputePoolMetrics, ComputeThreadPool, TaskPriority, ThreadPool},
    timer_wheel::TimerWheel,
};
use mio::{event::Event, Events};
use std::{
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, PoisonError, RwLock,
    },
    time::Duration,
};

pub const DEFAULT_EVENTS_CAPACITY: usize = 1024;
pub const DEFAULT_POLL_TIMEOUT_MS: u64 = 150;

pub struct ReactorOptions {
    pub direct_dispatch: bool,
}

pub struct Reactor {
    pub(crate) poll_handle: PollHandle,
    events: Arc<RwLock<Events>>,
    pool: ThreadPool,
    compute_pool: ComputeThreadPool,
    running: AtomicBool,
    poll_timeout_ms: u64,
    options: ReactorOptions,
    pub(crate) timer_wheel: Arc<TimerWheel>,
    /// Panics caught on the reactor thread; pool-mode panics are counted by the pool.
    direct_panics: AtomicU64,
}

impl Default for Reactor {
    fn default() -> Self {
        Self {
            poll_handle: PollHandle::new().unwrap(),
            events: Arc::new(RwLock::new(Events::with_capacity(DEFAULT_EVENTS_CAPACITY))),
            pool: ThreadPool::default(),
            compute_pool: ComputeThreadPool::new(
                std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(4),
            ),
            running: AtomicBool::new(false),
            poll_timeout_ms: DEFAULT_POLL_TIMEOUT_MS,
            options: ReactorOptions {
                direct_dispatch: false,
            },
            timer_wheel: Arc::new(TimerWheel::new()),
            direct_panics: AtomicU64::new(0),
        }
    }
}

impl Reactor {
    pub fn new(pool_size: usize, events_capacity: usize, poll_timeout_ms: u64) -> Result<Self> {
        Ok(Self {
            poll_handle: PollHandle::new()?,
            events: Arc::new(RwLock::new(Events::with_capacity(events_capacity))),
            pool: ThreadPool::new(pool_size),
            compute_pool: ComputeThreadPool::default(),
            running: AtomicBool::new(false),
            poll_timeout_ms,
            options: ReactorOptions {
                direct_dispatch: false,
            },
            timer_wheel: Arc::new(TimerWheel::new()),
            direct_panics: AtomicU64::new(0),
        })
    }

    pub fn new_with_options(
        pool_size: usize,
        events_capacity: usize,
        poll_timeout_ms: u64,
        options: ReactorOptions,
    ) -> Result<Self> {
        Ok(Self {
            poll_handle: PollHandle::new()?,
            events: Arc::new(RwLock::new(Events::with_capacity(events_capacity))),
            pool: ThreadPool::new(pool_size),
            compute_pool: ComputeThreadPool::default(),
            running: AtomicBool::new(false),
            poll_timeout_ms,
            options,
            timer_wheel: Arc::new(TimerWheel::new()),
            direct_panics: AtomicU64::new(0),
        })
    }

    pub fn run(&self) -> Result<()> {
        self.running.store(true, Ordering::SeqCst);

        while self.running.load(Ordering::SeqCst) {
            // Shorten the poll timeout if a timer is due soon.
            let poll_timeout = match self.timer_wheel.time_until_next() {
                Some(until_next) => {
                    let configured = Duration::from_millis(self.poll_timeout_ms);
                    until_next.min(configured)
                }
                None => Duration::from_millis(self.poll_timeout_ms),
            };

            // Handler panics are caught, so these locks are never poisoned in practice.
            let _ = self.poll_handle.poll(
                &mut self.events.write().unwrap_or_else(PoisonError::into_inner),
                Some(poll_timeout),
            )?;

            for event in self
                .events
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .iter()
            {
                self.dispatch_event(event.clone())?;
            }

            // Advance the timer wheel and fire expired timers.
            self.timer_wheel.tick();
        }
        Ok(())
    }

    pub fn get_shutdown_handle(&self) -> ShutdownHandle<'_> {
        ShutdownHandle {
            running: &self.running,
            poll_handle: &self.poll_handle,
        }
    }

    pub fn dispatch_event(&self, event: Event) -> Result<()> {
        let token = event.token();
        if token == WAKER_TOKEN {
            return Ok(());
        }
        let is_readable = event.is_readable();
        let is_writable = event.is_writable();

        let registry = self.poll_handle.get_registery();

        if self.options.direct_dispatch {
            // FAST PATH: Direct execution on reactor thread
            if let Some(entry) = registry.get(&token) {
                let interest = entry.1.interest;
                let handler = entry.1.handler.as_ref();
                if (interest.is_readable() && is_readable)
                    || (interest.is_writable() && is_writable)
                {
                    let result = catch_unwind(AssertUnwindSafe(|| handler.handle_event(&event)));
                    if result.is_err() {
                        self.direct_panics.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            Ok(())
        } else {
            // SLOW PATH: Dispatch to thread pool. Routing by token keeps one
            // source's events on one worker, so they never run concurrently or
            // out of poll order.
            self.pool.exec_keyed(token.0, move || {
                let entry = registry.get(&token);
                if let Some(entry) = entry {
                    let interest = entry.1.interest;
                    let handler = entry.1.handler.as_ref();
                    if (interest.is_readable() && is_readable)
                        || (interest.is_writable() && is_writable)
                    {
                        handler.handle_event(&event);
                    }
                }
            })
        }
    }

    pub fn spawn_compute<F>(&self, task: F, priority: TaskPriority)
    where
        F: FnOnce() + Send + 'static,
    {
        self.compute_pool.spawn(task, priority);
    }

    /// Handler panics caught since creation, in both pool and direct mode.
    pub fn handler_panics(&self) -> u64 {
        self.direct_panics.load(Ordering::Relaxed) + self.pool.panics()
    }

    pub fn get_compute_metrics(&self) -> Arc<ComputePoolMetrics> {
        self.compute_pool.metrics()
    }

    pub fn get_events(&self) -> Arc<RwLock<Events>> {
        self.events.clone()
    }
}

pub struct ShutdownHandle<'a> {
    running: &'a AtomicBool,
    poll_handle: &'a PollHandle,
}

impl ShutdownHandle<'_> {
    pub fn shutdown(&self) {
        self.running.store(false, Ordering::SeqCst);
        self.poll_handle.wake().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::*;
    use mio::{Interest, Token};
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::Duration;

    #[derive(Clone)]
    struct TestHandler {
        counter: Arc<Mutex<usize>>,
        condition: Arc<Condvar>,
    }

    impl EventHandler for TestHandler {
        fn handle_event(&self, _event: &Event) {
            let mut count = self.counter.lock().unwrap();
            *count += 1;
            self.condition.notify_one();
        }
    }

    #[cfg(unix)]
    #[test]
    fn test_reactor_start_stop() {
        let reactor = Arc::new(Reactor::default());
        let shutdown_handle = reactor.get_shutdown_handle();

        let reactor_clone = Arc::clone(&reactor);
        let handle = std::thread::spawn(move || {
            reactor_clone.run().unwrap();
        });

        std::thread::sleep(Duration::from_millis(100));

        shutdown_handle.shutdown();

        handle.join().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn test_with_pipe() -> std::io::Result<()> {
        use mio::net::UnixStream;

        let reactor =
            Arc::new(Reactor::new(2, DEFAULT_EVENTS_CAPACITY, DEFAULT_POLL_TIMEOUT_MS).unwrap());
        let counter = Arc::new(Mutex::new(0));
        let condition = Arc::new(Condvar::new());

        let (mut stream1, mut stream2) = UnixStream::pair()?;

        let handler = TestHandler {
            counter: Arc::clone(&counter),
            condition: Arc::clone(&condition),
        };

        let token = Token(1);

        reactor
            .poll_handle
            .register(&mut stream1, token, Interest::READABLE, handler)
            .unwrap();

        let reactor_clone = Arc::clone(&reactor);
        let handle = std::thread::spawn(move || {
            // Poll once
            let events_result = {
                let mut events = reactor_clone.events.write().unwrap();
                reactor_clone
                    .poll_handle
                    .poll(&mut events, Some(Duration::from_millis(100)))
            };

            if events_result.is_ok() {
                let events = reactor_clone.events.read().unwrap();
                for event in events.iter() {
                    let _ = reactor_clone.dispatch_event(event.clone());
                }
            }
        });

        std::io::Write::write_all(&mut stream2, b"test data")?;

        handle.join().unwrap();

        let count = counter.lock().unwrap();
        let result = condition
            .wait_timeout(count, Duration::from_millis(500))
            .unwrap();

        if !result.1.timed_out() {
            assert_eq!(*result.0, 1);
        }

        Ok(())
    }

    #[test]
    fn test_with_tcp() -> std::io::Result<()> {
        use mio::net::{TcpListener, TcpStream};
        use std::net::SocketAddr;

        let reactor =
            Arc::new(Reactor::new(2, DEFAULT_EVENTS_CAPACITY, DEFAULT_POLL_TIMEOUT_MS).unwrap());
        let counter = Arc::new(Mutex::new(0));
        let condition = Arc::new(Condvar::new());

        // Create a TCP listener on localhost
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let mut listener = TcpListener::bind(addr)?;
        let listener_addr = listener.local_addr()?;

        let handler = TestHandler {
            counter: Arc::clone(&counter),
            condition: Arc::clone(&condition),
        };

        let token = Token(1);

        reactor
            .poll_handle
            .register(&mut listener, token, Interest::READABLE, handler)
            .unwrap();

        let reactor_clone = Arc::clone(&reactor);
        let handle = std::thread::spawn(move || {
            // Poll once
            let events_result = {
                let mut events = reactor_clone.events.write().unwrap();
                reactor_clone
                    .poll_handle
                    .poll(&mut events, Some(Duration::from_millis(100)))
            };

            if events_result.is_ok() {
                let events = reactor_clone.events.read().unwrap();
                for event in events.iter() {
                    let _ = reactor_clone.dispatch_event(event.clone());
                }
            }
        });

        // Connect to trigger the event
        let _stream = TcpStream::connect(listener_addr)?;

        handle.join().unwrap();

        let count = counter.lock().unwrap();
        let result = condition
            .wait_timeout(count, Duration::from_millis(500))
            .unwrap();

        if !result.1.timed_out() {
            assert_eq!(*result.0, 1);
        }

        Ok(())
    }
}
