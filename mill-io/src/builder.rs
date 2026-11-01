use crate::{
    error::Result,
    reactor::{self, Reactor, ReactorConfig},
    thread_pool::DEFAULT_POOL_CAPACITY,
    EventLoop,
};
use std::{io, time::Duration};

/// Configures an [`EventLoop`]. Created by [`EventLoop::builder`].
///
/// Unset sizes default to the number of CPUs (4 if that cannot be read), except
/// the I/O pool of a [`low_latency`](Self::low_latency) loop, which defaults to
/// one worker. The events capacity defaults to
/// [`reactor::DEFAULT_EVENTS_CAPACITY`] and the poll timeout to
/// [`reactor::DEFAULT_POLL_TIMEOUT_MS`].
///
/// ```rust
/// use mill_io::EventLoop;
/// use std::time::Duration;
///
/// let event_loop = EventLoop::builder()
///     .io_workers(2)
///     .compute_workers(4)
///     .events_capacity(256)
///     .poll_timeout(Duration::from_millis(20))
///     .build()?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone)]
pub struct EventLoopBuilder {
    io_workers: Option<usize>,
    compute_workers: Option<usize>,
    events_capacity: usize,
    poll_timeout: Duration,
    low_latency: bool,
}

impl Default for EventLoopBuilder {
    fn default() -> Self {
        Self {
            io_workers: None,
            compute_workers: None,
            events_capacity: reactor::DEFAULT_EVENTS_CAPACITY,
            poll_timeout: Duration::from_millis(reactor::DEFAULT_POLL_TIMEOUT_MS),
            low_latency: false,
        }
    }
}

impl EventLoopBuilder {
    /// Number of threads that run event handlers in pool mode.
    pub fn io_workers(mut self, n: usize) -> Self {
        self.io_workers = Some(n);
        self
    }

    /// Number of threads in the compute pool used by
    /// [`spawn_compute`](EventLoop::spawn_compute).
    pub fn compute_workers(mut self, n: usize) -> Self {
        self.compute_workers = Some(n);
        self
    }

    /// Maximum number of events taken from the OS per poll.
    pub fn events_capacity(mut self, n: usize) -> Self {
        self.events_capacity = n;
        self
    }

    /// Longest time one poll waits when no timer is due sooner.
    pub fn poll_timeout(mut self, d: Duration) -> Self {
        self.poll_timeout = d;
        self
    }

    /// Runs handlers directly on the reactor thread, as
    /// [`EventLoop::new_low_latency`] does. Slow handlers then block all I/O.
    pub fn low_latency(mut self) -> Self {
        self.low_latency = true;
        self
    }

    /// Builds the loop.
    ///
    /// ## Errors
    ///
    /// If a worker count or the events capacity is zero, or the OS poller
    /// cannot be created.
    pub fn build(self) -> Result<EventLoop> {
        let config = self.config();
        for (value, what) in [
            (config.io_workers, "io_workers"),
            (config.compute_workers, "compute_workers"),
            (config.events_capacity, "events_capacity"),
        ] {
            if value == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("mill-io: {what} must be at least 1"),
                )
                .into());
            }
        }
        Self::finish(&config)
    }

    /// Builds without checking the sizes, as the pre-builder constructors did.
    pub(crate) fn build_unchecked(self) -> Result<EventLoop> {
        Self::finish(&self.config())
    }

    fn finish(config: &ReactorConfig) -> Result<EventLoop> {
        Ok(EventLoop {
            reactor: Reactor::with_config(config)?,
        })
    }

    fn config(&self) -> ReactorConfig {
        let cpus = || {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(DEFAULT_POOL_CAPACITY)
        };
        let default_io = if self.low_latency { 1 } else { cpus() };
        ReactorConfig {
            io_workers: self.io_workers.unwrap_or(default_io),
            compute_workers: self.compute_workers.unwrap_or_else(cpus),
            events_capacity: self.events_capacity,
            poll_timeout: self.poll_timeout,
            direct_dispatch: self.low_latency,
        }
    }
}
