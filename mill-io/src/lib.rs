//! # Mill-IO
//! A lightweight, production-ready event loop library for Rust that provides efficient non-blocking I/O management
//! without relying on heavyweight async runtimes like Tokio.
//! Mill-IO is a modular, reactor-based event loop built on top of [`mio`], offering cross-platform polling,
//! configurable thread pool integration, and object pooling for high-performance applications that need
//! fine-grained control over their I/O operations.
//! ## Core Philosophy
//! Mill-IO was designed for applications that require:
//! - **Predictable performance** with minimal runtime overhead
//! - **Runtime-agnostic architecture** that doesn't force async/await patterns
//! - **Direct control** over concurrency and resource management
//! - **Minimal dependencies** for reduced attack surface and faster builds
//! ## Features
//! - **Runtime-agnostic**: No dependency on Tokio or other async runtimes
//! - **Cross-platform**: Leverages mio's polling abstraction (epoll, kqueue, IOCP)
//! - **Thread pool integration**: Configurable worker threads for handling I/O events
//! - **Object pooling**: Reduces allocation overhead for frequent operations
//! - **Clean API**: Simple registration and handler interface
//! - **Thread-safe**: Lock-free operations in hot paths
//! ## Architecture Overview
//! ```text
//! +-------------+    +--------------+    +-------------+
//! | EventLoop   |----|   Reactor    |----| PollHandle  |
//! +-------------+    +--------------+    +-------------+
//!                             |
//!                             |
//!                    +--------------+    +-------------+
//!                    | ThreadPool   |----|   Workers   |
//!                    +--------------+    +-------------+
//! ```
//! ## Quick Start
//!
//! ```rust,no_run
//! use mill_io::{EventLoop, EventHandler};
//! use mio::{net::TcpListener, Interest, Token, event::Event};
//! use std::net::SocketAddr;
//!
//! struct EchoHandler;
//!
//! impl EventHandler for EchoHandler {
//!     fn handle_event(&self, event: &Event) {
//!         println!("Received event: {:?}", event);
//!         // Handle incoming connections and data
//!     }
//! }
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create event loop with default configuration
//!     let event_loop = EventLoop::default();
//!     
//!     // Bind to localhost
//!     let addr: SocketAddr = "127.0.0.1:8080".parse()?;
//!     let mut listener = TcpListener::bind(addr)?;
//!
//!     // Register the listener with a handler
//!     event_loop.register(
//!         &mut listener,
//!         Token(1),
//!         Interest::READABLE,
//!         EchoHandler
//!     )?;
//!
//!     println!("Server listening on 127.0.0.1:8080");
//!     
//!     // Start the event loop (blocks until stopped)
//!     event_loop.run()?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ```rust,no_run
//! use mill_io::EventLoop;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let event_loop = EventLoop::new(
//!         8,      // 8 worker threads
//!         1024,   // Handle up to 1024 events per poll
//!         100     // 100ms poll timeout
//!     )?;
//!     Ok(())
//! }
//! ```
//!
//! - [`EventLoop`]: Main entry point for registering I/O sources and running the event loop
//! - [`EventHandler`]: Trait for implementing custom event handling logic
//! - [`reactor`]: Core reactor implementation managing the event loop lifecycle
//! - [`thread_pool`]: Configurable thread pool for distributing work
//! - [`poll`]: Cross-platform polling abstraction and handler registry
//! - [`error`]: Error types and result handling
//!
//! For comprehensive examples and architectural details, see the [README](../README.md)
//! and [Architecture Guide](../docs/Arch.md).

#![cfg_attr(feature = "unstable-mpmc", feature(mpmc_channel))]

use mio::{Interest, Token};
pub mod error;
pub mod handler;
pub mod object_pool;
pub mod poll;
pub mod reactor;
pub mod thread_pool;
pub mod timer_wheel;

pub use handler::EventHandler;
pub use mio::event::Event;
pub use object_pool::{ObjectPool, PooledObject};
pub use thread_pool::{ComputePoolMetrics, TaskPriority};
pub use timer_wheel::TimerId;

use crate::{error::Result, reactor::ReactorOptions};
use std::time::Duration;

/// A convenient prelude module that re-exports commonly used types and traits.
///
/// This module provides a convenient way to import the most commonly used items from mill-io:
///
/// ```rust
/// use mill_io::prelude::*;
/// ```
///
/// This brings into scope:
/// - [`EventHandler`] - Trait for implementing event handling logic
/// - [`ObjectPool`] and [`PooledObject`] - Object pooling utilities
/// - [`reactor::Reactor`] - Core reactor implementation (advanced usage)
/// - [`thread_pool::ThreadPool`] - Thread pool implementation (advanced usage)
pub mod prelude {
    pub use crate::handler::EventHandler;
    pub use crate::object_pool::{ObjectPool, PooledObject};
    pub use crate::reactor::{self, Reactor};
    pub use crate::thread_pool::{self, ComputePoolMetrics, TaskPriority, ThreadPool};
    pub use crate::timer_wheel::{TimerId, TimerWheel};
}

/// The main event loop structure for registering I/O sources and handling events.
///
/// `EventLoop` is the primary interface for Mill-IO, providing a simple API for:
/// - Registering I/O sources (sockets, files, etc.) with event handlers
/// - Starting and stopping the event loop
/// - Managing the underlying reactor and thread pool
///
/// The event loop uses a reactor pattern internally, where I/O events are detected
/// by the polling mechanism and dispatched to registered handlers via a thread pool.
///
/// ## Example
///
/// Basic usage with default configuration:
///
/// ```rust,no_run
/// use mill_io::{EventLoop, EventHandler};
/// use mio::{net::TcpListener, Interest, Token, event::Event};
/// use std::net::SocketAddr;
///
/// struct MyHandler;
/// impl EventHandler for MyHandler {
///     fn handle_event(&self, event: &Event) {
///         println!("Event received: {:?}", event);
///     }
/// }
///
/// let event_loop = EventLoop::default();
/// let addr: SocketAddr = "127.0.0.1:0".parse()?;
/// let mut listener = TcpListener::bind(addr)?;
///
/// event_loop.register(&mut listener, Token(0), Interest::READABLE, MyHandler)?;
/// event_loop.run()?; // Blocks until stopped
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// Custom configuration:
///
/// ```rust,no_run
/// use mill_io::EventLoop;
///
/// let event_loop = EventLoop::new(
///     4,      // 4 worker threads
///     512,    // Buffer for 512 events per poll
///     50      // 50ms poll timeout
/// )?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct EventLoop {
    reactor: reactor::Reactor,
}

impl Default for EventLoop {
    /// Creates a new `EventLoop` with default configuration.
    ///
    /// The default configuration uses:
    /// - Number of worker threads equal to available CPU cores, falling back to 4 threads if CPU detection fails ([`thread_pool::DEFAULT_POOL_CAPACITY`])
    /// - 1024 events capacity ([`reactor::DEFAULT_EVENTS_CAPACITY`])
    /// - 150ms poll timeout ([`reactor::DEFAULT_POLL_TIMEOUT_MS`])
    ///
    /// # Panics
    ///
    /// Panics if the reactor cannot be initialized with default settings.
    fn default() -> Self {
        let reactor = reactor::Reactor::default();
        Self { reactor }
    }
}

impl EventLoop {
    /// Creates a new `EventLoop` with custom configuration.
    ///
    /// ## Arguments
    /// * `workers` - Number of worker threads in the thread pool (recommended: num_cpus)
    /// * `events_capacity` - Maximum number of events to poll per iteration (typical: 512-4096)
    /// * `poll_timeout_ms` - Poll timeout in milliseconds (balance between latency and CPU usage)
    ///
    /// ## Errors
    ///
    /// Returns an error if:
    /// - The reactor cannot be initialized
    /// - The thread pool cannot be created
    /// - The polling mechanism fails to initialize
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// use mill_io::EventLoop;
    ///
    /// // High-throughput configuration
    /// let event_loop = EventLoop::new(8, 2048, 50)?;
    ///
    /// // Low-latency configuration
    /// let event_loop = EventLoop::new(2, 256, 10)?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn new(workers: usize, events_capacity: usize, poll_timeout_ms: u64) -> Result<Self> {
        let reactor = reactor::Reactor::new(workers, events_capacity, poll_timeout_ms)?;
        Ok(Self { reactor })
    }

    /// Creates a new EventLoop optimized for low latency.
    ///
    /// This mode:
    /// - Executes handlers directly on the reactor thread (no thread pool dispatch)
    /// - Uses thread-local buffer pools (no lock contention)
    /// - Best for I/O-bound workloads with fast handlers
    ///
    /// WARNING: Slow handlers will block all I/O processing!
    pub fn new_low_latency(events_capacity: usize, poll_timeout_ms: u64) -> Result<Self> {
        let reactor = reactor::Reactor::new_with_options(
            1, // Minimal pool for compute tasks
            events_capacity,
            poll_timeout_ms,
            ReactorOptions {
                direct_dispatch: true,
            },
        )?;
        Ok(Self { reactor })
    }

    /// Registers an I/O source with the event loop and associates it with a handler.
    ///
    /// This method registers an I/O source (such as a TCP listener or socket) with the event loop.
    /// When events occur on the source, the provided handler will be invoked on a worker thread.
    ///
    ///
    /// ## Arguments
    /// * `source` - The I/O source to register (e.g., [`mio::net::TcpListener`])
    /// * `token` - Unique token for identifying events from this source
    /// * `interests` - I/O events to listen for ([`mio::Interest::READABLE`], [`mio::Interest::WRITABLE`])
    /// * `handler` - Event handler that will process events from this source
    ///
    /// ## Errors
    ///
    /// Returns an error if:
    /// - The token is already in use
    /// - The source cannot be registered with the underlying poll mechanism
    /// - The handler registry is full
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// use mill_io::{EventLoop, EventHandler};
    /// use mio::{net::TcpListener, Interest, Token, event::Event};
    /// use std::net::SocketAddr;
    ///
    /// struct ConnectionHandler;
    /// impl EventHandler for ConnectionHandler {
    ///     fn handle_event(&self, event: &Event) {
    ///         // Handle new connections
    ///     }
    /// }
    /// fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let event_loop = EventLoop::default();
    ///     let addr: SocketAddr = "0.0.0.0:8080".parse()?;
    ///     let mut listener = TcpListener::bind(addr)?;
    ///
    ///     event_loop.register(
    ///         &mut listener,
    ///         Token(0),
    ///         Interest::READABLE,
    ///         ConnectionHandler
    ///     )?;
    ///    Ok(())
    /// }
    /// ```
    pub fn register<H, S>(
        &self,
        source: &mut S,
        token: Token,
        interests: Interest,
        handler: H,
    ) -> Result<()>
    where
        H: EventHandler + Send + Sync + 'static,
        S: mio::event::Source + ?Sized,
    {
        self.reactor
            .poll_handle
            .register(source, token, interests, handler)
    }

    /// Deregisters an I/O source from the event loop.
    ///
    /// Removes the source from the polling mechanism and clears its associated handler.
    /// After deregistration, no more events will be delivered for this source.
    ///
    /// ## #Arguments
    /// * `source` - The I/O source to deregister
    /// * `token` - Token associated with the source during registration
    ///
    /// ## Error
    ///
    /// Returns an error if:
    /// - The source is not currently registered
    /// - The deregistration fails at the OS level
    /// - The token is invalid
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// use mill_io::{EventLoop, EventHandler};
    /// use mio::{net::TcpListener, Interest, Token, event::Event};
    /// use std::net::SocketAddr;
    ///
    /// struct Handler;
    /// impl EventHandler for Handler {
    ///     fn handle_event(&self, _: &Event) {}
    /// }
    /// fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     
    ///     let event_loop = EventLoop::default();
    ///     let addr: SocketAddr = "127.0.0.1:0".parse()?;
    ///     let mut listener = TcpListener::bind(addr)?;
    ///     let token = Token(0);
    ///
    ///     // Register
    ///     event_loop.register(&mut listener, token, Interest::READABLE, Handler)?;
    ///
    ///     // Later, deregister
    ///     event_loop.deregister(&mut listener, token)?;
    ///     Ok(())
    /// }
    /// ```
    pub fn deregister<S>(&self, source: &mut S, token: Token) -> Result<()>
    where
        S: mio::event::Source + ?Sized,
    {
        self.reactor.poll_handle.deregister(source, token)
    }

    /// Runs the event loop, blocking the current thread and dispatching events.
    ///
    /// This method starts the reactor's main loop, which will:
    /// 1. Poll for I/O events using the configured timeout
    /// 2. Dispatch events to registered handlers via the thread pool
    /// 3. Continue until [`stop()`](Self::stop) is called or an error occurs
    ///
    /// The method blocks the calling thread and will only return when the event loop
    /// is stopped or encounters a fatal error.
    ///
    /// ## Errors
    ///
    /// Returns an error if:
    /// - The polling mechanism fails
    /// - The thread pool encounters a fatal error
    /// - System resources are exhausted
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// use mill_io::EventLoop;
    ///
    /// let event_loop = EventLoop::default();
    /// // Register some handlers first...
    /// event_loop.run()
    /// # ; Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn run(&self) -> Result<()> {
        self.reactor.run()
    }

    /// Submits a CPU-intensive task to the compute thread pool with default (Normal) priority.
    ///
    /// This method allows offloading heavy computations (e.g., cryptography, image processing)
    /// to a dedicated thread pool, preventing the I/O event loop from being blocked.
    ///
    /// ## Arguments
    /// * `task` - The closure to execute
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// use mill_io::EventLoop;
    ///
    /// let event_loop = EventLoop::default();
    ///
    /// event_loop.spawn_compute(|| {
    ///     // Heavy computation here
    ///     let result = 2 + 2;
    ///     println!("Computed: {}", result);
    /// });
    /// ```
    pub fn spawn_compute<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.reactor.spawn_compute(task, TaskPriority::Normal);
    }

    /// Submits a CPU-intensive task to the compute thread pool with a specific priority.
    ///
    /// ## Arguments
    /// * `task` - The closure to execute
    /// * `priority` - The priority of the task
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// use mill_io::{EventLoop, TaskPriority};
    ///
    /// let event_loop = EventLoop::default();
    ///
    /// event_loop.spawn_compute_with_priority(|| {
    ///     // Heavy computation here
    /// }, TaskPriority::High);
    /// ```
    pub fn spawn_compute_with_priority<F>(&self, task: F, priority: TaskPriority)
    where
        F: FnOnce() + Send + 'static,
    {
        self.reactor.spawn_compute(task, priority);
    }

    /// Number of handler panics caught since creation (pool and direct mode).
    /// A panicking [`EventHandler`] is caught and counted; the loop keeps running.
    pub fn handler_panics(&self) -> u64 {
        self.reactor.handler_panics()
    }

    /// Returns metrics for the compute-intensive thread pool.
    pub fn get_compute_metrics(&self) -> std::sync::Arc<ComputePoolMetrics> {
        self.reactor.get_compute_metrics()
    }

    /// Schedule a one-shot timer that fires after `delay`.
    ///
    /// The callback runs on the reactor thread during timer processing.
    /// Keep callbacks fast to avoid blocking the event loop.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// use mill_io::EventLoop;
    /// use std::time::Duration;
    ///
    /// let event_loop = EventLoop::default();
    /// let timer_id = event_loop.schedule_once(Duration::from_secs(5), || {
    ///     println!("Timer fired!");
    /// });
    /// ```
    pub fn schedule_once<F>(&self, delay: Duration, callback: F) -> TimerId
    where
        F: FnOnce() + Send + 'static,
    {
        let id = self.reactor.timer_wheel.schedule_once(delay, callback);
        // Wake the reactor so it can recalculate its poll timeout.
        let _ = self.reactor.poll_handle.wake();
        id
    }

    /// Schedule a repeating timer that fires every `interval`.
    /// The first firing happens after one `interval` elapses.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// use mill_io::EventLoop;
    /// use std::time::Duration;
    ///
    /// let event_loop = EventLoop::default();
    /// let timer_id = event_loop.schedule_repeating(Duration::from_secs(1), || {
    ///     println!("Tick!");
    /// });
    /// ```
    pub fn schedule_repeating<F>(&self, interval: Duration, callback: F) -> TimerId
    where
        F: Fn() + Send + Sync + 'static,
    {
        let id = self
            .reactor
            .timer_wheel
            .schedule_repeating(interval, callback);
        let _ = self.reactor.poll_handle.wake();
        id
    }

    /// Cancel a pending timer. Returns true if the timer was found and cancelled.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// use mill_io::EventLoop;
    /// use std::time::Duration;
    ///
    /// let event_loop = EventLoop::default();
    /// let timer_id = event_loop.schedule_once(Duration::from_secs(10), || {});
    /// event_loop.cancel_timer(timer_id);
    /// ```
    pub fn cancel_timer(&self, timer_id: TimerId) -> bool {
        self.reactor.timer_wheel.cancel(timer_id)
    }

    /// Returns the number of currently scheduled timers.
    pub fn pending_timers(&self) -> usize {
        self.reactor.timer_wheel.pending_count()
    }

    /// Signals the event loop to stop gracefully.
    ///
    /// Sends a shutdown signal to the reactor, which will cause the main loop
    /// to exit after finishing the current polling cycle.
    ///
    /// This method is non-blocking and returns immediately. The actual shutdown happens
    /// asynchronously, and [`run()`](Self::run) will return once the shutdown is complete.
    ///
    /// Thread-safe: can be called from any thread.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// use mill_io::EventLoop;
    /// use std::thread;
    /// use std::sync::Arc;
    ///
    /// let event_loop = Arc::new(EventLoop::default());
    /// let event_loop_clone = Arc::clone(&event_loop);
    ///
    /// // Start event loop in background thread
    /// let handle = thread::spawn(move || {
    ///     // In a real application, you would handle the result properly
    ///     let _ = event_loop_clone.run();
    /// });
    ///
    /// // Stop after some time
    /// thread::sleep(std::time::Duration::from_secs(1));
    /// event_loop.stop();
    ///
    /// // Wait for shutdown
    /// let _ = handle.join();
    /// ```
    pub fn stop(&self) {
        let shutdown_handler = self.reactor.get_shutdown_handle();
        shutdown_handler.shutdown();
    }
}
