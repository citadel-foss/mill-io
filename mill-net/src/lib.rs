//! High-level networking abstractions based on Mill-IO event loop library.
//!
//! This library provides high-level networking abstractions that integrate with
//! Mill-IO's reactor-based event loop. The design eliminates the need for async/await
//! while providing efficient non-blocking I/O through handler callbacks.
//!
//! # Architecture Overview
//!
//! ```text
//! +-------------------------------------------------------------+
//! |                      User Application                       |
//! |  +--------------+          +-----------------+              |
//! |  | TcpServer/   |----------| Your Handler    |              |
//! |  | TcpClient    |          | (NetworkHandler)|              |
//! |  +--------------+          +-----------------+              |
//! +-------------+----------------------+------------------------+
//!              |                      | Callbacks
//!              | Register             | (on_connect, on_data, etc.)
//!              |                      |
//! +-------------------------------------------------------------+
//! |                      Mill-IO EventLoop                      |
//! |  +----------+       +----------+       +--------------+     |
//! |  | Reactor  |-------| Handlers |-------| Thread Pool  |     |
//! |  | (Poll)   |       | Registry |       |              |     |
//! |  +----------+       +----------+       +--------------+     |
//! +-------------+-----------------------------------------------+
//!              | OS Events
//!              |
//! +-------------------------------------------------------------+
//! |              Operating System (epoll/kqueue/IOCP)           |
//! +-------------------------------------------------------------+
//! ```
//!
//! The reactor continuously polls for I/O events. When an event occurs (data arrives,
//! connection accepted, etc.), it dispatches the event to a worker thread from the
//! thread pool, which then invokes the appropriate handler method you implemented.
//!
//! # Example
//!
//! ```rust,no_run
//! use mill_net::tcp::{TcpServer, TcpServerConfig};
//! use mill_net::tcp::{traits::{NetworkHandler, ConnectionId}, ServerContext};
//! use mill_io::{EventLoop, error::Result};
//! use std::sync::Arc;
//!
//! struct EchoHandler;
//!
//! impl NetworkHandler for EchoHandler {
//!     fn on_data(&self, _ctx: &ServerContext, _conn_id: ConnectionId, data: &[u8]) -> Result<()> {
//!         // Echo back the data
//!         println!("Received: {:?}", data);
//!         Ok(())
//!     }
//! }
//!
//! # fn main() -> Result<()> {
//! let config = TcpServerConfig::builder()
//!     .address("127.0.0.1:8080".parse().unwrap())
//!     .buffer_size(8192)
//!     .build();
//!
//! let server = Arc::new(TcpServer::new(config, EchoHandler)?);
//! let event_loop = Arc::new(EventLoop::default());
//!
//! server.start(&event_loop, mio::Token(0))?;
//! event_loop.run()?;
//! # Ok(())
//! # }
//! ```

pub mod errors;
pub mod tcp;
/// Test tools, such as a TCP proxy that splits, throttles, stalls and resets connections.
#[cfg(feature = "testing")]
pub mod testing;
