# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [4.0.0] - 2026-10-20

### Breaking

- **mill-io**: `register()` rejects `Token(0)`, which is now reserved for the waker; take tokens from `EventLoop::alloc_token()` (#103)
- **mill-io**: `stop()` is sticky and never panics: a stop called before `run()` makes that `run()` return at once, and each stop is consumed by one `run()` (#104)
- **mill-net**: `TcpClient::connect` takes a `TcpClientConfig`, `TcpClient::start` allocates its own token, and `on_connect` fires only once the handshake completes (#110, #111)
- **mill-net**: Sends go through a per-connection write queue and can fail with `NetworkError::Send(SendError::Backpressure)` above the high watermark (#110)
- **mill-net**: Dropping a `TcpClient` closes its connection; a started `TcpServer` runs until `TcpServer::stop` (#112)
- **mill-rpc**: Pending calls fail when the connection drops instead of waiting out their timeout (#114)

### Added

- **mill-io**: Timer wheel with `schedule_once`, `schedule_repeating`, `cancel_timer` and `pending_timers` (#86)
- **mill-io**: `EventLoop::post()`, `is_reactor_thread()` and `reregister()` (#104)
- **mill-io**: `EventLoop::handler_panics()` and `EventLoop::timer_panics()` count caught panics (#101, #107)
- **mill-net**: `ConnSender` sends, closes and pauses reads from any thread, with `on_drained`, `pause_reads` and `resume_reads` (#110)
- **mill-net**: Configurable high and low watermarks on `TcpServerConfig` and `TcpClientConfig` (#110)
- **mill-net**: `TcpClientConfig::connect_timeout`, reported through `on_error` (#111)
- **mill-net**: `TcpServer::stop` (#112)
- **mill-net**: `Layer` trait for handshakes and byte transforms, added with `with_layer` (#113)
- **mill-net**: `testing::ChaosProxy`, behind the `testing` feature, splits, throttles, stalls and resets TCP streams (#93, #94)
- **mill-rpc**: `slow_reader_timeout`: replies to a slow reader wait for `on_drained`, and the connection closes after the timeout (#114)
- CI enforces the MSRV, 1.74 (#88)

### Fixed

- **mill-io**: Timer deadlines saturate instead of wrapping, and zero-interval repeating timers no longer spin the reactor (#89, #90)
- **mill-io**: Panics in timer callbacks and event handlers are caught and counted instead of killing the reactor or a worker (#91, #101)
- **mill-io**: Compute pool shutdown no longer hangs in `drop`, and queue depth metrics no longer drift (#87, #96)
- **mill-io**: Pool mode sends all events for a token to the same worker, so a connection's events stay in order (#102)
- **mill-net**: Reads drain until `WouldBlock`, so large messages are no longer stuck until more bytes arrive (#109)
- **mill-net**: Large or concurrent sends no longer tear frames on `WouldBlock` (#110)
- **mill-net**: `keep_alive` and `buffer_size` are applied, and socket options go through `socket2` with no `unsafe` (#112)
- **mill-rpc**: Several clients can share one event loop (#105)
- **mill-rpc**: The server frames each connection separately, so a corrupt frame closes only that connection (#114)

## [3.0.0] - 2026-03-18

### Added

- **mill-net**: New crate providing high-level TCP networking built on mill-io's event loop (#81)
  - Lockfree connection management with atomic `ConnectionId` assignment
  - `NetworkHandler` trait with event callbacks (`on_connect`, `on_data`, `on_disconnect`, `on_writable`, `on_error`)
  - `TcpServer` and `TcpClient` with builder-pattern `TcpServerConfig` (buffer size, max connections, TCP_NODELAY, SO_KEEPALIVE)
  - `ServerContext` for sending data and managing connections from handler callbacks
  - No async runtime required — handler-based non-blocking I/O on top of epoll/kqueue/IOCP
- **mill-rpc**: New Axum-inspired RPC framework built on mill-io and mill-net (#85)
  - `mill_rpc::service!` macro for declarative service definitions with generated server traits, client structs, and dispatch logic
  - Selective code generation with `#[server]` and `#[client]` attributes
  - Multi-service hosting on a single port with automatic routing
  - Binary wire protocol with efficient framing, one-way calls, and ping/pong
  - Pluggable codec system (Bincode by default)
  - `RpcServer::builder()` and `RpcClient::connect()` APIs
- **mill-io**: Direct dispatching with low latency mode for latency-sensitive workloads (#83)
- Benchmarking suite for mill-io, mill-net, and mill-rpc (#82)

### Changed

- Split the monolithic crate into `mill-io` and `mill-net` as individual publishable crates (#81)
- Use `parking_lot` for minimal `Mutex` overhead across all crates (#84)
- Added crates.io publishing metadata (documentation, homepage, repository, readme, keywords) to all crates

### Fixed

- Replace `lockfree` with `lock_freedom` for soundness (#80)
- Update architecture diagrams to use consistent ASCII characters (#79)

## [2.0.1] - 2025-12-30


### Changed

- Replace lockfree with lock_freedom (#75, By @jayvdb)

### Documentation
- docs: Update documentation with new features and enhanced architecture diagrams (#78, By @hulxv)



## [2.0.0] - 2025-12-29

### Changed

- feat: fetching available cores instead of default capacity of ThreadPool (#73, By @Sansh2356)
- feat: event-loop-based Tcp Networking Layer (#74, By @hulxv)
- feat: compute-intensive threadpool (#77, By @hulxv)

### Fixed

- fix: bottlenecks in the poll handler and threadpool (#76, By @hulxv)

## [1.0.1] - 2025-9-16

### Documentation

- Fixed documentation errors throughout the codebase
- Enhanced README.md with better explanations and examples

### Development

- Bumped version for documentation

## [1.0.2] - 2025-10-17

### Changed

- Updated `mio-rs` to version `1.1.0` which includes the fix for macOS thread safety issues

### Fixed

- Resolved macOS thread safety issues with `mio::Event` in worker threads (initially fixed with custom Event wrapper #70, then properly resolved by updating mio-rs after upstream fix #72)

### Development

- Enhanced CI/CD with cross-platform testing workflows
- Fixed clippy linting issues
- Added unstable feature testing with nightly Rust channel
- Improved CI workflow to avoid using `unstable` features in stable & beta channels
- Added TCP tests and disabled UDS tests on Windows for better cross-platform support
- Fixed documentation errors

## [1.0.1] - 2025-9-16

### Documentation

- Fixed documentation errors throughout the codebase
- Enhanced README.md with better explanations and examples

### Development

- Bumped version for documentation fixes

## [1.0.0] - 2025-9-15

### Added

- Complete event loop implementation with reactor pattern
- Thread pool for efficient task execution
- Object pool for memory management optimization
- Polling abstraction layer with `PollHandle`
- Error handling module with custom error types
- Event loop registration and deregistration capabilities
- Multiple channel types (MPMC/MPSC) for inter-thread communication

### Examples

- **Echo Server**: Complete TCP echo server implementation
- **HTTP Server**: Basic HTTP server example
- **File Watcher**: File system monitoring example  
- **JSON-RPC Server**: JSON-RPC protocol server implementation
