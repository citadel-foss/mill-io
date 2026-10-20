# mill-io

Core reactor-based event loop built on top of [`mio`](https://crates.io/crates/mio), providing efficient non-blocking I/O management without async runtimes.

## Features

- **Runtime-agnostic**: No dependency on Tokio or other async runtimes
- **Cross-platform**: Leverages mio's polling abstraction (epoll, kqueue, IOCP)
- **Thread pool integration**: Configurable worker threads for handling I/O events
- **Compute pool**: Dedicated priority-based thread pool for CPU-intensive tasks
- **Object pooling**: Reduces allocation overhead for frequent operations
- **Clean API**: Simple registration and handler interface

## Installation

```toml
[dependencies]
mill-io = "4.0.0"
```

## Quick Start

```rust
use mill_io::{EventLoop, EventHandler};
use mio::{net::TcpListener, Interest, Token, event::Event};

struct EchoHandler;

impl EventHandler for EchoHandler {
    fn handle_event(&self, event: &Event) {
        // Handle incoming connections
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let event_loop = EventLoop::default();
    let mut listener = TcpListener::bind("127.0.0.1:8080".parse()?)?;
    
    event_loop.register(
        &mut listener,
        Token(1),
        Interest::READABLE,
        EchoHandler
    )?;
    
    println!("Server listening on 127.0.0.1:8080");
    event_loop.run()?;
    
    Ok(())
}
```

## Configuration

### Default Configuration

```rust
use mill_io::EventLoop;

// Uses CPU cores for workers, 1024 event capacity, 150ms timeout
let event_loop = EventLoop::default();
```

### Custom Configuration

```rust
use mill_io::EventLoop;

let event_loop = EventLoop::new(
    8,      // Number of worker threads
    2048,   // Maximum events per poll iteration
    50      // Poll timeout in milliseconds
)?;
```

## Compute Thread Pool

Mill-IO includes a dedicated thread pool for CPU-intensive operations, keeping the I/O event loop responsive.

```rust
use mill_io::{EventLoop, TaskPriority};

let event_loop = EventLoop::default();

// Spawn with default (Normal) priority
event_loop.spawn_compute(|| {
    // CPU-intensive work here
});

// Spawn with specific priority
event_loop.spawn_compute_with_priority(|| {
    // Critical computation
}, TaskPriority::Critical);
```

### Task Priorities

- `TaskPriority::Critical` - Urgent tasks, processed first
- `TaskPriority::High` - Important tasks
- `TaskPriority::Normal` - Default priority
- `TaskPriority::Low` - Background tasks

### Monitoring Metrics

```rust
let metrics = event_loop.get_compute_metrics();

println!("Tasks submitted: {}", metrics.tasks_submitted());
println!("Tasks completed: {}", metrics.tasks_completed());
println!("Active workers: {}", metrics.active_workers());
```

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](../LICENSE) for details.
