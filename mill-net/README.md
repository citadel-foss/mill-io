# mill-net

High-level networking components library built on top of [`mill-io`](../mill-io), providing connection management and a simple handler-based API.

## Features

- **Connection management**: Automatic handling of accept, read, write, and close
- **Handler-based API**: Implement simple traits to handle network events
- **Configurable**: Buffer sizes, connection limits, TCP options
- **Thread-safe**: Safe to use across multiple threads

## Installation

```toml
[dependencies]
mill-net = "4.0.0"
```

## Quick Start

### TCP Server

```rust
use mill_net::tcp::{TcpServer, TcpServerConfig, traits::*, ServerContext};
use mill_io::{EventLoop, error::Result};
use std::sync::Arc;

struct EchoHandler;

impl NetworkHandler for EchoHandler {
    fn on_connect(&self, _ctx: &ServerContext, conn_id: ConnectionId) -> Result<()> {
        println!("Client connected: {:?}", conn_id);
        Ok(())
    }

    fn on_data(&self, ctx: &ServerContext, conn_id: ConnectionId, data: &[u8]) -> Result<()> {
        // Echo back the data
        ctx.send_to(conn_id, data)?;
        Ok(())
    }

    fn on_disconnect(&self, _ctx: &ServerContext, conn_id: ConnectionId) -> Result<()> {
        println!("Client disconnected: {:?}", conn_id);
        Ok(())
    }
}

fn main() -> Result<()> {
    let event_loop = Arc::new(EventLoop::default());
    
    let config = TcpServerConfig::builder()
        .address("127.0.0.1:8080".parse().unwrap())
        .buffer_size(8192)
        .max_connections(1000)
        .build();

    let server = Arc::new(TcpServer::new(config, EchoHandler)?);
    server.start(&event_loop, event_loop.alloc_token())?;
    
    event_loop.run()?;
    Ok(())
}
```

### Server Context Operations

The `ServerContext` provides methods for interacting with connections:

```rust
// Send data to a specific connection
ctx.send_to(conn_id, b"Hello")?;

// Broadcast to all connections
ctx.broadcast(b"Message to all")?;

// Close a connection
ctx.close_connection(conn_id)?;
```

## Configuration

```rust
use mill_net::tcp::TcpServerConfig;
use std::time::Duration;

let config = TcpServerConfig::builder()
    .address("0.0.0.0:8080".parse().unwrap())
    .buffer_size(16384)          // Read buffer size
    .max_connections(10000)      // Connection limit
    .no_delay(true)              // Disable Nagle's algorithm
    .keep_alive(Some(Duration::from_secs(60)))
    .build();
```

### Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `address` | Required | Socket address to bind |
| `buffer_size` | 8192 | Read buffer size per connection |
| `max_connections` | 1024 | Maximum concurrent connections |
| `no_delay` | false | Disable Nagle's algorithm (TCP_NODELAY) |
| `keep_alive` | None | TCP keep-alive interval |

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](../LICENSE) for details.
