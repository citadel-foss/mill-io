# Mill-IO

A lightweight, production-ready event loop library for Rust that provides efficient non-blocking I/O management without relying on heavyweight async runtimes.

## Features

- **Runtime-agnostic**: No dependency on Tokio or other async runtimes
- **Cross-platform**: Leverages mio's polling abstraction (epoll, kqueue, IOCP)
- **Thread pool integration**: Configurable worker threads for handling I/O events
- **Compute pool**: Dedicated priority-based thread pool for CPU-intensive tasks
- **High-level networking**: High-level server/client components based on mill-io
- **RPC framework**: macro-driven RPC with type-safe clients and servers
- **Object pooling**: Reduces allocation overhead for frequent operations
- **Clean API**: Simple registration and handler interface

## Crates

| Crate                    | Description                                  |
| ------------------------ | -------------------------------------------- |
| [`mill-io`](./mill-io)   | Core reactor-based event loop                |
| [`mill-net`](./mill-net) | High-level TCP server/client networking      |
| [`mill-rpc`](./mill-rpc) | Macro-driven RPC framework (server + client) |

## Installation

For the core event loop only:

```toml
[dependencies]
mill-io = "4.0.0"
```

For high-level networking (includes mill-io as dependency):

```toml
[dependencies]
mill-net = "4.0.0"
```

For the RPC framework (includes mill-io and mill-net):

```toml
[dependencies]
mill-rpc = { path = "mill-rpc" }
```

## Architecture

For detailed architectural documentation, see [Architecture Guide](./docs/Arch.md).

## Platform Support

Supports all major platforms through mio:

- **Linux**: epoll-based polling
- **macOS**: kqueue-based polling  
- **Windows**: IOCP-based polling
- **FreeBSD/OpenBSD**: kqueue-based polling

Minimum supported Rust version: 1.70

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

## Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details.
