use std::net::SocketAddr;

/// Configuration for TCP server.
///
/// Controls server behavior including bind address, buffer sizes, connection limits,
/// and socket options. Use TcpServerConfig::builder() for ergonomic construction.
///
/// ## Socket Options
///
/// - no_delay: When enabled (default), disables Nagle's algorithm for lower latency
/// - keep_alive: Configures SO_KEEPALIVE to detect dead connections
///
/// ## Resource Limits
///
/// - buffer_size: Size of read buffers allocated from the pool
/// - max_connections: Hard limit on concurrent connections (None for unlimited)
#[derive(Clone)]
pub struct TcpServerConfig {
    /// Address to bind to
    pub address: SocketAddr,
    /// Size of connection buffer
    pub buffer_size: usize,
    /// Maximum number of connections
    pub max_connections: Option<usize>,
    /// Enable TCP_NODELAY
    pub no_delay: bool,
    /// SO_KEEPALIVE setting
    pub keep_alive: Option<std::time::Duration>,
    /// Queued outbound bytes at or above which sends get `SendError::Backpressure`
    pub high_watermark: usize,
    /// Queued outbound bytes at or below which `on_drained` fires after a refused send
    pub low_watermark: usize,
}

/// Default high watermark for servers and clients: 1 MiB.
pub const DEFAULT_HIGH_WATERMARK: usize = 1024 * 1024;
/// Default low watermark for servers and clients: 256 KiB.
pub const DEFAULT_LOW_WATERMARK: usize = 256 * 1024;

impl TcpServerConfig {
    /// Create a new builder for TcpServerConfig
    pub fn builder() -> TcpServerConfigBuilder {
        TcpServerConfigBuilder::new()
    }
}

impl Default for TcpServerConfig {
    fn default() -> Self {
        Self {
            address: "127.0.0.1:8080".parse().unwrap(),
            buffer_size: 8192,
            max_connections: None,
            no_delay: true,
            keep_alive: Some(std::time::Duration::from_secs(60)),
            high_watermark: DEFAULT_HIGH_WATERMARK,
            low_watermark: DEFAULT_LOW_WATERMARK,
        }
    }
}

/// Builder for TcpServerConfig using the builder pattern.
///
/// All fields are optional and will use defaults from TcpServerConfig::default()
/// if not explicitly set.
pub struct TcpServerConfigBuilder {
    address: Option<SocketAddr>,
    buffer_size: Option<usize>,
    max_connections: Option<usize>,
    no_delay: Option<bool>,
    keep_alive: Option<Option<std::time::Duration>>,
    high_watermark: Option<usize>,
    low_watermark: Option<usize>,
}

impl TcpServerConfigBuilder {
    /// Create a new builder with default values
    pub fn new() -> Self {
        Self {
            address: None,
            buffer_size: None,
            max_connections: None,
            no_delay: None,
            keep_alive: None,
            high_watermark: None,
            low_watermark: None,
        }
    }

    /// Set the address to bind to
    pub fn address(mut self, address: SocketAddr) -> Self {
        self.address = Some(address);
        self
    }

    /// Set the buffer size for connections
    pub fn buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = Some(size);
        self
    }

    /// Set the maximum number of connections
    pub fn max_connections(mut self, max: usize) -> Self {
        self.max_connections = Some(max);
        self
    }

    /// Enable or disable TCP_NODELAY
    pub fn no_delay(mut self, enabled: bool) -> Self {
        self.no_delay = Some(enabled);
        self
    }

    /// Set SO_KEEPALIVE duration
    pub fn keep_alive(mut self, duration: Option<std::time::Duration>) -> Self {
        self.keep_alive = Some(duration);
        self
    }

    /// Set the outbound queue's high watermark, in bytes
    pub fn high_watermark(mut self, bytes: usize) -> Self {
        self.high_watermark = Some(bytes);
        self
    }

    /// Set the outbound queue's low watermark, in bytes
    pub fn low_watermark(mut self, bytes: usize) -> Self {
        self.low_watermark = Some(bytes);
        self
    }

    /// Build the TcpServerConfig
    pub fn build(self) -> TcpServerConfig {
        let default = TcpServerConfig::default();
        TcpServerConfig {
            address: self.address.unwrap_or(default.address),
            buffer_size: self.buffer_size.unwrap_or(default.buffer_size),
            max_connections: self.max_connections.or(default.max_connections),
            no_delay: self.no_delay.unwrap_or(default.no_delay),
            keep_alive: self.keep_alive.unwrap_or(default.keep_alive),
            high_watermark: self.high_watermark.unwrap_or(default.high_watermark),
            low_watermark: self.low_watermark.unwrap_or(default.low_watermark),
        }
    }
}

impl Default for TcpServerConfigBuilder {
    fn default() -> Self {
        let default = TcpServerConfig::default();
        Self {
            address: Some(default.address),
            buffer_size: Some(default.buffer_size),
            max_connections: default.max_connections,
            no_delay: Some(default.no_delay),
            keep_alive: Some(default.keep_alive),
            high_watermark: Some(default.high_watermark),
            low_watermark: Some(default.low_watermark),
        }
    }
}

/// Configuration for [`TcpClient`](super::TcpClient).
#[derive(Clone, Debug)]
pub struct TcpClientConfig {
    /// How long the TCP handshake may take before the connect fails with `TimedOut`
    pub connect_timeout: std::time::Duration,
    /// Queued outbound bytes at or above which sends get `SendError::Backpressure`
    pub high_watermark: usize,
    /// Queued outbound bytes at or below which `on_drained` fires after a refused send
    pub low_watermark: usize,
    /// Enable TCP_NODELAY
    pub nodelay: bool,
}

impl Default for TcpClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout: std::time::Duration::from_secs(10),
            high_watermark: DEFAULT_HIGH_WATERMARK,
            low_watermark: DEFAULT_LOW_WATERMARK,
            nodelay: true,
        }
    }
}
