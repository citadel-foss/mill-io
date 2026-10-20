use crate::errors::Result;
use crate::errors::{NetworkError, NetworkEvent};

use super::ServerContext;

/// Unique identifier for connections.
///
/// Each TCP connection is assigned a unique ConnectionId when accepted or established.
/// The ID is generated atomically and remains constant for the connection's lifetime.
///
/// ConnectionIds are used to target specific connections for operations like sending
/// data or closing connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConnectionId(pub u64);

impl ConnectionId {
    pub fn new(id: u64) -> Self {
        ConnectionId(id)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

/// Handler for network events on TCP connections.
///
/// Implement this trait to define how your application responds to network events.
/// All methods except on_data have default implementations that do nothing.
///
/// The handler is invoked by worker threads from the event loop's thread pool,
/// so implementations must be thread-safe (Send + Sync).
///
/// ## Execution Context
///
/// Handler methods are called from worker threads in the thread pool. Multiple
/// handlers may execute concurrently for different connections. Your implementation
/// should be efficient to avoid blocking worker threads.
///
/// ## Error Handling
///
/// Return errors from handler methods to indicate processing failures. The connection
/// will be closed automatically when handlers return errors from on_data.
pub trait NetworkHandler: Send + Sync + 'static {
    /// Called when a non-error network event occurs
    fn on_event(&self, ctx: &ServerContext, event: NetworkEvent) -> Result<()> {
        let _ = (ctx, event);
        Ok(())
    }

    /// Called when connection is established (TCP only)
    fn on_connect(&self, ctx: &ServerContext, conn_id: ConnectionId) -> Result<()> {
        let _ = (ctx, conn_id);
        Ok(())
    }

    /// Called when data is received
    fn on_data(&self, ctx: &ServerContext, conn_id: ConnectionId, data: &[u8]) -> Result<()>;

    /// Called when connection is closed (TCP only)
    fn on_disconnect(&self, ctx: &ServerContext, conn_id: ConnectionId) -> Result<()> {
        let _ = (ctx, conn_id);
        Ok(())
    }

    /// Called on write readiness (for backpressure handling)
    fn on_writable(&self, ctx: &ServerContext, conn_id: ConnectionId) -> Result<()> {
        let _ = (ctx, conn_id);
        Ok(())
    }

    /// Called once the outbound queue falls to the low watermark after a send was refused
    /// with [`SendError::Backpressure`](super::SendError::Backpressure).
    fn on_drained(&self, ctx: &ServerContext, conn: ConnectionId) -> Result<()> {
        let _ = (ctx, conn);
        Ok(())
    }

    /// Called on errors
    fn on_error(&self, ctx: &ServerContext, conn_id: Option<ConnectionId>, error: NetworkError) {
        let _ = (ctx, conn_id, error);
    }
}
