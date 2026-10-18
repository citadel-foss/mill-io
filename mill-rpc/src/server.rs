//! RPC Server built on mill-net's TcpServer.
//!
//! The server accepts TCP connections, parses Mill-RPC frames, dispatches
//! requests to registered services, and sends back responses.

use crate::{Codec, RpcContext, RpcError, ServiceDispatch};
use mill_io::EventLoop;
use mill_net::errors::{NetworkError, Result};
use mill_net::tcp::config::TcpServerConfig;
use mill_net::tcp::traits::{ConnectionId, NetworkHandler};
use mill_net::tcp::{ServerContext, TcpServer};
use mill_rpc_core::protocol::{self, Frame, MessageType};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};

/// A registered service with its dispatch implementation.
struct RegisteredService {
    dispatcher: Box<dyn ServiceDispatch>,
}

/// RPC Server that hosts one or more services.
pub struct RpcServer {
    _tcp_server: Arc<TcpServer<RpcServerHandler>>,
}

impl RpcServer {
    /// Create a builder for configuring the RPC server.
    pub fn builder() -> RpcServerBuilder {
        RpcServerBuilder::new()
    }
}

/// Builder for constructing an RpcServer.
pub struct RpcServerBuilder {
    address: Option<SocketAddr>,
    codec: Codec,
    max_connections: Option<usize>,
    services: Vec<(u16, Box<dyn ServiceDispatch>)>,
    next_service_id: u16,
}

impl RpcServerBuilder {
    fn new() -> Self {
        Self {
            address: None,
            codec: Codec::bincode(),
            max_connections: None,
            services: Vec::new(),
            next_service_id: 0,
        }
    }

    /// Set the address to bind to.
    pub fn bind(mut self, addr: SocketAddr) -> Self {
        self.address = Some(addr);
        self
    }

    /// Set the codec for serialization.
    pub fn codec(mut self, codec: Codec) -> Self {
        self.codec = codec;
        self
    }

    /// Set the maximum number of connections.
    pub fn max_connections(mut self, max: usize) -> Self {
        self.max_connections = Some(max);
        self
    }

    /// Register a service implementation wrapped in its dispatcher.
    ///
    /// The service must implement `ServiceDispatch` - typically via the
    /// generated `{Name}Dispatcher` wrapper:
    ///
    /// ```ignore
    /// .service(CalculatorDispatcher(MyCalculator))
    /// ```
    pub fn service<S: ServiceDispatch>(mut self, svc: S) -> Self {
        let id = self.next_service_id;
        self.next_service_id += 1;
        self.services.push((id, Box::new(svc)));
        self
    }

    /// Register a service with an explicit service ID.
    pub fn service_with_id<S: ServiceDispatch>(mut self, id: u16, svc: S) -> Self {
        self.services.push((id, Box::new(svc)));
        self
    }

    /// Build and start the RPC server on the given event loop.
    pub fn build(self, event_loop: &Arc<EventLoop>) -> Result<RpcServer> {
        let addr = self
            .address
            .unwrap_or_else(|| "127.0.0.1:9000".parse().unwrap());

        let mut service_map = HashMap::new();
        for (id, dispatcher) in self.services {
            service_map.insert(id, RegisteredService { dispatcher });
        }

        let handler = RpcServerHandler {
            services: Arc::new(RwLock::new(service_map)),
            codec: self.codec,
            conn_buffers: Arc::new(Mutex::new(HashMap::new())),
        };

        let mut config_builder = TcpServerConfig::builder().address(addr);
        if let Some(max) = self.max_connections {
            config_builder = config_builder.max_connections(max);
        }
        let config = config_builder.build();

        let tcp_server = Arc::new(TcpServer::new(config, handler)?);
        tcp_server
            .clone()
            .start(event_loop, event_loop.alloc_token())?;

        log::info!("Mill-RPC server listening on {}", addr);

        Ok(RpcServer {
            _tcp_server: tcp_server,
        })
    }
}

/// Internal handler that bridges mill-net's NetworkHandler to RPC dispatch.
struct RpcServerHandler {
    services: Arc<RwLock<HashMap<u16, RegisteredService>>>,
    codec: Codec,
    /// Per-connection receive buffers for handling partial frames.
    conn_buffers: Arc<Mutex<HashMap<u64, Vec<u8>>>>,
}

impl NetworkHandler for RpcServerHandler {
    fn on_connect(&self, _ctx: &ServerContext, conn_id: ConnectionId) -> Result<()> {
        log::debug!("RPC connection established: {:?}", conn_id);
        self.conn_buffers
            .lock()
            .unwrap()
            .insert(conn_id.as_u64(), Vec::new());
        Ok(())
    }

    fn on_data(&self, ctx: &ServerContext, conn_id: ConnectionId, data: &[u8]) -> Result<()> {
        // Append incoming data to the connection's buffer.
        let mut buffers = self.conn_buffers.lock().unwrap();
        let buf = buffers.entry(conn_id.as_u64()).or_default();
        buf.extend_from_slice(data);

        // Try to parse complete frames.
        let (frames, consumed) = match protocol::parse_frames(buf) {
            Ok(result) => result,
            Err(e) => {
                log::error!("Frame parse error from {:?}: {}", conn_id, e);
                // Clear buffer on parse error - connection is likely corrupted.
                buf.clear();
                return Ok(());
            }
        };

        // Remove consumed bytes from the buffer.
        if consumed > 0 {
            buf.drain(..consumed);
        }

        // Drop the lock before processing frames (handlers may be slow).
        drop(buffers);

        // Process each complete frame.
        for frame in frames {
            self.handle_frame(ctx, conn_id, frame);
        }

        Ok(())
    }

    fn on_disconnect(&self, _ctx: &ServerContext, conn_id: ConnectionId) -> Result<()> {
        log::debug!("RPC connection closed: {:?}", conn_id);
        self.conn_buffers.lock().unwrap().remove(&conn_id.as_u64());
        Ok(())
    }

    fn on_error(&self, _ctx: &ServerContext, conn_id: Option<ConnectionId>, error: NetworkError) {
        log::error!("RPC network error (conn={:?}): {}", conn_id, error);
    }
}

impl RpcServerHandler {
    fn handle_frame(&self, ctx: &ServerContext, conn_id: ConnectionId, frame: Frame) {
        match frame.header.message_type {
            MessageType::Request => self.handle_request(ctx, conn_id, frame),
            MessageType::Ping => {
                let pong = Frame::pong();
                if let Err(e) = ctx.send_to(conn_id, &pong.encode()) {
                    log::error!("Failed to send pong to {:?}: {}", conn_id, e);
                }
            }
            MessageType::Cancel => {
                log::debug!("Cancel received from {:?} (not yet supported)", conn_id);
            }
            other => {
                log::warn!("Unexpected message type {:?} from {:?}", other, conn_id);
            }
        }
    }

    fn handle_request(&self, ctx: &ServerContext, conn_id: ConnectionId, frame: Frame) {
        let (request_id, service_id, method_id, args) = match frame.parse_request_payload() {
            Ok(parsed) => parsed,
            Err(e) => {
                log::error!("Invalid request payload from {:?}: {}", conn_id, e);
                return;
            }
        };

        let is_one_way = frame.header.flags.is_one_way();

        let rpc_ctx = RpcContext::new(request_id, service_id, method_id);

        // Look up the service and dispatch.
        let result = {
            let services = self.services.read().unwrap();
            match services.get(&service_id) {
                Some(svc) => svc
                    .dispatcher
                    .dispatch(&rpc_ctx, method_id, args, &self.codec),
                None => Err(RpcError::service_not_found(service_id)),
            }
        };

        // Don't send a response for one-way calls.
        if is_one_way {
            if let Err(e) = &result {
                log::error!("One-way request {}.{} failed: {}", service_id, method_id, e);
            }
            return;
        }

        // Send response or error frame.
        let response_frame = match result {
            Ok(resp_bytes) => Frame::response(request_id, resp_bytes),
            Err(rpc_err) => {
                let err_bytes = match self.codec.serialize(&rpc_err) {
                    Ok(b) => b,
                    Err(e) => {
                        log::error!("Failed to serialize error: {}", e);
                        return;
                    }
                };
                Frame::error(request_id, err_bytes)
            }
        };

        if let Err(e) = ctx.send_to(conn_id, &response_frame.encode()) {
            log::error!(
                "Failed to send response for request {} to {:?}: {}",
                request_id,
                conn_id,
                e
            );
        }
    }
}
