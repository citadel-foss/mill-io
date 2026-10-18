//! RPC Client built on mill-net's TcpClient.
//!
//! Connects to an RPC server, sends request frames, and waits for responses.

use crate::{RpcError, RpcTransport};
use mill_io::EventLoop;
use mill_net::tcp::traits::{ConnectionId, NetworkHandler};
use mill_net::tcp::{ServerContext, TcpClient};
use mill_rpc_core::protocol::{self, Frame, MessageType};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

/// Pending request waiting for a response.
struct PendingRequest {
    completed: bool,
    result: Option<Result<Vec<u8>, RpcError>>,
}

/// Shared state between the client and its network handler.
struct ClientShared {
    /// Map of request_id -> pending request.
    pending: Mutex<HashMap<u64, PendingRequest>>,
    /// Condvar to notify waiting callers when a response arrives.
    notify: Condvar,
    /// Receive buffer for partial frame parsing.
    recv_buf: Mutex<Vec<u8>>,
}

/// RPC client that connects to a Mill-RPC server.
pub struct RpcClient {
    tcp_client: Mutex<TcpClient<RpcClientHandler>>,
    shared: Arc<ClientShared>,
    next_request_id: AtomicU64,
    timeout: AtomicU64,
}

impl RpcClient {
    /// Connect to an RPC server.
    pub fn connect(addr: SocketAddr, event_loop: &Arc<EventLoop>) -> Result<Arc<Self>, RpcError> {
        let shared = Arc::new(ClientShared {
            pending: Mutex::new(HashMap::new()),
            notify: Condvar::new(),
            recv_buf: Mutex::new(Vec::new()),
        });

        let handler = RpcClientHandler {
            shared: shared.clone(),
        };

        let mut tcp_client = TcpClient::connect(addr, handler)
            .map_err(|e| RpcError::unavailable(format!("Connect failed: {}", e)))?;

        tcp_client
            .start(event_loop, event_loop.alloc_token())
            .map_err(|e| RpcError::unavailable(format!("Client start failed: {}", e)))?;

        Ok(Arc::new(Self {
            tcp_client: Mutex::new(tcp_client),
            shared,
            next_request_id: AtomicU64::new(1),
            timeout: AtomicU64::new(30 * 1000),
        }))
    }

    /// Set the default timeout for RPC calls.
    pub fn set_timeout(&self, timeout: Duration) {
        self.timeout
            .store(timeout.as_millis() as u64, Ordering::SeqCst);
    }

    fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout.load(Ordering::SeqCst))
    }

    /// Send a request and wait for a response (blocking).
    fn call_raw(
        &self,
        service_id: u16,
        method_id: u16,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, RpcError> {
        let request_id = self.next_request_id.fetch_add(1, Ordering::SeqCst);

        // Register the pending request before sending.
        {
            let mut pending = self.shared.pending.lock().unwrap();
            pending.insert(
                request_id,
                PendingRequest {
                    completed: false,
                    result: None,
                },
            );
        }

        let frame = Frame::request(request_id, service_id, method_id, payload, false);
        let send_result = {
            let client = self.tcp_client.lock().unwrap();
            client.send(&frame.encode())
        };
        if let Err(e) = send_result {
            let mut pending = self.shared.pending.lock().unwrap();
            pending.remove(&request_id);
            return Err(RpcError::unavailable(format!("Send failed: {}", e)));
        }

        let mut pending = self.shared.pending.lock().unwrap();
        let deadline = std::time::Instant::now() + self.timeout();

        loop {
            if let Some(req) = pending.get(&request_id) {
                if req.completed {
                    let req = pending.remove(&request_id).unwrap();
                    return req.result.unwrap();
                }
            } else {
                return Err(RpcError::internal("Pending request disappeared"));
            }

            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                pending.remove(&request_id);
                return Err(RpcError::deadline_exceeded(format!(
                    "Request {} timed out after {:?}",
                    request_id, self.timeout
                )));
            }

            let (guard, timeout_result) =
                self.shared.notify.wait_timeout(pending, remaining).unwrap();
            pending = guard;

            if timeout_result.timed_out() {
                pending.remove(&request_id);
                return Err(RpcError::deadline_exceeded(format!(
                    "Request {} timed out after {:?}",
                    request_id, self.timeout
                )));
            }
        }
    }

    /// Send a one-way request (fire-and-forget).
    pub fn call_oneway(
        &self,
        service_id: u16,
        method_id: u16,
        payload: Vec<u8>,
    ) -> Result<(), RpcError> {
        let request_id = self.next_request_id.fetch_add(1, Ordering::SeqCst);
        let frame = Frame::request(request_id, service_id, method_id, payload, true);

        let client = self.tcp_client.lock().unwrap();
        client
            .send(&frame.encode())
            .map_err(|e| RpcError::unavailable(format!("Send failed: {}", e)))?;
        Ok(())
    }
}

impl RpcTransport for RpcClient {
    fn call(&self, service_id: u16, method_id: u16, payload: Vec<u8>) -> Result<Vec<u8>, RpcError> {
        self.call_raw(service_id, method_id, payload)
    }
}

/// Network handler for the RPC client - receives response frames.
struct RpcClientHandler {
    shared: Arc<ClientShared>,
}

impl NetworkHandler for RpcClientHandler {
    fn on_data(
        &self,
        _ctx: &ServerContext,
        _conn_id: ConnectionId,
        data: &[u8],
    ) -> mill_net::errors::Result<()> {
        let mut recv_buf = self.shared.recv_buf.lock().unwrap();
        recv_buf.extend_from_slice(data);

        let (frames, consumed) = match protocol::parse_frames(&recv_buf) {
            Ok(r) => r,
            Err(e) => {
                log::error!("Client frame parse error: {}", e);
                recv_buf.clear();
                return Ok(());
            }
        };

        if consumed > 0 {
            recv_buf.drain(..consumed);
        }

        drop(recv_buf);

        for frame in frames {
            self.handle_frame(frame);
        }

        Ok(())
    }
}

impl RpcClientHandler {
    fn handle_frame(&self, frame: Frame) {
        match frame.header.message_type {
            MessageType::Response => {
                let (request_id, data) = match frame.parse_response_payload() {
                    Ok(r) => r,
                    Err(e) => {
                        log::error!("Invalid response payload: {}", e);
                        return;
                    }
                };

                let mut pending = self.shared.pending.lock().unwrap();
                if let Some(req) = pending.get_mut(&request_id) {
                    req.completed = true;
                    req.result = Some(Ok(data.to_vec()));
                }
                self.shared.notify.notify_all();
            }
            MessageType::Error => {
                let (request_id, err_data) = match frame.parse_response_payload() {
                    Ok(r) => r,
                    Err(e) => {
                        log::error!("Invalid error payload: {}", e);
                        return;
                    }
                };

                let rpc_err: RpcError = match bincode::deserialize(err_data) {
                    Ok(e) => e,
                    Err(_) => RpcError::internal(String::from_utf8_lossy(err_data).to_string()),
                };

                let mut pending = self.shared.pending.lock().unwrap();
                if let Some(req) = pending.get_mut(&request_id) {
                    req.completed = true;
                    req.result = Some(Err(rpc_err));
                }
                self.shared.notify.notify_all();
            }
            MessageType::Pong => {
                log::debug!("Received pong from server");
            }
            other => {
                log::warn!("Unexpected message type from server: {:?}", other);
            }
        }
    }
}
