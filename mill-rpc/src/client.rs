//! RPC Client built on mill-net's TcpClient.
//!
//! Connects to an RPC server, sends request frames, and waits for responses.

use crate::{RpcError, RpcStatus, RpcTransport};
use mill_io::EventLoop;
use mill_net::errors::NetworkError;
use mill_net::tcp::traits::{ConnectionId, NetworkHandler};
use mill_net::tcp::{ConnSender, SendError, ServerContext, TcpClient, TcpClientConfig};
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

/// Calls waiting for replies, and why the connection ended once it has.
#[derive(Default)]
struct Pending {
    requests: HashMap<u64, PendingRequest>,
    closed: Option<String>,
}

/// Shared state between the client and its network handler.
struct ClientShared {
    pending: Mutex<Pending>,
    /// Condvar to notify waiting callers when a response arrives.
    notify: Condvar,
    /// Receive buffer for partial frame parsing.
    recv_buf: Mutex<Vec<u8>>,
}

impl ClientShared {
    fn pending(&self) -> std::sync::MutexGuard<'_, Pending> {
        self.pending.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Fails every waiting call and every later one: the connection is gone.
    fn fail_all(&self, reason: String) {
        let mut pending = self.pending();
        for req in pending.requests.values_mut() {
            if !req.completed {
                req.completed = true;
                req.result = Some(Err(RpcError::unavailable(reason.clone())));
            }
        }
        pending.closed.get_or_insert(reason);
        drop(pending);
        self.recv_buf
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        self.notify.notify_all();
    }
}

/// RPC client that connects to a Mill-RPC server.
///
/// When the connection drops, calls waiting for a reply fail at once with
/// `RpcStatus::Unavailable`, and so do later calls.
pub struct RpcClient {
    _tcp_client: TcpClient<RpcClientHandler>,
    sender: ConnSender,
    shared: Arc<ClientShared>,
    next_request_id: AtomicU64,
    timeout: AtomicU64,
}

impl RpcClient {
    /// Connect to an RPC server.
    pub fn connect(addr: SocketAddr, event_loop: &Arc<EventLoop>) -> Result<Arc<Self>, RpcError> {
        let shared = Arc::new(ClientShared {
            pending: Mutex::new(Pending::default()),
            notify: Condvar::new(),
            recv_buf: Mutex::new(Vec::new()),
        });

        let handler = RpcClientHandler {
            shared: shared.clone(),
        };

        let mut tcp_client = TcpClient::connect(addr, TcpClientConfig::default(), handler)
            .map_err(|e| RpcError::unavailable(format!("Connect failed: {}", e)))?;

        tcp_client
            .start(event_loop)
            .map_err(|e| RpcError::unavailable(format!("Client start failed: {}", e)))?;

        Ok(Arc::new(Self {
            sender: tcp_client.sender(),
            _tcp_client: tcp_client,
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

        // Register the pending request before sending, so a disconnect fails it.
        {
            let mut pending = self.shared.pending();
            if let Some(reason) = &pending.closed {
                return Err(RpcError::unavailable(reason.clone()));
            }
            pending.requests.insert(
                request_id,
                PendingRequest {
                    completed: false,
                    result: None,
                },
            );
        }

        let frame = Frame::request(request_id, service_id, method_id, payload, false);
        if let Err(e) = self.sender.send(&frame.encode()) {
            self.shared.pending().requests.remove(&request_id);
            return Err(send_error(e));
        }

        let mut pending = self.shared.pending();
        let deadline = std::time::Instant::now() + self.timeout();

        loop {
            match pending.requests.get(&request_id) {
                Some(req) if req.completed => {
                    return pending
                        .requests
                        .remove(&request_id)
                        .and_then(|req| req.result)
                        .unwrap_or_else(|| Err(RpcError::internal("Completed without a result")));
                }
                Some(_) => {}
                None => return Err(RpcError::internal("Pending request disappeared")),
            }

            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                pending.requests.remove(&request_id);
                return Err(RpcError::deadline_exceeded(format!(
                    "Request {} timed out after {:?}",
                    request_id,
                    self.timeout()
                )));
            }

            pending = self
                .shared
                .notify
                .wait_timeout(pending, remaining)
                .unwrap_or_else(|e| e.into_inner())
                .0;
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

        self.sender.send(&frame.encode()).map_err(send_error)
    }
}

fn send_error(e: SendError) -> RpcError {
    match e {
        SendError::Backpressure => {
            RpcError::new(RpcStatus::ResourceExhausted, "Send queue is full")
        }
        e => RpcError::unavailable(format!("Send failed: {}", e)),
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
        let frames = {
            let mut recv_buf = self
                .shared
                .recv_buf
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            recv_buf.extend_from_slice(data);
            // A bad frame leaves the stream unsynchronised; failing closes the connection,
            // which fails the pending calls.
            let (frames, consumed) = protocol::parse_frames(&recv_buf)
                .map_err(|e| format!("Client frame parse error: {}", e))?;
            recv_buf.drain(..consumed);
            frames
        };

        for frame in frames {
            self.handle_frame(frame);
        }

        Ok(())
    }

    fn on_disconnect(
        &self,
        _ctx: &ServerContext,
        _conn_id: ConnectionId,
    ) -> mill_net::errors::Result<()> {
        self.shared.fail_all("Connection closed".to_string());
        Ok(())
    }

    fn on_error(&self, _ctx: &ServerContext, _conn_id: Option<ConnectionId>, error: NetworkError) {
        log::error!("RPC client network error: {}", error);
        // A failed connect fires neither on_connect nor on_disconnect.
        if let NetworkError::Connect(e) = &error {
            self.shared.fail_all(format!("Connect failed: {}", e));
        }
    }
}

impl RpcClientHandler {
    fn complete(&self, request_id: u64, result: Result<Vec<u8>, RpcError>) {
        let mut pending = self.shared.pending();
        if let Some(req) = pending.requests.get_mut(&request_id) {
            if !req.completed {
                req.completed = true;
                req.result = Some(result);
            }
        }
        drop(pending);
        self.shared.notify.notify_all();
    }

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

                self.complete(request_id, Ok(data.to_vec()));
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

                self.complete(request_id, Err(rpc_err));
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
