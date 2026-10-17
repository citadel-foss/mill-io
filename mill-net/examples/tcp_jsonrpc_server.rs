#![allow(clippy::new_without_default)]

use lock_freedom::map::Map;
use mill_io::error::Result;
use mill_io::EventLoop;
use mill_net::errors::{NetworkError, NetworkEvent};
use mill_net::tcp::{config::TcpServerConfig, traits::*, ConnectionId, ServerContext, TcpServer};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Serialize, Deserialize, Debug)]
pub enum RpcRequest {
    Ping,
    Echo { message: String },
    Add { a: i32, b: i32 },
    GetTime,
    SetValue { key: String, value: String },
    GetValue { key: String },
    ListKeys,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum RpcResponse {
    Pong,
    Echo { message: String },
    Sum { result: i32 },
    Time { timestamp: u64 },
    ValueSet { key: String },
    Value { key: String, value: Option<String> },
    Keys { keys: Vec<String> },
    Error { message: String },
}

type DataStore = Arc<Map<String, String>>;

pub struct RpcHandler {
    data_store: DataStore,
}

impl RpcHandler {
    pub fn new() -> Self {
        Self {
            data_store: Arc::new(Map::new()),
        }
    }

    fn handle_rpc_request(&self, data: &[u8]) -> RpcResponse {
        match serde_json::from_slice::<RpcRequest>(data) {
            Ok(request) => {
                println!("[INFO] Received RPC request: {:?}", request);
                self.process_request(request)
            }
            Err(e) => RpcResponse::Error {
                message: format!("Invalid JSON: {}", e),
            },
        }
    }

    fn process_request(&self, request: RpcRequest) -> RpcResponse {
        match request {
            RpcRequest::Ping => RpcResponse::Pong,

            RpcRequest::Echo { message } => RpcResponse::Echo { message },

            RpcRequest::Add { a, b } => RpcResponse::Sum { result: a + b },

            RpcRequest::GetTime => RpcResponse::Time {
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            },

            RpcRequest::SetValue { key, value } => {
                self.data_store.insert(key.clone(), value);
                RpcResponse::ValueSet { key }
            }

            RpcRequest::GetValue { key } => match self.data_store.get(&key) {
                Some(v) => RpcResponse::Value {
                    value: Some(v.val().to_string()),
                    key,
                },
                None => RpcResponse::Value { value: None, key },
            },

            RpcRequest::ListKeys => RpcResponse::Keys {
                keys: self
                    .data_store
                    .iter()
                    .map(|e| e.key().to_string())
                    .collect(),
            },
        }
    }
}

impl NetworkHandler for RpcHandler {
    fn on_event(&self, _ctx: &ServerContext, event: NetworkEvent) -> Result<()> {
        match event {
            NetworkEvent::ConnectionEstablished(conn_id, addr) => {
                println!("[INFO] New RPC connection: {:?} from {}", conn_id, addr);
            }
            NetworkEvent::ConnectionClosed(conn_id) => {
                println!("[INFO] RPC connection closed: {:?}", conn_id);
            }
            NetworkEvent::Listening(addr) => {
                println!("[INFO] RPC Server listening on {}", addr);
            }
        }
        Ok(())
    }

    fn on_data(&self, ctx: &ServerContext, conn_id: ConnectionId, data: &[u8]) -> Result<()> {
        let response = self.handle_rpc_request(data);

        match serde_json::to_vec(&response) {
            Ok(response_data) => {
                ctx.send_to(conn_id, &response_data)?;
            }
            Err(e) => {
                eprintln!("[ERROR] Failed to serialize response: {}", e);
            }
        }
        Ok(())
    }

    fn on_error(&self, _ctx: &ServerContext, conn_id: Option<ConnectionId>, error: NetworkError) {
        if let Some(id) = conn_id {
            eprintln!("[ERROR] Error on connection {:?}: {}", id, error);
        } else {
            eprintln!("[ERROR] Server error: {}", error);
        }
    }
}

fn main() -> Result<()> {
    let event_loop = Arc::new(EventLoop::default());

    let config = TcpServerConfig::builder()
        .address("127.0.0.1:8080".parse().unwrap())
        .build();

    let handler = RpcHandler::new();
    let server = Arc::new(TcpServer::new(config.clone(), handler)?);

    server.start(&event_loop, event_loop.alloc_token())?;

    println!("[INFO] RPC Server starting on {}...", config.address);
    println!("Available RPC methods:");
    println!("  Ping                                   - Simple ping/pong");
    println!("  Echo {{\"message\": \"hello\"}}              - Echo a message");
    println!("  Add {{\"a\": 5, \"b\": 3}}                   - Add two numbers");
    println!("  GetTime                                - Get current timestamp");
    println!("  SetValue {{\"key\": \"k\", \"value\": \"v\"}}   - Set key-value");
    println!("  GetValue {{\"key\": \"k\"}}                 - Get value by key");
    println!("  ListKeys                               - List all keys");
    println!();

    event_loop.run()?;

    Ok(())
}
