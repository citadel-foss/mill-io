use mill_io::{error::Result, EventHandler, EventLoop, ObjectPool, PooledObject};
use mio::{event::Event, net::TcpListener, Interest, Token};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    io::{Read, Write},
    net::SocketAddr,
    sync::{Arc, Mutex, OnceLock},
    time::SystemTime,
};

use lock_freedom::map::Map;

static EVENT_LOOP: OnceLock<EventLoop> = OnceLock::new();
fn event_loop() -> &'static EventLoop {
    EVENT_LOOP.get_or_init(EventLoop::default)
}

const LISTENER_TOKEN: Token = Token(1);
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

pub struct RpcServer {
    listener: Arc<Mutex<TcpListener>>,
    connections: Arc<Mutex<HashMap<Token, mio::net::TcpStream>>>,
    buffer_pool: ObjectPool<Vec<u8>>,
    data_store: DataStore,
}

impl RpcServer {
    pub fn new(listener: Arc<Mutex<TcpListener>>) -> Result<Self> {
        Ok(Self {
            listener,
            connections: Arc::new(Mutex::new(HashMap::new())),
            buffer_pool: ObjectPool::new(10, || vec![0; 4096]),
            data_store: DataStore::default(),
        })
    }

    fn accept_connections(&self) -> Result<()> {
        loop {
            match self.listener.lock().unwrap().accept() {
                Ok((mut stream, addr)) => {
                    println!("[INFO] New RPC connection from: {}", addr);

                    let token = event_loop().alloc_token();

                    let before = SystemTime::now();
                    event_loop().register(
                        &mut stream,
                        token,
                        Interest::READABLE,
                        RpcClient::new(
                            token,
                            self.connections.clone(),
                            self.buffer_pool.clone(),
                            self.data_store.clone(),
                        ),
                    )?;

                    let after = SystemTime::now();
                    let duration = after.duration_since(before).unwrap_or_default();
                    println!(
                        "[INFO] registered new client with token: {:?}, time: {}ms",
                        token,
                        duration.as_millis()
                    );
                    self.connections.lock().unwrap().insert(token, stream);
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    break;
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }
}

impl EventHandler for RpcServer {
    fn handle_event(&self, event: &Event) {
        if event.token() == LISTENER_TOKEN && event.is_readable() {
            if let Err(e) = self.accept_connections() {
                eprintln!("[ERROR] Couldn't accept connections: {}", e);
            }
        }
    }
}

// RPC Client handler
pub struct RpcClient {
    token: Token,
    connections: Arc<Mutex<HashMap<Token, mio::net::TcpStream>>>,
    buffer_pool: ObjectPool<Vec<u8>>,
    data_store: DataStore,
}

impl RpcClient {
    fn new(
        token: Token,
        connections: Arc<Mutex<HashMap<Token, mio::net::TcpStream>>>,
        buffer_pool: ObjectPool<Vec<u8>>,
        data_store: DataStore,
    ) -> Self {
        Self {
            token,
            connections,
            buffer_pool,
            data_store,
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

    fn disconnect(&self) {
        println!("[INFO] RPC client disconnected: {:?}", self.token);
        if let Ok(mut connections) = self.connections.lock() {
            if let Some(mut stream) = connections.remove(&self.token) {
                if let Err(e) = event_loop().deregister(&mut stream, self.token) {
                    eprintln!(
                        "[ERROR] Failed to deregister client {:?}: {}",
                        self.token, e
                    );
                }
            }
        }
    }
}

impl EventHandler for RpcClient {
    fn handle_event(&self, event: &Event) {
        println!("[INFO] handling event for client: {:?}", self.token);
        if !event.is_readable() {
            return;
        }

        let mut connections = match self.connections.lock() {
            Ok(conn) => conn,
            Err(_) => return,
        };

        let stream = match connections.get_mut(&self.token) {
            Some(s) => s,
            None => return,
        };

        let mut buffer: PooledObject<Vec<u8>> = self.buffer_pool.acquire();

        match stream.read(buffer.as_mut()) {
            Ok(0) => {
                drop(connections);
                self.disconnect();
            }
            Ok(bytes_read) => {
                let response = self.handle_rpc_request(&buffer.as_ref()[..bytes_read]);

                match serde_json::to_vec(&response) {
                    Ok(response_data) => {
                        if let Err(e) = stream.write_all(&response_data) {
                            eprintln!("[Error] Couldn't writing response: {}", e);
                            drop(connections);
                            self.disconnect();
                        }
                    }
                    Err(e) => {
                        eprintln!("[Error] Couldn't serializing response: {}", e);
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // Expected for non-blocking I/O
            }
            Err(e) => {
                eprintln!(
                    "[Error] Couldn't reading from client {:?}: {}",
                    self.token, e
                );
                drop(connections);
                self.disconnect();
            }
        }
    }
}

fn main() -> Result<()> {
    let addr: SocketAddr = "127.0.0.1:8080".parse()?;
    let listener = mio::net::TcpListener::bind(addr)?;
    let listener = Arc::new(Mutex::new(listener));

    println!("[INFO]RPC Server starting on {}...", addr);
    println!("Available RPC methods:");
    println!("  ping                                   - Simple ping/pong");
    println!("  echo {{\"message\": \"hello\"}}              - Echo a message");
    println!("  add {{\"a\": 5, \"b\": 3}}                   - Add two numbers");
    println!("  get_time                               - Get current timestamp");
    println!("  set_value {{\"key\": \"k\", \"value\": \"v\"}}   - Set key-value");
    println!("  get_value {{\"key\": \"k\"}}                 - Get value by key");
    println!("  list_keys                              - List all keys");
    println!();

    let server = RpcServer::new(Arc::clone(&listener))?;

    event_loop().register::<RpcServer, TcpListener>(
        &mut listener.lock().unwrap(),
        LISTENER_TOKEN,
        Interest::READABLE,
        server,
    )?;

    println!("[INFO] Server listening on {}", addr);
    event_loop().run()?;

    Ok(())
}
