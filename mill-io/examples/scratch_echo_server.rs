use mill_io::{error::Result, EventHandler, EventLoop, ObjectPool, PooledObject};
use mio::{
    event::Event,
    net::{TcpListener, TcpStream},
    Interest, Token,
};
use std::{
    collections::HashMap,
    io::{self, Read, Write},
    net::SocketAddr,
    sync::{Arc, Mutex, OnceLock},
};

static EVENT_LOOP: OnceLock<EventLoop> = OnceLock::new();
fn event_loop() -> &'static EventLoop {
    EVENT_LOOP.get_or_init(EventLoop::default)
}
const LISTENER: Token = Token(1);

pub struct EchoServerHandler {
    listener: Arc<Mutex<TcpListener>>,
    connections: Arc<Mutex<HashMap<Token, TcpStream>>>,
    buffer_pool: ObjectPool<Vec<u8>>,
}

impl EchoServerHandler {
    pub fn new(listener: Arc<Mutex<TcpListener>>) -> Result<Self> {
        Ok(EchoServerHandler {
            listener,
            connections: Arc::new(Mutex::new(HashMap::new())),
            buffer_pool: ObjectPool::new(10, || vec![0; 8192]),
        })
    }

    fn handle_listener_event(
        &self,
        connections: Arc<Mutex<HashMap<Token, TcpStream>>>,
        buffer_pool: &ObjectPool<Vec<u8>>,
    ) -> Result<()> {
        loop {
            println!("handling listener");
            match self.listener.lock().unwrap().accept() {
                Ok((mut stream, _)) => {
                    println!("handling stream: addr={:#?}", stream.local_addr());
                    let token = event_loop().alloc_token();
                    let connections_clone = connections.clone();
                    let buffer_pool_clone = buffer_pool.clone();

                    println!("register new event: token={token:?}");

                    event_loop().register(
                        &mut stream,
                        token,
                        Interest::READABLE | Interest::WRITABLE,
                        ClientHandler {
                            connections: connections_clone,
                            token,
                            buffer_pool: buffer_pool_clone,
                        },
                    )?;

                    connections.lock().unwrap().insert(token, stream);
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    // No more pending connections
                    break;
                }
                Err(e) => {
                    println!("error: {e:?}");
                    return Err(e.into());
                }
            }
        }
        Ok(())
    }
}

impl EventHandler for EchoServerHandler {
    fn handle_event(&self, event: &Event) {
        if event.token() == LISTENER {
            if let Err(e) = self.handle_listener_event(self.connections.clone(), &self.buffer_pool)
            {
                eprintln!("error accepting connection: {}", e);
            }
            println!(
                "new Connection: len={:#?}",
                self.connections.lock().unwrap().len()
            );
        }
    }
}

pub struct ClientHandler {
    connections: Arc<Mutex<HashMap<Token, TcpStream>>>,
    token: Token,
    buffer_pool: ObjectPool<Vec<u8>>,
}

impl EventHandler for ClientHandler {
    fn handle_event(&self, event: &Event) {
        let mut connections = self.connections.lock().unwrap();
        if let Some(stream) = connections.get_mut(&self.token) {
            if event.is_readable() {
                let mut buffer: PooledObject<Vec<u8>> = self.buffer_pool.acquire();
                match stream.read(buffer.as_mut()) {
                    Ok(0) => {
                        println!("client disconnected: {:?}", self.token);
                        if let Some(mut disconnected_stream) = connections.remove(&self.token) {
                            if let Err(e) =
                                event_loop().deregister(&mut disconnected_stream, self.token)
                            {
                                eprintln!("Failed to deregister client {:?}: {}", self.token, e);
                            }
                        }
                    }
                    Ok(n) => {
                        println!(
                            "Received {} bytes from client {:?}: {:?}",
                            n,
                            self.token,
                            String::from_utf8_lossy(&buffer.as_ref()[..n])
                        );
                        if let Err(e) = stream.write_all(&buffer.as_ref()[..n]) {
                            eprintln!("Error writing to client {:?}: {}", self.token, e);
                            if let Some(mut disconnected_stream) = connections.remove(&self.token) {
                                if let Err(e) =
                                    event_loop().deregister(&mut disconnected_stream, self.token)
                                {
                                    eprintln!(
                                        "Failed to deregister client {:?}: {}",
                                        self.token, e
                                    );
                                }
                            }
                        }
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        // This is expected for non-blocking I/O
                    }
                    Err(e) => {
                        eprintln!("Error reading from client {:?}: {}", self.token, e);
                        if let Some(mut disconnected_stream) = connections.remove(&self.token) {
                            if let Err(e) =
                                event_loop().deregister(&mut disconnected_stream, self.token)
                            {
                                eprintln!("Failed to deregister client {:?}: {}", self.token, e);
                            }
                        }
                    }
                }
            }
        }
    }
}

fn main() -> Result<()> {
    let addr: SocketAddr = "127.0.0.1:8080".parse()?;
    let listener = TcpListener::bind(addr)?;
    let listener = Arc::new(Mutex::new(listener));

    let server_handler = EchoServerHandler::new(Arc::clone(&listener))?;

    event_loop().register::<EchoServerHandler, TcpListener>(
        &mut listener.lock().unwrap(),
        LISTENER,
        Interest::READABLE,
        server_handler,
    )?;

    println!("Echo server listening on {}", addr);
    event_loop().run()?;

    Ok(())
}
