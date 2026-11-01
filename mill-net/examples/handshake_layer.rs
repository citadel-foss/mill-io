//! A `Layer` added with `with_layer` on both a server and a client. Each side opens with a
//! hello (a magic tag and a one-byte key), then XORs what it sends with its own key.
//! The XOR is a stand-in for a real transform such as TLS; it hides nothing.
//!
//! `on_connect` fires only after both hellos are exchanged, a send made before that is held
//! until then, and a peer that skips the hello is dropped with `on_error` and never
//! reaches `on_connect`.
//!
//! Run with `cargo run -p mill-net --example handshake_layer`.

use mill_io::EventLoop;
use mill_net::errors::{NetworkError, Result};
use mill_net::tcp::{
    ConnectionId, Layer, NetworkHandler, ServerContext, TcpClient, TcpClientConfig, TcpServer,
    TcpServerConfig,
};
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

const MAGIC: &[u8; 4] = b"MILL";
const HELLO_LEN: usize = MAGIC.len() + 1;
const WAIT: Duration = Duration::from_secs(5);

struct XorHello {
    key: u8,
    peer_key: Option<u8>,
    hello: Vec<u8>,
}

impl XorHello {
    fn new(key: u8) -> Self {
        Self {
            key,
            peer_key: None,
            hello: Vec::with_capacity(HELLO_LEN),
        }
    }
}

impl Layer for XorHello {
    fn start(&mut self, out: &mut Vec<u8>) -> io::Result<()> {
        out.extend_from_slice(MAGIC);
        out.push(self.key);
        Ok(())
    }

    fn decode(&mut self, input: &[u8], up: &mut Vec<u8>, _out: &mut Vec<u8>) -> io::Result<()> {
        let mut input = input;
        if self.peer_key.is_none() {
            let take = (HELLO_LEN - self.hello.len()).min(input.len());
            self.hello.extend_from_slice(&input[..take]);
            input = &input[take..];
            if !MAGIC.starts_with(&self.hello[..self.hello.len().min(MAGIC.len())]) {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "bad hello"));
            }
            if self.hello.len() < HELLO_LEN {
                return Ok(());
            }
            self.peer_key = Some(self.hello[MAGIC.len()]);
        }
        if let Some(key) = self.peer_key {
            up.extend(input.iter().map(|byte| byte ^ key));
        }
        Ok(())
    }

    fn encode(&mut self, frame: &[u8], out: &mut Vec<u8>) -> io::Result<()> {
        out.extend(frame.iter().map(|byte| byte ^ self.key));
        Ok(())
    }

    fn is_ready(&self) -> bool {
        self.peer_key.is_some()
    }
}

struct ShoutingServer {
    errors: mpsc::Sender<String>,
}

impl NetworkHandler for ShoutingServer {
    fn on_connect(&self, _ctx: &ServerContext, conn: ConnectionId) -> Result<()> {
        println!("[server] {conn:?} finished the hello");
        Ok(())
    }

    fn on_data(&self, ctx: &ServerContext, conn: ConnectionId, data: &[u8]) -> Result<()> {
        println!("[server] got {:?}", String::from_utf8_lossy(data));
        ctx.send_to(conn, &data.to_ascii_uppercase())
    }

    fn on_error(&self, _ctx: &ServerContext, conn: Option<ConnectionId>, error: NetworkError) {
        println!("[server] dropped {conn:?}: {error}");
        let _ = self.errors.send(error.to_string());
    }
}

struct Client {
    replies: mpsc::Sender<String>,
}

impl NetworkHandler for Client {
    fn on_connect(&self, _ctx: &ServerContext, conn: ConnectionId) -> Result<()> {
        println!("[client] {conn:?} finished the hello");
        Ok(())
    }

    fn on_data(&self, _ctx: &ServerContext, _conn: ConnectionId, data: &[u8]) -> Result<()> {
        let reply = String::from_utf8_lossy(data).into_owned();
        println!("[client] got {reply:?}");
        let _ = self.replies.send(reply);
        Ok(())
    }
}

fn read_raw(stream: &mut TcpStream, len: usize) -> io::Result<Vec<u8>> {
    let mut bytes = vec![0; len];
    stream.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn main() -> Result<()> {
    let event_loop = Arc::new(EventLoop::default());
    let (errors, server_errors) = mpsc::channel();
    let server = Arc::new(
        TcpServer::new(
            TcpServerConfig::builder()
                .address("127.0.0.1:0".parse()?)
                .build(),
            ShoutingServer { errors },
        )?
        .with_layer(|| Box::new(XorHello::new(0x5a))),
    );
    Arc::clone(&server).start(&event_loop, event_loop.alloc_token())?;
    let addr = server.local_addr()?;

    let runner = {
        let event_loop = Arc::clone(&event_loop);
        thread::spawn(move || event_loop.run().expect("event loop failed"))
    };

    let (replies, client_replies) = mpsc::channel();
    let mut client = TcpClient::connect(addr, TcpClientConfig::default(), Client { replies })?
        .with_layer(XorHello::new(0xa5))?;
    client.send(b"sent before the hello finished")?;
    client.start(&event_loop)?;
    let reply = client_replies.recv_timeout(WAIT)?;
    assert_eq!(reply, "SENT BEFORE THE HELLO FINISHED");

    let mut raw = TcpStream::connect(addr)?;
    raw.set_read_timeout(Some(WAIT))?;
    let hello = read_raw(&mut raw, HELLO_LEN)?;
    println!("[raw] server hello on the wire: {hello:02x?}");
    raw.write_all(b"MILL\x01")?;
    raw.write_all(&b"ok".map(|byte| byte ^ 0x01))?;
    let wire = read_raw(&mut raw, 2)?;
    let key = hello[MAGIC.len()];
    let plain: Vec<u8> = wire.iter().map(|byte| byte ^ key).collect();
    println!(
        "[raw] reply on the wire {wire:02x?}, decoded {:?}",
        String::from_utf8_lossy(&plain)
    );
    assert_eq!(plain, b"OK");

    let mut plain_peer = TcpStream::connect(addr)?;
    plain_peer.write_all(b"GET / HTTP/1.0\r\n\r\n")?;
    let error = server_errors.recv_timeout(WAIT)?;
    assert!(error.contains("bad hello"), "{error}");

    drop(client);
    server.stop()?;
    event_loop.stop();
    runner.join().expect("event loop thread panicked");
    Ok(())
}
