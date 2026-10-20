use mill_net::errors::Result;
use mill_net::tcp::config::TcpServerConfig;
use mill_net::tcp::{
    ConnectionId, NetworkHandler, ServerContext, TcpClient, TcpClientConfig, TcpServer,
};
use parking_lot::Mutex;
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

mod common;

use common::{pool_and_direct, spawn_loop, stop_loop, wait_until};

const MESSAGE_LEN: usize = 4 * 1024 * 1024;

fn message() -> Vec<u8> {
    (0..MESSAGE_LEN).map(|i| (i % 251) as u8).collect()
}

#[derive(Clone, Default)]
struct Collector {
    received: Arc<Mutex<Vec<u8>>>,
}

impl NetworkHandler for Collector {
    fn on_data(&self, _ctx: &ServerContext, _conn: ConnectionId, data: &[u8]) -> Result<()> {
        self.received.lock().extend_from_slice(data);
        Ok(())
    }
}

#[test]
fn single_4mb_message_without_followup() {
    for (mode, event_loop) in pool_and_direct() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let expected = message();

        let sent = expected.clone();
        let server = thread::spawn(move || {
            let (mut peer, _) = listener.accept().unwrap();
            peer.write_all(&sent).unwrap();
            // Returned so it stays open with nothing more to send: only the bytes already
            // written may wake the client.
            peer
        });

        let collector = Collector::default();
        let received = Arc::clone(&collector.received);
        let mut client = TcpClient::connect(addr, TcpClientConfig::default(), collector).unwrap();
        client.start(&event_loop).unwrap();
        let runner = spawn_loop(&event_loop);

        let started = Instant::now();
        let complete = wait_until(Duration::from_secs(2), || {
            received.lock().len() >= MESSAGE_LEN
        });
        let elapsed = started.elapsed();
        let got = received.lock().len();
        assert!(
            complete,
            "{mode}: client received {got} of {MESSAGE_LEN} bytes within 2 s"
        );
        assert!(
            *received.lock() == expected,
            "{mode}: bytes differ after {elapsed:?}"
        );

        let peer = server.join().unwrap();
        stop_loop(&event_loop, runner);
        drop((client, peer));
    }
}

#[test]
fn server_reads_4mb_without_followup() {
    for (mode, event_loop) in pool_and_direct() {
        let collector = Collector::default();
        let received = Arc::clone(&collector.received);
        let config = TcpServerConfig::builder()
            .address("127.0.0.1:0".parse().unwrap())
            .build();
        let server = Arc::new(TcpServer::new(config, collector).unwrap());
        let addr = server.local_addr().unwrap();
        Arc::clone(&server)
            .start(&event_loop, event_loop.alloc_token())
            .unwrap();
        let runner = spawn_loop(&event_loop);

        let expected = message();
        let sent = expected.clone();
        let writer = thread::spawn(move || {
            let mut peer = TcpStream::connect(addr).unwrap();
            peer.write_all(&sent).unwrap();
            peer
        });

        let complete = wait_until(Duration::from_secs(2), || {
            received.lock().len() >= MESSAGE_LEN
        });
        let got = received.lock().len();
        assert!(
            complete,
            "{mode}: server received {got} of {MESSAGE_LEN} bytes within 2 s"
        );
        assert!(*received.lock() == expected, "{mode}: bytes differ");

        let peer = writer.join().unwrap();
        stop_loop(&event_loop, runner);
        drop(peer);
    }
}

#[derive(Clone, Default)]
struct ChunkSizes {
    largest: Arc<Mutex<usize>>,
    total: Arc<Mutex<usize>>,
}

impl NetworkHandler for ChunkSizes {
    fn on_data(&self, _ctx: &ServerContext, _conn: ConnectionId, data: &[u8]) -> Result<()> {
        let mut largest = self.largest.lock();
        *largest = (*largest).max(data.len());
        *self.total.lock() += data.len();
        Ok(())
    }
}

#[test]
fn buffer_size_sets_the_read_chunk() {
    for buffer_size in [1024, 32 * 1024] {
        let event_loop = Arc::new(mill_io::EventLoop::new(2, 1024, 10).unwrap());
        let sizes = ChunkSizes::default();
        let (largest, total) = (Arc::clone(&sizes.largest), Arc::clone(&sizes.total));
        let config = TcpServerConfig::builder()
            .address("127.0.0.1:0".parse().unwrap())
            .buffer_size(buffer_size)
            .build();
        let server = Arc::new(TcpServer::new(config, sizes).unwrap());
        let addr = server.local_addr().unwrap();
        Arc::clone(&server)
            .start(&event_loop, event_loop.alloc_token())
            .unwrap();
        let runner = spawn_loop(&event_loop);

        let writer = thread::spawn(move || {
            let mut peer = TcpStream::connect(addr).unwrap();
            peer.write_all(&message()).unwrap();
            peer
        });
        assert!(wait_until(Duration::from_secs(5), || *total.lock() == MESSAGE_LEN));
        let largest = *largest.lock();
        assert!(
            largest <= buffer_size,
            "a {largest}-byte chunk with buffer_size {buffer_size}"
        );
        if buffer_size > 8192 {
            assert!(
                largest > 8192,
                "buffer_size {buffer_size} still read at most {largest} bytes at a time"
            );
        }

        let peer = writer.join().unwrap();
        stop_loop(&event_loop, runner);
        drop((peer, server));
    }
}
