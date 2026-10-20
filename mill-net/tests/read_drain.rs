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
use std::time::Instant;

use mill_io::EventLoop;

mod common;

use common::{socket_report, spawn_loop, stop_loop, wait_for_progress};

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

#[mill_io::test(modes = "both", workers = 2, poll_timeout_ms = 10)]
fn single_4mb_message_without_followup(event_loop: &Arc<EventLoop>) {
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
    client.start(event_loop).unwrap();

    let started = Instant::now();
    if let Err(stall) = wait_for_progress(MESSAGE_LEN, || received.lock().len()) {
        panic!(
            "client bytes stalled: {stall}; sockets:\n{}",
            socket_report(addr.port())
        );
    }
    let elapsed = started.elapsed();
    assert!(
        *received.lock() == expected,
        "bytes differ after {elapsed:?}"
    );

    let _peer = server.join().unwrap();
}

#[mill_io::test(modes = "both", workers = 2, poll_timeout_ms = 10)]
fn server_reads_4mb_without_followup(event_loop: &Arc<EventLoop>) {
    let collector = Collector::default();
    let received = Arc::clone(&collector.received);
    let config = TcpServerConfig::builder()
        .address("127.0.0.1:0".parse().unwrap())
        .build();
    let server = Arc::new(TcpServer::new(config, collector).unwrap());
    let addr = server.local_addr().unwrap();
    Arc::clone(&server)
        .start(event_loop, event_loop.alloc_token())
        .unwrap();

    let expected = message();
    let sent = expected.clone();
    let writer = thread::spawn(move || {
        let mut peer = TcpStream::connect(addr).unwrap();
        peer.write_all(&sent).unwrap();
        peer
    });

    if let Err(stall) = wait_for_progress(MESSAGE_LEN, || received.lock().len()) {
        panic!(
            "server bytes stalled: {stall}; sockets:\n{}",
            socket_report(addr.port())
        );
    }
    assert!(*received.lock() == expected, "bytes differ");

    let _peer = writer.join().unwrap();
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
        if let Err(stall) = wait_for_progress(MESSAGE_LEN, || *total.lock()) {
            panic!(
                "buffer_size {buffer_size}: bytes stalled: {stall}; sockets:\n{}",
                socket_report(addr.port())
            );
        }
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
