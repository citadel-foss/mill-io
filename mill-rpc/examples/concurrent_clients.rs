//! Concurrent clients stress test.
//!
//! Run with: cargo run --example concurrent_clients

use mill_io::EventLoop;
use mill_rpc::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

// Both sides in one binary
mill_rpc::service! {
    service Counter {
        fn increment() -> u64;
        fn get() -> u64;
    }
}

struct AtomicCounter {
    value: AtomicU64,
}

impl counter::Service for AtomicCounter {
    fn increment(&self, _ctx: &RpcContext) -> u64 {
        self.value.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn get(&self, _ctx: &RpcContext) -> u64 {
        self.value.load(Ordering::SeqCst)
    }
}

#[mill_io::main(workers = 4, compute_workers = 4, poll_timeout_ms = 100)]
fn main(event_loop: &Arc<EventLoop>) {
    env_logger::init();

    let num_clients = 4;
    let requests_per_client = 100;

    let addr = "127.0.0.1:9005".parse().unwrap();
    let _server = RpcServer::builder()
        .bind(addr)
        .service(counter::server(AtomicCounter {
            value: AtomicU64::new(0),
        }))
        .build(event_loop)
        .expect("Failed to start server");

    println!(
        "Spawning {} clients, {} requests each...\n",
        num_clients, requests_per_client
    );

    let start = Instant::now();
    let clients: Vec<_> = (0..num_clients)
        .map(|client_id| {
            let transport = RpcClient::connect(addr, event_loop).unwrap();
            // Each client blocks on its calls, so it runs as a compute task, not on the reactor.
            mill_io::spawn(move || {
                let client = counter::Client::new(transport, Codec::bincode(), 0);
                let results: Vec<u64> = (0..requests_per_client)
                    .map(|_| client.increment().unwrap())
                    .collect();
                println!(
                    "  Client {} done: first={}, last={}",
                    client_id,
                    results.first().unwrap(),
                    results.last().unwrap()
                );
                results
            })
        })
        .collect();

    let mut all: Vec<u64> = clients
        .into_iter()
        .flat_map(|client| client.join().unwrap())
        .collect();

    let elapsed = start.elapsed();
    all.sort();
    all.dedup();

    let total = (num_clients * requests_per_client) as usize;
    println!("\n--- Results ---");
    println!("Total requests: {}", total);
    println!("Unique values:  {}", all.len());
    println!("Time:           {:?}", elapsed);
    println!(
        "Throughput:     {:.0} req/s",
        total as f64 / elapsed.as_secs_f64()
    );

    assert_eq!(all.len(), total, "No lost updates");
    println!("\nConcurrency test passed!");
}
