//! Echo RPC client.
//!
//! Run the server first: cargo run --example echo_server
//! Then run: cargo run --example echo_client

use mill_io::EventLoop;
use mill_rpc::prelude::*;
use std::sync::Arc;

mill_rpc::service! {
    #[client]
    service Echo {
        fn echo(message: String) -> String;
        fn echo_uppercase(message: String) -> String;
        fn echo_repeat(message: String, times: u32) -> String;
        fn request_count() -> u64;
    }
}

#[mill_io::main(workers = 2, poll_timeout_ms = 100)]
fn main(event_loop: &Arc<EventLoop>) {
    env_logger::init();

    let addr = "127.0.0.1:9002".parse().unwrap();
    let transport = RpcClient::connect(addr, event_loop).expect("Failed to connect to echo server");

    let client = echo::Client::new(transport, Codec::bincode(), 0);

    // Basic echo
    let reply = client.echo("Hello, Mill-RPC!".into()).unwrap();
    println!("echo: {}", reply);
    assert_eq!(reply, "Hello, Mill-RPC!");

    // Uppercase
    let reply = client.echo_uppercase("hello world".into()).unwrap();
    println!("uppercase: {}", reply);
    assert_eq!(reply, "HELLO WORLD");

    // Repeat
    let reply = client.echo_repeat("ha".into(), 3).unwrap();
    println!("repeat: {}", reply);
    assert_eq!(reply, "hahaha");

    // Request count
    let count = client.request_count().unwrap();
    println!("server handled {} requests", count);
    assert_eq!(count, 3); // echo + uppercase + repeat

    println!("\nAll echo tests passed!");
}
