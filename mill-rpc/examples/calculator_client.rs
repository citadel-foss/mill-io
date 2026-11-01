//! Calculator RPC client.
//!
//! Run the server first: cargo run --example calculator_server
//! Then run: cargo run --example calculator_client

use mill_io::EventLoop;
use mill_rpc::prelude::*;
use std::sync::Arc;

// Define the service — only generate client side
mill_rpc::service! {
    #[client]
    service Calculator {
        fn add(a: i32, b: i32) -> i32;
        fn subtract(a: i32, b: i32) -> i32;
        fn multiply(a: i64, b: i64) -> i64;
        fn divide(a: f64, b: f64) -> f64;
        fn negate(x: i32) -> i32;
    }
}

#[mill_io::main(workers = 2, poll_timeout_ms = 100)]
fn main(event_loop: &Arc<EventLoop>) {
    env_logger::init();

    let addr = "127.0.0.1:9001".parse().unwrap();
    let transport =
        RpcClient::connect(addr, event_loop).expect("Failed to connect to calculator server");

    let client = calculator::Client::new(transport, Codec::bincode(), 0);

    println!("Connected to calculator server");

    let sum = client.add(10, 25).unwrap();
    println!("10 + 25 = {}", sum);

    let diff = client.subtract(100, 42).unwrap();
    println!("100 - 42 = {}", diff);

    let product = client.multiply(7, 8).unwrap();
    println!("7 * 8 = {}", product);

    let quotient = client.divide(355.0, 113.0).unwrap();
    println!("355 / 113 = {:.6}", quotient);

    let neg = client.negate(42).unwrap();
    println!("negate(42) = {}", neg);

    let nan = client.divide(1.0, 0.0).unwrap();
    println!("1.0 / 0.0 = {} (NaN: {})", nan, nan.is_nan());

    println!("\nAll calculations completed!");
}
