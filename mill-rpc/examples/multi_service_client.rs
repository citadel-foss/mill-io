//! Multi-service RPC client — calls two services on one server.
//!
//! Run the server first: cargo run --example multi_service_server
//! Then run: cargo run --example multi_service_client

use mill_io::EventLoop;
use mill_rpc::prelude::*;
use std::sync::Arc;

mill_rpc::service! {
    #[client]
    service MathService {
        fn factorial(n: u64) -> u64;
        fn fibonacci(n: u32) -> u64;
        fn is_prime(n: u64) -> bool;
        fn gcd(a: u64, b: u64) -> u64;
    }
}

mill_rpc::service! {
    #[client]
    service StringService {
        fn reverse(s: String) -> String;
        fn word_count(s: String) -> u32;
        fn contains(haystack: String, needle: String) -> bool;
    }
}

#[mill_io::main(workers = 2, poll_timeout_ms = 100)]
fn main(event_loop: &Arc<EventLoop>) {
    env_logger::init();

    let addr = "127.0.0.1:9004".parse().unwrap();
    let transport = RpcClient::connect(addr, event_loop).expect("Failed to connect");

    let math = math_service::Client::new(transport.clone(), Codec::bincode(), 0);
    let strings = string_service::Client::new(transport, Codec::bincode(), 1);

    println!("=== Math Service ===\n");

    let f = math.factorial(10).unwrap();
    println!("10! = {}", f);

    let fib = math.fibonacci(20).unwrap();
    println!("fib(20) = {}", fib);

    for n in [2, 7, 15, 17] {
        let prime = math.is_prime(n).unwrap();
        println!("is_prime({}) = {}", n, prime);
    }

    let g = math.gcd(48, 18).unwrap();
    println!("gcd(48, 18) = {}", g);

    println!("\n=== String Service ===\n");

    let rev = strings.reverse("Hello, World!".into()).unwrap();
    println!("reverse(\"Hello, World!\") = {:?}", rev);

    let wc = strings.word_count("The quick brown fox".into()).unwrap();
    println!("word_count = {}", wc);

    let has = strings.contains("rustacean".into(), "rust".into()).unwrap();
    println!("contains(\"rustacean\", \"rust\") = {}", has);

    println!("\nAll multi-service tests passed!");
}
