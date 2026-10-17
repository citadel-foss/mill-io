//! Multi-service RPC server — two services on one port.
//!
//! Run with: cargo run --example multi_service_server
//! Then connect with: cargo run --example multi_service_client

use mill_io::EventLoop;
use mill_rpc::prelude::*;
use std::sync::Arc;

mill_rpc::service! {
    #[server]
    service MathService {
        fn factorial(n: u64) -> u64;
        fn fibonacci(n: u32) -> u64;
        fn is_prime(n: u64) -> bool;
        fn gcd(a: u64, b: u64) -> u64;
    }
}

mill_rpc::service! {
    #[server]
    service StringService {
        fn reverse(s: String) -> String;
        fn word_count(s: String) -> u32;
        fn contains(haystack: String, needle: String) -> bool;
    }
}

struct MathImpl;

impl math_service::Service for MathImpl {
    fn factorial(&self, _ctx: &RpcContext, n: u64) -> u64 {
        (1..=n).product()
    }

    fn fibonacci(&self, _ctx: &RpcContext, n: u32) -> u64 {
        match n {
            0 => 0,
            1 => 1,
            _ => {
                let (mut a, mut b) = (0u64, 1u64);
                for _ in 2..=n {
                    let tmp = a + b;
                    a = b;
                    b = tmp;
                }
                b
            }
        }
    }

    fn is_prime(&self, _ctx: &RpcContext, n: u64) -> bool {
        if n < 2 {
            return false;
        }
        if n < 4 {
            return true;
        }

        if n % 2 == 0 {
            return false;
        }
        let mut i = 3;
        while i * i <= n {
            if n % i == 0 {
                return false;
            }
            i += 2;
        }
        true
    }

    fn gcd(&self, _ctx: &RpcContext, mut a: u64, mut b: u64) -> u64 {
        while b != 0 {
            let tmp = b;
            b = a % b;
            a = tmp;
        }
        a
    }
}

struct StringImpl;

impl string_service::Service for StringImpl {
    fn reverse(&self, _ctx: &RpcContext, s: String) -> String {
        s.chars().rev().collect()
    }

    fn word_count(&self, _ctx: &RpcContext, s: String) -> u32 {
        s.split_whitespace().count() as u32
    }

    fn contains(&self, _ctx: &RpcContext, haystack: String, needle: String) -> bool {
        haystack.contains(&needle)
    }
}

fn main() {
    env_logger::init();
    let event_loop = Arc::new(EventLoop::new(4, 1024, 100).unwrap());

    let addr = "127.0.0.1:9004".parse().unwrap();
    let _server = RpcServer::builder()
        .bind(addr)
        .service(math_service::server(MathImpl)) // service_id = 0
        .service(string_service::server(StringImpl)) // service_id = 1
        .build(&event_loop)
        .expect("Failed to start multi-service server");

    println!("Multi-service server listening on {}", addr);
    println!("  Service 0: MathService");
    println!("  Service 1: StringService");
    event_loop.run().unwrap();
}
