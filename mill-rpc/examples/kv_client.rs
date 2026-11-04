//! Key-value store RPC client.
//!
//! Run the server first: cargo run --example kv_server
//! Then run: cargo run --example kv_client

use mill_io::EventLoop;
use mill_rpc::prelude::*;
use std::sync::Arc;

mill_rpc::service! {
    #[client]
    service KeyValue {
        fn get(key: String) -> Option<String>;
        fn set(key: String, value: String) -> Option<String>;
        fn delete(key: String) -> bool;
        fn keys() -> Vec<String>;
        fn len() -> u64;
        fn is_empty() -> bool;
        fn clear() -> u64;
    }
}

#[mill_io::main(workers = 2, poll_timeout_ms = 100)]
fn main(event_loop: &Arc<EventLoop>) {
    env_logger::init();

    let addr = "127.0.0.1:9003".parse().unwrap();
    let transport = RpcClient::connect(addr, event_loop).expect("Failed to connect to KV server");

    let kv = key_value::Client::new(transport, Codec::bincode(), 0);

    println!("=== Key-Value Store Client ===\n");

    let len = kv.len().unwrap();
    println!("Initial store size: {}", len);

    kv.set("name".into(), "Alice".into()).unwrap();
    println!("SET name=Alice");

    kv.set("city".into(), "Berlin".into()).unwrap();
    println!("SET city=Berlin");

    kv.set("lang".into(), "Rust".into()).unwrap();
    println!("SET lang=Rust");

    let val = kv.get("name".into()).unwrap();
    println!("GET name -> {:?}", val);

    let val = kv.get("missing".into()).unwrap();
    println!("GET missing -> {:?}", val);

    let mut keys = kv.keys().unwrap();
    keys.sort();
    println!("KEYS -> {:?}", keys);

    let old = kv.set("name".into(), "Bob".into()).unwrap();
    println!("SET name=Bob (old: {:?})", old);

    let existed = kv.delete("city".into()).unwrap();
    println!("DEL city -> existed: {}", existed);

    let len = kv.len().unwrap();
    println!("Store size: {}", len);

    let removed = kv.clear().unwrap();
    println!("CLEAR -> removed {} entries", removed);

    println!("\nAll KV tests passed!");
}
