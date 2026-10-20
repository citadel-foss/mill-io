use mill_io::EventLoop;
use mill_rpc::prelude::*;
use std::{net::SocketAddr, sync::Arc, thread, time::Duration};

mill_rpc::service! {
    service Arith {
        fn add(a: u64, b: u64) -> u64;
    }
}

struct Adder;

impl arith::Service for Adder {
    fn add(&self, _ctx: &RpcContext, a: u64, b: u64) -> u64 {
        a + b
    }
}

#[test]
fn two_clients_share_one_loop() {
    const CALLS: u64 = 1000;

    let event_loop = Arc::new(EventLoop::new(4, 1024, 10).unwrap());
    let server = RpcServer::builder()
        .bind("127.0.0.1:0".parse().unwrap())
        .service(arith::server(Adder))
        .build(&event_loop)
        .unwrap();
    let addr = server.local_addr().unwrap();
    let runner = {
        let event_loop = Arc::clone(&event_loop);
        thread::spawn(move || event_loop.run().map_err(|e| e.to_string()))
    };

    let transports: Vec<Arc<RpcClient>> = (0..2)
        .map(|_| RpcClient::connect(addr, &event_loop).unwrap())
        .collect();

    let callers: Vec<_> = transports
        .iter()
        .enumerate()
        .map(|(id, transport)| {
            transport.set_timeout(Duration::from_secs(5));
            let client = arith::Client::new(transport.clone(), Codec::bincode(), 0);
            thread::spawn(move || {
                let base = (id as u64 + 1) * 1_000_000;
                for i in 0..CALLS {
                    let reply = client
                        .add(base, i)
                        .map_err(|e| format!("client {id}, call {i}: {e}"))?;
                    if reply != base + i {
                        return Err(format!(
                            "client {id}, call {i}: got {reply}, expected {}",
                            base + i
                        ));
                    }
                }
                Ok::<(), String>(())
            })
        })
        .collect();

    let results: Vec<_> = callers.into_iter().map(|c| c.join().unwrap()).collect();
    event_loop.stop();
    assert_eq!(runner.join().unwrap(), Ok(()));
    for result in results {
        assert_eq!(result, Ok(()));
    }
}

#[test]
fn two_servers_share_one_loop() {
    const CALLS: u64 = 200;

    let event_loop = Arc::new(EventLoop::new(4, 1024, 10).unwrap());
    let servers: Vec<RpcServer> = (0..2)
        .map(|_| {
            RpcServer::builder()
                .bind("127.0.0.1:0".parse().unwrap())
                .service(arith::server(Adder))
                .build(&event_loop)
                .unwrap()
        })
        .collect();
    let addrs: Vec<SocketAddr> = servers.iter().map(|s| s.local_addr().unwrap()).collect();
    let runner = {
        let event_loop = Arc::clone(&event_loop);
        thread::spawn(move || event_loop.run().map_err(|e| e.to_string()))
    };

    let callers: Vec<_> = (0..4)
        .map(|id| {
            let transport = RpcClient::connect(addrs[id % 2], &event_loop).unwrap();
            transport.set_timeout(Duration::from_secs(5));
            let client = arith::Client::new(transport, Codec::bincode(), 0);
            thread::spawn(move || {
                let base = (id as u64 + 1) * 1_000_000;
                for i in 0..CALLS {
                    let reply = client
                        .add(base, i)
                        .map_err(|e| format!("client {id}, call {i}: {e}"))?;
                    if reply != base + i {
                        return Err(format!("client {id}, call {i}: got {reply}"));
                    }
                }
                Ok::<(), String>(())
            })
        })
        .collect();

    let results: Vec<_> = callers.into_iter().map(|c| c.join().unwrap()).collect();
    event_loop.stop();
    assert_eq!(runner.join().unwrap(), Ok(()));
    for result in results {
        assert_eq!(result, Ok(()));
    }
}
