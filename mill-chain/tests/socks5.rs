use mill_chain::testing::{
    local_dns_lookups, HttpReply, MockHttpServer, MockReply, MockScript, MockServer, MockSocks5,
};
use mill_chain::{
    Chain, ChainClient, ChainConfig, ChainError, CoreAuth, CoreConfig, Socks5Config, Socks5Error,
    TransportConfig,
};
use serde_json::json;
use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr, TcpListener, UdpSocket};
use std::thread;
use std::time::{Duration, Instant};

const MS: Duration = Duration::from_millis(1);

fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if cond() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(2 * MS);
    }
}

fn echo_server() -> MockServer {
    MockServer::start(MockScript::new(|request| {
        MockReply::Result(request["params"].clone())
    }))
    .unwrap()
}

fn via_proxy(proxy: SocketAddr, isolate: bool) -> TransportConfig {
    TransportConfig {
        socks5: Some(Socks5Config {
            proxy: proxy.to_string(),
            isolate,
        }),
        tls: None,
    }
}

fn spawn(host: &str, port: u16, transport: TransportConfig) -> ChainClient {
    Chain::spawn(ChainConfig::RawLineJsonRpcVia {
        host: host.to_string(),
        port,
        transport,
        events_capacity: 16,
    })
    .unwrap()
}

fn ping(client: &ChainClient) -> Result<serde_json::Value, ChainError> {
    client
        .handle()
        .deadline(5000 * MS)
        .call_raw("ping", json!([1]))
}

/// An address of this machine other than loopback: the source address of the default
/// route. No packet is sent.
fn non_loopback_ip() -> IpAddr {
    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
    socket.connect("192.0.2.1:9").unwrap();
    let ip = socket.local_addr().unwrap().ip();
    assert!(!ip.is_loopback() && !ip.is_unspecified(), "{}", ip);
    ip
}

#[test]
fn proxied_connect_never_resolves_locally() {
    let server = echo_server();
    let proxy = MockSocks5::start(0).unwrap();
    let client = spawn(
        "localhost",
        server.addr().port(),
        via_proxy(proxy.addr(), false),
    );
    assert_eq!(ping(&client), Ok(json!([1])));

    let connects = proxy.connects();
    assert_eq!(connects.len(), 1);
    assert_eq!(connects[0].atyp, 3);
    assert_eq!(connects[0].host, "localhost");
    assert_eq!(connects[0].port, server.addr().port());
    assert_eq!(
        (&connects[0].username, &connects[0].password),
        (&None, &None)
    );
    assert_eq!(local_dns_lookups(&client), 0);
    client.shutdown().unwrap();
}

#[test]
fn reply_codes_map_to_errors() {
    let codes: Vec<u8> = (0x01..=0x08).chain(0xF0..=0xF7).collect();
    let mut messages = HashSet::new();
    for code in codes {
        let expected = Socks5Error::from_reply(code).unwrap();
        assert_eq!(expected.reply_code(), Some(code));
        assert!(!matches!(expected, Socks5Error::Other(_)), "{:#x}", code);

        let proxy = MockSocks5::start(code).unwrap();
        let client = spawn("example.onion", 50001, via_proxy(proxy.addr(), false));
        let result = ping(&client);
        assert_eq!(
            result,
            Err(ChainError::Transport(format!(
                "handshake failed: {}",
                expected
            ))),
            "{:#x}",
            code
        );
        assert!(messages.insert(expected.to_string()));
        assert_eq!(proxy.connects()[0].host, "example.onion");
        assert_eq!(client.stats().pending_requests, 0);
        client.shutdown().unwrap();
    }
    assert_eq!(messages.len(), 16);
}

#[test]
fn proxy_host_is_not_hardcoded() {
    let ip = non_loopback_ip();
    let server = echo_server();
    let proxy = MockSocks5::start_on(SocketAddr::new(ip, 0), 0).unwrap();
    let transport = TransportConfig {
        socks5: Some(Socks5Config {
            proxy: format!("{}:{}", ip, proxy.addr().port()),
            isolate: false,
        }),
        tls: None,
    };
    let client = spawn("127.0.0.1", server.addr().port(), transport);
    assert_eq!(ping(&client), Ok(json!([1])));
    let connects = proxy.connects();
    assert_eq!(connects.len(), 1);
    assert_eq!(
        (connects[0].atyp, connects[0].host.as_str()),
        (1, "127.0.0.1")
    );
    assert_eq!(local_dns_lookups(&client), 0);
    client.shutdown().unwrap();
}

#[test]
fn isolated_connections_send_fresh_credentials() {
    let server = echo_server();
    let proxy = MockSocks5::start(0).unwrap();
    let clients: Vec<ChainClient> = (0..2)
        .map(|_| {
            spawn(
                "localhost",
                server.addr().port(),
                via_proxy(proxy.addr(), true),
            )
        })
        .collect();
    for client in &clients {
        assert_eq!(ping(client), Ok(json!([1])));
    }
    server.restart();
    assert_eq!(
        clients[0]
            .handle()
            .deadline(5000 * MS)
            .retries(3)
            .call_raw("again", json!([2])),
        Ok(json!([2]))
    );

    assert!(wait_until(5000 * MS, || proxy.connects().len() == 4));
    let connects = proxy.connects();
    let credentials: HashSet<_> = connects
        .iter()
        .map(|c| (c.username.clone().unwrap(), c.password.clone().unwrap()))
        .collect();
    assert_eq!(credentials.len(), 4, "{:?}", connects);
    for client in clients {
        client.shutdown().unwrap();
    }
}

#[test]
fn ipv6_literal_goes_as_atyp_4() {
    let proxy = MockSocks5::start(0x04).unwrap();
    let client = spawn("[2001:db8::7]", 50002, via_proxy(proxy.addr(), false));
    assert!(matches!(ping(&client), Err(ChainError::Transport(_))));
    let connect = &proxy.connects()[0];
    assert_eq!((connect.atyp, connect.host.as_str()), (4, "2001:db8::7"));
    assert_eq!(connect.port, 50002);
    client.shutdown().unwrap();
}

/// Accepts one connection, answers its greeting with `method`, and returns the greeting.
fn greeting_answered_with(method: u8) -> (SocketAddr, thread::JoinHandle<[u8; 3]>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut greeting = [0; 3];
        stream.read_exact(&mut greeting).unwrap();
        stream.write_all(&[5, method]).unwrap();
        greeting
    });
    (addr, server)
}

#[test]
fn unisolated_connections_offer_only_no_auth() {
    let (proxy, server) = greeting_answered_with(0xFF);
    let client = spawn("example.onion", 1, via_proxy(proxy, false));
    assert!(matches!(ping(&client), Err(ChainError::Transport(_))));
    assert_eq!(server.join().unwrap(), [5, 1, 0]);
    client.shutdown().unwrap();
}

#[test]
fn refused_auth_method_fails_calls() {
    let (proxy, refuser) = greeting_answered_with(0xFF);
    let client = spawn("example.onion", 1, via_proxy(proxy, true));
    assert_eq!(
        ping(&client),
        Err(ChainError::Transport(format!(
            "handshake failed: {}",
            Socks5Error::NoAcceptableAuth
        )))
    );
    assert_eq!(refuser.join().unwrap(), [5, 1, 2]);
    client.shutdown().unwrap();
}

#[test]
fn handshake_failure_spares_calls_with_retries() {
    let proxy = MockSocks5::start(0x01).unwrap();
    let client = spawn("example.onion", 1, via_proxy(proxy.addr(), false));
    let result = client
        .handle()
        .deadline(3000 * MS)
        .retries(2)
        .call_raw("ping", json!([]));
    assert!(
        matches!(result, Err(ChainError::Transport(_))),
        "{:?}",
        result
    );
    assert!(proxy.connects().len() >= 3, "{:?}", proxy.connects());
    client.shutdown().unwrap();
}

#[test]
fn direct_hostname_resolves_off_the_loop() {
    let server = echo_server();
    let client = spawn(
        "localhost",
        server.addr().port(),
        TransportConfig::default(),
    );
    let result = client
        .handle()
        .deadline(5000 * MS)
        .call_raw("ping", json!([3]));
    assert_eq!(result, Ok(json!([3])));
    assert!(local_dns_lookups(&client) >= 1);
    client.shutdown().unwrap();
}

#[test]
fn malformed_proxy_is_refused_at_spawn() {
    for proxy in ["9050", "tor:", "::1:9050"] {
        let transport = TransportConfig {
            socks5: Some(Socks5Config {
                proxy: proxy.to_string(),
                isolate: false,
            }),
            tls: None,
        };
        let result = Chain::spawn(ChainConfig::RawLineJsonRpcVia {
            host: "example.onion".into(),
            port: 1,
            transport,
            events_capacity: 1,
        });
        assert!(
            matches!(result, Err(ChainError::Transport(_))),
            "{}: {:?}",
            proxy,
            result
        );
    }
}

#[test]
fn stalled_proxy_handshake_is_closed_at_the_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let proxy = listener.local_addr().unwrap();
    let silent = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut greeting = [0; 3];
        stream.read_exact(&mut greeting).unwrap();
        let mut rest = Vec::new();
        stream.read_to_end(&mut rest).unwrap();
        Instant::now()
    });
    let client = spawn("example.onion", 1, via_proxy(proxy, false));
    let start = Instant::now();
    let result = client
        .handle()
        .deadline(300 * MS)
        .call_raw("ping", json!([]));
    assert_eq!(result, Err(ChainError::Timeout));
    let closed_at = silent.join().unwrap();
    assert!(closed_at - start < 1500 * MS, "{:?}", closed_at - start);
    client.shutdown().unwrap();
}

fn core_via(node_port: u16, transport: TransportConfig) -> ChainClient {
    let mut config = CoreConfig::new(
        format!("http://localhost:{}", node_port),
        CoreAuth::UserPass {
            user: "u".into(),
            pass: "p".into(),
        },
    );
    config.transport = transport;
    Chain::spawn(ChainConfig::Core(config)).unwrap()
}

#[test]
fn core_rpc_goes_through_the_proxy() {
    let node = MockHttpServer::start(|r| HttpReply::result(r, json!(812_000))).unwrap();
    let proxy = MockSocks5::start(0).unwrap();
    let client = core_via(node.addr().port(), via_proxy(proxy.addr(), true));
    let result = client
        .handle()
        .deadline(5000 * MS)
        .call_raw("getblockcount", json!([]));
    assert_eq!(result, Ok(json!(812_000)));

    let connects = proxy.connects();
    assert_eq!(connects.len(), 1);
    assert_eq!(
        (connects[0].atyp, connects[0].host.as_str()),
        (3, "localhost")
    );
    assert!(connects[0].username.is_some());
    assert_eq!(local_dns_lookups(&client), 0);
    let host = format!("localhost:{}", node.addr().port());
    assert_eq!(node.requests_seen()[0].header("host"), Some(host.as_str()));
    client.shutdown().unwrap();
}

#[test]
fn core_rpc_reports_a_refused_proxy_request() {
    let proxy = MockSocks5::start(0x05).unwrap();
    let client = core_via(8332, via_proxy(proxy.addr(), false));
    let result = client
        .handle()
        .deadline(5000 * MS)
        .call_raw("getblockcount", json!([]));
    let expected = Socks5Error::ConnectionRefused.to_string();
    assert!(
        matches!(&result, Err(ChainError::Transport(m)) if m.contains(&expected)),
        "{:?}",
        result
    );
    client.shutdown().unwrap();
}

/// A proxy that closes its first connection after the greeting once `close` fires, and
/// relays later ones to `upstream`.
fn closes_first_then_relays(
    upstream: SocketAddr,
    close: std::sync::mpsc::Receiver<()>,
) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let Ok((mut first, _)) = listener.accept() else {
            return;
        };
        let mut greeting = [0; 3];
        let _ = first.read_exact(&mut greeting);
        let _ = close.recv();
        drop(first);
        for client in listener.incoming() {
            let Ok(client) = client else { return };
            let Ok(server) = std::net::TcpStream::connect(upstream) else {
                return;
            };
            let (mut c_in, mut s_out) = (client.try_clone().unwrap(), server.try_clone().unwrap());
            thread::spawn(move || {
                let _ = std::io::copy(&mut c_in, &mut s_out);
            });
            let (mut s_in, mut c_out) = (server, client);
            thread::spawn(move || {
                let _ = std::io::copy(&mut s_in, &mut c_out);
            });
        }
    });
    addr
}

#[test]
fn proxy_closing_mid_handshake_is_retried() {
    let server = echo_server();
    let socks = MockSocks5::start(0).unwrap();
    let (close, closed) = std::sync::mpsc::channel();
    let proxy = closes_first_then_relays(socks.addr(), closed);
    let client = spawn("localhost", server.addr().port(), via_proxy(proxy, false));
    let handle = client.handle().deadline(5000 * MS);
    let caller = thread::spawn(move || handle.call_raw("ping", json!([1])));
    assert!(wait_until(2000 * MS, || client.stats().pending_requests == 1));
    close.send(()).unwrap();
    assert_eq!(caller.join().unwrap(), Ok(json!([1])));
    assert_eq!(socks.connects().len(), 1);
    client.shutdown().unwrap();
}
