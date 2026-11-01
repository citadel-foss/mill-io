//! Against a real Tor daemon; runs only with `MILL_CHAIN_TOR_IT=1`. The daemon's ports
//! come from `MILL_CHAIN_TOR_SOCKS` and `MILL_CHAIN_TOR_CONTROL` (default 127.0.0.1:9050
//! and :9051), its control password from `MILL_CHAIN_TOR_PASSWORD` (default empty).

use mill_chain::testing::{local_dns_lookups, MockReply, MockScript, MockServer};
use mill_chain::{Chain, ChainClient, ChainConfig, ChainError, Socks5Config, TransportConfig};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

const ONION_PORT: u16 = 50001;
/// Publishing a fresh onion service's descriptor can take minutes.
const ONION_READY: Duration = Duration::from_secs(300);
const CALL_DEADLINE: Duration = Duration::from_secs(120);

fn enabled() -> bool {
    let on = std::env::var("MILL_CHAIN_TOR_IT").as_deref() == Ok("1");
    if !on {
        eprintln!("skipped: set MILL_CHAIN_TOR_IT=1 to run against Tor");
    }
    on
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

/// An authenticated Tor control connection.
struct Control {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Control {
    fn connect() -> Self {
        let addr = env_or("MILL_CHAIN_TOR_CONTROL", "127.0.0.1:9051");
        let stream = TcpStream::connect(&addr).unwrap_or_else(|e| panic!("{}: {}", addr, e));
        let mut control = Self {
            reader: BufReader::new(stream.try_clone().unwrap()),
            writer: stream,
        };
        let password = env_or("MILL_CHAIN_TOR_PASSWORD", "");
        control.command(&format!("AUTHENTICATE \"{}\"", password));
        control
    }

    /// Sends `line` and returns the reply lines, up to the final `250 OK`.
    fn command(&mut self, line: &str) -> Vec<String> {
        self.writer
            .write_all(format!("{}\r\n", line).as_bytes())
            .unwrap();
        let mut lines = Vec::new();
        loop {
            let reply = self.read_line().expect("control connection closed");
            if reply.starts_with("650") {
                continue;
            }
            assert!(reply.starts_with("250"), "{}: {}", line, reply);
            let last = reply.as_bytes().get(3) == Some(&b' ');
            lines.push(reply);
            if last {
                return lines;
            }
        }
    }

    fn read_line(&mut self) -> Option<String> {
        let mut line = String::new();
        match self.reader.read_line(&mut line) {
            Ok(0) => None,
            Ok(_) => Some(line.trim_end().to_string()),
            Err(e) if is_timeout(&e) => Some(String::new()),
            Err(e) => panic!("control read: {}", e),
        }
    }

    /// An onion service forwarding `ONION_PORT` to `target`, alive while `self` is.
    fn add_onion(&mut self, target: u16) -> String {
        let reply = self.command(&format!(
            "ADD_ONION NEW:ED25519-V3 Flags=DiscardPK Port={},127.0.0.1:{}",
            ONION_PORT, target
        ));
        let id = reply
            .iter()
            .find_map(|l| l.strip_prefix("250-ServiceID="))
            .expect("ADD_ONION gave no ServiceID");
        format!("{}.onion", id)
    }
}

fn is_timeout(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
    )
}

fn echo_server() -> MockServer {
    MockServer::start(MockScript::new(|request| {
        MockReply::Result(request["params"].clone())
    }))
    .unwrap()
}

fn spawn_via_tor(onion: &str, isolate: bool) -> ChainClient {
    Chain::spawn(ChainConfig::RawLineJsonRpcVia {
        host: onion.to_string(),
        port: ONION_PORT,
        transport: TransportConfig {
            socks5: Some(Socks5Config {
                proxy: env_or("MILL_CHAIN_TOR_SOCKS", "127.0.0.1:9050"),
                isolate,
            }),
            tls: None,
        },
        events_capacity: 16,
    })
    .unwrap()
}

/// Calls until the onion service answers. While its descriptor is still being
/// published, Tor refuses the CONNECT with an onion reply code.
fn wait_reachable(client: &ChainClient) {
    let give_up = Instant::now() + ONION_READY;
    let mut last = None;
    while Instant::now() < give_up {
        match client
            .handle()
            .deadline(CALL_DEADLINE)
            .call_raw("ping", json!(["ready"]))
        {
            Ok(value) => {
                assert_eq!(value, json!(["ready"]));
                return;
            }
            Err(e @ (ChainError::Transport(_) | ChainError::Timeout)) => last = Some(e),
            Err(e) => panic!("unexpected error: {:?}", e),
        }
    }
    panic!(
        "onion service unreachable after {:?}: {:?}",
        ONION_READY, last
    );
}

#[test]
fn raw_jsonrpc_over_onion() {
    if !enabled() {
        return;
    }
    let server = echo_server();
    let mut control = Control::connect();
    let onion = control.add_onion(server.addr().port());
    let client = spawn_via_tor(&onion, true);
    wait_reachable(&client);

    for i in 0..100u64 {
        let params = json!([i, format!("request {}", i)]);
        let result = client
            .handle()
            .deadline(CALL_DEADLINE)
            .call_raw("echo", params.clone());
        assert_eq!(result, Ok(params));
    }
    let echoed = server
        .requests_seen()
        .iter()
        .filter(|r| r["method"] == "echo")
        .count();
    assert_eq!(echoed, 100);
    assert_eq!(local_dns_lookups(&client), 0);
    client.shutdown().unwrap();
}

/// `650 STREAM <id> SUCCEEDED <circuit> <target> ...` as (stream, circuit, target).
fn succeeded_stream(line: &str) -> Option<(String, String, String)> {
    let mut words = line.strip_prefix("650 STREAM ")?.split(' ');
    let stream = words.next()?;
    if words.next()? != "SUCCEEDED" {
        return None;
    }
    let circuit = words.next()?;
    let target = words.next()?;
    Some((stream.into(), circuit.into(), target.into()))
}

#[test]
fn isolated_connections_use_distinct_circuits() {
    if !enabled() {
        return;
    }
    let server = echo_server();
    let mut control = Control::connect();
    let onion = control.add_onion(server.addr().port());
    let warmup = spawn_via_tor(&onion, true);
    wait_reachable(&warmup);
    warmup.shutdown().unwrap();

    let mut events = Control::connect();
    events.command("SETEVENTS STREAM");
    events
        .writer
        .set_read_timeout(Some(Duration::from_millis(200)))
        .unwrap();

    let clients: Vec<ChainClient> = (0..2).map(|_| spawn_via_tor(&onion, true)).collect();
    for client in &clients {
        wait_reachable(client);
    }

    let target = format!("{}:{}", onion, ONION_PORT);
    let mut circuits: HashMap<String, String> = HashMap::new();
    let give_up = Instant::now() + Duration::from_secs(30);
    while circuits.len() < 2 && Instant::now() < give_up {
        let Some(line) = events.read_line() else {
            break;
        };
        if let Some((stream, circuit, to)) = succeeded_stream(&line) {
            if to == target {
                circuits.insert(stream, circuit);
            }
        }
    }
    assert_eq!(circuits.len(), 2, "streams seen: {:?}", circuits);
    let distinct: HashSet<&String> = circuits.values().collect();
    assert_eq!(
        distinct.len(),
        2,
        "isolated streams shared a circuit: {:?}",
        circuits
    );
    for client in clients {
        client.shutdown().unwrap();
    }
}

#[test]
fn stream_events_parse() {
    assert_eq!(
        succeeded_stream("650 STREAM 21 SUCCEEDED 9 abc.onion:50001 SOCKS_USERNAME=\"u\""),
        Some(("21".into(), "9".into(), "abc.onion:50001".into()))
    );
    assert_eq!(
        succeeded_stream("650 STREAM 21 NEW 0 abc.onion:50001"),
        None
    );
    assert_eq!(succeeded_stream("650 CIRC 9 BUILT"), None);
}
