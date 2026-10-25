use mill_chain::testing::{batch_raw, parse_http_response, HttpReply, HttpRequest, MockHttpServer};
use mill_chain::{Chain, ChainClient, ChainConfig, ChainError, CoreAuth, CoreConfig};
use mill_net::testing::{ChaosConfig, ChaosProxy};
use proptest::prelude::*;
use serde_json::{json, Value};
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::atomic::{AtomicIsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const MS: Duration = Duration::from_millis(1);
const SECS: Duration = Duration::from_secs(1);

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

fn user_pass() -> CoreAuth {
    CoreAuth::UserPass {
        user: "alice".into(),
        pass: "secret".into(),
    }
}

fn config(url: String) -> CoreConfig {
    CoreConfig::new(url, user_pass())
}

fn spawn(config: CoreConfig) -> ChainClient {
    Chain::spawn(ChainConfig::Core(config)).unwrap()
}

/// Answers every request, batched or not, with its own params.
fn echo(request: &HttpRequest) -> HttpReply {
    match request.json() {
        Some(Value::Array(batch)) => {
            let replies: Vec<Value> = batch
                .iter()
                .map(|item| json!({"result": item["params"], "error": null, "id": item["id"]}))
                .collect();
            HttpReply::json(200, &Value::Array(replies))
        }
        body => HttpReply::result(request, body.map_or(Value::Null, |r| r["params"].clone())),
    }
}

fn methods(server: &MockHttpServer, method: &str) -> Vec<HttpRequest> {
    server
        .requests_seen()
        .into_iter()
        .filter(|r| r.method().as_deref() == Some(method))
        .collect()
}

fn temp_path(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("mill-chain-http-{}-{}", std::process::id(), name));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(".cookie")
}

#[derive(Debug, Clone)]
struct Framing {
    chunks: Option<Vec<usize>>,
    extension: bool,
    trailers: bool,
    close: bool,
}

fn encode(body: &[u8], framing: &Framing) -> Vec<u8> {
    let mut wire = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n".to_vec();
    if framing.close {
        wire.extend_from_slice(b"Connection: close\r\n");
    }
    match &framing.chunks {
        None => {
            wire.extend_from_slice(format!("Content-Length: {}\r\n\r\n", body.len()).as_bytes());
            wire.extend_from_slice(body);
        }
        Some(sizes) => {
            wire.extend_from_slice(b"Transfer-Encoding: chunked\r\n\r\n");
            let mut rest = body;
            let mut sizes = sizes.iter().cycle();
            while !rest.is_empty() {
                let n = (*sizes.next().unwrap()).min(rest.len());
                wire.extend_from_slice(format!("{:X}", n).as_bytes());
                if framing.extension {
                    wire.extend_from_slice(b";ext=\"v\"");
                }
                wire.extend_from_slice(b"\r\n");
                wire.extend_from_slice(&rest[..n]);
                wire.extend_from_slice(b"\r\n");
                rest = &rest[n..];
            }
            wire.extend_from_slice(b"0\r\n");
            if framing.trailers {
                wire.extend_from_slice(b"X-Checksum: abc\r\n");
            }
            wire.extend_from_slice(b"\r\n");
        }
    }
    wire
}

fn framing() -> impl Strategy<Value = Framing> {
    (
        prop::option::of(prop::collection::vec(1usize..300, 1..8)),
        any::<bool>(),
        any::<bool>(),
        any::<bool>(),
    )
        .prop_map(|(chunks, extension, trailers, close)| Framing {
            chunks,
            extension,
            trailers,
            close,
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10_000))]

    #[test]
    fn chunked_and_length_bodies_roundtrip(
        body in prop::collection::vec(any::<u8>(), 0..2048),
        framing in framing(),
        cuts in prop::collection::vec(any::<prop::sample::Index>(), 0..16),
    ) {
        let wire = encode(&body, &framing);
        let mut cuts: Vec<usize> = cuts.iter().map(|c| c.index(wire.len() + 1)).collect();
        cuts.sort_unstable();
        let mut reads = Vec::new();
        let mut start = 0;
        for cut in cuts.into_iter().chain([wire.len()]) {
            reads.push(&wire[start..cut]);
            start = cut;
        }
        let parsed = parse_http_response(reads, 1 << 20);
        prop_assert_eq!(parsed, Ok((200, body)));
    }
}

#[test]
fn twenty_mb_response_with_one_byte_splits() {
    let big = "ab".repeat(10 * 1024 * 1024);
    let reply = big.clone();
    let server = MockHttpServer::start(move |r| match r.method().as_deref() {
        Some("getblock") => HttpReply::result(r, Value::String(reply.clone())).chunked(64 * 1024),
        _ => echo(r),
    })
    .unwrap();
    let chaos = ChaosConfig {
        split_writes: Some(1),
        ..ChaosConfig::default()
    };
    let proxy = ChaosProxy::start(server.addr(), chaos).unwrap();
    let mut config = config(format!("http://{}", proxy.addr()));
    config.request_timeout = 600 * SECS;
    let client = spawn(config);
    let handle = client.handle().deadline(600 * SECS);
    let result = handle.call_raw("getblock", json!(["00", 0])).unwrap();
    assert_eq!(result.as_str().map(str::len), Some(big.len()));
    assert!(result == Value::String(big));
    assert!(handle.call_raw("getblockcount", json!([])) == Ok(json!([])));
    assert_eq!(server.connection_count(), 1);
}

#[test]
fn long_call_is_sent_once() {
    let server = MockHttpServer::start(|r| match r.method().as_deref() {
        Some("rescanblockchain") => {
            HttpReply::result(r, json!({"start_height": 0, "stop_height": 10})).after(20 * SECS)
        }
        _ => echo(r),
    })
    .unwrap();
    let mut config = config(server.url());
    config.request_timeout = SECS;
    let client = spawn(config);
    let handle = client.handle();

    let long = {
        let handle = handle.clone();
        thread::spawn(move || {
            let started = Instant::now();
            (
                handle.call_raw("rescanblockchain", json!([0])),
                started.elapsed(),
            )
        })
    };
    assert!(wait_until(5 * SECS, || methods(
        &server,
        "rescanblockchain"
    )
    .len()
        == 1));
    let started = Instant::now();
    assert_eq!(handle.call_raw("getblockcount", json!([7])), Ok(json!([7])));
    assert!(started.elapsed() < SECS, "the pool waited on the long lane");

    let (result, took) = long.join().unwrap();
    assert_eq!(result, Ok(json!({"start_height": 0, "stop_height": 10})));
    assert!(took >= 19 * SECS, "{:?}", took);
    let rescans = methods(&server, "rescanblockchain");
    assert_eq!(rescans.len(), 1);
    let chain_calls = methods(&server, "getblockcount");
    assert_ne!(rescans[0].conn, chain_calls[0].conn);
}

#[test]
fn long_call_is_not_resent_when_its_connection_drops() {
    let server = MockHttpServer::start(|r| match r.method().as_deref() {
        Some("importdescriptors") => HttpReply::drop_connection(),
        _ => echo(r),
    })
    .unwrap();
    let client = spawn(config(server.url()));
    let handle = client.handle().retries(3);
    assert!(matches!(
        handle.call_raw("importdescriptors", json!([[]])),
        Err(ChainError::Transport(_))
    ));
    assert_eq!(methods(&server, "importdescriptors").len(), 1);
    assert_eq!(handle.call_raw("getblockcount", json!([1])), Ok(json!([1])));
}

#[test]
fn chain_and_wallet_routing() {
    let server = MockHttpServer::start(echo).unwrap();
    let mut with_wallet = config(server.url());
    with_wallet.wallet = Some("swap wallet/1".into());
    let client = spawn(with_wallet);
    let handle = client.handle();
    handle.call_raw("getblockcount", json!([])).unwrap();
    handle.wallet_call_raw("listunspent", json!([0])).unwrap();
    handle.call_raw("rescanblockchain", json!([])).unwrap();
    handle
        .wallet_call_raw("importdescriptors", json!([[]]))
        .unwrap();
    batch_raw(
        &handle,
        &[("getblockhash", json!([0])), ("getblockhash", json!([1]))],
    )
    .unwrap();
    let paths: Vec<(Option<String>, String)> = server
        .requests_seen()
        .into_iter()
        .map(|r| (r.method(), r.path))
        .collect();
    let wallet = "/wallet/swap%20wallet%2F1".to_string();
    assert_eq!(
        paths,
        [
            (Some("getblockcount".into()), "/".into()),
            (Some("listunspent".into()), wallet.clone()),
            (Some("rescanblockchain".into()), "/".into()),
            (Some("importdescriptors".into()), wallet),
            (None, "/".into()),
        ]
    );

    let chain_only = spawn(config(server.url()));
    assert!(matches!(
        chain_only
            .handle()
            .wallet_call_raw("listunspent", json!([])),
        Err(ChainError::Unsupported(_))
    ));
}

#[test]
fn error_mapping() {
    let server = MockHttpServer::start(|r| {
        if r.basic_auth() != Some(("alice".into(), "secret".into())) {
            return HttpReply::status(401);
        }
        match r.method().as_deref() {
            Some("getrawtransaction") => {
                HttpReply::error(r, -5, "No such mempool or blockchain transaction")
            }
            Some("getblock") => HttpReply::error(r, -8, "Block height out of range"),
            Some("nosuchmethod") => HttpReply::json(
                404,
                &json!({"result": null, "error": {"code": -32601, "message": "Method not found"}, "id": r.json().unwrap()["id"]}),
            ),
            Some("drop") => HttpReply::drop_connection(),
            Some("forbidden") => HttpReply::status(403),
            Some("teapot") => HttpReply::new(418, "short and stout"),
            _ => echo(r),
        }
    })
    .unwrap();
    let client = spawn(config(server.url()));
    let handle = client.handle();

    // Raw calls never interpret codes: -5 stays a server error, and only typed queries
    // read it as absence.
    let not_found = handle
        .call_raw("getrawtransaction", json!(["00"]))
        .unwrap_err();
    assert_eq!(
        not_found,
        ChainError::Server {
            code: -5,
            message: "No such mempool or blockchain transaction".into()
        }
    );
    assert!(!not_found.is_absence());
    assert_eq!(
        handle.call_raw("getblock", json!([])),
        Err(ChainError::Server {
            code: -8,
            message: "Block height out of range".into()
        })
    );
    assert_eq!(
        handle.call_raw("nosuchmethod", json!([])),
        Err(ChainError::Server {
            code: -32601,
            message: "Method not found".into()
        })
    );
    assert!(matches!(
        handle.call_raw("drop", json!([])),
        Err(ChainError::Transport(_))
    ));
    assert_eq!(
        handle.call_raw("forbidden", json!([])),
        Err(ChainError::Auth)
    );
    assert!(matches!(
        handle.call_raw("teapot", json!([])),
        Err(ChainError::Protocol(m)) if m.contains("418")
    ));
    assert_eq!(handle.call_raw("ping", json!([1])), Ok(json!([1])));

    let wrong = CoreConfig::new(
        server.url(),
        CoreAuth::UserPass {
            user: "alice".into(),
            pass: "wrong".into(),
        },
    );
    let client = spawn(wrong);
    let wrong = client.handle();
    assert_eq!(wrong.call_raw("ping", json!([])), Err(ChainError::Auth));
    assert_eq!(methods(&server, "ping").len(), 2);
}

#[test]
fn unchanged_cookie_after_a_401_is_auth() {
    let cookie = temp_path("unchanged");
    std::fs::write(&cookie, "__cookie__:stale").unwrap();
    let server = MockHttpServer::start(|_| HttpReply::status(401)).unwrap();
    let client = spawn(CoreConfig::new(server.url(), CoreAuth::Cookie(cookie)));
    let handle = client.handle();
    assert_eq!(handle.call_raw("ping", json!([])), Err(ChainError::Auth));
    assert_eq!(server.requests_seen().len(), 1);
}

#[test]
fn cookie_is_reread_on_401() {
    let cookie = temp_path("reread");
    std::fs::write(&cookie, "__cookie__:old").unwrap();
    let server = MockHttpServer::start(|r| match r.basic_auth() {
        Some((_, pass)) if pass == "new" => echo(r),
        _ => HttpReply::status(401),
    })
    .unwrap();
    let client = spawn(CoreConfig::new(
        server.url(),
        CoreAuth::Cookie(cookie.clone()),
    ));
    let handle = client.handle();
    assert_eq!(handle.call_raw("ping", json!([])), Err(ChainError::Auth));
    std::fs::write(&cookie, "__cookie__:new\n").unwrap();
    let callers: Vec<_> = (0..8)
        .map(|i| {
            let handle = handle.clone();
            thread::spawn(move || handle.call_raw("ping", json!([i])))
        })
        .collect();
    for (i, caller) in callers.into_iter().enumerate() {
        assert_eq!(caller.join().unwrap(), Ok(json!([i])));
    }
}

#[test]
fn missing_cookie_appears_later() {
    let cookie = temp_path("missing");
    let _ = std::fs::remove_file(&cookie);
    let server = MockHttpServer::start(|r| match r.basic_auth() {
        Some((user, pass)) if user == "__cookie__" && pass == "fresh" => echo(r),
        _ => HttpReply::status(401),
    })
    .unwrap();
    let client = spawn(CoreConfig::new(
        server.url(),
        CoreAuth::Cookie(cookie.clone()),
    ));
    let handle = client.handle();
    let early = handle.call_raw("ping", json!([]));
    assert_eq!(early, Err(ChainError::Auth));
    assert!(!early.unwrap_err().is_absence());
    std::fs::write(&cookie, "__cookie__:fresh").unwrap();
    assert_eq!(handle.call_raw("ping", json!([2])), Ok(json!([2])));
}

#[test]
fn work_queue_503_storm_then_recovery() {
    let refusals = Arc::new(AtomicIsize::new(40));
    let left = Arc::clone(&refusals);
    let server = MockHttpServer::start(move |r| {
        let refuse = left.fetch_sub(1, Ordering::SeqCst) > 0;
        if refuse {
            HttpReply::new(503, "Work queue depth exceeded")
        } else {
            echo(r)
        }
    })
    .unwrap();
    let client = spawn(config(server.url()));
    // With retries(0): 503s do not use up retries.
    let callers: Vec<_> = (0..16)
        .map(|i| {
            let handle = client.handle();
            thread::spawn(move || handle.call_raw("getblockhash", json!([i])))
        })
        .collect();
    for (i, caller) in callers.into_iter().enumerate() {
        assert_eq!(caller.join().unwrap(), Ok(json!([i])));
    }
    assert_eq!(server.requests_seen().len(), 56);
    assert_eq!(client.stats().requests_sent, 56);
}

#[test]
fn long_lane_503_is_not_resent() {
    let server =
        MockHttpServer::start(|_| HttpReply::new(503, "Work queue depth exceeded")).unwrap();
    let client = spawn(config(server.url()));
    let handle = client.handle();
    assert!(matches!(
        handle.call_raw("rescanblockchain", json!([])),
        Err(ChainError::Transport(m)) if m.contains("503")
    ));
    assert_eq!(server.requests_seen().len(), 1);
}

#[test]
fn batch_replies_in_a_different_order() {
    let server = MockHttpServer::start(|r| {
        let Some(Value::Array(batch)) = r.json() else {
            return echo(r);
        };
        let mut replies: Vec<Value> = batch
            .iter()
            .map(|item| {
                if item["method"] == "getrawtransaction" {
                    json!({"result": null, "error": {"code": -5, "message": "nope"}, "id": item["id"]})
                } else {
                    json!({"result": item["params"], "error": null, "id": item["id"]})
                }
            })
            .collect();
        replies.reverse();
        replies.swap(0, 1);
        HttpReply::json(200, &Value::Array(replies))
    })
    .unwrap();
    let client = spawn(config(server.url()));
    let handle = client.handle();
    let results = batch_raw(
        &handle,
        &[
            ("getblockhash", json!([0])),
            ("getblockhash", json!([1])),
            ("getrawtransaction", json!(["00"])),
            ("getblockhash", json!([3])),
        ],
    )
    .unwrap();
    assert_eq!(
        results,
        [
            Ok(json!([0])),
            Ok(json!([1])),
            Err(ChainError::Server {
                code: -5,
                message: "nope".into()
            }),
            Ok(json!([3])),
        ]
    );
    assert_eq!(batch_raw(&handle, &[]).unwrap(), []);
    assert_eq!(server.requests_seen().len(), 1);
}

#[test]
fn connection_close_mid_pool() {
    let server = MockHttpServer::start(|r| {
        let reply = echo(r);
        if r.json().unwrap()["params"][0].as_u64().unwrap() % 3 == 0 {
            reply.closing()
        } else {
            reply
        }
    })
    .unwrap();
    let client = spawn(config(server.url()));
    let handle = client.handle();
    for i in 0..30u64 {
        assert_eq!(handle.call_raw("getblockhash", json!([i])), Ok(json!([i])));
    }
    assert_eq!(server.connection_count(), 11);
    let callers: Vec<_> = (0..40u64)
        .map(|i| {
            let handle = client.handle();
            thread::spawn(move || handle.call_raw("getblockhash", json!([i])))
        })
        .collect();
    for (i, caller) in callers.into_iter().enumerate() {
        assert_eq!(caller.join().unwrap(), Ok(json!([i])));
    }
}

#[test]
fn server_closing_idle_connections_is_harmless() {
    let server = MockHttpServer::start(echo).unwrap();
    let client = spawn(config(server.url()));
    let handle = client.handle();
    assert_eq!(handle.call_raw("a", json!([1])), Ok(json!([1])));
    server.restart();
    assert!(wait_until(5 * SECS, || client.stats().connections_open == 0));
    assert_eq!(handle.call_raw("b", json!([2])), Ok(json!([2])));
    assert_eq!(server.connection_count(), 2);
}

#[test]
fn body_over_the_cap_fails_without_breaking_other_connections() {
    let server = MockHttpServer::start(|r| match r.method().as_deref() {
        Some("big") => HttpReply::result(r, Value::String("x".repeat(4096))),
        Some("bigchunked") => HttpReply::result(r, Value::String("x".repeat(4096))).chunked(100),
        Some("slow") => echo(r).after(500 * MS),
        _ => echo(r),
    })
    .unwrap();
    let mut config = config(server.url());
    config.max_body_bytes = 1024;
    let client = spawn(config);
    let handle = client.handle();
    let slow = {
        let handle = handle.clone();
        thread::spawn(move || handle.call_raw("slow", json!([9])))
    };
    assert!(wait_until(5 * SECS, || methods(&server, "slow").len() == 1));
    for method in ["big", "bigchunked"] {
        assert!(matches!(
            handle.call_raw(method, json!([])),
            Err(ChainError::Protocol(m)) if m.contains("1024")
        ));
    }
    assert_eq!(slow.join().unwrap(), Ok(json!([9])));
    assert_eq!(handle.call_raw("small", json!([1])), Ok(json!([1])));
}

#[test]
fn body_exactly_at_the_cap_is_accepted() {
    let server = MockHttpServer::start(|r| {
        let id = r.json().unwrap()["id"].clone();
        let mut body = json!({"result": "ok", "error": null, "id": id}).to_string();
        body.extend(std::iter::repeat(' ').take(1024 - body.len()));
        let reply = HttpReply::new(200, body);
        if r.method().as_deref() == Some("chunked") {
            reply.chunked(100)
        } else {
            reply
        }
    })
    .unwrap();
    let client = spawn(CoreConfig {
        max_body_bytes: 1024,
        ..config(server.url())
    });
    let handle = client.handle();
    assert_eq!(handle.call_raw("plain", json!([])), Ok(json!("ok")));
    assert_eq!(handle.call_raw("chunked", json!([])), Ok(json!("ok")));
}

#[test]
fn core_config_defaults() {
    let config = CoreConfig::default();
    assert_eq!(config.rpc_url, "http://127.0.0.1:8332");
    assert_eq!(
        config.auth,
        CoreAuth::UserPass {
            user: String::new(),
            pass: String::new()
        }
    );
    assert_eq!(config.wallet, None);
    assert_eq!(config.rpc_connections, 4);
    assert_eq!(config.request_timeout, 30 * SECS);
    assert_eq!(config.max_body_bytes, 32 * 1024 * 1024);
    assert_eq!(config.zmq, Default::default());
    assert_eq!(
        CoreConfig::new("http://h:1", user_pass()).rpc_connections,
        4
    );
}

#[test]
fn timed_out_request_closes_its_connection() {
    let server = MockHttpServer::start(|r| match r.method().as_deref() {
        Some("slow") => echo(r).after(2 * SECS),
        _ => echo(r),
    })
    .unwrap();
    let mut config = config(server.url());
    config.rpc_connections = 1;
    config.request_timeout = 300 * MS;
    let client = spawn(config);
    let handle = client.handle();
    let started = Instant::now();
    assert_eq!(
        handle.call_raw("slow", json!([1])),
        Err(ChainError::Timeout)
    );
    assert!(started.elapsed() < SECS);
    assert_eq!(handle.call_raw("fast", json!([2])), Ok(json!([2])));
    assert_eq!(server.connection_count(), 2);
    assert_eq!(client.stats().pending_requests, 0);
}

#[test]
fn pool_is_bounded_by_rpc_connections() {
    let server = MockHttpServer::start(|r| echo(r).after(20 * MS)).unwrap();
    let mut config = config(server.url());
    config.rpc_connections = 3;
    let client = spawn(config);
    let callers: Vec<_> = (0..24)
        .map(|i| {
            let handle = client.handle();
            thread::spawn(move || handle.call_raw("getblockhash", json!([i])))
        })
        .collect();
    for (i, caller) in callers.into_iter().enumerate() {
        assert_eq!(caller.join().unwrap(), Ok(json!([i])));
    }
    assert_eq!(server.connection_count(), 3);
    assert!(client.stats().connections_open <= 3);
}

#[test]
fn unreachable_node_is_transport() {
    let addr = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let client = spawn(config(format!("http://{}", addr)));
    let started = Instant::now();
    let error = client
        .handle()
        .call_raw("getblockcount", json!([]))
        .unwrap_err();
    assert!(matches!(error, ChainError::Transport(_)), "{:?}", error);
    assert!(!error.is_absence());
    assert!(started.elapsed() < 5 * SECS);
}

#[test]
fn bad_urls_are_refused() {
    for url in [
        "https://127.0.0.1:8332",
        "127.0.0.1:8332",
        "http://127.0.0.1",
    ] {
        assert!(matches!(
            Chain::spawn(ChainConfig::Core(config(url.into()))),
            Err(ChainError::Unsupported(_))
        ));
    }
}

#[test]
fn shutdown_fails_calls_in_flight() {
    let server = MockHttpServer::start(|r| echo(r).after(30 * SECS)).unwrap();
    let client = spawn(config(server.url()));
    let callers: Vec<_> = (0..6)
        .map(|_| {
            let handle = client.handle();
            thread::spawn(move || handle.call_raw("rescanblockchain", json!([])))
        })
        .chain((0..6).map(|_| {
            let handle = client.handle();
            thread::spawn(move || handle.call_raw("getblockcount", json!([])))
        }))
        .collect();
    assert!(wait_until(5 * SECS, || client.stats().pending_requests == 12));
    let started = Instant::now();
    client.shutdown().unwrap();
    assert!(started.elapsed() < SECS);
    for caller in callers {
        assert_eq!(caller.join().unwrap(), Err(ChainError::Shutdown));
    }
}

#[test]
fn node_that_closes_at_once_is_backed_off_from() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let accepts = Arc::new(AtomicIsize::new(0));
    let counted = Arc::clone(&accepts);
    thread::spawn(move || {
        for stream in listener.incoming() {
            counted.fetch_add(1, Ordering::SeqCst);
            drop(stream);
        }
    });
    let client = spawn(config(format!("http://{}", addr)));
    let handle = client.handle().deadline(2 * SECS).retries(100);
    assert!(handle.call_raw("getblockcount", json!([])).is_err());
    let accepted = accepts.load(Ordering::SeqCst);
    assert!((2..30).contains(&accepted), "{} connects in 2 s", accepted);
}
