use mill_chain::testing::{local_dns_lookups, MockSocks5};
use mill_chain::{
    ChainClient, ChainConfig, ChainError, MillChain, Socks5Config, TlsConfig, TlsVerify,
    TransportConfig,
};
use mill_net::testing::{ChaosConfig, ChaosProxy};
use rcgen::{date_time_ymd, BasicConstraints, Certificate, CertificateParams, IsCa, KeyPair};
use rustls::crypto::ring;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::{ServerConfig, ServerConnection, StreamOwned};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const MS: Duration = Duration::from_millis(1);

struct Ca {
    cert: Certificate,
    key: KeyPair,
}

fn ca_params(name: &str) -> CertificateParams {
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, name);
    params
}

fn ca(name: &str) -> Ca {
    let key = KeyPair::generate().unwrap();
    let cert = ca_params(name).self_signed(&key).unwrap();
    Ca { cert, key }
}

impl Ca {
    fn pem(&self) -> String {
        self.cert.pem()
    }

    fn der(&self) -> CertificateDer<'static> {
        self.cert.der().clone()
    }

    /// An intermediate CA signed by this one.
    fn intermediate(&self, name: &str) -> Ca {
        let key = KeyPair::generate().unwrap();
        let cert = ca_params(name)
            .signed_by(&key, &self.cert, &self.key)
            .unwrap();
        Ca { cert, key }
    }

    fn leaf(&self, names: &[&str], expired: bool) -> Identity {
        let key = KeyPair::generate().unwrap();
        let names: Vec<String> = names.iter().map(|n| n.to_string()).collect();
        let mut params = CertificateParams::new(names).unwrap();
        if expired {
            params.not_before = date_time_ymd(2020, 1, 1);
            params.not_after = date_time_ymd(2021, 1, 1);
        }
        let cert = params.signed_by(&key, &self.cert, &self.key).unwrap();
        Identity {
            chain: vec![cert.der().clone()],
            key,
        }
    }
}

struct Identity {
    chain: Vec<CertificateDer<'static>>,
    key: KeyPair,
}

fn self_signed(names: &[&str]) -> Identity {
    let key = KeyPair::generate().unwrap();
    let names: Vec<String> = names.iter().map(|n| n.to_string()).collect();
    let cert = CertificateParams::new(names)
        .unwrap()
        .self_signed(&key)
        .unwrap();
    Identity {
        chain: vec![cert.der().clone()],
        key,
    }
}

fn sha256(der: &CertificateDer<'_>) -> [u8; 32] {
    Sha256::digest(der.as_ref()).into()
}

/// A PEM file removed on drop.
struct TempFile(PathBuf);

impl TempFile {
    fn new(tag: &str, contents: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "mill-chain-tls-{}-{}-{:?}.pem",
            std::process::id(),
            tag,
            thread::current().id()
        ));
        std::fs::write(&path, contents).unwrap();
        Self(path)
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// A TLS line-JSON-RPC server on 127.0.0.1 that echoes each request's params.
struct TlsServer {
    addr: SocketAddr,
    stop: Arc<std::sync::atomic::AtomicBool>,
    streams: Arc<Mutex<Vec<TcpStream>>>,
    threads: Arc<Mutex<Vec<JoinHandle<()>>>>,
    acceptor: Option<JoinHandle<()>>,
}

impl TlsServer {
    fn start(identity: &Identity) -> Self {
        let key = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(identity.key.serialize_der()));
        let config = ServerConfig::builder_with_provider(Arc::new(ring::default_provider()))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(identity.chain.clone(), key)
            .unwrap();
        Self::serve(Arc::new(config))
    }

    fn serve(config: Arc<ServerConfig>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let streams = Arc::new(Mutex::new(Vec::new()));
        let threads = Arc::new(Mutex::new(Vec::new()));
        let acceptor = {
            let (stop, streams, threads) = (stop.clone(), streams.clone(), threads.clone());
            thread::spawn(move || {
                for stream in listener.incoming() {
                    if stop.load(std::sync::atomic::Ordering::SeqCst) {
                        return;
                    }
                    let Ok(stream) = stream else { continue };
                    streams.lock().unwrap().push(stream.try_clone().unwrap());
                    let config = Arc::clone(&config);
                    threads
                        .lock()
                        .unwrap()
                        .push(thread::spawn(move || echo(config, stream)));
                }
            })
        };
        Self {
            addr,
            stop,
            streams,
            threads,
            acceptor: Some(acceptor),
        }
    }
}

fn echo(config: Arc<ServerConfig>, stream: TcpStream) {
    let Ok(conn) = ServerConnection::new(config) else {
        return;
    };
    let mut tls = BufReader::new(StreamOwned::new(conn, stream));
    let mut line = String::new();
    loop {
        line.clear();
        match tls.read_line(&mut line) {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
        let Ok(request) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let reply = json!({"jsonrpc": "2.0", "id": request["id"], "result": request["params"]});
        let stream = tls.get_mut();
        if stream
            .write_all(format!("{}\n", reply).as_bytes())
            .and_then(|()| stream.flush())
            .is_err()
        {
            return;
        }
    }
}

impl Drop for TlsServer {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::SeqCst);
        let _ = TcpStream::connect(self.addr);
        if let Some(acceptor) = self.acceptor.take() {
            let _ = acceptor.join();
        }
        for stream in self.streams.lock().unwrap().drain(..) {
            let _ = stream.shutdown(Shutdown::Both);
        }
        for thread in self.threads.lock().unwrap().drain(..) {
            let _ = thread.join();
        }
    }
}

fn tls(server_name: Option<&str>, verify: TlsVerify) -> TransportConfig {
    TransportConfig {
        socks5: None,
        tls: Some(TlsConfig {
            server_name: server_name.map(str::to_string),
            verify,
        }),
    }
}

fn spawn(host: &str, port: u16, transport: TransportConfig) -> ChainClient {
    MillChain::spawn(ChainConfig::RawLineJsonRpcVia {
        host: host.to_string(),
        port,
        transport,
        events_capacity: 16,
    })
    .unwrap()
}

fn ping(client: &ChainClient) -> Result<Value, ChainError> {
    client
        .handle()
        .deadline(5000 * MS)
        .call_raw("ping", json!(["tls"]))
}

/// The `Transport` text a rejected handshake gives a call.
fn rejection(server: &TlsServer, transport: TransportConfig) -> String {
    let client = spawn("127.0.0.1", server.addr.port(), transport);
    let result = ping(&client);
    client.shutdown().unwrap();
    match result {
        Err(ChainError::Transport(text)) => {
            assert!(text.starts_with("handshake failed: TLS: "), "{}", text);
            text
        }
        other => panic!("expected a TLS rejection, got {:?}", other),
    }
}

#[test]
fn valid() {
    let root = ca("root");
    let server = TlsServer::start(&root.leaf(&["localhost"], false));
    let ca_file = TempFile::new("valid", &root.pem());
    let transport = tls(Some("localhost"), TlsVerify::CaFile(ca_file.0.clone()));
    let client = spawn("127.0.0.1", server.addr.port(), transport);
    for i in 0..20 {
        let result = client
            .handle()
            .deadline(5000 * MS)
            .call_raw("n", json!([i]));
        assert_eq!(result, Ok(json!([i])));
    }
    client.shutdown().unwrap();
}

#[test]
fn wrong_host_rejected() {
    let root = ca("root");
    let server = TlsServer::start(&root.leaf(&["other.example"], false));
    let ca_file = TempFile::new("wrong-host", &root.pem());
    let text = rejection(
        &server,
        tls(Some("localhost"), TlsVerify::CaFile(ca_file.0.clone())),
    );
    assert!(text.contains("not valid for name"), "{}", text);
}

#[test]
fn expired_rejected() {
    let root = ca("root");
    let server = TlsServer::start(&root.leaf(&["localhost"], true));
    let ca_file = TempFile::new("expired", &root.pem());
    let text = rejection(
        &server,
        tls(Some("localhost"), TlsVerify::CaFile(ca_file.0.clone())),
    );
    assert!(text.contains("certificate expired"), "{}", text);
}

#[test]
fn pin_mismatch_rejected() {
    let served = self_signed(&["localhost"]);
    let other = self_signed(&["localhost"]);
    let server = TlsServer::start(&served);
    let text = rejection(
        &server,
        tls(None, TlsVerify::PinSha256(sha256(&other.chain[0]))),
    );
    assert!(
        text.contains("does not match the pinned SHA-256"),
        "{}",
        text
    );
    let got: String = sha256(&served.chain[0])
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect();
    assert!(text.ends_with(&got), "{}", text);
}

#[test]
fn rejections_are_distinct() {
    let root = ca("root");
    let ca_file = TempFile::new("distinct", &root.pem());
    let by_ca = || tls(Some("localhost"), TlsVerify::CaFile(ca_file.0.clone()));
    let wrong_host = rejection(
        &TlsServer::start(&root.leaf(&["other.example"], false)),
        by_ca(),
    );
    let expired = rejection(&TlsServer::start(&root.leaf(&["localhost"], true)), by_ca());
    let unknown_issuer = rejection(&TlsServer::start(&self_signed(&["localhost"])), by_ca());
    let pin = rejection(
        &TlsServer::start(&self_signed(&["localhost"])),
        tls(None, TlsVerify::PinSha256([7; 32])),
    );
    assert!(
        unknown_issuer.contains("UnknownIssuer"),
        "{}",
        unknown_issuer
    );
    let all = [wrong_host, expired, unknown_issuer, pin];
    for (i, a) in all.iter().enumerate() {
        for b in &all[i + 1..] {
            assert_ne!(a, b);
        }
    }
}

#[test]
fn pinned_self_signed_accepted() {
    let identity = self_signed(&["electrum.invalid"]);
    let pin = sha256(&identity.chain[0]);
    let server = TlsServer::start(&identity);
    let client = spawn(
        "127.0.0.1",
        server.addr.port(),
        tls(None, TlsVerify::PinSha256(pin)),
    );
    assert_eq!(ping(&client), Ok(json!(["tls"])));
    client.shutdown().unwrap();
}

#[test]
fn insecure_for_tests_accepts_any_certificate() {
    let server = TlsServer::start(&self_signed(&["anything.invalid"]));
    let client = spawn(
        "127.0.0.1",
        server.addr.port(),
        tls(None, TlsVerify::InsecureForTests),
    );
    assert_eq!(ping(&client), Ok(json!(["tls"])));
    client.shutdown().unwrap();
}

#[test]
fn tls_over_socks() {
    let root = ca("root");
    let server = TlsServer::start(&root.leaf(&["localhost"], false));
    let ca_file = TempFile::new("socks", &root.pem());
    let proxy = MockSocks5::start(0).unwrap();
    let transport = TransportConfig {
        socks5: Some(Socks5Config {
            proxy: proxy.addr().to_string(),
            isolate: true,
        }),
        tls: Some(TlsConfig {
            server_name: None,
            verify: TlsVerify::CaFile(ca_file.0.clone()),
        }),
    };
    let client = spawn("localhost", server.addr.port(), transport);
    for i in 0..10 {
        let result = client
            .handle()
            .deadline(5000 * MS)
            .call_raw("n", json!([i]));
        assert_eq!(result, Ok(json!([i])));
    }
    let connects = proxy.connects();
    assert_eq!(connects.len(), 1);
    assert_eq!(
        (connects[0].atyp, connects[0].host.as_str()),
        (3, "localhost")
    );
    assert_eq!(local_dns_lookups(&client), 0);
    client.shutdown().unwrap();
}

#[test]
fn tls_one_byte_splits() {
    let root = ca("root");
    let server = TlsServer::start(&root.leaf(&["localhost"], false));
    let ca_file = TempFile::new("splits", &root.pem());
    let chaos = ChaosProxy::start(
        server.addr,
        ChaosConfig {
            split_writes: Some(1),
            ..ChaosConfig::default()
        },
    )
    .unwrap();
    let transport = tls(Some("localhost"), TlsVerify::CaFile(ca_file.0.clone()));
    let client = spawn("127.0.0.1", chaos.addr().port(), transport);
    let big = "x".repeat(20_000);
    for i in 0..5 {
        let result = client
            .handle()
            .deadline(20_000 * MS)
            .call_raw("n", json!([i, big]));
        assert_eq!(result, Ok(json!([i, big])));
    }
    assert_eq!(chaos.connections_seen(), 1);
    client.shutdown().unwrap();
}

#[test]
fn intermediate_chain_accepted() {
    let root = ca("root");
    let intermediate = root.intermediate("intermediate");
    let mut identity = intermediate.leaf(&["localhost"], false);
    identity.chain.push(intermediate.der());
    let server = TlsServer::start(&identity);
    let ca_file = TempFile::new("chain", &root.pem());
    let client = spawn(
        "127.0.0.1",
        server.addr.port(),
        tls(Some("localhost"), TlsVerify::CaFile(ca_file.0.clone())),
    );
    assert_eq!(ping(&client), Ok(json!(["tls"])));
    client.shutdown().unwrap();
    assert_ne!(root.der(), intermediate.der());
}

#[test]
fn onion_names_skip_only_the_name_check() {
    let root = ca("root");
    let ca_file = TempFile::new("onion", &root.pem());
    let onion = "pg6mmjiyjmcrsslvykfwnntlaru7p5svn6y2ymmju6nubxndf4pscryd.onion";
    let server = TlsServer::start(&root.leaf(&["localhost"], false));
    let client = spawn(
        "127.0.0.1",
        server.addr.port(),
        tls(Some(onion), TlsVerify::CaFile(ca_file.0.clone())),
    );
    assert_eq!(ping(&client), Ok(json!(["tls"])));
    client.shutdown().unwrap();

    let expired = TlsServer::start(&root.leaf(&["localhost"], true));
    let text = rejection(
        &expired,
        tls(Some(onion), TlsVerify::CaFile(ca_file.0.clone())),
    );
    assert!(text.contains("certificate expired"), "{}", text);
    let stranger = TlsServer::start(&self_signed(&[onion]));
    let text = rejection(
        &stranger,
        tls(Some(onion), TlsVerify::CaFile(ca_file.0.clone())),
    );
    assert!(text.contains("UnknownIssuer"), "{}", text);
}

#[test]
fn stalled_tls_handshake_is_closed_at_the_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let silent = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut hello = Vec::new();
        let _ = stream.read_to_end(&mut hello);
        (Instant::now(), hello.len())
    });
    let pin = sha256(&self_signed(&["x"]).chain[0]);
    let client = spawn("127.0.0.1", port, tls(None, TlsVerify::PinSha256(pin)));
    let start = Instant::now();
    let result = client
        .handle()
        .deadline(300 * MS)
        .call_raw("ping", json!([]));
    assert_eq!(result, Err(ChainError::Timeout));
    let (closed_at, hello_len) = silent.join().unwrap();
    assert!(hello_len > 0, "no ClientHello was sent");
    assert!(closed_at - start < 1500 * MS, "{:?}", closed_at - start);
    assert_eq!(client.stats().connections_open, 0);
    let start = Instant::now();
    client.shutdown().unwrap();
    assert!(start.elapsed() < 1000 * MS, "{:?}", start.elapsed());
}

#[test]
fn bad_tls_configs_fail_at_spawn() {
    let missing = tls(None, TlsVerify::CaFile("/nonexistent/ca.pem".into()));
    let garbage = TempFile::new("garbage", "not a certificate\n");
    let empty = tls(None, TlsVerify::CaFile(garbage.0.clone()));
    let bad_name = tls(Some("not a hostname"), TlsVerify::WebPki);
    for transport in [missing, empty, bad_name] {
        let result = MillChain::spawn(ChainConfig::RawLineJsonRpcVia {
            host: "127.0.0.1".into(),
            port: 1,
            transport: transport.clone(),
            events_capacity: 1,
        });
        assert!(
            matches!(result, Err(ChainError::Transport(_))),
            "{:?}: {:?}",
            transport,
            result
        );
    }
    let webpki = MillChain::spawn(ChainConfig::RawLineJsonRpcVia {
        host: "127.0.0.1".into(),
        port: 1,
        transport: tls(None, TlsVerify::WebPki),
        events_capacity: 1,
    });
    webpki.unwrap().shutdown().unwrap();
}
