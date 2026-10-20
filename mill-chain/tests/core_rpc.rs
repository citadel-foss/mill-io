//! Against a regtest bitcoind (28.1 in CI). The binary comes from `BITCOIND_EXE`, or
//! `bitcoind` on `PATH`.

use bitcoind::bitcoincore_rpc::{self, RpcApi};
use bitcoind::{BitcoinD, Conf};
use mill_chain::MillChain;
use mill_chain::{ChainClient, ChainConfig, ChainError, ChainHandle, CoreAuth, CoreConfig};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

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
        thread::sleep(Duration::from_millis(20));
    }
}

fn exe() -> String {
    bitcoind::exe_path().expect("set BITCOIND_EXE to a bitcoind 28.1 binary")
}

fn node(extra: &[&'static str]) -> BitcoinD {
    let mut conf = Conf::default();
    conf.args.extend_from_slice(extra);
    BitcoinD::with_conf(exe(), &conf).unwrap()
}

fn core_config(node: &BitcoinD) -> CoreConfig {
    CoreConfig::new(
        node.rpc_url(),
        CoreAuth::Cookie(node.params.cookie_file.clone()),
    )
}

fn connect(node: &BitcoinD, wallet: Option<&str>) -> ChainClient {
    let mut config = core_config(node);
    config.wallet = wallet.map(str::to_string);
    MillChain::spawn(ChainConfig::Core(config)).unwrap()
}

/// Makes calls and records which methods round-tripped.
struct Calls {
    seen: Vec<&'static str>,
}

impl Calls {
    fn chain(&mut self, h: &ChainHandle, method: &'static str, params: Value) -> Value {
        self.seen.push(method);
        h.call_raw(method, params)
            .unwrap_or_else(|e| panic!("{}: {}", method, e))
    }

    fn wallet(&mut self, h: &ChainHandle, method: &'static str, params: Value) -> Value {
        self.seen.push(method);
        h.wallet_call_raw(method, params)
            .unwrap_or_else(|e| panic!("{}: {}", method, e))
    }
}

#[test]
fn every_openswap_rpc_round_trips() {
    let node = node(&["-txindex=1"]);
    let client = connect(&node, Some("mill"));
    let h = client.handle();
    let mut calls = Calls { seen: Vec::new() };

    let created = calls.chain(&h, "createwallet", json!(["mill"]));
    assert_eq!(created["name"], "mill");
    let mine = h.wallet_call_raw("getnewaddress", json!([])).unwrap();
    h.call_raw("generatetoaddress", json!([101, mine])).unwrap();

    let info = calls.chain(&h, "getblockchaininfo", json!([]));
    assert_eq!(info["chain"], "regtest");
    assert_eq!(calls.chain(&h, "getblockcount", json!([])), json!(101));
    let hash = calls.chain(&h, "getblockhash", json!([1]));
    assert_eq!(
        calls.chain(&h, "getblockheader", json!([hash, true]))["height"],
        1
    );
    let raw_header = calls.chain(&h, "getblockheader", json!([hash, false]));
    assert_eq!(raw_header.as_str().map(str::len), Some(160));
    let raw_block = calls.chain(&h, "getblock", json!([hash, 0]));
    assert!(raw_block.as_str().is_some_and(|hex| hex.len() > 160));
    let coinbase = h.call_raw("getblock", json!([hash, 1])).unwrap()["tx"][0].clone();
    assert!(calls
        .chain(&h, "getrawtransaction", json!([coinbase]))
        .is_string());
    assert_eq!(
        calls.chain(&h, "getrawtransaction", json!([coinbase, true]))["txid"],
        coinbase
    );

    let utxos = calls.wallet(&h, "listunspent", json!([1]));
    let utxo = &utxos[0];
    let amount = utxo["amount"].as_f64().unwrap();
    let dest = h.wallet_call_raw("getnewaddress", json!([])).unwrap();
    let outputs = json!({ dest.as_str().unwrap(): ((amount - 0.001) * 1e8).round() / 1e8 });
    let inputs = json!([{"txid": utxo["txid"], "vout": utxo["vout"]}]);
    let unsigned = h
        .call_raw("createrawtransaction", json!([inputs, outputs]))
        .unwrap();
    let signed = h
        .wallet_call_raw("signrawtransactionwithwallet", json!([unsigned]))
        .unwrap();
    assert_eq!(signed["complete"], true);
    let txid = calls.chain(&h, "sendrawtransaction", json!([signed["hex"]]));
    assert!(calls.chain(&h, "getmempoolentry", json!([txid]))["vsize"].is_u64());
    let mempool = calls.chain(&h, "getrawmempool", json!([]));
    assert!(mempool.as_array().unwrap().contains(&txid));
    assert!(calls.chain(&h, "gettxout", json!([txid, 0]))["value"].is_f64());
    assert_eq!(
        calls.chain(&h, "gettxout", json!([utxo["txid"], utxo["vout"]])),
        Value::Null
    );
    let spending = calls.chain(&h, "gettxspendingprevout", json!([inputs]));
    assert_eq!(spending[0]["spendingtxid"], txid);

    let address = format!("addr({})", dest.as_str().unwrap());
    let descriptor =
        h.call_raw("getdescriptorinfo", json!([address])).unwrap()["descriptor"].clone();
    assert_eq!(
        calls.chain(&h, "deriveaddresses", json!([descriptor])),
        json!([dest])
    );
    assert!(calls.chain(&h, "estimatesmartfee", json!([6])).is_object());

    assert!(calls
        .wallet(&h, "listtransactions", json!(["*", 10, 0]))
        .is_array());
    let wallets = calls.chain(&h, "listwallets", json!([]));
    assert!(wallets.as_array().unwrap().contains(&json!("mill")));
    let on_disk = calls.chain(&h, "listwalletdir", json!([]));
    assert!(on_disk["wallets"]
        .as_array()
        .unwrap()
        .iter()
        .any(|w| w["name"] == "mill"));
    assert_eq!(
        calls.wallet(&h, "getaddressinfo", json!([dest]))["ismine"],
        true
    );
    assert_eq!(
        calls.wallet(&h, "getwalletinfo", json!([]))["walletname"],
        "mill"
    );

    let watch = connect(&node, Some("watch"));
    let w = watch.handle();
    calls.chain(
        &w,
        "createwallet",
        json!(["watch", true, true, "", false, true]),
    );
    let imported = calls.wallet(
        &w,
        "importdescriptors",
        json!([[{"desc": descriptor, "timestamp": 0}]]),
    );
    assert_eq!(imported[0]["success"], true, "{}", imported);
    assert_eq!(
        calls.wallet(&w, "rescanblockchain", json!([0]))["start_height"],
        0
    );
    w.call_raw("unloadwallet", json!(["watch"])).unwrap();
    assert_eq!(
        calls.chain(&w, "loadwallet", json!(["watch"]))["name"],
        "watch"
    );

    let network = calls.chain(&h, "getnetworkinfo", json!([]));
    assert!(network["version"].as_u64().unwrap() >= 240_000);
    assert!(calls.chain(&h, "getindexinfo", json!([]))["txindex"].is_object());
    assert!(calls.chain(&h, "getzmqnotifications", json!([])).is_array());

    let mut seen = calls.seen.clone();
    seen.sort_unstable();
    seen.dedup();
    let mut expected = vec![
        "getblockchaininfo",
        "getblockcount",
        "getblockhash",
        "getblockheader",
        "getblock",
        "getrawtransaction",
        "getmempoolentry",
        "gettxout",
        "getrawmempool",
        "gettxspendingprevout",
        "sendrawtransaction",
        "deriveaddresses",
        "estimatesmartfee",
        "listunspent",
        "listtransactions",
        "listwallets",
        "listwalletdir",
        "loadwallet",
        "createwallet",
        "getaddressinfo",
        "importdescriptors",
        "rescanblockchain",
        "getwalletinfo",
        "getnetworkinfo",
        "getindexinfo",
        "getzmqnotifications",
    ];
    expected.sort_unstable();
    assert_eq!(seen, expected);
}

/// A bitcoind started by hand, so it can come back on the same datadir and port.
struct Process(Child);

impl Process {
    fn start(datadir: &Path, port: u16) -> Self {
        let child = Command::new(exe())
            .args([
                "-regtest".to_string(),
                format!("-datadir={}", datadir.display()),
                format!("-rpcport={}", port),
                "-listen=0".to_string(),
                "-printtoconsole=0".to_string(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        Process(child)
    }

    /// Waits until a separate client, reading the cookie afresh, gets an answer.
    fn wait_ready(&self, url: &str, cookie: &Path) {
        let ready = wait_until(60 * SECS, || {
            let auth = bitcoincore_rpc::Auth::CookieFile(cookie.to_path_buf());
            bitcoincore_rpc::Client::new(url, auth)
                .and_then(|probe| probe.get_block_count())
                .is_ok()
        });
        assert!(ready, "bitcoind did not come up");
    }

    fn stop(mut self, handle: &ChainHandle) {
        handle.call_raw("stop", json!([])).unwrap();
        self.0.wait().unwrap();
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn cookie_rotation_after_restart() {
    let datadir = bitcoind::tempfile::tempdir().unwrap();
    let port = bitcoind::get_available_port().unwrap();
    let url = format!("http://127.0.0.1:{}", port);
    let cookie: PathBuf = datadir.path().join("regtest").join(".cookie");

    // Spawned before the node, while the cookie does not exist yet.
    let config = CoreConfig::new(url.clone(), CoreAuth::Cookie(cookie.clone()));
    let client = MillChain::spawn(ChainConfig::Core(config)).unwrap();
    let handle = client.handle();

    let first = Process::start(datadir.path(), port);
    first.wait_ready(&url, &cookie);
    let old_cookie = std::fs::read_to_string(&cookie).unwrap();
    assert_eq!(handle.call_raw("getblockcount", json!([])), Ok(json!(0)));
    first.stop(&handle);

    let second = Process::start(datadir.path(), port);
    second.wait_ready(&url, &cookie);
    let new_cookie = std::fs::read_to_string(&cookie).unwrap();
    assert_ne!(
        old_cookie, new_cookie,
        "the restart did not rotate the cookie"
    );

    let sent = client.stats().requests_sent;
    assert_eq!(handle.call_raw("getblockcount", json!([])), Ok(json!(0)));
    // The first try carried the old cookie and got a 401; the resend used the new one.
    assert_eq!(client.stats().requests_sent, sent + 2);
    assert_eq!(handle.call_raw("getblockcount", json!([])), Ok(json!(0)));
    assert_eq!(client.stats().requests_sent, sent + 3);
    drop(second);
}

#[test]
fn work_queue_full_is_absorbed() {
    let node = node(&["-rpcthreads=2", "-rpcworkqueue=2"]);
    let mut config = core_config(&node);
    // Far more connections than the node's 2 threads and 2 queue slots, so it answers 503.
    config.rpc_connections = 64;
    let client = MillChain::spawn(ChainConfig::Core(config)).unwrap();
    let left = Arc::new(AtomicIsize::new(1000));
    let errors = Arc::new(AtomicUsize::new(0));
    let callers: Vec<_> = (0..64)
        .map(|_| {
            let handle = client.handle();
            let left = Arc::clone(&left);
            let errors = Arc::clone(&errors);
            thread::spawn(move || {
                while left.fetch_sub(1, Ordering::SeqCst) > 0 {
                    match handle.call_raw("getblockhash", json!([0])) {
                        Ok(hash) if hash.is_string() => {}
                        other => {
                            eprintln!("call failed: {:?}", other);
                            errors.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                }
            })
        })
        .collect();
    for caller in callers {
        caller.join().unwrap();
    }
    assert_eq!(errors.load(Ordering::SeqCst), 0);
    let sent = client.stats().requests_sent;
    eprintln!("1000 calls took {} requests", sent);
    assert!(
        sent > 1000,
        "the node never refused a request, so nothing was absorbed"
    );
}

#[test]
fn stopped_node_is_transport_not_absence() {
    let mut node = node(&[]);
    let client = connect(&node, None);
    let handle = client.handle();
    assert!(handle.call_raw("getblockcount", json!([])).is_ok());
    node.stop().unwrap();
    let error = handle
        .call_raw("getrawtransaction", json!(["00".repeat(32)]))
        .unwrap_err();
    assert!(matches!(error, ChainError::Transport(_)), "{:?}", error);
    assert!(!error.is_absence());
}
