//! Captures Bitcoin Core's raw replies to the RPCs mill-chain's typed queries make, for the
//! parse-parity test in `tests/core_parity.rs`.
//!
//! ```text
//! BITCOIND_EXE=/path/to/bitcoind cargo run -p mill-chain --example capture_core_golden
//! ```
//!
//! It starts a regtest node, builds a small wallet history, and writes
//! `tests/golden/core-<version>.json`, or the path given as the first argument.

use bitcoind::bitcoincore_rpc::{self, jsonrpc, Client, RpcApi};
use bitcoind::{BitcoinD, Conf};
use mill_chain::bitcoin::{Address, Amount, OutPoint, Txid};
use serde_json::{json, Value};
use std::error::Error;
use std::thread;
use std::time::{Duration, Instant};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

struct Capture {
    entries: Vec<Value>,
}

impl Capture {
    /// Calls `method` and records the reply, or the error object Core answered with.
    fn call(&mut self, client: &Client, method: &str, params: Value) -> Result<Value> {
        let args = params.as_array().cloned().unwrap_or_default();
        let entry = match client.call::<Value>(method, &args) {
            Ok(result) => json!({"method": method, "params": params, "result": result}),
            Err(bitcoincore_rpc::Error::JsonRpc(jsonrpc::Error::Rpc(e))) => json!({
                "method": method,
                "params": params,
                "error": {"code": e.code, "message": e.message},
            }),
            Err(e) => return Err(e.into()),
        };
        let reply = entry.get("result").cloned().unwrap_or(Value::Null);
        self.entries.push(entry);
        Ok(reply)
    }

    /// Records a reply no regtest node gives, marked as such.
    fn synthetic(&mut self, method: &str, params: Value, result: Value) {
        self.entries.push(json!({
            "method": method,
            "params": params,
            "result": result,
            "synthetic": true,
        }));
    }
}

fn free_port() -> Result<u16> {
    Ok(bitcoind::get_available_port()?)
}

fn wait_until(mut cond: impl FnMut() -> bool) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !cond() {
        if Instant::now() >= deadline {
            return Err("timed out waiting for the wallet".into());
        }
        thread::sleep(Duration::from_millis(20));
    }
    Ok(())
}

fn main() -> Result<()> {
    let exe = bitcoind::exe_path()?;
    let mut conf = Conf::default();
    let zmq = [
        format!("-zmqpubsequence=tcp://0.0.0.0:{}", free_port()?),
        format!("-zmqpubrawtx=tcp://0.0.0.0:{}", free_port()?),
        format!("-zmqpubrawblock=tcp://127.0.0.1:{}", free_port()?),
    ];
    conf.args.push("-txindex=1");
    conf.args.push("-keypool=1");
    for arg in &zmq {
        conf.args.push(arg);
    }
    let node = BitcoinD::with_conf(exe, &conf)?;
    let funder = &node.client;
    let auth = || bitcoincore_rpc::Auth::CookieFile(node.params.cookie_file.clone());
    let root = Client::new(&node.rpc_url(), auth())?;
    let chain = &root;
    let mut cap = Capture {
        entries: Vec::new(),
    };

    let to_self = funder.get_new_address(None, None)?.assume_checked();
    funder.generate_to_address(101, &to_self)?;
    let keys = node.create_wallet("keys")?;
    let spare = node.create_wallet("spare")?;
    drop(spare);
    chain.call::<Value>("unloadwallet", &[json!("spare")])?;

    cap.call(
        chain,
        "createwallet",
        json!(["watch", true, false, null, false, true]),
    )?;
    let watch = Client::new(&format!("{}/wallet/watch", node.rpc_url()), auth())?;
    cap.call(chain, "listwallets", json!([]))?;
    cap.call(chain, "listwalletdir", json!([]))?;
    cap.call(chain, "loadwallet", json!(["spare"]))?;
    chain.call::<Value>("unloadwallet", &[json!("spare")])?;

    let address: Address = keys.get_new_address(None, None)?.assume_checked();
    let info: Value = keys.call("getaddressinfo", &[json!(address)])?;
    let key_desc = format!("wpkh({})", info["pubkey"].as_str().ok_or("no pubkey")?);
    let checksum: Value = chain.call("getdescriptorinfo", &[json!(key_desc)])?;
    let desc = format!(
        "{}#{}",
        key_desc,
        checksum["checksum"].as_str().ok_or("no checksum")?
    );
    let listed: Value = keys.call("listdescriptors", &[])?;
    let ranged = listed["descriptors"]
        .as_array()
        .ok_or("no descriptors")?
        .iter()
        .filter_map(|d| d["desc"].as_str())
        .find(|d| d.starts_with("wpkh(") && d.contains("/1/*)"))
        .ok_or("no ranged wpkh descriptor")?
        .to_string();
    let (key_part, _) = desc.split_once('#').ok_or("no checksum")?;
    let rejected = format!("{}#00000000", key_part);
    cap.call(
        &watch,
        "importdescriptors",
        json!([[
            {"desc": desc, "timestamp": "now"},
            {"desc": ranged, "timestamp": "now", "range": [0, 20]},
            {"desc": rejected, "timestamp": "now"},
        ]]),
    )?;
    cap.call(chain, "deriveaddresses", json!([desc]))?;
    cap.call(chain, "deriveaddresses", json!([ranged, [0, 2]]))?;

    let first = funder.send_to_address(
        &address,
        Amount::ONE_BTC,
        None,
        None,
        None,
        None,
        None,
        None,
    )?;
    let mined = funder.generate_to_address(1, &to_self)?[0];
    let second = funder.send_to_address(
        &address,
        Amount::from_sat(30_000_000),
        None,
        None,
        None,
        None,
        None,
        None,
    )?;
    wait_until(|| {
        watch
            .list_unspent(Some(0), None, None, None, None)
            .is_ok_and(|u| u.len() == 2)
    })?;

    let height = chain.get_block_count()?;
    cap.call(chain, "getblockchaininfo", json!([]))?;
    cap.call(chain, "getblockhash", json!([height]))?;
    cap.call(chain, "getblockhash", json!([height + 1]))?;
    cap.call(chain, "getblockheader", json!([mined, false]))?;
    cap.call(chain, "getblockheader", json!([mined, true]))?;
    cap.call(chain, "getblock", json!([mined, 0]))?;

    let unknown = Txid::from_raw_hash(mill_chain::bitcoin::hashes::Hash::all_zeros());
    for txid in [first, second, unknown] {
        cap.call(chain, "getrawtransaction", json!([txid, false]))?;
        cap.call(chain, "getrawtransaction", json!([txid, true]))?;
        cap.call(chain, "getmempoolentry", json!([txid]))?;
    }
    let first_tx = chain.get_raw_transaction(&first, None)?;
    let second_tx = chain.get_raw_transaction(&second, None)?;
    let paid = |tx: &mill_chain::bitcoin::Transaction| {
        tx.output
            .iter()
            .position(|o| o.script_pubkey == address.script_pubkey())
            .map(|vout| vout as u32)
    };
    let first_out = OutPoint::new(first, paid(&first_tx).ok_or("first pays nothing")?);
    let second_out = OutPoint::new(second, paid(&second_tx).ok_or("second pays nothing")?);
    let coinbase = chain.get_block_info(&mined)?.tx[0];
    for (outpoint, mempool) in [
        (first_out, true),
        (second_out, true),
        (OutPoint::new(coinbase, 0), true),
        (OutPoint::new(first, 9), true),
    ] {
        cap.call(
            chain,
            "gettxout",
            json!([outpoint.txid, outpoint.vout, mempool]),
        )?;
    }
    cap.call(chain, "getrawmempool", json!([false]))?;
    let spent = second_tx.input[0].previous_output;
    if spent.txid != first {
        cap.call(chain, "getrawtransaction", json!([spent.txid, true]))?;
    }
    cap.call(chain, "getblockcount", json!([]))?;
    cap.call(chain, "gettxout", json!([spent.txid, spent.vout, false]))?;
    cap.call(
        chain,
        "gettxspendingprevout",
        json!([[{"txid": spent.txid, "vout": spent.vout}]]),
    )?;
    let hex = mill_chain::bitcoin::consensus::encode::serialize_hex(&second_tx);
    cap.call(chain, "sendrawtransaction", json!([hex]))?;
    let mined_hex = mill_chain::bitcoin::consensus::encode::serialize_hex(&first_tx);
    cap.call(chain, "sendrawtransaction", json!([mined_hex]))?;
    cap.call(chain, "estimatesmartfee", json!([6]))?;
    cap.synthetic(
        "estimatesmartfee",
        json!([2]),
        json!({"feerate": 0.00012345, "blocks": 2}),
    );

    cap.call(&watch, "listunspent", json!([0, 9_999_999]))?;
    cap.call(&watch, "listtransactions", json!(["*", 10, 0, true]))?;
    cap.call(&watch, "getaddressinfo", json!([address.to_string()]))?;
    cap.call(&watch, "rescanblockchain", json!([height]))?;
    cap.call(&watch, "getwalletinfo", json!([]))?;
    cap.call(&watch, "listdescriptors", json!([]))?;
    cap.synthetic(
        "getwalletinfo",
        json!([]),
        json!({"walletname": "watch", "descriptors": true, "scanning": {"duration": 12, "progress": 0.25}}),
    );

    cap.call(chain, "getnetworkinfo", json!([]))?;
    cap.call(chain, "getindexinfo", json!([]))?;
    cap.call(chain, "getzmqnotifications", json!([]))?;

    let version = chain.version()?;
    let name = format!("{}.{}", version / 10_000, (version / 100) % 100);
    let path = std::env::args().nth(1).unwrap_or_else(|| {
        format!(
            "{}/tests/golden/core-{}.json",
            env!("CARGO_MANIFEST_DIR"),
            name
        )
    });
    let golden = json!({"bitcoind": name, "captures": cap.entries});
    std::fs::write(&path, serde_json::to_string_pretty(&golden)? + "\n")?;
    println!(
        "wrote {} captures to {}",
        golden["captures"].as_array().map_or(0, Vec::len),
        path
    );
    Ok(())
}
