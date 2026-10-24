//! Typed Bitcoin Core queries, against a regtest bitcoind (28.1 and 25.x in CI) and
//! against a scripted HTTP mock for answers a real node cannot be made to give. The
//! binary comes from `BITCOIND_EXE`, or `bitcoind` on `PATH`.

use bitcoind::bitcoincore_rpc::{self, RpcApi};
use bitcoind::{BitcoinD, Conf};
use mill_chain::bitcoin::hashes::Hash;
use mill_chain::bitcoin::{Address, Amount, BlockHash, Network, OutPoint, Txid};
use mill_chain::testing::{
    HttpReply, HttpRequest, MockHttpServer, MockReply, MockScript, MockServer,
};
use mill_chain::{ChainClient, ChainConfig, ChainError, CoreAuth, CoreConfig, MillChain};
use mill_chain::{EngineOptions, TxStatus};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
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
    bitcoind::exe_path().expect("set BITCOIND_EXE to a bitcoind binary")
}

fn node(extra: &[&'static str]) -> BitcoinD {
    let mut conf = Conf::default();
    conf.args.extend_from_slice(extra);
    BitcoinD::with_conf(exe(), &conf).unwrap()
}

fn core_config(node: &BitcoinD, wallet: Option<&str>) -> CoreConfig {
    let mut config = CoreConfig::new(
        node.rpc_url(),
        CoreAuth::Cookie(node.params.cookie_file.clone()),
    );
    config.wallet = wallet.map(str::to_string);
    config
}

fn connect(node: &BitcoinD, wallet: Option<&str>) -> ChainClient {
    MillChain::spawn(ChainConfig::Core(core_config(node, wallet))).unwrap()
}

/// Mines `n` blocks to the node's default wallet.
fn mine(node: &BitcoinD, n: u64) -> Vec<BlockHash> {
    let to = node
        .client
        .get_new_address(None, None)
        .unwrap()
        .assume_checked();
    node.client.generate_to_address(n, &to).unwrap()
}

/// Sends `sats` from the node's default wallet to a fresh address of its own.
fn send(node: &BitcoinD, sats: u64) -> Txid {
    let to = node
        .client
        .get_new_address(None, None)
        .unwrap()
        .assume_checked();
    node.client
        .send_to_address(
            &to,
            Amount::from_sat(sats),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap()
}

/// The best block hash the mocks report unless a test says otherwise.
fn mock_tip() -> BlockHash {
    BlockHash::from_byte_array([0x77; 32])
}

/// A Core client of a mock that answers every JSON-RPC request, single or batched, with
/// `answer(method, params)`: a result, or an error code and message. `getbestblockhash`
/// is answered with [`mock_tip`].
fn mock_core<F>(answer: F) -> (MockHttpServer, ChainClient)
where
    F: Fn(&str, &Value) -> Result<Value, (i64, String)> + Send + Sync + 'static,
{
    let server = MockHttpServer::start(move |req: &HttpRequest| rpc_reply(req, &answer)).unwrap();
    let client = spawn_quick(server.url());
    (server, client)
}

/// A Core client of the mock at `url` that backs off for at most 50 ms between connects.
fn spawn_quick(url: String) -> ChainClient {
    let config = CoreConfig::new(
        url,
        CoreAuth::UserPass {
            user: "u".into(),
            pass: "p".into(),
        },
    );
    let options = EngineOptions {
        backoff_max: Duration::from_millis(50),
        ..EngineOptions::default()
    };
    MillChain::spawn_with(ChainConfig::Core(config), options).unwrap()
}

fn rpc_reply<F>(req: &HttpRequest, answer: &F) -> HttpReply
where
    F: Fn(&str, &Value) -> Result<Value, (i64, String)>,
{
    let one = |request: &Value| {
        let method = request["method"].as_str().unwrap_or_default();
        let reply = match method {
            "getbestblockhash" => Ok(json!(mock_tip())),
            _ => answer(method, &request["params"]),
        };
        match reply {
            Ok(result) => json!({"result": result, "error": null, "id": request["id"]}),
            Err((code, message)) => json!({
                "result": null,
                "error": {"code": code, "message": message},
                "id": request["id"],
            }),
        }
    };
    match req.json() {
        Some(Value::Array(requests)) => {
            HttpReply::json(200, &Value::Array(requests.iter().map(one).collect()))
        }
        Some(request) => {
            let reply = one(&request);
            let status = if reply["error"].is_null() { 200 } else { 500 };
            HttpReply::json(status, &reply)
        }
        None => HttpReply::status(400),
    }
}

fn no_such_tx() -> (i64, String) {
    (
        -5,
        "No such mempool or blockchain transaction. Use gettransaction for wallet transactions."
            .into(),
    )
}

fn txid(byte: u8) -> Txid {
    Txid::from_byte_array([byte; 32])
}

fn block_hash(byte: u8) -> BlockHash {
    BlockHash::from_byte_array([byte; 32])
}

#[test]
fn network_mapping_all_chains() {
    for (chain, network) in [
        ("main", Network::Bitcoin),
        ("test", Network::Testnet),
        ("testnet4", Network::Testnet4),
        ("signet", Network::Signet),
        ("regtest", Network::Regtest),
    ] {
        let (_server, client) = mock_core(move |method, _| {
            assert_eq!(method, "getblockchaininfo");
            Ok(json!({"chain": chain, "blocks": 7, "bestblockhash": "11".repeat(32)}))
        });
        assert_eq!(client.handle().network(), Ok(network), "{}", chain);
    }
    // `Network`'s own names are not Core's: openswap's chain_name bug in reverse.
    for chain in ["bitcoin", "testnet", "", "Main"] {
        let (_server, client) = mock_core(move |_, _| {
            Ok(json!({"chain": chain, "blocks": 7, "bestblockhash": "11".repeat(32)}))
        });
        let error = client.handle().network().unwrap_err();
        assert!(
            matches!(error, ChainError::Protocol(_)),
            "{}: {:?}",
            chain,
            error
        );
    }
}

#[test]
fn rebroadcast_is_ok() {
    let node = node(&["-txindex=1"]);
    let client = connect(&node, None);
    let h = client.handle();
    mine(&node, 101);
    let txid = send(&node, 100_000);
    let tx = h.transaction(&txid).unwrap();

    // Still in the mempool: Core answers with the txid.
    assert_eq!(h.broadcast(&tx), Ok(txid));
    assert_eq!(h.tx_status(&txid), Ok(TxStatus::Mempool));

    // In a block: Core refuses with -27, which is a success here.
    mine(&node, 1);
    let sent = client.stats().requests_sent;
    assert_eq!(h.broadcast(&tx), Ok(txid));
    assert_eq!(client.stats().requests_sent, sent + 1);
    assert!(matches!(h.tx_status(&txid), Ok(TxStatus::Confirmed { .. })));

    // A transaction Core rejects is still an error.
    let mut forged = (*tx).clone();
    forged.output[0].value -= Amount::from_sat(1);
    let error = h.broadcast(&forged).unwrap_err();
    assert!(
        matches!(
            error,
            ChainError::Server {
                code: -25 | -26,
                ..
            }
        ),
        "{:?}",
        error
    );
}

#[test]
fn unknown_vs_unreachable() {
    let mut node = node(&["-txindex=1"]);
    let client = connect(&node, None);
    let h = client.handle();
    mine(&node, 1);
    let unknown = txid(0xab);

    assert_eq!(h.tx_status(&unknown), Err(ChainError::NotFound));
    assert_eq!(h.transaction(&unknown), Err(ChainError::NotFound));

    node.stop().unwrap();
    for error in [
        h.tx_status(&unknown).unwrap_err(),
        h.transaction(&unknown).unwrap_err(),
    ] {
        assert!(matches!(error, ChainError::Transport(_)), "{:?}", error);
        assert!(!error.is_absence());
    }
}

#[test]
fn absence_needs_a_complete_txindex() {
    let node = node(&[]);
    let client = connect(&node, None);
    let h = client.handle();
    mine(&node, 101);
    let txid = send(&node, 100_000);
    mine(&node, 1);

    // Mined, but without -txindex Core cannot find it, and says so with -5.
    for error in [
        h.transaction(&txid).unwrap_err(),
        h.tx_status(&txid).unwrap_err(),
    ] {
        assert!(
            matches!(&error, ChainError::Server { code: -5, message } if message.contains("-txindex")),
            "{:?}",
            error
        );
        assert!(!error.is_absence());
    }
}

#[test]
fn chain_queries_on_regtest() {
    let node = node(&["-txindex=1"]);
    let client = connect(&node, None);
    let h = client.handle();
    let hashes = mine(&node, 101);

    let tip = h.tip().unwrap();
    assert_eq!((tip.height, tip.hash), (101, hashes[100]));
    assert_eq!(h.network(), Ok(Network::Regtest));
    assert_eq!(h.block_hash(5), Ok(hashes[4]));
    assert_eq!(h.block_hash(102), Err(ChainError::NotFound));
    assert_eq!(h.header(102), Err(ChainError::NotFound));
    assert_eq!(h.block(102).unwrap_err(), ChainError::NotFound);
    assert_eq!(h.header(5).unwrap().block_hash(), hashes[4]);
    assert_eq!(
        h.header(4).unwrap().block_hash(),
        h.header(5).unwrap().prev_blockhash
    );
    let block = h.block(1).unwrap();
    assert_eq!(block.block_hash(), hashes[0]);

    let coinbase = OutPoint::new(block.txdata[0].compute_txid(), 0);
    let out = h.tx_out(&coinbase, true).unwrap().unwrap();
    assert!(out.coinbase);
    assert_eq!(out.confirmations, 101);
    assert_eq!(out.value, block.txdata[0].output[0].value);
    assert_eq!(out.script_pubkey, block.txdata[0].output[0].script_pubkey);
    assert_eq!(h.tx_out(&OutPoint::new(coinbase.txid, 9), true), Ok(None));

    let txid = send(&node, 100_000);
    let tx = h.transaction(&txid).unwrap();
    assert_eq!(tx.compute_txid(), txid);
    assert!(h.mempool_txids().unwrap().contains(&txid));
    assert_eq!(h.tx_status(&txid), Ok(TxStatus::Mempool));
    let funding = tx.input[0].previous_output;
    // Spent only in the mempool: live without the mempool, gone with it.
    assert!(h.tx_out(&funding, false).unwrap().is_some());
    assert_eq!(h.tx_out(&funding, true), Ok(None));
    let fresh = h.tx_out(&OutPoint::new(txid, 0), true).unwrap().unwrap();
    assert_eq!((fresh.confirmations, fresh.coinbase), (0, false));
    assert_eq!(h.tx_out(&OutPoint::new(txid, 0), false), Ok(None));

    // Regtest has no fee history.
    assert_eq!(h.estimate_fee(6), Err(ChainError::NotFound));

    let mined = mine(&node, 1)[0];
    assert_eq!(
        h.tx_status(&txid),
        Ok(TxStatus::Confirmed {
            height: 102,
            block_hash: mined
        })
    );
    assert!(!h.mempool_txids().unwrap().contains(&txid));
}

#[test]
fn reorged_out_transaction_returns_to_the_mempool() {
    let node = node(&["-txindex=1"]);
    let client = connect(&node, None);
    let h = client.handle();
    mine(&node, 101);
    let txid = send(&node, 100_000);
    let mined = mine(&node, 1)[0];
    let confirmed = TxStatus::Confirmed {
        height: 102,
        block_hash: mined,
    };
    assert_eq!(h.tx_status(&txid), Ok(confirmed));

    node.client.invalidate_block(&mined).unwrap();
    assert_eq!(h.tx_status(&txid), Ok(TxStatus::Mempool));
    node.client.reconsider_block(&mined).unwrap();
    assert_eq!(h.tx_status(&txid), Ok(confirmed));
}

#[test]
fn fee_estimates_convert_and_never_read_zero() {
    let (_server, client) = mock_core(|method, params| {
        assert_eq!(method, "estimatesmartfee");
        match params[0].as_u64() {
            Some(2) => Ok(json!({"feerate": 0.0002, "blocks": 2})),
            Some(3) => Ok(json!({"feerate": 0.00001234, "blocks": 3})),
            Some(144) => {
                Ok(json!({"errors": ["Insufficient data or no feerate found"], "blocks": 0}))
            }
            Some(5) => Ok(json!({"blocks": 5})),
            Some(6) => Ok(json!({"feerate": "0.0002", "blocks": 6})),
            _ => Err((-8, "Invalid conf_target".into())),
        }
    });
    let h = client.handle();
    assert_eq!(h.estimate_fee(2).unwrap().sat_per_vb(), 20.0);
    assert_eq!(h.estimate_fee(3).unwrap().sat_per_vb(), 1.234);
    assert_eq!(h.estimate_fee(144), Err(ChainError::NotFound));
    assert_eq!(h.estimate_fee(5), Err(ChainError::NotFound));
    assert!(matches!(h.estimate_fee(6), Err(ChainError::Protocol(_))));
    assert!(matches!(
        h.estimate_fee(2000),
        Err(ChainError::Server { code: -8, .. })
    ));
}

#[test]
fn absence_needs_both_reads_to_agree() {
    let (_server, client) = mock_core(|method, _| match method {
        "getrawtransaction" => Err(no_such_tx()),
        "getmempoolentry" => Err((-5, "Transaction not in mempool".into())),
        other => panic!("unexpected {}", other),
    });
    let h = client.handle();
    assert_eq!(h.tx_status(&txid(1)), Err(ChainError::NotFound));
    assert_eq!(h.transaction(&txid(1)), Err(ChainError::NotFound));

    // Entered the mempool between the two reads.
    let (_server, client) = mock_core(|method, _| match method {
        "getrawtransaction" => Err(no_such_tx()),
        "getmempoolentry" => Ok(json!({"vsize": 100})),
        other => panic!("unexpected {}", other),
    });
    assert_eq!(client.handle().tx_status(&txid(1)), Ok(TxStatus::Mempool));

    // The mempool read failed: nothing is known.
    let (_server, client) = mock_core(|method, _| match method {
        "getrawtransaction" => Err(no_such_tx()),
        "getmempoolentry" => Err((-32603, "Internal error".into())),
        other => panic!("unexpected {}", other),
    });
    let error = client.handle().tx_status(&txid(1)).unwrap_err();
    assert!(
        matches!(error, ChainError::Server { code: -32603, .. }),
        "{:?}",
        error
    );
}

#[test]
fn absence_while_a_block_connects_is_read_again() {
    // The first reads straddle a new block, which Core's txindex has not caught up with;
    // the second find the transaction in it.
    let tx = txid(1);
    let in_block = block_hash(2);
    let tips = Arc::new(AtomicUsize::new(0));
    let reads = Arc::clone(&tips);
    let restless = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let moving = Arc::clone(&restless);
    let server = MockHttpServer::start(move |req: &HttpRequest| {
        let body = req.json().unwrap();
        let single = !body.is_array();
        let calls = body.as_array().cloned().unwrap_or_else(|| vec![body.clone()]);
        let replies: Vec<Value> = calls
            .iter()
            .map(|call| {
                let first = reads.load(Ordering::SeqCst) < 2;
                let (result, error) = match call["method"].as_str().unwrap() {
                    "getbestblockhash" => {
                        let n = reads.fetch_add(1, Ordering::SeqCst);
                        let tip = match (moving.load(Ordering::SeqCst), n) {
                            (true, n) => n as u8,
                            (false, 0) => 5,
                            (false, _) => 6,
                        };
                        (json!(block_hash(tip)), Value::Null)
                    }
                    "getrawtransaction" if first => (Value::Null, json!({"code": -5,
                        "message": "No such mempool or blockchain transaction. Use gettransaction for wallet transactions."})),
                    "getrawtransaction" if call["params"][1] == json!(true) => (
                        json!({"txid": tx, "blockhash": in_block, "confirmations": 1}),
                        Value::Null,
                    ),
                    "getrawtransaction" => (Value::Null, json!({"code": -5,
                        "message": "No such mempool or blockchain transaction. Use gettransaction for wallet transactions."})),
                    "getmempoolentry" => (Value::Null, json!({"code": -5, "message": "Transaction not in mempool"})),
                    "getblockheader" => (json!({"height": 9, "confirmations": 1}), Value::Null),
                    other => panic!("unexpected {}", other),
                };
                json!({"result": result, "error": error, "id": call["id"]})
            })
            .collect();
        match single {
            true => HttpReply::json(200, &replies[0]),
            false => HttpReply::json(200, &Value::Array(replies)),
        }
    })
    .unwrap();
    let client = spawn_quick(server.url());
    assert_eq!(
        client.handle().tx_status(&tx),
        Ok(TxStatus::Confirmed {
            height: 9,
            block_hash: in_block
        })
    );
    // With the tip still, absence stands; with a tip that keeps moving, it never settles.
    assert_eq!(client.handle().transaction(&tx), Err(ChainError::NotFound));
    restless.store(true, Ordering::SeqCst);
    let error = client.handle().transaction(&tx).unwrap_err();
    assert!(matches!(error, ChainError::Protocol(_)), "{:?}", error);
}

#[test]
fn tx_status_rereads_a_transaction_that_moved() {
    let tx = txid(1);
    let in_block = block_hash(2);
    // First read: in the mempool by getrawtransaction, gone by getmempoolentry (mined
    // in between). Second read: confirmed.
    let reads = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&reads);
    let (_server, client) = mock_core(move |method, _| match method {
        "getrawtransaction" => {
            if seen.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(json!({"txid": tx, "hex": ""}))
            } else {
                Ok(json!({"txid": tx, "blockhash": in_block, "confirmations": 1}))
            }
        }
        "getmempoolentry" => Err((-5, "Transaction not in mempool".into())),
        "getblockheader" => Ok(json!({"hash": in_block, "height": 77, "confirmations": 1})),
        other => panic!("unexpected {}", other),
    });
    assert_eq!(
        client.handle().tx_status(&tx),
        Ok(TxStatus::Confirmed {
            height: 77,
            block_hash: in_block
        })
    );
    assert_eq!(reads.load(Ordering::SeqCst), 2);

    // Under a block that left the best chain, and not in the mempool: not found.
    let (_server, client) = mock_core(move |method, _| match method {
        "getrawtransaction" => Ok(json!({"txid": tx, "blockhash": in_block, "confirmations": 0})),
        "getmempoolentry" => Err((-5, "Transaction not in mempool".into())),
        other => panic!("unexpected {}", other),
    });
    assert_eq!(client.handle().tx_status(&tx), Err(ChainError::NotFound));

    // A block hash Core cannot place, or a mempool entry of the wrong shape.
    let (_server, client) = mock_core(move |method, _| match method {
        "getrawtransaction" => Ok(json!({"txid": tx, "blockhash": in_block})),
        "getmempoolentry" => Err((-5, "Transaction not in mempool".into())),
        other => panic!("unexpected {}", other),
    });
    let error = client.handle().tx_status(&tx).unwrap_err();
    assert!(matches!(error, ChainError::Protocol(_)), "{:?}", error);
    let (_server, client) = mock_core(move |method, _| match method {
        "getrawtransaction" => Err(no_such_tx()),
        "getmempoolentry" => Ok(json!(null)),
        other => panic!("unexpected {}", other),
    });
    let error = client.handle().tx_status(&tx).unwrap_err();
    assert!(matches!(error, ChainError::Protocol(_)), "{:?}", error);

    // Never settles: an error, not absence.
    let (_server, client) = mock_core(move |method, _| match method {
        "getrawtransaction" => Ok(json!({"txid": tx, "hex": ""})),
        "getmempoolentry" => Err((-5, "Transaction not in mempool".into())),
        other => panic!("unexpected {}", other),
    });
    let error = client.handle().tx_status(&tx).unwrap_err();
    assert!(matches!(error, ChainError::Protocol(_)), "{:?}", error);

    // Confirmed by getrawtransaction, reorged out before getblockheader, then read again:
    // under the stale block and back in the mempool.
    let reads = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&reads);
    let (_server, client) = mock_core(move |method, _| match method {
        "getrawtransaction" => {
            let confirmations = if seen.fetch_add(1, Ordering::SeqCst) == 0 {
                1
            } else {
                0
            };
            Ok(json!({"txid": tx, "blockhash": in_block, "confirmations": confirmations}))
        }
        "getmempoolentry" => Ok(json!({"vsize": 100})),
        "getblockheader" => Ok(json!({"hash": in_block, "height": 77, "confirmations": -1})),
        other => panic!("unexpected {}", other),
    });
    assert_eq!(client.handle().tx_status(&tx), Ok(TxStatus::Mempool));
    assert_eq!(reads.load(Ordering::SeqCst), 2);
}

/// Every typed query, with what a mock answers when it works.
fn every_query(h: &mill_chain::ChainHandle) -> Vec<(&'static str, ChainError)> {
    let tx = txid(1);
    let mut errors = Vec::new();
    let mut check = |name: &'static str, result: Result<(), ChainError>| {
        if let Err(e) = result {
            errors.push((name, e));
        } else {
            panic!("{} succeeded", name);
        }
    };
    check("tip", h.tip().map(drop));
    check("network", h.network().map(drop));
    check("block_hash", h.block_hash(1).map(drop));
    check("header", h.header(1).map(drop));
    check("block", h.block(1).map(drop));
    check("transaction", h.transaction(&tx).map(drop));
    check("tx_status", h.tx_status(&tx).map(drop));
    check("tx_out", h.tx_out(&OutPoint::new(tx, 0), true).map(drop));
    check(
        "broadcast",
        h.broadcast(&mill_chain::bitcoin::constants::genesis_block(Network::Regtest).txdata[0])
            .map(drop),
    );
    check("estimate_fee", h.estimate_fee(6).map(drop));
    check("mempool_txids", h.mempool_txids().map(drop));
    errors
}

#[test]
fn failures_are_never_absence() {
    // The connection drops before any answer.
    let server = MockHttpServer::start(|_| HttpReply::drop_connection()).unwrap();
    let client = spawn_quick(server.url());
    for (name, error) in every_query(&client.handle()) {
        assert!(
            matches!(error, ChainError::Transport(_)),
            "{}: {:?}",
            name,
            error
        );
    }

    // Replies of the wrong shape.
    let (_server, client) = mock_core(|method, _| match method {
        "getblockhash" => Ok(json!("not a hash")),
        "gettxout" => Ok(json!({"value": "1.0"})),
        "getmempoolentry" => Ok(json!({})),
        _ => Ok(json!(42)),
    });
    for (name, error) in every_query(&client.handle()) {
        assert!(
            matches!(error, ChainError::Protocol(_)),
            "{}: {:?}",
            name,
            error
        );
    }

    // -5 from a node that cannot tell: no txindex.
    let (_server, client) = mock_core(|method, _| {
        match method {
        "getrawtransaction" => Err((
            -5,
            "No such mempool transaction. Use -txindex or provide a block hash to enable blockchain transaction queries. Use gettransaction for wallet transactions.".into(),
        )),
        "getmempoolentry" => Err((-5, "Transaction not in mempool".into())),
        other => panic!("unexpected {}", other),
    }
    });
    let h = client.handle();
    for error in [
        h.transaction(&txid(1)).unwrap_err(),
        h.tx_status(&txid(1)).unwrap_err(),
    ] {
        assert!(
            matches!(error, ChainError::Server { code: -5, .. }),
            "{:?}",
            error
        );
        assert!(!error.is_absence());
    }

    // A node that never answers within the deadline.
    let server =
        MockHttpServer::start(|req| HttpReply::result(req, json!(1)).after(Duration::from_secs(5)))
            .unwrap();
    let client = spawn_quick(server.url());
    let h = client.handle().deadline(Duration::from_millis(100));
    for (name, error) in every_query(&h) {
        assert_eq!(error, ChainError::Timeout, "{}", name);
    }
}

#[test]
fn a_returned_object_must_be_the_one_asked_for() {
    let genesis = mill_chain::bitcoin::constants::genesis_block(Network::Regtest);
    let header_hex = mill_chain::bitcoin::consensus::encode::serialize_hex(&genesis.header);
    let block_hex = mill_chain::bitcoin::consensus::encode::serialize_hex(&genesis);
    let tx_hex = mill_chain::bitcoin::consensus::encode::serialize_hex(&genesis.txdata[0]);
    let (_server, client) = mock_core(move |method, _| match method {
        "getblockhash" => Ok(json!(block_hash(9))),
        "getblockheader" => Ok(json!(header_hex)),
        "getblock" => Ok(json!(block_hex)),
        "getrawtransaction" => Ok(json!(tx_hex)),
        "sendrawtransaction" => Ok(json!(txid(3))),
        other => panic!("unexpected {}", other),
    });
    let h = client.handle();
    assert!(matches!(h.header(1), Err(ChainError::Protocol(_))));
    assert!(matches!(h.block(1), Err(ChainError::Protocol(_))));
    assert!(matches!(
        h.transaction(&txid(1)),
        Err(ChainError::Protocol(_))
    ));
    assert!(matches!(
        h.broadcast(&genesis.txdata[0]),
        Err(ChainError::Protocol(_))
    ));
}

#[test]
fn pruned_blocks_are_unsupported_not_absent() {
    let (_server, client) = mock_core(|method, _| match method {
        "getblockhash" => Ok(json!(block_hash(9))),
        "getblock" => Err((-1, "Block not available (pruned data)".into())),
        other => panic!("unexpected {}", other),
    });
    let error = client.handle().block(3).unwrap_err();
    assert!(matches!(error, ChainError::Unsupported(_)), "{:?}", error);
}

#[test]
fn one_deadline_covers_a_whole_query() {
    let genesis = mill_chain::bitcoin::constants::genesis_block(Network::Regtest);
    let hash = genesis.block_hash();
    let header_hex = mill_chain::bitcoin::consensus::encode::serialize_hex(&genesis.header);
    let server = MockHttpServer::start(move |req| {
        let result = match req.method().as_deref() {
            Some("getblockhash") => json!(hash),
            _ => json!(header_hex),
        };
        HttpReply::result(req, result).after(Duration::from_millis(300))
    })
    .unwrap();
    let client = spawn_quick(server.url());
    // Each of header's two calls fits in 500 ms; both together do not.
    assert_eq!(
        client
            .handle()
            .deadline(Duration::from_millis(500))
            .header(0),
        Err(ChainError::Timeout)
    );
    assert_eq!(
        client
            .handle()
            .deadline(Duration::from_secs(5))
            .header(0)
            .unwrap()
            .block_hash(),
        hash
    );
}

#[test]
fn typed_queries_need_bitcoin_core() {
    let server = MockServer::start(MockScript::new(|_| MockReply::Never)).unwrap();
    let client = MillChain::spawn_with(
        ChainConfig::RawLineJsonRpc {
            addr: server.addr(),
            events_capacity: 16,
        },
        EngineOptions::default(),
    )
    .unwrap();
    let h = client.handle();
    for (name, error) in every_query(&h) {
        assert!(
            matches!(error, ChainError::Unsupported(_)),
            "{}: {:?}",
            name,
            error
        );
    }
    assert_eq!(server.requests_seen().len(), 0);
}

/// A wallet holding keys, apart from the default wallet that funds and mines, so the
/// default wallet never spends what the watch-only wallet watches.
fn key_holder(node: &BitcoinD) -> bitcoincore_rpc::Client {
    node.create_wallet("keys").unwrap()
}

/// A descriptor for a fresh address of `keys`, with its checksum, and the address.
fn watched_key(keys: &bitcoincore_rpc::Client) -> (String, Address) {
    let address = keys.get_new_address(None, None).unwrap().assume_checked();
    let info: Value = keys.call("getaddressinfo", &[json!(address)]).unwrap();
    let pubkey = info["pubkey"].as_str().unwrap();
    let desc = format!("wpkh({})", pubkey);
    let info: Value = keys.call("getdescriptorinfo", &[json!(desc)]).unwrap();
    let checksum = info["checksum"].as_str().unwrap();
    (format!("{}#{}", desc, checksum), address)
}

fn with_checksum(node: &BitcoinD, desc: &str) -> String {
    let info: Value = node
        .client
        .call("getdescriptorinfo", &[json!(desc)])
        .unwrap();
    format!("{}#{}", desc, info["checksum"].as_str().unwrap())
}

/// The default wallet's public receive descriptor for wpkh, ranged.
fn ranged_descriptor(node: &BitcoinD) -> String {
    let listed: Value = node.client.call("listdescriptors", &[]).unwrap();
    listed["descriptors"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["desc"].as_str().unwrap().to_string())
        .find(|d| d.starts_with("wpkh(") && d.contains("/0/*)"))
        .unwrap()
}

#[test]
fn wallet_bootstrap_idempotent() {
    let node = node(&[]);
    let client = connect(&node, Some("watch"));
    let h = client.handle();
    let w = h.core_wallet().unwrap();
    let requests = || client.stats().requests_sent;

    // Neither loaded nor on disk: listwallets, listwalletdir, createwallet.
    let before = requests();
    w.prepare("watch").unwrap();
    assert_eq!(requests() - before, 3);
    let info = h.wallet_call_raw("getwalletinfo", json!([])).unwrap();
    assert_eq!(info["walletname"], "watch");
    assert_eq!(info["private_keys_enabled"], false);
    assert_eq!(info["descriptors"], true);

    // Loaded: listwallets only.
    let before = requests();
    w.prepare("watch").unwrap();
    assert_eq!(requests() - before, 1);

    // On disk only: listwallets, listwalletdir, loadwallet.
    h.call_raw("unloadwallet", json!(["watch"])).unwrap();
    assert!(matches!(
        h.wallet_call_raw("getwalletinfo", json!([])),
        Err(ChainError::Server { code: -18, .. })
    ));
    let before = requests();
    w.prepare("watch").unwrap();
    assert_eq!(requests() - before, 3);
    let loaded = h.call_raw("listwallets", json!([])).unwrap();
    assert!(loaded.as_array().unwrap().contains(&json!("watch")));
    assert_eq!(
        h.wallet_call_raw("getwalletinfo", json!([])).unwrap()["private_keys_enabled"],
        false
    );
}

#[test]
fn import_descriptor_failure_is_an_error() {
    let node = node(&[]);
    let client = connect(&node, Some("watch"));
    let w = client.handle().core_wallet().unwrap();
    w.prepare("watch").unwrap();
    let (desc, address) = watched_key(&key_holder(&node));
    let good = json!({"desc": desc, "timestamp": "now"});
    let (key, _) = desc.split_once('#').unwrap();
    let bad = json!({"desc": format!("{}#00000000", key), "timestamp": "now"});
    let unchecked = json!({"desc": "wpkh(not-a-key)", "timestamp": "now"});

    for reqs in [
        vec![bad.clone()],
        vec![unchecked.clone()],
        vec![good.clone(), bad.clone()],
    ] {
        let error = w.import_descriptors(&reqs).unwrap_err();
        let ChainError::Server { message, .. } = &error else {
            panic!("{:?}", error);
        };
        let rejected = reqs
            .iter()
            .filter(|r| **r != good)
            .map(|r| r["desc"].as_str().unwrap());
        for desc in rejected {
            assert!(message.contains(desc), "{} does not name {}", message, desc);
        }
        assert!(!error.is_absence());
    }

    // The valid descriptor of the mixed batch was imported all the same.
    assert!(w.address_info(&address).unwrap().is_mine);
    w.import_descriptors(&[good]).unwrap();
    w.import_descriptors(&[]).unwrap();
}

#[test]
fn wallet_views_on_regtest() {
    let node = node(&["-txindex=1"]);
    let client = connect(&node, Some("watch"));
    let h = client.handle();
    let w = h.core_wallet().unwrap();
    w.prepare("watch").unwrap();
    mine(&node, 101);
    let (desc, address) = watched_key(&key_holder(&node));
    w.import_descriptors(&[json!({"desc": desc, "timestamp": "now"})])
        .unwrap();

    let info = w.address_info(&address).unwrap();
    assert!(
        info.is_mine && info.solvable && !info.is_watch_only,
        "{:?}",
        info
    );
    assert!(info.desc.as_deref().is_some_and(|d| d.starts_with("wpkh(")));
    let elsewhere = node
        .client
        .get_new_address(None, None)
        .unwrap()
        .assume_checked();
    assert!(!w.address_info(&elsewhere).unwrap().is_mine);
    assert_eq!(
        w.derive_addresses(&desc, None).unwrap(),
        vec![address.as_unchecked().clone()]
    );

    let first = node
        .client
        .send_to_address(
            &address,
            Amount::ONE_BTC,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
    // minconf 0 includes the mempool. The wallet hears of mempool transactions
    // asynchronously.
    assert!(wait_until(10 * SECS, || w
        .list_unspent(0, None)
        .unwrap()
        .len()
        == 1));
    let unconfirmed = w.list_unspent(0, None).unwrap();
    assert_eq!(unconfirmed[0].confirmations, 0);
    assert_eq!(w.list_unspent(1, None), Ok(Vec::new()));
    mine(&node, 1);
    let second = node
        .client
        .send_to_address(
            &address,
            Amount::from_sat(30_000),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

    let confirmed = w.list_unspent(1, None).unwrap();
    assert_eq!(confirmed.len(), 1);
    let utxo = &confirmed[0];
    assert_eq!(utxo.outpoint.txid, first);
    assert_eq!(utxo.value, Amount::ONE_BTC);
    assert_eq!(utxo.script_pubkey, address.script_pubkey());
    assert_eq!(utxo.confirmations, 1);
    assert_eq!(utxo.address.as_ref(), Some(address.as_unchecked()));
    assert!(utxo.descriptor.is_some());
    // A descriptor wallet calls whatever it tracks spendable, keys or not.
    assert!(utxo.spendable && utxo.solvable && utxo.safe);
    assert_eq!(
        (utxo.witness_script.as_ref(), utxo.redeem_script.as_ref()),
        (None, None)
    );
    let tx = h.transaction(&first).unwrap();
    assert_eq!(
        tx.output[utxo.outpoint.vout as usize].value,
        Amount::ONE_BTC
    );
    assert_eq!(w.list_unspent(0, Some(0)).unwrap().len(), 1);
    assert_eq!(w.list_unspent(0, None).unwrap().len(), 2);

    // Oldest first, as Core orders them.
    let history = w.list_transactions(10, 0).unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!((history[0].txid, history[1].txid), (first, second));
    let received = &history[0];
    assert_eq!(received.category, mill_chain::TxCategory::Receive);
    assert_eq!(received.amount, mill_chain::bitcoin::SignedAmount::ONE_BTC);
    assert_eq!(received.confirmations, 1);
    assert_eq!(received.block_height, Some(102));
    assert_eq!(received.block_hash, Some(h.block_hash(102).unwrap()));
    assert_eq!(received.address.as_ref(), Some(address.as_unchecked()));
    assert_eq!(received.fee, None);
    assert_eq!(history[1].confirmations, 0);
    assert_eq!(history[1].block_hash, None);
    assert_eq!(w.list_transactions(1, 0).unwrap()[0].txid, second);
    assert_eq!(w.list_transactions(1, 1).unwrap()[0].txid, first);

    assert_eq!(w.scanning_status(), Ok(None));
    w.rescan_from(0).unwrap();
    assert_eq!(w.scanning_status(), Ok(None));
    assert_eq!(w.list_unspent(0, None).unwrap().len(), 2);
}

#[test]
fn imported_ranges_are_checked_from_zero() {
    // Core tops a ranged descriptor up to the keypool size; one keeps the range as imported.
    let node = node(&["-keypool=1"]);
    let client = connect(&node, Some("watch"));
    let w = client.handle().core_wallet().unwrap();
    w.prepare("watch").unwrap();
    let ranged = ranged_descriptor(&node);
    assert!(!w.imported_range_covers(&ranged, 0).unwrap());
    w.import_descriptors(&[json!({"desc": ranged, "timestamp": "now", "range": [0, 20]})])
        .unwrap();
    assert!(w.imported_range_covers(&ranged, 20).unwrap());
    assert!(!w.imported_range_covers(&ranged, 21).unwrap());
    assert_eq!(w.derive_addresses(&ranged, Some([0, 2])).unwrap().len(), 3);

    let change = ranged.replace("/0/*)", "/1/*)");
    let change = with_checksum(&node, change.split_once('#').unwrap().0);
    w.import_descriptors(&[json!({"desc": change, "timestamp": "now", "range": [5, 50]})])
        .unwrap();
    assert!(!w.imported_range_covers(&change, 10).unwrap());
}

/// A Core client with wallet `wallet` of a mock answering with `answer`, which also sees the
/// request path.
fn mock_wallet<F>(wallet: Option<&str>, answer: F) -> (MockHttpServer, ChainClient)
where
    F: Fn(&str, &str, &Value) -> Result<Value, (i64, String)> + Send + Sync + 'static,
{
    let server = MockHttpServer::start(move |req: &HttpRequest| {
        let path = req.path.clone();
        rpc_reply(req, &|method: &str, params: &Value| {
            answer(&path, method, params)
        })
    })
    .unwrap();
    let mut config = CoreConfig::new(
        server.url(),
        CoreAuth::UserPass {
            user: "u".into(),
            pass: "p".into(),
        },
    );
    config.wallet = wallet.map(str::to_string);
    let client = MillChain::spawn(ChainConfig::Core(config)).unwrap();
    (server, client)
}

fn methods(server: &MockHttpServer) -> Vec<String> {
    server
        .requests_seen()
        .iter()
        .filter_map(HttpRequest::method)
        .collect()
}

#[test]
fn wallet_calls_need_a_configured_wallet() {
    let (server, client) = mock_wallet(None, |path, method, _| {
        assert_eq!(path, "/");
        match method {
            "deriveaddresses" => Ok(json!(["bcrt1qq6hag67dl53wl99vzg42z8eyzfz2xlkvwk6f7m"])),
            "listwallets" => Ok(json!(["other"])),
            "listwalletdir" => Ok(json!({"wallets": [{"name": "other"}]})),
            "createwallet" => Ok(json!({"name": "w"})),
            other => panic!("unexpected {}", other),
        }
    });
    let w = client.handle().core_wallet().unwrap();
    w.prepare("w").unwrap();
    assert_eq!(
        methods(&server),
        ["listwallets", "listwalletdir", "createwallet"]
    );
    assert_eq!(
        server.requests_seen()[2].json().unwrap()["params"],
        json!(["w", true, false, null, false, true])
    );

    let a = Address::p2wpkh(
        &mill_chain::bitcoin::CompressedPublicKey::from_slice(&[2; 33]).unwrap(),
        Network::Regtest,
    );
    let errors = [
        w.list_unspent(0, None).map(drop),
        w.list_transactions(10, 0).map(drop),
        w.address_info(&a).map(drop),
        w.import_descriptors(&[json!({"desc": "x"})]),
        w.rescan_from(0),
        w.scanning_status().map(drop),
        w.imported_range_covers("x", 1).map(drop),
    ];
    for error in errors {
        assert!(
            matches!(error, Err(ChainError::Unsupported(_))),
            "{:?}",
            error
        );
    }
    assert_eq!(server.requests_seen().len(), 3);
    // A node RPC, which needs no wallet.
    assert_eq!(w.derive_addresses("addr(x)", None).unwrap().len(), 1);
    assert_eq!(server.requests_seen()[3].path, "/");
}

#[test]
fn wallet_bootstrap_survives_races() {
    // Created elsewhere between listwalletdir and createwallet.
    let (server, client) = mock_wallet(Some("w"), |_, method, _| {
        match method {
        "listwallets" => Ok(json!([])),
        "listwalletdir" => Ok(json!({"wallets": []})),
        "createwallet" => Err((-4, "Wallet file verification failed. Failed to create database path '/x/w'. Database already exists.".into())),
        "loadwallet" => Ok(json!({"name": "w"})),
        other => panic!("unexpected {}", other),
    }
    });
    client.handle().core_wallet().unwrap().prepare("w").unwrap();
    assert_eq!(
        methods(&server),
        ["listwallets", "listwalletdir", "createwallet", "loadwallet"]
    );

    // Loaded elsewhere between listwallets and loadwallet.
    let (_server, client) = mock_wallet(Some("w"), |_, method, _| match method {
        "listwallets" => Ok(json!([])),
        "listwalletdir" => Ok(json!({"wallets": [{"name": "w"}]})),
        "loadwallet" => Err((-35, "Wallet \"w\" is already loaded.".into())),
        other => panic!("unexpected {}", other),
    });
    client.handle().core_wallet().unwrap().prepare("w").unwrap();

    // Any other failure is reported.
    let (_server, client) = mock_wallet(Some("w"), |_, method, _| match method {
        "listwallets" => Ok(json!([])),
        "listwalletdir" => Ok(json!({"wallets": [{"name": "w"}]})),
        "loadwallet" => Err((-4, "Wallet file verification failed.".into())),
        other => panic!("unexpected {}", other),
    });
    let error = client
        .handle()
        .core_wallet()
        .unwrap()
        .prepare("w")
        .unwrap_err();
    assert!(
        matches!(error, ChainError::Server { code: -4, .. }),
        "{:?}",
        error
    );

    // A malformed listing is not "no such wallet".
    let (server, client) = mock_wallet(Some("w"), |_, method, _| match method {
        "listwallets" => Ok(json!({"w": true})),
        other => panic!("unexpected {}", other),
    });
    let error = client
        .handle()
        .core_wallet()
        .unwrap()
        .prepare("w")
        .unwrap_err();
    assert!(matches!(error, ChainError::Protocol(_)), "{:?}", error);
    assert_eq!(methods(&server), ["listwallets"]);
}

#[test]
fn wallet_failures_are_errors() {
    let (server, client) = mock_wallet(Some("w"), |path, method, params| {
        let node_rpc = method == "deriveaddresses";
        assert_eq!(path == "/", node_rpc, "{} on {}", method, path);
        match (method, params[0].as_array().map(Vec::len)) {
            ("importdescriptors", Some(1)) => match params[0][0]["desc"].as_str() {
                Some("short") => Ok(json!([])),
                Some("no-success") => Ok(json!([{}])),
                Some("bare") => Ok(json!([{"success": false}])),
                _ => Ok(json!([{"success": true, "warnings": ["Range not given"]}])),
            },
            ("listtransactions", _) => Ok(json!([{"txid": "11".repeat(32), "vout": 0,
                "category": "conflicted", "amount": 1.0, "confirmations": 1, "time": 1}])),
            ("listunspent", _) => Ok(json!([{"txid": "11".repeat(32), "vout": 0}])),
            ("getwalletinfo", _) => Ok(json!({"walletname": "w"})),
            ("rescanblockchain", _) => Ok(json!(null)),
            ("deriveaddresses", _) => Ok(json!(["not an address"])),
            ("getaddressinfo", _) => Ok(json!({"ismine": "yes"})),
            (other, _) => panic!("unexpected {}", other),
        }
    });
    let w = client.handle().core_wallet().unwrap();
    let import = |desc: &str| w.import_descriptors(&[json!({"desc": desc})]);
    assert!(matches!(import("short"), Err(ChainError::Protocol(_))));
    assert!(matches!(import("no-success"), Err(ChainError::Protocol(_))));
    assert!(matches!(
        import("bare"),
        Err(ChainError::Server { code: -1, message }) if message.contains("bare")
    ));
    assert_eq!(import("fine"), Ok(()));
    assert!(matches!(
        w.list_transactions(10, 0),
        Err(ChainError::Protocol(_))
    ));
    assert!(matches!(
        w.list_unspent(0, None),
        Err(ChainError::Protocol(_))
    ));
    assert!(matches!(w.scanning_status(), Err(ChainError::Protocol(_))));
    assert!(matches!(w.rescan_from(0), Err(ChainError::Protocol(_))));
    assert!(matches!(
        w.derive_addresses("x", None),
        Err(ChainError::Protocol(_))
    ));
    let a = Address::p2wpkh(
        &mill_chain::bitcoin::CompressedPublicKey::from_slice(&[2; 33]).unwrap(),
        Network::Regtest,
    );
    assert!(matches!(w.address_info(&a), Err(ChainError::Protocol(_))));
    // Not a descriptor wallet, as far as the reply says.
    assert!(matches!(
        w.imported_range_covers("x", 1),
        Err(ChainError::Unsupported(_))
    ));
    assert!(methods(&server).iter().all(|m| m != "listdescriptors"));

    let server = MockHttpServer::start(|_| HttpReply::drop_connection()).unwrap();
    let mut config = CoreConfig::new(
        server.url(),
        CoreAuth::UserPass {
            user: "u".into(),
            pass: "p".into(),
        },
    );
    config.wallet = Some("w".into());
    let options = EngineOptions {
        backoff_max: Duration::from_millis(50),
        ..EngineOptions::default()
    };
    let client = MillChain::spawn_with(ChainConfig::Core(config), options).unwrap();
    let w = client.handle().core_wallet().unwrap();
    let errors = [
        w.prepare("w"),
        w.list_unspent(0, None).map(drop),
        w.list_transactions(10, 0).map(drop),
        w.address_info(&a).map(drop),
        w.derive_addresses("addr(x)", None).map(drop),
        w.import_descriptors(&[json!({"desc": "x"})]),
        w.rescan_from(0),
        w.scanning_status().map(drop),
        w.imported_range_covers("x", 1).map(drop),
    ];
    for error in errors {
        assert!(
            matches!(error, Err(ChainError::Transport(_))),
            "{:?}",
            error
        );
    }
}

#[test]
fn rescan_progress_and_ranges_from_listings() {
    let (_server, client) = mock_wallet(Some("w"), |_, method, _| match method {
        "getwalletinfo" => Ok(json!({"walletname": "w", "descriptors": true,
            "scanning": {"duration": 12, "progress": 0.25}})),
        "listdescriptors" => Ok(json!({"wallet_name": "w", "descriptors": [
            {"desc": "a", "range": [20, 200]},
            {"desc": "b", "range": [0, 200]},
            {"desc": "c", "range": [0, 100]},
            {"desc": "d", "range": [0, 99]},
            {"desc": "e"},
            {"desc": "f", "range": [0]},
        ]})),
        other => panic!("unexpected {}", other),
    });
    let w = client.handle().core_wallet().unwrap();
    assert_eq!(
        w.scanning_status(),
        Ok(Some(mill_chain::ScanProgress {
            duration: Duration::from_secs(12),
            progress: 0.25
        }))
    );
    for (desc, covers) in [
        ("a", false),
        ("b", true),
        ("c", true),
        ("d", false),
        ("e", false),
        ("missing", false),
    ] {
        assert_eq!(w.imported_range_covers(desc, 100), Ok(covers), "{}", desc);
    }
    assert!(matches!(
        w.imported_range_covers("f", 100),
        Err(ChainError::Protocol(_))
    ));
}
