//! Typed Bitcoin Core queries, against a regtest bitcoind (28.1 and 25.x in CI) and
//! against a scripted HTTP mock for answers a real node cannot be made to give. The
//! binary comes from `BITCOIND_EXE`, or `bitcoind` on `PATH`.

use bitcoind::bitcoincore_rpc::jsonrpc::serde;
use bitcoind::bitcoincore_rpc::{self, RpcApi};
use bitcoind::{BitcoinD, Conf};
use mill_chain::bitcoin::consensus::encode::{deserialize_hex, serialize_hex};
use mill_chain::bitcoin::hashes::Hash;
use mill_chain::bitcoin::{Address, Amount, Block, BlockHash, Network, OutPoint, ScriptBuf};
use mill_chain::bitcoin::{Transaction, Txid};
use mill_chain::testing::{
    HttpReply, HttpRequest, MockHttpServer, MockReply, MockScript, MockServer,
};
use mill_chain::{ChainClient, ChainConfig, ChainError, CoreAuth, CoreConfig, MillChain};
use mill_chain::{EngineOptions, TxStatus};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

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

/// Returns once every wallet on the node has processed the blocks and mempool transactions
/// validated so far, which Core hands to wallets through an asynchronous queue.
fn synced(node: &BitcoinD) {
    node.client
        .call::<Value>("syncwithvalidationinterfacequeue", &[])
        .unwrap();
}

/// Mines `n` blocks to the node's default wallet.
fn mine(node: &BitcoinD, n: u64) -> Vec<BlockHash> {
    let to = node
        .client
        .get_new_address(None, None)
        .unwrap()
        .assume_checked();
    let mined = node.client.generate_to_address(n, &to).unwrap();
    synced(node);
    mined
}

/// Sends `amount` from the node's default wallet to `to`.
fn pay(node: &BitcoinD, to: &Address, amount: Amount) -> Txid {
    let txid = node
        .client
        .send_to_address(to, amount, None, None, None, None, None, None)
        .unwrap();
    synced(node);
    txid
}

/// Sends `sats` from the node's default wallet to a fresh address of its own.
fn send(node: &BitcoinD, sats: u64) -> Txid {
    let to = node
        .client
        .get_new_address(None, None)
        .unwrap()
        .assume_checked();
    pay(node, &to, Amount::from_sat(sats))
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
    rpc_reply_with_tip(req, &mock_tip, answer)
}

/// [`rpc_reply`], with `getbestblockhash` answered by `tip`.
fn rpc_reply_with_tip<F>(req: &HttpRequest, tip: &dyn Fn() -> BlockHash, answer: &F) -> HttpReply
where
    F: Fn(&str, &Value) -> Result<Value, (i64, String)>,
{
    let one = |request: &Value| {
        let method = request["method"].as_str().unwrap_or_default();
        let reply = match method {
            "getbestblockhash" => Ok(json!(tip())),
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

    let first = pay(&node, &address, Amount::ONE_BTC);
    // minconf 0 includes the mempool.
    let unconfirmed = w.list_unspent(0, None).unwrap();
    assert_eq!(unconfirmed.len(), 1);
    assert_eq!(unconfirmed[0].confirmations, 0);
    assert_eq!(w.list_unspent(1, None), Ok(Vec::new()));
    mine(&node, 1);
    let second = pay(&node, &address, Amount::from_sat(30_000));

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

/// A `-zmqpub<topic>=tcp://<bind>:<free port>` argument and the port.
fn zmq_arg(topic: &str, bind: &str) -> (&'static str, u16) {
    let port = bitcoind::get_available_port().unwrap();
    let arg = format!("-zmqpub{}=tcp://{}:{}", topic, bind, port);
    (Box::leak(arg.into_boxed_str()), port)
}

fn unsupported_message(error: ChainError) -> &'static str {
    match error {
        ChainError::Unsupported(message) => message,
        other => panic!("expected Unsupported, got {:?}", other),
    }
}

#[test]
fn requirements_check_rejects_bad_nodes() {
    // No txindex.
    let (sequence, _) = zmq_arg("sequence", "127.0.0.1");
    let (rawtx, _) = zmq_arg("rawtx", "127.0.0.1");
    let node_without_index = node(&[sequence, rawtx]);
    let error = connect(&node_without_index, None)
        .handle()
        .node_requirements()
        .unwrap_err();
    assert!(unsupported_message(error).contains("txindex"));
    drop(node_without_index);

    // Older than 24, from a mock: nothing else is asked.
    for (version, ok) in [(230_200, false), (239_999, false), (240_000, true)] {
        let (server, client) = mock_core(move |method, _| match method {
            "getnetworkinfo" => Ok(json!({"version": version, "subversion": "/Satoshi/"})),
            "getindexinfo" => Ok(json!({"txindex": {"synced": true, "best_block_height": 1}})),
            "getzmqnotifications" => Ok(json!([
                {"type": "pubsequence", "address": "tcp://127.0.0.1:1", "hwm": 1000},
                {"type": "pubrawtx", "address": "tcp://127.0.0.1:2", "hwm": 1000},
            ])),
            other => panic!("unexpected {}", other),
        });
        match client.handle().node_requirements() {
            Ok(info) => assert!(ok && info.version == version, "{}", version),
            Err(error) => {
                assert!(!ok, "{}", version);
                assert!(unsupported_message(error).contains("version"));
                assert_eq!(server.requests_seen().len(), 1);
            }
        }
    }

    // A required topic is missing.
    for (published, missing) in [("rawtx", "sequence"), ("sequence", "rawtx")] {
        let (only, _) = zmq_arg(published, "127.0.0.1");
        let (rawblock, _) = zmq_arg("rawblock", "127.0.0.1");
        let node = node(&["-txindex=1", only, rawblock]);
        let error = connect(&node, None)
            .handle()
            .node_requirements()
            .unwrap_err();
        let message = unsupported_message(error);
        assert!(
            message.contains("zmq") && message.contains(missing),
            "{}",
            message
        );
    }

    // Every requirement met.
    let (sequence, sequence_port) = zmq_arg("sequence", "127.0.0.1");
    let (rawtx, rawtx_port) = zmq_arg("rawtx", "127.0.0.1");
    let node = node(&["-txindex=1", sequence, rawtx]);
    let info = connect(&node, None).handle().node_requirements().unwrap();
    assert!(info.version >= 240_000);
    assert!(info.txindex);
    assert_eq!(info.zmq.len(), 2);
    assert_eq!(
        info.zmq["sequence"],
        format!("tcp://127.0.0.1:{}", sequence_port)
    );
    assert_eq!(info.zmq["rawtx"], format!("tcp://127.0.0.1:{}", rawtx_port));
}

#[test]
fn zmq_endpoints_discovered_per_topic() {
    let (sequence, sequence_port) = zmq_arg("sequence", "0.0.0.0");
    let (rawtx, rawtx_port) = zmq_arg("rawtx", "0.0.0.0");
    let (rawblock, rawblock_port) = zmq_arg("rawblock", "0.0.0.0");
    let (hashblock, hashblock_port) = zmq_arg("hashblock", "127.0.0.1");
    let node = node(&["-txindex=1", sequence, rawtx, rawblock, hashblock]);
    // The RPC host, written as a name, replaces each wildcard bind.
    let mut config = core_config(&node, None);
    config.rpc_url = config.rpc_url.replace("127.0.0.1", "localhost");
    let client = MillChain::spawn(ChainConfig::Core(config)).unwrap();
    let info = client.handle().node_requirements().unwrap();
    let expected: std::collections::BTreeMap<String, String> = [
        ("sequence", format!("tcp://localhost:{}", sequence_port)),
        ("rawtx", format!("tcp://localhost:{}", rawtx_port)),
        ("rawblock", format!("tcp://localhost:{}", rawblock_port)),
        ("hashblock", format!("tcp://127.0.0.1:{}", hashblock_port)),
    ]
    .into_iter()
    .map(|(topic, endpoint)| (topic.to_string(), endpoint))
    .collect();
    assert_eq!(info.zmq, expected);
    let ports: std::collections::BTreeSet<_> =
        [sequence_port, rawtx_port, rawblock_port, hashblock_port].into();
    assert_eq!(ports.len(), 4, "each topic on its own port");
}

#[test]
fn requirements_failures_are_not_unsupported() {
    // A bitcoind built without ZMQ.
    let (_server, client) = mock_core(|method, _| match method {
        "getnetworkinfo" => Ok(json!({"version": 280_100})),
        "getindexinfo" => Ok(json!({"txindex": {"synced": false, "best_block_height": 1}})),
        "getzmqnotifications" => Err((-32601, "Method not found".into())),
        other => panic!("unexpected {}", other),
    });
    let message = unsupported_message(client.handle().node_requirements().unwrap_err());
    assert!(message.contains("zmq") && message.contains("sequence") && message.contains("rawtx"));

    // Any other failure is reported as it is.
    let (_server, client) = mock_core(|method, _| match method {
        "getnetworkinfo" => Ok(json!({"version": 280_100})),
        "getindexinfo" => Err((-32603, "Internal error".into())),
        "getzmqnotifications" => Ok(json!([])),
        other => panic!("unexpected {}", other),
    });
    assert!(matches!(
        client.handle().node_requirements(),
        Err(ChainError::Server { code: -32603, .. })
    ));
    let (_server, client) = mock_core(|method, _| match method {
        "getnetworkinfo" => Ok(json!({"version": "28.1"})),
        _ => Ok(json!([])),
    });
    assert!(matches!(
        client.handle().node_requirements(),
        Err(ChainError::Protocol(_))
    ));
    let server = MockHttpServer::start(|_| HttpReply::drop_connection()).unwrap();
    let error = spawn_quick(server.url())
        .handle()
        .node_requirements()
        .unwrap_err();
    assert!(matches!(error, ChainError::Transport(_)), "{:?}", error);
}

/// Spends `outpoint`, which `keys` holds the key to, back to the default wallet, and
/// returns the signed transaction without broadcasting it.
fn sign_spend(
    node: &BitcoinD,
    keys: &bitcoincore_rpc::Client,
    outpoint: OutPoint,
    value: Amount,
) -> Transaction {
    let to = node
        .client
        .get_new_address(None, None)
        .unwrap()
        .assume_checked();
    let inputs = json!([{"txid": outpoint.txid, "vout": outpoint.vout}]);
    let send = (value - Amount::from_sat(10_000)).to_btc();
    let outputs = json!({ to.to_string(): send });
    let unsigned: Value = keys
        .call("createrawtransaction", &[inputs, outputs])
        .unwrap();
    let signed: Value = keys
        .call("signrawtransactionwithwallet", &[unsigned])
        .unwrap();
    assert_eq!(signed["complete"], true, "{}", signed);
    deserialize_hex(signed["hex"].as_str().unwrap()).unwrap()
}

/// The output of `txid` paying to `address`.
fn output_to(node: &BitcoinD, txid: Txid, address: &Address) -> (OutPoint, ScriptBuf) {
    let tx = node.client.get_raw_transaction(&txid, None).unwrap();
    let vout = tx
        .output
        .iter()
        .position(|o| o.script_pubkey == address.script_pubkey())
        .unwrap();
    (OutPoint::new(txid, vout as u32), address.script_pubkey())
}

#[test]
fn fund_watch_broadcast_confirm() {
    let node = node(&["-txindex=1"]);
    let client = connect(&node, Some("watch"));
    let h = client.handle();
    let w = h.core_wallet().unwrap();
    w.prepare("watch").unwrap();
    let keys = key_holder(&node);
    let (desc, address) = watched_key(&keys);
    w.import_descriptors(&[json!({"desc": desc, "timestamp": "now"})])
        .unwrap();
    mine(&node, 101);

    let funding = pay(&node, &address, Amount::ONE_BTC);
    mine(&node, 1);
    let utxos = w.list_unspent(1, None).unwrap();
    assert_eq!(utxos.len(), 1);
    let (outpoint, script) = (utxos[0].outpoint, utxos[0].script_pubkey.clone());
    assert_eq!(outpoint.txid, funding);
    assert_eq!(script, address.script_pubkey());
    assert_eq!(h.spending_tx(&outpoint, &script, false), Ok(None));
    assert_eq!(h.spending_tx(&outpoint, &script, true), Ok(None));

    let spend = sign_spend(&node, &keys, outpoint, Amount::ONE_BTC);
    let txid = h.broadcast(&spend).unwrap();
    assert_eq!(txid, spend.compute_txid());
    assert_eq!(h.tx_status(&txid), Ok(TxStatus::Mempool));
    assert_eq!(
        h.spending_tx(&outpoint, &script, false)
            .unwrap()
            .map(|tx| tx.compute_txid()),
        Some(txid)
    );
    // Only in the mempool: not a confirmed spend.
    assert_eq!(h.spending_tx(&outpoint, &script, true), Ok(None));

    let mined = mine(&node, 1)[0];
    assert_eq!(
        h.tx_status(&txid),
        Ok(TxStatus::Confirmed {
            height: 103,
            block_hash: mined
        })
    );
    let confirmed = h.spending_tx(&outpoint, &script, true).unwrap().unwrap();
    assert_eq!(*confirmed, spend);
    assert!(w
        .list_unspent(0, None)
        .unwrap()
        .iter()
        .all(|u| u.outpoint != outpoint));
}

#[test]
fn spend_lookup_is_bounded() {
    let node = node(&["-txindex=1"]);
    let keys = key_holder(&node);
    mine(&node, 101);
    let address = keys.get_new_address(None, None).unwrap().assume_checked();
    let funding = pay(&node, &address, Amount::ONE_BTC);
    mine(&node, 1);
    let (outpoint, script) = output_to(&node, funding, &address);
    mine(&node, 199);
    let spend = sign_spend(&node, &keys, outpoint, Amount::ONE_BTC);
    node.client.send_raw_transaction(&spend).unwrap();
    mine(&node, 1);
    let funded_at = node.client.get_block_count().unwrap() - 200;
    assert_eq!(funded_at, 102);

    // A fresh client, so nothing is cached.
    let client = connect(&node, None);
    let h = client.handle();
    for confirmed_only in [true, false] {
        let client = connect(&node, None);
        let h = client.handle();
        let found = h.spending_tx(&outpoint, &script, confirmed_only).unwrap();
        let used = client.stats().requests_sent;
        eprintln!("confirmed_only {}: {} requests", confirmed_only, used);
        assert!(used <= 10, "{} requests", used);
        assert_eq!(found.as_deref(), Some(&spend));
    }

    // Again on one client: the blocks come from its cache.
    let found = h.spending_tx(&outpoint, &script, true).unwrap();
    assert_eq!(found.as_deref(), Some(&spend));
    let first = client.stats().requests_sent;
    let found = h.spending_tx(&outpoint, &script, true).unwrap();
    assert_eq!(found.as_deref(), Some(&spend));
    let again = client.stats().requests_sent - first;
    assert!(again <= 5, "{} requests with every block cached", again);
}

#[test]
fn spend_lookup_edges_on_regtest() {
    let node = node(&["-txindex=1"]);
    let client = connect(&node, None);
    let h = client.handle();
    let keys = key_holder(&node);
    mine(&node, 101);

    // Funded and spent in the same block.
    let address = keys.get_new_address(None, None).unwrap().assume_checked();
    let funding = pay(&node, &address, Amount::ONE_BTC);
    let (outpoint, script) = output_to(&node, funding, &address);
    let spend = sign_spend(&node, &keys, outpoint, Amount::ONE_BTC);
    h.broadcast(&spend).unwrap();
    // Funding unconfirmed: no confirmed spend can exist, but the mempool has one.
    assert_eq!(h.spending_tx(&outpoint, &script, true), Ok(None));
    assert_eq!(
        h.spending_tx(&outpoint, &script, false).unwrap().as_deref(),
        Some(&spend)
    );
    let block = mine(&node, 1)[0];
    let mined = h.block(102).unwrap();
    assert_eq!(mined.block_hash(), block);
    assert!(mined.txdata.iter().any(|tx| tx.compute_txid() == funding));
    assert_eq!(
        h.spending_tx(&outpoint, &script, true).unwrap().as_deref(),
        Some(&spend)
    );

    // An output still unspent, an output that does not exist, a transaction that does not.
    let (change, change_script) = output_to(&node, spend.compute_txid(), &{
        let out = &spend.output[0];
        Address::from_script(&out.script_pubkey, Network::Regtest).unwrap()
    });
    assert_eq!(h.spending_tx(&change, &change_script, true), Ok(None));
    assert_eq!(h.spending_tx(&change, &change_script, false), Ok(None));
    let missing = OutPoint::new(spend.compute_txid(), 7);
    assert_eq!(
        h.spending_tx(&missing, &change_script, true),
        Err(ChainError::NotFound)
    );
    let unknown = OutPoint::new(txid(0xcd), 0);
    assert_eq!(
        h.spending_tx(&unknown, &change_script, false),
        Err(ChainError::NotFound)
    );

    // An OP_RETURN output never enters the UTXO set and can never be spent.
    let unsigned: Value = node
        .client
        .call(
            "createrawtransaction",
            &[json!([]), json!({"data": "6d696c6c"})],
        )
        .unwrap();
    let funded: Value = node.client.call("fundrawtransaction", &[unsigned]).unwrap();
    let signed: Value = node
        .client
        .call("signrawtransactionwithwallet", &[funded["hex"].clone()])
        .unwrap();
    let data_tx: Transaction = deserialize_hex(signed["hex"].as_str().unwrap()).unwrap();
    let data_txid = h.broadcast(&data_tx).unwrap();
    mine(&node, 1);
    let vout = data_tx
        .output
        .iter()
        .position(|o| o.script_pubkey.is_op_return())
        .unwrap();
    let data = OutPoint::new(data_txid, vout as u32);
    assert_eq!(h.tx_out(&data, false), Ok(None));
    assert_eq!(
        h.spending_tx(&data, &data_tx.output[vout].script_pubkey, true),
        Ok(None)
    );
}

/// A chain of `n` blocks for a mock: block 0 holds the funding transaction, block
/// `spent_at` the spend (if any), and block `i` carries `pad(i)` bytes of padding.
struct MockChain {
    blocks: Vec<Block>,
    funding: Transaction,
    spend: Transaction,
}

impl MockChain {
    fn new(n: usize, spent_at: Option<usize>, pad: impl Fn(usize) -> usize) -> Self {
        let genesis = mill_chain::bitcoin::constants::genesis_block(Network::Regtest);
        let mut funding = genesis.txdata[0].clone();
        funding.output[0].script_pubkey = ScriptBuf::from(vec![0x51]);
        funding.input[0].sequence = mill_chain::bitcoin::Sequence(7);
        let mut spend = genesis.txdata[0].clone();
        spend.input[0].previous_output = OutPoint::new(funding.compute_txid(), 0);
        let mut blocks: Vec<Block> = Vec::new();
        for i in 0..n {
            let mut block = genesis.clone();
            block.header.prev_blockhash = blocks
                .last()
                .map_or(BlockHash::all_zeros(), Block::block_hash);
            block.header.time = i as u32;
            block.txdata[0].output[0].script_pubkey = ScriptBuf::from(vec![0x51; pad(i)]);
            if i == 0 {
                block.txdata.push(funding.clone());
            }
            if Some(i) == spent_at {
                block.txdata.push(spend.clone());
            }
            block.header.merkle_root = block.compute_merkle_root().unwrap();
            blocks.push(block);
        }
        MockChain {
            blocks,
            funding,
            spend,
        }
    }

    fn outpoint(&self) -> OutPoint {
        OutPoint::new(self.funding.compute_txid(), 0)
    }

    /// Answers the spend lookup's calls; `tx_out` is `gettxout`'s answer.
    fn answer(&self, method: &str, params: &Value, tx_out: &Value) -> Result<Value, (i64, String)> {
        let tip = self.blocks.len() - 1;
        let height_of = |hash: &str| {
            self.blocks
                .iter()
                .position(|b| b.block_hash().to_string() == hash)
                .unwrap()
        };
        match method {
            "getblockcount" => Ok(json!(tip)),
            "gettxout" => Ok(tx_out.clone()),
            "getrawtransaction" => Ok(json!({
                "txid": self.funding.compute_txid(),
                "blockhash": self.blocks[0].block_hash(),
                "confirmations": tip + 1,
                "vout": [{"scriptPubKey": {"hex": "51"}}],
            })),
            "getblockheader" => {
                let height = height_of(params[0].as_str().unwrap());
                Ok(json!({"height": height, "confirmations": tip + 1 - height}))
            }
            "getblockhash" => match self.blocks.get(params[0].as_u64().unwrap() as usize) {
                Some(block) => Ok(json!(block.block_hash())),
                None => Err((-8, "Block height out of range".into())),
            },
            "getblock" => {
                let height = height_of(params[0].as_str().unwrap());
                Ok(json!(serialize_hex(&self.blocks[height])))
            }
            other => panic!("unexpected {}", other),
        }
    }
}

fn mock_spends(
    chain: MockChain,
    tx_out: Value,
    max_body_bytes: usize,
) -> (MockHttpServer, ChainClient, Arc<MockChain>) {
    let chain = Arc::new(chain);
    let serving = Arc::clone(&chain);
    let server = MockHttpServer::start(move |req: &HttpRequest| {
        rpc_reply(req, &|method: &str, params: &Value| {
            serving.answer(method, params, &tx_out)
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
    config.max_body_bytes = max_body_bytes;
    let client = MillChain::spawn(ChainConfig::Core(config)).unwrap();
    (server, client, chain)
}

fn getblock_batches(server: &MockHttpServer) -> Vec<usize> {
    server
        .requests_seen()
        .iter()
        .filter_map(|req| match req.json() {
            Some(Value::Array(calls)) if calls.iter().all(|c| c["method"] == "getblock") => {
                Some(calls.len())
            }
            _ => None,
        })
        .collect()
}

#[test]
fn spend_scan_shrinks_batches_that_overflow() {
    // Blocks grow, so batches sized from the first ones overflow the body cap.
    let chain = MockChain::new(16, Some(15), |i| 100 + i * i * 40);
    let largest = serialize_hex(&chain.blocks[15]).len();
    let (server, client, chain) = mock_spends(chain, Value::Null, largest + 1000);
    let found = client
        .handle()
        .spending_tx(&chain.outpoint(), &ScriptBuf::new(), true)
        .unwrap();
    assert_eq!(found.as_deref(), Some(&chain.spend));
    let batches = getblock_batches(&server);
    let fetched: usize = batches.iter().sum();
    assert!(fetched > 16, "no batch was refetched: {:?}", batches);
    assert!(batches.iter().any(|&n| n > 1), "{:?}", batches);

    // A single block over the cap is an error, never "no spend".
    let chain = MockChain::new(4, Some(3), |i| if i == 2 { 4000 } else { 10 });
    let (_server, client, chain) = mock_spends(chain, Value::Null, 2000);
    let error = client
        .handle()
        .spending_tx(&chain.outpoint(), &ScriptBuf::new(), true)
        .unwrap_err();
    assert!(matches!(error, ChainError::Protocol(_)), "{:?}", error);
}

#[test]
fn spend_lookup_never_guesses() {
    // Core says spent, but no block up to a tip that does not move spends it.
    let chain = MockChain::new(5, None, |_| 10);
    let (_server, client, chain) = mock_spends(chain, Value::Null, 1 << 20);
    let error = client
        .handle()
        .spending_tx(&chain.outpoint(), &ScriptBuf::new(), true)
        .unwrap_err();
    assert!(matches!(error, ChainError::Protocol(_)), "{:?}", error);

    // Unspent in the confirmed set: no confirmed spend, without scanning.
    let chain = MockChain::new(5, None, |_| 10);
    let (server, client, chain) = mock_spends(chain, json!({"value": 1.0}), 1 << 20);
    assert_eq!(
        client
            .handle()
            .spending_tx(&chain.outpoint(), &ScriptBuf::new(), true),
        Ok(None)
    );
    assert!(getblock_batches(&server).is_empty());

    // A funding block Core cannot place.
    let chain = Arc::new(MockChain::new(2, None, |_| 10));
    let serving = Arc::clone(&chain);
    let server = MockHttpServer::start(move |req: &HttpRequest| {
        rpc_reply(req, &|method: &str, params: &Value| match method {
            "getrawtransaction" => Ok(json!({"txid": serving.funding.compute_txid(),
                "blockhash": serving.blocks[0].block_hash(), "vout": [{"scriptPubKey": {"hex": "51"}}]})),
            _ => serving.answer(method, params, &Value::Null),
        })
    })
    .unwrap();
    let error = spawn_quick(server.url())
        .handle()
        .spending_tx(&chain.outpoint(), &ScriptBuf::new(), true)
        .unwrap_err();
    assert!(matches!(error, ChainError::Protocol(_)), "{:?}", error);

    // A gettxout reply that is not an output, and a mempool answer about another output.
    let chain = MockChain::new(2, None, |_| 10);
    let (_server, client, chain) = mock_spends(chain, json!(false), 1 << 20);
    let error = client
        .handle()
        .spending_tx(&chain.outpoint(), &ScriptBuf::new(), true)
        .unwrap_err();
    assert!(matches!(error, ChainError::Protocol(_)), "{:?}", error);
    let chain = Arc::new(MockChain::new(2, None, |_| 10));
    let serving = Arc::clone(&chain);
    let server = MockHttpServer::start(move |req: &HttpRequest| {
        rpc_reply(req, &|method: &str, params: &Value| match method {
            "gettxspendingprevout" => Ok(json!([{"txid": params[0][0]["txid"], "vout": 1}])),
            _ => serving.answer(method, params, &Value::Null),
        })
    })
    .unwrap();
    let error = spawn_quick(server.url())
        .handle()
        .spending_tx(&chain.outpoint(), &ScriptBuf::new(), false)
        .unwrap_err();
    assert!(matches!(error, ChainError::Protocol(_)), "{:?}", error);

    // A block arrives during every first read: never settled.
    let chain = Arc::new(MockChain::new(3, Some(2), |_| 10));
    let serving = Arc::clone(&chain);
    let tips = Arc::new(AtomicUsize::new(0));
    let server = MockHttpServer::start(move |req: &HttpRequest| {
        let tip = || block_hash(tips.fetch_add(1, Ordering::SeqCst) as u8);
        rpc_reply_with_tip(req, &tip, &|method: &str, params: &Value| {
            serving.answer(method, params, &Value::Null)
        })
    })
    .unwrap();
    let error = spawn_quick(server.url())
        .handle()
        .spending_tx(&chain.outpoint(), &ScriptBuf::new(), true)
        .unwrap_err();
    assert!(matches!(error, ChainError::Protocol(_)), "{:?}", error);

    // Spent at first, unspent once the scan comes up empty (a reorg undid the spend): with
    // the mempool in question, the lookup starts again rather than answer "no spend".
    let chain = Arc::new(MockChain::new(3, None, |_| 10));
    let serving = Arc::clone(&chain);
    let outs = Arc::new(AtomicUsize::new(0));
    let server = MockHttpServer::start(move |req: &HttpRequest| {
        rpc_reply(req, &|method: &str, params: &Value| match method {
            "gettxspendingprevout" => Ok(json!([{"txid": params[0][0]["txid"], "vout": 0}])),
            "gettxout" if outs.fetch_add(1, Ordering::SeqCst) % 2 == 1 => {
                Ok(json!({"value": 1.0, "scriptPubKey": {"hex": "51"}}))
            }
            _ => serving.answer(method, params, &Value::Null),
        })
    })
    .unwrap();
    let client = spawn_quick(server.url());
    let error = client
        .handle()
        .spending_tx(&chain.outpoint(), &ScriptBuf::new(), false)
        .unwrap_err();
    assert!(matches!(error, ChainError::Protocol(_)), "{:?}", error);
    assert_eq!(
        client
            .handle()
            .spending_tx(&chain.outpoint(), &ScriptBuf::new(), true),
        Ok(None)
    );

    // A failure part way through the scan.
    let chain = Arc::new(MockChain::new(5, Some(4), |_| 10));
    let serving = Arc::clone(&chain);
    let server = MockHttpServer::start(move |req: &HttpRequest| {
        rpc_reply(req, &|method: &str, params: &Value| match method {
            "getblock" if params[0] == json!(serving.blocks[3].block_hash()) => {
                Err((-32603, "Internal error".into()))
            }
            _ => serving.answer(method, params, &Value::Null),
        })
    })
    .unwrap();
    let error = spawn_quick(server.url())
        .handle()
        .spending_tx(&chain.outpoint(), &ScriptBuf::new(), true)
        .unwrap_err();
    assert!(
        matches!(error, ChainError::Server { code: -32603, .. }),
        "{:?}",
        error
    );

    // The mempool names a transaction that does not spend the outpoint.
    let chain = Arc::new(MockChain::new(2, None, |_| 10));
    let serving = Arc::clone(&chain);
    let unrelated = serialize_hex(&chain.funding);
    let server = MockHttpServer::start(move |req: &HttpRequest| {
        rpc_reply(req, &|method: &str, params: &Value| match method {
            "gettxspendingprevout" => Ok(json!([{"txid": params[0][0]["txid"], "vout": 0,
                "spendingtxid": serving.funding.compute_txid()}])),
            "getrawtransaction" if params[1] == json!(false) => Ok(json!(unrelated)),
            _ => serving.answer(method, params, &Value::Null),
        })
    })
    .unwrap();
    let error = spawn_quick(server.url())
        .handle()
        .spending_tx(&chain.outpoint(), &ScriptBuf::new(), false)
        .unwrap_err();
    assert!(matches!(error, ChainError::Protocol(_)), "{:?}", error);

    // The connection drops.
    let server = MockHttpServer::start(|_| HttpReply::drop_connection()).unwrap();
    let error = spawn_quick(server.url())
        .handle()
        .spending_tx(&OutPoint::new(txid(1), 0), &ScriptBuf::new(), true)
        .unwrap_err();
    assert!(matches!(error, ChainError::Transport(_)), "{:?}", error);
}

/// Bitcoin Core replies captured by `examples/capture_core_golden.rs`.
struct Golden {
    name: String,
    captures: Vec<Value>,
}

impl Golden {
    fn load_all() -> Vec<Golden> {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden");
        let mut goldens: Vec<Golden> = std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|e| e == "json"))
            .map(|path| {
                let golden: Value =
                    serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
                Golden {
                    name: golden["bitcoind"].as_str().unwrap().to_string(),
                    captures: golden["captures"].as_array().unwrap().clone(),
                }
            })
            .collect();
        goldens.sort_by(|a, b| a.name.cmp(&b.name));
        goldens
    }

    fn entries(&self, synthetic: bool) -> Vec<Value> {
        self.captures
            .iter()
            .filter(|c| c["synthetic"].as_bool().unwrap_or(false) == synthetic)
            .cloned()
            .collect()
    }

    /// The real captures, and the synthetic ones no real capture has the method and params
    /// of.
    fn served(&self) -> Vec<Value> {
        let real = self.entries(false);
        let extra: Vec<Value> = self
            .entries(true)
            .into_iter()
            .filter(|s| {
                !real
                    .iter()
                    .any(|r| r["method"] == s["method"] && r["params"] == s["params"])
            })
            .collect();
        real.into_iter().chain(extra).collect()
    }

    /// The captured result of `method` with `params`.
    fn result(&self, method: &str, params: Value) -> Value {
        self.captures
            .iter()
            .find(|c| c["method"] == method && c["params"] == params && c.get("result").is_some())
            .unwrap_or_else(|| panic!("{}: no result for {} {}", self.name, method, params))
            ["result"]
            .clone()
    }

    fn synthetic(&self, method: &str) -> Value {
        self.entries(true)
            .into_iter()
            .find(|c| c["method"] == method)
            .unwrap_or_else(|| panic!("{}: no synthetic {}", self.name, method))["result"]
            .clone()
    }

    fn with(&self, method: &str, f: impl Fn(&Value) -> bool) -> Vec<Value> {
        self.entries(false)
            .into_iter()
            .filter(|c| c["method"] == method && f(c))
            .collect()
    }
}

/// Answers each request with the capture of the same method and params, else the only
/// capture of that method; records which captures were used.
fn serve(
    entries: Vec<Value>,
    used: Arc<std::sync::Mutex<Vec<bool>>>,
) -> (MockHttpServer, ChainClient) {
    *used.lock().unwrap() = vec![false; entries.len()];
    mock_wallet(Some("watch"), move |_, method, params| {
        let same = |c: &&Value| c["method"] == method;
        let index = entries
            .iter()
            .position(|c| same(&c) && c["params"] == *params)
            .or_else(|| {
                let mut of_method = entries.iter().enumerate().filter(|(_, c)| same(c));
                match (of_method.next(), of_method.next()) {
                    (Some((i, _)), None) => Some(i),
                    _ => None,
                }
            });
        let Some(index) = index else {
            return Err((-32601, format!("no capture for {} {}", method, params)));
        };
        used.lock().unwrap()[index] = true;
        let capture = &entries[index];
        match capture.get("error") {
            Some(e) => Err((
                e["code"].as_i64().unwrap(),
                e["message"].as_str().unwrap().into(),
            )),
            None => Ok(capture["result"].clone()),
        }
    })
}

fn rpc_json<T: serde::de::DeserializeOwned>(value: Value) -> T {
    serde_json::from_value(value).unwrap()
}

/// The differences from `bitcoincore-rpc-json` this test finds, by their id in
/// `tests/golden/EXPECTED_DIFFS.md`.
struct Diffs {
    seen: std::collections::BTreeSet<&'static str>,
}

impl Diffs {
    fn expect(&mut self, id: &'static str) {
        self.seen.insert(id);
    }
}

#[test]
fn parse_parity_with_bitcoincore_rpc_json() {
    use bitcoind::bitcoincore_rpc::json as rpcjson;
    use mill_chain::bitcoin::block::Header;
    use mill_chain::{Tip, TxOutInfo, WalletTx, WalletUtxo};

    let goldens = Golden::load_all();
    assert!(!goldens.is_empty(), "no golden files");
    let mut diffs = Diffs {
        seen: Default::default(),
    };
    for golden in &goldens {
        eprintln!("checking bitcoind {}", golden.name);
        let used = Arc::new(std::sync::Mutex::new(Vec::new()));
        let served = golden.served();
        let (_server, client) = serve(served.clone(), Arc::clone(&used));
        let h = client.handle();
        let w = h.core_wallet().unwrap();

        // Chain state.
        let info: rpcjson::GetBlockchainInfoResult =
            rpc_json(golden.result("getblockchaininfo", json!([])));
        assert_eq!(
            h.tip(),
            Ok(Tip {
                height: info.blocks,
                hash: info.best_block_hash
            })
        );
        assert_eq!(h.network(), Ok(info.chain));
        let height = info.blocks;
        let hash: BlockHash = rpc_json(golden.result("getblockhash", json!([height])));
        assert_eq!(h.block_hash(height), Ok(hash));
        assert_eq!(h.block_hash(height + 1), Err(ChainError::NotFound));
        let header: Header = deserialize_hex(
            golden
                .result("getblockheader", json!([hash, false]))
                .as_str()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(h.header(height), Ok(header));
        let block: Block = deserialize_hex(
            golden
                .result("getblock", json!([hash, 0]))
                .as_str()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(*h.block(height).unwrap(), block);

        // Transactions, and where they stand.
        for capture in golden.with("getrawtransaction", |c| c["params"][1] == json!(false)) {
            let txid: Txid = rpc_json(capture["params"][0].clone());
            let Some(hex) = capture.get("result") else {
                assert_eq!(h.transaction(&txid), Err(ChainError::NotFound));
                assert_eq!(h.tx_status(&txid), Err(ChainError::NotFound));
                continue;
            };
            let tx: Transaction = deserialize_hex(hex.as_str().unwrap()).unwrap();
            assert_eq!(*h.transaction(&txid).unwrap(), tx);
            let verbose: rpcjson::GetRawTransactionResult =
                rpc_json(golden.result("getrawtransaction", json!([txid, true])));
            assert_eq!(verbose.transaction().unwrap(), tx);
            let expected = match verbose.blockhash {
                Some(block_hash) if verbose.confirmations.unwrap_or(0) > 0 => {
                    let header: rpcjson::GetBlockHeaderResult =
                        rpc_json(golden.result("getblockheader", json!([block_hash, true])));
                    TxStatus::Confirmed {
                        height: header.height as u64,
                        block_hash,
                    }
                }
                _ => {
                    let _: rpcjson::GetMempoolEntryResult =
                        rpc_json(golden.result("getmempoolentry", json!([txid])));
                    TxStatus::Mempool
                }
            };
            assert_eq!(h.tx_status(&txid), Ok(expected));
        }

        // Outputs.
        for capture in golden.with("gettxout", |_| true) {
            let params = &capture["params"];
            let outpoint = OutPoint::new(
                rpc_json(params[0].clone()),
                params[1].as_u64().unwrap() as u32,
            );
            let parsed: Option<rpcjson::GetTxOutResult> = rpc_json(capture["result"].clone());
            let expected = parsed.map(|out| TxOutInfo {
                value: out.value,
                script_pubkey: ScriptBuf::from(out.script_pub_key.hex),
                confirmations: out.confirmations,
                coinbase: out.coinbase,
            });
            assert_eq!(h.tx_out(&outpoint, params[2] == json!(true)), Ok(expected));
        }
        let mempool: Vec<Txid> = rpc_json(golden.result("getrawmempool", json!([false])));
        assert_eq!(h.mempool_txids(), Ok(mempool));

        // The mempool spender, parsed as openswap parses it.
        #[derive(serde::Deserialize)]
        #[serde(crate = "bitcoind::bitcoincore_rpc::jsonrpc::serde")]
        struct SpendingPrevout {
            spendingtxid: Option<Txid>,
        }
        let capture = &golden.with("gettxspendingprevout", |_| true)[0];
        let prevout = &capture["params"][0][0];
        let spent = OutPoint::new(
            rpc_json(prevout["txid"].clone()),
            prevout["vout"].as_u64().unwrap() as u32,
        );
        let spenders: Vec<SpendingPrevout> = rpc_json(capture["result"].clone());
        let found = h.spending_tx(&spent, &ScriptBuf::new(), false).unwrap();
        assert_eq!(found.map(|tx| tx.compute_txid()), spenders[0].spendingtxid);

        // Broadcasts: a mempool transaction is answered with its txid; one already mined
        // is an error to bitcoincore-rpc and a success here.
        for capture in golden.with("sendrawtransaction", |_| true) {
            let tx: Transaction = deserialize_hex(capture["params"][0].as_str().unwrap()).unwrap();
            match capture.get("result") {
                Some(txid) => assert_eq!(h.broadcast(&tx), Ok(rpc_json::<Txid>(txid.clone()))),
                None => {
                    assert_eq!(capture["error"]["code"], -27);
                    assert_eq!(h.broadcast(&tx), Ok(tx.compute_txid()));
                    diffs.expect("D3");
                }
            }
        }

        // Fees.
        let none: rpcjson::EstimateSmartFeeResult =
            rpc_json(golden.result("estimatesmartfee", json!([6])));
        assert!(none.fee_rate.is_none() && none.errors.is_some());
        assert_eq!(h.estimate_fee(6), Err(ChainError::NotFound));
        let some: rpcjson::EstimateSmartFeeResult =
            rpc_json(golden.result("estimatesmartfee", json!([2])));
        let per_kvb = some.fee_rate.unwrap();
        assert_eq!(
            h.estimate_fee(2).unwrap().sat_per_vb(),
            per_kvb.to_sat() as f64 / 1000.0
        );

        // Wallet views.
        let unspent: Vec<rpcjson::ListUnspentResultEntry> =
            rpc_json(golden.result("listunspent", json!([0, 9_999_999])));
        let expected: Vec<WalletUtxo> = unspent
            .into_iter()
            .map(|u| WalletUtxo {
                outpoint: OutPoint::new(u.txid, u.vout),
                value: u.amount,
                script_pubkey: u.script_pub_key,
                confirmations: u.confirmations,
                address: u.address,
                label: u.label,
                descriptor: u.descriptor,
                witness_script: u.witness_script,
                redeem_script: u.redeem_script,
                spendable: u.spendable,
                solvable: u.solvable,
                safe: u.safe,
            })
            .collect();
        assert!(!expected.is_empty());
        assert_eq!(w.list_unspent(0, None), Ok(expected));
        let history: Vec<rpcjson::ListTransactionResult> =
            rpc_json(golden.result("listtransactions", json!(["*", 10, 0, true])));
        let expected: Vec<WalletTx> = history
            .into_iter()
            .map(|t| WalletTx {
                txid: t.info.txid,
                vout: t.detail.vout,
                category: match t.detail.category {
                    rpcjson::GetTransactionResultDetailCategory::Send => {
                        mill_chain::TxCategory::Send
                    }
                    rpcjson::GetTransactionResultDetailCategory::Receive => {
                        mill_chain::TxCategory::Receive
                    }
                    rpcjson::GetTransactionResultDetailCategory::Generate => {
                        mill_chain::TxCategory::Generate
                    }
                    rpcjson::GetTransactionResultDetailCategory::Immature => {
                        mill_chain::TxCategory::Immature
                    }
                    rpcjson::GetTransactionResultDetailCategory::Orphan => {
                        mill_chain::TxCategory::Orphan
                    }
                },
                amount: t.detail.amount,
                fee: t.detail.fee,
                confirmations: t.info.confirmations,
                block_hash: t.info.blockhash,
                block_height: t.info.blockheight.map(u64::from),
                time: t.info.time,
                address: t.detail.address,
                label: t.detail.label,
            })
            .collect();
        assert!(!expected.is_empty());
        assert_eq!(w.list_transactions(10, 0), Ok(expected));

        let capture = &golden.with("getaddressinfo", |_| true)[0];
        let address: Address =
            rpc_json::<Address<_>>(capture["params"][0].clone()).assume_checked();
        let raw = &capture["result"];
        let ours = w.address_info(&address).unwrap();
        let parsed: rpcjson::GetAddressInfoResult = rpc_json(raw.clone());
        assert_eq!(Some(ours.is_mine), parsed.is_mine);
        assert_eq!(ours.is_watch_only, parsed.is_watchonly.unwrap_or(false));
        assert_eq!(ours.is_mine, raw["ismine"].as_bool().unwrap());
        assert_eq!(ours.solvable, raw["solvable"].as_bool().unwrap());
        assert_eq!(ours.desc.as_deref(), raw["desc"].as_str());

        for capture in golden.with("deriveaddresses", |_| true) {
            let params = &capture["params"];
            let range = params
                .get(1)
                .map(|r| [r[0].as_u64().unwrap() as u32, r[1].as_u64().unwrap() as u32]);
            let expected: Vec<Address<mill_chain::bitcoin::address::NetworkUnchecked>> =
                rpc_json(capture["result"].clone());
            assert_eq!(
                w.derive_addresses(params[0].as_str().unwrap(), range),
                Ok(expected)
            );
        }

        // Imports: bitcoincore-rpc hands back the per-item results; a rejected item is an
        // error here.
        let capture = &golden.with("importdescriptors", |_| true)[0];
        let reqs = capture["params"][0].as_array().unwrap().clone();
        let results: Vec<rpcjson::ImportMultiResult> = rpc_json(capture["result"].clone());
        let rejected: Vec<&str> = reqs
            .iter()
            .zip(&results)
            .filter(|(_, r)| !r.success)
            .map(|(req, _)| req["desc"].as_str().unwrap())
            .collect();
        assert!(!rejected.is_empty());
        match w.import_descriptors(&reqs) {
            Err(ChainError::Server { message, .. }) => {
                assert!(
                    rejected.iter().all(|desc| message.contains(desc)),
                    "{}",
                    message
                );
                diffs.expect("D1");
            }
            other => panic!("{:?}", other),
        }

        #[derive(serde::Deserialize)]
        #[serde(crate = "bitcoind::bitcoincore_rpc::jsonrpc::serde")]
        struct Rescan {
            start_height: usize,
            #[allow(dead_code)]
            stop_height: Option<usize>,
        }
        let rescan = &golden.with("rescanblockchain", |_| true)[0];
        let parsed: Rescan = rpc_json(rescan["result"].clone());
        assert_eq!(
            parsed.start_height as u64,
            rescan["params"][0].as_u64().unwrap()
        );
        assert_eq!(w.rescan_from(parsed.start_height as u64), Ok(()));

        // Rescan progress, parsed as openswap parses it.
        #[derive(serde::Deserialize)]
        #[serde(crate = "bitcoind::bitcoincore_rpc::jsonrpc::serde")]
        struct Scanning {
            scanning: Option<rpcjson::ScanningDetails>,
        }
        let expect_scan = |scanning: Option<rpcjson::ScanningDetails>| match scanning {
            Some(rpcjson::ScanningDetails::Scanning { duration, progress }) => {
                Some((duration as u64, progress))
            }
            _ => None,
        };
        let parsed: Scanning = rpc_json(golden.result("getwalletinfo", json!([])));
        let ours = w.scanning_status().unwrap();
        assert_eq!(
            ours.map(|p| (p.duration.as_secs(), p.progress as f32)),
            expect_scan(parsed.scanning)
        );

        // Imported ranges, checked as openswap checks them.
        #[derive(serde::Deserialize)]
        #[serde(crate = "bitcoind::bitcoincore_rpc::jsonrpc::serde")]
        struct Listed {
            desc: String,
            range: Option<[u32; 2]>,
        }
        #[derive(serde::Deserialize)]
        #[serde(crate = "bitcoind::bitcoincore_rpc::jsonrpc::serde")]
        struct Descriptors {
            descriptors: Vec<Listed>,
        }
        let listed: Descriptors = rpc_json(golden.result("listdescriptors", json!([])));
        assert!(listed.descriptors.iter().any(|d| d.range.is_some()));
        for entry in &listed.descriptors {
            for last_index in [0, 19, 20, 21, 999, 1000] {
                let openswap = entry
                    .range
                    .is_some_and(|[start, end]| start == 0 && end >= last_index);
                assert_eq!(
                    w.imported_range_covers(&entry.desc, last_index),
                    Ok(openswap),
                    "{} {}",
                    entry.desc,
                    last_index
                );
            }
        }

        // Node requirements.
        let network: rpcjson::GetNetworkInfoResult =
            rpc_json(golden.result("getnetworkinfo", json!([])));
        let indexes: rpcjson::GetIndexInfoResult =
            rpc_json(golden.result("getindexinfo", json!([])));
        let notifications: Vec<rpcjson::GetZmqNotificationsResult> =
            rpc_json(golden.result("getzmqnotifications", json!([])));
        let node = h.node_requirements().unwrap();
        assert_eq!(node.version as usize, network.version);
        assert_eq!(node.txindex, indexes.txindex.is_some());
        assert_eq!(node.zmq.len(), notifications.len());
        for n in &notifications {
            let topic = n.notification_type.strip_prefix("pub").unwrap();
            let port = n.address.rsplit_once(':').unwrap().1;
            if n.address.starts_with("tcp://0.0.0.0:") {
                assert_eq!(node.zmq[topic], format!("tcp://127.0.0.1:{}", port));
                diffs.expect("D2");
            } else {
                assert_eq!(node.zmq[topic], n.address);
            }
        }

        // Wallet bootstrap: loaded, on disk only, and absent.
        let loaded: Vec<String> = rpc_json(golden.result("listwallets", json!([])));
        let on_disk: rpcjson::ListWalletDirResult =
            rpc_json(golden.result("listwalletdir", json!([])));
        let _: rpcjson::LoadWalletResult =
            rpc_json(golden.with("loadwallet", |_| true)[0]["result"].clone());
        let _: rpcjson::LoadWalletResult =
            rpc_json(golden.with("createwallet", |_| true)[0]["result"].clone());
        let unloaded = on_disk
            .wallets
            .iter()
            .map(|w| w.name.as_str())
            .find(|name| !loaded.iter().any(|l| l == name))
            .unwrap();
        for (name, requests) in [(loaded[0].as_str(), 1), (unloaded, 3), ("absent", 3)] {
            let before = client.stats().requests_sent;
            assert_eq!(w.prepare(name), Ok(()), "{}", name);
            assert_eq!(client.stats().requests_sent - before, requests, "{}", name);
        }

        let unused: Vec<String> = served
            .iter()
            .zip(used.lock().unwrap().iter())
            .filter(|(_, used)| !**used)
            .map(|(c, _)| format!("{} {}", c["method"], c["params"]))
            .collect();
        assert!(
            unused.is_empty(),
            "{}: captures never asked for: {:?}",
            golden.name,
            unused
        );

        // A rescan in progress, which only a synthetic reply shows.
        let synthetic = golden.synthetic("getwalletinfo");
        let parsed: Scanning = rpc_json(synthetic.clone());
        let (_server, client) = mock_wallet(Some("watch"), move |_, _, _| Ok(synthetic.clone()));
        let progress = client
            .handle()
            .core_wallet()
            .unwrap()
            .scanning_status()
            .unwrap();
        assert!(progress.is_some());
        assert_eq!(
            progress.map(|p| (p.duration.as_secs(), p.progress as f32)),
            expect_scan(parsed.scanning)
        );
    }

    let listed = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/golden/EXPECTED_DIFFS.md"
    ))
    .unwrap();
    let documented: std::collections::BTreeSet<&str> = listed
        .lines()
        .filter_map(|line| line.strip_prefix("## "))
        .filter_map(|title| title.split(':').next())
        .filter(|id| {
            id.strip_prefix('D')
                .is_some_and(|n| n.parse::<u32>().is_ok())
        })
        .collect();
    assert_eq!(
        diffs.seen, documented,
        "the differences found must be exactly those in EXPECTED_DIFFS.md"
    );
}
