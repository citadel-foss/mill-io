//! Typed chain queries against Bitcoin Core, built from raw JSON-RPC calls.
//!
//! Fund safety: an error object becomes [`ChainError::NotFound`] only where Core's code and
//! message together say the thing does not exist. Core reuses `-5` for "cannot tell" (no
//! `-txindex`, index still syncing), and those stay [`ChainError::Server`].

use crate::engine::Route;
use crate::types::TxOutInfo;
use crate::{ChainError, ChainHandle, FeeRate, Tip, TxStatus};
use bitcoin::block::Header;
use bitcoin::consensus::encode::{deserialize_hex, serialize_hex};
use bitcoin::consensus::Decodable;
use bitcoin::{Amount, Block, BlockHash, Network, OutPoint, ScriptBuf};
use bitcoin::{Transaction, Txid};
use crossbeam_channel::RecvTimeoutError;
use serde_json::{json, Value};
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// `RPC_MISC_ERROR`.
const MISC_ERROR: i64 = -1;
/// `RPC_INVALID_ADDRESS_OR_KEY`.
const INVALID_ADDRESS_OR_KEY: i64 = -5;
/// `RPC_INVALID_PARAMETER`.
const INVALID_PARAMETER: i64 = -8;
/// `RPC_VERIFY_ERROR`.
const VERIFY_ERROR: i64 = -25;
/// `RPC_VERIFY_REJECTED`.
const VERIFY_REJECTED: i64 = -26;
/// `RPC_VERIFY_ALREADY_IN_CHAIN`, renamed `RPC_VERIFY_ALREADY_IN_UTXO_SET` in Core 28.
const VERIFY_ALREADY_IN_CHAIN: i64 = -27;

/// How often a query re-reads state that moved between two of its calls.
const ATTEMPTS: usize = 3;
/// How often a caller waiting on the compute pool checks its cancel flag.
const CANCEL_POLL: Duration = Duration::from_millis(5);

/// Replies to calls made between two reads of the best block hash.
pub(crate) struct Bracketed {
    /// The best block changed between the two reads.
    pub(crate) tip_moved: bool,
    /// One reply per call, in order.
    pub(crate) replies: Vec<Result<Value, ChainError>>,
}

impl Bracketed {
    fn pop(&mut self) -> Option<Result<Value, ChainError>> {
        self.replies.pop()
    }
}

fn moving(txid: &Txid) -> ChainError {
    ChainError::Protocol(format!(
        "{} kept moving between the mempool and blocks",
        txid
    ))
}

/// A job for the compute pool.
pub(crate) type Job<T> = Box<dyn FnOnce() -> Result<T, ChainError> + Send>;

/// One typed query: every call it makes shares the handle's deadline, counted from when the
/// query started.
pub(crate) struct Query<'a> {
    handle: &'a ChainHandle,
    deadline: Option<Instant>,
}

impl<'a> Query<'a> {
    /// Starts a query; [`ChainError::Unsupported`] naming `what` on a backend that is not
    /// Bitcoin Core.
    pub(crate) fn new(handle: &'a ChainHandle, what: &'static str) -> Result<Self, ChainError> {
        if handle.runtime().core().is_none() {
            return Err(ChainError::Unsupported(what));
        }
        Ok(Self {
            handle,
            deadline: handle.deadline_from_now(),
        })
    }

    pub(crate) fn chain(&self, method: &str, params: Value) -> Result<Value, ChainError> {
        self.handle
            .call_by(method, params, Route::Chain, self.deadline)
    }

    /// One HTTP request carrying every call; one result per call, in order.
    pub(crate) fn batch(
        &self,
        calls: &[(&str, Value)],
    ) -> Result<Vec<Result<Value, ChainError>>, ChainError> {
        self.handle.batch_by(calls, self.deadline)
    }

    /// Sends `calls` in one batch between two reads of the best block hash. While a block
    /// connects, Core's txindex can lag its mempool for an instant, so an answer of
    /// absence counts only when the tip did not move.
    pub(crate) fn bracketed(&self, calls: &[(&str, Value)]) -> Result<Bracketed, ChainError> {
        let mut all = Vec::with_capacity(calls.len() + 2);
        all.push(("getbestblockhash", json!([])));
        all.extend(calls.iter().cloned());
        all.push(("getbestblockhash", json!([])));
        let mut replies = self.batch(&all)?.into_iter();
        let short = || ChainError::Protocol("batch reply too short".into());
        let before: BlockHash = parse_str(&replies.next().ok_or_else(short)??, "getbestblockhash")?;
        let mut replies: Vec<_> = replies.collect();
        let after: BlockHash = parse_str(&replies.pop().ok_or_else(short)??, "getbestblockhash")?;
        if replies.len() != calls.len() {
            return Err(short());
        }
        Ok(Bracketed {
            tip_moved: before != after,
            replies,
        })
    }

    /// Runs `jobs` on the compute pool and returns their results in order, or the first
    /// error. Waits no longer than the query's deadline.
    pub(crate) fn on_pool<T: Send + 'static>(
        &self,
        jobs: Vec<Job<T>>,
    ) -> Result<Vec<T>, ChainError> {
        let n = jobs.len();
        let (tx, rx) = crossbeam_channel::bounded(n);
        for (index, job) in jobs.into_iter().enumerate() {
            let tx = tx.clone();
            self.handle.runtime().spawn_compute(Box::new(move || {
                let _ = tx.send((index, job()));
            }))?;
        }
        drop(tx);
        let mut done: Vec<Option<T>> = (0..n).map(|_| None).collect();
        for _ in 0..n {
            let (index, result) = self.recv(&rx)?;
            done[index] = Some(result?);
        }
        done.into_iter()
            .map(|result| result.ok_or_else(|| ChainError::Protocol("compute job lost".into())))
            .collect()
    }

    fn recv<T>(&self, rx: &crossbeam_channel::Receiver<T>) -> Result<T, ChainError> {
        loop {
            if self.handle.is_cancelled() {
                return Err(ChainError::Cancelled);
            }
            let left = self
                .deadline
                .map(|deadline| deadline.saturating_duration_since(Instant::now()));
            if left.is_some_and(|left| left.is_zero()) {
                return Err(ChainError::Timeout);
            }
            let wait = left.map_or(CANCEL_POLL, |left| left.min(CANCEL_POLL));
            match rx.recv_timeout(wait) {
                Ok(value) => return Ok(value),
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => return Err(ChainError::Shutdown),
            }
        }
    }

    pub(crate) fn tip(&self) -> Result<Tip, ChainError> {
        let info = self.chain("getblockchaininfo", json!([]))?;
        Ok(Tip {
            height: as_u64(field(&info, "blocks", "getblockchaininfo")?, "blocks")?,
            hash: parse_str(
                field(&info, "bestblockhash", "getblockchaininfo")?,
                "bestblockhash",
            )?,
        })
    }

    pub(crate) fn network(&self) -> Result<Network, ChainError> {
        let info = self.chain("getblockchaininfo", json!([]))?;
        network_from_chain(as_str(
            field(&info, "chain", "getblockchaininfo")?,
            "chain",
        )?)
    }

    pub(crate) fn block_hash(&self, height: u64) -> Result<BlockHash, ChainError> {
        match self.chain("getblockhash", json!([height])) {
            Ok(hash) => parse_str(&hash, "getblockhash"),
            Err(e) if is_height_out_of_range(&e) => Err(ChainError::NotFound),
            Err(e) => Err(e),
        }
    }

    pub(crate) fn header(&self, height: u64) -> Result<Header, ChainError> {
        let hash = self.block_hash(height)?;
        let header: Header = decode_hex(
            &self.chain("getblockheader", json!([hash, false]))?,
            "getblockheader",
        )?;
        if header.block_hash() != hash {
            return Err(ChainError::Protocol(format!(
                "getblockheader {} returned header {}",
                hash,
                header.block_hash()
            )));
        }
        Ok(header)
    }

    pub(crate) fn block(&self, height: u64) -> Result<Arc<Block>, ChainError> {
        let hash = self.block_hash(height)?;
        let hex = match self.chain("getblock", json!([hash, 0])) {
            Ok(Value::String(hex)) => hex,
            Ok(other) => return Err(unexpected("getblock", &other)),
            Err(e) if is_pruned(&e) => return Err(ChainError::Unsupported("block: pruned")),
            Err(e) => return Err(e),
        };
        let mut blocks = self.on_pool(vec![decode_block_job(hash, hex)])?;
        blocks
            .pop()
            .ok_or_else(|| ChainError::Protocol("compute job lost".into()))
    }

    pub(crate) fn transaction(&self, txid: &Txid) -> Result<Arc<Transaction>, ChainError> {
        for _ in 0..ATTEMPTS {
            let mut replies = self.bracketed(&[("getrawtransaction", json!([txid, false]))])?;
            let hex = match replies.pop() {
                Some(Ok(hex)) => hex,
                Some(Err(e)) if is_tx_absent(&e) && replies.tip_moved => continue,
                Some(Err(e)) if is_tx_absent(&e) => return Err(ChainError::NotFound),
                Some(Err(e)) => return Err(e),
                None => return Err(ChainError::Protocol("batch reply too short".into())),
            };
            let tx: Transaction = decode_hex(&hex, "getrawtransaction")?;
            if tx.compute_txid() != *txid {
                return Err(ChainError::Protocol(format!(
                    "getrawtransaction {} returned {}",
                    txid,
                    tx.compute_txid()
                )));
            }
            return Ok(Arc::new(tx));
        }
        Err(moving(txid))
    }

    /// Reads the transaction and the mempool in one request, then the block's height. A
    /// transaction that moved between two reads (mined, or reorged out) is read again.
    pub(crate) fn tx_status(&self, txid: &Txid) -> Result<TxStatus, ChainError> {
        for _ in 0..ATTEMPTS {
            let bracketed = self.bracketed(&[
                ("getrawtransaction", json!([txid, true])),
                ("getmempoolentry", json!([txid])),
            ])?;
            let tip_moved = bracketed.tip_moved;
            let mut replies = bracketed.replies.into_iter();
            let (raw, entry) = match (replies.next(), replies.next()) {
                (Some(raw), Some(entry)) => (raw, entry),
                _ => return Err(ChainError::Protocol("batch reply too short".into())),
            };
            let raw = match raw {
                Ok(raw) if raw.is_object() => Some(raw),
                Ok(other) => return Err(unexpected("getrawtransaction", &other)),
                Err(e) if is_tx_absent(&e) => None,
                Err(e) => return Err(e),
            };
            let in_mempool = match entry {
                Ok(entry) if entry.is_object() => true,
                Ok(other) => return Err(unexpected("getmempoolentry", &other)),
                Err(e) if is_not_in_mempool(&e) => false,
                Err(e) => return Err(e),
            };
            let (block, confirmations) = match &raw {
                Some(raw) => block_and_confirmations(raw, "getrawtransaction")?,
                None => (None, 0),
            };
            if let (Some(block_hash), true) = (block, confirmations > 0) {
                let header = self.chain("getblockheader", json!([block_hash, true]))?;
                let still_active = as_i64(
                    field(&header, "confirmations", "getblockheader")?,
                    "confirmations",
                )? > 0;
                if still_active {
                    let height = as_u64(field(&header, "height", "getblockheader")?, "height")?;
                    return Ok(TxStatus::Confirmed { height, block_hash });
                }
                continue;
            }
            if in_mempool {
                return Ok(TxStatus::Mempool);
            }
            match (raw, block) {
                // Unknown, or only under a block that left the best chain, while the tip
                // stood still.
                (None, _) | (Some(_), Some(_)) if !tip_moved => return Err(ChainError::NotFound),
                // A block came or went during the reads, or the transaction left the
                // mempool between them.
                _ => continue,
            }
        }
        Err(moving(txid))
    }

    pub(crate) fn tx_out(
        &self,
        outpoint: &OutPoint,
        include_mempool: bool,
    ) -> Result<Option<TxOutInfo>, ChainError> {
        let reply = self.chain(
            "gettxout",
            json!([outpoint.txid, outpoint.vout, include_mempool]),
        )?;
        if reply.is_null() {
            return Ok(None);
        }
        let script = field(&reply, "scriptPubKey", "gettxout")?;
        Ok(Some(TxOutInfo {
            value: btc(field(&reply, "value", "gettxout")?, "value")?,
            script_pubkey: script_hex(field(script, "hex", "gettxout.scriptPubKey")?, "hex")?,
            confirmations: as_u32(field(&reply, "confirmations", "gettxout")?, "confirmations")?,
            coinbase: as_bool(field(&reply, "coinbase", "gettxout")?, "coinbase")?,
        }))
    }

    pub(crate) fn broadcast(&self, tx: &Transaction) -> Result<Txid, ChainError> {
        let txid = tx.compute_txid();
        match self.chain("sendrawtransaction", json!([serialize_hex(tx)])) {
            Ok(reply) => {
                let sent: Txid = parse_str(&reply, "sendrawtransaction")?;
                if sent != txid {
                    return Err(ChainError::Protocol(format!(
                        "sendrawtransaction of {} returned {}",
                        txid, sent
                    )));
                }
                Ok(txid)
            }
            Err(e) if is_already_known(&e) => Ok(txid),
            Err(e) => Err(e),
        }
    }

    pub(crate) fn estimate_fee(&self, conf_target: u16) -> Result<FeeRate, ChainError> {
        let reply = self.chain("estimatesmartfee", json!([conf_target]))?;
        if !reply.is_object() {
            return Err(unexpected("estimatesmartfee", &reply));
        }
        let Some(per_kvb) = opt_field(&reply, "feerate") else {
            if let Some(errors) = opt_field(&reply, "errors") {
                log::debug!("no fee estimate for {} blocks: {}", conf_target, errors);
            }
            return Err(ChainError::NotFound);
        };
        let per_kvb = btc(per_kvb, "feerate")?;
        Ok(FeeRate::from_sat_per_vb(per_kvb.to_sat() as f64 / 1000.0))
    }

    pub(crate) fn mempool_txids(&self) -> Result<Vec<Txid>, ChainError> {
        let reply = self.chain("getrawmempool", json!([false]))?;
        as_array(&reply, "getrawmempool")?
            .iter()
            .map(|txid| parse_str(txid, "getrawmempool"))
            .collect()
    }
}

/// A verbose transaction's block and confirmations: 0 confirmations when it is in no block,
/// or in one off the best chain.
pub(crate) fn block_and_confirmations(
    raw: &Value,
    what: &str,
) -> Result<(Option<BlockHash>, u64), ChainError> {
    let block = opt_field(raw, "blockhash")
        .map(|hash| parse_str::<BlockHash>(hash, "blockhash"))
        .transpose()?;
    let confirmations = match (block, opt_field(raw, "confirmations")) {
        (_, Some(confirmations)) => as_u64(confirmations, "confirmations")?,
        (None, None) => 0,
        (Some(_), None) => {
            return Err(ChainError::Protocol(format!(
                "{}: a block hash without confirmations",
                what
            )))
        }
    };
    Ok((block, confirmations))
}

/// Decodes a block from hex and checks it hashes to `hash` and its transactions match its
/// header.
pub(crate) fn decode_block_job(hash: BlockHash, hex: String) -> Job<Arc<Block>> {
    Box::new(move || {
        let block: Block = deserialize_hex(&hex)
            .map_err(|e| ChainError::Protocol(format!("getblock {}: {}", hash, e)))?;
        if block.block_hash() != hash {
            return Err(ChainError::Protocol(format!(
                "getblock {} returned block {}",
                hash,
                block.block_hash()
            )));
        }
        if !block.check_merkle_root() || !block.check_witness_commitment() {
            return Err(ChainError::Protocol(format!(
                "getblock {}: transactions do not match the header",
                hash
            )));
        }
        Ok(Arc::new(block))
    })
}

/// The network a `getblockchaininfo` `chain` string names.
pub(crate) fn network_from_chain(chain: &str) -> Result<Network, ChainError> {
    match chain {
        "main" => Ok(Network::Bitcoin),
        "test" => Ok(Network::Testnet),
        "testnet4" => Ok(Network::Testnet4),
        "signet" => Ok(Network::Signet),
        "regtest" => Ok(Network::Regtest),
        other => Err(ChainError::Protocol(format!("unknown chain {:?}", other))),
    }
}

/// Core's answer that the transaction is in neither the mempool nor a complete txindex.
pub(crate) fn is_tx_absent(e: &ChainError) -> bool {
    matches!(e, ChainError::Server { code: INVALID_ADDRESS_OR_KEY, message }
        if message.starts_with("No such mempool or blockchain transaction"))
}

/// `getmempoolentry`'s answer for a transaction not in the mempool.
pub(crate) fn is_not_in_mempool(e: &ChainError) -> bool {
    matches!(
        e,
        ChainError::Server {
            code: INVALID_ADDRESS_OR_KEY,
            ..
        }
    )
}

fn is_height_out_of_range(e: &ChainError) -> bool {
    matches!(e, ChainError::Server { code: INVALID_PARAMETER, message }
        if message.starts_with("Block height out of range"))
}

pub(crate) fn is_pruned(e: &ChainError) -> bool {
    matches!(e, ChainError::Server { code: MISC_ERROR, message } if message.contains("pruned data"))
}

/// Core already has the transaction: in a block (`-27`) or in its mempool.
fn is_already_known(e: &ChainError) -> bool {
    match e {
        ChainError::Server {
            code: VERIFY_ALREADY_IN_CHAIN,
            ..
        } => true,
        ChainError::Server {
            code: VERIFY_ERROR | VERIFY_REJECTED,
            message,
        } => [
            "txn-already-known",
            "txn-already-in-mempool",
            "txn-same-nonwitness-data-in-mempool",
        ]
        .iter()
        .any(|reason| message.contains(reason)),
        _ => false,
    }
}

fn unexpected(what: &str, value: &Value) -> ChainError {
    let mut shown = value.to_string();
    shown.truncate(80);
    ChainError::Protocol(format!("{}: unexpected {}", what, shown))
}

pub(crate) fn field<'v>(value: &'v Value, name: &str, what: &str) -> Result<&'v Value, ChainError> {
    value
        .get(name)
        .ok_or_else(|| ChainError::Protocol(format!("{}: no {:?} field", what, name)))
}

/// A field that is present and not `null`.
pub(crate) fn opt_field<'v>(value: &'v Value, name: &str) -> Option<&'v Value> {
    value.get(name).filter(|v| !v.is_null())
}

pub(crate) fn as_str<'v>(value: &'v Value, what: &str) -> Result<&'v str, ChainError> {
    value.as_str().ok_or_else(|| unexpected(what, value))
}

pub(crate) fn as_u64(value: &Value, what: &str) -> Result<u64, ChainError> {
    value.as_u64().ok_or_else(|| unexpected(what, value))
}

pub(crate) fn as_u32(value: &Value, what: &str) -> Result<u32, ChainError> {
    value
        .as_u64()
        .and_then(|n| u32::try_from(n).ok())
        .ok_or_else(|| unexpected(what, value))
}

pub(crate) fn as_i64(value: &Value, what: &str) -> Result<i64, ChainError> {
    value.as_i64().ok_or_else(|| unexpected(what, value))
}

pub(crate) fn as_bool(value: &Value, what: &str) -> Result<bool, ChainError> {
    value.as_bool().ok_or_else(|| unexpected(what, value))
}

pub(crate) fn as_array<'v>(value: &'v Value, what: &str) -> Result<&'v Vec<Value>, ChainError> {
    value.as_array().ok_or_else(|| unexpected(what, value))
}

pub(crate) fn parse_str<T>(value: &Value, what: &str) -> Result<T, ChainError>
where
    T: FromStr,
    T::Err: Display,
{
    as_str(value, what)?
        .parse()
        .map_err(|e| ChainError::Protocol(format!("{}: {}", what, e)))
}

/// A BTC amount, parsed as `bitcoincore-rpc` does.
pub(crate) fn btc(value: &Value, what: &str) -> Result<Amount, ChainError> {
    let btc = value.as_f64().ok_or_else(|| unexpected(what, value))?;
    Amount::from_btc(btc).map_err(|e| ChainError::Protocol(format!("{}: {}", what, e)))
}

pub(crate) fn script_hex(value: &Value, what: &str) -> Result<ScriptBuf, ChainError> {
    ScriptBuf::from_hex(as_str(value, what)?)
        .map_err(|e| ChainError::Protocol(format!("{}: {}", what, e)))
}

pub(crate) fn decode_hex<T: Decodable>(value: &Value, what: &str) -> Result<T, ChainError> {
    deserialize_hex(as_str(value, what)?)
        .map_err(|e| ChainError::Protocol(format!("{}: {}", what, e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn server(code: i64, message: &str) -> ChainError {
        ChainError::Server {
            code,
            message: message.into(),
        }
    }

    #[test]
    fn only_a_complete_index_answers_absence() {
        let suffix = ". Use gettransaction for wallet transactions.";
        assert!(is_tx_absent(&server(
            -5,
            &format!("No such mempool or blockchain transaction{}", suffix)
        )));
        for unsure in [
            "No such mempool transaction. Use -txindex or provide a block hash to enable blockchain transaction queries",
            "No such mempool transaction. Blockchain transactions are still in the process of being indexed",
            "No such transaction found in the provided block",
        ] {
            assert!(!is_tx_absent(&server(-5, &format!("{}{}", unsure, suffix))), "{}", unsure);
        }
        assert!(!is_tx_absent(&server(
            -8,
            "No such mempool or blockchain transaction"
        )));
        assert!(!is_tx_absent(&ChainError::Timeout));
        assert!(!is_tx_absent(&ChainError::Transport("reset".into())));
    }

    #[test]
    fn already_known_broadcasts_are_recognised() {
        assert!(is_already_known(&server(
            -27,
            "Transaction already in block chain"
        )));
        assert!(is_already_known(&server(
            -27,
            "Transaction outputs already in utxo set"
        )));
        assert!(is_already_known(&server(-26, "txn-already-known")));
        assert!(is_already_known(&server(-26, "txn-already-in-mempool")));
        assert!(is_already_known(&server(
            -26,
            "txn-same-nonwitness-data-in-mempool"
        )));
        assert!(!is_already_known(&server(-26, "min relay fee not met")));
        assert!(!is_already_known(&server(
            -25,
            "bad-txns-inputs-missingorspent"
        )));
        assert!(!is_already_known(&server(-22, "TX decode failed")));
        assert!(!is_already_known(&ChainError::Timeout));
    }

    #[test]
    fn heights_and_pruning_are_classified() {
        assert!(is_height_out_of_range(&server(
            -8,
            "Block height out of range"
        )));
        assert!(!is_height_out_of_range(&server(-8, "Invalid parameter")));
        assert!(is_pruned(&server(-1, "Block not available (pruned data)")));
        assert!(!is_pruned(&server(-1, "Block not found on disk")));
    }

    #[test]
    fn every_chain_name_maps() {
        assert_eq!(network_from_chain("main"), Ok(Network::Bitcoin));
        assert_eq!(network_from_chain("test"), Ok(Network::Testnet));
        assert_eq!(network_from_chain("testnet4"), Ok(Network::Testnet4));
        assert_eq!(network_from_chain("signet"), Ok(Network::Signet));
        assert_eq!(network_from_chain("regtest"), Ok(Network::Regtest));
        for bad in ["bitcoin", "testnet", "", "Main"] {
            assert!(
                matches!(network_from_chain(bad), Err(ChainError::Protocol(_))),
                "{}",
                bad
            );
        }
    }

    #[test]
    fn amounts_parse_like_bitcoincore_rpc() {
        assert_eq!(btc(&json!(0.00012345), "a"), Ok(Amount::from_sat(12_345)));
        assert_eq!(
            btc(&json!(21_000_000.0), "a"),
            Ok(Amount::from_sat(2_100_000_000_000_000))
        );
        assert!(btc(&json!(-1.0), "a").is_err());
        assert!(btc(&json!("1.0"), "a").is_err());
    }

    #[test]
    fn decoded_blocks_must_match_their_hash() {
        let genesis = bitcoin::constants::genesis_block(Network::Regtest);
        let hex = serialize_hex(&genesis);
        let block = decode_block_job(genesis.block_hash(), hex.clone())().unwrap();
        assert_eq!(*block, genesis);
        let other = bitcoin::constants::genesis_block(Network::Bitcoin).block_hash();
        assert!(matches!(
            decode_block_job(other, hex)(),
            Err(ChainError::Protocol(_))
        ));
        assert!(matches!(
            decode_block_job(other, "zz".into())(),
            Err(ChainError::Protocol(_))
        ));
        let mut tampered = genesis.clone();
        tampered.txdata[0].output[0].value = Amount::from_sat(1);
        assert_eq!(tampered.block_hash(), genesis.block_hash());
        assert!(matches!(
            decode_block_job(genesis.block_hash(), serialize_hex(&tampered))(),
            Err(ChainError::Protocol(_))
        ));
    }
}
