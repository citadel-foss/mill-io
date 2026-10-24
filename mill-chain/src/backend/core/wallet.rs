//! Bitcoin Core's watch-only wallet: bootstrap, descriptor imports, rescans and the wallet's
//! view of its outputs and transactions.

use super::chain::{as_array, as_bool, as_i64, as_str, as_u32, as_u64, btc, field, opt_field};
use super::chain::{parse_str, script_hex, signed_btc, Query};
use crate::{ChainError, ChainHandle};
use bitcoin::address::NetworkUnchecked;
use bitcoin::{Address, Amount, BlockHash, OutPoint, ScriptBuf, SignedAmount, Txid};
use serde_json::{json, Value};
use std::time::Duration;

/// `RPC_WALLET_ERROR`, which `createwallet` answers when the wallet already exists.
const WALLET_ERROR: i64 = -4;
/// `RPC_WALLET_ALREADY_LOADED`.
const WALLET_ALREADY_LOADED: i64 = -35;
/// `listunspent`'s own default `maxconf`.
const MAX_CONFIRMATIONS: u32 = 9_999_999;

/// Bitcoin Core's wallet RPCs, from [`ChainHandle::core_wallet`].
///
/// [`prepare`](Self::prepare) works on any wallet name and
/// [`derive_addresses`](Self::derive_addresses) needs no wallet. Every other call goes to the
/// wallet named by [`CoreConfig::wallet`](crate::CoreConfig::wallet), and fails with
/// [`ChainError::Unsupported`] when none is set. Calls keep the deadline, retries and cancel
/// flag of the handle the wallet came from.
#[derive(Debug, Clone)]
pub struct CoreWallet {
    handle: ChainHandle,
}

/// An unspent output of the wallet, as `listunspent` reports it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WalletUtxo {
    /// The output.
    pub outpoint: OutPoint,
    /// Its value.
    pub value: Amount,
    /// The script it pays to.
    pub script_pubkey: ScriptBuf,
    /// Confirmations; 0 while in the mempool.
    pub confirmations: u32,
    /// The address it pays to, if the script has one.
    pub address: Option<Address<NetworkUnchecked>>,
    /// The address's label.
    pub label: Option<String>,
    /// The descriptor that matched it.
    pub descriptor: Option<String>,
    /// The witness script of a P2WSH output, if known.
    pub witness_script: Option<ScriptBuf>,
    /// The redeem script of a P2SH output, if known.
    pub redeem_script: Option<ScriptBuf>,
    /// The wallet holds the keys to spend it.
    pub spendable: bool,
    /// The wallet knows how to spend it, ignoring missing keys.
    pub solvable: bool,
    /// Core considers it safe to spend: confirmed, or from the wallet's own transactions.
    pub safe: bool,
}

/// Why a wallet transaction entry exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TxCategory {
    /// The wallet paid out.
    Send,
    /// The wallet was paid.
    Receive,
    /// A mature coinbase output.
    Generate,
    /// A coinbase output not yet spendable.
    Immature,
    /// A coinbase output whose block left the best chain.
    Orphan,
}

/// One entry of the wallet's history, as `listtransactions` reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalletTx {
    /// The transaction.
    pub txid: Txid,
    /// The output the entry is about.
    pub vout: u32,
    /// Why the entry exists.
    pub category: TxCategory,
    /// The amount, negative for [`TxCategory::Send`].
    pub amount: SignedAmount,
    /// The fee, negative, on [`TxCategory::Send`] entries.
    pub fee: Option<SignedAmount>,
    /// Confirmations; 0 in the mempool, negative when in conflict with the best chain.
    pub confirmations: i32,
    /// The confirming block.
    pub block_hash: Option<BlockHash>,
    /// The confirming block's height.
    pub block_height: Option<u64>,
    /// When the wallet first saw it, in seconds since the Unix epoch.
    pub time: u64,
    /// The address involved, if the script has one.
    pub address: Option<Address<NetworkUnchecked>>,
    /// The address's label.
    pub label: Option<String>,
}

/// What the wallet knows of an address, as `getaddressinfo` reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddressInfo {
    /// The wallet tracks it.
    pub is_mine: bool,
    /// The wallet tracks it without the keys. Descriptor wallets report every address they
    /// track as `is_mine` and this as `false`, keys or not.
    pub is_watch_only: bool,
    /// The wallet knows how to spend it, ignoring missing keys.
    pub solvable: bool,
    /// The descriptor for it, when solvable.
    pub desc: Option<String>,
}

/// How far a running rescan has got.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScanProgress {
    /// How long it has been running.
    pub duration: Duration,
    /// How much is done, from 0.0 to 1.0.
    pub progress: f64,
}

impl CoreWallet {
    pub(crate) fn new(handle: ChainHandle) -> Self {
        Self { handle }
    }

    fn query(&self, what: &'static str) -> Result<Query<'_>, ChainError> {
        Query::new(&self.handle, what)
    }

    /// Loads wallet `name`, creating it as a descriptor wallet with private keys disabled
    /// if it does not exist. Does nothing if it is already loaded.
    pub fn prepare(&self, name: &str) -> Result<(), ChainError> {
        let q = self.query("prepare")?;
        let loaded = q.chain("listwallets", json!([]))?;
        if strings(&loaded, "listwallets")?.any(|w| w == name) {
            return Ok(());
        }
        let dir = q.chain("listwalletdir", json!([]))?;
        let on_disk = as_array(field(&dir, "wallets", "listwalletdir")?, "wallets")?
            .iter()
            .map(|w| as_str(field(w, "name", "listwalletdir")?, "name"))
            .collect::<Result<Vec<_>, _>>()?;
        if on_disk.contains(&name) {
            return load(&q, name);
        }
        let created = q.chain(
            "createwallet",
            json!([name, true, false, Value::Null, false, true]),
        );
        match created {
            Ok(_) => Ok(()),
            // Created by someone else since `listwalletdir`.
            Err(ChainError::Server {
                code: WALLET_ERROR,
                message,
            }) if message.contains("already exists") => load(&q, name),
            Err(e) => Err(e),
        }
    }

    /// The wallet's unspent outputs with `minconf` to `maxconf` confirmations, both
    /// inclusive; `minconf` 0 includes the mempool, and `None` means no upper bound. Locked
    /// outputs are listed too.
    pub fn list_unspent(
        &self,
        minconf: u32,
        maxconf: Option<u32>,
    ) -> Result<Vec<WalletUtxo>, ChainError> {
        let q = self.query("list_unspent")?;
        let maxconf = maxconf.unwrap_or(MAX_CONFIRMATIONS);
        let reply = q.wallet("listunspent", json!([minconf, maxconf]))?;
        as_array(&reply, "listunspent")?
            .iter()
            .map(wallet_utxo)
            .collect()
    }

    /// Up to `count` entries of the wallet's history, after skipping the `skip` newest,
    /// oldest first as Core orders them. Watch-only entries are included.
    pub fn list_transactions(
        &self,
        count: usize,
        skip: usize,
    ) -> Result<Vec<WalletTx>, ChainError> {
        let q = self.query("list_transactions")?;
        let reply = q.wallet("listtransactions", json!(["*", count, skip, true]))?;
        as_array(&reply, "listtransactions")?
            .iter()
            .map(wallet_tx)
            .collect()
    }

    /// What the wallet knows of `addr`.
    pub fn address_info(&self, addr: &Address) -> Result<AddressInfo, ChainError> {
        let q = self.query("address_info")?;
        let reply = q.wallet("getaddressinfo", json!([addr.to_string()]))?;
        let what = "getaddressinfo";
        Ok(AddressInfo {
            is_mine: as_bool(field(&reply, "ismine", what)?, "ismine")?,
            is_watch_only: opt_field(&reply, "iswatchonly")
                .map(|v| as_bool(v, "iswatchonly"))
                .transpose()?
                .unwrap_or(false),
            solvable: as_bool(field(&reply, "solvable", what)?, "solvable")?,
            desc: opt_string(&reply, "desc")?,
        })
    }

    /// The addresses `desc` derives, over the inclusive index `range` for a ranged
    /// descriptor. A node RPC: it needs no configured wallet.
    pub fn derive_addresses(
        &self,
        desc: &str,
        range: Option<[u32; 2]>,
    ) -> Result<Vec<Address<NetworkUnchecked>>, ChainError> {
        let q = self.query("derive_addresses")?;
        let params = match range {
            Some(range) => json!([desc, range]),
            None => json!([desc]),
        };
        let reply = q.chain("deriveaddresses", params)?;
        as_array(&reply, "deriveaddresses")?
            .iter()
            .map(|a| parse_str(a, "deriveaddresses"))
            .collect()
    }

    /// Imports `importdescriptors` request objects. Fails if Core rejects any of them,
    /// with [`ChainError::Server`] naming each rejected descriptor; the others stay
    /// imported.
    ///
    /// This runs on the long-call lane and may rescan for a long time: use a handle whose
    /// deadline allows for it.
    pub fn import_descriptors(&self, reqs: &[Value]) -> Result<(), ChainError> {
        let q = self.query("import_descriptors")?;
        let reply = q.wallet("importdescriptors", json!([reqs]))?;
        let results = as_array(&reply, "importdescriptors")?;
        if results.len() != reqs.len() {
            return Err(ChainError::Protocol(format!(
                "importdescriptors: {} results for {} requests",
                results.len(),
                reqs.len()
            )));
        }
        let mut code = None;
        let mut failures = Vec::new();
        for (req, result) in reqs.iter().zip(results) {
            let success = as_bool(field(result, "success", "importdescriptors")?, "success")?;
            if let Some(warnings) = opt_field(result, "warnings") {
                log::warn!("importdescriptors {}: {}", req["desc"], warnings);
            }
            if success {
                continue;
            }
            let error = opt_field(result, "error");
            let message = error
                .and_then(|e| e.get("message"))
                .and_then(Value::as_str)
                .unwrap_or("rejected");
            code = code.or_else(|| error.and_then(|e| e.get("code")).and_then(Value::as_i64));
            failures.push(format!("{}: {}", req["desc"], message));
        }
        if failures.is_empty() {
            return Ok(());
        }
        Err(ChainError::Server {
            code: code.unwrap_or(-1),
            message: format!(
                "importdescriptors rejected {} of {}: {}",
                failures.len(),
                reqs.len(),
                failures.join("; ")
            ),
        })
    }

    /// Rescans the chain from `height` for the wallet's descriptors, returning when the
    /// rescan is done.
    ///
    /// This runs on the long-call lane and is never resent: use a handle whose deadline
    /// allows for it, and [`scanning_status`](Self::scanning_status) to follow it.
    pub fn rescan_from(&self, height: u64) -> Result<(), ChainError> {
        let q = self.query("rescan_from")?;
        let reply = q.wallet("rescanblockchain", json!([height]))?;
        as_u64(
            field(&reply, "start_height", "rescanblockchain")?,
            "start_height",
        )?;
        Ok(())
    }

    /// The running rescan's progress; `None` when the wallet is not rescanning.
    pub fn scanning_status(&self) -> Result<Option<ScanProgress>, ChainError> {
        let q = self.query("scanning_status")?;
        let reply = q.wallet("getwalletinfo", json!([]))?;
        let scanning = field(&reply, "scanning", "getwalletinfo")?;
        if scanning == &Value::Bool(false) {
            return Ok(None);
        }
        let what = "getwalletinfo.scanning";
        let progress = field(scanning, "progress", what)?
            .as_f64()
            .ok_or_else(|| ChainError::Protocol(format!("{}: progress is not a number", what)))?;
        Ok(Some(ScanProgress {
            duration: Duration::from_secs(as_u64(field(scanning, "duration", what)?, "duration")?),
            progress,
        }))
    }

    /// Whether descriptor `desc` is imported over a range starting at index 0 and reaching
    /// at least `last_index`.
    ///
    /// `desc` must be written as `listdescriptors` lists it, checksum included. Fails with
    /// [`ChainError::Unsupported`] on a legacy wallet, which has no `listdescriptors`.
    pub fn imported_range_covers(&self, desc: &str, last_index: u32) -> Result<bool, ChainError> {
        let q = self.query("imported_range_covers")?;
        let info = q.wallet("getwalletinfo", json!([]))?;
        let descriptors = opt_field(&info, "descriptors")
            .map(|v| as_bool(v, "descriptors"))
            .transpose()?;
        if descriptors != Some(true) {
            return Err(ChainError::Unsupported(
                "imported_range_covers: not a descriptor wallet",
            ));
        }
        let listed = q.wallet("listdescriptors", json!([]))?;
        for entry in as_array(
            field(&listed, "descriptors", "listdescriptors")?,
            "descriptors",
        )? {
            if as_str(field(entry, "desc", "listdescriptors")?, "desc")? != desc {
                continue;
            }
            let Some(range) = opt_field(entry, "range") else {
                return Ok(false);
            };
            let range = as_array(range, "range")?;
            let [start, end] = range.as_slice() else {
                return Err(ChainError::Protocol(
                    "listdescriptors: range is not a pair".into(),
                ));
            };
            // Core lists the range with both ends inclusive.
            return Ok(
                as_u64(start, "range")? == 0 && as_u64(end, "range")? >= u64::from(last_index)
            );
        }
        Ok(false)
    }
}

fn load(q: &Query<'_>, name: &str) -> Result<(), ChainError> {
    match q.chain("loadwallet", json!([name])) {
        Ok(_) => Ok(()),
        // Loaded by someone else since `listwallets`.
        Err(ChainError::Server {
            code: WALLET_ALREADY_LOADED,
            ..
        }) => Ok(()),
        Err(e) => Err(e),
    }
}

fn strings<'v>(
    value: &'v Value,
    what: &'static str,
) -> Result<impl Iterator<Item = &'v str>, ChainError> {
    let items = as_array(value, what)?
        .iter()
        .map(|v| as_str(v, what))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(items.into_iter())
}

fn opt_string(value: &Value, name: &str) -> Result<Option<String>, ChainError> {
    opt_field(value, name)
        .map(|v| as_str(v, name).map(str::to_string))
        .transpose()
}

fn opt_script(value: &Value, name: &str) -> Result<Option<ScriptBuf>, ChainError> {
    opt_field(value, name)
        .map(|v| script_hex(v, name))
        .transpose()
}

fn opt_address(value: &Value) -> Result<Option<Address<NetworkUnchecked>>, ChainError> {
    opt_field(value, "address")
        .map(|v| parse_str(v, "address"))
        .transpose()
}

fn wallet_utxo(entry: &Value) -> Result<WalletUtxo, ChainError> {
    let what = "listunspent";
    Ok(WalletUtxo {
        outpoint: OutPoint::new(
            parse_str(field(entry, "txid", what)?, "txid")?,
            as_u32(field(entry, "vout", what)?, "vout")?,
        ),
        value: btc(field(entry, "amount", what)?, "amount")?,
        script_pubkey: script_hex(field(entry, "scriptPubKey", what)?, "scriptPubKey")?,
        confirmations: as_u32(field(entry, "confirmations", what)?, "confirmations")?,
        address: opt_address(entry)?,
        label: opt_string(entry, "label")?,
        descriptor: opt_string(entry, "desc")?,
        witness_script: opt_script(entry, "witnessScript")?,
        redeem_script: opt_script(entry, "redeemScript")?,
        spendable: as_bool(field(entry, "spendable", what)?, "spendable")?,
        solvable: as_bool(field(entry, "solvable", what)?, "solvable")?,
        safe: as_bool(field(entry, "safe", what)?, "safe")?,
    })
}

fn wallet_tx(entry: &Value) -> Result<WalletTx, ChainError> {
    let what = "listtransactions";
    let category = match as_str(field(entry, "category", what)?, "category")? {
        "send" => TxCategory::Send,
        "receive" => TxCategory::Receive,
        "generate" => TxCategory::Generate,
        "immature" => TxCategory::Immature,
        "orphan" => TxCategory::Orphan,
        other => {
            return Err(ChainError::Protocol(format!(
                "listtransactions: unknown category {:?}",
                other
            )))
        }
    };
    let confirmations = as_i64(field(entry, "confirmations", what)?, "confirmations")?;
    Ok(WalletTx {
        txid: parse_str(field(entry, "txid", what)?, "txid")?,
        vout: as_u32(field(entry, "vout", what)?, "vout")?,
        category,
        amount: signed_btc(field(entry, "amount", what)?, "amount")?,
        fee: opt_field(entry, "fee")
            .map(|v| signed_btc(v, "fee"))
            .transpose()?,
        confirmations: i32::try_from(confirmations).map_err(|_| {
            ChainError::Protocol(format!("listtransactions: confirmations {}", confirmations))
        })?,
        block_hash: opt_field(entry, "blockhash")
            .map(|v| parse_str(v, "blockhash"))
            .transpose()?,
        block_height: opt_field(entry, "blockheight")
            .map(|v| as_u64(v, "blockheight"))
            .transpose()?,
        time: as_u64(field(entry, "time", what)?, "time")?,
        address: opt_address(entry)?,
        label: opt_string(entry, "label")?,
    })
}
