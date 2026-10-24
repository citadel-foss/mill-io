//! Finding the transaction that spends an outpoint, in a bounded number of requests.
//!
//! One request reads the tip, the outpoint in the confirmed UTXO set, the funding
//! transaction and, unless only confirmed spends count, the mempool spender. A second reads
//! the funding block's height, a third the block hashes from there to the tip. The blocks
//! then come in batches sized from the largest block seen so far, so that a reply fits
//! `max_body_bytes`; they are decoded on the compute pool and kept in the client's block
//! cache.
//!
//! Fund safety: `Ok(None)` is returned only when Core shows the outpoint unspent in the
//! confirmed UTXO set, or shows its funding transaction outside the best chain, with the
//! best block unchanged across the reads. An outpoint Core reports spent whose spender the
//! scan cannot find is an error. The scan has no depth limit beyond the query's deadline.

use super::chain::{as_array, as_i64, as_u64, block_and_confirmations, decode_block_job, field};
use super::chain::{is_pruned, is_tx_absent};
use super::chain::{opt_field, parse_str, script_hex, Job, Query};
use crate::proto::http::is_body_too_large;
use crate::ChainError;
use bitcoin::{Block, BlockHash, OutPoint, Transaction, Txid};
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

/// Most bytes of blocks asked for in one request. The reply's JSON is parsed on the event
/// loop thread, so this bounds how long a batch holds it up.
const MAX_BATCH_REPLY_BYTES: usize = 8 * 1024 * 1024;
/// Most blocks asked for in one request, whatever their size.
const MAX_BLOCKS_PER_REQUEST: usize = 128;
/// Most block hashes asked for in one request.
const MAX_HASHES_PER_REQUEST: u64 = 2000;
/// Bytes of JSON around one block's hex in a batch reply.
const REPLY_OVERHEAD: usize = 64;
/// Core never adds an output with a script longer than this to the UTXO set.
const MAX_SCRIPT_SIZE: usize = 10_000;
/// How often the lookup starts over when the chain changed under it.
const ATTEMPTS: usize = 3;
/// Serialized bytes of blocks a client keeps.
const CACHE_BYTES: usize = 64 * 1024 * 1024;
/// Blocks a client keeps.
const CACHE_BLOCKS: usize = 1024;

/// Decoded blocks by hash, the oldest dropped first once the cache is full.
#[derive(Debug, Default)]
pub(crate) struct BlockCache {
    blocks: HashMap<BlockHash, Arc<Block>>,
    order: VecDeque<(BlockHash, usize)>,
    bytes: usize,
}

impl BlockCache {
    pub(crate) fn get(&self, hash: &BlockHash) -> Option<Arc<Block>> {
        self.blocks.get(hash).cloned()
    }

    pub(crate) fn insert(&mut self, hash: BlockHash, block: Arc<Block>) {
        let size = block.total_size();
        if size > CACHE_BYTES || self.blocks.contains_key(&hash) {
            return;
        }
        while self.bytes + size > CACHE_BYTES || self.order.len() >= CACHE_BLOCKS {
            let Some((old, old_size)) = self.order.pop_front() else {
                break;
            };
            self.blocks.remove(&old);
            self.bytes -= old_size;
        }
        self.blocks.insert(hash, block);
        self.order.push_back((hash, size));
        self.bytes += size;
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.blocks.len()
    }
}

/// A block in scan order, with its hex length when it was fetched rather than cached.
struct Fetched {
    hash: BlockHash,
    block: Arc<Block>,
    hex_len: Option<usize>,
}

enum Lookup {
    Spent(Arc<Transaction>),
    NoSpend,
    /// The chain or the mempool changed between two reads: start again.
    Moved,
}

enum Scan {
    Found(BlockHash, Arc<Transaction>),
    NotFound,
    /// A height in the range is no longer on the best chain.
    Moved,
}

/// The transaction spending `outpoint`: from the mempool unless `confirmed_only`, else
/// from the best chain.
pub(crate) fn spending_tx(
    q: &Query<'_>,
    outpoint: &OutPoint,
    confirmed_only: bool,
) -> Result<Option<Arc<Transaction>>, ChainError> {
    for _ in 0..ATTEMPTS {
        match lookup(q, outpoint, confirmed_only)? {
            Lookup::Spent(tx) => return Ok(Some(tx)),
            Lookup::NoSpend => return Ok(None),
            Lookup::Moved => {}
        }
    }
    Err(ChainError::Protocol(format!(
        "the chain kept changing while looking up the spend of {}",
        outpoint
    )))
}

fn lookup(q: &Query<'_>, outpoint: &OutPoint, confirmed_only: bool) -> Result<Lookup, ChainError> {
    let confirmed_out = ("gettxout", json!([outpoint.txid, outpoint.vout, false]));
    // Core evaluates these in order. The mempool is read before the confirmed UTXO set, so
    // a spend mined between the two reads is seen by one of them, and the whole batch
    // sits between two reads of the best block, so a block arriving in it is noticed.
    let mut calls = vec![("getblockcount", json!([]))];
    if !confirmed_only {
        let prevout = json!([{"txid": outpoint.txid, "vout": outpoint.vout}]);
        calls.push(("gettxspendingprevout", json!([prevout])));
    }
    calls.push(confirmed_out.clone());
    calls.push(("getrawtransaction", json!([outpoint.txid, true])));
    let bracketed = q.bracketed(&calls)?;
    if bracketed.tip_moved {
        return Ok(Lookup::Moved);
    }
    let mut replies = bracketed.replies.into_iter();
    let mut next = || {
        replies
            .next()
            .unwrap_or_else(|| Err(ChainError::Protocol("batch reply too short".into())))
    };
    let tip = as_u64(&next()?, "getblockcount")?;
    let spender = match confirmed_only {
        true => None,
        false => Some(next()?),
    };
    let unspent = is_unspent(&next()?)?;
    let funding = match next() {
        Ok(funding) if funding.is_object() => funding,
        Ok(_) => {
            return Err(ChainError::Protocol(
                "getrawtransaction: not an object".into(),
            ))
        }
        Err(e) if is_tx_absent(&e) => return Err(ChainError::NotFound),
        Err(e) => return Err(e),
    };
    let outputs = as_array(field(&funding, "vout", "getrawtransaction")?, "vout")?;
    let Some(output) = outputs.get(outpoint.vout as usize) else {
        return Err(ChainError::NotFound);
    };
    if let Some(spender) = spender {
        if let Some(found) = mempool_spend(q, outpoint, &spender)? {
            return Ok(found);
        }
    }
    if unspent {
        return Ok(Lookup::NoSpend);
    }
    let script = field(output, "scriptPubKey", "getrawtransaction.vout")?;
    let script = script_hex(field(script, "hex", "scriptPubKey")?, "scriptPubKey.hex")?;
    if script.is_op_return() || script.len() > MAX_SCRIPT_SIZE {
        return Ok(Lookup::NoSpend);
    }
    let (block, confirmations) = block_and_confirmations(&funding, "getrawtransaction")?;
    let Some(block) = block.filter(|_| confirmations > 0) else {
        // Not in the best chain, so nothing in it can spend the output.
        return Ok(Lookup::NoSpend);
    };
    let Some(height) = active_height(q, &block)? else {
        return Ok(Lookup::Moved);
    };

    let (mut from, mut to) = (height, tip);
    for _ in 0..ATTEMPTS {
        match scan(q, outpoint, from, to)? {
            Scan::Found(hash, tx) => {
                return match active_height(q, &hash)? {
                    Some(_) => Ok(Lookup::Spent(tx)),
                    None => Ok(Lookup::Moved),
                };
            }
            Scan::Moved => return Ok(Lookup::Moved),
            Scan::NotFound => {}
        }
        // Core says spent, yet no block up to `to` spends it: blocks came since the tip
        // was read, or the chain changed.
        let replies = q.batch(&[("getblockcount", json!([])), confirmed_out.clone()])?;
        let mut replies = replies.into_iter();
        let (Some(count), Some(out)) = (replies.next(), replies.next()) else {
            return Err(ChainError::Protocol("batch reply too short".into()));
        };
        let new_tip = as_u64(&count?, "getblockcount")?;
        if is_unspent(&out?)? {
            // Unspent after all: a reorg undid the spend, which may be back in the mempool.
            return match confirmed_only {
                true => Ok(Lookup::NoSpend),
                false => Ok(Lookup::Moved),
            };
        }
        if new_tip <= to {
            return Ok(Lookup::Moved);
        }
        (from, to) = (to + 1, new_tip);
    }
    Ok(Lookup::Moved)
}

/// `gettxout`'s answer: an output object, or `null` for spent or absent.
fn is_unspent(reply: &Value) -> Result<bool, ChainError> {
    match reply {
        Value::Null => Ok(false),
        Value::Object(out) if out.contains_key("value") => Ok(true),
        _ => Err(ChainError::Protocol("gettxout: not an output".into())),
    }
}

/// The mempool spender `gettxspendingprevout` reported, fetched.
fn mempool_spend(
    q: &Query<'_>,
    outpoint: &OutPoint,
    reply: &Value,
) -> Result<Option<Lookup>, ChainError> {
    let what = "gettxspendingprevout";
    let entries = as_array(reply, what)?;
    let [entry] = entries.as_slice() else {
        return Err(ChainError::Protocol(format!(
            "{}: {} entries for one outpoint",
            what,
            entries.len()
        )));
    };
    let txid: Txid = parse_str(field(entry, "txid", what)?, "txid")?;
    let vout = as_u64(field(entry, "vout", what)?, "vout")?;
    if txid != outpoint.txid || vout != u64::from(outpoint.vout) {
        return Err(ChainError::Protocol(format!(
            "{}: answered for {}:{}",
            what, txid, vout
        )));
    }
    let Some(spender) = opt_field(entry, "spendingtxid") else {
        return Ok(None);
    };
    let spender = parse_str(spender, "spendingtxid")?;
    match q.transaction(&spender) {
        Ok(tx) if spends(&tx, outpoint) => Ok(Some(Lookup::Spent(tx))),
        Ok(_) => Err(ChainError::Protocol(format!(
            "{} named {}, which does not spend {}",
            what, spender, outpoint
        ))),
        // Gone from the mempool, and not mined, since it was named.
        Err(ChainError::NotFound) => Ok(Some(Lookup::Moved)),
        Err(e) => Err(e),
    }
}

/// The height of `block` if it is on the best chain.
fn active_height(q: &Query<'_>, block: &BlockHash) -> Result<Option<u64>, ChainError> {
    let header = q.chain("getblockheader", json!([block, true]))?;
    let confirmations = as_i64(
        field(&header, "confirmations", "getblockheader")?,
        "confirmations",
    )?;
    if confirmations < 1 {
        return Ok(None);
    }
    Ok(Some(as_u64(
        field(&header, "height", "getblockheader")?,
        "height",
    )?))
}

fn spends(tx: &Transaction, outpoint: &OutPoint) -> bool {
    tx.input
        .iter()
        .any(|input| input.previous_output == *outpoint)
}

/// Scans the best chain's blocks `from..=to`, in order, for a spend of `outpoint`.
fn scan(q: &Query<'_>, outpoint: &OutPoint, from: u64, to: u64) -> Result<Scan, ChainError> {
    let budget = (q.core().max_body_bytes() / 4 * 3).min(MAX_BATCH_REPLY_BYTES);
    let mut per_block: Option<usize> = None;
    let mut limit = MAX_BLOCKS_PER_REQUEST;
    let mut start = from;
    while start <= to {
        let end = to.min(start.saturating_add(MAX_HASHES_PER_REQUEST - 1));
        let Some(hashes) = block_hashes(q, start, end)? else {
            return Ok(Scan::Moved);
        };
        let mut i = 0;
        while i < hashes.len() {
            let want = per_block.map_or(1, |size| (budget / size).clamp(1, limit));
            let chunk = &hashes[i..hashes.len().min(i + want)];
            let fetched = match fetch_blocks(q, chunk) {
                Ok(fetched) => fetched,
                Err(e) if is_body_too_large(&e) && chunk.len() > 1 => {
                    limit = chunk.len() / 2;
                    continue;
                }
                Err(e) => return Err(e),
            };
            for Fetched {
                hash,
                block,
                hex_len,
            } in fetched
            {
                if let Some(hex_len) = hex_len {
                    let size = hex_len + REPLY_OVERHEAD;
                    per_block = Some(per_block.map_or(size, |seen| seen.max(size)));
                }
                if let Some(tx) = block.txdata.iter().find(|tx| spends(tx, outpoint)) {
                    return Ok(Scan::Found(hash, Arc::new(tx.clone())));
                }
            }
            i += chunk.len();
        }
        start = end + 1;
    }
    Ok(Scan::NotFound)
}

/// The best chain's block hashes at heights `from..=to`; `None` if the chain no longer
/// reaches `to`.
fn block_hashes(q: &Query<'_>, from: u64, to: u64) -> Result<Option<Vec<BlockHash>>, ChainError> {
    let calls: Vec<(&str, Value)> = (from..=to)
        .map(|height| ("getblockhash", json!([height])))
        .collect();
    let mut hashes = Vec::with_capacity(calls.len());
    for reply in q.batch(&calls)? {
        match reply {
            Ok(hash) => hashes.push(parse_str(&hash, "getblockhash")?),
            Err(ChainError::Server { code: -8, .. }) => return Ok(None),
            Err(e) => return Err(e),
        }
    }
    Ok(Some(hashes))
}

/// The blocks `hashes`, in order, from the cache or one batched request, with the hex
/// length of each fetched one.
fn fetch_blocks(q: &Query<'_>, hashes: &[BlockHash]) -> Result<Vec<Fetched>, ChainError> {
    let core = q.core();
    let cached: Vec<Option<Arc<Block>>> = hashes.iter().map(|h| core.cached_block(h)).collect();
    let missing: Vec<BlockHash> = hashes
        .iter()
        .zip(&cached)
        .filter(|(_, block)| block.is_none())
        .map(|(hash, _)| *hash)
        .collect();
    let calls: Vec<(&str, Value)> = missing
        .iter()
        .map(|hash| ("getblock", json!([hash, 0])))
        .collect();
    let mut jobs: Vec<Job<Arc<Block>>> = Vec::with_capacity(missing.len());
    let mut hex_lens = Vec::with_capacity(missing.len());
    for (hash, reply) in missing.iter().zip(q.batch(&calls)?) {
        let hex = match reply {
            Ok(Value::String(hex)) => hex,
            Ok(_) => return Err(ChainError::Protocol("getblock: not a hex string".into())),
            Err(e) if is_pruned(&e) => return Err(ChainError::Unsupported("block: pruned")),
            Err(e) => return Err(e),
        };
        hex_lens.push(hex.len());
        jobs.push(decode_block_job(*hash, hex));
    }
    let mut decoded = q.on_pool(jobs)?.into_iter().zip(hex_lens);
    let mut blocks = Vec::with_capacity(hashes.len());
    for (hash, cached) in hashes.iter().zip(cached) {
        match cached {
            Some(block) => blocks.push(Fetched {
                hash: *hash,
                block,
                hex_len: None,
            }),
            None => {
                let (block, hex_len) = decoded
                    .next()
                    .ok_or_else(|| ChainError::Protocol("compute job lost".into()))?;
                core.cache_block(*hash, Arc::clone(&block));
                blocks.push(Fetched {
                    hash: *hash,
                    block,
                    hex_len: Some(hex_len),
                });
            }
        }
    }
    Ok(blocks)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::hashes::Hash;
    use bitcoin::Network;

    fn block_of(size: usize, seed: u8) -> Arc<Block> {
        let mut block = bitcoin::constants::genesis_block(Network::Regtest);
        block.header.nonce = u32::from(seed);
        block.txdata[0].output[0].script_pubkey = bitcoin::ScriptBuf::from(vec![0x51; size]);
        Arc::new(block)
    }

    #[test]
    fn the_cache_drops_the_oldest_blocks_first() {
        let mut cache = BlockCache::default();
        let blocks: Vec<Arc<Block>> = (0..3).map(|i| block_of(CACHE_BYTES / 3, i)).collect();
        for block in &blocks {
            cache.insert(block.block_hash(), Arc::clone(block));
        }
        assert_eq!(cache.len(), 2);
        assert!(cache.get(&blocks[0].block_hash()).is_none());
        assert!(cache.get(&blocks[2].block_hash()).is_some());
        assert!(cache.bytes <= CACHE_BYTES);

        let huge = block_of(CACHE_BYTES + 1, 9);
        cache.insert(huge.block_hash(), huge.clone());
        assert!(cache.get(&huge.block_hash()).is_none());
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn the_cache_holds_a_bounded_number_of_blocks() {
        let mut cache = BlockCache::default();
        let block = block_of(1, 0);
        for i in 0..CACHE_BLOCKS + 10 {
            let mut hash = [0u8; 32];
            hash[..8].copy_from_slice(&(i as u64).to_le_bytes());
            cache.insert(BlockHash::from_byte_array(hash), Arc::clone(&block));
        }
        assert_eq!(cache.len(), CACHE_BLOCKS);
        cache.insert(BlockHash::from_byte_array([0; 32]), block_of(2, 1));
        assert_eq!(cache.len(), CACHE_BLOCKS);
    }
}
