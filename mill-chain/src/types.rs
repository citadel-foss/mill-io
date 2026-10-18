//! Chain facts shared by every backend.

use bitcoin::block::Header;
use bitcoin::{Amount, Block, BlockHash, OutPoint, ScriptBuf, Transaction, Txid};
use std::sync::Arc;

/// The best block a backend knows of.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Tip {
    /// Height of the block.
    pub height: u64,
    /// Hash of the block.
    pub hash: BlockHash,
}

/// Where a transaction stands, as far as the backend knows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TxStatus {
    /// The backend has not seen the transaction.
    Unknown,
    /// The transaction is in the backend's mempool.
    Mempool,
    /// The transaction is in a block on the best chain.
    Confirmed {
        /// Height of the block.
        height: u64,
        /// Hash of the block.
        block_hash: BlockHash,
    },
}

/// An unspent output paying to a watched script.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Utxo {
    /// The output.
    pub outpoint: OutPoint,
    /// Its value.
    pub value: Amount,
    /// The script it pays to.
    pub script_pubkey: ScriptBuf,
    /// Height of the confirming block; `None` while it is in the mempool.
    pub height: Option<u64>,
}

/// A transaction that touches a watched script.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HistoryItem {
    /// The transaction.
    pub txid: Txid,
    /// Height of the confirming block; `None` while it is in the mempool.
    pub height: Option<u64>,
}

/// A fee rate in satoshis per virtual byte.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct FeeRate(f64);

impl FeeRate {
    /// A fee rate of `sat_per_vb` satoshis per virtual byte.
    pub fn from_sat_per_vb(sat_per_vb: f64) -> Self {
        FeeRate(sat_per_vb)
    }

    /// The rate in satoshis per virtual byte.
    pub fn sat_per_vb(self) -> f64 {
        self.0
    }

    /// The rate as a [`bitcoin::FeeRate`], rounded up to whole satoshis per 1000 weight
    /// units; negative and NaN rates become zero.
    pub fn to_bitcoin(self) -> bitcoin::FeeRate {
        let sat_per_kwu = (self.0 * 250.0).ceil();
        bitcoin::FeeRate::from_sat_per_kwu(if sat_per_kwu > 0.0 {
            sat_per_kwu as u64
        } else {
            0
        })
    }
}

/// Whether a backend can currently answer queries and deliver events.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BackendHealth {
    /// Connected, and the event feed is live.
    Up,
    /// Answering queries, but part of it is not working, such as the event feed.
    Degraded,
    /// Not answering.
    Down,
}

/// Something that happened on the chain or to the backend.
///
/// Events are delivered on a bounded channel. When a consumer falls behind and the
/// channel fills, later events are dropped and a single [`Resync`](Self::Resync) takes
/// their place: the consumer must then rebuild its view from queries.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChainEvent {
    /// A block joined the best chain.
    BlockConnected {
        /// Its height.
        height: u64,
        /// Its hash.
        hash: BlockHash,
        /// Its header.
        header: Header,
        /// The full block, when the backend fetched it anyway.
        block: Option<Arc<Block>>,
    },
    /// A block left the best chain in a reorganisation.
    BlockDisconnected {
        /// Its height.
        height: u64,
        /// Its hash.
        hash: BlockHash,
    },
    /// A transaction touching a watched script or outpoint was seen.
    TxSeen {
        /// The transaction.
        tx: Arc<Transaction>,
        /// Where it stands.
        status: TxStatus,
    },
    /// Events were lost; state derived from earlier events must be rebuilt.
    Resync {
        /// Why events were lost.
        reason: String,
    },
    /// The backend's health changed.
    BackendState(BackendHealth),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kwu(sat_per_vb: f64) -> u64 {
        FeeRate::from_sat_per_vb(sat_per_vb)
            .to_bitcoin()
            .to_sat_per_kwu()
    }

    #[test]
    fn fee_rate_converts_rounding_up() {
        assert_eq!(kwu(1.0), 250);
        assert_eq!(kwu(1.001), 251);
        assert_eq!(kwu(-3.0), 0);
        assert_eq!(kwu(f64::NAN), 0);
        assert_eq!(FeeRate::from_sat_per_vb(2.5).sat_per_vb(), 2.5);
    }
}
