//! Bitcoin chain backends on the mill-io event loop.
//!
//! mill-chain talks to Bitcoin Core or an Electrum server from one event loop thread it
//! owns, and offers blocking calls that any number of `std::thread`s can make.
//!
//! # Errors and fund safety
//!
//! Every query returns [`ChainError`]. Only [`ChainError::NotFound`] means "this does
//! not exist"; any other error means the answer is unknown. Check
//! [`ChainError::is_absence`] before acting on absence.

#![deny(missing_docs)]

pub use bitcoin;

mod backend;
mod client;
#[cfg_attr(not(feature = "testing"), allow(dead_code))]
mod engine;
mod error;
#[cfg_attr(not(feature = "testing"), allow(dead_code))]
mod layer;
#[cfg_attr(not(feature = "testing"), allow(dead_code))]
mod proto;
#[cfg(feature = "testing")]
pub mod testing;
mod types;

pub use backend::core::{AddressInfo, CoreWallet, ScanProgress, TxCategory, WalletTx, WalletUtxo};
pub use backend::core::{CoreAuth, CoreConfig, NodeInfo, ZmqConfig};
pub use client::{Chain, ChainClient, ChainConfig, ChainHandle};
pub use engine::{ClientStats, EngineOptions};
pub use error::ChainError;
pub use layer::{Socks5Config, Socks5Error, TlsConfig, TlsVerify, TransportConfig};
pub use types::{BackendHealth, ChainEvent, FeeRate, HistoryItem, Tip, TxOutInfo, TxStatus, Utxo};
