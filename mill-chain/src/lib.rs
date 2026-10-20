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

mod error;
mod types;

pub use error::ChainError;
pub use types::{BackendHealth, ChainEvent, FeeRate, HistoryItem, Tip, TxStatus, Utxo};
