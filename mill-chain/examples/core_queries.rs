//! Typed chain queries against Bitcoin Core, and how to read their errors.
//!
//! ```text
//! cargo run -p mill-chain --example core_queries [txid]
//! ```
//!
//! Checks the node's requirements, prints the tip and its header, fee estimates for a few
//! targets, and where `txid` stands. Only `ChainError::NotFound` (`is_absence()`) means
//! the node answered "it does not exist"; any other error means the answer is unknown.
//! The node is set by `BITCOIN_RPC_URL` and `BITCOIN_RPC_AUTH` or `BITCOIN_RPC_COOKIE`.

mod support;

use mill_chain::bitcoin::Txid;
use mill_chain::{ChainConfig, ChainError, MillChain, TxStatus};
use std::error::Error;
use std::process::ExitCode;
use std::time::Duration;

fn main() -> Result<ExitCode, Box<dyn Error>> {
    let txid: Option<Txid> = std::env::args().nth(1).map(|t| t.parse()).transpose()?;
    let client = MillChain::spawn(ChainConfig::Core(support::core_config(None)))?;
    let chain = client.handle().deadline(Duration::from_secs(10));

    match chain.node_requirements() {
        Ok(node) => {
            println!("bitcoind {}, txindex {}", node.version, node.txindex);
            for (topic, endpoint) in &node.zmq {
                println!("  zmq {:<9} {}", topic, endpoint);
            }
        }
        Err(ChainError::Unsupported(problem)) => println!("node not usable yet: {}", problem),
        Err(e) => return Err(e.into()),
    }

    let tip = chain.tip()?;
    let header = chain.header(tip.height)?;
    println!("tip {} at height {}", tip.hash, tip.height);
    println!("  time {}, previous {}", header.time, header.prev_blockhash);
    println!(
        "{} block above the tip: {:?}",
        tip.height + 1,
        chain.block_hash(tip.height + 1)
    );

    for target in [2, 6, 144] {
        match chain.estimate_fee(target) {
            Ok(rate) => println!("fee for {} blocks: {:.2} sat/vB", target, rate.sat_per_vb()),
            Err(e) if e.is_absence() => println!("fee for {} blocks: no estimate yet", target),
            Err(e) => return Err(e.into()),
        }
    }

    let Some(txid) = txid else {
        return Ok(ExitCode::SUCCESS);
    };
    match chain.tx_status(&txid) {
        Ok(TxStatus::Confirmed { height, block_hash }) => {
            println!("{} confirmed at {} in {}", txid, height, block_hash);
        }
        Ok(status) => println!("{}: {:?}", txid, status),
        Err(e) if e.is_absence() => {
            println!(
                "{} is in neither the mempool nor the chain: safe to rebroadcast",
                txid
            );
        }
        Err(e) => {
            println!("{}: unknown ({}); do not act as if it were absent", txid, e);
            return Ok(ExitCode::FAILURE);
        }
    }
    Ok(ExitCode::SUCCESS)
}
