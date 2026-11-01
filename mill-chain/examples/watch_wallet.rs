//! A watch-only wallet on Bitcoin Core: bootstrap, import, rescan, and the wallet's view.
//!
//! ```text
//! cargo run -p mill-chain --example watch_wallet <wallet> <descriptor#checksum> [rescan-from]
//! ```
//!
//! Loads or creates `wallet` as a descriptor wallet without private keys, imports the
//! descriptor (indexes 0 to 99 if it is ranged, unless that range is already imported),
//! rescans from height `rescan-from` (default 0), then lists the wallet's unspent outputs
//! and history.
//!
//! The rescan, which can take hours on mainnet, runs as a task on the program's own mill-io
//! compute pool, with a handle whose deadline allows for it. A timer reads its progress
//! every half second, handing each read to the pool because a blocking call on the loop
//! thread would stall it, and arms the next read only once the last one answered. The node
//! is set by `BITCOIN_RPC_URL` and `BITCOIN_RPC_AUTH` or `BITCOIN_RPC_COOKIE`.

mod support;

use mill_chain::{Chain, ChainConfig, CoreWallet};
use serde_json::json;
use std::error::Error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

const LAST_INDEX: u32 = 99;
const PROGRESS_INTERVAL: Duration = Duration::from_millis(500);

struct Progress {
    wallet: CoreWallet,
    rescanned: AtomicBool,
}

fn report_later(progress: Arc<Progress>) {
    mill_io::after(PROGRESS_INTERVAL, move || {
        mill_io::spawn(move || report(progress));
    });
}

fn report(progress: Arc<Progress>) {
    if progress.rescanned.load(Ordering::SeqCst) {
        return;
    }
    match progress.wallet.scanning_status() {
        Ok(Some(scan)) => println!(
            "rescan {:.0}% after {:?}",
            scan.progress * 100.0,
            scan.duration
        ),
        Ok(None) => {}
        Err(e) => println!("rescan progress unknown: {}", e),
    }
    report_later(progress);
}

#[mill_io::main(low_latency, events = 64, poll_timeout_ms = 100)]
fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let usage = "usage: watch_wallet <wallet> <descriptor#checksum> [rescan-from]";
    let name = args.next().ok_or(usage)?;
    let descriptor = args.next().ok_or(usage)?;
    let rescan_from: u64 = args.next().map_or(Ok(0), |h| h.parse())?;

    let client = Chain::spawn(ChainConfig::Core(support::core_config(Some(&name))))?;
    let wallet = client.handle().core_wallet()?;
    wallet.prepare(&name)?;

    let ranged = descriptor.contains('*');
    if ranged && wallet.imported_range_covers(&descriptor, LAST_INDEX)? {
        println!("already imported up to index {}", LAST_INDEX);
    } else {
        let mut request = json!({"desc": descriptor, "timestamp": "now"});
        if ranged {
            request["range"] = json!([0, LAST_INDEX]);
        }
        wallet.import_descriptors(&[request])?;
        println!("imported {}", descriptor);
    }

    let long_calls = client.handle().deadline(Duration::from_secs(6 * 3600));
    let rescan = mill_io::spawn(move || {
        long_calls
            .core_wallet()
            .and_then(|wallet| wallet.rescan_from(rescan_from))
    });
    let progress = Arc::new(Progress {
        wallet: wallet.clone(),
        rescanned: AtomicBool::new(false),
    });
    report_later(Arc::clone(&progress));

    let result = rescan.join()?;
    progress.rescanned.store(true, Ordering::SeqCst);
    result?;
    println!("rescanned from height {}", rescan_from);

    for utxo in wallet.list_unspent(0, None)? {
        println!(
            "unspent {} {} ({} conf){}",
            utxo.outpoint,
            utxo.value,
            utxo.confirmations,
            if utxo.solvable { "" } else { ", not solvable" }
        );
    }
    for tx in wallet.list_transactions(20, 0)? {
        println!(
            "{:?} {}:{} {} ({} conf)",
            tx.category, tx.txid, tx.vout, tx.amount, tx.confirmations
        );
    }
    Ok(())
}
