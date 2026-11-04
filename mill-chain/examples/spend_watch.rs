//! Waiting for an output to be spent on a mill-io event loop, without ever mistaking "could
//! not ask" for "unspent".
//!
//! ```text
//! cargo run -p mill-chain --example spend_watch <txid:vout> [confirmed|any] [give-up-secs]
//! ```
//!
//! Polls `spending_tx` every 2 s until the output is spent (in a block only, with
//! `confirmed`, the default; or in the mempool too, with `any`), or until `give-up-secs`
//! (default 600) pass, when a timer sets the handle's cancel flag. The program's own
//! `EventLoop` keeps time: a timer hands each poll to the compute pool, because a
//! blocking call on the loop thread would stall it. Each poll arms the next only once its
//! answer is in, so a slow query never overlaps the next one.
//!
//! Only `Ok(None)` means "not spent", only `NotFound` means the output's transaction does
//! not exist, and `Cancelled` means the answer is still unknown; any other error is
//! reported and asked again. The node is set by `BITCOIN_RPC_URL` and `BITCOIN_RPC_AUTH`
//! or `BITCOIN_RPC_COOKIE`.

mod support;

use mill_chain::bitcoin::{OutPoint, ScriptBuf, Txid};
use mill_chain::{Chain, ChainConfig, ChainError, ChainHandle};
use std::error::Error;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, SyncSender};
use std::sync::Arc;
use std::time::Duration;

const POLL_INTERVAL: Duration = Duration::from_secs(2);

enum Outcome {
    Spent(Txid),
    NoSuchTransaction,
    GaveUp,
}

struct Watch {
    chain: ChainHandle,
    outpoint: OutPoint,
    script: ScriptBuf,
    confirmed_only: bool,
    done: SyncSender<Outcome>,
}

fn poll_later(watch: Arc<Watch>) {
    mill_io::after(POLL_INTERVAL, move || {
        mill_io::spawn(move || poll(watch));
    });
}

fn poll(watch: Arc<Watch>) {
    let answer = watch
        .chain
        .spending_tx(&watch.outpoint, &watch.script, watch.confirmed_only);
    let outcome = match answer {
        Ok(Some(spend)) => Outcome::Spent(spend.compute_txid()),
        Err(e) if e.is_absence() => Outcome::NoSuchTransaction,
        Err(ChainError::Cancelled) => Outcome::GaveUp,
        Ok(None) => {
            println!("not spent yet");
            return poll_later(watch);
        }
        Err(e) => {
            println!("could not tell ({}); asking again", e);
            return poll_later(watch);
        }
    };
    let _ = watch.done.send(outcome);
}

#[mill_io::main(low_latency, events = 64, poll_timeout_ms = 100)]
fn main() -> Result<ExitCode, Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let outpoint: OutPoint = args
        .next()
        .ok_or("usage: spend_watch <txid:vout> [confirmed|any] [give-up-secs]")?
        .parse()?;
    let confirmed_only = args.next().as_deref() != Some("any");
    let give_up = Duration::from_secs(args.next().map_or(Ok(600), |s| s.parse())?);

    let client = Chain::spawn(ChainConfig::Core(support::core_config(None)))?;
    let stop = Arc::new(AtomicBool::new(false));
    let chain = client
        .handle()
        .deadline(Duration::from_secs(30))
        .cancel_on(Arc::clone(&stop));
    let funding = chain.transaction(&outpoint.txid)?;
    let script = funding
        .output
        .get(outpoint.vout as usize)
        .ok_or("no such output")?
        .script_pubkey
        .clone();
    println!(
        "watching {} for a {} spend",
        outpoint,
        if confirmed_only {
            "confirmed"
        } else {
            "mempool or confirmed"
        }
    );

    mill_io::after(give_up, move || stop.store(true, Ordering::SeqCst));
    // The answer comes from whichever poll ends the chain, so a channel carries it.
    let (done, outcome) = mpsc::sync_channel(1);
    let watch = Arc::new(Watch {
        chain,
        outpoint,
        script,
        confirmed_only,
        done,
    });
    mill_io::spawn(move || poll(watch));

    let outcome = outcome.recv()?;
    let code = match outcome {
        Outcome::Spent(txid) => {
            println!("spent by {}", txid);
            ExitCode::SUCCESS
        }
        Outcome::NoSuchTransaction => {
            println!("{} does not exist", outpoint.txid);
            ExitCode::FAILURE
        }
        Outcome::GaveUp => {
            println!(
                "gave up after {:?}; the output may or may not be spent",
                give_up
            );
            ExitCode::FAILURE
        }
    };
    println!("{:?}", client.stats());
    Ok(code)
}
