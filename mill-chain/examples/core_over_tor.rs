//! Bitcoin Core's RPC as a Tor onion service, through Tor's SOCKS port, optionally over TLS.
//!
//! ```text
//! BITCOIN_RPC_URL=http://<address>.onion:8332 BITCOIN_RPC_AUTH=user:pass \
//!     cargo run -p mill-chain --features tls --example core_over_tor
//! ```
//!
//! The onion name goes to the proxy unresolved, and `isolate` puts every connection on a
//! circuit of its own. `TOR_SOCKS` sets the proxy (default `127.0.0.1:9050`). For an RPC
//! endpoint behind a TLS terminator, `RPC_TLS_PIN` (the SHA-256 of its certificate, in
//! hex) or `RPC_TLS_CA` (a PEM file) turns TLS on; the name check is skipped for `.onion`
//! hosts, the pin or chain is not. Reaching an onion service can take tens of seconds, so
//! both `CoreConfig::request_timeout` (which caps every ordinary call) and the handle's
//! deadline are long.

mod support;

use mill_chain::bitcoin::hex::FromHex;
use mill_chain::{Chain, ChainConfig, Socks5Config, TlsConfig, TlsVerify, TransportConfig};
use std::env;
use std::error::Error;
use std::time::{Duration, Instant};

const ONION_PATIENCE: Duration = Duration::from_secs(180);

fn tls_from_env() -> Result<Option<TlsConfig>, Box<dyn Error>> {
    let verify = match (env::var("RPC_TLS_PIN"), env::var("RPC_TLS_CA")) {
        (Ok(pin), _) => TlsVerify::PinSha256(<[u8; 32]>::from_hex(&pin)?),
        (_, Ok(ca)) => TlsVerify::CaFile(ca.into()),
        _ => return Ok(None),
    };
    Ok(Some(TlsConfig {
        server_name: None,
        verify,
    }))
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut config = support::core_config(None);
    config.request_timeout = ONION_PATIENCE;
    config.transport = TransportConfig {
        socks5: Some(Socks5Config {
            proxy: env::var("TOR_SOCKS").unwrap_or_else(|_| "127.0.0.1:9050".into()),
            isolate: true,
        }),
        tls: tls_from_env()?,
    };
    println!(
        "{} through {:?}, TLS {}",
        config.rpc_url,
        config.transport.socks5,
        config.transport.tls.is_some()
    );

    let client = Chain::spawn(ChainConfig::Core(config))?;
    let chain = client.handle().deadline(ONION_PATIENCE);
    let started = Instant::now();
    let tip = chain.tip()?;
    println!(
        "tip {} at {} after {:?}",
        tip.hash,
        tip.height,
        started.elapsed()
    );
    let started = Instant::now();
    println!(
        "network {:?} after {:?}",
        chain.network()?,
        started.elapsed()
    );
    println!("{:?}", client.stats());
    Ok(())
}
