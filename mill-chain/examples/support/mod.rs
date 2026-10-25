//! How the examples reach Bitcoin Core, from the environment:
//!
//! - `BITCOIN_RPC_URL`: `http://host:port`, default `http://127.0.0.1:18443` (regtest);
//! - `BITCOIN_RPC_AUTH`: `user:password`, or else
//! - `BITCOIN_RPC_COOKIE`: the node's `.cookie` file, default `~/.bitcoin/regtest/.cookie`.

use mill_chain::{CoreAuth, CoreConfig};
use std::env;
use std::path::PathBuf;

/// A [`CoreConfig`] for the node the environment names, using `wallet` for wallet calls.
pub fn core_config(wallet: Option<&str>) -> CoreConfig {
    let url = env::var("BITCOIN_RPC_URL").unwrap_or_else(|_| "http://127.0.0.1:18443".into());
    let auth = match env::var("BITCOIN_RPC_AUTH") {
        Ok(user_pass) => {
            let (user, pass) = user_pass.split_once(':').unwrap_or((&user_pass, ""));
            CoreAuth::UserPass {
                user: user.to_string(),
                pass: pass.to_string(),
            }
        }
        Err(_) => CoreAuth::Cookie(cookie_path()),
    };
    let mut config = CoreConfig::new(url, auth);
    config.wallet = wallet.map(str::to_string);
    config
}

fn cookie_path() -> PathBuf {
    if let Ok(path) = env::var("BITCOIN_RPC_COOKIE") {
        return PathBuf::from(path);
    }
    let home = env::var("HOME").unwrap_or_default();
    PathBuf::from(home).join(".bitcoin/regtest/.cookie")
}
