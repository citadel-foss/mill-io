//! Bitcoin Core over its JSON-RPC HTTP server.

pub(crate) mod chain;
mod rpc;
pub(crate) mod spend;
mod wallet;

pub(crate) use rpc::CoreRpc;
pub use wallet::{AddressInfo, CoreWallet, ScanProgress, TxCategory, WalletTx, WalletUtxo};

use crate::layer::TransportConfig;
use crate::ChainError;
use bitcoin::{Block, BlockHash};
use chain::{as_array, as_str, as_u32, field, opt_field, Query};
use serde_json::{json, Value};
use spend::BlockCache;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

/// How to reach a Bitcoin Core node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreConfig {
    /// The RPC server, as `http://host:port`.
    pub rpc_url: String,
    /// The RPC credentials.
    pub auth: CoreAuth,
    /// The wallet [`ChainHandle::wallet_call_raw`](crate::ChainHandle::wallet_call_raw)
    /// talks to, at `/wallet/<name>`.
    pub wallet: Option<String>,
    /// Most connections open at once for ordinary calls; match it to the node's
    /// `-rpcthreads`. Default 4. The long-call lane has one more of its own.
    pub rpc_connections: usize,
    /// How long an ordinary call may take from submission to reply; the handle's
    /// deadline still applies on top. Long-lane calls have no such limit. Default 30 s.
    pub request_timeout: Duration,
    /// Largest response body accepted; a larger one fails its call with
    /// [`ChainError::Protocol`](crate::ChainError::Protocol). Default 32 MiB.
    pub max_body_bytes: usize,
    /// ZMQ notifications. Unused so far.
    pub zmq: ZmqConfig,
    /// How the RPC server is reached; the default is a direct TCP connection. Through a
    /// SOCKS5 proxy, the `rpc_url` host is resolved by the proxy.
    pub transport: TransportConfig,
}

impl CoreConfig {
    /// Events held for the consumer before they are replaced by one `Resync`.
    pub(crate) const EVENTS_CAPACITY: usize = 1024;

    /// A config for `rpc_url` and `auth` with every other field at its default.
    pub fn new(rpc_url: impl Into<String>, auth: CoreAuth) -> Self {
        Self {
            rpc_url: rpc_url.into(),
            auth,
            ..Self::default()
        }
    }
}

impl Default for CoreConfig {
    /// `http://127.0.0.1:8332` with an empty user and password, no wallet, 4 connections,
    /// a 30 s request timeout, a 32 MiB body cap and a direct connection.
    fn default() -> Self {
        Self {
            rpc_url: "http://127.0.0.1:8332".to_string(),
            auth: CoreAuth::UserPass {
                user: String::new(),
                pass: String::new(),
            },
            wallet: None,
            rpc_connections: 4,
            request_timeout: Duration::from_secs(30),
            max_body_bytes: 32 * 1024 * 1024,
            zmq: ZmqConfig::default(),
            transport: TransportConfig::default(),
        }
    }
}

/// RPC credentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoreAuth {
    /// `-rpcuser` and `-rpcpassword`, or an `-rpcauth` entry.
    UserPass {
        /// The user name.
        user: String,
        /// The password.
        pass: String,
    },
    /// The node's `.cookie` file. It is read when first needed and again whenever the
    /// node refuses it, so a node restart, which writes a new cookie, needs no
    /// reconfiguration.
    Cookie(PathBuf),
}

/// Where Bitcoin Core publishes ZMQ notifications.
#[non_exhaustive]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ZmqConfig {}

/// State the typed queries of one Core client share across its handles. The cache is
/// locked only by callers, never across a request.
#[derive(Debug)]
pub(crate) struct CoreShared {
    /// The `rpc_url` host, standing in for a ZMQ endpoint bound to every interface.
    rpc_host: String,
    max_body_bytes: usize,
    blocks: Mutex<BlockCache>,
}

impl CoreShared {
    pub(crate) fn new(rpc_host: String, max_body_bytes: usize) -> Self {
        Self {
            rpc_host,
            max_body_bytes,
            blocks: Mutex::new(BlockCache::default()),
        }
    }

    pub(crate) fn max_body_bytes(&self) -> usize {
        self.max_body_bytes
    }

    pub(crate) fn cached_block(&self, hash: &BlockHash) -> Option<Arc<Block>> {
        self.cache().get(hash)
    }

    pub(crate) fn cache_block(&self, hash: BlockHash, block: Arc<Block>) {
        self.cache().insert(hash, block);
    }

    fn cache(&self) -> MutexGuard<'_, BlockCache> {
        self.blocks.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// What the node offers, once it meets every requirement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeInfo {
    /// Bitcoin Core's version, as `getnetworkinfo` reports it: 280100 for 28.1.0.
    pub version: u32,
    /// `-txindex` is on.
    pub txindex: bool,
    /// Every ZMQ topic the node publishes (`sequence`, `rawtx`, `rawblock`, `hashblock`,
    /// `hashtx`), with its `tcp://host:port` endpoint. An endpoint bound to every interface
    /// names the `rpc_url` host instead.
    pub zmq: BTreeMap<String, String>,
}

/// The oldest Bitcoin Core supported: `gettxspendingprevout` arrived in 24.0.
const MIN_VERSION: u32 = 240_000;
/// `RPC_METHOD_NOT_FOUND`, which a bitcoind built without ZMQ answers
/// `getzmqnotifications` with.
const METHOD_NOT_FOUND: i64 = -32601;

/// Checks the node is Core 24 or later with `-txindex` and publishes the `sequence` and
/// `rawtx` topics; [`ChainError::Unsupported`] naming the first problem found otherwise.
/// The version is checked first, before asking an old node for anything else.
pub(crate) fn node_requirements(q: &Query<'_>) -> Result<NodeInfo, ChainError> {
    let network = q.chain("getnetworkinfo", json!([]))?;
    let version = as_u32(field(&network, "version", "getnetworkinfo")?, "version")?;
    if version < MIN_VERSION {
        return Err(ChainError::Unsupported(
            "Bitcoin Core version is below 24; 24 or later is required",
        ));
    }
    let indexes = q.chain("getindexinfo", json!([]))?;
    if !indexes.is_object() {
        return Err(ChainError::Protocol("getindexinfo: not an object".into()));
    }
    if opt_field(&indexes, "txindex").is_none() {
        return Err(ChainError::Unsupported(
            "txindex is off; run Bitcoin Core with -txindex=1",
        ));
    }
    let notifications = match q.chain("getzmqnotifications", json!([])) {
        Ok(notifications) => notifications,
        Err(ChainError::Server {
            code: METHOD_NOT_FOUND,
            ..
        }) => {
            return Err(ChainError::Unsupported(
                "zmq is not built into this bitcoind; topics sequence and rawtx are required",
            ))
        }
        Err(e) => return Err(e),
    };
    let zmq = zmq_endpoints(&notifications, &q.core().rpc_host)?;
    if !zmq.contains_key("sequence") {
        return Err(ChainError::Unsupported(
            "zmq topic sequence is not published; set -zmqpubsequence",
        ));
    }
    if !zmq.contains_key("rawtx") {
        return Err(ChainError::Unsupported(
            "zmq topic rawtx is not published; set -zmqpubrawtx",
        ));
    }
    Ok(NodeInfo {
        version,
        txindex: true,
        zmq,
    })
}

/// Each published topic's endpoint, keyed by the topic without its `pub` prefix. The first
/// endpoint listed wins when a topic is published twice.
fn zmq_endpoints(list: &Value, rpc_host: &str) -> Result<BTreeMap<String, String>, ChainError> {
    let mut endpoints = BTreeMap::new();
    for notification in as_array(list, "getzmqnotifications")? {
        let what = "getzmqnotifications";
        let kind = as_str(field(notification, "type", what)?, "type")?;
        let address = as_str(field(notification, "address", what)?, "address")?;
        let topic = kind.strip_prefix("pub").unwrap_or(kind);
        endpoints
            .entry(topic.to_string())
            .or_insert_with(|| reachable_endpoint(address, rpc_host));
    }
    Ok(endpoints)
}

/// `address` with a wildcard bind host replaced by `rpc_host`. Anything but `tcp://` is
/// returned as is.
fn reachable_endpoint(address: &str, rpc_host: &str) -> String {
    let wildcard = address
        .strip_prefix("tcp://")
        .and_then(|rest| rest.rsplit_once(':'))
        .filter(|(host, _)| matches!(*host, "0.0.0.0" | "*" | "::" | "[::]"));
    match wildcard {
        Some((_, port)) => format!("tcp://{}:{}", rpc_host, port),
        None => address.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wildcard_binds_name_the_rpc_host() {
        for bind in ["0.0.0.0", "*", "::", "[::]"] {
            assert_eq!(
                reachable_endpoint(&format!("tcp://{}:28332", bind), "node.local"),
                "tcp://node.local:28332",
                "{}",
                bind
            );
        }
        assert_eq!(
            reachable_endpoint("tcp://0.0.0.0:28332", "[::1]"),
            "tcp://[::1]:28332"
        );
        for kept in [
            "tcp://127.0.0.1:28332",
            "tcp://10.0.0.2:28333",
            "tcp://[::1]:28332",
            "ipc:///tmp/zmq.sock",
            "tcp://0.0.0.0",
        ] {
            assert_eq!(reachable_endpoint(kept, "node.local"), kept);
        }
    }

    #[test]
    fn topics_lose_their_pub_prefix_and_keep_the_first_endpoint() {
        let list = json!([
            {"type": "pubsequence", "address": "tcp://0.0.0.0:1", "hwm": 1000},
            {"type": "pubrawtx", "address": "tcp://127.0.0.1:2", "hwm": 1000},
            {"type": "pubrawtx", "address": "tcp://127.0.0.1:3", "hwm": 1000},
        ]);
        let endpoints = zmq_endpoints(&list, "h").unwrap();
        assert_eq!(endpoints.len(), 2);
        assert_eq!(endpoints["sequence"], "tcp://h:1");
        assert_eq!(endpoints["rawtx"], "tcp://127.0.0.1:2");
        assert!(matches!(
            zmq_endpoints(&json!([{"type": "pubrawtx"}]), "h"),
            Err(ChainError::Protocol(_))
        ));
        assert!(matches!(
            zmq_endpoints(&json!({}), "h"),
            Err(ChainError::Protocol(_))
        ));
    }
}
