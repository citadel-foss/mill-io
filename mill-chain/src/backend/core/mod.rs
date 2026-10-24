//! Bitcoin Core over its JSON-RPC HTTP server.

mod rpc;

pub(crate) use rpc::CoreRpc;

use std::path::PathBuf;
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
    /// a 30 s request timeout and a 32 MiB body cap.
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
