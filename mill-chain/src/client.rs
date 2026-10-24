//! The blocking facade: [`MillChain`] starts a client, [`ChainClient`] owns it and
//! [`ChainHandle`] makes calls.

use crate::backend::core::{CoreConfig, CoreRpc};
use crate::engine::{Call, ClientStats, EngineOptions, EventSink, Route, Runtime};
#[cfg(feature = "testing")]
use crate::engine::{Endpoint, Engine};
#[cfg(feature = "testing")]
use crate::layer::{Dialer, TransportConfig};
#[cfg(feature = "testing")]
use crate::proto::line::LineJsonRpc;
use crate::{ChainError, ChainEvent};
use crossbeam_channel::{Receiver, RecvTimeoutError};
use mill_io::EventLoop;
use serde_json::Value;
use std::fmt;
#[cfg(feature = "testing")]
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const EVENTS_PER_POLL: usize = 256;
const POLL_TIMEOUT_MS: u64 = 100;
/// How often a caller with a cancel flag checks it.
const CANCEL_POLL: Duration = Duration::from_millis(5);
/// How long a caller waits past its deadline, or after cancelling, for the loop to answer
/// before giving up on it.
const LOOP_GRACE: Duration = Duration::from_secs(1);

/// Which backend to talk to, and how.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum ChainConfig {
    /// Line-delimited JSON-RPC over plain TCP, as [`MockServer`](crate::testing::MockServer)
    /// speaks it.
    #[cfg(feature = "testing")]
    RawLineJsonRpc {
        /// The server's address.
        addr: SocketAddr,
        /// Events held for the consumer before they are replaced by one `Resync`.
        events_capacity: usize,
    },
    /// Line-delimited JSON-RPC to `host:port` through `transport`. The host may be a
    /// hostname, which goes unresolved to a SOCKS5 proxy if one is set.
    #[cfg(feature = "testing")]
    RawLineJsonRpcVia {
        /// The server's hostname or IP address.
        host: String,
        /// The server's port.
        port: u16,
        /// How to reach it.
        transport: TransportConfig,
        /// Events held for the consumer before they are replaced by one `Resync`.
        events_capacity: usize,
    },
    /// Bitcoin Core's JSON-RPC server over HTTP/1.1.
    Core(CoreConfig),
}

impl ChainConfig {
    fn events_capacity(&self) -> usize {
        match self {
            #[cfg(feature = "testing")]
            ChainConfig::RawLineJsonRpc {
                events_capacity, ..
            } => *events_capacity,
            #[cfg(feature = "testing")]
            ChainConfig::RawLineJsonRpcVia {
                events_capacity, ..
            } => *events_capacity,
            ChainConfig::Core(_) => CoreConfig::EVENTS_CAPACITY,
        }
    }
}

fn runtime(
    config: ChainConfig,
    options: EngineOptions,
    event_loop: Arc<EventLoop>,
    events: EventSink,
) -> Result<Arc<dyn Runtime>, ChainError> {
    match config {
        #[cfg(feature = "testing")]
        ChainConfig::RawLineJsonRpc { addr, .. } => {
            let endpoint = Endpoint {
                dialer: Dialer::direct(addr),
                codec: Box::new(LineJsonRpc::new()),
            };
            Ok(Engine::new(endpoint, options, event_loop, events))
        }
        #[cfg(feature = "testing")]
        ChainConfig::RawLineJsonRpcVia {
            host,
            port,
            transport,
            ..
        } => {
            let endpoint = Endpoint {
                dialer: Dialer::new(&host, port, &transport)?,
                codec: Box::new(LineJsonRpc::new()),
            };
            Ok(Engine::new(endpoint, options, event_loop, events))
        }
        ChainConfig::Core(core) => Ok(CoreRpc::new(core, options, event_loop, events)?),
    }
}

/// Starts chain clients.
#[derive(Debug)]
pub struct MillChain;

impl MillChain {
    /// Starts a client with [`EngineOptions::default`]; see [`spawn_with`](Self::spawn_with).
    pub fn spawn(config: ChainConfig) -> Result<ChainClient, ChainError> {
        Self::spawn_with(config, EngineOptions::default())
    }

    /// Starts a client on its own event loop thread and begins connecting.
    ///
    /// Returns once the thread runs, without waiting for the connection: an unreachable
    /// backend is retried in the background, and calls queue until it answers. A
    /// connection whose handshake fails (a SOCKS5 proxy refusing the request) fails the
    /// queued calls that have no retries left with [`ChainError::Transport`].
    ///
    /// Fails at once if the config is malformed, such as a proxy that is not `host:port`.
    pub fn spawn_with(
        config: ChainConfig,
        options: EngineOptions,
    ) -> Result<ChainClient, ChainError> {
        let event_loop = EventLoop::new_low_latency(EVENTS_PER_POLL, POLL_TIMEOUT_MS)
            .map(Arc::new)
            .map_err(|e| ChainError::Transport(format!("cannot create the event loop: {}", e)))?;
        let (events_tx, events_rx) = EventSink::channel(config.events_capacity());
        let engine = runtime(config, options, Arc::clone(&event_loop), events_tx)?;

        let loop_engine = Arc::clone(&engine);
        let spawned = thread::Builder::new()
            .name("mill-chain".to_string())
            .spawn(move || {
                loop_engine.set_loop_thread();
                loop_engine.start();
                if let Err(e) = event_loop.run() {
                    log::error!("mill-chain event loop failed: {}", e);
                }
                loop_engine.close_all();
            });
        let thread = match spawned {
            Ok(thread) => thread,
            Err(e) => {
                engine.close_all();
                engine.release_loop();
                return Err(ChainError::Transport(format!(
                    "cannot start the event loop thread: {}",
                    e
                )));
            }
        };

        Ok(ChainClient {
            inner: Arc::new(Inner {
                engine,
                thread: Mutex::new(Some(thread)),
                events: events_rx,
            }),
        })
    }
}

/// A running chain client. Clones share the same loop thread and connection.
///
/// The loop stops on [`shutdown`](Self::shutdown) or when the last clone is dropped;
/// handles outliving it get [`ChainError::Shutdown`].
#[derive(Clone)]
pub struct ChainClient {
    inner: Arc<Inner>,
}

struct Inner {
    engine: Arc<dyn Runtime>,
    thread: Mutex<Option<JoinHandle<()>>>,
    events: Receiver<ChainEvent>,
}

impl ChainClient {
    /// A handle with the default deadline and retries, for making calls.
    pub fn handle(&self) -> ChainHandle {
        let options = self.inner.engine.options();
        ChainHandle {
            engine: Arc::clone(&self.inner.engine),
            deadline: options.default_deadline,
            retries: options.default_retries,
            cancel: None,
        }
    }

    /// The events channel. Every clone of the receiver reads from the same queue; it
    /// disconnects after shutdown.
    pub fn events(&self) -> Receiver<ChainEvent> {
        self.inner.events.clone()
    }

    /// Current counters.
    pub fn stats(&self) -> ClientStats {
        self.inner.engine.stats()
    }

    /// Stops the loop, fails every pending call with [`ChainError::Shutdown`], closes the
    /// connection and joins the loop thread. Other clones see the client as shut down.
    ///
    /// Fails with [`ChainError::WouldDeadlock`] on the loop thread itself.
    pub fn shutdown(self) -> Result<(), ChainError> {
        self.inner.shutdown()
    }

    #[cfg(feature = "testing")]
    pub(crate) fn engine(&self) -> &Arc<dyn Runtime> {
        &self.inner.engine
    }
}

impl Inner {
    fn shutdown(&self) -> Result<(), ChainError> {
        if self.engine.is_loop_thread() {
            return Err(ChainError::WouldDeadlock);
        }
        self.engine.stop();
        let thread = self
            .thread
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(thread) = thread {
            if thread.join().is_err() {
                log::error!("mill-chain loop thread panicked");
            }
        }
        // Dropped here, after the join, so its worker threads are gone on return.
        drop(self.engine.release_loop());
        Ok(())
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if self.engine.is_loop_thread() {
            self.engine.stop();
            self.engine.release_loop();
        } else {
            let _ = self.shutdown();
        }
    }
}

impl fmt::Debug for ChainClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChainClient")
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

/// Makes blocking calls on a client. Cheap to clone; every handle shares the client's
/// connection, and each carries its own deadline, retries and cancel flag.
#[derive(Clone)]
pub struct ChainHandle {
    engine: Arc<dyn Runtime>,
    deadline: Duration,
    retries: u8,
    cancel: Option<Arc<AtomicBool>>,
}

impl ChainHandle {
    /// Each call fails with [`ChainError::Timeout`] after `d`, retries and reconnect waits
    /// included. `Duration::ZERO` is already expired.
    pub fn deadline(mut self, d: Duration) -> Self {
        self.deadline = d;
        self
    }

    /// Retry a call up to `n` times when its connection drops or its write fails. Server,
    /// protocol and timeout errors are never retried.
    pub fn retries(mut self, n: u8) -> Self {
        self.retries = n;
        self
    }

    /// Calls fail with [`ChainError::Cancelled`] once `flag` is set. A call made while it
    /// is set never reaches the wire.
    pub fn cancel_on(mut self, flag: Arc<AtomicBool>) -> Self {
        self.cancel = Some(flag);
        self
    }

    /// Raw JSON-RPC call on JSON-RPC backends. Treated as idempotent for retries.
    ///
    /// Every JSON-RPC error object becomes [`ChainError::Server`]; codes are not
    /// interpreted. Blocks until the reply, the deadline, cancellation or shutdown.
    ///
    /// On Bitcoin Core it goes to `/`. `importdescriptors` and `rescanblockchain` take the
    /// long-call lane: their own connection, no `request_timeout`, and never resent.
    ///
    /// # Panics
    ///
    /// In debug builds, when called on the client's own loop thread, where release builds
    /// return [`ChainError::WouldDeadlock`].
    pub fn call_raw(&self, method: &str, params: Value) -> Result<Value, ChainError> {
        self.call(method, params, Route::Chain)
    }

    /// Like [`call_raw`](Self::call_raw), on Bitcoin Core's wallet endpoint
    /// `/wallet/<name>` for [`CoreConfig::wallet`].
    ///
    /// Fails with [`ChainError::Unsupported`] on other backends, or when no wallet is
    /// configured.
    pub fn wallet_call_raw(&self, method: &str, params: Value) -> Result<Value, ChainError> {
        self.call(method, params, Route::Wallet)
    }

    /// Sends `calls` as one JSON-RPC batch to Bitcoin Core's `/` and returns one result
    /// per call, in order, whatever order the server answered in. Treated as idempotent
    /// for retries; a batch holding a long-lane method takes the long-call lane.
    ///
    /// Fails with [`ChainError::Unsupported`] on other backends.
    #[cfg_attr(not(feature = "testing"), allow(dead_code))]
    pub(crate) fn batch_raw(
        &self,
        calls: &[(&str, Value)],
    ) -> Result<Vec<Result<Value, ChainError>>, ChainError> {
        if calls.is_empty() {
            return Ok(Vec::new());
        }
        let requests = calls
            .iter()
            .map(|(method, params)| serde_json::json!({"method": method, "params": params}))
            .collect();
        let replies = self.call("", Value::Array(requests), Route::ChainBatch)?;
        crate::proto::jsonrpc::split_batch_reply(replies, calls.len())
    }

    fn call(&self, method: &str, params: Value, route: Route) -> Result<Value, ChainError> {
        if self.engine.is_loop_thread() {
            if cfg!(debug_assertions) {
                panic!("mill-chain: blocking call on the event loop thread (WouldDeadlock)");
            }
            return Err(ChainError::WouldDeadlock);
        }
        if self.engine.is_closed() {
            return Err(ChainError::Shutdown);
        }
        if self.is_cancelled() {
            return Err(ChainError::Cancelled);
        }
        if self.deadline.is_zero() {
            return Err(ChainError::Timeout);
        }
        let deadline = Instant::now().checked_add(self.deadline);
        let (reply, replies) = crossbeam_channel::bounded(1);
        let id = self.engine.submit(Call {
            method: method.to_string(),
            params,
            route,
            deadline,
            retries: self.retries,
            cancel: self.cancel.clone(),
            reply,
        })?;
        self.wait(id, deadline, &replies)
    }

    fn is_cancelled(&self) -> bool {
        self.cancel
            .as_ref()
            .is_some_and(|flag| flag.load(Ordering::SeqCst))
    }

    /// The loop ends the call by its deadline; `LOOP_GRACE` past it the caller stops
    /// waiting for a loop that is stuck.
    fn wait(
        &self,
        id: u64,
        deadline: Option<Instant>,
        replies: &Receiver<Result<Value, ChainError>>,
    ) -> Result<Value, ChainError> {
        let give_up = deadline.and_then(|d| d.checked_add(LOOP_GRACE));
        loop {
            let mut wait = give_up.map(|t| t.saturating_duration_since(Instant::now()));
            if self.cancel.is_some() {
                wait = Some(wait.map_or(CANCEL_POLL, |w| w.min(CANCEL_POLL)));
            }
            let received = match wait {
                Some(wait) => replies.recv_timeout(wait),
                None => replies.recv().map_err(|_| RecvTimeoutError::Disconnected),
            };
            match received {
                Ok(result) => return result,
                Err(RecvTimeoutError::Disconnected) => return Err(ChainError::Shutdown),
                Err(RecvTimeoutError::Timeout) => {}
            }
            if self.is_cancelled() {
                self.engine.abandon(id, ChainError::Cancelled);
                return match replies.recv_timeout(LOOP_GRACE) {
                    Ok(result) => result,
                    Err(_) => Err(ChainError::Cancelled),
                };
            }
            if give_up.is_some_and(|t| Instant::now() >= t) {
                self.engine.abandon(id, ChainError::Timeout);
                return Err(ChainError::Timeout);
            }
        }
    }
}

impl fmt::Debug for ChainHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChainHandle")
            .field("deadline", &self.deadline)
            .field("retries", &self.retries)
            .field("cancellable", &self.cancel.is_some())
            .finish_non_exhaustive()
    }
}
