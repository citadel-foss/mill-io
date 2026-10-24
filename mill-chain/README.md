# mill-chain

Bitcoin chain backends (Bitcoin Core, Electrum) on the
[mill-io](../mill-io) event loop, with a blocking facade for programs built on
`std::thread`. It uses its own types over `bitcoin` 0.32, which it re-exports, and
depends on neither `tokio`, `bitcoincore-rpc` nor `electrum-client`.

## Error contract

Every query returns `Result<_, ChainError>`.

- `ChainError::NotFound` is the only error that means "the backend answered, and this
  does not exist". `ChainError::is_absence()` is true for it and nothing else.
- Every other variant (`Transport`, `Timeout`, `Cancelled`, `Shutdown`, `Auth`,
  `Server`, `Protocol`, `Unsupported`, `WouldDeadlock`) means the answer is unknown.
  Treating one of them as "absent" can lose funds: a swap that reads a timeout as
  "transaction unknown" may broadcast a stale state or abandon a coin it still owns.
- A dropped connection is always `Transport`, never `NotFound`.

## Threading rules

- `MillChain::spawn` starts one event loop thread (mill-io, direct dispatch) that owns
  the backend connection and every protocol state machine. Nothing else runs on it.
- `ChainClient` and `ChainHandle` are `Clone + Send + Sync`. Calls block the calling
  thread until a reply, the handle's deadline, its cancel flag, or shutdown. All handles
  share one connection; each carries its own deadline, retry count and cancel flag.
- Never call a blocking method on the loop thread itself: it would wait for a reply that
  thread has to read. Debug builds panic; release builds return
  `ChainError::WouldDeadlock`.
- Deadlines cover the whole call, including retries and reconnect waits.
  `Duration::ZERO` is already expired. A call whose cancel flag is already set never
  reaches the wire.
- Only transport failures (a dropped connection, a failed write) are retried, up to the
  handle's `retries`. Server errors, protocol errors and timeouts are not.
- While the connection is down, calls wait in a queue of at most
  `EngineOptions::reconnect_queue_limit`; a call that finds it full fails at once with
  `Transport("reconnect queue full")`. Reconnects back off exponentially from
  `backoff_initial` to `backoff_max`, with ±20% jitter.
- `ChainClient::shutdown` fails every pending call with `ChainError::Shutdown`, closes the
  connection, joins the loop thread and disconnects the events channel. Dropping the last
  `ChainClient` does the same.
