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
- Events arrive on a bounded channel. If the consumer falls behind and it fills, later
  events are dropped and a single `ChainEvent::Resync` is queued in their place; nothing
  more is queued until the consumer has read it. After a `Resync`, rebuild state from
  queries.
- `ChainClient::shutdown` fails every pending call with `ChainError::Shutdown`, closes the
  connection, joins the loop thread and disconnects the events channel. Dropping the last
  `ChainClient` does the same.

## Bitcoin Core

`ChainConfig::Core` talks to bitcoind's JSON-RPC server over HTTP/1.1.

- Calls share a pool of keep-alive connections, at most `CoreConfig::rpc_connections`
  (default 4; match it to `-rpcthreads`). Each connection carries one request at a time. A
  503 (work queue full) is resent after a short backoff until the call's deadline,
  without using up its `retries`, since the node never ran it.
- `call_raw` goes to `/` and `wallet_call_raw` to `/wallet/<name>`. Error objects become
  `ChainError::Server` with their code, whatever the HTTP status: raw calls do not read
  `-5` as `NotFound`.
- With cookie auth the cookie is read when first needed and again on a 401, so a node
  restart needs no reconfiguration. A 401 that a fresh cookie does not fix is
  `ChainError::Auth`.
- `importdescriptors` and `rescanblockchain` take a long-call lane: one connection of their
  own, no `request_timeout` (only the handle's deadline), and never resent, not even with
  `retries`.
- A call that ends while its request is in flight (deadline, cancel) closes that
  connection; bitcoind still finishes the request.

## Transports

`TransportConfig` says how a backend is reached; `CoreConfig::transport` sets it for
Bitcoin Core.

- Without a proxy, a backend given by hostname is resolved on the loop's compute pool,
  never on the loop thread. Successive reconnects walk the resolved addresses. Core
  resolves its first hop (the node, or the proxy) once, when the client starts.
- With `socks5`, the hostname goes to the proxy as a SOCKS5 domain name (ATYP 3) and is
  never resolved locally; `.onion` names work as given. The proxy itself is `host:port`
  on any host, such as a Tor container at `10.21.22.10:9050`.
- `isolate: true` sends a fresh random username and password on every connection, so
  Tor builds a separate circuit for each one.
- A handshake that fails after the peer was reached (the proxy refusing the request,
  with each SOCKS5 and Tor reply code mapped to its own `Socks5Error` text) fails the
  queued calls that have no retries left with `ChainError::Transport("handshake failed:
  ...")`. Reconnecting goes on as usual. A peer that closes or resets the socket
  mid-handshake is a dropped connection instead: queued calls wait for the reconnect.
- A handshake lasts as long as the calls waiting on it: when the last one reaches its
  deadline (`ChainError::Timeout`), the half-open socket is closed. With no call waiting,
  a handshake is given up after 120 s through a proxy, 30 s for direct TLS.
- With `tls` (the `tls` feature), the connection runs rustls with the ring provider,
  over the proxy if one is set. `TlsVerify` picks WebPKI roots, a CA file, or a SHA-256
  pin of the server's leaf certificate; `InsecureForTests` exists only with `testing`.
  The name check is skipped for `.onion` hosts; the chain or pin is still checked.

## Examples

Each example is a short program in [`examples/`](examples); its module doc gives the run
command. They reach Bitcoin Core through `BITCOIN_RPC_URL` (default
`http://127.0.0.1:18443`) and `BITCOIN_RPC_AUTH=user:password` or `BITCOIN_RPC_COOKIE`
(default `~/.bitcoin/regtest/.cookie`).

| Example | Shows |
| --- | --- |
| `core_queries` | `node_requirements`, the tip and a header, `estimate_fee`, `tx_status`, and reading `ChainError::is_absence` |
| `watch_wallet` | `core_wallet`: `prepare`, `import_descriptors`, a rescan with `scanning_status` progress, `list_unspent`, `list_transactions` |
| `spend_watch` | polling `spending_tx` on a mill-io timer, with a give-up timer on the handle's cancel flag; only `Ok(None)` means "not spent" |
| `shared_handle` | one client shared by many tasks on a mill-io compute pool, each with its own deadline, retries or cancel flag; `stats()` |
| `core_over_tor` | an onion RPC endpoint through Tor's SOCKS port with `isolate`, optionally over TLS with a pinned certificate or a CA file (`--features tls`) |

A regtest node that serves all of them, `node_requirements` included:

```text
bitcoind -regtest -txindex=1 -fallbackfee=0.0001 \
    -zmqpubsequence=tcp://127.0.0.1:28332 -zmqpubrawtx=tcp://127.0.0.1:28333
```

RPC auth is the node's cookie by default; with `-rpcuser`/`-rpcpassword` or `-rpcauth`, set
`BITCOIN_RPC_AUTH`. `-fallbackfee` only lets the node's own wallet fund the examples'
addresses on regtest, where there are no fee estimates.
