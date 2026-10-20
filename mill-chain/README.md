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
