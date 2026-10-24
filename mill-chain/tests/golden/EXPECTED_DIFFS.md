# Expected differences from bitcoincore-rpc-json

`core_parity.rs::parse_parity_with_bitcoincore_rpc_json` feeds each capture in this directory
to mill-chain's typed queries and to `bitcoincore-rpc-json` 0.19, and compares the results.
Every difference it finds must have an entry here, and every entry must be found; the test
reads the `## D<n>:` headings.

The captures come from `examples/capture_core_golden.rs`. Entries marked `"synthetic": true`
are replies a regtest node cannot give (a fee estimate, a rescan in progress), written by
hand in the same shape.

## D1: importdescriptors rejects a descriptor

- **bitcoincore-rpc-json:** `Vec<ImportMultiResult>` with `success: false` on the rejected
  item; the call itself succeeds.
- **mill-chain:** `CoreWallet::import_descriptors` returns `ChainError::Server` naming every
  rejected descriptor.
- **C0 link:** bug 2, `import_descriptors` discarding the per-descriptor results. openswap
  upstream checks them since #1150; mill-chain never hands back an unchecked result.

## D2: ZMQ endpoints bound to every interface

- **bitcoincore-rpc-json:** `GetZmqNotificationsResult.address` as Core prints it, such as
  `tcp://0.0.0.0:28332`, which no client can connect to.
- **mill-chain:** `NodeInfo.zmq` maps the topic to `tcp://<rpc_url host>:28332`; specific
  addresses are kept as they are.
- **C0 link:** bug 5, ZMQ topic addresses never compared with `--zmq`, which breaks nodes
  that publish each topic on its own port (Umbrel).

## D3: rebroadcast of a mined transaction

- **bitcoincore-rpc-json / bitcoincore-rpc:** `sendrawtransaction` answering `-27`
  ("Transaction outputs already in utxo set" on 28.x, "Transaction already in block chain"
  before) is an error.
- **mill-chain:** `ChainHandle::broadcast` returns `Ok(txid)`, as for a transaction already
  in the mempool.
- **C0 link:** none. This is the "already known counts as success" rule of README §2.2,
  which openswap's maker applies itself after `send_raw_transaction`
  (`is_already_broadcast_error` in `src/maker/handlers.rs`).

## Not differences

These read the same raw replies the same way once fields are converted, so they have no
entry above.

- **`getblockchaininfo.chain`:** `bitcoincore-rpc-json` maps Core's names with
  `Network::from_core_arg`, as `ChainHandle::network` does. openswap's own `chain_name()`
  prints the `Network` back with `Display` (`bitcoin`, `testnet`), which is C0 bug 1; that
  string is not part of either parse.
- **`estimatesmartfee` without an estimate:** `fee_rate: None` there, `ChainError::NotFound`
  here. With an estimate, BTC/kvB becomes sat/vB the way openswap converts it.
- **`getwalletinfo.scanning`:** `ScanningDetails` keeps `progress` as `f32` and `duration` as
  seconds; `ScanProgress` has an `f64` and a `Duration`, compared after narrowing.
- **`-5` from `getrawtransaction`:** an error reply, so outside both parsers. mill-chain
  reads it as `NotFound` only when Core says the txindex is complete; openswap reads every
  `-5` as unknown.
