# Distributed KV coordination harness

A runnable Rust KV foundation for comparing coordination protocols. It includes HTTP, static sharding and routing, durable local protocol-state records, multiplexed TCP peer RPCs, fault hooks, and correctness/smoke tests.

**Current coordinator: `local`, an unreplicated single-owner baseline.** Any configured node accepts HTTP requests and forwards each key to its sole owner. Owner failure returns 503. There is no replica failover, consensus, membership service, or CP replication protocol yet. The baseline rejects multi-owner shard configurations rather than pretending to replicate.

## Requirements and verification

- Rust 1.89 or later, Cargo, and Python 3 for the process smoke test.
- Direct Rust dependencies are limited to `tokio`, `axum`, `serde`, `serde_json`, `tracing`, and `tracing-subscriber`. `Cargo.lock` pins their transitive dependencies. There are no additional dev dependencies.

```sh
bash scripts/verify.sh
```

The script checks formatting, runs Clippy with warnings denied, runs Rust tests, builds the binary, and runs three real server processes over HTTP. The process test kills an owner with SIGKILL, restarts it, then kills and restarts the entire cluster. It verifies acknowledged values and a deletion survive. It uses temporary directories and ephemeral loopback ports and cleans up its processes.

These are validation tests, not performance measurements or a proof of a distributed protocol.

## Run one node

```sh
cargo build --locked
./target/debug/distributed-kv-harness --config configs/single.json --node 1 --data-dir data/single
```

In another terminal:

```sh
curl -i -X PUT http://127.0.0.1:8080/example -H 'Content-Type: application/json' -d '{"hello":"world"}'
curl -i http://127.0.0.1:8080/example
curl -i -X DELETE http://127.0.0.1:8080/example
curl -i http://127.0.0.1:8080/example
curl -s http://127.0.0.1:8080/__harness/status
```

Expected statuses are 201, 200, 204, and 404. PUT returns 201 on replacement as well as insertion; DELETE of a missing key returns 204. PUT accepts JSON objects only, with a 1 MiB HTTP body limit. Keys are URL-decoded UTF-8 strings of 1–1024 bytes in a single encoded path segment. Other methods follow Axum's method handling, including HEAD for a GET route.

Ctrl-C stops the server. Restart with exactly the same configuration and directory to recover its state.

## Run three routed nodes

Run one command per terminal:

```sh
./target/debug/distributed-kv-harness --config configs/three-local.json --node 1 --data-dir data/node1
./target/debug/distributed-kv-harness --config configs/three-local.json --node 2 --data-dir data/node2
./target/debug/distributed-kv-harness --config configs/three-local.json --node 3 --data-dir data/node3
```

PUT a key through port 8081 and GET it through ports 8082 or 8083. Each request is served by, or forwarded to, the same configured owner. This demonstrates routing, not replication. All three entry points report `unreplicated_single_owner` at `/__harness/status`.

Placement uses fixed FNV-1a 64-bit over decoded key bytes, modulo the explicit shard count. `shards[i]` is an ordered list of node IDs, reserved for future replica groups. The local baseline requires one ID per list. All nodes use the same config file. Peer handshakes check the full configuration, sender/destination IDs, and wire version. A configuration mismatch fails rather than serving a different ownership map. This is a trusted local experimental network; peer identities are configuration checks, not authentication.

## Plug in coordination

Implement `Coordinator` in `src/coordinator.rs`'s interface:

- `execute(Operation)` handles an HTTP client operation, including routing or coordination.
- `on_peer(from, payload)` handles protocol-specific JSON RPCs and can reply asynchronously.
- `name()` matches the configured coordinator name.

Supply a `CoordinatorFactory` to `RunningNode::with_listeners`. A factory receives `CoordinatorContext` containing the node ID, placement/configuration, `JournalStore`, `Transport`, and metrics. `RunningNode::start` is the convenience entry point for the shipped local factory; a new binary selection branch can supply another factory through `with_listeners`.

The test in `tests/replaceable_coordinator.rs` supplies a different coordinator without changing HTTP, storage, or transport. It holds one RPC open, completes another RPC on the same connection, then releases the first. This checks both the replacement seam and out-of-order reply matching.

Protocol responsibilities remain with the coordinator: timestamp allocation, quorum/membership rules, read eligibility, which metadata to persist, background tasks, retry/deduplication policy, recovery, and the point at which success is safe. Request futures can be cancelled or time out. Protocol state machines must continue any necessary work independently of a client future. The local journal already persists and publishes an enqueued commit even if its caller disappears.

## Persistence and peer behavior

`JournalStore` stores opaque JSON protocol records keyed by strings, not just public JSON objects. A future protocol can atomically persist a value with timestamps, validity metadata, or tombstones in one record, or publish a batch of related records atomically. The baseline wraps objects in its own state record and retains deletion tombstones.

A dedicated blocking writer owns an exclusively locked WAL. It writes length-delimited checksummed entries, calls `sync_all`, and then publishes records to the in-memory map before acknowledging. Already queued commits are opportunistically grouped (up to 32 commands) into one flush; there is no artificial batching delay. Replay is streaming. An incomplete final entry is truncated; complete checksum failures, malformed entries, identity mismatches, and invalid headers fail startup. A journal error fails the store closed. The checksum detects accidental corruption and is not cryptographic.

The full cluster configuration and node ID are bound into the WAL header. Changing placement/configuration requires a new experiment directory; migration and WAL compaction are future work. WAL and tombstone storage grow with the workload. SIGKILL recovery validates process-crash recovery, not hardware power-loss or disk-loss behavior.

Peer RPCs use a 4-byte big-endian length and JSON payload, capped at 2 MiB. Connections are lazy, persistent, and multiplexed using request IDs. Dedicated readers preserve partially read frames. Queues and per-connection concurrency are bounded. Each node allocates a worker/queue per remote peer; idle pool cost must be included in future scaling measurements.

RPC timeout covers the injected delay, queue wait, connection setup, and reply wait. A timeout or lost reply can leave a mutation's outcome unknown. The transport does not automatically retry failed requests. New calls reconnect when a link has failed. A future protocol must retain its own operation IDs across retries when needed.

Rust integration tests can use `RunningNode.faults.set(peer, LinkFault::Block)`, `Delay(duration)`, or `DropResponse`, then `clear()`. These are directed outgoing-link hooks; block both directions to model a bidirectional partition. They are process-local test APIs, not an exposed HTTP control plane or a complete deterministic message scheduler.

## Instrumentation and current boundary

`GET /__harness/status` reports configuration/guarantee and counters. KV HTTP calls, errors (including 404), forwarded operations, sent/received RPC requests, established outbound connections, completed framed peer bytes, durable flush groups, durable record count, and successfully synced WAL bytes are tracked. Peer bytes include handshakes and frame headers; they exclude TCP/IP overhead and incomplete frames. WAL bytes exclude the startup header and failed flushes. These counters are not NIC or block-device measurements.

Implemented tests cover required HTTP semantics, object/body validation, every ingress/owner pair, concurrent requests, owner unavailability/restart, persistent deletion, faults and unknown write outcomes, peer configuration rejection, connection reuse/multiplexing, WAL exclusivity, truncated tails, and corrupt records.

The benchmark driver, history recorder/linearizability checker, deterministic message scheduler, complete protocol adapters, lease/membership service, and online reconfiguration have not been implemented. See [benchmark requirements and protocol screening](docs/benchmark_scope.md) for the next research decisions. This is the deliberate implementation stopping point.
