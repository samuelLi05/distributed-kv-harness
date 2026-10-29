# Distributed KV coordination harness

A runnable Rust KV foundation for comparing coordination protocols. It includes HTTP, static sharding and routing, durable local protocol-state records, multiplexed TCP peer RPCs, fault hooks, and correctness/smoke tests.

Available coordinators:

- `local`: the original durable single-owner baseline, without replication.
- `abd_basic`: static RF=3 multi-writer ABD with two-phase quorum reads.
- `abd`: the same implementation with a safe one-phase read when two durable replies agree on the complete tag/value.

Every node accepts HTTP. ABD ingress nodes contact the key's owners directly, including when the ingress owns no replica. A majority is required for every successful operation. A 2+1 partition permits the majority to progress; an ingress unable to reach two owners returns 503, including for missing-key reads. Safety uses logical versions and durable quorums, not clocks or time-based leases. This provides linearizable single-key operations under the stated crash-recovery assumptions, rather than the full etcd feature set.

## Requirements and verification

- Rust 1.89 or later, Cargo, and Python 3 for the process smoke test.
- Direct Rust dependencies are limited to `tokio`, `axum`, `serde`, `serde_json`, `tracing`, and `tracing-subscriber`. `Cargo.lock` pins their transitive dependencies. There are no additional dev dependencies.

```sh
bash scripts/verify.sh
```

The script checks formatting, runs Clippy with warnings denied, runs Rust tests, builds the binary, and runs three real server processes over HTTP for each coordinator. The process tests kill one node with SIGKILL, exercise the surviving quorum for ABD, restart the node, then kill and restart the entire cluster. It verifies acknowledged values and a deletion survive. It uses temporary directories and ephemeral loopback ports and cleans up its processes.

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

Placement uses fixed FNV-1a 64-bit over decoded key bytes, modulo the explicit shard count. `shards[i]` is an ordered list of replica IDs. The local baseline requires one ID per list; both ABD modes require three. All nodes use the same config file. Peer handshakes check the full configuration, sender/destination IDs, and wire version. A configuration mismatch fails rather than serving a different ownership map. This is a trusted local experimental network; peer identities are configuration checks, not authentication.

## Plug in coordination

Implement `Coordinator` in `src/coordinator.rs`'s interface:

- `execute(Operation)` handles an HTTP client operation, including routing or coordination.
- `on_peer(from, payload)` handles protocol-specific JSON RPCs and can reply asynchronously.
- `name()` matches the configured coordinator name.
- `initialize()` completes any durable boot metadata before listeners start serving; it defaults to no work.

Supply a `CoordinatorFactory` to `RunningNode::with_listeners`. A factory receives `CoordinatorContext` containing the node ID, placement/configuration, `JournalStore`, `Transport`, and metrics. `RunningNode::start` selects the shipped local or ABD factory from configuration. Additional adapters can use `with_listeners`.

The test in `tests/replaceable_coordinator.rs` supplies a different coordinator without changing HTTP, storage, or transport. It holds one RPC open, completes another RPC on the same connection, then releases the first. This checks both the replacement seam and out-of-order reply matching.

Protocol responsibilities remain with the coordinator: timestamp allocation, quorum/membership rules, read eligibility, which metadata to persist, background tasks, retry/deduplication policy, recovery, and the point at which success is safe. Request futures can be cancelled or time out. Protocol state machines must continue any necessary work independently of a client future. The local journal already persists and publishes an enqueued commit even if its caller disappears.

## Persistence and peer behavior

`JournalStore` stores opaque JSON protocol records keyed by strings, not just public JSON objects. A protocol can atomically persist a value with timestamps, validity metadata, or tombstones in one record, or publish a batch of related records atomically. The baseline wraps objects in its own state record and retains deletion tombstones.

A dedicated blocking writer owns an exclusively locked WAL. It writes length-delimited checksummed entries, calls `sync_all`, and then publishes records to the in-memory map before acknowledging. Already queued commits are opportunistically grouped (up to 32 commands) into one flush; there is no artificial batching delay. Replay is streaming. An incomplete final entry is truncated; complete checksum failures, malformed entries, identity mismatches, and invalid headers fail startup. A journal error fails the store closed. The checksum detects accidental corruption and is not cryptographic.

The full cluster configuration and node ID are bound into the WAL header. Changing placement/configuration requires a new experiment directory; migration and WAL compaction are future work. WAL and tombstone storage grow with the workload. SIGKILL recovery validates process-crash recovery, not hardware power-loss or disk-loss behavior.

Peer RPCs use a 4-byte big-endian length and JSON payload, capped at 2 MiB. Connections are lazy, persistent, and multiplexed using request IDs. Dedicated readers preserve partially read frames. Queues and per-connection concurrency are bounded. Each node allocates a worker/queue per remote peer; idle pool cost must be included in future scaling measurements.

RPC timeout covers the injected delay, queue wait, connection setup, and reply wait. A timeout or lost reply can leave a mutation's outcome unknown. The transport does not automatically retry failed requests. New calls reconnect when a link has failed. A future protocol must retain its own operation IDs across retries when needed.

Rust integration tests can use `RunningNode.faults.set(peer, LinkFault::Block)`, `Delay(duration)`, or `DropResponse`, then `clear()`. These are directed outgoing-link hooks; block both directions to model a bidirectional partition. `simulation::Network` is a separate transport with explicit pending RPC IDs and serializable delivery steps: select IDs out of order, duplicate deliveries, lose requests/replies, or withhold messages. The saved schedule test replays exactly. This schedules RPC delivery, not Tokio tasks, disk execution, or the whole machine. No fault-control endpoint is exposed over HTTP.

`JournalStore.faults.pause(PausePoint)` pauses the dedicated WAL thread before append, before sync, after sync, or before ACK. Dropping the pause guard resumes it. Integration tests cancel a higher-version request at each boundary and deliver an older version afterward. A subprocess test also kills a process at each journal boundary and reopens its WAL; the before-sync result is treated as uncertain. The apply workers retain serialization until persistence completes.

## Instrumentation and current boundary

`GET /__harness/status` reports configuration/guarantee and counters. KV HTTP calls, errors (including 404), forwarded operations, sent/received RPC requests, established outbound connections, completed framed peer bytes, durable flush groups, durable record count, successfully synced WAL bytes, sync duration, fast reads and read write-backs are tracked. The status endpoint also reports process CPU time and peak RSS since boot via self-only `getrusage` on macOS/Linux. Other platforms report unavailable process telemetry. Peer bytes include handshakes and frame headers; they exclude TCP/IP overhead and incomplete frames. WAL bytes exclude the startup header and failed flushes. These counters are not NIC or block-device measurements.

Implemented tests cover required HTTP semantics, object/body validation, every ingress/owner pair, concurrent requests, owner unavailability/restart, persistent deletion, faults and unknown write outcomes, peer configuration rejection, connection reuse/multiplexing, WAL exclusivity, truncated tails, and corrupt records.

The bounded history recorder/checker, RPC scheduling transport, ABD adapters and open-loop benchmark driver are implemented. The checker exhaustively explores at most 24 operations per key and 100,000 states; larger searches return `Inconclusive`. Histories must start from absent keys and contain entire experiments, not arbitrary windows. Unknown writes may take effect or be omitted; they are never silently discarded.

## Run a replicated cluster

Use separate data directories from the local baseline and one command per terminal:

```sh
./target/debug/distributed-kv-harness --config configs/three-abd.json --node 1 --data-dir data/abd1
./target/debug/distributed-kv-harness --config configs/three-abd.json --node 2 --data-dir data/abd2
./target/debug/distributed-kv-harness --config configs/three-abd.json --node 3 --data-dir data/abd3
```

The example uses HTTP ports 8081–8083. Use the same curl operations above with those ports. Stop any one node and use a surviving HTTP endpoint; reads and writes continue. No shard membership change is performed. Restart the node using its intact WAL. Lost/reset data directories, Byzantine nodes, hardware power-loss guarantees, online configuration changes, compaction, and tombstone reclamation are not implemented.

## Reproduce benchmarks

```sh
cargo build --release --locked --bins
python3 scripts/benchmark.py --output work/abd-results
```

Default: 3/6/9 local server processes, both ABD modes, RF=3, 256 shards assigned to disjoint replica groups, 100,000 preloaded keys, 1 KiB objects, 128 driver workers, 500/2,000 offered operations/s, five workload regimes and three repetitions. Each server uses two Tokio runtime workers; the separate Rust driver uses four. Each server also has a blocking WAL thread; worker counts are not CPU affinity limits. The driver preloads through HTTP and checks every response. It uses persistent HTTP/1.1 connections with no automatic retry.

To run the initial measured matrix:

```sh
python3 scripts/benchmark.py --output work/abd-milestone --rates 250,4000 --seconds 3
```

Options include `--ingress owner`, `--payload 256` or `16384`, `--nodes`, `--rates`, `--workers`, `--keys`, `--repeats` and `--workloads`. Warmup is separate and warms server state; each measured driver process establishes its own HTTP connections. Short runs therefore include connection startup costs. Each case saves its exact specification, environment/source snapshot, summary, per-node counters/CPU/peak RSS, and compressed raw request samples. Peer bytes are application frames, not NIC bytes. CPU covers all server processes; driver CPU is separate. Peak RSS is a process-lifetime high-water mark, including preload and previous cases; it is not instantaneous RSS. WAL growth and flush time are reported separately from filesystem/device traffic.

Open-loop timestamps distinguish scheduled arrival, actual dispatch and completion. A bounded client queue rejects excess arrivals explicitly; these are counted as load-generator errors, never successful operations. Service latency excludes client queue delay; scheduled latency includes it. Throughput counts successful responses completed inside the measurement window, with draining reported separately. Overloaded cases describe the bounded experiment and do not establish a server-only maximum.

### Experimental coordinator comparisons

`--protocols` prints the static registry and declared contracts as JSON. `gus` is an RF=3, crash-recovery adaptation of Gus with replica forwarding, speculative writes/retagging, exact-tag views and a durable read-transfer slow path. It passes the shared majority-protocol contract tests, plus targeted cancellation, stale-message and scheduled-fault cases. It remains an experimental implementation; sampled histories are not a proof. See [architecture](docs/architecture.md) for protocol behavior and recovery.

Use the same cluster JSON with `coordinator` changed to the selected name and fresh data directories. Protocols share the HTTP API, framing, placement, journal and benchmark driver. Never change an existing data directory's protocol/configuration identity.

The driver accepts `warmup_seconds`, `capture_resources` and `run_id`. Warmup uses the same worker HTTP clients as measurement, every PUT changes its fixed-size object, and resources are measured after outstanding work drains. Use `python3 scripts/benchmark.py --protocols abd,gus --nodes 3 --rates 250 --seconds 15 --warmup 3 --repeats 5 --output work/comparison` after building release binaries. Original historical results use an older driver and must remain separate.
