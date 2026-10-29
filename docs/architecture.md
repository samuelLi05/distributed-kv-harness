# Architecture

## Shared harness

A static protocol registry describes replication, ingress and availability contracts. The existing opaque WAL, HTTP API and transport remain common. Coordinators have local initialization, peer-enabled startup, readiness, diagnostics and shutdown hooks. Accepted WAL/apply work and peer handlers remain counted after client cancellation.

The driver changes every PUT's fixed-size JSON object using run/phase/sequence identifiers, preserves worker HTTP clients between warmup and measurement, excludes warmup counters, and samples resource boundaries after pending work drains. GET shape checks are performance checks, not a linearizability oracle. Bounded correctness histories are separate tests. A failed drain is explicitly flagged and rejected by orchestration. No application-level mutation retry is introduced.

## ABD replication

`abd` and `abd_basic` use RF=3 majority quorums. Writes query a majority for the maximum logical timestamp, then persist a higher-tagged value or tombstone on a majority. Reads select the largest returned tag. Basic mode confirms a second phase; fast mode returns when two durable replies agree on the complete tag/value, otherwise it repairs a majority. Every ingress node coordinates directly with the key's owners. Striped apply workers serialize replica updates until journal commits complete, including after caller cancellation.

## Gus replication

The `gus` experiment uses RF=3, owner-replica coordination, speculative tags, stale-proposal retagging, exact-tag view receipts and retained versions. Nonowner ingress probes an owner once, caches that route, and forwards; a failed mutation is never automatically retried. Recovery rebuilds a typed index from opaque WAL records and resets remote views conservatively. Persisted boot incarnations distinguish unpublished tags after restart. Local same-key writes are serialized; concurrent local reads share a gate.

Implementation reference: [paper, Algorithm 1 and the n=3 read-transfer optimization](https://arxiv.org/pdf/2304.09382), and [author's implementation](https://github.com/bc-computing/gus-epaxos). This is a Rust crash-recovery adaptation, not a performance reproduction of the Go artifact. A remote offer ACK follows persistence, and local installation completes before HTTP success. Retag decisions persist locally before commit messages are emitted. The reference artifact replies before its local logging step; this adapter completes local persistence before acknowledging the write.

A slow read transfers a newer version from the contacted replica, or repairs a local version to that replica, before relying on two durable copies. A live writer rejects another reader's attempt to promote that writer's unfinished speculative operation: otherwise the read could expose a value before a later retag decision. Owned protocol tasks survive caller cancellation; persistence is not aborted by the response deadline. Missing commit values trigger an explicit value repair, rather than applying a retag to the wrong operation. Background view tasks are bounded and included in pending-work accounting. Version/view reclamation is deferred and its growth is reported.
