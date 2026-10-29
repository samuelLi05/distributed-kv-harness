# Architecture

## Shared harness

A static protocol registry describes replication, ingress and availability contracts. The existing opaque WAL, HTTP API and transport remain common. Coordinators have local initialization, peer-enabled startup, readiness, diagnostics and shutdown hooks. Accepted WAL/apply work and peer handlers remain counted after client cancellation.

The driver changes every PUT's fixed-size JSON object using run/phase/sequence identifiers, preserves worker HTTP clients between warmup and measurement, excludes warmup counters, and samples resource boundaries after pending work drains. GET shape checks are performance checks, not a linearizability oracle. Bounded correctness histories are separate tests. A failed drain is explicitly flagged and rejected by orchestration. No application-level mutation retry is introduced.

## ABD replication

`abd` and `abd_basic` use RF=3 majority quorums. Writes query a majority for the maximum logical timestamp, then persist a higher-tagged value or tombstone on a majority. Reads select the largest returned tag. Basic mode confirms a second phase; fast mode returns when two durable replies agree on the complete tag/value, otherwise it repairs a majority. Every ingress node coordinates directly with the key's owners. Striped apply workers serialize replica updates until journal commits complete, including after caller cancellation.
