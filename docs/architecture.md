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

## Fixed-membership Hermes and batched reads

`hermes_static` uses logical tags, durable INV before ACK, ALL-three completion, exact-tag VAL, and replay of the original tag/value. Writes at different replicas can overlap and supersede each other. A replay allocates no new timestamp. Validation is volatile: the durable record remains invalid, and a process restart requires replay before that key can be read. This avoids a second WAL write for every validation while preserving acknowledged values on stable disks. It also means restart/read recovery requires all configured members. An absent unrecorded key is the initial state; acknowledged writes cannot have missed that member.

The configuration is immutable. Members are never removed, and neither side of a 2+1 partition can complete writes. Local reads may complete on a valid isolated replica because no other partition can complete a conflicting all-member write. This is a different availability contract from ABD/Gus, not a fully fault-tolerant Hermes deployment. Timing assumptions are unnecessary only because this experiment makes no membership changes.

`hermes_alr_static` buffers eligible local values and returns them after a subsequent majority confirmation of the fixed epoch. It batches already-pending reads (maximum 128), without a batching timer. This prices the ALR data path and refuses minority reads; it does not implement or validate configuration agreement, fencing, state transfer, or failover. Unknown epochs fail closed.

References: [Hermes, ASPLOS 2020](https://vasigavr1.github.io/files/Hermes-ASPLOS20.pdf), [LAW theorem and ALR, VLDB 2025](https://www.vldb.org/pvldb/vol18/p2831-giortamis.pdf). The common request/reply transport sends empty replies for VAL and view messages as well. Those frames are included in the measured traffic.

## Read-copy ordering and state compatibility

Read repair now has a distinct conditional apply mode, with the freshness check inside the serialized worker. A newer receiver rejects the copy and the reader refreshes its evidence. Only the original writer can issue a larger-tag commit. Delayed offers preserve prior installation and the original acceptance ACK rather than downgrading the durable installed flag or incorrectly causing another retag. Receipt/writer messages are restricted to the owning cohort.

Gus now rejects old experimental state schemas and the shared transport rejects the earlier wire generation. All servers in an experiment use the same build. Retained bookkeeping entries are counted incrementally, removing a dataset scan from the status/measurement path.

## Benchmark checkpoints

Benchmark resume validates identical binary hashes and parameters, refuses active process manifests, retains complete fresh-cluster batches, and archives an incomplete batch's samples, logs, configuration and partial results before recreating that entire batch from fresh directories. It never retries individual mutations. Resume source/environment snapshots are retained alongside the original provenance.
