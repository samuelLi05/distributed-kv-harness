# Performance findings and investigated problems

## Wait for normal validation before replay

Initial hot-key tests exposed deadline errors and hundreds of redundant replays. The coordinator now waits for a live invalidation to receive VAL before replaying. It waits at most 20 ms plus twice the configured artificial peer RTT. A matching validation wakes readers immediately. Recovered invalidations replay immediately. The timeout controls recovery timing; every replay still requires all three durable acknowledgments. No clock bound is used for safety.

Controlled before/after: RF=3, three processes, one hot key, 50% GET/50% PUT, 1 KiB changing objects, 2,000 preloaded keys, one-second connection-preserving warmup, five-second measurement, three repetitions. Same storage, transport and driver source. Builds and complete source snapshots are saved separately. These are short local measurements, not a general throughput claim.

| Protocol | Offered/s | Replay grace ms | Success/s | Other errors (median/run) | CPU µs/success | Peer B/success | Replays (median/run) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| hermes_alr_static | 250 | 0 | 248 | 0 | 1617 | 2805 | 533 |
| hermes_alr_static | 250 | 20 | 249 | 0 | 1163 | 1569 | 0 |
| hermes_alr_static | 4000 | 0 | 277 | 14 | 1508 | 2908 | 801 |
| hermes_alr_static | 4000 | 20 | 342 | 0 | 1025 | 1564 | 1 |
| hermes_static | 250 | 0 | 248 | 0 | 1338 | 2785 | 565 |
| hermes_static | 250 | 20 | 249 | 0 | 920 | 1445 | 0 |
| hermes_static | 4000 | 0 | 259 | 202 | 1447 | 3058 | 795 |
| hermes_static | 4000 | 20 | 342 | 0 | 733 | 1429 | 0 |

At 4,000 offered requests/s, median static-Hermes success rate increased from 259 to 342/s (32%); ALR increased from 277 to 342/s (24%). Other response/deadline errors dropped to zero in all six revised stress runs. Generator rejections remain high and are retained in the raw data; this is bounded-client overload behavior.

Artifacts: `outputs/replay-before`, `outputs/replay-after`, and `outputs/replay-comparison`. Each contains configurations, seeds, source/binary identity, counters and compressed samples.

## Measurement method

Added symmetric per-RPC latency without serializing requests on a connection writer. A concurrent-request test verifies pipelining. Six-node RF=3 groups are used for routing experiments because three nodes make every ingress an owner.

Outgoing RPCs are now counted through reply/expiry even when a quorum cancels its caller. WAL/apply/background work is counted independently. Resource snapshots require a quiet pending-work boundary; runs that cannot settle are flagged. Continuous RSS polling was removed when snapshots are enabled, because getrusage already preserves the process high-water mark.
