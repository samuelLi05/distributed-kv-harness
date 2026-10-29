# ABD benchmark results

Environment: macOS-26.6.2-arm64-arm-64bit-Mach-O; 18 logical CPUs; one physical host. Two Tokio workers plus one WAL thread per server, and four runtime workers for the separate driver. All processes share the host's CPU, memory and disk.

Measured runs: 180. RF=3; 256 static shards; 100,000 preloaded keys and 1,024-byte objects for the initial matrix. Confirm exact parameters in each saved specification. Cells below are medians; CSV/JSON aggregates also retain minimum and maximum across repetitions. These ranges are not confidence intervals.

Offered requests: 1,147,500. Generator rejections: 355,825. Other response/transport errors: 0.

## Equal light load: 3 nodes, 250 offered operations/s

CPU is summed across server processes and divided by successful operations, including drain. Peer bytes count transmitted application frames once, including responses; HTTP and TCP/IP headers are excluded. Latency below is client service p99 and excludes scheduling/queue delay. Scheduled p99 is retained in the CSV.

| Workload | CPU µs/op: basic → fast | Peer B/op: basic → fast | GET p99 ms: basic → fast |
| --- | ---: | ---: | ---: |
| read_only | 1412 → 783 | 4959 → 2439 | 1.55 → 1.22 |
| read_heavy | 1422 → 849 | 4859 → 2459 | 1.58 → 1.14 |
| mixed | 1614 → 1365 | 3878 → 2669 | 7.49 → 1.22 |
| write_heavy | 1645 → 1649 | 2873 → 2873 | 0.00 → 0.00 |
| hot_mixed | 1605 → 1330 | 3934 → 3149 | 29.14 → 25.94 |

Workloads: read-heavy is 95% GET/5% PUT; mixed is 50/50; write-heavy is 100% PUT. Hot-mixed is 50/50 on one key. A zero GET latency in the write-only row means no GET requests.

## Stress load: 4000 offered operations/s

Each cell is successful completions/s followed by generator-rejected percentage. These are bounded-client overload experiments, not measured server-only maximum throughput. Errors are retained, and responses arriving after the measurement window do not inflate throughput.

| Nodes | Workload | Basic QPS / rejects | Fast QPS / rejects |
| ---: | --- | ---: | ---: |
| 3 | read_only | 3998 / 0.0% | 3999 / 0.0% |
| 3 | read_heavy | 3997 / 0.0% | 3996 / 0.0% |
| 3 | mixed | 2949 / 23.1% | 3322 / 13.8% |
| 3 | write_heavy | 1629 / 56.1% | 1589 / 57.1% |
| 3 | hot_mixed | 741 / 78.3% | 782 / 77.3% |
| 6 | read_only | 3997 / 0.0% | 3997 / 0.0% |
| 6 | read_heavy | 3996 / 0.0% | 3997 / 0.0% |
| 6 | mixed | 1870 / 50.0% | 2080 / 44.8% |
| 6 | write_heavy | 1138 / 68.3% | 1123 / 68.7% |
| 6 | hot_mixed | 1077 / 69.9% | 1074 / 70.0% |
| 9 | read_only | 3999 / 0.0% | 3999 / 0.0% |
| 9 | read_heavy | 3997 / 0.0% | 3998 / 0.0% |
| 9 | mixed | 2393 / 37.0% | 2817 / 26.4% |
| 9 | write_heavy | 1348 / 63.1% | 1415 / 61.4% |
| 9 | hot_mixed | 1407 / 61.6% | 1471 / 60.0% |

## Persistence and memory

Flush time is elapsed `sync_all` time, not CPU time or a block-device measurement. RSS is a process-lifetime high-water mark, including preload and earlier cases; summing those marks is not simultaneous host peak RSS.

| Nodes | Fast mode workload at stress load | Records/flush | Mean flush ms | Durable records/PUT | Sum of process RSS high-water marks MiB |
| ---: | --- | ---: | ---: | ---: | ---: |
| 3 | write_heavy | 13.42 | 6.81 | 2.999 | 973.2 |
| 3 | hot_mixed | 1.00 | 8.30 | 0.875 | 973.4 |
| 6 | write_heavy | 6.66 | 8.30 | 2.998 | 1005.5 |
| 6 | hot_mixed | 1.00 | 7.74 | 0.619 | 1005.5 |
| 9 | write_heavy | 10.19 | 8.82 | 2.999 | 1060.6 |
| 9 | hot_mixed | 1.00 | 7.36 | 0.468 | 1060.8 |

Durable records/PUT counts all replica record appends, including read-repair work, divided by successful PUTs. It can be below one for overlapping replacements: a replica may acknowledge an older proposal after a higher tag is already durable. Thus differences in hot-key logical throughput are not evidence that the key acquired more replicas or independent disks.

## Preload-warmed idle baseline

Measured before the workload matrix. Connections subsequently opened by random ingress are not represented by this initial idle pool.

| Nodes | Protocol | Summed CPU seconds / 2 s | Transmitted peer bytes |
| ---: | --- | ---: | ---: |
| 3 | abd | 0.0035 | 0 |
| 3 | abd_basic | 0.0040 | 0 |
| 6 | abd | 0.0084 | 0 |
| 6 | abd_basic | 0.0077 | 0 |
| 9 | abd | 0.0092 | 0 |
| 9 | abd_basic | 0.0119 | 0 |

## Reproduce and inspect

Run the benchmark command recorded in `environment.json`, then:

```sh
python3 scripts/summarize_benchmarks.py PATH_TO_RUNS PATH_TO_REPORT
```

The run directory contains source snapshots, binary hashes, seeds, exact configurations, per-run summaries, before/after counters and gzip-compressed request samples. The CSVs retain all runs and operation-specific service/scheduled p99 latency. Warmup and preload are excluded from the measured-run table. Warmup uses a separate driver, so measured runs include their own HTTP connection startup. Synthetic object bytes are repeated across writes; ABD still advances and persists versions. Future value-elision comparisons must use changing values.

The matrix changes process count on one host. It cannot establish physical horizontal scaling, a cluster-wide network bandwidth ceiling, or superiority over Raft/Gus/Hermes. Independent disks/hosts, longer repeated runs, owner-aware ingress and added peer RTT are follow-up experiments.
