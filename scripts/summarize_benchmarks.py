#!/usr/bin/env python3
"""Summarize saved benchmark runs without discarding overload or inventing confidence intervals."""
import argparse
import csv
import json
from pathlib import Path
import statistics


def metrics(run):
    counters = run["server_metrics_delta"]
    successes = run["successes"]
    batches = sum(m["durable_batches"] for m in counters)
    reads = sum(m["abd_fast_reads"] + m["abd_read_repairs"] for m in counters)
    puts = run["operations"].get("PUT", {})
    successful_puts = puts.get("offered", 0) - puts.get("errors", 0)
    result = {
        "successful_qps": run["successful_completions_in_window"] / run["window_seconds"],
        "error_fraction": run["errors"] / run["offered"],
        "generator_reject_fraction": run["rejected"] / run["offered"],
        "server_or_transport_errors": run["errors"] - run["rejected"],
        "server_cpu_us_per_success": sum(run["server_cpu_seconds"].values()) * 1e6 / successes,
        "peer_sent_bytes_per_success": sum(m["peer_bytes_sent"] for m in counters) / successes,
        "rpc_requests_per_success": sum(m["rpc_requests_sent"] for m in counters) / successes,
        "wal_bytes_per_success": sum(m["wal_bytes"] for m in counters) / successes,
        "records_per_flush": sum(m["durable_records"] for m in counters) / batches if batches else 0,
        "mean_flush_ms": sum(m["durable_flush_ns"] for m in counters) / batches / 1e6 if batches else 0,
        "fast_read_fraction": sum(m["abd_fast_reads"] for m in counters) / reads if reads else 0,
        "durable_records_per_successful_put": sum(m["durable_records"] for m in counters) / successful_puts if successful_puts else 0,
        "sum_process_peak_rss_mib": sum(run["server_peak_rss_bytes_after"].values()) / 1024**2,
        "driver_cpu_seconds": run["driver_cpu_seconds"],
    }
    for method in ("GET", "PUT"):
        stats = run["operations"].get(method)
        for latency in ("service_ns", "scheduled_ns"):
            result[f"{method.lower()}_{latency}_p99_ms"] = stats[latency]["p99"] / 1e6 if stats else 0
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    runs = json.loads((args.input / "results.json").read_text())
    environment = json.loads((args.input / "environment.json").read_text())
    args.output.mkdir(parents=True, exist_ok=True)
    groups = {}
    raw_rows = []
    for run in runs:
        case = run["case"]
        key = (case["nodes"], case["protocol"], case["workload"], case["rate"], case["ingress"])
        values = metrics(run)
        groups.setdefault(key, []).append(values)
        raw_rows.append({**case, **values})
    aggregates = []
    for key, rows in sorted(groups.items()):
        row = dict(zip(("nodes", "protocol", "workload", "rate", "ingress"), key))
        row["repeats"] = len(rows)
        for metric in rows[0]:
            values = [r[metric] for r in rows]
            row[metric] = statistics.median(values)
            row[metric + "_min"] = min(values)
            row[metric + "_max"] = max(values)
        aggregates.append(row)
    for name, rows in (("per_run.csv", raw_rows), ("aggregates.csv", aggregates)):
        with (args.output / name).open("w", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
    (args.output / "aggregates.json").write_text(json.dumps(aggregates, indent=2))
    first_nodes = min(r["nodes"] for r in aggregates)
    low_rate, high_rate = min(r["rate"] for r in aggregates), max(r["rate"] for r in aggregates)
    def select(nodes, protocol, workload, rate):
        return next(r for r in aggregates if (r["nodes"],r["protocol"],r["workload"],r["rate"]) == (nodes,protocol,workload,rate))
    lines = [
        "# ABD benchmark results", "",
        f"Environment: {environment['platform']}; {environment['logical_cpus']} logical CPUs; one physical host. "
        "Two Tokio workers plus one WAL thread per server, and four runtime workers for the separate driver. All processes share the host's CPU, memory and disk.", "",
        f"Measured runs: {len(runs)}. RF=3; 256 static shards; {runs[0]['spec']['keys']:,} preloaded keys and {runs[0]['spec']['payload_bytes']:,}-byte objects "
        "for the initial matrix. Confirm exact parameters in each saved specification. Cells below are medians; "
        "CSV/JSON aggregates also retain minimum and maximum across repetitions. These ranges are not confidence intervals.", "",
        f"Offered requests: {sum(r['offered'] for r in runs):,}. "
        f"Generator rejections: {sum(r['rejected'] for r in runs):,}. "
        f"Other response/transport errors: {sum(r['errors']-r['rejected'] for r in runs):,}.", "",
        f"## Equal light load: {first_nodes} nodes, {low_rate} offered operations/s", "",
        "CPU is summed across server processes and divided by successful operations, including drain. "
        "Peer bytes count transmitted application frames once, including responses; HTTP and TCP/IP headers are excluded. "
        "Latency below is client service p99 and excludes scheduling/queue delay. Scheduled p99 is retained in the CSV.", "",
        "| Workload | CPU µs/op: basic → fast | Peer B/op: basic → fast | GET p99 ms: basic → fast |",
        "| --- | ---: | ---: | ---: |",
    ]
    workloads = [w for w in ["read_only", "read_heavy", "mixed", "write_heavy", "hot_mixed"] if any(r["workload"] == w for r in aggregates)]
    for workload in workloads:
        a, b = [select(first_nodes, p, workload, low_rate) for p in ("abd_basic", "abd")]
        lines.append(f"| {workload} | {a['server_cpu_us_per_success']:.0f} → {b['server_cpu_us_per_success']:.0f} | {a['peer_sent_bytes_per_success']:.0f} → {b['peer_sent_bytes_per_success']:.0f} | {a['get_service_ns_p99_ms']:.2f} → {b['get_service_ns_p99_ms']:.2f} |")
    lines += ["", "Workloads: read-heavy is 95% GET/5% PUT; mixed is 50/50; write-heavy is 100% PUT. "
              "Hot-mixed is 50/50 on one key. A zero GET latency in the write-only row means no GET requests.", "",
              f"## Stress load: {high_rate} offered operations/s", "",
              "Each cell is successful completions/s followed by generator-rejected percentage. "
              "These are bounded-client overload experiments, not measured server-only maximum throughput. "
              "Errors are retained, and responses arriving after the measurement window do not inflate throughput.", "",
              "| Nodes | Workload | Basic QPS / rejects | Fast QPS / rejects |", "| ---: | --- | ---: | ---: |"]
    for nodes in sorted({r["nodes"] for r in aggregates}):
        for workload in workloads:
            a, b = [select(nodes, p, workload, high_rate) for p in ("abd_basic", "abd")]
            lines.append(f"| {nodes} | {workload} | {a['successful_qps']:.0f} / {100*a['generator_reject_fraction']:.1f}% | {b['successful_qps']:.0f} / {100*b['generator_reject_fraction']:.1f}% |")
    lines += ["", "## Persistence and memory", "",
              "Flush time is elapsed `sync_all` time, not CPU time or a block-device measurement. "
              "RSS is a process-lifetime high-water mark, including preload and earlier cases; summing those marks is not simultaneous host peak RSS.", "",
              "| Nodes | Fast mode workload at stress load | Records/flush | Mean flush ms | Durable records/PUT | Sum of process RSS high-water marks MiB |", "| ---: | --- | ---: | ---: | ---: | ---: |"]
    for nodes in sorted({r["nodes"] for r in aggregates}):
        for workload in ("write_heavy", "hot_mixed"):
            row = select(nodes, "abd", workload, high_rate)
            lines.append(f"| {nodes} | {workload} | {row['records_per_flush']:.2f} | {row['mean_flush_ms']:.2f} | {row['durable_records_per_successful_put']:.3f} | {row['sum_process_peak_rss_mib']:.1f} |")
    lines += ["", "Durable records/PUT counts all replica record appends, including read-repair work, divided by successful PUTs. "
              "It can be below one for overlapping replacements: a replica may acknowledge an older proposal after a higher tag is already durable. "
              "Thus differences in hot-key logical throughput are not evidence that the key acquired more replicas or independent disks."]
    lines += ["", "## Preload-warmed idle baseline", "",
              "Measured before the workload matrix. Connections subsequently opened by random ingress are not represented by this initial idle pool.", "",
              "| Nodes | Protocol | Summed CPU seconds / 2 s | Transmitted peer bytes |", "| ---: | --- | ---: | ---: |"]
    for path in sorted(args.input.glob("idle-*.json")):
        idle = json.loads(path.read_text())
        before, after = idle["before"], idle["after"]
        cpu = sum(b["process"]["cpu_seconds"]-a["process"]["cpu_seconds"] for a,b in zip(before,after))
        sent = sum(b["metrics"]["peer_bytes_sent"]-a["metrics"]["peer_bytes_sent"] for a,b in zip(before,after))
        lines.append(f"| {len(after)} | {after[0]['coordinator']} | {cpu:.4f} | {sent} |")
    lines += ["", "## Reproduce and inspect", "",
              "Run the benchmark command recorded in `environment.json`, then:", "",
              "```sh", "python3 scripts/summarize_benchmarks.py PATH_TO_RUNS PATH_TO_REPORT", "```", "",
              "The run directory contains source snapshots, binary hashes, seeds, exact configurations, "
              "per-run summaries, before/after counters and gzip-compressed request samples. "
              "The CSVs retain all runs and operation-specific service/scheduled p99 latency. "
              "Warmup and preload are excluded from the measured-run table. Warmup uses a separate driver, so measured runs include their own HTTP connection startup. Synthetic object bytes are repeated across writes; ABD still advances and persists versions. Future value-elision comparisons must use changing values.", "",
              "The matrix changes process count on one host. It cannot establish physical horizontal scaling, "
              "a cluster-wide network bandwidth ceiling, or superiority over Raft/Gus/Hermes. "
              "Independent disks/hosts, longer repeated runs, owner-aware ingress and added peer RTT are follow-up experiments.", ""]
    (args.output / "measurements.md").write_text("\n".join(lines))
    print(f"Summarized {len(runs)} runs into {args.output}")


if __name__ == "__main__":
    main()
