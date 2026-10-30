#!/usr/bin/env python3
"""Aggregate arbitrary coordinator experiments. Python stdlib only; retain errors and ranges."""
import argparse
import csv
import json
from pathlib import Path
import statistics

AXES=("nodes","protocol","workload","rate","ingress","peer_rtt_ms","keys","payload_bytes","replay_grace_ms","seconds","warmup_seconds","workers","build","delete_percent")
def metrics(run):
    successes=run["successes"]
    def per(value): return value/successes if successes else None
    counters=run["server_metrics_delta"]
    total=lambda key:sum(m.get(key,0) for m in counters)
    batches=total("durable_batches")
    result={
        "successful_qps":run["successful_completions_in_window"]/run["window_seconds"],
        "error_percent":100*run["errors"]/run["offered"],
        "generator_reject_percent":100*run["rejected"]/run["offered"],
        "other_errors":run["errors"]-run["rejected"],
        "cpu_us_per_success":per(sum(run["server_cpu_seconds"].values())*1e6),
        "peer_bytes_per_success":per(total("peer_bytes_sent")),
        "peer_frames_per_success":per(total("rpc_requests_sent")+total("rpc_replies_sent")),
        "peer_rpcs_per_success":per(total("rpc_requests_sent")),
        "wal_bytes_per_success":per(total("wal_bytes")),
        "records_per_success":per(total("durable_records")),
        "records_per_flush":total("durable_records")/batches if batches else 0,
        "mean_flush_ms":total("durable_flush_ns")/batches/1e6 if batches else 0,
        "sum_peak_rss_mib":sum(run["server_peak_rss_bytes_after"].values())/1024**2,
        "warmup_errors":run["warmup_errors"],
        "drained":int(run["settled"]),
    }
    for method in ("GET","PUT","DELETE"):
        stats=run["operations"].get(method)
        for kind in ("successful_service_ns","successful_scheduled_ns"):
            result[f"{method.lower()}_{kind}_p99_ms"]=stats[kind]["p99"]/1e6 if stats else None
    before=run["resources_before"];after=run["resources_after"]
    for key in ("fast_writes","retags","fast_reads","read_transfers","view_messages","background_errors","writes","local_reads","replays","validations","stale_vals","sync_batches","synced_reads","retained_versions"):
        result[f"protocol_{key}"]=sum(b["diagnostics"].get(key,0)-a["diagnostics"].get(key,0) for a,b in zip(before,after))
    result["retained_versions_after"]=sum(s["diagnostics"].get("retained_versions",0) for s in after)
    result["alr_reads_per_batch"]=result["protocol_synced_reads"]/result["protocol_sync_batches"] if result["protocol_sync_batches"] else None
    fast=result["protocol_fast_writes"];slow=result["protocol_retags"]
    result["gus_fast_write_percent"]=100*fast/(fast+slow) if fast+slow else None
    return result

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs",nargs="+",type=Path)
    parser.add_argument("--output",required=True,type=Path)
    args=parser.parse_args();groups={};rows=[];profiles={};environments=[]
    runs=[]
    for source in args.inputs:
        environment=json.loads((source/"environment.json").read_text());environments.append({"source":str(source.resolve()),**environment})
        for run in json.loads((source/"results.json").read_text()):
            case={**run["case"],"keys":run["spec"]["keys"],"payload_bytes":run["spec"]["payload_bytes"]}
            case.setdefault("peer_rtt_ms",0)
            case["replay_grace_ms"]=run["resources_after"][0]["diagnostics"].get("replay_grace_ms",0)
            for option in ("seconds","warmup_seconds","workers","delete_percent"):case[option]=run["spec"][option]
            case["build"]=environment["binaries_sha256"]["server"]+":"+environment["binaries_sha256"]["driver"]
            profiles[case["protocol"]]=run["protocol"]
            key=tuple(case[a] for a in AXES);values=metrics(run)
            groups.setdefault(key,[]).append(values);rows.append({**case,**values,"source":str(source.resolve())});runs.append(run)
    aggregates=[]
    for key,group in sorted(groups.items()):
        row=dict(zip(AXES,key));row["repeats"]=len(group)
        for metric in group[0]:
            values=[r[metric] for r in group if r[metric] is not None]
            row[metric]=statistics.median(values) if values else None
            row[metric+"_min"]=min(values) if values else None;row[metric+"_max"]=max(values) if values else None
        aggregates.append(row)
    args.output.mkdir(parents=True,exist_ok=True)
    for name,data in (("per_run.csv",rows),("aggregates.csv",aggregates)):
        with (args.output/name).open("w",newline="") as f:
            writer=csv.DictWriter(f,fieldnames=list(data[0]));writer.writeheader();writer.writerows(data)
    (args.output/"aggregates.json").write_text(json.dumps(aggregates,indent=2))
    (args.output/"environments.json").write_text(json.dumps(environments,indent=2))
    offered=sum(r["offered"] for r in runs);rejected=sum(r["rejected"] for r in runs);other=sum(r["errors"]-r["rejected"] for r in runs)
    lines=["# Coordinator comparison measurements","",f"{len(runs)} runs; {offered:,} offered requests; {rejected:,} generator rejections; {other:,} other response/transport errors.","",
        "All runs use one physical host with shared CPU, memory and SSD. RF=3; two Tokio workers and one WAL thread per server; a separate four-worker open-loop driver. Cells are medians; CSV/JSON retain every run and min/max ranges. Ranges are not confidence intervals.","",
        "CPU and peer/WAL costs divide all measured and drained work by successful measured operations. Throughput counts completions inside the scheduled window. Successful scheduled p99 includes client queue delay; errors/rejections remain separate. Peak RSS is since boot, including preload/earlier cases. Peer frames count requests and responses once, excluding handshake frames; bytes include framing/handshakes and exclude HTTP and TCP/IP headers.","",
        "## Declared contracts","","| Protocol | Majority write progress | Experimental | Ingress |","| --- | --- | --- | --- |"]
    for name,d in sorted(profiles.items()):lines.append(f"| {name} | {'yes' if d['majority_progress'] else 'no'} | {d['experimental']} | {d['ingress']} |")
    lines += ["","Fixed-membership Hermes cannot be ranked as having ABD/Gus availability. Its ALR mode measures checks against an immutable epoch, not full configuration recovery."]
    profile_axes=("keys","payload_bytes","seconds","warmup_seconds","workers","build","delete_percent")
    profile_groups={}
    for row in aggregates:profile_groups.setdefault(tuple(row[k] for k in profile_axes),[]).append(row)
    def fmt(value):return "—" if value is None else f"{value:.2f}"
    for profile,subset in sorted(profile_groups.items()):
        keys,payload,seconds,warmup,workers,build,deletes=profile
        lines += ["",f"## {keys:,} keys; {payload:,} B; {seconds:g} s window; {warmup:g} s warmup; {workers} workers; build {build[:8]}; {deletes}% of mutations DELETE","",
            "| N | Protocol | Workload | Offered/s | Ingress | Added RTT ms | Replay grace ms | Success/s | Reject % | Other errors | CPU µs/success | Peer B/success | GET scheduled p99 ms | PUT scheduled p99 ms |",
            "| ---: | --- | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
        for r in subset:
            lines.append(f"| {r['nodes']} | {r['protocol']} | {r['workload']} | {r['rate']} | {r['ingress']} | {r['peer_rtt_ms']} | {r['replay_grace_ms']} | {r['successful_qps']:.0f} | {r['generator_reject_percent']:.2f} | {r['other_errors']:.0f} | {fmt(r['cpu_us_per_success'])} | {fmt(r['peer_bytes_per_success'])} | {fmt(r['get_successful_scheduled_ns_p99_ms'])} | {fmt(r['put_successful_scheduled_ns_p99_ms'])} |")
    lines += ["","## Reproduction","","Each input directory contains exact specifications/configurations, seeds, source snapshots, binary SHA256 hashes, before/after resources, summaries, server logs and compressed request samples. Warmup shares measured worker connections, values change, and unsettled runs are flagged rather than silently discarded. CPU comparisons include each adapter's state indexing/serialization choices; common infrastructure alone does not isolate theoretical protocol cost.","",
        "```sh","python3 scripts/compare_protocols.py RUN_DIRECTORY ... --output REPORT_DIRECTORY","```",""]
    (args.output/"measurements.md").write_text("\n".join(lines));print(f"Summarized {len(runs)} runs into {args.output}")
if __name__=="__main__":main()
