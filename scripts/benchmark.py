#!/usr/bin/env python3
"""Repeatable RF=3 multi-process experiments; Python standard library only.

All local processes share the host. These are harness/coordination measurements,
not evidence of physical-machine horizontal scaling. The Rust driver is open-loop.
"""
import argparse
import gzip
import hashlib
import http.client
import json
import os
from pathlib import Path
import platform
import resource
import shutil
import signal
import socket
import subprocess
import tempfile
import time

WORKLOADS = {
    "read_only": (0, None), "read_heavy": (5, None), "mixed": (50, None),
    "write_heavy": (100, None), "hot_mixed": (50, 1),
}


def get_status(address):
    host, port = address.split(":")
    connection = http.client.HTTPConnection(host, int(port), timeout=10)
    try:
        connection.request("GET", "/__harness/status")
        response = connection.getresponse()
        assert response.status == 200
        return json.loads(response.read())
    finally:
        connection.close()


def process_stats(nodes):
    return {str(node["id"]): get_status(node["http"])["process"] for node in nodes}


def stop(processes):
    for process in processes:
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
    for process in processes:
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def driver(binary, output, name, spec, servers):
    spec_path = output / f"{name}.spec.json"
    samples = output / f"{name}.samples.jsonl"
    summary = output / f"{name}.summary.json"
    spec_path.write_text(json.dumps(spec, indent=2))
    before_usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    started = time.monotonic()
    with (output / f"{name}.driver.log").open("wb") as log:
        process = subprocess.Popen([str(binary), str(spec_path), str(samples), str(summary)], stdout=log, stderr=subprocess.STDOUT)
        peak = {}
        last = time.monotonic()
        try:
            while process.poll() is None:
                if time.monotonic() - started > 600:
                    raise RuntimeError("driver exceeded 600 s")
                if not spec.get("capture_resources",False) and time.monotonic() - last >= 0.25:
                    stats = process_stats(servers)
                    for pid, item in stats.items():
                        peak[pid] = max(peak.get(pid, 0), item["peak_rss_bytes"])
                    last = time.monotonic()
                time.sleep(0.01)
        except BaseException:
            if process.poll() is None:
                process.kill()
            process.wait()
            raise
    if process.returncode:
        raise RuntimeError((output / f"{name}.driver.log").read_text())
    result = json.loads(summary.read_text())
    after_usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    # Includes driver startup, output writing and monitoring subprocesses; servers remain running.
    result["driver_whole_process_cpu_seconds"] = (
        after_usage.ru_utime + after_usage.ru_stime - before_usage.ru_utime - before_usage.ru_stime
    )
    result["server_peak_rss_bytes_since_boot"] = {str(s["node_id"]):s["process"]["peak_rss_bytes"] for s in result.get("resources_after",[])} or peak
    result["driver_wall_seconds"] = time.monotonic() - started
    with samples.open("rb") as source, gzip.open(str(samples) + ".gz", "wb") as target:
        shutil.copyfileobj(source, target)
    samples.unlink()
    summary.write_text(json.dumps(result, indent=2))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--binary", type=Path, default=Path("target/release/distributed-kv-harness"))
    parser.add_argument("--driver", type=Path, default=Path("target/release/kv-bench"))
    parser.add_argument("--nodes", default="3,6,9")
    parser.add_argument("--protocols", default="abd_basic,abd")
    parser.add_argument("--rates", default="500,2000")
    parser.add_argument("--workloads", default=",".join(WORKLOADS))
    parser.add_argument("--ingress", choices=["owner", "random"], default="random")
    parser.add_argument("--seconds", type=float, default=15)
    parser.add_argument("--warmup", type=float, default=3)
    parser.add_argument("--peer-rtt-ms", type=int, default=0)
    parser.add_argument("--hot-low-rate", type=int)
    parser.add_argument("--delete-percent", type=int, default=0)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--keys", type=int, default=100000)
    parser.add_argument("--payload", type=int, default=1024)
    parser.add_argument("--workers", type=int, default=128)
    args = parser.parse_args()
    counts = [int(n) for n in args.nodes.split(",")]
    protocols = args.protocols.split(",")
    rates = [int(n) for n in args.rates.split(",")]
    workloads = args.workloads.split(",")
    assert all(n >= 3 and n % 3 == 0 for n in counts)

    assert all(w in WORKLOADS for w in workloads)
    assert args.repeats > 0 and args.seconds > 0 and args.keys > 0
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    binary, bench = args.binary.resolve(strict=True), args.driver.resolve(strict=True)
    registry = {d["name"]: d for d in json.loads(subprocess.check_output([str(binary), "--protocols"], text=True))}
    assert all(p in registry and registry[p]["replicas"] == 3 for p in protocols)
    metadata = {
        "arguments": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
        "platform": platform.platform(), "machine": platform.machine(), "logical_cpus": os.cpu_count(),
        "rustc": subprocess.check_output(["rustc", "--version"], text=True).strip(),
        "git_head": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "git_status": subprocess.check_output(["git", "status", "--short"], text=True),
        "git_diff": subprocess.check_output(["git", "diff"], text=True),
        "binaries_sha256": {"server":hashlib.sha256(binary.read_bytes()).hexdigest(), "driver":hashlib.sha256(bench.read_bytes()).hexdigest()},
        "protocol_registry": registry, "server_runtime_threads_per_process": 2, "driver_runtime_threads": 4,
        "physical_hosts": 1, "cpu_measurement": "self getrusage, microsecond CPU accounting; peak RSS since process boot",
        "utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    # Snapshot sources too: HEAD alone does not identify an uncommitted experiment.
    source = output / "source"
    source.mkdir()
    for name in ("Cargo.toml", "Cargo.lock", "src", "scripts", "tests", "configs"):
        path = Path(name)
        if path.is_dir():
            shutil.copytree(path, source / name, ignore=shutil.ignore_patterns("__pycache__"))
        else:
            shutil.copy2(path, source / name)
    (output / "environment.json").write_text(json.dumps(metadata, indent=2))
    rows = []
    for count in counts:
        for protocol in protocols:
            processes, reservations = [], []
            with tempfile.TemporaryDirectory(prefix="kv-benchmark-") as temp:
                root = Path(temp)
                try:
                    for _ in range(count * 2):
                        sock = socket.socket()
                        sock.bind(("127.0.0.1", 0))
                        reservations.append(sock)
                    ports = [s.getsockname()[1] for s in reservations]
                    config = {
                        "cluster_id": f"benchmark-{count}-{protocol}", "coordinator": protocol,
                        "nodes": [{"id": i+1, "http": f"127.0.0.1:{ports[2*i]}", "peer": f"127.0.0.1:{ports[2*i+1]}"} for i in range(count)],
                        "shards": [[3*(s % (count//3))+j for j in (1,2,3)] for s in range(256)],
                        "rpc_timeout_ms": 2000, "benchmark_peer_rtt_ms": args.peer_rtt_ms,
                    }
                    config_path = output / f"cluster-{count}-{protocol}.json"
                    config_path.write_text(json.dumps(config, indent=2))
                    for i in range(count):
                        reservations[2*i].close()
                        reservations[2*i+1].close()
                        with (output / f"server-{count}-{protocol}-{i+1}.log").open("wb") as log:
                            processes.append(subprocess.Popen([str(binary), "--config", str(config_path), "--node", str(i+1), "--data-dir", str(root / str(i+1))], stdout=log, stderr=subprocess.STDOUT, env={**os.environ, "TOKIO_WORKER_THREADS": "2", "RUST_LOG": "warn"}))
                    process_record=output / f"processes-{count}-{protocol}.json"
                    process_record.write_text(json.dumps({"active":True,"servers":[{"node":node["id"],"pid":process.pid} for node,process in zip(config["nodes"],processes)]},indent=2))
                    for node, process in zip(config["nodes"], processes):
                        deadline = time.monotonic()+20
                        while True:
                            if process.poll() is not None:
                                raise RuntimeError("server exited; inspect saved server logs")
                            try:
                                get_status(node["http"])
                                break
                            except (OSError, http.client.HTTPException):
                                if time.monotonic() > deadline:
                                    raise
                                time.sleep(0.02)
                    base = {"config": str(config_path), "mode": "preload", "rate": 1000, "seconds": args.seconds,
                            "keys": args.keys, "active_keys": args.keys, "mutation_percent": 100, "delete_percent": args.delete_percent,
                            "payload_bytes": args.payload, "ingress": "owner", "workers": args.workers, "seed": 7163, "capture_resources": True}
                    driver(bench, output, f"preload-{count}-{protocol}", base, config["nodes"])
                    idle_before = [get_status(node["http"]) for node in config["nodes"]]
                    time.sleep(2)
                    idle_after = [get_status(node["http"]) for node in config["nodes"]]
                    (output / f"idle-{count}-{protocol}.json").write_text(json.dumps({"seconds":2,"before":idle_before,"after":idle_after}, indent=2))
                    cases = [(w, args.hot_low_rate if w == "hot_mixed" and args.hot_low_rate and rate == min(rates) else rate) for w in workloads for rate in rates]
                    for repeat in range(args.repeats):
                        # Rotate case order; paired protocol runs retain identical request seeds.
                        order = cases[repeat % len(cases):] + cases[:repeat % len(cases)]
                        for workload, rate in order:
                            name = f"n{count}-{protocol}-{workload}-r{rate}-rep{repeat}"
                            mutations, active = WORKLOADS[workload]
                            spec = {**base, "mode": "run", "rate": rate, "mutation_percent": mutations,
                                    "active_keys": active or args.keys, "ingress": args.ingress,
                                    "seed": 7163 + repeat * 100 + workloads.index(workload),
                                    "warmup_seconds": args.warmup, "capture_resources": True, "run_id": len(rows)+1}
                            result = driver(bench, output, name, spec, config["nodes"])
                            assert result["settled"], "protocol work did not drain; inspect counters"
                            before_status, after_status = result["resources_before"], result["resources_after"]
                            before = [s["metrics"] for s in before_status]
                            after = [s["metrics"] for s in after_status]
                            cpu_before = {str(s["node_id"]):s["process"] for s in before_status}
                            cpu_after = {str(s["node_id"]):s["process"] for s in after_status}
                            result["server_metrics_delta"] = [{key: b[key]-a[key] for key in a} for a,b in zip(before,after)]
                            result["server_metrics_before"] = before
                            result["server_metrics_after"] = after
                            result["server_cpu_seconds"] = {pid: cpu_after[pid]["cpu_seconds"]-item["cpu_seconds"] for pid,item in cpu_before.items()}
                            result["server_peak_rss_bytes_after"] = {pid:item["peak_rss_bytes"] for pid,item in cpu_after.items()}
                            result["wal_file_bytes_after"] = sum(p.stat().st_size for p in root.glob("*/state.wal"))
                            result["case"] = {"nodes":count,"protocol":protocol,"workload":workload,"rate":rate,"repeat":repeat,"ingress":args.ingress,"peer_rtt_ms":args.peer_rtt_ms}
                            (output / f"{name}.summary.json").write_text(json.dumps(result, indent=2))
                            rows.append(result)
                            (output / "results.json").write_text(json.dumps(rows, indent=2))
                            qps=result["successful_completions_in_window"]/result["window_seconds"]
                            print(f"{name}: {qps:.0f} successful/s, {result['errors']} errors, {result['rejected']} generator rejects", flush=True)
                finally:
                    stop(processes)
                    if processes:
                        (output / f"processes-{count}-{protocol}.json").write_text(json.dumps({"active":False,"servers":[{"node":i+1,"pid":p.pid} for i,p in enumerate(processes)]},indent=2))
                    for sock in reservations:
                        sock.close()
    print(f"Saved {len(rows)} measured runs and raw compressed samples to {output}")


if __name__ == "__main__":
    main()
