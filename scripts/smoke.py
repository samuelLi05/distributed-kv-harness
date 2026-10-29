#!/usr/bin/env python3
"""Real HTTP/process/crash validation using only the Python standard library. Not a benchmark."""
import argparse
import http.client
import json
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time


def request(port, method, path, body=None):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        encoded = None if body is None else json.dumps(body).encode()
        connection.request(method, path, body=encoded, headers={"Content-Type": "application/json"})
        response = connection.getresponse()
        data = response.read()
        return response.status, json.loads(data) if data else None
    finally:
        connection.close()


def stable_hash(key):
    value = 0xCBF29CE484222325
    for byte in key.encode():
        value = ((value ^ byte) * 0x100000001B3) & ((1 << 64) - 1)
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("target/debug/distributed-kv-harness"))
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    processes = {}
    reservations = []
    with tempfile.TemporaryDirectory(prefix="distributed-kv-smoke-") as temporary:
        root = Path(temporary)
        try:
            ports = []
            for _ in range(6):
                reservation = socket.socket()
                reservation.bind(("127.0.0.1", 0))
                ports.append(reservation.getsockname()[1])
                reservations.append(reservation)
            config = {
                "cluster_id": "process-smoke-v1", "coordinator": "local",
                "nodes": [
                    {"id": i + 1, "http": f"127.0.0.1:{ports[2*i]}", "peer": f"127.0.0.1:{ports[2*i+1]}"}
                    for i in range(3)
                ],
                "shards": [[1], [2], [3], [1], [2], [3]], "rpc_timeout_ms": 2000,
            }
            config_path = root / "cluster.json"
            config_path.write_text(json.dumps(config))

            def start(index):
                for reservation in reservations[2*index:2*index+2]:
                    reservation.close()
                with (root / f"node-{index+1}.log").open("ab") as log:
                    processes[index] = subprocess.Popen(
                        [str(binary), "--config", str(config_path), "--node", str(index+1), "--data-dir", str(root / f"node-{index+1}")],
                        stdout=log, stderr=subprocess.STDOUT,
                    )

            def ready(index):
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    process = processes[index]
                    if process.poll() is not None:
                        raise AssertionError((root / f"node-{index+1}.log").read_text())
                    try:
                        status, body = request(ports[2*index], "GET", "/__harness/status")
                        if status == 200:
                            assert body["guarantee"] == "unreplicated_single_owner"
                            return
                    except (OSError, http.client.HTTPException):
                        pass
                    time.sleep(0.02)
                raise AssertionError(f"node {index+1} did not become ready")

            def kill(index):
                processes[index].kill()
                processes[index].wait(timeout=5)

            for index in range(3):
                start(index)
            for index in range(3):
                ready(index)

            expected = {f"process-key-{i}": {"index": i, "nested": {"ok": True}} for i in range(30)}
            for i, (key, value) in enumerate(expected.items()):
                assert request(ports[2*(i % 3)], "PUT", f"/{key}", value)[0] == 201
            for key, value in expected.items():
                for ingress in range(3):
                    assert request(ports[2*ingress], "GET", f"/{key}") == (200, value)

            owned_by_two = [key for key in expected if config["shards"][stable_hash(key) % len(config["shards"])][0] == 2]
            assert len(owned_by_two) >= 2
            kept, deleted = owned_by_two[:2]
            assert request(ports[0], "DELETE", f"/{deleted}")[0] == 204
            del expected[deleted]
            kill(1)  # Abrupt process loss, without graceful shutdown or store.close().
            assert request(ports[0], "GET", f"/{kept}")[0] == 503
            start(1)
            ready(1)
            assert request(ports[0], "GET", f"/{kept}") == (200, expected[kept])
            assert request(ports[0], "GET", f"/{deleted}")[0] == 404

            for index in range(3):
                kill(index)
            for index in range(3):
                start(index)
            for index in range(3):
                ready(index)
            for key, value in expected.items():
                for ingress in range(3):
                    assert request(ports[2*ingress], "GET", f"/{key}") == (200, value)
            assert request(ports[4], "GET", f"/{deleted}")[0] == 404
            print("PASS: 3 real processes; 30 keys through all HTTP entry points; owner loss returns 503; SIGKILL/restart and whole-cluster restart preserve acknowledged values and deletion.")
        except BaseException:
            for log in root.glob("*.log"):
                print(f"--- {log.name} ---\n{log.read_text()}")
            raise
        finally:
            for reservation in reservations:
                reservation.close()
            for process in processes.values():
                if process.poll() is None:
                    process.send_signal(signal.SIGINT)
            for process in processes.values():
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)


if __name__ == "__main__":
    main()
