"""Checkpoint tests use only the Python standard library."""
import copy
import json
from pathlib import Path
import tempfile
import unittest

from benchmark import resume_batches


class ResumeTests(unittest.TestCase):
    def fixture(self, root):
        metadata = {"arguments": {"output": str(root), "keys": 20}, "binaries_sha256": {"server": "same", "driver": "same"}}
        (root / "environment.json").write_text(json.dumps(metadata))
        cases = {("mixed", 250, 0), ("mixed", 250, 1)}
        expected = {(3, "abd"): cases, (3, "gus"): cases}
        rows = []
        for protocol, reps in (("abd", (0, 1)), ("gus", (0,))):
            (root / f"processes-3-{protocol}.json").write_text('{"active":false}')
            for rep in reps:
                rows.append({"case": {"nodes": 3, "protocol": protocol, "workload": "mixed", "rate": 250, "repeat": rep}, "settled": True})
                (root / f"n3-{protocol}-mixed-r250-rep{rep}.samples.jsonl.gz").write_bytes(b"original raw evidence")
        (root / "preload-3-gus.driver.log").write_text("failure evidence")
        (root / "results.json").write_text(json.dumps(rows))
        return metadata, expected

    def test_keeps_complete_batches_and_archives_partial_cluster_without_mixing(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            metadata, expected = self.fixture(root)
            resumed = {**metadata, "arguments": {**metadata["arguments"], "resume": True}}
            rows, completed, source = resume_batches(root, resumed, expected)
            self.assertEqual(completed, {(3, "abd")})
            self.assertEqual(len(rows), 2)
            self.assertEqual(json.loads((root / "results.json").read_text()), rows)
            self.assertTrue((root / "n3-abd-mixed-r250-rep0.samples.jsonl.gz").exists())
            archived = source.parent / "aborted-3-gus"
            self.assertEqual((archived / "preload-3-gus.driver.log").read_text(), "failure evidence")
            self.assertEqual(len(json.loads((archived / "results.json").read_text())), 1)
            self.assertFalse((root / "n3-gus-mixed-r250-rep0.samples.jsonl.gz").exists())
            self.assertEqual(json.loads((root / "environment.json").read_text()), metadata)

    def test_rejects_changed_build_parameters_active_processes_and_missing_evidence(self):
        for change in ("build", "parameters", "active", "samples"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                metadata, expected = self.fixture(root)
                attempt = copy.deepcopy(metadata)
                if change == "build": attempt["binaries_sha256"]["server"] = "different"
                if change == "parameters": attempt["arguments"]["keys"] = 30
                if change == "active": (root / "processes-3-gus.json").write_text('{"active":true}')
                if change == "samples": (root / "n3-abd-mixed-r250-rep0.samples.jsonl.gz").unlink()
                before = (root / "results.json").read_bytes()
                with self.assertRaises(RuntimeError): resume_batches(root, attempt, expected)
                self.assertEqual((root / "results.json").read_bytes(), before)
                self.assertFalse((root / "resumes").exists())


if __name__ == "__main__":
    unittest.main()
