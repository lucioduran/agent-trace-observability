import asyncio
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from io import BytesIO, StringIO
from pathlib import Path
from unittest.mock import patch

from agent_trace.__main__ import main
from agent_trace.experiment import run, verify
from agent_trace.pipeline import Classifier, Pipeline, retrieve, tokens
from agent_trace.telemetry import BoundedExporter, Telemetry


class PipelineTests(unittest.TestCase):
    def test_trained_classifier_and_retrieval(self):
        model = Classifier()
        self.assertEqual(model.predict("invoice refund payment"), "billing")
        self.assertEqual(model.predict("repair broken device"), "support")
        self.assertIn("Device", retrieve("device repair"))
        self.assertEqual(tokens("Hello, WORLD!"), ["hello", "world"])
        with self.assertRaises(ValueError):
            model.predict("123")

    def test_pipeline_success_faults_and_invalid_inputs(self):
        telemetry = Telemetry(BoundedExporter(BytesIO()))
        self.addCleanup(telemetry.close)
        pipeline = Pipeline(telemetry)
        self.assertEqual(asyncio.run(pipeline.run("invoice payment")), "billing-queue")
        self.assertEqual(asyncio.run(pipeline.run("device repair")), "support-queue")
        for fault, error in [
            ("retrieval", OSError),
            ("model", ValueError),
            ("tool", TimeoutError),
            ("invalid", ValueError),
        ]:
            with self.assertRaises(error):
                asyncio.run(pipeline.run("invoice", fault=fault))
        for query in ("1", "x" * 4097):
            with self.assertRaises(ValueError):
                asyncio.run(pipeline.run(query))

    def test_experiment_roundtrip_and_no_overwrite(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            report = run(output, 8, 2)
            self.assertEqual(
                report["outcomes"], {"success": 2, "retrieval": 2, "model": 2, "tool": 2}
            )
            self.assertEqual(verify(output), {"traces": 8, "spans": 26})
            self.assertIsNone(report["cost_usd"])
            with self.assertRaises(FileExistsError):
                run(output, 8, 2)
            (output / "traces.jsonl").write_bytes(b"tampered")
            with self.assertRaisesRegex(ValueError, "digest"):
                verify(output)
        for runs, concurrency in [(0, 1), (10001, 1), (1, 0), (1, 33)]:
            with self.assertRaises(ValueError):
                run(Path("never-created"), runs, concurrency)

    def test_report_corruption(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            original = run(output, 4, 1)
            for key, value in [
                ("fixture_sha256", "bad"),
                ("runs", 20),
                ("outcomes", {}),
                ("exporter", {"written": 0}),
            ]:
                report = dict(original)
                report[key] = value
                (output / "report.json").write_text(json.dumps(report))
                with self.assertRaises(ValueError):
                    verify(output)

    def test_cli_commands_and_safe_errors(self):
        with tempfile.TemporaryDirectory() as directory, redirect_stdout(StringIO()):
            output = str(Path(directory) / "run")
            for args in (
                ["run", "--output", output, "--runs", "4"],
                ["verify", "--output", output],
            ):
                with patch("sys.argv", ["agent_trace", *args]):
                    self.assertEqual(main(), 0)
            with patch("sys.argv", ["agent_trace", "run", "--output", output]):
                self.assertEqual(main(), 2)

    def test_rehashed_structural_corruption(self):
        import hashlib

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            original_report = run(output, 4, 1)
            original_rows = [
                json.loads(line) for line in (output / "traces.jsonl").read_text().splitlines()
            ]
            roots = [i for i, row in enumerate(original_rows) if row["component"] == "pipeline"]
            error_root = next(i for i in roots if original_rows[i]["status"] == "ERROR")
            changes = [
                (0, "span_id", original_rows[1]["span_id"]),
                (0, "parent_span_id", "0" * 16),
                (0, "component", "tool"),
                (roots[0], "status", "ERROR"),
                (error_root, "failure_cause", "internal"),
                (0, "duration_ns", -1),
                (0, "trace_id", "0" * 32),
            ]
            for index, key, value in changes:
                rows = [dict(row) for row in original_rows]
                rows[index][key] = value
                data = "".join(json.dumps(row) + "\n" for row in rows).encode()
                report = dict(original_report)
                report["trace_sha256"] = hashlib.sha256(data).hexdigest()
                (output / "traces.jsonl").write_bytes(data)
                (output / "report.json").write_text(json.dumps(report))
                with self.subTest(key=key), self.assertRaises(ValueError):
                    verify(output)
