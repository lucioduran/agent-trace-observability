"""Reproducible local executions and saved evidence validation."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import platform
import statistics
import time
from collections import Counter, defaultdict
from importlib.metadata import version
from pathlib import Path

from agent_trace.pipeline import DOCUMENTS, TRAINING, Pipeline
from agent_trace.telemetry import BoundedExporter, Telemetry


def fingerprint() -> str:
    payload = json.dumps([TRAINING, DOCUMENTS], separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def run(output: Path, runs: int, concurrency: int) -> dict[str, object]:
    if not 1 <= runs <= 10_000 or not 1 <= concurrency <= 32:
        raise ValueError("Runs must be 1..10000 and concurrency 1..32")
    output.mkdir(parents=True, exist_ok=False)
    latencies: list[float] = []
    outcomes: Counter[str] = Counter()
    started = time.perf_counter()
    with (output / "traces.jsonl").open("wb") as sink:
        exporter = BoundedExporter(sink, max_records=runs * 4, max_bytes=runs * 3000)
        telemetry = Telemetry(exporter)
        pipeline = Pipeline(telemetry)

        async def execute() -> None:
            semaphore = asyncio.Semaphore(concurrency)

            async def request(index: int) -> None:
                async with semaphore:
                    fault = ("none", "retrieval", "model", "tool")[index % 4]
                    start = time.perf_counter()
                    try:
                        await pipeline.run("invoice payment receipt", fault=fault)
                    except (OSError, ValueError):
                        outcomes[fault] += 1
                    else:
                        outcomes["success"] += 1
                    latencies.append((time.perf_counter() - start) * 1000)

            await asyncio.gather(*(request(index) for index in range(runs)))

        try:
            asyncio.run(execute())
        finally:
            telemetry.close()
    elapsed = time.perf_counter() - started
    trace_bytes = (output / "traces.jsonl").read_bytes()
    report: dict[str, object] = {
        "schema_version": 1,
        "runs": runs,
        "concurrency": concurrency,
        "outcomes": dict(outcomes),
        "elapsed_seconds": elapsed,
        "latency_ms": {"median": statistics.median(latencies), "max": max(latencies)},
        "exporter": exporter.stats(),
        "trace_sha256": hashlib.sha256(trace_bytes).hexdigest(),
        "fixture_sha256": fingerprint(),
        "environment": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "logical_cpus": os.cpu_count(),
            "python": platform.python_version(),
            "opentelemetry_sdk": version("opentelemetry-sdk"),
            "thread_limit": os.environ.get("OMP_NUM_THREADS", "unset"),
        },
        "cost_usd": None,
        "energy_joules": None,
        "limitations": [
            "Local naive Bayes classifier, not LLM inference",
            "No baseline overhead comparison",
            "Cost and energy not measured",
            "Faults deliberately injected; timings measured locally",
        ],
    }
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    verify(output)
    return report


def verify(output: Path) -> dict[str, int]:
    report = json.loads((output / "report.json").read_text(encoding="utf-8"))
    data = (output / "traces.jsonl").read_bytes()
    if hashlib.sha256(data).hexdigest() != report["trace_sha256"]:
        raise ValueError("Trace digest mismatch")
    if report["fixture_sha256"] != fingerprint():
        raise ValueError("Fixture digest mismatch")
    rows = [json.loads(line) for line in data.splitlines()]
    groups = defaultdict(list)
    for row in rows:
        groups[row["trace_id"]].append(row)
    roots = {r["trace_id"]: r for r in rows if r["component"] == "pipeline"}
    if len(roots) != report["runs"]:
        raise ValueError("Root trace count mismatch")
    if len({r["span_id"] for r in rows}) != len(rows):
        raise ValueError("Duplicate span identifiers")
    expected: Counter[str] = Counter()
    for trace_id, root in roots.items():
        group = groups[trace_id]
        children = [r for r in group if r["component"] != "pipeline"]
        if root["parent_span_id"] is not None or any(
            r["parent_span_id"] != root["span_id"] for r in children
        ):
            raise ValueError("Parent linkage mismatch")
        failed = [r for r in children if r["status"] == "ERROR"]
        outcome = failed[0]["component"] if failed else "success"
        components = {
            "success": ["retrieval", "model", "tool"],
            "retrieval": ["retrieval"],
            "model": ["retrieval", "model"],
            "tool": ["retrieval", "model", "tool"],
        }[outcome]
        if [r["component"] for r in children] != components or len(failed) > 1:
            raise ValueError("Component sequence mismatch")
        if root["status"] != ("ERROR" if failed else "OK"):
            raise ValueError("Root status mismatch")
        if failed and root["failure_cause"] != failed[0]["failure_cause"]:
            raise ValueError("Failure cause mismatch")
        expected[outcome] += 1
    if any(r["trace_id"] not in roots or r["duration_ns"] < 0 for r in rows):
        raise ValueError("Orphan span or invalid duration")
    if dict(expected) != report["outcomes"]:
        raise ValueError("Outcome mismatch")
    stats = report["exporter"]
    if (
        stats["written"] != len(rows)
        or stats["bytes_written"] != len(data)
        or stats["dropped"]
        or stats["failures"]
    ):
        raise ValueError("Export accounting mismatch")
    return {"traces": len(roots), "spans": len(rows)}
