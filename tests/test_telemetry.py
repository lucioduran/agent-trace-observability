import asyncio
import io
import json
import unittest
from concurrent.futures import ThreadPoolExecutor

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.trace import Status, StatusCode

from agent_trace.telemetry import BoundedExporter, Telemetry, extract, failure_cause, inject

SECRET = "synthetic-credential-DO-NOT-EXPORT"


class BrokenSink(io.BytesIO):
    def write(self, data):
        raise OSError(SECRET)


class ShortSink(io.BytesIO):
    def write(self, data):
        super().write(data[:10])
        return 10


class FlushSink(io.BytesIO):
    def flush(self):
        raise OSError(SECRET)


class TelemetryTests(unittest.TestCase):
    def setUp(self):
        self.sink = io.BytesIO()
        self.exporter = BoundedExporter(self.sink)
        self.telemetry = Telemetry(self.exporter)
        self.addCleanup(self.telemetry.close)

    def records(self):
        return [json.loads(line) for line in self.sink.getvalue().splitlines()]

    def test_export_drops_all_unapproved_surfaces(self):
        nested = json.dumps({"auth": {"password": SECRET}, "items": [SECRET]})
        self.telemetry.provider._resource = Resource({"host.name": SECRET})
        with self.telemetry.tracer.start_as_current_span(SECRET) as span:
            for key in ("prompt", "authorization", "payload", SECRET, "component.version"):
                span.set_attribute(key, nested)
            span.set_attribute("failure.cause", SECRET)
            span.set_attribute("cost.usd", SECRET)
            span.add_event(SECRET, {"payload": nested})
            span.add_event("failure", {SECRET: nested})
            span.record_exception(ValueError(SECRET))
            span.set_status(Status(StatusCode.ERROR, SECRET))
        raw = self.sink.getvalue().decode()
        self.assertNotIn(SECRET, raw)
        row = self.records()[0]
        self.assertEqual(row["component"], "unknown")
        self.assertIsNone(row["failure_cause"])
        self.assertIsNone(row["cost_usd"])
        self.assertEqual(row["events"], ["failure"])
        self.assertEqual(row["status"], "ERROR")

    def test_error_propagates_without_message_or_stack(self):
        with self.assertRaisesRegex(TimeoutError, SECRET):
            with self.telemetry.span("tool"):
                raise TimeoutError(SECRET)
        row = self.records()[0]
        self.assertEqual(row["failure_cause"], "timeout")
        self.assertNotIn(SECRET, self.sink.getvalue().decode())
        self.assertEqual(row["events"], ["failure"])

    def test_numeric_cost_contract(self):
        for value in (0.125, 0, -1, float("nan"), float("inf"), True, 1_000_001, 10**400):
            with self.telemetry.span("model") as span:
                span.set_attribute("cost.usd", value)
        self.assertEqual(
            [r["cost_usd"] for r in self.records()], [0.125, 0, None, None, None, None, None, None]
        )

    def test_unknown_component_and_causes(self):
        with self.assertRaises(ValueError), self.telemetry.span(SECRET):
            pass
        for error, expected in [
            (ValueError(), "invalid_input"),
            (OSError(), "unavailable"),
            (RuntimeError(), "internal"),
            (KeyboardInterrupt(), "cancelled"),
        ]:
            self.assertEqual(failure_cause(error), expected)

    def test_traceparent_roundtrip_strips_baggage_and_tracestate(self):
        with self.telemetry.span("pipeline") as parent:
            carrier = inject()
            carrier.update({"baggage": SECRET, "tracestate": "vendor=" + SECRET})
            with self.telemetry.span("tool", context=extract(carrier)) as child:
                self.assertEqual(
                    child.get_span_context().trace_id, parent.get_span_context().trace_id
                )
                self.assertEqual(set(inject()), {"traceparent"})
        rows = self.records()
        self.assertEqual(rows[0]["parent_span_id"], rows[1]["span_id"])
        self.assertNotIn(SECRET, self.sink.getvalue().decode())
        for value in ("invalid", "x" * 129, "00-" + "0" * 32 + "-" + "0" * 16 + "-01", 4):
            self.assertFalse(
                trace.get_current_span(extract({"traceparent": value})).get_span_context().is_valid
            )

    def test_async_concurrency_and_thread_handoff(self):
        async def child():
            await asyncio.sleep(0)
            with self.telemetry.span("retrieval"):
                await asyncio.sleep(0)

        async def request():
            with self.telemetry.span("pipeline"):
                await asyncio.gather(child(), child())
                await asyncio.to_thread(lambda: self.thread_child())

        async def main():
            await asyncio.gather(*(request() for _ in range(8)))

        asyncio.run(main())
        rows = self.records()
        roots = {r["trace_id"]: r["span_id"] for r in rows if r["component"] == "pipeline"}
        self.assertEqual(len(roots), 8)
        self.assertEqual(len(rows), 32)
        for row in rows:
            if row["component"] != "pipeline":
                self.assertEqual(row["parent_span_id"], roots[row["trace_id"]])

    def thread_child(self):
        with self.telemetry.span("tool"):
            pass

    def test_cancelled_task_records_failure(self):
        async def main():
            with self.telemetry.span("pipeline"):
                raise asyncio.CancelledError(SECRET)

        with self.assertRaises(asyncio.CancelledError):
            asyncio.run(main())
        self.assertEqual(self.records()[0]["failure_cause"], "cancelled")

    def test_lifetime_caps_and_shutdown(self):
        for limits in ({"max_records": 2}, {"max_bytes": 1}):
            sink = io.BytesIO()
            exporter = BoundedExporter(sink, **limits)
            telemetry = Telemetry(exporter)
            with ThreadPoolExecutor(max_workers=3) as pool:
                list(pool.map(lambda _, telemetry=telemetry: self.emit(telemetry), range(12)))
            self.assertLessEqual(exporter.bytes_written, exporter.max_bytes)
            self.assertLessEqual(exporter.written, exporter.max_records)
            self.assertEqual(exporter.dropped + exporter.written, 12)
            self.assertTrue(exporter.force_flush())
            telemetry.close()
            self.emit(telemetry)
            self.assertEqual(exporter.dropped + exporter.written, 13)
        for limits in (
            {"max_records": 0},
            {"max_bytes": -1},
            {"max_records": True},
            {"max_bytes": float("inf")},
        ):
            with self.assertRaises(ValueError):
                BoundedExporter(io.BytesIO(), **limits)

    def emit(self, telemetry):
        with telemetry.span("tool"):
            pass

    def test_sink_failures_are_contained_without_retry_or_logging(self):
        for sink in (BrokenSink(), ShortSink(), FlushSink()):
            exporter = BoundedExporter(sink)
            telemetry = Telemetry(exporter)
            with self.assertNoLogs(level="WARNING"):
                self.emit(telemetry)
                self.emit(telemetry)
            self.assertEqual(exporter.stats()["failures"], 1)
            self.assertEqual(exporter.stats()["dropped"], 2)
            self.assertFalse(exporter.force_flush())
            telemetry.close()
