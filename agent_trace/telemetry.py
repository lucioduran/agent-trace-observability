"""Allowlisted export boundary and owned tracing lifecycle."""

from __future__ import annotations

import json
import math
import threading
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from typing import BinaryIO

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan, SpanLimits, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, SpanExporter, SpanExportResult
from opentelemetry.trace import NonRecordingSpan, SpanContext, Status, StatusCode, TraceState
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

VERSIONS = {
    "pipeline": "1",
    "retrieval": "token-overlap-v1",
    "model": "naive-bayes-v1",
    "tool": "catalog-v1",
}
CAUSES = {"timeout", "invalid_input", "unavailable", "internal", "cancelled"}


def failure_cause(error: BaseException) -> str:
    if isinstance(error, TimeoutError):
        return "timeout"
    if isinstance(error, ValueError):
        return "invalid_input"
    if isinstance(error, OSError):
        return "unavailable"
    if not isinstance(error, Exception):
        return "cancelled"
    return "internal"


def project(span: ReadableSpan) -> dict[str, object]:
    """Build a fresh record; never serialize an SDK span or caller payload."""
    component = span.name if span.name in VERSIONS else "unknown"
    attrs = span.attributes or {}
    cause = attrs.get("failure.cause")
    context = span.context
    cost = attrs.get("cost.usd")
    valid_cost = (
        isinstance(cost, (int, float))
        and not isinstance(cost, bool)
        and math.isfinite(cost)
        and 0 <= cost <= 1_000_000
    )
    return {
        "schema_version": 1,
        "trace_id": f"{context.trace_id:032x}" if context else None,
        "span_id": f"{context.span_id:016x}" if context else None,
        "parent_span_id": f"{span.parent.span_id:016x}" if span.parent else None,
        "component": component,
        "component_version": VERSIONS.get(component),
        "start_ns": span.start_time,
        "duration_ns": max(0, (span.end_time or 0) - (span.start_time or 0)),
        "status": span.status.status_code.name,
        "failure_cause": cause if isinstance(cause, str) and cause in CAUSES else None,
        "cost_usd": cost if valid_cost else None,
        "cost_source": "caller_reported" if valid_cost else "unavailable",
        "energy_joules": None,
        "events": ["failure" for event in span.events if event.name == "failure"][:8],
    }


class BoundedExporter(SpanExporter):
    """Synchronous sink with lifetime record/byte limits and no retry buffer."""

    def __init__(self, sink: BinaryIO, *, max_records: int = 10_000, max_bytes: int = 8_000_000):
        if max_records < 1 or max_bytes < 1:
            raise ValueError("Export limits must be positive")
        self.sink = sink
        self.max_records = max_records
        self.max_bytes = max_bytes
        self.written = 0
        self.bytes_written = 0
        self.dropped = 0
        self.failures = 0
        self.closed = False
        self.failed = False
        self._lock = threading.Lock()

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        with self._lock:
            result = SpanExportResult.SUCCESS
            for span in spans:
                if self.closed or self.failed or self.written >= self.max_records:
                    self.dropped += 1
                    result = SpanExportResult.FAILURE
                    continue
                data = (
                    json.dumps(project(span), separators=(",", ":"), allow_nan=False) + "\n"
                ).encode()
                if self.bytes_written + len(data) > self.max_bytes:
                    self.dropped += 1
                    result = SpanExportResult.FAILURE
                    continue
                try:
                    count = self.sink.write(data)
                    if count != len(data):
                        raise OSError("Incomplete trace write")
                    self.sink.flush()
                except Exception:
                    self.failures += 1
                    self.dropped += 1
                    self.failed = True
                    result = SpanExportResult.FAILURE
                    continue
                self.written += 1
                self.bytes_written += len(data)
            return result

    def shutdown(self) -> None:
        with self._lock:
            self.closed = True

    def force_flush(self, timeout_millis: int = 30_000) -> bool:
        with self._lock:
            return not self.failed

    def stats(self) -> dict[str, int | bool]:
        with self._lock:
            return {
                "written": self.written,
                "bytes_written": self.bytes_written,
                "dropped": self.dropped,
                "failures": self.failures,
                "failed": self.failed,
            }


class Telemetry:
    def __init__(self, exporter: BoundedExporter):
        self.exporter = exporter
        self.provider = TracerProvider(
            resource=Resource({"service.name": "owned-agent-pipeline"}),
            span_limits=SpanLimits(
                max_attributes=16,
                max_events=8,
                max_links=0,
                max_attribute_length=128,
                max_event_attributes=4,
            ),
            shutdown_on_exit=False,
        )
        self.provider.add_span_processor(SimpleSpanProcessor(exporter))
        self.tracer = self.provider.get_tracer("agent_trace", "0.1.0")

    @contextmanager
    def span(self, component: str, *, context: Context | None = None) -> Iterator[trace.Span]:
        if component not in VERSIONS:
            raise ValueError("Unknown component")
        with self.tracer.start_as_current_span(
            component,
            context=context,
            record_exception=False,
            set_status_on_exception=False,
        ) as span:
            try:
                yield span
            except BaseException as error:
                span.set_attribute("failure.cause", failure_cause(error))
                span.add_event("failure")
                span.set_status(Status(StatusCode.ERROR))
                raise
            else:
                span.set_status(Status(StatusCode.OK))

    def close(self) -> None:
        self.provider.shutdown()


def inject() -> dict[str, str]:
    carrier: dict[str, str] = {}
    TraceContextTextMapPropagator().inject(carrier)
    return {key: value for key, value in carrier.items() if key == "traceparent"}


def extract(carrier: Mapping[str, str]) -> Context:
    value = carrier.get("traceparent", "")
    if not isinstance(value, str) or len(value) > 128:
        return Context()
    parsed = TraceContextTextMapPropagator().extract({"traceparent": value}, context=Context())
    parent = trace.get_current_span(parsed).get_span_context()
    if not parent.is_valid:
        return Context()
    clean = SpanContext(parent.trace_id, parent.span_id, True, parent.trace_flags, TraceState())
    return trace.set_span_in_context(NonRecordingSpan(clean), Context())
