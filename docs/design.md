# Export boundary and operating contract

The owned provider uses the real OpenTelemetry Python SDK, `SimpleSpanProcessor`
and a custom `SpanExporter`. It does not install a global provider. Span lifetimes,
parentage, IDs, timestamps, status and context propagation come from the SDK.
The local JSONL format is project-specific, not OTLP. No collector compatibility
or third-party auto-instrumentation integration has been tested.

## Data flow

```text
local query -> retrieval -> trained naive Bayes model -> catalog tool
                    OpenTelemetry spans and context
                                |
                  fresh allowlisted record projection
                                |
                 locked, bounded synchronous JSONL sink
```

The model trains token counts from four original MIT-licensed examples at startup.
Laplace-smoothed token likelihoods select one of two classes with deterministic
lexical tie breaking. Retrieval selects one of three owned documents by token
intersection. The tool performs a real local catalog lookup. This is a small
classification workload, not an LLM or a model-quality evaluation.

## Privacy boundary

`project()` creates a fresh record. No general span serializer, object `repr`,
exception formatter or payload walker is used. Unknown attributes and arbitrary
nested content are discarded as a whole, including JSON encoded as strings.
The same rule covers event attributes, exception messages and stack traces,
status descriptions, resources, links, scope names and tracestate. Unknown span
names become `unknown`. Only the fixed `failure` event name is retained, at most
eight times. Component versions come from the owned registry, never caller text.
Failure causes are fixed categories. Numeric cost must be finite, nonnegative,
not boolean and at most one million USD; it is explicitly caller-reported.
The local pipeline does not supply a cost, so cost and energy are null.

Automatic exception recording is disabled in the facade. A failing operation
sets a fixed error category and rethrows the original exception to its caller.
That caller is responsible for its own logs. Raw third-party SDK spans may still
hold sensitive values in memory before projection. Do not attach additional
exporters or use this as a general process-wide secret scrubber. SDK warnings,
application logs, process memory, crash dumps and malicious application code are
outside the export guarantee. Numeric metadata and trace identifiers are not a
covert-channel defense. Caller-reported costs are not billing measurements.

## Context

Asyncio tasks inherit context using the SDK's context variables. `asyncio.to_thread`
propagates it into model execution. W3C `traceparent` is the only transmitted
header; baggage and tracestate are excluded. Extraction starts from a fresh
context and rejects invalid or overlong headers. External trace IDs are
unauthenticated correlation hints, not authorization. Callers must decide when
to trust incoming parent context; ordinary `ThreadPoolExecutor` submissions need
an explicit carrier or copied context.

## Bounds and failure behavior

The owned provider caps attributes at 16, events at 8, links at zero, and attribute
string length at 128. The exporter serializes and writes one record at a time
under a lock. It has lifetime byte and record caps, no retry queue and no retention
buffer. Limits do not rotate: create a new owned sink for a new export lifetime.
When full, it drops records and returns FAILURE. A write or flush error trips a
permanent circuit, increments failure/drop counters and suppresses exception text.
Further spans are counted as dropped. Short writes are failures. A partial final
line can remain on disk; report verification rejects incomplete evidence.
`bytes_written` counts confirmed complete writes, not partially persisted bytes.
The sink remains owned by its caller and must be closed after provider shutdown.

This synchronous design bounds buffered export memory, not application latency.
A blocked or slow sink can block the caller; no hard I/O timeout or durability
beyond `flush` is claimed. Use a local file or bounded nonblocking adapter. There
is no `fsync`, rotation, retry, remote delivery or exactly-once guarantee.

## Evidence

The saved run executes four alternating cases: success, retrieval unavailable,
model invalid input, and tool timeout. Wall-clock timings are measured, while
faults are deliberately injected. The verifier checks digests, fixture identity,
parent linkage, unique span IDs, expected component sequence, root status, failure
cause agreement and exporter accounting. Digests detect accidental changes, not
malicious replacement of both report and trace. It does not recompute historical
latencies, certify absence of arbitrary secrets, or validate external traces.

## References

- [OpenTelemetry Python instrumentation](https://opentelemetry.io/docs/languages/python/instrumentation/)
- [OpenTelemetry exception specification](https://opentelemetry.io/docs/specs/otel/trace/exceptions/)
- [Python SDK tracing API](https://opentelemetry-python.readthedocs.io/en/latest/sdk/trace.html)
- [GitHub Python setup and platform availability](https://github.com/actions/setup-python/blob/main/docs/advanced-usage.md)

SDK and dependencies are pinned in `requirements-dev.txt`. CI pins action commits
and uses Ubuntu 24.04, macOS 15 and Windows 2022 with Python 3.11.9, avoiding the
Ubuntu 26.04 `ubuntu-latest` migration. Local evidence uses Python 3.11.13.
