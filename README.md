# Agent Trace Observability

[![CI](https://github.com/lucioduran/agent-trace-observability/actions/workflows/ci.yml/badge.svg)](https://github.com/lucioduran/agent-trace-observability/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Inspect an agent pipeline's latency, versions, failures and trace relationships without exporting its prompts or exception messages.

## Problem

Useful agent traces can also expose credentials, retrieved documents and tool payloads. Redacting only prompt attributes leaves other paths, such as exception events and status descriptions. This project places a narrow, tested export boundary around an owned pipeline.

## Scope

Project 20 of the engineering evidence series. Actual OpenTelemetry Python spans instrument local token-overlap retrieval, a trained naive Bayes classifier and a catalog tool. An owned JSONL exporter preserves approved operational metadata and discards everything else. The pipeline, training examples, fault injection and exporter are included under MIT.

No account, API key, hosted inference, GPU or paid service is required. This is a local classifier workload, not LLM inference or an OTLP collector integration.

## Quick start

Use Python 3.11 and a fresh virtual environment:

```sh
git clone https://github.com/lucioduran/agent-trace-observability.git
cd agent-trace-observability
python -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements-dev.txt
python -m agent_trace verify --output results/local
```

On Windows activate with `.venv\Scripts\Activate.ps1`. Verification reads the committed evidence without running a benchmark. Expected result: 400 traces and 1,300 spans.

## Usage

```sh
OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 \
  python -m agent_trace run --output build/demo --runs 100 --concurrency 4
python -m agent_trace verify --output build/demo
```

Every run cycles through success, retrieval failure, model failure and tool timeout. The destination must be new. `traces.jsonl` contains parent-linked records, while `report.json` contains timings, outcomes, versions, digests and export accounting. Exit code 0 indicates success; 2 indicates invalid input or evidence failure. A failed write can leave incomplete files, which are not valid evidence.

Use the instrumentation independently:

```python
from agent_trace.telemetry import BoundedExporter, Telemetry, extract, inject

with open("trace.jsonl", "wb") as sink:
    telemetry = Telemetry(BoundedExporter(sink, max_records=1000, max_bytes=1_000_000))
    try:
        with telemetry.span("pipeline"):
            carrier = inject()
            with telemetry.span("tool", context=extract(carrier)):
                result = {"ready": True}
    finally:
        telemetry.close()
```

Components are `pipeline`, `retrieval`, `model` and `tool`. Exceptions retain their original behavior for the caller, while exports contain only a fixed failure category. `inject` and `extract` transport W3C trace IDs without baggage or tracestate. See the [export contract](docs/design.md) before adapting the provider.

## Architecture

```text
Query -> retrieval -> model -> tool
             |          |       |
             +-- OpenTelemetry spans --+
                                       |
                         allowlisted record projection
                                       |
                         bounded local JSONL exporter
```

The dedicated SDK provider does not change global telemetry configuration. Context variables preserve async parentage; thread handoff uses `asyncio.to_thread`. A fresh record projection excludes arbitrary span names, payloads, event attributes, exceptions, status descriptions, resources, links and scope metadata. Approved component versions, duration, status, fixed error categories and trace IDs remain correlated. Optional numeric cost is marked caller-reported; the local run has no cost or energy measurement.

The synchronous exporter has lifetime record and byte limits, no retry buffer, and a permanent failure circuit after a sink error. The [design notes](docs/design.md) cover privacy assumptions, sink behavior and propagation boundaries.

## Reproduction

See [exact commands, hardware and methodology](docs/reproduction.md). The recorded run used the shared exclusive benchmark lock, four concurrent requests and native thread limits of one. CI only verifies committed evidence and runs small tests. Reproduction produces different IDs and timings; the corpus and trained model recipe are deterministic and fingerprinted.

## Results

[Raw traces](results/local/traces.jsonl) and [machine-readable report](results/local/report.json) come from 400 real local executions on an Intel Xeon W-3223, 48 GiB RAM, macOS 26.7.1, Python 3.11.13 and OpenTelemetry SDK 1.39.1.

| Measurement | Recorded result |
| --- | ---: |
| Requests / concurrency | 400 / 4 |
| Successful requests | 100 |
| Injected retrieval / model / tool failures | 100 / 100 / 100 |
| Exported spans | 1,300 |
| Dropped spans / exporter failures | 0 / 0 |
| Confirmed exported bytes | 463,011 |
| Total execution time | 0.146 s |
| Median / maximum request latency | 1.388 / 4.380 ms |
| Cost / energy | Unavailable / unavailable |

Failures are synthetic; execution and timings are real. This single short run demonstrates trace linkage and failure attribution. It does not establish production throughput, model accuracy or instrumentation overhead.

## Testing

```sh
python -m ruff check .
python -m ruff format --check .
python -m mypy
python -m coverage run -m unittest discover -v
python -m coverage report
python -m agent_trace verify --output results/local
python -m build
```

The 15 tests cover classification, retrieval, all fault modes, nested payload exclusion, attributes, events, exceptions, async cancellation, concurrent context isolation, thread handoff, W3C propagation, cost validation, byte/record bounds, short writes, write/flush failures, CLI errors and tampered evidence. Local branch-aware coverage is 99%, with a 90% floor. CI runs on Ubuntu 24.04, macOS 14 and Windows 2022 with Python 3.11.9 and pinned action commits.

## Limitations

The exporter protects its output, not process memory, application logs, third-party exporters or malicious application code. Dropping payloads deliberately sacrifices content debugging. Trace IDs are correlation hints, not authenticated identity. A slow sink can block a caller; no hard I/O timeout, remote delivery, retry, rotation or disk durability guarantee is provided. Partial writes may leave an incomplete final line.

Only the owned pipeline and local JSONL sink are exercised. No external collector interoperability, independent security review, human review study, energy measurement, billing validation or comparative overhead benchmark is claimed. [Further boundaries](docs/design.md) are part of the contract.

## Contributing

Keep changes focused and add meaningful regression tests for every behavior. Extend the fixed export contract deliberately; do not add raw payloads or exception text. Run the checks above. Put design explanations in `docs/`, keep code comments sparse, and use English throughout.

## License

[MIT](LICENSE), including the original local training and retrieval fixtures.
