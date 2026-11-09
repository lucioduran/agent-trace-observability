# Agent Trace Observability

Owned OpenTelemetry instrumentation and privacy-preserving local trace exports.

## Problem

Agent traces can leak prompts, credentials and exception messages. This project keeps useful operational metadata while dropping unapproved content at the export boundary.

## Scope

Actual OpenTelemetry spans for an owned local retrieval, trained text classifier and tool pipeline. No hosted inference or external collector is required.

## Quick start

Python 3.11 is required. Install with `python -m pip install -r requirements-dev.txt`.

## Usage

Run `python -m agent_trace run --output build/demo --runs 100`.

## Architecture

A dedicated provider sends spans through an allowlisted bounded JSONL exporter.

## Reproduction

Local runs preserve traces, summary and hardware metadata.

## Results

Measured evidence will be recorded in `results/`.

## Testing

Run `python -m unittest discover -v`.

## Limitations

The local classifier is not a language model. Cost and energy are unavailable. Raw SDK spans remain in process memory until export and must not be sent to other exporters.

## Contributing

Add focused tests for behavior and privacy boundaries. Use English and keep design notes outside code.

## License

[MIT](LICENSE).
