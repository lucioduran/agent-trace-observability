"""Command-line entry point."""

import argparse
import json
from pathlib import Path

from agent_trace.experiment import run, verify


def main() -> int:
    parser = argparse.ArgumentParser(description="Run and verify owned local agent traces")
    sub = parser.add_subparsers(dest="command", required=True)
    execute = sub.add_parser("run")
    execute.add_argument("--output", type=Path, required=True)
    execute.add_argument("--runs", type=int, default=100)
    execute.add_argument("--concurrency", type=int, default=4)
    check = sub.add_parser("verify")
    check.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = (
            run(args.output, args.runs, args.concurrency)
            if args.command == "run"
            else verify(args.output)
        )
    except (OSError, ValueError, KeyError, TypeError):
        print(json.dumps({"error": "Run or evidence validation failed"}))
        return 2
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
