"""Small owned retrieval, classification and tool execution pipeline."""

from __future__ import annotations

import asyncio
import math
import re
from collections import Counter

from agent_trace.telemetry import Telemetry

TRAINING = (
    ("billing", "invoice payment charge refund account"),
    ("billing", "refund receipt payment billing invoice"),
    ("support", "repair broken device help support"),
    ("support", "device error troubleshooting repair help"),
)
DOCUMENTS = (
    "Invoice payment receipts are available from billing.",
    "Device repair and troubleshooting are available from support.",
    "Refund requests need a receipt and account reference.",
)
CATALOG = {"billing": "billing-queue", "support": "support-queue"}
FAULTS = ("none", "retrieval", "model", "tool")


def tokens(text: str) -> list[str]:
    return re.findall(r"[a-z]+", text.lower())


class Classifier:
    def __init__(self) -> None:
        self.counts: dict[str, Counter[str]] = {}
        for label, text in TRAINING:
            self.counts.setdefault(label, Counter()).update(tokens(text))
        self.vocabulary = set().union(*(set(counts) for counts in self.counts.values()))

    def predict(self, text: str) -> str:
        words = tokens(text)
        if not words:
            raise ValueError("Input has no words")
        scores = {
            label: sum(
                math.log((counts[word] + 1) / (counts.total() + len(self.vocabulary)))
                for word in words
            )
            for label, counts in self.counts.items()
        }
        return max(sorted(scores), key=scores.__getitem__)


def retrieve(query: str) -> str:
    words = set(tokens(query))
    return max(DOCUMENTS, key=lambda document: len(words & set(tokens(document))))


class Pipeline:
    def __init__(self, telemetry: Telemetry):
        self.telemetry = telemetry
        self.model = Classifier()

    async def run(self, query: str, *, fault: str = "none") -> str:
        if fault not in FAULTS:
            raise ValueError("Unknown fault")
        if len(query) > 4096:
            raise ValueError("Query exceeds input limit")
        with self.telemetry.span("pipeline"):
            with self.telemetry.span("retrieval"):
                await asyncio.sleep(0)
                if fault == "retrieval":
                    raise OSError("Injected retrieval failure")
                document = retrieve(query)
            with self.telemetry.span("model"):
                if fault == "model":
                    raise ValueError("Injected model failure")
                if not tokens(query):
                    raise ValueError("Input has no words")
                label = await asyncio.to_thread(self.model.predict, query + " " + document)
            with self.telemetry.span("tool"):
                await asyncio.sleep(0)
                if fault == "tool":
                    raise TimeoutError("Injected tool timeout")
                return CATALOG[label]
