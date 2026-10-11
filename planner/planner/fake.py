"""FakePlanner: returns fixture plans so tests and the C++ side never need
network access or an API key."""

from __future__ import annotations

import copy
import json
from collections import deque
from pathlib import Path
from typing import Any, Iterable

from .llm import Planner
from .paths import FIXTURES_DIR
from .schema import ValidationIssue


def load_fixture_plan(name: str, fixtures_dir: Path = FIXTURES_DIR) -> dict:
    """Return the `plan` object of examples/plans/<name>.json."""
    doc = json.loads((fixtures_dir / f"{name}.json").read_text())
    return doc["plan"]


class FakePlanner(Planner):
    """Deterministic planner.

    * `script`: raw outputs returned in order, one per propose() call (lets
      tests drive the repair loop with an invalid first answer).
    * otherwise returns the `default` fixture plan.
    Every call is recorded in `calls` as (request, feedback).
    """

    def __init__(self, default: str | dict = "company_research", script: Iterable[Any] = ()):
        self.default = load_fixture_plan(default) if isinstance(default, str) else default
        self.script = deque(script)
        self.calls: list[tuple[str, list[ValidationIssue] | None]] = []

    def propose(self, request: str, feedback: list[ValidationIssue] | None = None) -> Any:
        self.calls.append((request, feedback))
        raw = self.script.popleft() if self.script else self.default
        return copy.deepcopy(raw)
