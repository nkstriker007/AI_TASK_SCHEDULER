"""Benchmark workloads (design doc section 11), written as validated plan files.

    python -m bench.gen                     # -> bench/workloads/*.json

  chain-10      10 tasks in a chain              mostly sequential: parallelism should not help
  fanout-32     32 independent tasks -> 1 join   highly parallel: near-linear scaling up to the cap
  layered-24    random DAG, 6 levels x 4, skewed moderate parallelism; where policies differ
                durations, -> 1 join
  straggler-16  16 parallel tasks, one 10x slower tail latency
  tiny-48       48 tasks of 10 ms -> 1 join     where overhead dominates

The design doc's tiny-200 has 200 tasks, but a plan holds at most 50 (contract #1), so the
tiny workload has 48 (plus the join). E3 sweeps task duration separately (fanout()).
Durations are estimated_seconds hints: ats_sim uses them as is and the sim executor sleeps
for them times SIM_TIME_SCALE, so one plan serves both the simulator and the real runs.
Everything is deterministic (seeded).
"""

from __future__ import annotations

import argparse
import json
import random
from pathlib import Path
from typing import Any

from planner.validate import validate_plan_file

WORKLOADS_DIR = Path(__file__).resolve().parents[1] / "workloads"
CREATED_AT = "2026-11-03T00:00:00Z"


def _task(n: int, deps: list[int], seconds: float, type_: str = "research") -> dict[str, Any]:
    return {"id": f"t{n}", "type": type_, "title": f"{type_} {n}", "instruction": f"Synthetic {type_} task {n}.",
            "depends_on": [f"t{d}" for d in deps], "hints": {"estimated_seconds": round(seconds, 4)}}


def _plan_file(name: str, summary: str, tasks: list[dict[str, Any]]) -> dict[str, Any]:
    doc = {"request_id": f"bench_{name}", "created_at": CREATED_AT,
           "plan": {"schema_version": 1, "summary": summary, "tasks": tasks}}
    result = validate_plan_file(doc)
    if not result.ok:
        raise ValueError(f"{name} is not a valid plan: {[str(i) for i in result.errors]}")
    return doc


def chain(n: int = 10, seconds: float = 2.0) -> dict[str, Any]:
    tasks = [_task(i, [i - 1] if i > 1 else [], seconds) for i in range(1, n + 1)]
    return _plan_file(f"chain-{n}", f"{n} tasks in a chain", tasks)


def fanout(n: int = 32, seconds: float = 2.0, join_seconds: float | None = None, name: str | None = None) -> dict[str, Any]:
    tasks = [_task(i, [], seconds) for i in range(1, n + 1)]
    tasks.append(_task(n + 1, list(range(1, n + 1)), seconds if join_seconds is None else join_seconds, "report"))
    return _plan_file(name or f"fanout-{n}", f"{n} independent tasks and a join", tasks)


def layered(levels: int = 6, width: int = 4, seed: int = 42) -> dict[str, Any]:
    """Each task depends on 1-2 tasks of the level above; durations are skewed (log-normal),
    so a few long tasks sit on the critical path, which is where critical-path priority helps."""
    rng = random.Random(seed)
    tasks, prev, n = [], [], 0
    for _ in range(levels):
        level = []
        for _ in range(width):
            n += 1
            deps = sorted(rng.sample(prev, k=min(len(prev), rng.choice([1, 2])))) if prev else []
            seconds = min(12.0, max(0.5, rng.lognormvariate(0.6, 0.9)))
            tasks.append(_task(n, deps, seconds))
            level.append(n)
        prev = level
    tasks.append(_task(n + 1, prev, 2.0, "report"))
    return _plan_file(f"layered-{levels * width}", f"random DAG, {levels} levels of {width}, skewed durations", tasks)


def straggler(n: int = 16, seconds: float = 2.0, slow_factor: float = 10.0) -> dict[str, Any]:
    tasks = [_task(i, [], seconds * (slow_factor if i == n else 1.0)) for i in range(1, n + 1)]
    tasks.append(_task(n + 1, list(range(1, n + 1)), seconds, "report"))
    return _plan_file(f"straggler-{n}", f"{n} parallel tasks, one {slow_factor:g}x slower", tasks)


def tiny(n: int = 48, seconds: float = 0.01) -> dict[str, Any]:
    return fanout(n, seconds, join_seconds=seconds, name=f"tiny-{n}")


def workloads() -> dict[str, dict[str, Any]]:
    docs = [chain(), fanout(), layered(), straggler(), tiny()]
    return {d["request_id"].removeprefix("bench_"): d for d in docs}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", type=Path, default=WORKLOADS_DIR)
    a = p.parse_args(argv)
    a.out.mkdir(parents=True, exist_ok=True)
    for name, doc in workloads().items():
        path = a.out / f"{name}.json"
        path.write_text(json.dumps(doc, indent=2) + "\n")
        print(f"wrote {path} ({len(doc['plan']['tasks'])} tasks)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
