"""Planner evaluation: structural assertions over generated plans (design doc section 6).

    python -m planner.eval                         # every case against Groq (one call each, plus repairs)
    python -m planner.eval --case trivial          # one case
    python -m planner.eval --fake company_research # offline plumbing check, no API key
    python -m planner.eval --json results.json     # also write a machine-readable report

Titles and wording vary between runs, so plans are never compared literally;
each case asserts on width, depth, sinks, task counts and independence.
Exit code: 0 if every case passes, 1 otherwise.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import yaml

from .llm import PlanRejected, Planner
from .paths import EVAL_CASES
from .schema import ExecutionPlan

ASSERTIONS = {
    "min_tasks", "max_tasks", "min_width", "max_width", "min_depth", "max_depth",
    "single_sink", "sink_type", "independent", "forbid_types",
}


# --- plan metrics --------------------------------------------------------------


@dataclass
class PlanMetrics:
    tasks: int
    width: int  # most tasks on one level
    depth: int  # number of levels
    sinks: list[str]
    types: dict[str, str]  # task id -> type


def _parents(plan: ExecutionPlan) -> dict[str, list[str]]:
    return {t.id: list(t.depends_on) for t in plan.tasks}


def levels(plan: ExecutionPlan) -> dict[str, int]:
    """Level of each task: length of the longest path from a root (roots are 0).
    The plan is validated, so it is acyclic."""
    parents = _parents(plan)
    memo: dict[str, int] = {}

    def level(tid: str) -> int:
        if tid not in memo:
            memo[tid] = 1 + max((level(p) for p in parents[tid]), default=-1)
        return memo[tid]

    return {tid: level(tid) for tid in parents}


def plan_metrics(plan: ExecutionPlan) -> PlanMetrics:
    lv = levels(plan)
    per_level: dict[int, int] = {}
    for n in lv.values():
        per_level[n] = per_level.get(n, 0) + 1
    has_children = {p for t in plan.tasks for p in t.depends_on}
    return PlanMetrics(
        tasks=len(plan.tasks),
        width=max(per_level.values()),
        depth=len(per_level),
        sinks=[t.id for t in plan.tasks if t.id not in has_children],
        types={t.id: t.type.value for t in plan.tasks},
    )


def _ancestors(plan: ExecutionPlan) -> dict[str, set[str]]:
    parents = _parents(plan)
    memo: dict[str, set[str]] = {}

    def anc(tid: str) -> set[str]:
        if tid not in memo:
            memo[tid] = set()
            for p in parents[tid]:
                memo[tid] |= {p} | anc(p)
        return memo[tid]

    return {tid: anc(tid) for tid in parents}


def max_independent(plan: ExecutionPlan, task_type: str) -> int:
    """Largest set of `task_type` tasks with no dependency path between any two
    (a maximum antichain). By Dilworth's theorem this is n minus a maximum
    matching in the reachability graph; plans have at most 50 tasks."""
    nodes = [t.id for t in plan.tasks if t.type.value == task_type]
    anc = _ancestors(plan)
    succ = {u: [v for v in nodes if u in anc[v]] for u in nodes}  # u reaches v
    match: dict[str, str] = {}  # right node -> left node

    def augment(u: str, seen: set[str]) -> bool:
        for v in succ[u]:
            if v not in seen:
                seen.add(v)
                if v not in match or augment(match[v], seen):
                    match[v] = u
                    return True
        return False

    matched = sum(augment(u, set()) for u in nodes)
    return len(nodes) - matched


# --- assertions ----------------------------------------------------------------


def check(expect: dict[str, Any], plan: ExecutionPlan) -> list[str]:
    """Return a human-readable failure per violated assertion (empty = pass)."""
    unknown = set(expect) - ASSERTIONS
    if unknown:
        raise ValueError(f"unknown assertion(s): {sorted(unknown)}")
    m = plan_metrics(plan)
    failures = []

    def bound(name: str, value: int, op: str) -> None:
        if name in expect:
            limit = expect[name]
            ok = value >= limit if op == ">=" else value <= limit
            if not ok:
                failures.append(f"{name}: got {value}, want {op} {limit}")

    bound("min_tasks", m.tasks, ">=")
    bound("max_tasks", m.tasks, "<=")
    bound("min_width", m.width, ">=")
    bound("max_width", m.width, "<=")
    bound("min_depth", m.depth, ">=")
    bound("max_depth", m.depth, "<=")
    if expect.get("single_sink") and len(m.sinks) != 1:
        failures.append(f"single_sink: got {len(m.sinks)} sinks {m.sinks}")
    if "sink_type" in expect and not (len(m.sinks) == 1 and m.types[m.sinks[0]] == expect["sink_type"]):
        failures.append(f"sink_type: want one {expect['sink_type']} sink, got {[m.types[s] for s in m.sinks]}")
    if "independent" in expect:
        spec = expect["independent"]
        n = max_independent(plan, spec["type"])
        if n < spec["min"]:
            failures.append(f"independent: {n} mutually independent {spec['type']} tasks, want >= {spec['min']}")
    for forbidden in expect.get("forbid_types", []):
        used = [tid for tid, ty in m.types.items() if ty == forbidden]
        if used:
            failures.append(f"forbid_types: {forbidden} used by {used}")
    return failures


# --- running cases ---------------------------------------------------------------


@dataclass
class CaseResult:
    name: str
    passed: bool
    attempts: int = 0  # 1, or 2 when the repair loop ran
    seconds: float = 0.0
    metrics: dict[str, Any] | None = None
    failures: list[str] = field(default_factory=list)
    plan: dict[str, Any] | None = None


def load_cases(path: Path = EVAL_CASES) -> dict[str, dict[str, Any]]:
    cases = yaml.safe_load(path.read_text())["cases"]
    for name, case in cases.items():
        unknown = set(case["expect"]) - ASSERTIONS
        if unknown:
            raise ValueError(f"{name}: unknown assertion(s) {sorted(unknown)}")
    return cases


def run_case(planner: Planner, name: str, case: dict[str, Any]) -> CaseResult:
    start = time.monotonic()
    try:
        plan = planner.generate_plan(case["request"])
    except PlanRejected as exc:
        return CaseResult(name, False, planner.last_attempts, time.monotonic() - start,
                          failures=["plan rejected: " + "; ".join(str(i) for i in exc.issues)])
    elapsed = time.monotonic() - start
    m = plan_metrics(plan)
    failures = check(case["expect"], plan)
    return CaseResult(
        name, not failures, planner.last_attempts, elapsed,
        metrics={"tasks": m.tasks, "width": m.width, "depth": m.depth, "sinks": m.sinks},
        failures=failures,
        plan=plan.model_dump(mode="json", exclude_none=True),
    )


def build_planner(args: argparse.Namespace) -> Planner:
    if args.fake:
        from .fake import FakePlanner

        return FakePlanner(default=args.fake)
    from .llm import GroqPlanner

    return GroqPlanner(model=args.model)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--cases", type=Path, default=EVAL_CASES)
    parser.add_argument("--case", action="append", help="run only this case (repeatable)")
    parser.add_argument("--model", help="Groq model id")
    parser.add_argument("--fake", metavar="FIXTURE", help="use FakePlanner with examples/plans/FIXTURE.json")
    parser.add_argument("--json", type=Path, help="write results (including plans) to this file")
    args = parser.parse_args(argv)

    cases = load_cases(args.cases)
    names = args.case or list(cases)
    missing = [n for n in names if n not in cases]
    if missing:
        parser.error(f"unknown case(s): {missing}; known: {list(cases)}")

    planner = build_planner(args)
    results = []
    for name in names:  # sequential on purpose: stays within provider rate limits
        try:
            r = run_case(planner, name, cases[name])
        except Exception as exc:  # provider error: record it and keep going
            r = CaseResult(name, False, failures=[f"error: {type(exc).__name__}: {exc}"])
        results.append(r)
        m = r.metrics or {}
        shape = f"tasks={m.get('tasks', '-')} width={m.get('width', '-')} depth={m.get('depth', '-')}"
        print(f"{'PASS' if r.passed else 'FAIL'}  {name:18} {shape:26} attempts={r.attempts} {r.seconds:5.1f}s")
        for f in r.failures:
            print(f"      - {f}")

    passed = sum(r.passed for r in results)
    print(f"{passed}/{len(results)} cases passed")
    if args.json:
        args.json.write_text(json.dumps([asdict(r) for r in results], indent=2) + "\n")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
