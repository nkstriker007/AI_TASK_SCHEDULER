"""E7: what does the system buy on real requests? (design doc 11, real Groq calls)

    make up-llm                                   # the stack the distributed mode runs on
    python -m bench.e7                            # -> bench/results/e7.csv
    python -m bench.e7 --case two_companies --cooldown 0

For each eval request, one plan from the Groq planner (timed, tokens counted), then:

  distributed  the plan on the running stack (POST /plans): atsd + 3 LLM workers, at most
               LLM_INFLIGHT calls in flight (2 on Groq's on_demand tier, D38)
  sequential   the same plan in this process, one task at a time in dependency order, with the
               same executors and prompts: an agent doing the steps one by one
  single       one large LLM call that answers the whole request (no plan)

Latency of the first two includes the planner call, so the planner's share is visible. A cooldown
before each mode lets Groq's tokens-per-minute window empty, so one mode's 429s do not slow the
next; 429s inside a mode are part of its latency (that is what a rate-limited deployment sees).
"""

from __future__ import annotations

import argparse
import asyncio
import time
from pathlib import Path
from typing import Any

import yaml
from planner.messages import TaskMessage, attempt_id, result_key
from planner.paths import EVAL_CASES
from planner.schema import ExecutionPlan, TaskType
from worker.executors.base import ExecutionContext, RetryableError
from worker.executors.llm import DEFAULT_EXECUTOR_MODEL, LLMExecutor, make_client

from . import live
from .sim import RESULTS_DIR, write_csv

CASES = ("two_companies", "four_companies", "mostly_sequential")
FIELDS = ("case", "mode", "status", "latency_s", "planner_s", "planner_share", "exec_s", "tasks", "llm_calls",
          "tokens_in", "tokens_out", "cost_usd", "retries", "output_chars", "request_id")
SINGLE_PROMPT = ("You are a careful analyst. Answer the user's request completely in one response: do all the "
                 "research, summaries and comparisons it asks for and finish with the requested report, in Markdown. "
                 "Answer from your own knowledge.")


def _order(p: ExecutionPlan) -> list[str]:
    done: list[str] = []
    pending = [t.id for t in p.tasks]
    deps = {t.id: t.depends_on for t in p.tasks}
    while pending:
        ready = [t for t in pending if all(d in done for d in deps[t])]
        done += ready
        pending = [t for t in pending if t not in ready]
    return done


async def sequential(p: ExecutionPlan, request_id: str) -> dict[str, Any]:
    """Run each task in dependency order in this process; wait out 429s like atsd would (D45)."""
    async with make_client() as client:
        return await _sequential(p, request_id, client)


async def _sequential(p: ExecutionPlan, request_id: str, client: Any) -> dict[str, Any]:
    tasks = {t.id: t for t in p.tasks}
    results: dict[str, Any] = {}
    tokens, calls, retries = [0, 0], 0, 0
    t0 = time.monotonic()
    for tid in _order(p):
        t = tasks[tid]
        if t.type == TaskType.expand:
            raise SystemExit("E7 cases have no expand task; use E8 for open-ended requests")
        ex = LLMExecutor(t.type, client=client)
        for attempt in range(1, 9):
            msg = TaskMessage(request_id=request_id, task_id=tid, attempt=attempt,
                              attempt_id=attempt_id(request_id, tid, attempt), type=t.type, instruction=t.instruction,
                              inputs={d: result_key(request_id, d) for d in t.depends_on},
                              deadline_ms=int((time.time() + 300) * 1000))
            calls += 1
            try:
                r = await ex.run(ExecutionContext(msg, {d: results[d] for d in t.depends_on}))
                break
            except RetryableError as exc:
                retries += 1
                await asyncio.sleep(max((exc.retry_after_ms or 0) / 1000, min(32, 2 ** attempt)))
        else:
            return {"status": "FAILED", "seconds": time.monotonic() - t0}
        results[tid] = r.output
        tokens[0] += r.tokens_in or 0
        tokens[1] += r.tokens_out or 0
    sink = [t.id for t in p.tasks if not any(t.id in o.depends_on for o in p.tasks)][-1]
    return {"status": "SUCCEEDED", "seconds": time.monotonic() - t0, "tokens_in": tokens[0], "tokens_out": tokens[1],
            "llm_calls": calls, "retries": retries, "output": results[sink]}


async def single(request: str) -> dict[str, Any]:
    async with make_client() as client:
        return await _single(request, client)


async def _single(request: str, client: Any) -> dict[str, Any]:
    t0 = time.monotonic()
    retries = 0
    for attempt in range(1, 9):
        try:
            r = await client.chat.completions.create(
                model=DEFAULT_EXECUTOR_MODEL, reasoning_effort="low", timeout=300,
                messages=[{"role": "system", "content": SINGLE_PROMPT}, {"role": "user", "content": request}])
            break
        except Exception as exc:
            if type(exc).__name__ != "RateLimitError":
                raise
            retries += 1
            await asyncio.sleep(max(live._retry_after(exc, 2.0), min(32, 2 ** attempt)))
    u = r.usage
    return {"status": "SUCCEEDED", "seconds": time.monotonic() - t0, "tokens_in": u.prompt_tokens,
            "tokens_out": u.completion_tokens, "llm_calls": attempt, "retries": retries,
            "output": r.choices[0].message.content or ""}


def _row(case: str, mode: str, planned: live.Planned | None, r: dict[str, Any], tasks: int, rid: str = "") -> dict:
    planner_s = planned.seconds if planned else 0.0
    tin = r.get("tokens_in", 0) + (planned.tokens_in if planned else 0)
    tout = r.get("tokens_out", 0) + (planned.tokens_out if planned else 0)
    latency = planner_s + r["seconds"]
    return {"case": case, "mode": mode, "status": r["status"], "latency_s": round(latency, 2),
            "planner_s": round(planner_s, 2), "planner_share": round(planner_s / latency, 3) if latency else 0,
            "exec_s": round(r["seconds"], 2), "tasks": tasks,
            "llm_calls": r.get("llm_calls", 0) + (planned.calls if planned else 0),
            "tokens_in": tin, "tokens_out": tout, "cost_usd": round(live.cost(tin, tout), 6),
            "retries": r.get("retries", 0), "output_chars": len(live.output_text(r.get("output"))), "request_id": rid}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--case", action="append", help=f"eval case (repeatable; default {', '.join(CASES)})")
    ap.add_argument("--cooldown", type=float, default=60)
    ap.add_argument("--api", default=live.API)
    ap.add_argument("--out", type=Path, default=RESULTS_DIR / "e7.csv")
    a = ap.parse_args(argv)
    cases = yaml.safe_load(EVAL_CASES.read_text())["cases"]
    rows: list[dict] = []
    for name in a.case or CASES:
        request = " ".join(cases[name]["request"].split())
        print(f"== {name}: {request}", flush=True)
        live.cooldown(a.cooldown)
        planned = live.plan(request)
        n = len(planned.plan.tasks)
        print(f"  planner {planned.seconds:.1f}s, {n} tasks, attempts={planned.attempts}", flush=True)

        live.cooldown(a.cooldown)
        d = live.run_on_stack(planned.plan, a.api)
        rows.append(_row(name, "distributed", planned, {
            "status": d.status, "seconds": d.seconds, "tokens_in": d.tokens_in, "tokens_out": d.tokens_out,
            "llm_calls": d.llm_calls, "retries": d.retries, "output": d.output}, n, d.request_id))
        print(f"  distributed {rows[-1]['latency_s']}s {d.status} retries={d.retries}", flush=True)

        live.cooldown(a.cooldown)
        s = live.arun(sequential(planned.plan, f"seq_{name}"))
        rows.append(_row(name, "sequential", planned, s, n))
        print(f"  sequential  {rows[-1]['latency_s']}s {s['status']} retries={s.get('retries', 0)}", flush=True)

        live.cooldown(a.cooldown)
        one = live.arun(single(request))
        rows.append(_row(name, "single", None, one, 0))
        print(f"  single      {rows[-1]['latency_s']}s", flush=True)
        write_csv(rows, a.out, FIELDS)  # after every case, so a crash keeps what ran
    print(f"wrote {a.out} ({len(rows)} rows)")
    return 0 if all(r["status"] == "SUCCEEDED" for r in rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())
