"""E8: does dynamic expansion beat planning once? (design doc 11, real Groq calls)

    make up-llm
    python -m bench.e8                            # -> bench/results/e8.csv

The 3 open-ended eval requests (the items are not named in the request), each run two ways on
the running stack:

  expand   the normal planner: an expand task discovers the items at runtime and atsd splices one
           task per item (D50, D53)
  static   the planner told never to use expand: it guesses the items (and so the fan-out) at
           planning time from model knowledge, one task per guessed item

Measured: latency (planner + run), the items covered (one task per item) against the count the
request asks for, what expansion costs (the expand task's tokens: a discovery call and a subplan
call, D50, plus one repair call if needed), tokens and cost, and for expand the time from the expand commit (`graph_expanded` in
atsd's log) to the first dispatch of a spliced task.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import yaml
from planner.llm import load_system_prompt
from planner.paths import EVAL_CASES

from . import live
from .sim import RESULTS_DIR, write_csv

CASES = ("top_chip_companies", "largest_cloud_providers", "trending_databases")
FIELDS = ("case", "mode", "status", "latency_s", "planner_s", "exec_s", "requested_items", "items", "item_names",
          "planner_calls", "expand_tokens", "llm_calls", "tokens_in", "tokens_out", "cost_usd", "retries",
          "splice_delay_ms", "output_chars", "request_id")
STATIC_RULE = ("2. Never use the expand type. When the request does not name the items (for example \"the top 5 AI "
               "chip companies\"), choose them yourself from your own knowledge and write one task per item, "
               "naming the item in its title.")
NUMBERS = {"two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 8, "ten": 10}
PER_ITEM_TYPES = ("research", "summarize", "analyze")
# A static plan may still add a task that finds the items ("Identify the three largest cloud
# providers") next to its guessed per-item tasks; it is not an item.
DISCOVERY = re.compile(r"^\s*(identify|find|determine|list|select|discover)\b", re.IGNORECASE)


def static_items(tasks: list[dict]) -> list[dict]:
    sinks = {t["id"] for t in tasks} - {d for t in tasks for d in t["depends_on"] or []}
    return [t for t in tasks if t["type"] in PER_ITEM_TYPES and t["id"] not in sinks
            and not DISCOVERY.match(t["title"] or "")]


def static_prompt() -> str:
    lines = load_system_prompt().splitlines()
    out = [STATIC_RULE if line.startswith("2. ") else line for line in lines]
    out = [line.replace(", expand.", ".") if line.startswith("1. ") else line for line in out]
    return "\n".join(line for line in out if "for expand tasks only" not in line)


def requested_items(request: str) -> int | None:
    m = re.search(r"\b(?:top|the)\s+(\d+|" + "|".join(NUMBERS) + r")\b", request, re.IGNORECASE)
    if not m:
        return None
    word = m.group(1).lower()
    return int(word) if word.isdigit() else NUMBERS[word]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--case", action="append")
    ap.add_argument("--cooldown", type=float, default=60)
    ap.add_argument("--api", default=live.API)
    ap.add_argument("--out", type=Path, default=RESULTS_DIR / "e8.csv")
    a = ap.parse_args(argv)
    cases = yaml.safe_load(EVAL_CASES.read_text())["cases"]
    rows: list[dict] = []
    for name in a.case or CASES:
        request = " ".join(cases[name]["request"].split())
        print(f"== {name}: {request}", flush=True)
        for mode, prompt in (("expand", None), ("static", static_prompt())):
            live.cooldown(a.cooldown)
            planned = live.plan(request, prompt)
            types = {t.id: t.type.value for t in planned.plan.tasks}
            run = live.run_on_stack(planned.plan, a.api)
            expands = [tid for tid, ty in types.items() if ty == "expand"]
            if mode == "expand":
                items = [t for t in run.tasks if "." in t["id"] and not t["id"].endswith(".join")]
            else:
                items = static_items(run.tasks)
            usage_by_task = (live.http("GET", f"/requests/{run.request_id}", api=a.api).get("usage") or {}).get("by_task", {})
            # tokens of the expand task(s); usage counts one llm_call per committed task (D37), not per call
            expand_tokens = sum(usage_by_task.get(e, {}).get(k, 0) for e in expands for k in ("tokens_in", "tokens_out"))
            delay = None
            if expands:
                ev = live.atsd_events(run.request_id)
                expanded = [e["ts_ms"] for e in ev if e["event"] == "graph_expanded"]
                first = [e["ts_ms"] for e in ev if e["event"] == "dispatched" and "." in e.get("task", "")]
                delay = min(first) - min(expanded) if expanded and first else None
            tin, tout = run.tokens_in + planned.tokens_in, run.tokens_out + planned.tokens_out
            rows.append({
                "case": name, "mode": mode, "status": run.status,
                "latency_s": round(planned.seconds + run.seconds, 2), "planner_s": round(planned.seconds, 2),
                "exec_s": round(run.seconds, 2), "requested_items": requested_items(request), "items": len(items),
                "item_names": "; ".join(t["title"] or t["id"] for t in items),
                "planner_calls": planned.calls, "expand_tokens": expand_tokens,
                "llm_calls": run.llm_calls + planned.calls, "tokens_in": tin, "tokens_out": tout,
                "cost_usd": round(live.cost(tin, tout), 6), "retries": run.retries, "splice_delay_ms": delay,
                "output_chars": len(live.output_text(run.output)), "request_id": run.request_id})
            r = rows[-1]
            print(f"  {mode:<6} {r['latency_s']}s {run.status} items={r['items']} ({r['item_names']}) "
                  f"expand_tokens={expand_tokens} splice_delay={delay}ms retries={run.retries}", flush=True)
        write_csv(rows, a.out, FIELDS)  # after every case, so a crash keeps what ran
    print(f"wrote {a.out} ({len(rows)} rows)")
    return 0 if all(r["status"] == "SUCCEEDED" for r in rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())
