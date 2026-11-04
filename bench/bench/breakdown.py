"""Where does a workflow request's latency go? Critical-path breakdown from atsd's event log.

    python -m bench.breakdown                      # every workflow row of bench/results/e9.csv
    python -m bench.breakdown --request req_ab12   # one request (repeatable)
    python -m bench.breakdown --runs ../runs --api http://localhost:8000

A request finishes when its last task does, and that task waited for its slowest parent, and so on:
the chain of those tasks is the critical path, and only time on it adds to latency. For every task
on the path the time from "ready" (its last parent finished) to "done" is split into:

  slot wait   ready -> first dispatch: no free LLM slot, or the capability was paused after a 429
  retries     first dispatch -> the dispatch of the attempt that succeeded (failed attempts, backoff)
  pickup      that dispatch -> the worker reports task_started (Redis hops, worker pickup)
  LLM call    task_started -> atsd records the completion (the executor's model call, result write)

plus the planner (from e9.csv, measured by bench.e9) and "submit + poll": the rest of the measured
latency (POST /plans, atsd picking the request up, the benchmark's 0.5 s status polling).

atsd's event log is read from `runs/<run_id>/events.jsonl` (docker-compose mounts it into the repo);
task dependencies come from `GET /requests/{id}` on the running API.
"""

from __future__ import annotations

import argparse
import collections
import csv
import json
import re
from pathlib import Path
from typing import Any

from . import live
from .sim import RESULTS_DIR, write_csv

REPO = Path(__file__).resolve().parents[2]
PARTS = ("planner", "slot_wait", "retries", "pickup", "llm_call", "submit_poll")
LABELS = {"planner": "planner", "slot_wait": "slot wait / 429 pause", "retries": "retries + backoff",
          "pickup": "pickup", "llm_call": "LLM call", "submit_poll": "submit + poll"}
FIELDS = ("request_id", "dataset", "id", "latency_s", *[f"{p}_s" for p in PARTS], "critical_path", "pauses_429")


def read_events(runs_dir: Path) -> dict[str, list[dict]]:
    """atsd's event log lines, grouped by request."""
    by: dict[str, list[dict]] = collections.defaultdict(list)
    for f in sorted(runs_dir.glob("*/events.jsonl")):
        for line in f.read_text().splitlines():
            try:
                e = json.loads(line)
            except json.JSONDecodeError:
                continue
            if e.get("request"):
                by[e["request"]].append(e)
    return by


def dependencies(tasks: list[dict[str, Any]]) -> dict[str, list[str]]:
    """Task -> parents, including spliced tasks (t2.1 depends on t2; t2.join on every t2.<n>; a
    planned dependent of t2 depends on t2.join once it exists), from GET /requests/{id}'s tasks."""
    ids = [t["id"] for t in tasks]
    deps = {t["id"]: list(t.get("depends_on") or []) for t in tasks}
    for tid in ids:
        m = re.fullmatch(r"(t\d+)\.(\d+|join)", tid)
        if not m:
            continue
        parent = m.group(1)
        if m.group(2) == "join":
            deps[tid] = [x for x in ids if re.fullmatch(re.escape(parent) + r"\.\d+", x)]
        else:
            deps[tid] = [parent]
    for tid, parents in deps.items():
        if re.fullmatch(r"t\d+", tid):
            deps[tid] = [f"{p}.join" if f"{p}.join" in deps else p for p in parents]
    return deps


def breakdown(events: list[dict], deps: dict[str, list[str]]) -> dict[str, Any]:
    """Seconds per part along the critical path inside atsd, and the path itself."""
    start = next((e["ts_ms"] for e in events if e["event"] in ("request_accepted", "request_resumed")), None)
    done: dict[str, tuple[int, int]] = {}  # task -> (ts, attempt) of its first completion
    dispatched: dict[str, dict[int, int]] = collections.defaultdict(dict)
    started: dict[str, dict[int, int]] = collections.defaultdict(dict)
    for e in events:
        task, attempt = e.get("task"), e.get("attempt", 0)
        if e["event"] in ("completed", "join_completed") and task not in done:
            done[task] = (e["ts_ms"], attempt)
        elif e["event"] in ("dispatched", "speculated"):
            dispatched[task].setdefault(attempt, e["ts_ms"])
        elif e["event"] == "started":
            started[task].setdefault(attempt, e["ts_ms"])
    parts = dict.fromkeys(("slot_wait", "retries", "pickup", "llm_call"), 0.0)
    pauses = sum(1 for e in events if e["event"] == "capability_paused")
    if start is None or not done:
        return {**parts, "path": [], "atsd_s": 0.0, "pauses_429": pauses}

    task = max(done, key=lambda t: done[t][0])  # the last task to finish
    end = done[task][0]
    path = []
    while task is not None:
        path.append(task)
        parents = [p for p in deps.get(task, []) if p in done]
        crit = max(parents, key=lambda p: done[p][0]) if parents else None
        ready = done[crit][0] if crit else start
        finish, attempt = done[task]
        if dispatched.get(task):
            first = min(dispatched[task].values())
            final = dispatched[task].get(attempt, max(dispatched[task].values()))
            began = started.get(task, {}).get(attempt)
            parts["slot_wait"] += max(0, first - ready)
            parts["retries"] += max(0, final - first)
            if began is not None and began >= final:
                parts["pickup"] += began - final
                parts["llm_call"] += max(0, finish - began)
            else:  # a started event can be missing (lost, or the backup's): count it all as the call
                parts["llm_call"] += max(0, finish - final)
        # else: a join or a result restored after a restart: no time of its own
        task = crit
    out = {k: v / 1000 for k, v in parts.items()}
    return {**out, "path": list(reversed(path)), "atsd_s": (end - start) / 1000, "pauses_429": pauses}


def row(rid: str, events: list[dict], deps: dict[str, list[str]], planner_s: float, latency_s: float,
        dataset: str = "", qid: str = "") -> dict[str, Any]:
    b = breakdown(events, deps)
    inside = sum(b[p] for p in ("slot_wait", "retries", "pickup", "llm_call"))
    r = {"request_id": rid, "dataset": dataset, "id": qid, "latency_s": round(latency_s, 2),
         "planner_s": round(planner_s, 2), "critical_path": " -> ".join(b["path"]), "pauses_429": b["pauses_429"]}
    for p in ("slot_wait", "retries", "pickup", "llm_call"):
        r[f"{p}_s"] = round(b[p], 3)
    r["submit_poll_s"] = round(max(0.0, latency_s - planner_s - inside), 2)
    return r


def report(rows: list[dict]) -> str:
    """Per-request lines and the share of all measured latency each part took."""
    total = sum(float(r["latency_s"]) for r in rows) or 1.0
    lines = ["Where workflow latency went (critical path):", "",
             "| part | seconds (all requests) | share |", "|---|---|---|"]
    for p in PARTS:
        s = sum(float(r[f"{p}_s"]) for r in rows)
        lines.append(f"| {LABELS[p]} | {s:.1f} | {100 * s / total:.0f}% |")
    lines += ["", f"{len(rows)} requests, {total:.1f} s in total; "
              f"{sum(int(r['pauses_429']) for r in rows)} capability pauses after a 429.", ""]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--request", action="append", help="request id (repeatable); default: workflow rows of e9.csv")
    ap.add_argument("--e9", type=Path, default=RESULTS_DIR / "e9.csv")
    ap.add_argument("--runs", type=Path, default=REPO / "runs", help="atsd's runs directory")
    ap.add_argument("--api", default=live.API)
    ap.add_argument("--out", type=Path, default=RESULTS_DIR / "e9_breakdown.csv")
    a = ap.parse_args(argv)

    targets: list[dict[str, Any]] = []
    if a.request:
        targets = [{"request_id": r, "planner_s": 0.0, "latency_s": None, "dataset": "", "id": ""} for r in a.request]
    elif a.e9.exists():
        with a.e9.open() as f:
            targets = [r for r in csv.DictReader(f) if r["mode"] == "workflow" and r["request_id"]]
    if not targets:
        raise SystemExit(f"no workflow requests: run bench.e9 first or pass --request")
    events = read_events(a.runs)
    if not events:
        raise SystemExit(f"no atsd event log under {a.runs} (docker-compose writes it to ./runs)")

    rows = []
    for t in targets:
        rid = t["request_id"]
        if rid not in events:
            print(f"{rid}: not in the event log under {a.runs}, skipped")
            continue
        try:
            deps = dependencies(live.http("GET", f"/requests/{rid}", api=a.api)["tasks"])
        except Exception as exc:
            print(f"{rid}: cannot read its tasks from {a.api} ({exc}), skipped")
            continue
        b = breakdown(events[rid], deps)
        latency = float(t["latency_s"]) if t.get("latency_s") else b["atsd_s"]
        r = row(rid, events[rid], deps, float(t.get("planner_s") or 0), latency, t.get("dataset", ""), t.get("id", ""))
        rows.append(r)
        print(f"{rid}  {r['latency_s']:6.1f}s  planner {r['planner_s']:.1f}  slot {r['slot_wait_s']:.1f}  "
              f"retries {r['retries_s']:.1f}  pickup {r['pickup_s']:.2f}  LLM {r['llm_call_s']:.1f}  "
              f"submit+poll {r['submit_poll_s']:.1f}   {r['critical_path']}")
    if not rows:
        raise SystemExit("nothing to report")
    write_csv(rows, a.out, FIELDS)
    print()
    print(report(rows))
    print(f"wrote {a.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
