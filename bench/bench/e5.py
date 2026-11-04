"""E5: does speculative execution cut tail latency? Plus the latency profile (p50/p95/p99).

    python -m bench.e5                         # -> bench/results/e5.csv, e5_latency.csv
    python -m bench.e5 --repeats 10            # quicker
    python -m bench.e5 --redis redis://localhost:6379/9   # an existing Redis (the db is flushed)

Workload: 16 independent tasks of 2 s and a join (design doc 11: straggler workload), at 1/10
time scale, on the real atsd, real Redis and 16 in-process sim workers. Every attempt straggles
(runs 10x longer) with probability 10 %, decided by worker/faults.py from (seed, attempt_id).
Each seed runs twice, with speculation off and on (ATSD_SPECULATION, D56): the same primary
attempts straggle in both runs, so the comparison is paired; a backup is a new attempt id and
gets a fresh draw.

Per run (e5.csv): makespan (atsd's first dispatch to last completion), backups dispatched and
won, and wasted work: execution time of attempts whose result lost the SET NX race, measured by
the workers (exec_ms), against the useful work.

Latency profile (e5_latency.csv, one row per sample, both modes):
  pickup    atsd dispatches an attempt -> the worker reports task_started (Redis hop, worker
            pickup, Redis hop back)
  report    the worker reports task_completed -> atsd records it (Redis hop, atsd's loop)
  handoff   the last parent's completion (worker clock) -> atsd dispatches the child: the whole
            scheduling step between two dependent tasks
Workers and atsd run on one host, so worker and atsd timestamps share a clock.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

from planner import keys
from planner.messages import STREAM_FIELD
from redis.asyncio import Redis
from worker.executors import SimExecutor
from worker.faults import Fault, FaultConfig
from worker.main import Worker, WorkerConfig
from worker.registry import ExecutorRegistry

from .run import ATSD, LocalRedis
from .sim import RESULTS_DIR

TASKS = 16
TASK_SECONDS = 2.0
TIME_SCALE = 0.1          # 2 s tasks run as 200 ms
STRAGGLER_RATE = 0.10
STRAGGLER_FACTOR = 10.0
RUN_FIELDS = ("mode", "seed", "status", "makespan", "backups", "backups_won", "useful_ms", "wasted_ms", "stragglers")
LATENCY_FIELDS = ("mode", "seed", "metric", "ms")


def plan_doc(request_id: str, tasks: int = TASKS, seconds: float = TASK_SECONDS) -> dict[str, Any]:
    items = [{"id": f"t{i}", "type": "research", "title": f"Task t{i}", "instruction": f"Do t{i}.",
              "depends_on": [], "hints": {"estimated_seconds": seconds}} for i in range(1, tasks + 1)]
    items.append({"id": f"t{tasks + 1}", "type": "report", "title": "Join", "instruction": "Join the results.",
                  "depends_on": [t["id"] for t in items], "hints": {"estimated_seconds": seconds}})
    return {"request_id": request_id, "created_at": "2026-11-03T00:00:00Z",
            "plan": {"schema_version": 1, "summary": "E5 straggler fan-out", "tasks": items}}


async def run_once(redis_url: str, doc: dict[str, Any], workers: int, speculation: bool, seed: int,
                   straggler_rate: float = STRAGGLER_RATE, time_scale: float = TIME_SCALE,
                   atsd: Path = ATSD, timeout_s: float = 120) -> dict[str, Any]:
    """One run. Returns the run row and the latency samples."""
    r = Redis.from_url(redis_url, decode_responses=True)
    await r.flushdb()
    rid = doc["request_id"]
    faults = FaultConfig(rates={Fault.straggler: straggler_rate}, seed=seed, straggler_factor=STRAGGLER_FACTOR)
    with tempfile.TemporaryDirectory() as runs:
        env = {**os.environ, "REDIS_URL": redis_url, "ATSD_CAPABILITY": "sim", "ATSD_POLICY": "fifo",
               "ATSD_SIM_MAX_INFLIGHT": "0", "ATSD_SIM_TIMEOUT_MS": str(int(timeout_s * 1000)),
               "ATSD_RUNS_DIR": runs, "ATSD_JANITOR_INTERVAL_MS": "0",
               "ATSD_SPECULATION": "1" if speculation else "0"}
        proc = subprocess.Popen([str(atsd)], env=env, stderr=subprocess.DEVNULL)  # noqa: ASYNC220
        stop = asyncio.Event()
        status = None
        try:
            for _ in range(100):  # atsd creates its groups at startup
                if await r.exists(keys.REQUESTS_STREAM):
                    break
                await asyncio.sleep(0.05)
            pool = []
            for i in range(workers):
                registry = ExecutorRegistry()
                registry.register(SimExecutor(time_scale=time_scale, faults=faults))
                pool.append(Worker(r, WorkerConfig(worker_id=f"e5-{i}", block_ms=20, heartbeat_s=1.0), registry))
            runners = [asyncio.create_task(w.run(stop)) for w in pool]
            await asyncio.sleep(0.2)  # workers joined their consumer group
            await r.xadd(keys.REQUESTS_STREAM, {STREAM_FIELD: json.dumps(doc)})
            deadline = time.monotonic() + timeout_s
            while time.monotonic() < deadline:
                status = await r.hget(keys.request_key(rid), "status")
                if status in ("SUCCEEDED", "FAILED", "REJECTED"):
                    break
                await asyncio.sleep(0.01)
            stop.set()
            await asyncio.gather(*runners, return_exceptions=True)  # losers finish and report
            worker_events = [json.loads(f[STREAM_FIELD]) for _, f in await r.xrange(keys.EVENTS_STREAM)]
        finally:
            proc.terminate()
            proc.wait(timeout=10)
            await r.aclose()
        log = [json.loads(line) for f in Path(runs).glob("*/events.jsonl") for line in f.read_text().splitlines()]
    return analyse(doc, log, worker_events, speculation, seed, status, faults)


def analyse(doc: dict[str, Any], log: list[dict], worker_events: list[dict], speculation: bool, seed: int,
            status: str | None, faults: FaultConfig) -> dict[str, Any]:
    """Run row + latency samples from atsd's event log and the workers' task:events."""
    rid = doc["request_id"]
    mode = "speculation" if speculation else "baseline"
    atsd = [e for e in log if e.get("request") == rid]
    dispatched = {(e["task"], e["attempt"]): e["ts_ms"] for e in atsd if e["event"] in ("dispatched", "speculated")}
    recorded = {e["task"]: e["ts_ms"] for e in atsd if e["event"] == "completed"}
    first_dispatch = min(dispatched.values()) if dispatched else None
    makespan = (max(recorded.values()) - first_dispatch) / 1000 if recorded and first_dispatch else float("nan")

    def split(aid: str) -> tuple[str, int]:
        _, task, attempt = aid.split(":")
        return task, int(attempt)

    started = {split(e["attempt_id"]): e["ts_ms"] for e in worker_events if e["event"] == "task_started"}
    completions = [e for e in worker_events if e["event"] == "task_completed"]
    useful = sum(e.get("exec_ms") or 0 for e in completions if e.get("committed"))
    wasted = sum(e.get("exec_ms") or 0 for e in completions if not e.get("committed"))
    winner_done = {split(e["attempt_id"])[0]: e["ts_ms"] for e in completions if e.get("committed")}

    samples = []
    for key, ts in dispatched.items():
        if key in started:
            samples.append({"mode": mode, "seed": seed, "metric": "pickup", "ms": started[key] - ts})
    for task, ts in recorded.items():
        if task in winner_done:
            samples.append({"mode": mode, "seed": seed, "metric": "report", "ms": ts - winner_done[task]})
    deps = {t["id"]: t["depends_on"] for t in doc["plan"]["tasks"]}
    for task, parents in deps.items():
        child = dispatched.get((task, 1))
        if parents and child is not None and all(p in winner_done for p in parents):
            samples.append({"mode": mode, "seed": seed, "metric": "handoff",
                            "ms": child - max(winner_done[p] for p in parents)})

    stragglers = sum(1 for (task, attempt) in dispatched
                     if faults.decide(task, attempt, f"{rid}:{task}:{attempt}") == Fault.straggler)
    row = {"mode": mode, "seed": seed, "status": status, "makespan": round(makespan, 4),
           "backups": sum(1 for e in atsd if e["event"] == "speculated"),
           "backups_won": sum(1 for e in atsd if e["event"] == "speculation_won"),
           "useful_ms": useful, "wasted_ms": wasted, "stragglers": stragglers}
    return {"row": row, "samples": samples}


def percentile(values: list[float], q: float) -> float:
    """Linear interpolation between closest ranks (numpy's default)."""
    if not values:
        return float("nan")
    v = sorted(values)
    pos = q / 100 * (len(v) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(v) - 1)
    return v[lo] + (pos - lo) * (v[hi] - v[lo])


def write(path: Path, fields: tuple[str, ...], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, lineterminator="\n")  # "\r\n" would not survive `git am`
        w.writeheader()
        w.writerows(rows)


async def sweep(redis_url: str, repeats: int, workers: int) -> tuple[list[dict], list[dict]]:
    rows, samples = [], []
    for seed in range(1, repeats + 1):
        for speculation in (False, True):
            # Same request id in both modes: faults hash (seed, attempt_id), so the same primary
            # attempts straggle in both runs (each run has its own flushed Redis and atsd).
            doc = plan_doc(f"req_e5_{seed}")
            result = await run_once(redis_url, doc, workers, speculation, seed)
            rows.append(result["row"])
            samples.extend(result["samples"])
            r = result["row"]
            print(f"seed {seed:3d} {r['mode']:11s} {r['status']}  makespan {r['makespan']:.3f} s  "
                  f"stragglers {r['stragglers']}  backups {r['backups']} (won {r['backups_won']})")
    return rows, samples


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--repeats", type=int, default=40, help="seeds; each runs with and without speculation")
    p.add_argument("--workers", type=int, default=TASKS)
    p.add_argument("--redis", help="existing Redis URL (its database is flushed)")
    p.add_argument("--results", type=Path, default=RESULTS_DIR)
    a = p.parse_args(argv)
    if not ATSD.exists():
        raise SystemExit(f"atsd not found at {ATSD}: run `make build` first (needs hiredis)")
    logging.getLogger("worker").setLevel(logging.ERROR)  # one "injected fault" line per straggler otherwise
    with LocalRedis(a.redis) as url:
        rows, samples = asyncio.run(sweep(url, a.repeats, a.workers))
    write(a.results / "e5.csv", RUN_FIELDS, rows)
    write(a.results / "e5_latency.csv", LATENCY_FIELDS, samples)
    for mode in ("baseline", "speculation"):
        m = [r["makespan"] for r in rows if r["mode"] == mode and r["status"] == "SUCCEEDED"]
        print(f"{mode:11s} makespan p50 {percentile(m, 50):.3f} s  p95 {percentile(m, 95):.3f} s  "
              f"p99 {percentile(m, 99):.3f} s  ({len(m)} runs)")
    print(f"wrote {a.results / 'e5.csv'} and {a.results / 'e5_latency.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
