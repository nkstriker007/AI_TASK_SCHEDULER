"""Distributed runs for E1-E3 (design doc 11): the real atsd binary, real Redis, N sim workers.

    python -m bench.run                          # E1, E2, E3 -> bench/results/dist.csv
    python -m bench.run --experiments e3 --repeats 3
    python -m bench.run --redis redis://localhost:6379/9   # an existing Redis (the db is flushed)

  E1  Does the distributed runtime match the simulator? Each workload on 4 workers; measured
      makespan against ats_sim's, both at the same time scale. The gap is the overhead.
  E2  How does it scale? Workers 1, 2, 4, 8, 16; speedup against a measured 1-worker run
      (never inferred, design doc 3.10) and against the simulator's speedup.
  E3  What does each task cost in overhead? 32 independent tasks + a join on 8 workers, with
      task durations from 2 ms to 1 s; efficiency = simulated / measured makespan, and the
      overhead per step = (measured - simulated) / the number of tasks the busiest simulated
      worker runs one after another (5 here: 4 rounds of 8 parallel tasks, then the join).

Each run starts a fresh atsd (its own event log) on a flushed Redis, runs the workers in this
process (sim tasks are asyncio sleeps, so they run concurrently), submits one plan file and
waits for the request to finish. Timings come from atsd's event log: makespan is first
dispatch to last completion. Workers in one process share an event loop, so the overhead
measured here includes Python scheduling as well as Redis round trips and atsd.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import socket
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

from planner import keys
from planner.messages import STREAM_FIELD
from redis.asyncio import Redis
from worker.executors import SimExecutor
from worker.main import Worker, WorkerConfig
from worker.registry import ExecutorRegistry

from . import gen
from .sim import ATS_SIM, RESULTS_DIR, simulate, write_csv

REPO = Path(__file__).resolve().parents[2]
ATSD = REPO / "scheduler" / "build" / "atsd"
FIELDS = ("experiment", "workload", "tasks", "workers", "time_scale", "task_seconds", "repeat",
          "makespan", "sim_makespan", "overhead", "overhead_pct", "steps", "per_step_overhead_ms", "status")
E1_WORKLOADS = ("chain-10", "fanout-32", "layered-24", "straggler-16")
E2_WORKLOADS = ("chain-10", "fanout-32", "layered-24")
E2_WORKERS = (1, 2, 4, 8, 16)
E3_DURATIONS = (0.002, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0)
TIME_SCALE = 0.05  # 2 s tasks run as 100 ms: fast runs, still far above the overhead


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class LocalRedis:
    """A throwaway redis-server, unless a URL is given."""

    def __init__(self, url: str | None):
        self.url, self.proc = url, None

    def __enter__(self) -> str:
        if self.url:
            return self.url
        exe = shutil.which("redis-server")
        if not exe:
            raise SystemExit("needs redis-server on PATH (brew install redis) or --redis URL")
        port = _free_port()
        self.proc = subprocess.Popen([exe, "--port", str(port), "--save", "", "--appendonly", "no"],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(0.3)
        return f"redis://127.0.0.1:{port}/0"

    def __exit__(self, *exc) -> None:
        if self.proc:
            self.proc.terminate()
            self.proc.wait(timeout=5)


async def run_once(redis_url: str, plan_doc: dict[str, Any], workers: int, time_scale: float,
                   atsd: Path = ATSD, timeout_s: float = 600) -> dict[str, Any]:
    """Run one plan file end to end; return makespan, per-task overhead and status."""
    r = Redis.from_url(redis_url, decode_responses=True)
    await r.flushdb()
    rid = plan_doc["request_id"]
    with tempfile.TemporaryDirectory() as runs:
        env = {**os.environ, "REDIS_URL": redis_url, "ATSD_CAPABILITY": "sim", "ATSD_POLICY": "fifo",
               "ATSD_SIM_MAX_INFLIGHT": "0", "ATSD_SIM_TIMEOUT_MS": str(int(timeout_s * 1000)),
               "ATSD_RUNS_DIR": runs, "ATSD_JANITOR_INTERVAL_MS": "0"}
        proc = subprocess.Popen([str(atsd)], env=env, stderr=subprocess.DEVNULL)  # noqa: ASYNC220 (starts at once)
        stop = asyncio.Event()
        try:
            for _ in range(100):  # atsd creates its groups at startup
                if await r.exists(keys.REQUESTS_STREAM):
                    break
                await asyncio.sleep(0.05)
            pool = []
            for i in range(workers):
                registry = ExecutorRegistry()
                registry.register(SimExecutor(time_scale=time_scale))
                pool.append(Worker(r, WorkerConfig(worker_id=f"bench-{i}", block_ms=50, heartbeat_s=1.0), registry))
            runners = [asyncio.create_task(w.run(stop)) for w in pool]
            await asyncio.sleep(0.2)  # workers joined their consumer group
            await r.xadd(keys.REQUESTS_STREAM, {STREAM_FIELD: json.dumps(plan_doc)})
            deadline = time.monotonic() + timeout_s
            status = None
            while time.monotonic() < deadline:
                status = await r.hget(keys.request_key(rid), "status")
                if status in ("SUCCEEDED", "FAILED", "REJECTED"):
                    break
                await asyncio.sleep(0.02)
            stop.set()
            await asyncio.gather(*runners, return_exceptions=True)
        finally:
            proc.terminate()
            proc.wait(timeout=10)
            await r.aclose()
        log = [json.loads(line) for f in Path(runs).glob("*/events.jsonl") for line in f.read_text().splitlines()]

    events = [e for e in log if e.get("request") == rid]
    dispatched = [e["ts_ms"] for e in events if e["event"] == "dispatched"]
    completed = [e["ts_ms"] for e in events if e["event"] == "completed"]
    makespan = (max(completed) - min(dispatched)) / 1000 if completed and dispatched else float("nan")
    return {"makespan": makespan, "status": status}


def _row(experiment: str, workload: str, doc: dict, workers: int, scale: float, repeat: int,
         measured: dict, sim: tuple[float, int], task_seconds: float | None = None) -> dict[str, Any]:
    sim_makespan, steps = sim
    overhead = measured["makespan"] - sim_makespan
    return {"experiment": experiment, "workload": workload, "tasks": len(doc["plan"]["tasks"]), "workers": workers,
            "time_scale": scale, "task_seconds": task_seconds, "repeat": repeat,
            "makespan": round(measured["makespan"], 4), "sim_makespan": round(sim_makespan, 4),
            "overhead": round(overhead, 4), "overhead_pct": round(100 * overhead / sim_makespan, 2),
            "steps": steps, "per_step_overhead_ms": round(1000 * overhead / steps, 2), "status": measured["status"]}


def _simulate(doc: dict, workers: int, scale: float, ats_sim: Path) -> tuple[float, int]:
    """(makespan at this time scale, tasks run one after another by the busiest simulated worker)."""
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(doc, f)
    try:
        r = simulate(Path(f.name), workers, "fifo", ats_sim)
    finally:
        os.unlink(f.name)
    per_worker: dict[int, int] = {}
    for a in r["attempts"]:
        per_worker[a["worker"]] = per_worker.get(a["worker"], 0) + 1
    return r["makespan"] * scale, max(per_worker.values())


async def experiments(which: set[str], repeats: int, redis_url: str, atsd: Path, ats_sim: Path) -> list[dict]:
    wl = gen.workloads()
    rows: list[dict] = []

    async def measure(exp, name, doc, workers, scale, rep, task_seconds=None):
        sim = _simulate(doc, workers, scale, ats_sim)
        m = await run_once(redis_url, doc, workers, scale, atsd)
        row = _row(exp, name, doc, workers, scale, rep, m, sim, task_seconds)
        rows.append(row)
        print(f"  {exp} {name:<13} workers={workers:<2} scale={scale:<6} measured={row['makespan']:.3f}s "
              f"sim={row['sim_makespan']:.3f}s overhead={row['overhead_pct']:+.1f}% {row['status']}", flush=True)

    for rep in range(1, repeats + 1):
        if "e1" in which:
            for name in E1_WORKLOADS:
                await measure("E1", name, wl[name], 4, TIME_SCALE, rep)
        if "e2" in which:
            for name in E2_WORKLOADS:
                for n in E2_WORKERS:
                    await measure("E2", name, wl[name], n, TIME_SCALE, rep)
        if "e3" in which:
            for d in E3_DURATIONS:
                doc = gen.fanout(32, seconds=d, join_seconds=d, name=f"e3-{d:g}s")
                await measure("E3", doc["request_id"].removeprefix("bench_"), doc, 8, 1.0, rep, d)
    return rows


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--experiments", default="e1,e2,e3")
    p.add_argument("--repeats", type=int, default=1)
    p.add_argument("--redis", default=None, help="use this Redis (its database is flushed) instead of a throwaway one")
    p.add_argument("--out", type=Path, default=RESULTS_DIR / "dist.csv")
    p.add_argument("--atsd", type=Path, default=ATSD)
    p.add_argument("--ats-sim", type=Path, default=ATS_SIM)
    a = p.parse_args(argv)
    for binary in (a.atsd, a.ats_sim):
        if not binary.exists():
            raise SystemExit(f"{binary} not found: run `make build` (atsd needs hiredis)")
    which = {e.strip().lower() for e in a.experiments.split(",") if e.strip()}
    t0 = time.time()
    with LocalRedis(a.redis) as url:
        rows = asyncio.run(experiments(which, a.repeats, url, a.atsd, a.ats_sim))
    write_csv(rows, a.out, FIELDS)
    print(f"wrote {a.out} ({len(rows)} runs, {time.time() - t0:.0f}s)")
    return 0 if all(r["status"] == "SUCCEEDED" for r in rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())
