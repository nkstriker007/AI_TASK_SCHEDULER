"""End to end with the real C++ scheduler: real Redis + the atsd binary + worker/ workers.

Skipped unless atsd has been built (make build, needs hiredis) and a Redis is available.
Set ATSD_BIN to use a binary somewhere else.
"""

import asyncio
import json
import os
import subprocess
import time
from pathlib import Path

import pytest
from redis.asyncio import Redis

from planner import keys
from planner.fake import load_fixture_plan
from planner.messages import STREAM_FIELD, TaskEvent, result_key
from planner.schema import PlanFile
from worker.main import Worker, WorkerConfig

ATSD = Path(os.environ.get("ATSD_BIN", Path(__file__).resolve().parents[2] / "scheduler" / "build" / "atsd"))
pytestmark = pytest.mark.skipif(not ATSD.exists(), reason=f"atsd not built at {ATSD}")


def plan_file(name, request_id, scale=0.01):
    plan = load_fixture_plan(name)
    for t in plan["tasks"]:
        if t.get("hints") and t["hints"].get("estimated_seconds") is not None:
            t["hints"]["estimated_seconds"] *= scale
    return PlanFile(request_id=request_id, created_at="2026-10-18T00:00:00Z", plan=plan)


class Stack:
    """atsd as a subprocess plus N in-process workers, against one Redis."""

    def __init__(self, redis_url, tmp_path, workers=3, sim_timeout_ms=30_000):
        self.redis_url, self.tmp_path, self.n_workers, self.sim_timeout_ms = redis_url, tmp_path, workers, sim_timeout_ms

    async def __aenter__(self):
        self.r = Redis.from_url(self.redis_url, decode_responses=True)
        env = {**os.environ, "REDIS_URL": self.redis_url, "ATSD_CAPABILITY": "sim",
               "ATSD_SIM_TIMEOUT_MS": str(self.sim_timeout_ms), "ATSD_RUNS_DIR": str(self.tmp_path / "runs")}
        self.atsd = subprocess.Popen([str(ATSD)], env=env, stderr=subprocess.PIPE, text=True)
        # atsd creates its consumer groups at startup; wait until requests:new has the group.
        for _ in range(100):
            try:
                if any(g["name"] == keys.SCHEDULER_GROUP for g in await self.r.xinfo_groups(keys.REQUESTS_STREAM)):
                    break
            except Exception:
                pass
            await asyncio.sleep(0.05)
        self.stop = asyncio.Event()
        self.workers = [Worker(self.r, WorkerConfig(worker_id=f"w-{i}", block_ms=100, heartbeat_s=0.5))
                        for i in range(self.n_workers)]
        self.runners = []
        return self

    def start_workers(self):
        self.runners = [asyncio.create_task(w.run(self.stop)) for w in self.workers]

    async def submit(self, pf: PlanFile):
        body = pf.model_dump_json(exclude_none=True)
        await self.r.set(keys.plan_key(pf.request_id), body)
        await self.r.xadd(keys.REQUESTS_STREAM, {STREAM_FIELD: body})

    async def wait_status(self, request_id, done=("SUCCEEDED", "FAILED", "REJECTED"), timeout=15):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            status = await self.r.hget(keys.request_key(request_id), "status")
            if status in done:
                return status
            await asyncio.sleep(0.05)
        raise AssertionError(f"{request_id} did not finish; status {status!r}")

    async def events(self, request_id):
        out = []
        for _, f in await self.r.xrange(keys.EVENTS_STREAM):
            ev = TaskEvent.model_validate_json(f[STREAM_FIELD]).root
            if ev.attempt_id.startswith(request_id + ":"):
                out.append(ev)
        return out

    async def __aexit__(self, *exc):
        self.stop.set()
        await asyncio.gather(*self.runners, return_exceptions=True)
        self.atsd.terminate()
        try:
            _, self.atsd_log = self.atsd.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            self.atsd.kill()
            _, self.atsd_log = self.atsd.communicate()
        await self.r.aclose()


def run(coro):
    asyncio.run(coro)


def test_company_research_runs_on_three_workers(redis_url, tmp_path):
    async def body():
        async with Stack(redis_url, tmp_path) as s:
            s.start_workers()
            pf = plan_file("company_research", "req_e2e1")
            await s.submit(pf)
            assert await s.wait_status("req_e2e1") == "SUCCEEDED"

            evs = await s.events("req_e2e1")
            started = {e.attempt_id.split(":")[1]: e.ts_ms for e in evs if e.event == "task_started"}
            ended = {e.attempt_id.split(":")[1]: e.ts_ms for e in evs if e.event == "task_completed"}
            for t in pf.plan.tasks:
                for dep in t.depends_on:
                    assert started[t.id] >= ended[dep], f"{t.id} started before {dep} finished"
            assert len({e.worker_id for e in evs}) >= 2  # spread across workers
            final = json.loads(await s.r.get(result_key("req_e2e1", "t6")))
            assert final["content"].startswith("[sim] report t6")
            status = await s.r.hgetall(keys.request_key("req_e2e1"))
            assert status["sink"] == "t6"
            assert all(json.loads(status[f"task:{t.id}"]) == {"state": "COMPLETED", "attempt": 1}
                       for t in pf.plan.tasks)
        assert "1 succeeded" in s.atsd_log
        log = (next((tmp_path / "runs").iterdir()) / "events.jsonl").read_text().splitlines()
        assert json.loads(log[-1])["event"] == "request_succeeded"

    run(body())


def test_attempt_held_by_a_dead_worker_is_recovered_by_the_deadline(redis_url, tmp_path):
    """D10: atsd's attempt deadline is the only lost-attempt detector. A consumer takes t1 and
    dies without acking or reporting; atsd re-dispatches attempt 2 and the request completes."""

    async def body():
        async with Stack(redis_url, tmp_path, workers=2, sim_timeout_ms=1_500) as s:
            pf = plan_file("chain", "req_e2e2")
            await s.submit(pf)
            # The "dead" worker grabs attempt 1 of t1 and never answers.
            grabbed = []
            for _ in range(100):
                resp = await s.r.xreadgroup(keys.WORKERS_GROUP, "dead-worker", {"tasks:sim": ">"}, count=1, block=50) \
                    if await s.r.exists("tasks:sim") else None
                if resp:
                    grabbed = resp[0][1]
                    break
                try:
                    await s.r.xgroup_create("tasks:sim", keys.WORKERS_GROUP, id="0", mkstream=True)
                except Exception:
                    pass
            assert grabbed and json.loads(grabbed[0][1][STREAM_FIELD])["attempt_id"] == "req_e2e2:t1:1"

            s.start_workers()
            assert await s.wait_status("req_e2e2", timeout=20) == "SUCCEEDED"
            status = await s.r.hgetall(keys.request_key("req_e2e2"))
            assert json.loads(status["task:t1"]) == {"state": "COMPLETED", "attempt": 2}
            completions = [e for e in await s.events("req_e2e2") if e.event == "task_completed"]
            assert sum(e.committed for e in completions) == 3  # one committed result per task
        assert "1 lost attempts" in s.atsd_log

    run(body())


def test_invalid_plan_is_rejected(redis_url, tmp_path):
    async def body():
        async with Stack(redis_url, tmp_path, workers=1) as s:
            bad = json.loads((Path(__file__).resolve().parents[2] / "examples/plans/invalid_cycle.json").read_text())
            bad["request_id"] = "req_e2e3"
            await s.r.xadd(keys.REQUESTS_STREAM, {STREAM_FIELD: json.dumps(bad)})
            assert await s.wait_status("req_e2e3") == "REJECTED"
            assert "CYCLE" in await s.r.hget(keys.request_key("req_e2e3"), "error")
            assert not await s.r.exists("tasks:sim") or await s.r.xlen("tasks:sim") == 0  # nothing dispatched

    run(body())


def test_two_requests_run_concurrently(redis_url, tmp_path):
    async def body():
        async with Stack(redis_url, tmp_path) as s:
            s.start_workers()
            await s.submit(plan_file("company_research", "req_e2e4a"))
            await s.submit(plan_file("diamond", "req_e2e4b"))
            assert await s.wait_status("req_e2e4a") == "SUCCEEDED"
            assert await s.wait_status("req_e2e4b") == "SUCCEEDED"
            for rid in ("req_e2e4a", "req_e2e4b"):
                for e in await s.events(rid):
                    assert e.attempt_id.startswith(rid + ":")

    run(body())
