"""End to end with the real C++ scheduler: real Redis + the atsd binary + worker/ workers.

Skipped unless atsd has been built (make build, needs hiredis) and a Redis is available.
Set ATSD_BIN to use a binary somewhere else.
"""

import asyncio
import json
import os
import signal
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

    def __init__(self, redis_url, tmp_path, workers=3, sim_timeout_ms=30_000, atsd_env=None):
        self.redis_url, self.tmp_path, self.n_workers, self.sim_timeout_ms = redis_url, tmp_path, workers, sim_timeout_ms
        self.atsd_env = atsd_env or {}
        self.atsd = None
        self.atsd_log = ""

    async def start_atsd(self):
        env = {**os.environ, "REDIS_URL": self.redis_url, "ATSD_CAPABILITY": "sim",
               "ATSD_SIM_TIMEOUT_MS": str(self.sim_timeout_ms), "ATSD_RUNS_DIR": str(self.tmp_path / "runs"),
               **self.atsd_env}
        self.atsd = subprocess.Popen([str(ATSD)], env=env, stderr=subprocess.PIPE, text=True)
        # atsd creates its consumer groups at startup; wait until requests:new has the group.
        for _ in range(100):
            try:
                if any(g["name"] == keys.SCHEDULER_GROUP for g in await self.r.xinfo_groups(keys.REQUESTS_STREAM)):
                    break
            except Exception:
                pass
            await asyncio.sleep(0.05)

    async def stop_atsd(self, sig=signal.SIGTERM, timeout=15):
        """Signals atsd and waits for it to exit (without blocking the workers' event loop)."""
        proc, self.atsd = self.atsd, None
        proc.send_signal(sig)
        deadline = time.monotonic() + timeout
        while proc.poll() is None and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        if proc.poll() is None:
            proc.kill()
        _, err = proc.communicate()
        self.atsd_log += err
        return proc.returncode, err

    async def __aenter__(self):
        self.r = Redis.from_url(self.redis_url, decode_responses=True)
        await self.start_atsd()
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

    async def wait_event(self, request_id, kind, task_id, timeout=10):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for e in await self.events(request_id):
                if e.event == kind and e.attempt_id.split(":")[1] == task_id:
                    return e
            await asyncio.sleep(0.02)
        raise AssertionError(f"no {kind} for {request_id}:{task_id}")

    async def __aexit__(self, *exc):
        self.stop.set()
        await asyncio.gather(*self.runners, return_exceptions=True)
        if self.atsd is not None:
            self.atsd.terminate()
            try:
                _, err = self.atsd.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                self.atsd.kill()
                _, err = self.atsd.communicate()
            self.atsd_log += err
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


# --------------------------------------------------------------------------------- Day 4 (Person B)

def wide_plan_file(request_id, n, seconds):
    """n independent tasks of `seconds` each, joined by a report."""
    tasks = [{"id": f"t{i}", "type": "research", "title": f"Task t{i}", "instruction": f"Do t{i}.",
              "depends_on": [], "hints": {"estimated_seconds": seconds}} for i in range(1, n + 1)]
    tasks.append({"id": f"t{n + 1}", "type": "report", "title": "Report", "instruction": "Join.",
                  "depends_on": [t["id"] for t in tasks], "hints": {"estimated_seconds": seconds}})
    return PlanFile.model_validate({"request_id": request_id, "created_at": "2026-11-01T00:00:00Z",
                                    "plan": {"schema_version": 1, "summary": "wide", "tasks": tasks}})


def starts(events):
    return sorted((e.ts_ms, e.attempt_id) for e in events if e.event == "task_started")


def test_sigterm_drains_in_flight_attempts_and_the_next_atsd_resumes(redis_url, tmp_path):
    """D43 + D44: SIGTERM while t1 runs. atsd waits for t1 to report, dispatches nothing more and
    exits 0; the request stays pending. A new atsd resumes it without running t1 again."""

    async def body():
        async with Stack(redis_url, tmp_path, workers=2) as s:
            s.start_workers()
            await s.submit(plan_file("chain", "req_d4a", scale=1.0))  # 1 s per task
            await s.wait_event("req_d4a", "task_started", "t1")
            code, log = await s.stop_atsd(signal.SIGTERM)
            assert code == 0
            assert "draining" in log and "atsd: drained" in log
            status = await s.r.hgetall(keys.request_key("req_d4a"))
            assert status["status"] == "RUNNING"
            assert json.loads(status["task:t1"])["state"] == "COMPLETED"  # recorded before exit
            assert all(e.attempt_id.split(":")[1] == "t1" for e in await s.events("req_d4a"))  # t2 never sent
            pending = await s.r.xpending(keys.REQUESTS_STREAM, keys.SCHEDULER_GROUP)
            assert pending["pending"] == 1  # kept for the next atsd

            await s.start_atsd()
            assert await s.wait_status("req_d4a", timeout=20) == "SUCCEEDED"
            started = [e.attempt_id for e in await s.events("req_d4a") if e.event == "task_started"]
            assert started == ["req_d4a:t1:1", "req_d4a:t2:1", "req_d4a:t3:1"]  # each task ran once
            for _ in range(50):  # atsd acknowledges the entry right after writing SUCCEEDED
                if (await s.r.xpending(keys.REQUESTS_STREAM, keys.SCHEDULER_GROUP))["pending"] == 0:
                    break
                await asyncio.sleep(0.02)
            assert (await s.r.xpending(keys.REQUESTS_STREAM, keys.SCHEDULER_GROUP))["pending"] == 0
        assert "resumed 1 unfinished request" in s.atsd_log

    run(body())


def test_killed_atsd_is_resumed_by_the_next_one(redis_url, tmp_path):
    """D44: kill -9 while t2 runs; a new atsd finishes the request with one committed result per task."""

    async def body():
        async with Stack(redis_url, tmp_path, workers=2) as s:
            s.start_workers()
            await s.submit(plan_file("chain", "req_d4b", scale=1.0))
            await s.wait_event("req_d4b", "task_started", "t2")
            await s.stop_atsd(signal.SIGKILL)
            await s.start_atsd()
            assert await s.wait_status("req_d4b", timeout=20) == "SUCCEEDED"
            evs = await s.events("req_d4b")
            assert [e.attempt_id for e in evs if e.event == "task_started" and ":t1:" in e.attempt_id] == ["req_d4b:t1:1"]
            assert sum(e.committed for e in evs if e.event == "task_completed") == 3
            for t in ("t1", "t2", "t3"):
                assert await s.r.exists(result_key("req_d4b", t))

    run(body())


def test_fair_share_splits_a_capped_capacity_between_requests(redis_url, tmp_path):
    """D41: with 2 slots, fair share gives the second request a slot as soon as one frees; the
    FIFO baseline makes it wait until every ready task of the first request has started."""

    async def first_start_of_b(policy):
        async with Stack(redis_url, tmp_path / policy, workers=4,
                         atsd_env={"ATSD_POLICY": policy, "ATSD_SIM_MAX_INFLIGHT": "2"}) as s:
            s.start_workers()
            await s.submit(wide_plan_file(f"req_{policy}_a", 4, 0.3))
            await s.wait_event(f"req_{policy}_a", "task_started", "t1")
            await s.submit(wide_plan_file(f"req_{policy}_b", 2, 0.3))
            assert await s.wait_status(f"req_{policy}_a") == "SUCCEEDED"
            assert await s.wait_status(f"req_{policy}_b") == "SUCCEEDED"
            a = starts(await s.events(f"req_{policy}_a"))
            b = starts(await s.events(f"req_{policy}_b"))
            return a, b

    async def body():
        a, b = await first_start_of_b("fair-share")
        # B's first task starts before A's third: A held 2 slots, the first freed slot went to B.
        assert b[0][0] <= a[2][0]
        a, b = await first_start_of_b("fifo")
        # FIFO: all four of A's roots were ready first, so B waits behind them.
        assert b[0][0] >= a[3][0]

    run(body())


# --------------------------------------------------------------------------------- Day 5 (Person B)

def test_janitor_acks_the_dead_workers_entry_after_recovery(redis_url, tmp_path):
    """D49: a consumer takes t1 and dies without acking. atsd's deadline recovers the task
    (D10); the janitor then acknowledges the dead entry, so nothing is left pending."""

    async def body():
        env = {"ATSD_JANITOR_INTERVAL_MS": "200", "ATSD_JANITOR_MIN_IDLE_MS": "300"}
        async with Stack(redis_url, tmp_path, workers=2, sim_timeout_ms=1_000, atsd_env=env) as s:
            pf = plan_file("chain", "req_d5a")
            await s.submit(pf)
            try:
                await s.r.xgroup_create("tasks:sim", keys.WORKERS_GROUP, id="0", mkstream=True)
            except Exception:
                pass
            grabbed = []
            for _ in range(100):
                resp = await s.r.xreadgroup(keys.WORKERS_GROUP, "dead-worker", {"tasks:sim": ">"}, count=1, block=50)
                if resp:
                    grabbed = resp[0][1]
                    break
            assert grabbed and json.loads(grabbed[0][1][STREAM_FIELD])["attempt_id"] == "req_d5a:t1:1"

            s.start_workers()
            assert await s.wait_status("req_d5a", timeout=20) == "SUCCEEDED"
            # The janitor runs every 200 ms on entries idle 300 ms or more.
            for _ in range(100):
                if (await s.r.xpending("tasks:sim", keys.WORKERS_GROUP))["pending"] == 0:
                    break
                await asyncio.sleep(0.05)
            assert (await s.r.xpending("tasks:sim", keys.WORKERS_GROUP))["pending"] == 0
            completions = [e for e in await s.events("req_d5a") if e.event == "task_completed"]
            assert sum(e.committed for e in completions) == 3  # janitor never caused a re-run
        assert "1 lost attempts" in s.atsd_log
        assert "1 dead pending entries acknowledged" in s.atsd_log
        log = [json.loads(line) for line in (next((tmp_path / "runs").iterdir()) / "events.jsonl").read_text().splitlines()]
        acked = [e for e in log if e["event"] == "janitor_acked"]
        assert [(e["request"], e["task"], e["attempt"], e["worker"]) for e in acked] == \
            [("req_d5a", "t1", 1, "dead-worker")]

    run(body())
