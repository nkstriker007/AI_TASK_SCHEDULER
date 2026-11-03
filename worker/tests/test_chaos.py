"""Chaos (design doc 10.4, E6): the real atsd with workers that fail, hang, crash and get killed.

Every request must still complete, with zero duplicate commits, and the retries and lost
attempts must be visible in atsd's event log. Skipped unless atsd is built (see test_atsd_e2e).
"""

import asyncio
import json
from collections import Counter

from test_atsd_e2e import ATSD, Stack, plan_file  # noqa: F401  (ATSD drives the skip marker)
from test_atsd_e2e import pytestmark  # noqa: F401

from worker.executors import SimExecutor
from worker.faults import Fault, FaultConfig, parse_rules
from worker.main import Worker, WorkerConfig
from worker.registry import ExecutorRegistry

PLANS = [("company_research", "req_chaos1"), ("diamond", "req_chaos2"), ("chain", "req_chaos3"),
         ("join", "req_chaos4")]


def test_requests_complete_through_errors_hangs_crashes_and_a_killed_worker(redis_url, tmp_path):
    async def body():
        atsd_env = {"ATSD_MAX_ATTEMPTS": "8", "ATSD_POLICY": "fair-share"}
        async with Stack(redis_url, tmp_path, workers=3, sim_timeout_ms=1_500, atsd_env=atsd_env) as s:
            faults = FaultConfig(
                rates={Fault.error: 0.2, Fault.rate_limit: 0.05, Fault.hang: 0.05, Fault.straggler: 0.05},
                rules=parse_rules("t3@1=crash"),  # whichever worker runs a first attempt of t3
                seed=11, retry_after_ms=200, straggler_factor=3,
            )

            crashed = []  # workers really crashed (one), besides worker 0, which is killed below

            def crash_worker(i):
                def crash():  # what os._exit does to a real worker: everything stops, nothing is reported
                    if i == 0 or crashed:
                        return  # later crash decisions act as hangs, so a worker stays alive
                    crashed.append(i)
                    s.workers[i].abort()
                    s.runners[i].cancel()
                return crash

            s.workers = []
            for i in range(3):
                registry = ExecutorRegistry()
                worker_faults = FaultConfig(rates=faults.rates, rules=faults.rules, seed=faults.seed + i,
                                            retry_after_ms=faults.retry_after_ms, straggler_factor=faults.straggler_factor)
                registry.register(SimExecutor(time_scale=0.01, faults=worker_faults, crash=crash_worker(i)))
                s.workers.append(Worker(s.r, WorkerConfig(worker_id=f"w-{i}", block_ms=100, heartbeat_s=0.5), registry))
            s.start_workers()

            for name, rid in PLANS:
                await s.submit(plan_file(name, rid, scale=1.0))  # time_scale 0.01 shrinks durations
            for _ in range(200):  # `docker kill` on worker 0 while it is running an attempt
                if s.workers[0]._inflight:
                    break
                await asyncio.sleep(0.01)
            assert s.workers[0]._inflight, "worker 0 never took a task"
            s.workers[0].abort()
            s.runners[0].cancel()

            statuses = {rid: await s.wait_status(rid, timeout=60) for _, rid in PLANS}
            assert statuses == {rid: "SUCCEEDED" for _, rid in PLANS}

            committed, completions, retried = Counter(), Counter(), 0
            for _, rid in PLANS:
                for e in await s.events(rid):
                    if e.event == "task_completed":
                        task = tuple(e.attempt_id.split(":")[:2])
                        completions[task] += 1
                        committed[task] += e.committed
                status = await s.r.hgetall(f"request:{rid}")
                retried += sum(json.loads(v)["attempt"] > 1 for k, v in status.items() if k.startswith("task:"))
            assert set(committed.values()) == {1}, f"duplicate or missing commits: {committed}"
            assert retried > 0
            assert len(crashed) == 1 and crashed[0] in (1, 2)  # the crash fault really took a worker down

        log = [json.loads(line) for line in (next((tmp_path / "runs").iterdir()) / "events.jsonl").read_text().splitlines()]
        kinds = Counter(line["event"] for line in log)
        assert kinds["retry_scheduled"] > 0  # retries are logged
        assert kinds["attempt_lost"] >= 2  # the killed and the crashed worker's attempts, found by the deadline
        assert kinds["request_succeeded"] == len(PLANS)
        print(f"\nchaos: {sum(completions.values())} completions for {len(committed)} tasks, "
              f"{kinds['retry_scheduled']} retries, {kinds['attempt_lost']} lost attempts, 0 duplicate commits")

    asyncio.run(body())
