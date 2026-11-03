"""Fault injection (worker/faults.py), the sim executor's faults, and the worker's handling of
hangs (deadline) and an immediate stop (abort)."""

import asyncio
import time

import pytest
from redis.asyncio import Redis

from planner import keys
from planner.messages import STREAM_FIELD, TaskEvent, TaskMessage, attempt_id, result_key
from worker.executors import ExecutionContext, PermanentError, RetryableError, SimExecutor
from worker.faults import Fault, FaultConfig, FaultRule, parse_rules
from worker.main import Worker, WorkerConfig
from worker.registry import ExecutorRegistry

RID = "req_faults01"


def msg(task_id="t1", attempt=1, seconds=0.2, deadline_s=60):
    return TaskMessage(request_id=RID, task_id=task_id, attempt=attempt, attempt_id=attempt_id(RID, task_id, attempt),
                       type="research", instruction="x", deadline_ms=int((time.time() + deadline_s) * 1000),
                       estimated_seconds=seconds)


# --- FaultConfig ---------------------------------------------------------------------


def test_rules_parse_and_take_priority_over_rates():
    rules = parse_rules(" t2@1=crash, t3=hang ,t5@2=permanent")
    assert rules == (FaultRule("t2", 1, Fault.crash), FaultRule("t3", None, Fault.hang),
                     FaultRule("t5", 2, Fault.permanent))
    cfg = FaultConfig(rates={Fault.error: 1.0}, rules=rules)
    assert cfg.decide("t2", 1, "r:t2:1") == Fault.crash
    assert cfg.decide("t2", 2, "r:t2:2") == Fault.error  # the rule is for attempt 1 only
    assert cfg.decide("t3", 7, "r:t3:7") == Fault.hang   # every attempt


@pytest.mark.parametrize("text", ["t2", "t2=explode", "t2@x=error"])
def test_bad_rules_are_rejected(text):
    with pytest.raises(ValueError):
        parse_rules(text)


def test_rates_are_validated():
    with pytest.raises(ValueError):
        FaultConfig(rates={Fault.error: 0.7, Fault.hang: 0.5})
    with pytest.raises(ValueError):
        FaultConfig(rates={Fault.permanent: 0.1})  # rules only


def test_decisions_are_deterministic_and_match_the_rates():
    cfg = FaultConfig(rates={Fault.error: 0.2, Fault.rate_limit: 0.1}, seed=7)
    ids = [f"req:t{i}:1" for i in range(4000)]
    first = [cfg.decide("t", 1, a) for a in ids]
    assert first == [cfg.decide("t", 1, a) for a in ids]  # same seed, same faults
    assert abs(first.count(Fault.error) / 4000 - 0.2) < 0.03
    assert abs(first.count(Fault.rate_limit) / 4000 - 0.1) < 0.03
    other = [FaultConfig(rates=cfg.rates, seed=8).decide("t", 1, a) for a in ids]
    assert other != first  # another seed, another pattern


def test_from_env_and_disabled_by_default():
    assert not FaultConfig.from_env({}).enabled
    cfg = FaultConfig.from_env({"SIM_FAULT_ERROR_RATE": "0.2", "SIM_FAULT_HANG_RATE": "0.05", "SIM_FAULTS": "t1=crash",
                                "SIM_FAULT_SEED": "3", "SIM_FAULT_RETRY_AFTER_MS": "500"})
    assert cfg.enabled and cfg.rates == {Fault.error: 0.2, Fault.hang: 0.05} and cfg.seed == 3
    assert cfg.retry_after_ms == 500 and cfg.rules == (FaultRule("t1", None, Fault.crash),)
    assert WorkerConfig.from_env({"SIM_FAULT_ERROR_RATE": "0.1"}).faults.rates == {Fault.error: 0.1}


# --- sim executor faults ---------------------------------------------------------------


def run(executor, message):
    return asyncio.run(executor.run(ExecutionContext(message=message, inputs={})))


def sim(rule, **kw):
    return SimExecutor(faults=FaultConfig(rules=parse_rules(rule), retry_after_ms=750, straggler_factor=4), **kw)


def test_rate_limit_is_a_retryable_failure_with_retry_after():
    with pytest.raises(RetryableError) as e:
        run(sim("t1=rate_limit"), msg())
    assert e.value.retry_after_ms == 750


def test_permanent_and_error_faults():
    with pytest.raises(PermanentError):
        run(sim("t1=permanent"), msg())
    t0 = time.monotonic()
    with pytest.raises(RetryableError):
        run(sim("t1=error"), msg(seconds=0.2))
    assert 0.08 < time.monotonic() - t0 < 0.2  # fails part-way through


def test_straggler_takes_longer():
    t0 = time.monotonic()
    out = run(sim("t1=straggler"), msg(seconds=0.05))
    assert time.monotonic() - t0 >= 0.2 and out.output["simulated_seconds"] == pytest.approx(0.2)


def test_crash_calls_the_crash_hook_and_never_returns():
    crashed = []
    executor = sim("t1=crash", crash=lambda: crashed.append(True))

    async def body():
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(executor.run(ExecutionContext(message=msg(seconds=0.1), inputs={})), 0.5)

    asyncio.run(body())
    assert crashed == [True]


def test_no_faults_by_default():
    assert run(SimExecutor(), msg(seconds=0)).result_kind == "output"


# --- worker: hang -> deadline, abort -------------------------------------------------------


def with_redis(redis_url, body):
    async def main():
        r = Redis.from_url(redis_url, decode_responses=True)
        try:
            await body(r)
        finally:
            await r.aclose()
    asyncio.run(main())


async def events(r):
    return [TaskEvent.model_validate_json(f[STREAM_FIELD]).root for _, f in await r.xrange(keys.EVENTS_STREAM)]


def test_a_hung_attempt_is_abandoned_at_its_deadline(redis_url):
    async def body(r):
        registry = ExecutorRegistry()
        registry.register(sim("t1=hang"))
        w = Worker(r, WorkerConfig(worker_id="w-hang", block_ms=50), registry)
        await r.xadd("tasks:sim", {STREAM_FIELD: msg(deadline_s=0.4).model_dump_json()})
        stop = asyncio.Event()
        runner = asyncio.create_task(w.run(stop))
        t0 = time.monotonic()
        while w.processed < 1 and time.monotonic() - t0 < 5:
            await asyncio.sleep(0.02)
        stop.set()
        await runner
        failed = (await events(r))[-1]
        assert failed.event == "task_failed" and failed.error_class == "retryable" and "deadline" in failed.message
        assert time.monotonic() - t0 < 2  # did not wait forever
        assert (await r.xpending("tasks:sim", keys.WORKERS_GROUP))["pending"] == 0

    with_redis(redis_url, body)


def test_abort_abandons_attempts_without_event_or_ack(redis_url):
    async def body(r):
        w = Worker(r, WorkerConfig(worker_id="w-abort", block_ms=50))
        await r.xadd("tasks:sim", {STREAM_FIELD: msg(seconds=5).model_dump_json()})
        stop = asyncio.Event()
        runner = asyncio.create_task(w.run(stop))
        while not await r.exists(keys.EVENTS_STREAM):
            await asyncio.sleep(0.02)  # task_started is out: the attempt is running
        stop.set()
        w.abort()  # the second signal
        await asyncio.wait_for(runner, 2)
        assert [e.event for e in await events(r)] == ["task_started"]  # no completion, no failure
        assert (await r.xpending("tasks:sim", keys.WORKERS_GROUP))["pending"] == 1  # left for atsd's deadline
        assert await r.get(result_key(RID, "t1")) is None

    with_redis(redis_url, body)
