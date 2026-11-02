import asyncio
import json
import time

import pytest
from redis.asyncio import Redis

from planner import keys
from planner.fake import load_fixture_plan
from planner.messages import STREAM_FIELD, TaskEvent, TaskMessage, attempt_id, result_key
from planner.schema import PlanFile
from worker.executors import ExecutionResult, Executor, PermanentError, RetryableError
from worker.main import Worker, WorkerConfig
from worker.minisched import run_plan
from worker.registry import ExecutorRegistry, default_registry

RID = "req_test0001"


def msg(task_id="t1", attempt=1, type="research", seconds=0.0, inputs=(), **extra):
    return TaskMessage(
        request_id=RID, task_id=task_id, attempt=attempt, attempt_id=attempt_id(RID, task_id, attempt),
        type=type, instruction=f"do {task_id}", inputs={d: result_key(RID, d) for d in inputs},
        deadline_ms=int(time.time() * 1000) + 60_000, estimated_seconds=seconds, **extra,
    )


def config(**overrides):
    cfg = WorkerConfig(worker_id="w-test", block_ms=50, heartbeat_s=0.05)
    for k, v in overrides.items():
        setattr(cfg, k, v)
    return cfg


async def enqueue(r, message, stream="tasks:sim"):
    await r.xadd(stream, {STREAM_FIELD: message.model_dump_json()})


async def events(r):
    """Every event on task:events, validated against contract #2."""
    return [TaskEvent.model_validate_json(f[STREAM_FIELD]).root for _, f in await r.xrange(keys.EVENTS_STREAM)]


async def run_until(r, n, cfg=None, registry=None, workers=1, timeout=10):
    """Run `workers` workers until together they have handled n attempts, then stop them."""
    stop = asyncio.Event()
    ws = [Worker(r, cfg or config(worker_id=f"w-{i}"), registry) for i in range(workers)]

    async def watch():
        while sum(w.processed for w in ws) < n:
            await asyncio.sleep(0.01)
        stop.set()

    await asyncio.wait_for(asyncio.gather(watch(), *(w.run(stop) for w in ws)), timeout)
    return ws


def with_redis(fn):
    """Run an async test body with a fresh async Redis client."""
    def wrapper(redis_url):
        async def main():
            r = Redis.from_url(redis_url, decode_responses=True)
            try:
                await fn(r)
            finally:
                await r.aclose()
        asyncio.run(main())
    wrapper.__name__ = fn.__name__
    return wrapper


@with_redis
async def test_happy_path_commits_reports_and_acks(r):
    await enqueue(r, msg(seconds=0.05))
    await run_until(r, 1, config())
    started, completed = await events(r)
    assert started.event == "task_started" and started.attempt_id == f"{RID}:t1:1"
    assert completed.event == "task_completed" and completed.committed is True
    assert completed.result_key == result_key(RID, "t1")
    assert completed.exec_ms >= 50 and completed.tokens_in is None
    stored = json.loads(await r.get(result_key(RID, "t1")))
    assert stored["content"].startswith("[sim] research t1")
    pending = await r.xpending("tasks:sim", keys.WORKERS_GROUP)
    assert pending["pending"] == 0


@with_redis
async def test_inputs_are_loaded_by_task_id(r):
    await r.set(result_key(RID, "t1"), json.dumps({"content": "a"}))
    await r.set(result_key(RID, "t2"), json.dumps({"content": "b"}))
    await enqueue(r, msg("t3", type="compare", inputs=("t1", "t2")))
    await run_until(r, 1, config())
    assert json.loads(await r.get(result_key(RID, "t3")))["inputs"] == ["t1", "t2"]


@with_redis
async def test_missing_input_fails_permanently(r):
    await enqueue(r, msg("t3", inputs=("t1",)))
    await run_until(r, 1, config())
    failed = (await events(r))[-1]
    assert failed.event == "task_failed" and failed.error_class == "permanent"
    assert "t1" in failed.message
    assert await r.get(result_key(RID, "t3")) is None


@with_redis
async def test_second_commit_loses_and_keeps_first_result(r):
    await r.set(result_key(RID, "t1"), json.dumps({"content": "first"}))
    await enqueue(r, msg(attempt=2))
    await run_until(r, 1, config())
    completed = (await events(r))[-1]
    assert completed.committed is False
    assert json.loads(await r.get(result_key(RID, "t1"))) == {"content": "first"}


class Raising(Executor):
    capability = "sim"

    def __init__(self, exc):
        self.exc = exc

    async def run(self, ctx):
        raise self.exc


@pytest.mark.parametrize("exc, error_class, retry_after", [
    (RetryableError("429 rate limited", retry_after_ms=4000), "retryable", 4000),
    (PermanentError("400 bad request"), "permanent", None),
    (ValueError("bug"), "retryable", None),  # unclassified -> retryable, atsd caps attempts
])
def test_executor_errors_become_task_failed(redis_url, exc, error_class, retry_after):
    @with_redis
    async def body(r):
        registry = ExecutorRegistry()
        registry.register(Raising(exc))
        await enqueue(r, msg())
        await run_until(r, 1, config(), registry)
        failed = (await events(r))[-1]
        assert (failed.event, failed.error_class, failed.retry_after_ms) == ("task_failed", error_class, retry_after)
        assert await r.get(result_key(RID, "t1")) is None
        assert (await r.xpending("tasks:sim", keys.WORKERS_GROUP))["pending"] == 0

    body(redis_url)


@with_redis
async def test_llm_task_without_executor_fails_permanently(r):
    await enqueue(r, msg(), stream="tasks:llm")
    await run_until(r, 1, config(capabilities=("llm",)), default_registry(capabilities=("sim",)))
    failed = (await events(r))[-1]
    assert failed.error_class == "permanent" and "no executor" in failed.message


@with_redis
async def test_malformed_message_is_reported_and_acked(r):
    bad = json.loads(msg().model_dump_json())
    bad["surprise"] = 1  # unknown field: violates contract #2
    await r.xadd("tasks:sim", {STREAM_FIELD: json.dumps(bad)})
    await r.xadd("tasks:sim", {STREAM_FIELD: "not json"})
    await enqueue(r, msg("t2"))
    # Entries are read in order with concurrency 1, so once t2 is done both bad ones were handled.
    await run_until(r, 1, config())
    evs = await events(r)
    assert [e.event for e in evs] == ["task_failed", "task_started", "task_completed"]
    assert evs[0].error_class == "permanent" and evs[0].attempt_id == f"{RID}:t1:1"
    assert (await r.xpending("tasks:sim", keys.WORKERS_GROUP))["pending"] == 0


@with_redis
async def test_heartbeat_registers_and_clears_worker(r):
    stop = asyncio.Event()
    w = Worker(r, config(), default_registry())
    task = asyncio.create_task(w.run(stop))
    await asyncio.sleep(0.2)
    entry = await r.hgetall(keys.worker_key("w-test"))
    assert entry["capabilities"] == "sim" and entry["in_flight"] == "0"
    assert 0 < await r.ttl(keys.worker_key("w-test")) <= 10
    stop.set()
    await task
    assert await r.exists(keys.worker_key("w-test")) == 0


@with_redis
async def test_concurrency_runs_attempts_in_parallel(r):
    for i in range(1, 4):
        await enqueue(r, msg(f"t{i}", seconds=0.3))
    t0 = time.monotonic()
    await run_until(r, 3, config(concurrency=3))
    assert time.monotonic() - t0 < 0.8  # sequential would be 0.9 s


@with_redis
async def test_sim_time_scale(r):
    await enqueue(r, msg(seconds=10.0))
    await run_until(r, 1, config(sim_time_scale=0.01))
    assert json.loads(await r.get(result_key(RID, "t1")))["simulated_seconds"] == pytest.approx(0.1)


def test_config_from_env():
    cfg = WorkerConfig.from_env({"REDIS_URL": "redis://x:1/2", "WORKER_ID": "w-9",
                                 "WORKER_CAPABILITIES": "sim, llm", "WORKER_CONCURRENCY": "4",
                                 "SIM_TIME_SCALE": "0.1"})
    assert (cfg.redis_url, cfg.worker_id, cfg.capabilities, cfg.concurrency, cfg.sim_time_scale) == \
        ("redis://x:1/2", "w-9", ("sim", "llm"), 4, 0.1)
    with pytest.raises(ValueError):
        WorkerConfig.from_env({"WORKER_CONCURRENCY": "0"})


# --- end to end with the Python stand-in for atsd ------------------------------------------


def scaled(name, scale):
    plan = load_fixture_plan(name)
    for t in plan["tasks"]:
        t["hints"]["estimated_seconds"] *= scale
    return PlanFile(request_id=f"req_{name}", created_at="2026-10-11T00:00:00Z", plan=plan)


@with_redis
async def test_failed_root_cancels_descendants_with_attempt_zero(r):
    registry = ExecutorRegistry()
    registry.register(Raising(PermanentError("boom")))
    pf = scaled("chain", 0.01)  # t1 -> t2 -> t3
    stop = asyncio.Event()
    runner = asyncio.create_task(Worker(r, config(), registry).run(stop))
    report = await asyncio.wait_for(run_plan(r, pf), 10)
    stop.set()
    await runner

    assert not report.succeeded and "t1 failed (permanent)" in report.error
    status = await r.hgetall(keys.request_key(pf.request_id))
    assert status["status"] == "FAILED"
    assert json.loads(status["task:t1"]) == {"state": "FAILED", "attempt": 1}
    assert json.loads(status["task:t2"]) == {"state": "CANCELLED", "attempt": 0}  # never dispatched
    assert json.loads(status["task:t3"]) == {"state": "CANCELLED", "attempt": 0}


@with_redis
async def test_company_research_on_three_workers_respects_every_edge(r):
    pf = scaled("company_research", 0.01)  # 65 s critical path -> 0.65 s
    stop = asyncio.Event()
    workers = [Worker(r, config(worker_id=f"w-{i}")) for i in range(3)]
    runners = [asyncio.create_task(w.run(stop)) for w in workers]
    report = await asyncio.wait_for(run_plan(r, pf), 10)
    stop.set()
    await asyncio.gather(*runners)

    assert report.succeeded
    for t in pf.plan.tasks:
        for dep in t.depends_on:
            assert report.tasks[t.id].started_ms >= report.tasks[dep].ended_ms
    assert 0.6 <= report.makespan_s < 1.5  # critical path 0.65 s plus overhead
    assert len({run.worker_id for run in report.tasks.values()}) >= 2  # work was spread
    status = await r.hgetall(keys.request_key(pf.request_id))
    assert status["status"] == "SUCCEEDED" and status["sink"] == "t6"
    assert json.loads(status["task:t6"])["state"] == "COMPLETED"


@with_redis
async def test_existing_result_is_reported_without_running_again(r):
    calls = []

    class Counting(Executor):
        capability = "sim"

        async def run(self, ctx):
            calls.append(ctx.message.attempt_id)

    registry = ExecutorRegistry()
    registry.register(Counting())
    await r.set(result_key(RID, "t1"), json.dumps({"content": "from attempt 1"}))
    await enqueue(r, msg(attempt=2))
    await run_until(r, 1, config(), registry)
    done = (await events(r))[-1]
    assert calls == [] and (done.event, done.committed, done.exec_ms) == ("task_completed", False, 0)


@with_redis
async def test_attempt_past_its_deadline_is_not_run(r):
    late = msg()
    late = late.model_copy(update={"deadline_ms": int(time.time() * 1000) - 1})
    await enqueue(r, late)
    await run_until(r, 1, config())
    failed = (await events(r))[-1]
    assert (failed.event, failed.error_class) == ("task_failed", "retryable")
    assert "deadline" in failed.message and await r.get(result_key(RID, "t1")) is None


@with_redis
async def test_losing_a_race_keeps_the_winners_result(r):
    class Racing(Executor):
        capability = "sim"

        async def run(self, ctx):
            await r.set(result_key(RID, "t1"), json.dumps({"content": "winner"}))  # another attempt commits first
            return ExecutionResult(output={"content": "loser"})

    registry = ExecutorRegistry()
    registry.register(Racing())
    await enqueue(r, msg(attempt=2))
    await run_until(r, 1, config(), registry)
    assert (await events(r))[-1].committed is False
    assert json.loads(await r.get(result_key(RID, "t1"))) == {"content": "winner"}


@with_redis
async def test_sim_expand_commits_a_subplan(r):
    await enqueue(r, msg("t2", type="expand"))
    await run_until(r, 1, config())
    done = (await events(r))[-1]
    assert done.result_kind == "subplan"
    assert [t["id"] for t in json.loads(await r.get(result_key(RID, "t2")))["tasks"]] == ["s1", "s2"]


# --- Day 4: artifact spill and token accounting -------------------------------------


class Fixed(Executor):
    """Returns a fixed output (and token counts), recording the inputs it was given."""
    capability = "sim"

    def __init__(self, output, tokens=(None, None)):
        self.output, self.tokens, self.seen = output, tokens, []

    async def run(self, ctx):
        self.seen.append(ctx.inputs)
        return ExecutionResult(output=self.output, tokens_in=self.tokens[0], tokens_out=self.tokens[1])


def run_body(redis_url, body):
    async def main():
        r = Redis.from_url(redis_url, decode_responses=True)
        try:
            await body(r)
        finally:
            await r.aclose()
    asyncio.run(main())


def test_large_result_spills_to_an_artifact_and_inputs_resolve(redis_url, tmp_path):
    big = {"content": "x" * 70_000, "key_points": ["long"]}

    async def body(r):
        ex = Fixed(big)
        registry = ExecutorRegistry()
        registry.register(ex)
        cfg = config(artifacts_dir=tmp_path)
        await enqueue(r, msg("t1"))
        await enqueue(r, msg("t2", inputs=("t1",)))
        await run_until(r, 2, cfg, registry)

        pointer = json.loads(await r.get(result_key(RID, "t1")))
        assert pointer == {"artifact": f"{RID}/t1.1.json", "bytes": pointer["bytes"]} and pointer["bytes"] > 65_536
        assert json.loads((tmp_path / RID / "t1.1.json").read_text()) == big
        assert ex.seen[1] == {"t1": big}  # the dependent task got the content, not the pointer

    run_body(redis_url, body)


def test_losing_attempt_removes_its_artifact(redis_url, tmp_path):
    async def body(r):
        class LosesRace(Executor):
            capability = "sim"

            async def run(self, ctx):
                await r.set(result_key(RID, "t1"), json.dumps({"content": "winner"}))
                return ExecutionResult(output={"content": "y" * 70_000})

        registry = ExecutorRegistry()
        registry.register(LosesRace())
        await enqueue(r, msg(attempt=2))
        await run_until(r, 1, config(artifacts_dir=tmp_path), registry)
        assert (await events(r))[-1].committed is False
        assert not (tmp_path / RID / "t1.2.json").exists()

    run_body(redis_url, body)


def test_artifact_pointer_outside_the_directory_is_refused(redis_url, tmp_path):
    async def body(r):
        await r.set(result_key(RID, "t1"), json.dumps({"artifact": "../../etc/passwd", "bytes": 10}))
        await enqueue(r, msg("t2", inputs=("t1",)))
        await run_until(r, 1, config(artifacts_dir=tmp_path))
        failed = (await events(r))[-1]
        assert failed.error_class == "permanent" and "artifact" in failed.message

    run_body(redis_url, body)


def test_token_usage_is_summed_per_request_and_task(redis_url):
    async def body(r):
        registry = ExecutorRegistry()
        registry.register(Fixed({"content": "ok"}, tokens=(100, 20)))
        await enqueue(r, msg("t1"))
        await enqueue(r, msg("t2"))
        await enqueue(r, msg("t2", attempt=2))  # a second attempt costs tokens too (but does not run: result exists)
        await run_until(r, 3, config(), registry)
        usage = await r.hgetall(keys.usage_key(RID))
        assert (usage["tokens_in"], usage["tokens_out"], usage["llm_calls"]) == ("200", "40", "2")
        assert (usage["t1:tokens_in"], usage["t2:llm_calls"]) == ("100", "1")

    run_body(redis_url, body)


def test_sim_tasks_record_no_usage(redis_url):
    async def body(r):
        await enqueue(r, msg())
        await run_until(r, 1, config())
        assert await r.exists(keys.usage_key(RID)) == 0

    run_body(redis_url, body)
