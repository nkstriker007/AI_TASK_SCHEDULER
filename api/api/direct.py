"""Fast path (D58): answer a simple request with one LLM call, without the planner or atsd.

A request is simple when it is short and names no several things to look up or compare; the
planner's call would then be most of its latency (E7). The answer is stored under the same keys
as a workflow request (plan:<id> with a one-task plan, request:<id> in atsd's format, D25,
result:<id>:t1, usage:<id>), so GET /requests/{id} and /report work unchanged. It is never put
on requests:new, so atsd does not see it.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import time
from collections.abc import Callable
from pathlib import Path

from redis.asyncio import Redis

from planner import keys
from planner.cli import make_plan_file
from planner.messages import TaskMessage, attempt_id, result_key
from planner.schema import ExecutionPlan, PlanFile, PlannedTask, TaskType
from worker.executors.base import ExecutionContext, Executor, RetryableError
from worker.results import commit_result, record_usage

TASK_ID = "t1"
MAX_ATTEMPTS = 4
ATTEMPT_TIMEOUT_S = 120
MAX_SIMPLE_WORDS = 30
FAST_PATH_ENV = "ATS_FAST_PATH"  # "0" turns automatic routing off; mode "direct" still works
# Several things to look up or compare, or an open-ended list: the workflow's job.
_MULTI = re.compile(r"\b(and|or|vs|versus|compare|comparing|comparison|each|every|both|between|list|top|all|"
                    r"several|multiple|rank|ranking)\b|[,;]", re.IGNORECASE)


def fast_path_enabled(env: dict[str, str] | None = None) -> bool:
    return (os.environ if env is None else env).get(FAST_PATH_ENV, "1") != "0"


def is_simple(text: str) -> bool:
    """Short and about one thing: worth one direct call instead of a plan."""
    return len(text.split()) <= MAX_SIMPLE_WORDS and not _MULTI.search(text)


def default_executor() -> Executor:
    from worker.executors.llm import LLMExecutor, load_direct_prompt, model_for

    # The whole answer comes from this one call, so it uses the main model, not the small one
    # the per-type map gives report tasks.
    executor = LLMExecutor(TaskType.report, model=model_for(TaskType.research))
    executor.system_prompt = load_direct_prompt()
    return executor


def direct_plan_file(text: str) -> PlanFile:
    task = PlannedTask(id=TASK_ID, type=TaskType.report, title="Direct answer", instruction=text[:2000], depends_on=[])
    return make_plan_file(ExecutionPlan(schema_version=1, summary=text[:200], tasks=[task]))


async def _set_state(redis: Redis, rid: str, status: str, state: str, attempt: int, error: str | None = None) -> None:
    fields = {"status": status, "sink": TASK_ID, "updated_ms": str(int(time.time() * 1000)),
              f"task:{TASK_ID}": json.dumps({"state": state, "attempt": attempt})}
    if error:
        fields["error"] = error
    await redis.hset(keys.request_key(rid), mapping=fields)


async def submit(redis: Redis, text: str, executor_factory: Callable[[], Executor], artifacts_dir: Path | None,
                 tasks: set[asyncio.Task]) -> PlanFile:
    """Store the request, start answering it in the background, return its plan file."""
    plan_file = direct_plan_file(text)
    rid = plan_file.request_id
    if not await redis.set(keys.plan_key(rid), plan_file.model_dump_json(exclude_none=True), nx=True):
        raise RuntimeError("request id collision")
    await _set_state(redis, rid, "RUNNING", "RUNNING", 1)
    task = asyncio.create_task(_answer(redis, rid, text, executor_factory, artifacts_dir))
    tasks.add(task)
    task.add_done_callback(tasks.discard)
    return plan_file


async def _answer(redis: Redis, rid: str, text: str, executor_factory: Callable[[], Executor],
                  artifacts_dir: Path | None) -> None:
    executor = executor_factory()
    error = "no attempt made"
    for attempt in range(1, MAX_ATTEMPTS + 1):
        message = TaskMessage(request_id=rid, task_id=TASK_ID, attempt=attempt,
                              attempt_id=attempt_id(rid, TASK_ID, attempt), type=TaskType.report, instruction=text,
                              deadline_ms=int((time.time() + ATTEMPT_TIMEOUT_S) * 1000))
        await _set_state(redis, rid, "RUNNING", "RUNNING", attempt)
        try:
            result = await executor.run(ExecutionContext(message=message, inputs={}))
        except RetryableError as exc:  # rate limit, timeout, malformed output: wait as atsd would (D45)
            error = str(exc)
            if attempt < MAX_ATTEMPTS:
                await asyncio.sleep(max((exc.retry_after_ms or 0) / 1000, 2 ** attempt))
            continue
        except Exception as exc:  # PermanentError or anything unexpected ends the request
            error = f"{type(exc).__name__}: {exc}"
            break
        await record_usage(redis, message, result.tokens_in, result.tokens_out)
        await commit_result(redis, result_key(rid, TASK_ID), result.output, message=message,
                            artifacts_root=artifacts_dir)
        await _set_state(redis, rid, "SUCCEEDED", "COMPLETED", attempt)
        return
    await _set_state(redis, rid, "FAILED", "FAILED", MAX_ATTEMPTS, error[:500])
