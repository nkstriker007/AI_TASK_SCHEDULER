"""Development runner: drive one plan through real workers over Redis.

    python -m planner.devrun ../examples/plans/company_research.json --stream tasks:sim

This is NOT atsd. It is the smallest thing that speaks contract #2 from the
scheduler's side, so Person A's workers can be tested end to end before atsd
exists: dispatch every task whose dependencies are committed, read task:events,
retry retryable failures (and attempts that miss their deadline) with a new
attempt number, and stop when every task is done or one has failed for good.

Deliberately missing (atsd's job): scheduling policy (it dispatches in plan
order), splicing expand subplans (an expand task's result is treated as
finished output), cancellation, persistence across restarts.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .messages import STREAM_FIELD, TaskEvent, TaskMessage, attempt_id, parse_attempt_id, result_key
from .validate import validate_plan_file
from .worker import EVENTS_STREAM


@dataclass
class TaskRun:
    id: str
    type: str
    instruction: str
    depends_on: list[str]
    estimated_seconds: float | None
    state: str = "waiting"           # waiting | running | done | failed
    attempt: int = 0
    deadline_ms: int = 0
    not_before_ms: int = 0           # retry backoff
    committed_by: int | None = None  # attempt whose write won
    history: list[str] = field(default_factory=list)


@dataclass
class RunReport:
    request_id: str
    ok: bool
    seconds: float
    tasks: dict[str, TaskRun]
    duplicates: int


def run_plan(redis: Any, plan_file: dict, stream: str = "tasks:sim", *, request_id: str | None = None,
             max_attempts: int = 3, deadline_s: float = 120.0, backoff_ms: int = 500,
             timeout_s: float = 600.0, events_stream: str = EVENTS_STREAM, verbose: bool = True) -> RunReport:
    result = validate_plan_file(plan_file)
    if not result.ok:
        raise ValueError("plan is invalid: " + "; ".join(str(i) for i in result.errors))
    plan = result.value.plan
    request_id = request_id or f"dev_{uuid.uuid4().hex[:10]}"
    tasks = {t.id: TaskRun(t.id, t.type.value, t.instruction, list(t.depends_on),
                           t.hints.estimated_seconds if t.hints else None) for t in plan.tasks}
    say = print if verbose else (lambda *a, **k: None)

    # Read events from "now": everything this run cares about is published after this point.
    last_event_id = redis.xadd(events_stream, {STREAM_FIELD: "{}"}, maxlen=None)
    redis.xdel(events_stream, last_event_id)
    t0 = time.monotonic()
    duplicates = 0

    def now_ms() -> int:
        return int(time.time() * 1000)

    def dispatch(t: TaskRun) -> None:
        t.attempt += 1
        t.state = "running"
        t.deadline_ms = now_ms() + int(deadline_s * 1000)
        msg = TaskMessage(request_id=request_id, task_id=t.id, attempt=t.attempt,
                          attempt_id=attempt_id(request_id, t.id, t.attempt), type=t.type,
                          instruction=t.instruction, inputs={d: result_key(request_id, d) for d in t.depends_on},
                          deadline_ms=t.deadline_ms, estimated_seconds=t.estimated_seconds)
        redis.xadd(stream, {STREAM_FIELD: msg.model_dump_json()})
        t.history.append(f"dispatched #{t.attempt}")
        say(f"[{time.monotonic() - t0:6.1f}s] dispatch {t.id} attempt {t.attempt}")

    def retry_or_fail(t: TaskRun, why: str, retryable: bool, retry_after_ms: int | None = None) -> None:
        if retryable and t.attempt < max_attempts:
            t.state = "waiting"
            wait = retry_after_ms if retry_after_ms is not None else backoff_ms * 2 ** (t.attempt - 1)
            t.not_before_ms = now_ms() + wait
            say(f"[{time.monotonic() - t0:6.1f}s] retry    {t.id} after {why} (wait {wait} ms)")
        else:
            t.state = "failed"
            say(f"[{time.monotonic() - t0:6.1f}s] FAILED   {t.id}: {why}")

    while True:
        if any(t.state == "failed" for t in tasks.values()) or all(t.state == "done" for t in tasks.values()):
            break
        if time.monotonic() - t0 > timeout_s:
            raise TimeoutError(f"plan did not finish within {timeout_s} s")

        for t in tasks.values():
            if t.state == "waiting" and now_ms() >= t.not_before_ms \
                    and all(tasks[d].state == "done" for d in t.depends_on):
                dispatch(t)
            elif t.state == "running" and now_ms() > t.deadline_ms:
                retry_or_fail(t, f"deadline of attempt {t.attempt}", retryable=True)

        resp = redis.xread({events_stream: last_event_id}, count=100, block=200)
        for _s, entries in resp or []:
            for entry_id, fields in entries:
                last_event_id = entry_id
                try:
                    ev = TaskEvent.model_validate_json(fields.get(STREAM_FIELD, "")).root
                except Exception:
                    continue
                rid, tid, att = parse_attempt_id(ev.attempt_id)
                if rid != request_id or tid not in tasks:
                    continue
                t = tasks[tid]
                if ev.event == "task_started":
                    continue
                if ev.event == "task_completed":
                    if not ev.committed:
                        duplicates += 1
                    if t.state != "done":           # first completion wins (8.5)
                        t.state = "done"
                        t.committed_by = att
                        t.history.append(f"completed by #{att}")
                        say(f"[{time.monotonic() - t0:6.1f}s] done     {tid} (attempt {att}, "
                            f"committed={ev.committed}, {ev.exec_ms} ms)")
                elif ev.event == "task_failed":
                    if t.state == "running" and att == t.attempt:   # ignore stale attempts
                        retry_or_fail(t, f"{ev.error_class}: {ev.message}",
                                      ev.error_class == "retryable", ev.retry_after_ms)

    ok = all(t.state == "done" for t in tasks.values())
    return RunReport(request_id, ok, time.monotonic() - t0, tasks, duplicates)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Run a plan file through workers (development only, not atsd).")
    p.add_argument("plan", type=Path)
    p.add_argument("--stream", default="tasks:sim", choices=["tasks:sim", "tasks:llm"])
    p.add_argument("--redis-url", default=os.environ.get("ATS_REDIS_URL", "redis://localhost:6379/0"))
    p.add_argument("--deadline-s", type=float, default=120.0, help="per-attempt deadline")
    p.add_argument("--max-attempts", type=int, default=3)
    p.add_argument("--show", default=None, help="print this task's result at the end (default: the sink)")
    a = p.parse_args(argv)

    import redis

    client = redis.Redis.from_url(a.redis_url, decode_responses=True)
    report = run_plan(client, json.loads(a.plan.read_text()), a.stream,
                      max_attempts=a.max_attempts, deadline_s=a.deadline_s)
    print(f"\n{'succeeded' if report.ok else 'FAILED'} in {report.seconds:.1f}s, request {report.request_id}, "
          f"{report.duplicates} duplicate completion(s)")
    sinks = [t for t in report.tasks if not any(t in o.depends_on for o in report.tasks.values())]
    show = a.show or (sinks[-1] if sinks else None)
    if show:
        value = client.get(result_key(report.request_id, show))
        print(f"\nresult of {show}:\n{value}")
    return 0 if report.ok else 2


if __name__ == "__main__":
    sys.exit(main())
