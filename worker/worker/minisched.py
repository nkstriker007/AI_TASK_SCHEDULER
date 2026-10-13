"""minisched: a small Python stand-in for atsd, for testing workers and the API.

It is NOT the scheduler. atsd (C++, Person B) owns scheduling, deadlines, retries
and policies. This tool exists so the worker and API side can be exercised end to
end before atsd is ready, and as a reference for the request-status hash format
proposed in docs/messages.md. It dispatches FIFO, never retries, and fails the
request on the first task_failed.

    python -m worker.minisched examples/plans/diamond.json        # run one plan file
    python -m worker.minisched --serve                            # act as atsd: consume requests:new

Do not run --serve while atsd is running: both would consume requests:new.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from planner import keys
from planner.messages import STREAM_FIELD, TaskEvent, TaskMessage, attempt_id, parse_attempt_id, result_key
from planner.schema import PlanFile
from planner.validate import validate_plan_file

log = logging.getLogger("minisched")


@dataclass
class TaskRun:
    dispatched_ms: int | None = None
    started_ms: int | None = None
    ended_ms: int | None = None
    worker_id: str | None = None
    state: str = "WAITING"
    attempt: int = 0  # 0 until dispatched; minisched never retries, so at most 1


@dataclass
class RunReport:
    request_id: str
    succeeded: bool
    tasks: dict[str, TaskRun] = field(default_factory=dict)
    first_dispatch_ms: int = 0
    makespan_s: float = 0.0
    error: str | None = None


def _now_ms() -> int:
    return int(time.time() * 1000)


async def _last_event_id(redis: Redis) -> str:
    last = await redis.xrevrange(keys.EVENTS_STREAM, count=1)
    return last[0][0] if last else "0-0"


async def run_plan(redis: Redis, plan_file: PlanFile, capability: str = "sim",
                   attempt_timeout_s: float = 300.0, overall_timeout_s: float = 3600.0) -> RunReport:
    rid = plan_file.request_id
    tasks = {t.id: t for t in plan_file.plan.tasks}
    children: dict[str, list[str]] = {tid: [] for tid in tasks}
    remaining = {tid: len(t.depends_on) for tid, t in tasks.items()}
    for t in tasks.values():
        for dep in t.depends_on:
            children[dep].append(t.id)
    sinks = [tid for tid in tasks if not children[tid]]
    runs = {tid: TaskRun() for tid in tasks}
    report = RunReport(request_id=rid, succeeded=False, tasks=runs)
    status_key = keys.request_key(rid)

    async def set_status(status: str) -> None:
        await redis.hset(status_key, mapping={"status": status, "updated_ms": _now_ms()})

    async def set_task(tid: str) -> None:
        await redis.hset(status_key, f"task:{tid}", json.dumps({"state": runs[tid].state, "attempt": runs[tid].attempt}))

    async def dispatch(tid: str) -> None:
        t = tasks[tid]
        msg = TaskMessage(
            request_id=rid, task_id=tid, attempt=1, attempt_id=attempt_id(rid, tid, 1),
            type=t.type, instruction=t.instruction,
            inputs={d: result_key(rid, d) for d in t.depends_on},
            deadline_ms=_now_ms() + int(attempt_timeout_s * 1000),
            estimated_seconds=t.hints.estimated_seconds if t.hints else None,
        )
        runs[tid].dispatched_ms = _now_ms()
        runs[tid].attempt = 1
        runs[tid].state = "QUEUED"
        await redis.xadd(keys.task_stream(capability), {STREAM_FIELD: msg.model_dump_json()})
        await set_task(tid)

    last_id = await _last_event_id(redis)
    await redis.hset(status_key, mapping={
        "status": "RUNNING", "sink": sinks[0] if len(sinks) == 1 else "", "updated_ms": _now_ms(),
        **{f"task:{tid}": json.dumps({"state": "WAITING", "attempt": 0}) for tid in tasks},
    })
    report.first_dispatch_ms = _now_ms()
    for tid in tasks:
        if remaining[tid] == 0:
            await dispatch(tid)

    deadline = time.monotonic() + overall_timeout_s
    done = 0
    while done < len(tasks):
        if time.monotonic() > deadline:
            report.error = "timed out waiting for workers"
            break
        batches = await redis.xread({keys.EVENTS_STREAM: last_id}, block=1000)
        for _, entries in batches or []:
            for entry_id, fields in entries:
                last_id = entry_id
                event = TaskEvent.model_validate_json(fields[STREAM_FIELD]).root
                ev_rid, tid, _ = parse_attempt_id(event.attempt_id)
                if ev_rid != rid or tid not in runs or runs[tid].state in ("COMPLETED", "FAILED"):
                    continue  # another request, or a duplicate after the first completion
                run = runs[tid]
                if event.event == "task_started":
                    run.started_ms, run.worker_id, run.state = event.ts_ms, event.worker_id, "RUNNING"
                    await set_task(tid)
                elif event.event == "task_completed":
                    run.ended_ms, run.worker_id, run.state = event.ts_ms, event.worker_id, "COMPLETED"
                    run.started_ms = run.started_ms or run.dispatched_ms
                    done += 1
                    await set_task(tid)
                    for child in children[tid]:
                        remaining[child] -= 1
                        if remaining[child] == 0:
                            await dispatch(child)
                else:  # task_failed: no retries here (atsd's job)
                    run.ended_ms, run.state = event.ts_ms, "FAILED"
                    await set_task(tid)
                    report.error = f"{tid} failed ({event.error_class}): {event.message}"
                    for other, r in runs.items():
                        if r.state == "WAITING":
                            r.state = "CANCELLED"
                            await set_task(other)
                    await set_status("FAILED")
                    return _finish(report)

    report.succeeded = report.error is None
    await set_status("SUCCEEDED" if report.succeeded else "FAILED")
    return _finish(report)


def _finish(report: RunReport) -> RunReport:
    ends = [r.ended_ms for r in report.tasks.values() if r.ended_ms]
    if ends:
        report.makespan_s = (max(ends) - report.first_dispatch_ms) / 1000
    return report


def format_timeline(report: RunReport, plan_file: PlanFile) -> str:
    t0 = report.first_dispatch_ms
    lines = [f"request {report.request_id}: {len(report.tasks)} tasks"]
    for t in plan_file.plan.tasks:
        r = report.tasks[t.id]
        if r.started_ms is None:
            lines.append(f"  {t.id:<5} {r.state:<9} {t.title}")
            continue
        end = f"{(r.ended_ms - t0) / 1000:7.2f}" if r.ended_ms else "      -"
        lines.append(f"  {t.id:<5} {r.state:<9} start {(r.started_ms - t0) / 1000:7.2f}  end {end}"
                     f"  ({r.worker_id})  {t.title}")
    outcome = "SUCCEEDED" if report.succeeded else f"FAILED: {report.error}"
    lines.append(f"makespan {report.makespan_s:.2f}s (wall clock) | outcome {outcome}")
    return "\n".join(lines)


async def serve(redis: Redis, capability: str, stop: asyncio.Event) -> None:
    """Consume requests:new like atsd would and run each request concurrently."""
    try:
        await redis.xgroup_create(keys.REQUESTS_STREAM, keys.SCHEDULER_GROUP, id="0", mkstream=True)
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise
    running: set[asyncio.Task] = set()
    while not stop.is_set():
        batches = await redis.xreadgroup(keys.SCHEDULER_GROUP, "minisched", {keys.REQUESTS_STREAM: ">"},
                                         count=10, block=1000)
        for _, entries in batches or []:
            for entry_id, fields in entries:
                result = validate_plan_file(json.loads(fields[STREAM_FIELD]))
                await redis.xack(keys.REQUESTS_STREAM, keys.SCHEDULER_GROUP, entry_id)
                if not result.ok:
                    log.error("rejected plan file: %s", "; ".join(map(str, result.errors)))
                    continue
                pf = result.value
                log.info("request %s: %d tasks", pf.request_id, len(pf.plan.tasks))
                task = asyncio.create_task(_run_and_log(redis, pf, capability))
                running.add(task)
                task.add_done_callback(running.discard)
    await asyncio.gather(*running, return_exceptions=True)


async def _run_and_log(redis: Redis, pf: PlanFile, capability: str) -> None:
    report = await run_plan(redis, pf, capability)
    print(format_timeline(report, pf), flush=True)


async def _main(args: argparse.Namespace) -> int:
    redis = Redis.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True)
    try:
        if args.serve:
            stop = asyncio.Event()
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, stop.set)
            await serve(redis, args.capability, stop)
            return 0
        result = validate_plan_file(json.loads(Path(args.plan).read_text()))
        if not result.ok:
            print("\n".join(map(str, result.errors)), file=sys.stderr)
            return 1
        report = await run_plan(redis, result.value, args.capability)
        print(format_timeline(report, result.value))
        return 0 if report.succeeded else 2
    finally:
        await redis.aclose()


def main() -> None:
    parser = argparse.ArgumentParser(description="Python stand-in for atsd (testing only).")
    parser.add_argument("plan", nargs="?", help="plan file to run once")
    parser.add_argument("--serve", action="store_true", help="consume requests:new until Ctrl-C")
    parser.add_argument("--capability", default="sim", help="stream to dispatch to (default: sim)")
    args = parser.parse_args()
    if bool(args.plan) == args.serve:
        parser.error("give a plan file or --serve")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    sys.exit(asyncio.run(_main(args)))


if __name__ == "__main__":
    main()
