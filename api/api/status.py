"""Request status: merge the submitted plan (plan:{req}, written by the API) with the
runtime state (request:{req}, written by atsd) and the final result.

The request:{req} hash format is proposed in docs/messages.md ("Request intake and
status"). Until atsd picks a request up, the hash does not exist and the request
is reported as SUBMITTED with every task WAITING.
"""

from __future__ import annotations

import json
from typing import Any

from redis.asyncio import Redis

from planner import keys
from planner.messages import result_key
from planner.schema import PlanFile

SUBMITTED = "SUBMITTED"


def _sink(plan_file: PlanFile) -> str | None:
    has_child = {d for t in plan_file.plan.tasks for d in t.depends_on}
    sinks = [t.id for t in plan_file.plan.tasks if t.id not in has_child]
    return sinks[0] if len(sinks) == 1 else None


def _task_state(raw: str | None) -> dict[str, Any]:
    if raw is None:
        return {"state": "WAITING", "attempt": 0}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {"state": "UNKNOWN", "attempt": 0}
    return {"state": value.get("state", "UNKNOWN"), "attempt": value.get("attempt", 0)}


async def request_status(redis: Redis, request_id: str) -> dict[str, Any] | None:
    raw_plan = await redis.get(keys.plan_key(request_id))
    if raw_plan is None:
        return None
    plan_file = PlanFile.model_validate_json(raw_plan)
    runtime = await redis.hgetall(keys.request_key(request_id))
    status = runtime.get("status", SUBMITTED)
    sink = runtime.get("sink") or _sink(plan_file)

    tasks = []
    for t in plan_file.plan.tasks:
        tasks.append({"id": t.id, "type": t.type.value, "title": t.title, "depends_on": t.depends_on,
                      **_task_state(runtime.get(f"task:{t.id}"))})
    # Tasks spliced in by an expand task (t2.1, t2.2, ...) exist only in the runtime hash.
    planned = {t.id for t in plan_file.plan.tasks}
    for field in sorted(f for f in runtime if f.startswith("task:")):
        tid = field.removeprefix("task:")
        if tid not in planned:
            tasks.append({"id": tid, "type": None, "title": None, "depends_on": None,
                          **_task_state(runtime[field])})

    output = None
    sink_state = next((t["state"] for t in tasks if t["id"] == sink), None)
    if sink and sink_state == "COMPLETED":
        raw_result = await redis.get(result_key(request_id, sink))
        if raw_result is not None:
            output = json.loads(raw_result)

    return {
        "request_id": request_id,
        "created_at": plan_file.created_at,
        "summary": plan_file.plan.summary,
        "status": status,
        "updated_ms": int(runtime["updated_ms"]) if "updated_ms" in runtime else None,
        "sink": sink,
        "tasks": tasks,
        "output": output,
    }
