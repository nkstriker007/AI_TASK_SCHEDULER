"""Request status and final output assembly.

Merges the submitted plan (plan:<req>, written by the API), the runtime state
(request:<req>, written by atsd, format D25), the committed results (result:<req>:<task>,
written by workers, artifact files resolved) and token usage (usage:<req>, written by
workers). Until atsd picks a request up, the request is SUBMITTED with every task WAITING.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from redis.asyncio import Redis

from planner import keys
from planner.artifacts import is_pointer, read_artifact
from planner.messages import result_key
from planner.schema import PlanFile

SUBMITTED = "SUBMITTED"
FINISHED = ("SUCCEEDED", "FAILED", "REJECTED")
USAGE_FIELDS = ("tokens_in", "tokens_out", "llm_calls")


def _sinks(plan_file: PlanFile) -> list[str]:
    has_child = {d for t in plan_file.plan.tasks for d in t.depends_on}
    return [t.id for t in plan_file.plan.tasks if t.id not in has_child]


def _task_state(raw: str | None) -> dict[str, Any]:
    if raw is None:
        return {"state": "WAITING", "attempt": 0}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {"state": "UNKNOWN", "attempt": 0}
    return {"state": value.get("state", "UNKNOWN"), "attempt": value.get("attempt", 0)}


async def load_result(redis: Redis, request_id: str, task_id: str, artifacts_root: Path | None) -> Any:
    """A committed result: parsed JSON, artifact resolved; text if it is not JSON; None if absent."""
    raw = await redis.get(result_key(request_id, task_id))
    if raw is None:
        return None
    try:
        value = json.loads(raw)  # results are JSON (contract #2)
    except json.JSONDecodeError:
        return raw  # written before results were JSON: return the text, never a 500
    if is_pointer(value):
        if artifacts_root is None:
            return {**value, "error": "artifact directory not configured"}
        try:
            return read_artifact(artifacts_root, value)
        except (OSError, ValueError) as exc:
            return {**value, "error": f"cannot read artifact: {exc}"}
    return value


def _usage(raw: dict[str, str]) -> dict[str, Any] | None:
    if not raw:
        return None
    usage: dict[str, Any] = {f: int(raw.get(f, 0)) for f in USAGE_FIELDS}
    by_task: dict[str, dict[str, int]] = {}
    for field, value in raw.items():
        if ":" in field:
            tid, name = field.rsplit(":", 1)
            by_task.setdefault(tid, {})[name] = int(value)
    usage["by_task"] = dict(sorted(by_task.items()))
    return usage


async def request_status(redis: Redis, request_id: str, artifacts_root: Path | None = None,
                         include_outputs: bool = False) -> dict[str, Any] | None:
    raw_plan = await redis.get(keys.plan_key(request_id))
    if raw_plan is None:
        return None
    plan_file = PlanFile.model_validate_json(raw_plan)
    runtime = await redis.hgetall(keys.request_key(request_id))
    status = runtime.get("status", SUBMITTED)
    sinks = _sinks(plan_file)
    sink = runtime.get("sink") or (sinks[0] if len(sinks) == 1 else None)

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
    completed = [t["id"] for t in tasks if t["state"] == "COMPLETED"]

    # Final output: the single sink's result; with several sinks, each of them by task id.
    output = None
    if sink and sink in completed:
        output = await load_result(redis, request_id, sink, artifacts_root)
    elif not sink and status == "SUCCEEDED":
        output = {s: await load_result(redis, request_id, s, artifacts_root) for s in sinks}

    result: dict[str, Any] = {
        "request_id": request_id,
        "created_at": plan_file.created_at,
        "summary": plan_file.plan.summary,
        "status": status,
        "error": runtime.get("error"),
        "updated_ms": int(runtime["updated_ms"]) if "updated_ms" in runtime else None,
        "sink": sink,
        "tasks": tasks,
        "output": output,
        "usage": _usage(await redis.hgetall(keys.usage_key(request_id))),
    }
    if include_outputs:
        result["outputs"] = {tid: await load_result(redis, request_id, tid, artifacts_root) for tid in completed}
    return result


def _section(value: Any) -> str:
    if isinstance(value, dict) and isinstance(value.get("content"), str):
        text = value["content"].strip()
        points = [p for p in value.get("key_points") or [] if isinstance(p, str)]
        if points:
            text += "\n\n**Key points**\n\n" + "\n".join(f"- {p}" for p in points)
        return text
    return value if isinstance(value, str) else "```json\n" + json.dumps(value, indent=2) + "\n```"


def render_report(status: dict[str, Any]) -> str:
    """The final output as one Markdown document."""
    output = status["output"]
    if status["sink"] or not isinstance(output, dict):
        body = _section(output)
    else:  # several final tasks: one section each
        titles = {t["id"]: t["title"] or t["id"] for t in status["tasks"]}
        body = "\n\n".join(f"## {titles.get(tid, tid)}\n\n{_section(v)}" for tid, v in output.items())
    return f"# {status['summary']}\n\n{body}\n"
