"""Result store: load dependency results, commit this task's result (design doc 8.5, D11).

Results are written once with SET NX. If the key already exists, an earlier
attempt of the same task won and this attempt's output is discarded; because
tasks are pure, either output is a valid result.
"""

from __future__ import annotations

import json
from typing import Any

from redis.asyncio import Redis

from planner.messages import TaskMessage

from .executors import PermanentError


async def load_inputs(redis: Redis, message: TaskMessage) -> dict[str, Any]:
    """Dependency task id -> its committed result, parsed from JSON."""
    if not message.inputs:
        return {}
    deps = list(message.inputs)
    raw_values = await redis.mget([message.inputs[d] for d in deps])
    missing = [d for d, raw in zip(deps, raw_values) if raw is None]
    if missing:
        # atsd dispatches a task only after every dependency committed, so this is a
        # protocol bug, not something a retry would fix.
        raise PermanentError(f"missing input results for {', '.join(missing)}")
    try:
        return {d: json.loads(raw) for d, raw in zip(deps, raw_values)}
    except json.JSONDecodeError as exc:
        raise PermanentError(f"input result is not valid JSON: {exc.msg}") from exc


async def commit_result(redis: Redis, key: str, output: Any) -> bool:
    """Write the result once. True if this attempt's result was stored (first commit wins)."""
    value = json.dumps(output, ensure_ascii=False, separators=(",", ":"))
    return bool(await redis.set(key, value, nx=True))
