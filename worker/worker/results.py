"""Result store: load dependency results, commit this task's result (design doc 8.5, D11),
spill large results to artifact files (8.1), and account LLM token usage.

Results are written once with SET NX. If the key already exists, an earlier
attempt of the same task won and this attempt's output is discarded; because
tasks are pure, either output is a valid result.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from redis.asyncio import Redis

from planner import keys
from planner.artifacts import (
    ARTIFACT_LIMIT_BYTES,
    artifact_name,
    artifact_path,
    is_pointer,
    make_pointer,
    read_artifact,
    write_artifact,
)
from planner.messages import TaskMessage

from .executors import PermanentError


async def load_inputs(redis: Redis, message: TaskMessage, artifacts_root: Path | None = None) -> dict[str, Any]:
    """Dependency task id -> its committed result, parsed from JSON (artifact files resolved)."""
    if not message.inputs:
        return {}
    deps = list(message.inputs)
    raw_values = await redis.mget([message.inputs[d] for d in deps])
    missing = [d for d, raw in zip(deps, raw_values) if raw is None]
    if missing:
        # atsd dispatches a task only after every dependency committed, so this is a
        # protocol bug, not something a retry would fix.
        raise PermanentError(f"missing input results for {', '.join(missing)}")
    inputs = {}
    for d, raw in zip(deps, raw_values):
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise PermanentError(f"input result for {d} is not valid JSON: {exc.msg}") from exc
        if is_pointer(value):
            if artifacts_root is None:
                raise PermanentError(f"input result for {d} is an artifact but no artifacts directory is set")
            try:
                value = read_artifact(artifacts_root, value)
            except (OSError, ValueError) as exc:  # missing volume, bad name, corrupt file
                raise PermanentError(f"cannot read artifact for {d}: {exc}") from exc
        inputs[d] = value
    return inputs


async def commit_result(redis: Redis, key: str, output: Any, *, message: TaskMessage | None = None,
                        artifacts_root: Path | None = None, limit_bytes: int = ARTIFACT_LIMIT_BYTES) -> bool:
    """Write the result once. True if this attempt's result was stored (first commit wins).

    A result larger than `limit_bytes` goes to an artifact file and the key holds a pointer.
    If another attempt committed first, this attempt's file is removed again.
    """
    value = json.dumps(output, ensure_ascii=False, separators=(",", ":"))
    size = len(value.encode("utf-8"))
    if size <= limit_bytes or artifacts_root is None or message is None:
        return bool(await redis.set(key, value, nx=True))

    name = artifact_name(message.request_id, message.task_id, message.attempt)
    path = write_artifact(artifacts_root, name, value)
    pointer = json.dumps(make_pointer(name, size), separators=(",", ":"))
    committed = bool(await redis.set(key, pointer, nx=True))
    if not committed:
        path.unlink(missing_ok=True)
    return committed


async def record_usage(redis: Redis, message: TaskMessage, tokens_in: int | None, tokens_out: int | None) -> None:
    """Add one LLM call's tokens to usage:<request_id>, in total and for this task."""
    if tokens_in is None and tokens_out is None:
        return  # not an LLM call (sim)
    key = keys.usage_key(message.request_id)
    t = message.task_id
    async with redis.pipeline(transaction=True) as pipe:
        for field, n in (("tokens_in", tokens_in or 0), ("tokens_out", tokens_out or 0), ("llm_calls", 1)):
            pipe.hincrby(key, field, n)
            pipe.hincrby(key, f"{t}:{field}", n)
        await pipe.execute()


__all__ = ["artifact_path", "commit_result", "load_inputs", "record_usage"]
