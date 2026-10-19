"""LLM executor: one Groq chat completion per attempt (design doc 8.9, D27).

Moved here from planner/planner/executors.py when worker/ became the single worker.
The behaviour is the same, but the call is async (AsyncGroq) so a slow model call
does not block the worker's loop, its other attempts or its heartbeat.

- One call per attempt and no SDK retries (max_retries=0): atsd owns retries (8.6, D12).
- The request times out when the attempt's deadline passes, and no call is made
  once it has passed.
- Dependency results go into the user message in task-number order (t2 < t2.1 < t10).
- Research answers come from model knowledge (D15).
- Expand tasks use a Groq strict-mode subplan schema; the answer is then checked by
  validate_subplan, the same rule set as any subplan (8.11).
"""

from __future__ import annotations

import json
import os
import time
from importlib import resources
from typing import Any

from planner.messages import TaskMessage
from planner.schema import TaskType
from planner.validate import validate_subplan

from .base import ExecutionContext, ExecutionResult, Executor, PermanentError, RetryableError

DEFAULT_EXECUTOR_MODEL = "openai/gpt-oss-120b"
EXECUTOR_MODEL_ENV = "ATS_EXECUTOR_MODEL"

_NULLABLE_NUMBER = {"type": ["number", "null"]}

# Groq strict mode: every property required, additionalProperties false, optionals nullable.
# validate_subplan enforces the real Subplan rules afterwards (same approach as D18).
LLM_SUBPLAN_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "tasks": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "type": {"type": "string", "enum": [t.value for t in TaskType if t != TaskType.expand]},
                    "title": {"type": "string"},
                    "instruction": {"type": "string"},
                    "depends_on": {"type": "array", "items": {"type": "string"}},
                    "hints": {
                        "type": "object",
                        "properties": {"estimated_seconds": _NULLABLE_NUMBER},
                        "required": ["estimated_seconds"],
                        "additionalProperties": False,
                    },
                },
                "required": ["id", "type", "title", "instruction", "depends_on", "hints"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["tasks"],
    "additionalProperties": False,
}


def normalize_subplan(raw: Any) -> Any:
    """Drop strict-mode nulls so validate_subplan sees the real Subplan shape."""
    if isinstance(raw, dict) and isinstance(raw.get("tasks"), list):
        for t in raw["tasks"]:
            if isinstance(t, dict) and isinstance(t.get("hints"), dict):
                hints = {k: v for k, v in t["hints"].items() if v is not None}
                if hints:
                    t["hints"] = hints
                else:
                    del t["hints"]
    return raw


def load_executor_prompt() -> str:
    return resources.files("worker").joinpath("prompts/executor_system.md").read_text()


def input_text(value: Any) -> str:
    """A dependency's committed result as prompt text: its "content", or JSON for anything else."""
    if isinstance(value, dict) and isinstance(value.get("content"), str):
        return value["content"]
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False)


def _task_sort_key(task_id: str) -> tuple[int, ...]:
    return tuple(int(p) for p in task_id[1:].split("."))


def build_user_message(msg: TaskMessage, inputs: dict[str, Any]) -> str:
    parts = [f"Task type: {msg.type.value}", f"Task id: {msg.task_id}", "", "Instruction:", msg.instruction]
    if inputs:
        parts += ["", "Results of the tasks this one depends on:"]
        for dep in sorted(inputs, key=_task_sort_key):
            parts += ["", f"--- {dep} ---", input_text(inputs[dep])]
    if msg.type == TaskType.expand:
        parts += ["", "Return the subtasks as JSON matching the schema. Use local ids s1, s2, ... "
                      "Do not create expand tasks. At most 20 subtasks."]
    return "\n".join(parts)


def classify_provider_error(exc: Exception) -> RetryableError | PermanentError:
    """Map a Groq SDK (or network) exception to retryable / permanent (8.6)."""
    name = type(exc).__name__
    status = getattr(exc, "status_code", None)
    retry_after_ms = None
    headers = getattr(getattr(exc, "response", None), "headers", None) or {}
    try:
        if "retry-after" in headers:
            retry_after_ms = int(float(headers["retry-after"]) * 1000)
    except (TypeError, ValueError):
        retry_after_ms = None

    text = f"{name}: {exc}"
    if name in ("RateLimitError", "APITimeoutError", "APIConnectionError", "InternalServerError") \
            or status in (408, 409, 429) or (isinstance(status, int) and status >= 500) \
            or isinstance(exc, (TimeoutError, ConnectionError)):
        return RetryableError(text, retry_after_ms=retry_after_ms)
    return PermanentError(text)


class LLMExecutor(Executor):
    capability = "llm"

    def __init__(self, model: str | None = None, client: Any = None, clock=time.time):
        self.model = model or os.environ.get(EXECUTOR_MODEL_ENV, DEFAULT_EXECUTOR_MODEL)
        if client is None:
            from groq import AsyncGroq  # needs GROQ_API_KEY

            client = AsyncGroq(max_retries=0)  # retries belong to atsd, not the SDK (8.6, D12)
        self.client = client
        self.system_prompt = load_executor_prompt()
        self._clock = clock

    async def run(self, ctx: ExecutionContext) -> ExecutionResult:
        msg = ctx.message
        remaining_s = msg.deadline_ms / 1000 - self._clock()
        if remaining_s <= 0:
            raise RetryableError("deadline passed before the model call")

        kwargs: dict[str, Any] = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": self.system_prompt},
                {"role": "user", "content": build_user_message(msg, ctx.inputs)},
            ],
            "timeout": remaining_s,
        }
        if msg.type == TaskType.expand:
            kwargs["response_format"] = {
                "type": "json_schema",
                "json_schema": {"name": "subplan", "strict": True, "schema": LLM_SUBPLAN_SCHEMA},
            }

        try:
            response = await self.client.chat.completions.create(**kwargs)
        except Exception as exc:  # provider errors are classified, never swallowed
            raise classify_provider_error(exc) from exc

        usage = getattr(response, "usage", None)
        tokens_in = getattr(usage, "prompt_tokens", None)
        tokens_out = getattr(usage, "completion_tokens", None)
        content = (response.choices[0].message.content or "").strip()
        if not content:
            raise RetryableError("model returned an empty answer")

        if msg.type != TaskType.expand:
            return ExecutionResult(output={"content": content}, tokens_in=tokens_in, tokens_out=tokens_out)

        try:
            raw = normalize_subplan(json.loads(content))
        except json.JSONDecodeError as exc:
            raise RetryableError(f"expand output is not valid JSON ({exc.msg})") from exc
        result = validate_subplan(raw)
        if not result.ok:
            issues = "; ".join(str(i) for i in result.errors)
            raise RetryableError(f"expand output failed subplan validation: {issues}")
        return ExecutionResult(output=result.value.model_dump(mode="json", exclude_none=True),
                               result_kind="subplan", tokens_in=tokens_in, tokens_out=tokens_out)
