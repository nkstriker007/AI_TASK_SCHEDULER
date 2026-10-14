"""Executors: run one TaskMessage and return its result (design doc 8.8-8.9).

An executor never touches Redis. The worker fetches the dependency results,
calls execute(), and commits the output. That keeps executors testable with
no Redis and no network.

    SimExecutor   tasks:sim   sleeps for estimated_seconds (scaled), returns a canned output
    LLMExecutor   tasks:llm   one Groq call per task; expand tasks return a Subplan

Failures are raised as ExecutionError with the class atsd needs (8.6):
retryable (rate limit, timeout, network, 5xx, malformed model output) or
permanent (bad request, auth, permission).
"""

from __future__ import annotations

import json
import os
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from importlib import resources
from typing import Any, Literal

from .messages import TaskMessage
from .schema import TaskType
from .validate import validate_subplan

DEFAULT_EXECUTOR_MODEL = "openai/gpt-oss-120b"
EXECUTOR_MODEL_ENV = "ATS_EXECUTOR_MODEL"
DEFAULT_SIM_FANOUT = 2


@dataclass(frozen=True)
class ExecResult:
    output: str                                   # stored verbatim at the result key
    kind: Literal["output", "subplan"] = "output"
    tokens_in: int | None = None
    tokens_out: int | None = None


class ExecutionError(Exception):
    """A failed attempt, classified for atsd."""

    def __init__(self, message: str, retryable: bool, retry_after_ms: int | None = None):
        super().__init__(message)
        self.message = (message or "execution failed")[:2000]
        self.retryable = retryable
        self.retry_after_ms = retry_after_ms

    @property
    def error_class(self) -> Literal["retryable", "permanent"]:
        return "retryable" if self.retryable else "permanent"


class Executor(ABC):
    name: str

    @abstractmethod
    def execute(self, msg: TaskMessage, inputs: dict[str, str]) -> ExecResult:
        """Run one attempt. `inputs` maps dependency task id -> its committed result."""


# --- sim -------------------------------------------------------------------------


class SimExecutor(Executor):
    """Deterministic stand-in for an LLM call (8.9).

    Sleeps estimated_seconds * time_scale (default_seconds when the plan gave no
    hint). An expand task returns a Subplan of `fanout` research tasks, so atsd's
    splice path can be exercised without an LLM (see D21).
    """

    name = "sim"

    def __init__(self, time_scale: float = 1.0, default_seconds: float = 1.0,
                 fanout: int = DEFAULT_SIM_FANOUT, sleep=time.sleep):
        self.time_scale = time_scale
        self.default_seconds = default_seconds
        self.fanout = fanout
        self._sleep = sleep

    def execute(self, msg: TaskMessage, inputs: dict[str, str]) -> ExecResult:
        seconds = msg.estimated_seconds if msg.estimated_seconds is not None else self.default_seconds
        if seconds * self.time_scale > 0:
            self._sleep(seconds * self.time_scale)
        if msg.type == TaskType.expand:
            subplan = {"tasks": [
                {"id": f"s{i}", "type": "research", "title": f"Item {i} from {msg.task_id}",
                 "instruction": f"Research item {i} found by {msg.task_id}.", "depends_on": []}
                for i in range(1, self.fanout + 1)
            ]}
            return ExecResult(json.dumps(subplan), kind="subplan")
        used = ", ".join(sorted(inputs)) or "no inputs"
        return ExecResult(f"[sim] {msg.type.value} {msg.task_id} (attempt {msg.attempt}) using {used}")


# --- LLM -------------------------------------------------------------------------

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
    return resources.files("planner").joinpath("prompts/executor_system.md").read_text()


def build_user_message(msg: TaskMessage, inputs: dict[str, str]) -> str:
    parts = [f"Task type: {msg.type.value}", f"Task id: {msg.task_id}", "", "Instruction:", msg.instruction]
    if inputs:
        parts += ["", "Results of the tasks this one depends on:"]
        for dep in sorted(inputs, key=_task_sort_key):
            parts += ["", f"--- {dep} ---", inputs[dep]]
    if msg.type == TaskType.expand:
        parts += ["", "Return the subtasks as JSON matching the schema. Use local ids s1, s2, ... "
                      "Do not create expand tasks. At most 20 subtasks."]
    return "\n".join(parts)


def _task_sort_key(task_id: str) -> tuple[int, ...]:
    return tuple(int(p) for p in task_id[1:].split("."))


def classify_provider_error(exc: Exception) -> ExecutionError:
    """Map a Groq SDK (or network) exception to retryable / permanent (8.6)."""
    name = type(exc).__name__
    status = getattr(exc, "status_code", None)
    retry_after_ms = None
    response = getattr(exc, "response", None)
    headers = getattr(response, "headers", None) or {}
    try:
        if "retry-after" in headers:
            retry_after_ms = int(float(headers["retry-after"]) * 1000)
    except (TypeError, ValueError):
        retry_after_ms = None

    if name in ("RateLimitError", "APITimeoutError", "APIConnectionError", "InternalServerError") \
            or status in (408, 409, 429) or (isinstance(status, int) and status >= 500) \
            or isinstance(exc, (TimeoutError, ConnectionError)):
        return ExecutionError(f"{name}: {exc}", retryable=True, retry_after_ms=retry_after_ms)
    return ExecutionError(f"{name}: {exc}", retryable=False)


class LLMExecutor(Executor):
    """One chat completion per attempt. Research answers come from model knowledge (D15)."""

    name = "llm"

    def __init__(self, model: str | None = None, client: Any = None, clock=time.time):
        self.model = model or os.environ.get(EXECUTOR_MODEL_ENV, DEFAULT_EXECUTOR_MODEL)
        if client is None:
            from groq import Groq  # lazy: tests never need the SDK configured

            client = Groq(max_retries=0)  # retries belong to atsd, not the SDK (8.6)
        self.client = client
        self.system_prompt = load_executor_prompt()
        self._clock = clock

    def execute(self, msg: TaskMessage, inputs: dict[str, str]) -> ExecResult:
        remaining_s = msg.deadline_ms / 1000 - self._clock()
        if remaining_s <= 0:
            raise ExecutionError("deadline passed before the model call", retryable=True)

        kwargs: dict[str, Any] = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": self.system_prompt},
                {"role": "user", "content": build_user_message(msg, inputs)},
            ],
            "timeout": remaining_s,
        }
        if msg.type == TaskType.expand:
            kwargs["response_format"] = {
                "type": "json_schema",
                "json_schema": {"name": "subplan", "strict": True, "schema": LLM_SUBPLAN_SCHEMA},
            }

        try:
            response = self.client.chat.completions.create(**kwargs)
        except Exception as exc:  # provider errors are classified, never swallowed
            raise classify_provider_error(exc) from exc

        usage = getattr(response, "usage", None)
        tokens_in = getattr(usage, "prompt_tokens", None)
        tokens_out = getattr(usage, "completion_tokens", None)
        content = (response.choices[0].message.content or "").strip()
        if not content:
            raise ExecutionError("model returned an empty answer", retryable=True)

        if msg.type != TaskType.expand:
            return ExecResult(content, "output", tokens_in, tokens_out)

        try:
            raw = normalize_subplan(json.loads(content))
        except json.JSONDecodeError as exc:
            raise ExecutionError(f"expand output is not valid JSON ({exc.msg})", retryable=True) from exc
        result = validate_subplan(raw)
        if not result.ok:
            issues = "; ".join(str(i) for i in result.errors)
            raise ExecutionError(f"expand output failed subplan validation: {issues}", retryable=True)
        return ExecResult(json.dumps(result.value.model_dump(mode="json", exclude_none=True)),
                          "subplan", tokens_in, tokens_out)


__all__ = [
    "ExecResult",
    "ExecutionError",
    "Executor",
    "LLMExecutor",
    "LLM_SUBPLAN_SCHEMA",
    "SimExecutor",
    "build_user_message",
    "classify_provider_error",
]
