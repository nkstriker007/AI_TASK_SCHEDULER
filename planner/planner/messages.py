"""Task messages and task events: contract #2 (design doc section 8.4).

schemas/task_message.schema.json and schemas/task_event.schema.json are
generated from these models (see planner.export_schemas), exactly like the
plan schema. Examples live in examples/messages/.

Messages carry result keys, never result bodies, so stream entries stay small.
Each stream entry has a single field `data` holding the message as JSON.

    tasks:llm / tasks:sim   atsd -> workers   TaskMessage (one dispatched attempt)
    task:events             workers -> atsd   TaskStarted | TaskCompleted | TaskFailed
"""

from __future__ import annotations

from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, RootModel, model_validator

from .schema import TaskType

STREAM_FIELD = "data"

# request_id must not contain ':' because attempt_id is "<request_id>:<task_id>:<attempt>".
REQUEST_ID_PATTERN = r"^[^:]+$"
# Plan tasks are t1, t2, ...; tasks spliced in by an expand task are t2.1, t2.2, ... (8.11).
RUNTIME_TASK_ID_PATTERN = r"^t[0-9]+(\.[0-9]+)?$"
ATTEMPT_ID_PATTERN = r"^[^:]+:t[0-9]+(\.[0-9]+)?:[1-9][0-9]*$"
RESULT_KEY_PATTERN = r"^result:[^:]+:t[0-9]+(\.[0-9]+)?$"


def attempt_id(request_id: str, task_id: str, attempt: int) -> str:
    return f"{request_id}:{task_id}:{attempt}"


def parse_attempt_id(value: str) -> tuple[str, str, int]:
    """Inverse of attempt_id(): (request_id, task_id, attempt)."""
    request_id, task_id, attempt = value.split(":")
    return request_id, task_id, int(attempt)


def result_key(request_id: str, task_id: str) -> str:
    return f"result:{request_id}:{task_id}"


# --- tasks:llm / tasks:sim ------------------------------------------------------


class TaskMessage(BaseModel):
    """One dispatched attempt. Each retry is a new message with attempt + 1 (D10)."""

    model_config = ConfigDict(extra="forbid")
    request_id: str = Field(min_length=1, pattern=REQUEST_ID_PATTERN)
    task_id: str = Field(pattern=RUNTIME_TASK_ID_PATTERN)
    attempt: int = Field(ge=1)
    attempt_id: str = Field(pattern=ATTEMPT_ID_PATTERN)
    type: TaskType
    instruction: str = Field(min_length=1)
    # Dependency task id -> result key of its committed result (3.4: keyed by task id).
    inputs: dict[str, str] = Field(default_factory=dict)
    # Absolute deadline for this attempt, Unix epoch milliseconds. Owned by atsd (8.7).
    deadline_ms: int = Field(ge=0)
    # The plan's hints.estimated_seconds; the sim executor sleeps for this long (8.9).
    estimated_seconds: float | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def _attempt_id_matches(self) -> TaskMessage:
        expected = attempt_id(self.request_id, self.task_id, self.attempt)
        if self.attempt_id != expected:
            raise ValueError(f"attempt_id must be {expected!r} (got {self.attempt_id!r})")
        for dep, key in self.inputs.items():
            if key != result_key(self.request_id, dep):
                raise ValueError(f"inputs[{dep!r}] must be {result_key(self.request_id, dep)!r} (got {key!r})")
        return self


# --- task:events -----------------------------------------------------------------


class _EventBase(BaseModel):
    model_config = ConfigDict(extra="forbid")
    attempt_id: str = Field(pattern=ATTEMPT_ID_PATTERN)
    worker_id: str = Field(min_length=1)
    ts_ms: int = Field(ge=0)  # worker wall clock, Unix epoch milliseconds


class TaskStarted(_EventBase):
    event: Literal["task_started"]


class TaskCompleted(_EventBase):
    event: Literal["task_completed"]
    result_key: str = Field(pattern=RESULT_KEY_PATTERN)
    # False when SET NX found an earlier attempt's result: this attempt's output was discarded (8.5).
    committed: bool
    # "subplan" when an expand task's result is a Subplan for atsd to splice (8.11).
    result_kind: Literal["output", "subplan"] = "output"
    exec_ms: int = Field(ge=0)
    # LLM token usage; null for executors that make no LLM call (sim).
    tokens_in: int | None = Field(default=None, ge=0)
    tokens_out: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def _result_key_matches(self) -> TaskCompleted:
        request_id, task_id, _ = parse_attempt_id(self.attempt_id)
        if self.result_key != result_key(request_id, task_id):
            raise ValueError(f"result_key must be {result_key(request_id, task_id)!r} (got {self.result_key!r})")
        return self


class TaskFailed(_EventBase):
    event: Literal["task_failed"]
    # Classified by the worker (8.6): retryable -> atsd may retry; permanent -> fail now.
    error_class: Literal["retryable", "permanent"]
    message: str = Field(min_length=1, max_length=2000)
    # Provider back-off hint (e.g. 429 Retry-After); atsd honours it when present.
    retry_after_ms: int | None = Field(default=None, ge=0)


TaskEventUnion = Annotated[Union[TaskStarted, TaskCompleted, TaskFailed], Field(discriminator="event")]


class TaskEvent(RootModel[TaskEventUnion]):
    """Any entry on task:events; the `event` field selects the type."""


__all__ = [
    "STREAM_FIELD",
    "TaskCompleted",
    "TaskEvent",
    "TaskFailed",
    "TaskMessage",
    "TaskStarted",
    "attempt_id",
    "parse_attempt_id",
    "result_key",
]
