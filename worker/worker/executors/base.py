"""Executor interface.

An executor turns one task (its instruction plus its dependencies' results) into
one result. Executors are pure by default (D3): they write nothing except the
result the worker commits for them, which is what makes retries and duplicate
deliveries safe. A side-effecting executor must say so in `effects`.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, ClassVar, Literal

from planner.messages import TaskMessage


class RetryableError(Exception):
    """Transient failure (rate limit, timeout, network): atsd may dispatch a new attempt."""

    def __init__(self, message: str, retry_after_ms: int | None = None):
        super().__init__(message)
        self.retry_after_ms = retry_after_ms


class PermanentError(Exception):
    """Retrying cannot help (bad request, unknown type, missing input): fail the task now."""


@dataclass(frozen=True)
class ExecutionContext:
    message: TaskMessage
    # Dependency task id -> that task's committed result (parsed JSON), as in design doc 3.4.
    inputs: dict[str, Any]


@dataclass
class ExecutionResult:
    output: Any  # must be JSON-serializable; committed to result:{req}:{task}
    result_kind: Literal["output", "subplan"] = "output"
    tokens_in: int | None = None
    tokens_out: int | None = None


class Executor(ABC):
    capability: ClassVar[str]
    # Resources this executor writes besides its own result slot. Empty = pure (D3).
    effects: ClassVar[tuple[str, ...]] = ()

    @abstractmethod
    async def run(self, ctx: ExecutionContext) -> ExecutionResult: ...


__all__ = [
    "ExecutionContext",
    "ExecutionResult",
    "Executor",
    "PermanentError",
    "RetryableError",
]
