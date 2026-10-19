"""Executor registry: (capability, task type) -> executor (design doc 8.9).

The stream a message arrives on decides the capability: everything on tasks:sim
runs the sim executor whatever its type, while tasks:llm picks an executor per
task type. A task with no registered executor fails permanently with a clear
message instead of hanging until its deadline.
"""

from __future__ import annotations

from collections.abc import Iterable

from planner.schema import TaskType

from .executors import Executor, LLMExecutor, PermanentError, SimExecutor

ALL_TYPES: tuple[TaskType, ...] = tuple(TaskType)


class ExecutorRegistry:
    def __init__(self) -> None:
        self._executors: dict[tuple[str, TaskType], Executor] = {}

    def register(self, executor: Executor, types: Iterable[TaskType] = ALL_TYPES) -> None:
        for t in types:
            key = (executor.capability, TaskType(t))
            if key in self._executors:
                raise ValueError(f"executor already registered for {key[0]}/{key[1].value}")
            self._executors[key] = executor

    def capabilities(self) -> list[str]:
        return sorted({cap for cap, _ in self._executors})

    def get(self, capability: str, task_type: TaskType) -> Executor:
        try:
            return self._executors[(capability, task_type)]
        except KeyError:
            raise PermanentError(f"no executor for task type {task_type.value!r} on capability {capability!r}") from None


def default_registry(sim_time_scale: float = 1.0, sim_default_seconds: float = 10.0,
                     capabilities: Iterable[str] = ("sim",)) -> ExecutorRegistry:
    """Executors for the capabilities this worker serves. The Groq client is only created
    for an llm worker, so sim workers never need GROQ_API_KEY."""
    caps = set(capabilities)
    registry = ExecutorRegistry()
    if "sim" in caps:
        registry.register(SimExecutor(time_scale=sim_time_scale, default_seconds=sim_default_seconds))
    if "llm" in caps:
        registry.register(LLMExecutor())
    return registry
