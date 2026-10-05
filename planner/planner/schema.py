"""ExecutionPlan schema v1: the single source of truth for the plan contract.

schemas/*.schema.json are generated from these models (see planner.export_schemas).
Structured output guarantees shape only; semantic checks live in validate.py.
"""

from __future__ import annotations

from enum import Enum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

MAX_TASKS = 50
MAX_SUBPLAN_TASKS = 20

TASK_ID_PATTERN = r"^t[0-9]+$"
SUBPLAN_TASK_ID_PATTERN = r"^s[0-9]+$"


class TaskType(str, Enum):
    research = "research"
    summarize = "summarize"
    analyze = "analyze"
    compare = "compare"
    report = "report"
    expand = "expand"  # produces a subplan at runtime (section 8.11)


class SubplanTaskType(str, Enum):
    """Task types allowed inside a subplan: no nested expand (max depth 1)."""

    research = "research"
    summarize = "summarize"
    analyze = "analyze"
    compare = "compare"
    report = "report"


class SubplanHints(BaseModel):
    model_config = ConfigDict(extra="forbid")
    estimated_seconds: float | None = Field(default=None, ge=0)


class SubplanTask(BaseModel):
    """A task proposed by an expand executor. IDs are local (s1, s2, ...);
    the scheduler renames them to <expand_id>.1, <expand_id>.2, ... on splice."""

    model_config = ConfigDict(extra="forbid")
    id: str = Field(pattern=SUBPLAN_TASK_ID_PATTERN)
    type: SubplanTaskType
    title: str = Field(min_length=1, max_length=120)
    instruction: str = Field(min_length=1, max_length=2000)
    depends_on: list[str] = Field(default_factory=list)
    hints: SubplanHints | None = None


class Subplan(BaseModel):
    """Result of an expand task. Dependencies may only name other subplan tasks;
    every subplan task implicitly depends on the expand task itself."""

    model_config = ConfigDict(extra="forbid")
    tasks: list[SubplanTask] = Field(min_length=1, max_length=MAX_SUBPLAN_TASKS)


class Hints(BaseModel):
    model_config = ConfigDict(extra="forbid")
    estimated_seconds: float | None = Field(default=None, ge=0)
    # expand tasks only: expected number of spawned tasks, for priority estimates.
    expected_fanout: int | None = Field(default=None, ge=1)
    # expand tasks only, fixtures only: canned subplan the simulator splices
    # instead of calling an LLM. Never requested from the planner.
    sim_subplan: Subplan | None = None


class PlannedTask(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str = Field(pattern=TASK_ID_PATTERN)
    type: TaskType
    title: str = Field(min_length=1, max_length=120)
    instruction: str = Field(min_length=1, max_length=2000)
    depends_on: list[str] = Field(default_factory=list)
    hints: Hints | None = None


class ExecutionPlan(BaseModel):
    """Immutable planner output. Never mutated by the runtime."""

    model_config = ConfigDict(extra="forbid")
    schema_version: Literal[1] = 1
    summary: str = Field(min_length=1, max_length=300)
    tasks: list[PlannedTask] = Field(min_length=1, max_length=MAX_TASKS)


class PlanFile(BaseModel):
    """Envelope the C++ side reads. request_id is assigned by the runtime, never the LLM."""

    model_config = ConfigDict(extra="forbid")
    request_id: str = Field(min_length=1)
    created_at: str = Field(min_length=1)
    plan: ExecutionPlan


# --- validation issues (shared codes with the C++ loader) -------------------


class IssueCode(str, Enum):
    SCHEMA = "SCHEMA"
    SCHEMA_VERSION = "SCHEMA_VERSION"
    EMPTY_PLAN = "EMPTY_PLAN"
    TOO_MANY_TASKS = "TOO_MANY_TASKS"
    DUPLICATE_ID = "DUPLICATE_ID"
    UNKNOWN_DEPENDENCY = "UNKNOWN_DEPENDENCY"
    SELF_DEPENDENCY = "SELF_DEPENDENCY"
    DUPLICATE_DEPENDENCY = "DUPLICATE_DEPENDENCY"
    CYCLE = "CYCLE"
    TRANSITIVE_EDGE = "TRANSITIVE_EDGE"  # warning, Python only
    MULTIPLE_SINKS = "MULTIPLE_SINKS"  # warning, Python only


class ValidationIssue(BaseModel):
    code: IssueCode
    severity: Literal["error", "warning"]
    task_id: str = ""
    message: str

    def __str__(self) -> str:
        where = f" [{self.task_id}]" if self.task_id else ""
        return f"{self.severity.upper()} {self.code.value}{where}: {self.message}"
