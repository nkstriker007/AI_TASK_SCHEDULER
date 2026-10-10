<<<<<<< HEAD
"""Thin provider layer: generate_plan(request) -> ExecutionPlan.

One structured-output call to Groq. Structured output guarantees shape only,
so every response goes through validate_plan; nothing is trusted or repaired
silently. The bounded repair loop (one retry with the issue list) is Day 2 and
plugs into Planner.generate_plan via the `feedback` argument of propose().
"""

from __future__ import annotations

import json
import os
from abc import ABC, abstractmethod
from importlib import resources
from typing import Any

from .schema import ExecutionPlan, IssueCode, TaskType, ValidationIssue
from .validate import validate_plan

DEFAULT_MODEL = "openai/gpt-oss-120b"  # supports Groq strict structured outputs
MODEL_ENV = "ATS_PLANNER_MODEL"


class PlanRejected(Exception):
    """The planner's output failed validation."""

    def __init__(self, issues: list[ValidationIssue], raw: Any = None):
        self.issues = issues
        self.raw = raw
        super().__init__("plan rejected:\n" + "\n".join(f"  {i}" for i in issues))


class Planner(ABC):
    @abstractmethod
    def propose(self, request: str, feedback: list[ValidationIssue] | None = None) -> Any:
        """Return the provider's raw (unvalidated) plan JSON."""

    def generate_plan(self, request: str) -> ExecutionPlan:
        raw = self.propose(request)
        result = validate_plan(raw)
        if not result.ok:
            raise PlanRejected(result.issues, raw)
        return result.value


# --- LLM-facing schema -------------------------------------------------------
# Groq strict mode requires every property to be required and every object to
# set additionalProperties: false; optional values are expressed as nullable.
# Length/pattern/count limits are left to validate_plan, which enforces the
# full schema.py constraints afterwards. sim_subplan is fixture-only and is
# never requested from the model.

_NULLABLE_NUMBER = {"type": ["number", "null"]}
_NULLABLE_INTEGER = {"type": ["integer", "null"]}

LLM_PLAN_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "schema_version": {"type": "integer", "enum": [1]},
        "summary": {"type": "string"},
        "tasks": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "type": {"type": "string", "enum": [t.value for t in TaskType]},
                    "title": {"type": "string"},
                    "instruction": {"type": "string"},
                    "depends_on": {"type": "array", "items": {"type": "string"}},
                    "hints": {
                        "type": "object",
                        "properties": {
                            "estimated_seconds": _NULLABLE_NUMBER,
                            "expected_fanout": _NULLABLE_INTEGER,
                        },
                        "required": ["estimated_seconds", "expected_fanout"],
                        "additionalProperties": False,
                    },
                },
                "required": ["id", "type", "title", "instruction", "depends_on", "hints"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["schema_version", "summary", "tasks"],
    "additionalProperties": False,
}


def normalize_llm_output(raw: Any) -> Any:
    """Map strict-mode nulls back to the plan schema's optional fields."""
    if not isinstance(raw, dict) or not isinstance(raw.get("tasks"), list):
        return raw
    for task in raw["tasks"]:
        if not isinstance(task, dict) or not isinstance(task.get("hints"), dict):
            continue
        hints = {k: v for k, v in task["hints"].items() if v is not None}
        if hints:
            task["hints"] = hints
        else:
            del task["hints"]
    return raw


def load_system_prompt() -> str:
    return resources.files("planner").joinpath("prompts/planner_system.md").read_text()


def _feedback_message(issues: list[ValidationIssue]) -> str:
    lines = "\n".join(f"- {i}" for i in issues)
    return (
        "Your previous plan was rejected by the validator with these issues:\n"
        f"{lines}\n"
        "Return a corrected plan for the same request that fixes every issue."
    )


class GroqPlanner(Planner):
    def __init__(self, model: str | None = None, client: Any = None, max_retries: int = 2):
        self.model = model or os.environ.get(MODEL_ENV, DEFAULT_MODEL)
        if client is None:
            from groq import Groq  # imported lazily so tests never need the SDK configured

            client = Groq(max_retries=max_retries)  # reads GROQ_API_KEY
        self.client = client
        self.system_prompt = load_system_prompt()
        self.last_usage: Any = None

    def propose(self, request: str, feedback: list[ValidationIssue] | None = None) -> Any:
        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": request},
        ]
        if feedback:
            messages.append({"role": "user", "content": _feedback_message(feedback)})

        response = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            response_format={
                "type": "json_schema",
                "json_schema": {"name": "execution_plan", "strict": True, "schema": LLM_PLAN_SCHEMA},
            },
        )
        self.last_usage = getattr(response, "usage", None)
        choice = response.choices[0]
        content = choice.message.content or ""
        try:
            raw = json.loads(content)
        except json.JSONDecodeError as exc:
            raise PlanRejected(
                [
                    ValidationIssue(
                        code=IssueCode.SCHEMA,
                        severity="error",
                        message=f"model output is not valid JSON ({exc.msg}; finish_reason={choice.finish_reason})",
                    )
                ],
                content,
            ) from exc
        return normalize_llm_output(raw)


__all__ = [
    "DEFAULT_MODEL",
    "GroqPlanner",
    "LLM_PLAN_SCHEMA",
    "PlanRejected",
    "Planner",
]
=======
"""Thin provider layer: generate_plan(request) -> ExecutionPlan.

One structured-output call to Groq. Structured output guarantees shape only,
so every response goes through validate_plan; nothing is trusted or repaired
silently. Planner.generate_plan runs the bounded repair loop: one retry with
the issue list passed to propose() as `feedback`, then reject.
"""

from __future__ import annotations

import json
import os
from abc import ABC, abstractmethod
from importlib import resources
from typing import Any

from .schema import ExecutionPlan, IssueCode, TaskType, ValidationIssue
from .validate import validate_plan

DEFAULT_MODEL = "openai/gpt-oss-120b"  # supports Groq strict structured outputs
MODEL_ENV = "ATS_PLANNER_MODEL"


class PlanRejected(Exception):
    """The planner's output failed validation."""

    def __init__(self, issues: list[ValidationIssue], raw: Any = None):
        self.issues = issues
        self.raw = raw
        super().__init__("plan rejected:\n" + "\n".join(f"  {i}" for i in issues))


class Planner(ABC):
    last_attempts: int = 0  # propose() calls made by the last generate_plan

    @abstractmethod
    def propose(self, request: str, feedback: list[ValidationIssue] | None = None) -> Any:
        """Return the provider's raw (unvalidated) plan JSON."""

    def generate_plan(self, request: str) -> ExecutionPlan:
        """Propose, validate, and repair at most once (design doc section 6, D8).

        If the first proposal fails validation (or is not parseable), its issues
        are sent back through propose(feedback=...) once; if that also fails, the
        second attempt's issues are raised. `last_attempts` records 1 or 2 so the
        eval can report how often repair was needed.
        """
        feedback: list[ValidationIssue] | None = None
        for attempt in (1, 2):
            self.last_attempts = attempt
            try:
                raw = self.propose(request, feedback=feedback)
            except PlanRejected as exc:  # unparseable output counts as a failed attempt
                if attempt == 2:
                    raise
                feedback = exc.issues
                continue
            result = validate_plan(raw)
            if result.ok:
                return result.value
            if attempt == 2:
                raise PlanRejected(result.issues, raw)
            feedback = result.errors
        raise AssertionError("unreachable")


# --- LLM-facing schema -------------------------------------------------------
# Groq strict mode requires every property to be required and every object to
# set additionalProperties: false; optional values are expressed as nullable.
# Length/pattern/count limits are left to validate_plan, which enforces the
# full schema.py constraints afterwards. sim_subplan is fixture-only and is
# never requested from the model.

_NULLABLE_NUMBER = {"type": ["number", "null"]}
_NULLABLE_INTEGER = {"type": ["integer", "null"]}

LLM_PLAN_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "schema_version": {"type": "integer", "enum": [1]},
        "summary": {"type": "string"},
        "tasks": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "type": {"type": "string", "enum": [t.value for t in TaskType]},
                    "title": {"type": "string"},
                    "instruction": {"type": "string"},
                    "depends_on": {"type": "array", "items": {"type": "string"}},
                    "hints": {
                        "type": "object",
                        "properties": {
                            "estimated_seconds": _NULLABLE_NUMBER,
                            "expected_fanout": _NULLABLE_INTEGER,
                        },
                        "required": ["estimated_seconds", "expected_fanout"],
                        "additionalProperties": False,
                    },
                },
                "required": ["id", "type", "title", "instruction", "depends_on", "hints"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["schema_version", "summary", "tasks"],
    "additionalProperties": False,
}


def normalize_llm_output(raw: Any) -> Any:
    """Map strict-mode nulls back to the plan schema's optional fields."""
    if not isinstance(raw, dict) or not isinstance(raw.get("tasks"), list):
        return raw
    for task in raw["tasks"]:
        if not isinstance(task, dict) or not isinstance(task.get("hints"), dict):
            continue
        hints = {k: v for k, v in task["hints"].items() if v is not None}
        if hints:
            task["hints"] = hints
        else:
            del task["hints"]
    return raw


def load_system_prompt() -> str:
    return resources.files("planner").joinpath("prompts/planner_system.md").read_text()


def _feedback_message(issues: list[ValidationIssue]) -> str:
    lines = "\n".join(f"- {i}" for i in issues)
    return (
        "Your previous plan was rejected by the validator with these issues:\n"
        f"{lines}\n"
        "Return a corrected plan for the same request that fixes every issue."
    )


class GroqPlanner(Planner):
    def __init__(self, model: str | None = None, client: Any = None, max_retries: int = 2):
        self.model = model or os.environ.get(MODEL_ENV, DEFAULT_MODEL)
        if client is None:
            from groq import Groq  # imported lazily so tests never need the SDK configured

            client = Groq(max_retries=max_retries)  # reads GROQ_API_KEY
        self.client = client
        self.system_prompt = load_system_prompt()
        self.last_usage: Any = None
        self._last_content: str | None = None  # previous answer, replayed on a repair call

    def propose(self, request: str, feedback: list[ValidationIssue] | None = None) -> Any:
        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": request},
        ]
        if feedback:
            if self._last_content:
                messages.append({"role": "assistant", "content": self._last_content})
            messages.append({"role": "user", "content": _feedback_message(feedback)})

        response = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            response_format={
                "type": "json_schema",
                "json_schema": {"name": "execution_plan", "strict": True, "schema": LLM_PLAN_SCHEMA},
            },
        )
        self.last_usage = getattr(response, "usage", None)
        choice = response.choices[0]
        content = choice.message.content or ""
        self._last_content = content
        try:
            raw = json.loads(content)
        except json.JSONDecodeError as exc:
            raise PlanRejected(
                [
                    ValidationIssue(
                        code=IssueCode.SCHEMA,
                        severity="error",
                        message=f"model output is not valid JSON ({exc.msg}; finish_reason={choice.finish_reason})",
                    )
                ],
                content,
            ) from exc
        return normalize_llm_output(raw)


__all__ = [
    "DEFAULT_MODEL",
    "GroqPlanner",
    "LLM_PLAN_SCHEMA",
    "PlanRejected",
    "Planner",
]
>>>>>>> 5ebdf693386164bbcffe0b799e8fa8a6dc501faa
