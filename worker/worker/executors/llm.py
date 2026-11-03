"""LLM executors: one Groq chat completion per attempt (design doc 8.9, D27, D35).

Moved here from planner/planner/executors.py when worker/ became the single worker.
The behaviour is the same, but the call is async (AsyncGroq) so a slow model call
does not block the worker's loop, its other attempts or its heartbeat.

- One call per attempt and no SDK retries (max_retries=0): atsd owns retries (8.6, D12).
- The request times out when the attempt's deadline passes, and no call is made
  once it has passed.
- Dependency results go into the user message in task-number order (t2 < t2.1 < t10).
- Research answers come from model knowledge (D15).
- One executor per task type, each with its own prompt (common rules + the type's
  section, prompts/). Every type except expand answers in Groq strict JSON mode as
  {content, key_points}, which becomes the committed result (D35).
- Expand tasks take two calls (8.11): discovery ({content, key_points}: the items found), then
  the subplan in Groq strict-mode subplan schema, checked by validate_subplan (the same rules as
  any subplan). An invalid subplan is repaired once with the validator's issues; if it is still
  invalid the task fails permanently (D50). Token usage is the sum over the calls.
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
# gpt-oss models are reasoning models; "low" uses few reasoning tokens, which matters on
# tokens-per-minute rate limits (Groq: low | medium | high, gpt-oss only). "" sends nothing.
EXECUTOR_REASONING_ENV = "ATS_EXECUTOR_REASONING"
DEFAULT_EXECUTOR_REASONING = "low"

_NULLABLE_NUMBER = {"type": ["number", "null"]}

# Output of every type except expand (design doc 8.9; `sources` is left out because research
# answers from model knowledge only, D15, and invented sources would be worse than none).
LLM_OUTPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "content": {"type": "string"},
        "key_points": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["content", "key_points"],
    "additionalProperties": False,
}

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


def _prompt(name: str) -> str:
    return resources.files("worker").joinpath(f"prompts/{name}.md").read_text().strip()


def load_executor_prompt(task_type: TaskType) -> str:
    """System prompt for one task type: common rules, the type's section, the output format.
    For expand this is step 1 (discovery): the same {content, key_points} schema, but key_points are
    the item names, so it has its own output section. The shared one ("one short sentence each")
    came last and won, which made the spliced tasks' titles sentences (found in the Day 7 E8 run)."""
    output = "output_expand" if task_type == TaskType.expand else "output"
    return "\n\n".join([_prompt("common"), _prompt(task_type.value), _prompt(output)])


def load_subplan_prompt() -> str:
    """System prompt for step 2 of an expand task: turn the discovered items into a subplan."""
    return "\n\n".join([_prompt("common"), _prompt("subplan")])


def _subplan_request(msg: TaskMessage, discovery: dict[str, Any]) -> str:
    items = "\n".join(f"- {p}" for p in discovery.get("key_points", []))
    return "\n".join([
        f"Expand task {msg.task_id}. Instruction:", msg.instruction, "",
        "Items discovered in step 1:", discovery["content"], "",
        "Item names:", items or "(see above)", "",
        "Return the subtasks as JSON matching the schema. Local ids s1, s2, ...; no expand tasks; at most 20.",
    ])


def _check_subplan(content: str, items: list[str] | None = None) -> tuple[dict[str, Any] | None, str]:
    """(subplan, "") if valid, else (None, the problems as text for a repair prompt).

    Besides validate_subplan, every discovered item name must appear in some subtask's title or
    instruction: a subplan that re-discovers the items or uses placeholders ("company #1 from
    s1") is valid JSON but does not do what the expand task is for.
    """
    try:
        raw = normalize_subplan(json.loads(content))
    except json.JSONDecodeError as exc:
        return None, f"the answer is not valid JSON ({exc.msg})"
    result = validate_subplan(raw)
    if not result.ok:
        return None, "\n".join(f"- {i}" for i in result.errors)
    subplan = result.value.model_dump(mode="json", exclude_none=True)
    text = " ".join(f"{t['title']} {t['instruction']}" for t in subplan["tasks"]).lower()
    missing = [i for i in (items or []) if i.strip() and i.strip().lower() not in text]
    if missing:
        return None, "- no subtask names these discovered items: " + ", ".join(missing) + \
            ". Write one subtask per item, naming the item in its title and instruction."
    return subplan, ""


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


def make_client() -> Any:
    from groq import AsyncGroq  # needs GROQ_API_KEY

    return AsyncGroq(max_retries=0)  # retries belong to atsd, not the SDK (8.6, D12)


def parse_output(content: str) -> dict[str, Any]:
    """The model's {content, key_points} answer, checked. Malformed output is retryable (8.6)."""
    try:
        value = json.loads(content)
    except json.JSONDecodeError as exc:
        raise RetryableError(f"output is not valid JSON ({exc.msg})") from exc
    text = value.get("content") if isinstance(value, dict) else None
    points = value.get("key_points") if isinstance(value, dict) else None
    if not isinstance(text, str) or not text.strip():
        raise RetryableError("output has no content")
    if not isinstance(points, list) or not all(isinstance(p, str) for p in points):
        raise RetryableError("output key_points is not a list of strings")
    return {"content": text.strip(), "key_points": [p.strip() for p in points if p.strip()]}


class LLMExecutor(Executor):
    capability = "llm"

    def __init__(self, task_type: TaskType, model: str | None = None, client: Any = None, clock=time.time):
        self.task_type = TaskType(task_type)
        self.model = model or os.environ.get(EXECUTOR_MODEL_ENV, DEFAULT_EXECUTOR_MODEL)
        reasoning = os.environ.get(EXECUTOR_REASONING_ENV, DEFAULT_EXECUTOR_REASONING)
        self.reasoning_effort = reasoning if reasoning and "gpt-oss" in self.model else None
        self.client = client if client is not None else make_client()
        self.system_prompt = load_executor_prompt(self.task_type)
        self._clock = clock

    async def _call(self, msg: TaskMessage, messages: list[dict[str, str]], schema_name: str,
                    schema: dict[str, Any], usage: list[int]) -> str:
        """One model call, time-boxed to the attempt's deadline. Adds its tokens to `usage`."""
        remaining_s = msg.deadline_ms / 1000 - self._clock()
        if remaining_s <= 0:
            raise RetryableError("deadline passed before the model call")
        kwargs: dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "timeout": remaining_s,
            "response_format": {"type": "json_schema",
                                "json_schema": {"name": schema_name, "strict": True, "schema": schema}},
        }
        if self.reasoning_effort:
            kwargs["reasoning_effort"] = self.reasoning_effort
        try:
            response = await self.client.chat.completions.create(**kwargs)
        except Exception as exc:  # provider errors are classified, never swallowed
            raise classify_provider_error(exc) from exc
        u = getattr(response, "usage", None)
        usage[0] += getattr(u, "prompt_tokens", None) or 0
        usage[1] += getattr(u, "completion_tokens", None) or 0
        content = (response.choices[0].message.content or "").strip()
        if not content:
            raise RetryableError("model returned an empty answer")
        return content

    async def run(self, ctx: ExecutionContext) -> ExecutionResult:
        msg = ctx.message
        if msg.type != self.task_type:
            raise PermanentError(f"{self.task_type.value} executor got a {msg.type.value} task")
        usage = [0, 0]
        first = [{"role": "system", "content": self.system_prompt},
                 {"role": "user", "content": build_user_message(msg, ctx.inputs)}]
        output = parse_output(await self._call(msg, first, "task_output", LLM_OUTPUT_SCHEMA, usage))
        if msg.type != TaskType.expand:
            return ExecutionResult(output=output, tokens_in=usage[0], tokens_out=usage[1])

        # Expand, step 2: the subplan, repaired once if it fails validation (8.11, D50).
        messages = [{"role": "system", "content": load_subplan_prompt()},
                    {"role": "user", "content": _subplan_request(msg, output)}]
        items = output.get("key_points", [])
        content = await self._call(msg, messages, "subplan", LLM_SUBPLAN_SCHEMA, usage)
        subplan, problems = _check_subplan(content, items)
        if subplan is None:
            messages += [{"role": "assistant", "content": content},
                         {"role": "user", "content": "That subplan was rejected by the validator:\n" + problems +
                          "\nReturn a corrected subplan that fixes every issue."}]
            content = await self._call(msg, messages, "subplan", LLM_SUBPLAN_SCHEMA, usage)
            # After the one repair only validity is required: a subplan that misses an item is
            # still a usable result, an invalid one is not.
            subplan, problems = _check_subplan(content)
            if subplan is None:
                raise PermanentError("expand output failed subplan validation after one repair:\n" + problems)
        return ExecutionResult(output=subplan, result_kind="subplan", tokens_in=usage[0], tokens_out=usage[1])
