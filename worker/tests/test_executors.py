"""Executors without Redis or network: SimExecutor, LLMExecutor with a fake async Groq client.

Ported from planner/tests/test_executors.py when worker/ became the single worker.
"""

import asyncio
import json
import time
from types import SimpleNamespace

import pytest

from planner.messages import TaskMessage
from planner.validate import validate_subplan
from worker.executors import ExecutionContext, LLMExecutor, PermanentError, RetryableError, SimExecutor
from worker.executors.llm import LLM_SUBPLAN_SCHEMA, build_user_message, classify_provider_error


def msg(task_id="t5", type="compare", inputs=(), estimated_seconds=15.0, deadline_s=60, attempt=1):
    req = "req_test"
    return TaskMessage(
        request_id=req, task_id=task_id, attempt=attempt, attempt_id=f"{req}:{task_id}:{attempt}",
        type=type, instruction=f"Do {task_id}.", inputs={d: f"result:{req}:{d}" for d in inputs},
        deadline_ms=int((time.time() + deadline_s) * 1000), estimated_seconds=estimated_seconds)


def run(executor, message, inputs=None):
    return asyncio.run(executor.run(ExecutionContext(message=message, inputs=inputs or {})))


# --- SimExecutor ---------------------------------------------------------------------


def test_sim_expand_returns_a_valid_subplan():
    r = run(SimExecutor(time_scale=0, fanout=3), msg(task_id="t2", type="expand"))
    assert r.result_kind == "subplan"
    sub = validate_subplan(r.output)
    assert sub.ok and [t.id for t in sub.value.tasks] == ["s1", "s2", "s3"]


def test_sim_default_duration_matches_ats_sim():
    ctx = ExecutionContext(message=msg(estimated_seconds=None), inputs={})
    assert SimExecutor().duration(ctx) == 10.0  # same default as the C++ simulator (D21)


# --- LLMExecutor ---------------------------------------------------------------------


class FakeGroq:
    """Mimics AsyncGroq: chat.completions.create is awaited."""

    def __init__(self, content="answer", error=None, usage=(120, 40)):
        self.content, self.error, self.usage = content, error, usage
        self.calls = []
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self._create))

    async def _create(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        usage = SimpleNamespace(prompt_tokens=self.usage[0], completion_tokens=self.usage[1]) if self.usage else None
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=self.content))], usage=usage)


def test_llm_returns_answer_and_token_usage():
    client = FakeGroq("NVIDIA grew faster.")
    r = run(LLMExecutor(model="m", client=client), msg(inputs=["t3", "t4"]),
            {"t3": {"content": "Apple facts"}, "t4": {"content": "NVIDIA facts"}})
    assert (r.output, r.result_kind, r.tokens_in, r.tokens_out) == ({"content": "NVIDIA grew faster."}, "output", 120, 40)
    call = client.calls[0]
    assert call["model"] == "m" and "response_format" not in call
    assert 0 < call["timeout"] <= 60
    user = call["messages"][1]["content"]
    assert "Apple facts" in user and "NVIDIA facts" in user and user.index("t3") < user.index("t4")


def test_inputs_are_ordered_by_task_number_and_rendered_as_text():
    user = build_user_message(msg(inputs=["t10", "t2", "t2.1"]),
                              {"t10": {"content": "x"}, "t2": {"content": "y"}, "t2.1": {"tasks": []}})
    assert user.index("--- t2 ---") < user.index("--- t2.1 ---") < user.index("--- t10 ---")
    assert '{"tasks": []}' in user  # a non-output result (a Subplan) is shown as JSON


def test_llm_refuses_to_call_after_the_deadline():
    client = FakeGroq()
    with pytest.raises(RetryableError):
        run(LLMExecutor(client=client), msg(deadline_s=-1))
    assert client.calls == []


def test_llm_empty_answer_is_retryable():
    with pytest.raises(RetryableError):
        run(LLMExecutor(client=FakeGroq("   ")), msg())


def test_llm_expand_uses_strict_schema_and_returns_validated_subplan():
    raw = {"tasks": [
        {"id": "s1", "type": "research", "title": "NVIDIA", "instruction": "Research NVIDIA.", "depends_on": [],
         "hints": {"estimated_seconds": None}},
        {"id": "s2", "type": "research", "title": "AMD", "instruction": "Research AMD.", "depends_on": [],
         "hints": {"estimated_seconds": 20}},
    ]}
    client = FakeGroq(json.dumps(raw))
    r = run(LLMExecutor(client=client), msg(task_id="t2", type="expand"))
    assert client.calls[0]["response_format"]["json_schema"]["strict"] is True
    assert r.result_kind == "subplan"
    assert "hints" not in r.output["tasks"][0]  # nulls dropped
    assert r.output["tasks"][1]["hints"] == {"estimated_seconds": 20}
    assert validate_subplan(r.output).ok


@pytest.mark.parametrize("content", [
    "not json",
    json.dumps({"tasks": []}),  # empty subplan
    json.dumps({"tasks": [{"id": "s1", "type": "research", "title": "a", "instruction": "b",
                           "depends_on": ["s9"], "hints": {"estimated_seconds": None}}]}),  # unknown dep
])
def test_llm_bad_expand_output_is_retryable(content):
    with pytest.raises(RetryableError):
        run(LLMExecutor(client=FakeGroq(content)), msg(task_id="t2", type="expand"))


def test_subplan_schema_meets_groq_strict_rules():
    def objects(s):
        if isinstance(s, dict):
            if s.get("type") == "object":
                yield s
            for v in s.values():
                yield from objects(v)
        elif isinstance(s, list):
            for v in s:
                yield from objects(v)
    for obj in objects(LLM_SUBPLAN_SCHEMA):
        assert obj["additionalProperties"] is False
        assert sorted(obj["required"]) == sorted(obj["properties"])


# --- error classification ------------------------------------------------------------


def provider_error(name, status=None, headers=None):
    cls = type(name, (Exception,), {})
    exc = cls("boom")
    exc.status_code = status
    exc.response = SimpleNamespace(headers=headers or {})
    return exc


@pytest.mark.parametrize("exc, retryable, retry_after", [
    (provider_error("RateLimitError", 429, {"retry-after": "2.5"}), True, 2500),
    (provider_error("APITimeoutError"), True, None),
    (provider_error("APIConnectionError"), True, None),
    (provider_error("InternalServerError", 503), True, None),
    (provider_error("APIStatusError", 502), True, None),
    (TimeoutError("socket"), True, None),
    (provider_error("BadRequestError", 400), False, None),
    (provider_error("AuthenticationError", 401), False, None),
    (provider_error("PermissionDeniedError", 403), False, None),
])
def test_provider_errors_are_classified(exc, retryable, retry_after):
    err = classify_provider_error(exc)
    assert isinstance(err, RetryableError if retryable else PermanentError)
    assert getattr(err, "retry_after_ms", None) == retry_after


def test_llm_wraps_provider_errors():
    error = provider_error("RateLimitError", 429, {"retry-after": "1"})
    with pytest.raises(RetryableError) as e:
        run(LLMExecutor(client=FakeGroq(error=error)), msg())
    assert e.value.retry_after_ms == 1000
