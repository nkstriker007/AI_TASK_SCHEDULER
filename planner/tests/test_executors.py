"""Executors without Redis or network: SimExecutor, LLMExecutor with a fake Groq client."""

import json
import time
from types import SimpleNamespace

import pytest

from planner.executors import (
    LLM_SUBPLAN_SCHEMA,
    ExecutionError,
    LLMExecutor,
    SimExecutor,
    build_user_message,
    classify_provider_error,
)
from planner.messages import TaskMessage
from planner.validate import validate_subplan


def msg(task_id="t5", type="compare", inputs=(), estimated_seconds=15.0, deadline_s=60, attempt=1):
    req = "req_test"
    return TaskMessage(
        request_id=req, task_id=task_id, attempt=attempt, attempt_id=f"{req}:{task_id}:{attempt}",
        type=type, instruction=f"Do {task_id}.", inputs={d: f"result:{req}:{d}" for d in inputs},
        deadline_ms=int((time.time() + deadline_s) * 1000), estimated_seconds=estimated_seconds)


# --- SimExecutor -------------------------------------------------------------


def test_sim_sleeps_estimated_seconds_times_scale():
    slept = []
    r = SimExecutor(time_scale=0.1, sleep=slept.append).execute(msg(estimated_seconds=20), {})
    assert slept == [pytest.approx(2.0)]
    assert r.kind == "output" and r.tokens_in is None


def test_sim_uses_default_seconds_without_a_hint():
    slept = []
    SimExecutor(default_seconds=3, sleep=slept.append).execute(msg(estimated_seconds=None), {})
    assert slept == [3]


def test_sim_output_names_its_inputs():
    r = SimExecutor(time_scale=0).execute(msg(inputs=["t3", "t4"]), {"t3": "a", "t4": "b"})
    assert "t3" in r.output and "t4" in r.output


def test_sim_expand_returns_a_valid_subplan():
    r = SimExecutor(time_scale=0, fanout=3).execute(msg(task_id="t2", type="expand"), {})
    assert r.kind == "subplan"
    sub = validate_subplan(json.loads(r.output))
    assert sub.ok and [t.id for t in sub.value.tasks] == ["s1", "s2", "s3"]


# --- LLMExecutor -------------------------------------------------------------


class FakeGroq:
    def __init__(self, content="answer", error=None, usage=(120, 40)):
        self.content, self.error, self.usage = content, error, usage
        self.calls = []
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self._create))

    def _create(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        usage = SimpleNamespace(prompt_tokens=self.usage[0], completion_tokens=self.usage[1]) if self.usage else None
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=self.content))], usage=usage)


def test_llm_returns_answer_and_token_usage():
    client = FakeGroq("NVIDIA grew faster.")
    r = LLMExecutor(model="m", client=client).execute(msg(inputs=["t3", "t4"]), {"t3": "Apple facts", "t4": "NVIDIA facts"})
    assert (r.output, r.kind, r.tokens_in, r.tokens_out) == ("NVIDIA grew faster.", "output", 120, 40)
    call = client.calls[0]
    assert call["model"] == "m" and "response_format" not in call
    assert 0 < call["timeout"] <= 60
    user = call["messages"][1]["content"]
    assert "Apple facts" in user and "NVIDIA facts" in user and user.index("t3") < user.index("t4")


def test_inputs_are_ordered_by_task_number_not_text():
    user = build_user_message(msg(inputs=["t10", "t2", "t2.1"]), {"t10": "x", "t2": "y", "t2.1": "z"})
    assert user.index("--- t2 ---") < user.index("--- t2.1 ---") < user.index("--- t10 ---")


def test_llm_refuses_to_call_after_the_deadline():
    client = FakeGroq()
    with pytest.raises(ExecutionError) as e:
        LLMExecutor(client=client).execute(msg(deadline_s=-1), {})
    assert e.value.retryable and client.calls == []


def test_llm_empty_answer_is_retryable():
    with pytest.raises(ExecutionError) as e:
        LLMExecutor(client=FakeGroq("   ")).execute(msg(), {})
    assert e.value.retryable


def test_llm_expand_uses_strict_schema_and_returns_validated_subplan():
    raw = {"tasks": [
        {"id": "s1", "type": "research", "title": "NVIDIA", "instruction": "Research NVIDIA.", "depends_on": [],
         "hints": {"estimated_seconds": None}},
        {"id": "s2", "type": "research", "title": "AMD", "instruction": "Research AMD.", "depends_on": [],
         "hints": {"estimated_seconds": 20}},
    ]}
    client = FakeGroq(json.dumps(raw))
    r = LLMExecutor(client=client).execute(msg(task_id="t2", type="expand"), {})
    assert client.calls[0]["response_format"]["json_schema"]["strict"] is True
    assert r.kind == "subplan"
    sub = json.loads(r.output)
    assert "hints" not in sub["tasks"][0]                       # nulls dropped
    assert sub["tasks"][1]["hints"] == {"estimated_seconds": 20}
    assert validate_subplan(sub).ok


@pytest.mark.parametrize("content", [
    "not json",
    json.dumps({"tasks": []}),                                                       # empty subplan
    json.dumps({"tasks": [{"id": "s1", "type": "research", "title": "a", "instruction": "b",
                           "depends_on": ["s9"], "hints": {"estimated_seconds": None}}]}),  # unknown dep
])
def test_llm_bad_expand_output_is_retryable(content):
    with pytest.raises(ExecutionError) as e:
        LLMExecutor(client=FakeGroq(content)).execute(msg(task_id="t2", type="expand"), {})
    assert e.value.retryable


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


# --- error classification ----------------------------------------------------


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
    assert err.retryable is retryable
    assert err.retry_after_ms == retry_after
    assert err.error_class == ("retryable" if retryable else "permanent")


def test_llm_wraps_provider_errors():
    with pytest.raises(ExecutionError) as e:
        LLMExecutor(client=FakeGroq(error=provider_error("RateLimitError", 429, {"retry-after": "1"}))).execute(msg(), {})
    assert e.value.retryable and e.value.retry_after_ms == 1000
