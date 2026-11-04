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
from planner.schema import TaskType
from worker.executors.llm import (
    DEFAULT_EXECUTOR_MODEL,
    LLM_OUTPUT_SCHEMA,
    LLM_SUBPLAN_SCHEMA,
    SMALL_EXECUTOR_MODEL,
    build_user_message,
    classify_provider_error,
    input_budget,
    input_text,
    load_executor_prompt,
    load_subplan_prompt,
    model_for,
    parse_output,
)


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
    """Mimics AsyncGroq: chat.completions.create is awaited. `content` is one answer for every
    call, or a list of answers returned in order (the last one repeats)."""

    def __init__(self, content=json.dumps({"content": "answer", "key_points": ["a"]}), error=None, usage=(120, 40)):
        self.answers = content if isinstance(content, list) else [content]
        self.error, self.usage = error, usage
        self.calls = []
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self._create))

    async def _create(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        answer = self.answers[min(len(self.calls), len(self.answers)) - 1]
        usage = SimpleNamespace(prompt_tokens=self.usage[0], completion_tokens=self.usage[1]) if self.usage else None
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=answer))], usage=usage)


DISCOVERY = json.dumps({"content": "NVIDIA: leader\nAMD: second", "key_points": ["NVIDIA", "AMD"]})


def subplan_json(*tasks):
    return json.dumps({"tasks": [
        {"id": tid, "type": "research", "title": f"Research {tid}", "instruction": f"Research item {tid}.",
         "depends_on": list(deps), "hints": {"estimated_seconds": None}} for tid, deps in tasks]})


def test_llm_returns_answer_and_token_usage():
    client = FakeGroq(json.dumps({"content": "NVIDIA grew faster.", "key_points": ["NVIDIA ahead", " "]}))
    r = run(LLMExecutor(TaskType.compare, model="m", client=client), msg(inputs=["t3", "t4"]),
            {"t3": {"content": "Apple facts"}, "t4": {"content": "NVIDIA facts"}})
    assert (r.output, r.result_kind, r.tokens_in, r.tokens_out) == \
        ({"content": "NVIDIA grew faster.", "key_points": ["NVIDIA ahead"]}, "output", 120, 40)
    call = client.calls[0]
    assert call["model"] == "m"
    assert call["response_format"]["json_schema"]["schema"] == LLM_OUTPUT_SCHEMA
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
        run(LLMExecutor(TaskType.compare, client=client), msg(deadline_s=-1))
    assert client.calls == []


@pytest.mark.parametrize("content", [
    "   ",
    "not json",
    json.dumps({"content": "", "key_points": []}),
    json.dumps({"content": "x", "key_points": "not a list"}),
])
def test_llm_empty_or_malformed_answer_is_retryable(content):
    with pytest.raises(RetryableError):
        run(LLMExecutor(TaskType.compare, client=FakeGroq(content)), msg())


def test_each_type_has_its_own_prompt():
    prompts = {t: load_executor_prompt(t) for t in TaskType}
    assert len(set(prompts.values())) == len(TaskType)
    for t, text in prompts.items():
        assert f"Task type: {t.value}" in text and "no web access" in text and "key_points" in text
    assert "step 1 of 2: discovery" in prompts[TaskType.expand]
    # expand's key_points are item names: the shared "one short sentence each" must not reach it
    assert "one short sentence each" not in prompts[TaskType.expand]
    assert all("one short sentence each" in prompts[t] for t in TaskType if t != TaskType.expand)
    assert "never a sentence or a description." in prompts[TaskType.expand]
    assert prompts[TaskType.expand].rstrip().endswith("final_answer: null.")
    subplan = load_subplan_prompt()
    assert "step 2 of 2" in subplan and "Never expand" in subplan and "key_points" not in subplan
    assert "name and nothing else" in subplan  # titles are item names, not sentences (Day 7 E8 run)


def test_executor_refuses_a_task_of_another_type():
    with pytest.raises(PermanentError):
        run(LLMExecutor(TaskType.report, client=FakeGroq()), msg(type="compare"))


def test_registry_has_one_llm_executor_per_type(monkeypatch):
    import worker.executors.llm as llm_module
    from worker.registry import default_registry

    monkeypatch.setattr(llm_module, "make_client", lambda: FakeGroq())
    registry = default_registry(capabilities=("llm",))
    executors = {t: registry.get("llm", t) for t in TaskType}
    assert len({id(e) for e in executors.values()}) == len(TaskType)
    assert all(e.task_type == t for t, e in executors.items())


def test_expand_discovers_then_emits_a_validated_subplan():
    raw = {"tasks": [
        {"id": "s1", "type": "research", "title": "NVIDIA", "instruction": "Research NVIDIA.", "depends_on": [],
         "hints": {"estimated_seconds": None}},
        {"id": "s2", "type": "research", "title": "AMD", "instruction": "Research AMD.", "depends_on": [],
         "hints": {"estimated_seconds": 20}},
    ]}
    client = FakeGroq([DISCOVERY, json.dumps(raw)])
    r = run(LLMExecutor(TaskType.expand, client=client), msg(task_id="t2", type="expand"))
    discovery_call, subplan_call = client.calls
    assert discovery_call["response_format"]["json_schema"]["name"] == "task_output"
    assert subplan_call["response_format"]["json_schema"] == {"name": "subplan", "strict": True, "schema": LLM_SUBPLAN_SCHEMA}
    assert "step 2 of 2" in subplan_call["messages"][0]["content"]
    assert "NVIDIA: leader" in subplan_call["messages"][1]["content"]  # step 2 sees what step 1 found
    assert r.result_kind == "subplan" and (r.tokens_in, r.tokens_out) == (240, 80)  # both calls counted
    assert "hints" not in r.output["tasks"][0]  # nulls dropped
    assert r.output["tasks"][1]["hints"] == {"estimated_seconds": 20}
    assert validate_subplan(r.output).ok


@pytest.mark.parametrize("bad", [
    "not json",
    json.dumps({"tasks": []}),  # empty subplan
    subplan_json(("s1", ["s9"])),  # unknown dependency
])
def test_an_invalid_subplan_is_repaired_once(bad):
    client = FakeGroq([DISCOVERY, bad, subplan_json(("s1", []), ("s2", []))])
    r = run(LLMExecutor(TaskType.expand, client=client), msg(task_id="t2", type="expand"))
    assert [t["id"] for t in r.output["tasks"]] == ["s1", "s2"]
    repair = client.calls[2]["messages"]
    assert repair[-2] == {"role": "assistant", "content": bad}  # the rejected answer is replayed
    assert "rejected by the validator" in repair[-1]["content"]
    assert len(client.calls) == 3


def test_a_subplan_still_invalid_after_repair_fails_permanently():
    bad = subplan_json(("s1", ["s1"]))  # self-dependency, twice
    client = FakeGroq([DISCOVERY, bad, bad])
    with pytest.raises(PermanentError) as e:
        run(LLMExecutor(TaskType.expand, client=client), msg(task_id="t2", type="expand"))
    assert "SELF_DEPENDENCY" in str(e.value) and len(client.calls) == 3


def test_malformed_discovery_is_retryable():
    with pytest.raises(RetryableError):
        run(LLMExecutor(TaskType.expand, client=FakeGroq("not json")), msg(task_id="t2", type="expand"))


def test_output_schemas_meet_groq_strict_rules():
    def objects(s):
        if isinstance(s, dict):
            if s.get("type") == "object":
                yield s
            for v in s.values():
                yield from objects(v)
        elif isinstance(s, list):
            for v in s:
                yield from objects(v)
    for obj in [*objects(LLM_SUBPLAN_SCHEMA), *objects(LLM_OUTPUT_SCHEMA)]:
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
        run(LLMExecutor(TaskType.compare, client=FakeGroq(error=error)), msg())
    assert e.value.retry_after_ms == 1000


def test_reasoning_effort_is_low_for_gpt_oss_only(monkeypatch):
    monkeypatch.delenv("ATS_EXECUTOR_REASONING", raising=False)
    client = FakeGroq()
    run(LLMExecutor(TaskType.compare, model="openai/gpt-oss-120b", client=client), msg())
    run(LLMExecutor(TaskType.compare, model="llama-3.3-70b-versatile", client=client), msg())
    assert client.calls[0]["reasoning_effort"] == "low" and "reasoning_effort" not in client.calls[1]


def test_discovery_message_does_not_ask_for_subtasks():
    user = build_user_message(msg(task_id="t2", type="expand"), {})
    assert "subtasks" not in user and "s1" not in user


def test_a_subplan_that_misses_discovered_items_is_repaired():
    placeholder = json.dumps({"tasks": [
        {"id": "s1", "type": "research", "title": "Rank the companies", "instruction": "Find and rank the companies.",
         "depends_on": [], "hints": {"estimated_seconds": None}},
        {"id": "s2", "type": "research", "title": "Company #1", "instruction": "Research the top company from s1.",
         "depends_on": ["s1"], "hints": {"estimated_seconds": None}}]})
    good = json.dumps({"tasks": [
        {"id": "s1", "type": "research", "title": "Research NVIDIA", "instruction": "Research NVIDIA.",
         "depends_on": [], "hints": {"estimated_seconds": None}},
        {"id": "s2", "type": "research", "title": "Research AMD", "instruction": "Research AMD.",
         "depends_on": [], "hints": {"estimated_seconds": None}}]})
    client = FakeGroq([DISCOVERY, placeholder, good])
    r = run(LLMExecutor(TaskType.expand, client=client), msg(task_id="t2", type="expand"))
    assert [t["title"] for t in r.output["tasks"]] == ["Research NVIDIA", "Research AMD"]
    assert "NVIDIA, AMD" in client.calls[2]["messages"][-1]["content"]  # the repair names the missing items


def test_after_repair_a_valid_subplan_is_accepted_even_if_it_misses_an_item():
    partial = subplan_json(("s1", []))  # names neither item, but valid
    client = FakeGroq([DISCOVERY, partial, partial])
    r = run(LLMExecutor(TaskType.expand, client=client), msg(task_id="t2", type="expand"))
    assert r.result_kind == "subplan" and len(client.calls) == 3


# --- D58: models per type, compact inputs, final_answer ---------------------------------------


def test_model_per_task_type():
    assert model_for(TaskType.research, {}) == DEFAULT_EXECUTOR_MODEL
    assert model_for(TaskType.summarize, {}) == SMALL_EXECUTOR_MODEL
    assert model_for(TaskType.report, {}) == SMALL_EXECUTOR_MODEL
    # one model chosen for everything: the per-type defaults step aside
    assert model_for(TaskType.report, {"ATS_EXECUTOR_MODEL": "m"}) == "m"
    # explicit map, others on the base model; "" turns the map off
    env = {"ATS_EXECUTOR_MODELS": "compare=small, report=big", "ATS_EXECUTOR_MODEL": "base"}
    assert [model_for(t, env) for t in (TaskType.compare, TaskType.report, TaskType.summarize)] == ["small", "big", "base"]
    assert model_for(TaskType.summarize, {"ATS_EXECUTOR_MODELS": ""}) == DEFAULT_EXECUTOR_MODEL
    with pytest.raises(ValueError):
        model_for(TaskType.report, {"ATS_EXECUTOR_MODELS": "nonsense"})
    with pytest.raises(ValueError):
        model_for(TaskType.report, {"ATS_EXECUTOR_MODELS": "search=x"})


def test_executor_uses_its_types_model(monkeypatch):
    monkeypatch.delenv("ATS_EXECUTOR_MODEL", raising=False)
    monkeypatch.delenv("ATS_EXECUTOR_MODELS", raising=False)
    client = FakeGroq()
    run(LLMExecutor(TaskType.summarize, client=client), msg(type="summarize"))
    assert client.calls[0]["model"] == SMALL_EXECUTOR_MODEL
    assert LLMExecutor(TaskType.summarize, model="x", client=client).model == "x"  # explicit wins


def test_long_inputs_are_shortened_with_their_key_points():
    long = {"content": "intro\n" + "fact line\n" * 400, "key_points": ["p1", "p2"], "final_answer": "42"}
    text = input_text(long, 1000)
    assert len(text) < 1200 and "[... shortened]" in text and "- p1\n- p2" in text and text.endswith("Answer: 42")
    short = {"content": "short", "key_points": ["p1"]}
    assert input_text(short, 1000) == "short"  # untouched: key points only when cut
    assert input_text(long, 0).count("fact line") == 400  # 0 = no limit


def test_input_budget_is_split_across_inputs():
    assert input_budget(2, {}) == 3000
    assert input_budget(20, {}) == 800  # floor per input
    assert input_budget(3, {"ATS_INPUT_BUDGET_CHARS": "0"}) == 0
    message = build_user_message(msg(inputs=["t1", "t2"]), {
        "t1": {"content": "a" * 10000, "key_points": []}, "t2": {"content": "b" * 10, "key_points": []}})
    assert message.count("a") < 3100 and "b" * 10 in message


def test_final_answer_is_part_of_the_schema_and_kept():
    assert "final_answer" in LLM_OUTPUT_SCHEMA["required"]
    assert parse_output(json.dumps({"content": "c", "key_points": [], "final_answer": " Paris "}))["final_answer"] == "Paris"
    assert "final_answer" not in parse_output(json.dumps({"content": "c", "key_points": [], "final_answer": None}))
    assert "final_answer" not in parse_output(json.dumps({"content": "c", "key_points": []}))  # older outputs

