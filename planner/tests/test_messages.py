import copy
import json

import pytest
from pydantic import ValidationError

from planner.messages import (
    TaskCompleted,
    TaskEvent,
    TaskFailed,
    TaskMessage,
    TaskStarted,
    attempt_id,
    parse_attempt_id,
    result_key,
)
from planner.paths import MESSAGES_DIR

EXAMPLES = sorted(MESSAGES_DIR.glob("*.json"))


def example(name):
    return json.loads((MESSAGES_DIR / f"{name}.json").read_text())


@pytest.mark.parametrize("path", EXAMPLES, ids=lambda p: p.stem)
def test_examples_validate_and_round_trip(path):
    data = json.loads(path.read_text())
    model = TaskMessage if path.stem.startswith("task_message") else TaskEvent
    parsed = model.model_validate(data)
    assert parsed.model_dump(mode="json") == data


def test_event_union_dispatches_on_event_field():
    kinds = {p.stem: type(TaskEvent.model_validate(example(p.stem)).root)
             for p in EXAMPLES if not p.stem.startswith("task_message")}
    assert kinds["task_started"] is TaskStarted
    assert kinds["task_completed_subplan"] is TaskCompleted
    assert kinds["task_failed"] is TaskFailed
    assert TaskEvent.model_validate(example("task_completed_subplan")).root.result_kind == "subplan"


def test_ids_and_keys():
    assert attempt_id("req_1", "t2.1", 3) == "req_1:t2.1:3"
    assert parse_attempt_id("req_1:t2.1:3") == ("req_1", "t2.1", 3)
    assert result_key("req_1", "t5") == "result:req_1:t5"


def test_optional_fields_default():
    msg = example("task_message")
    del msg["inputs"], msg["estimated_seconds"]
    parsed = TaskMessage.model_validate(msg)
    assert parsed.inputs == {} and parsed.estimated_seconds is None

    done = example("task_completed")
    del done["result_kind"], done["tokens_in"], done["tokens_out"]
    parsed = TaskCompleted.model_validate(done)
    assert parsed.result_kind == "output" and parsed.tokens_in is None


@pytest.mark.parametrize("change", [
    {"attempt_id": "req_2f9c1a7e:t5:1"},                  # does not match attempt
    {"attempt": 0, "attempt_id": "req_2f9c1a7e:t5:0"},    # attempts start at 1
    {"task_id": "s1", "attempt_id": "req_2f9c1a7e:s1:2"}, # subplan-local ids never reach workers
    {"request_id": "req:x", "attempt_id": "req:x:t5:2"},  # ':' would break attempt_id
    {"type": "sim"},                                      # not a task type
    {"inputs": {"t3": "result:req_other:t3"}},            # inputs are this request's result keys
    {"deadline_ms": -1},
    {"result": "inline bodies are not allowed"},          # unknown field
])
def test_task_message_rejects(change):
    msg = {**example("task_message"), **change}
    with pytest.raises(ValidationError):
        TaskMessage.model_validate(msg)


@pytest.mark.parametrize("name,change", [
    ("task_started", {"event": "task_paused"}),
    ("task_started", {"worker_id": ""}),
    ("task_completed", {"result_key": "result:req_2f9c1a7e:t4"}),  # another task's slot
    ("task_completed", {"result_kind": "text"}),
    ("task_completed", {"exec_ms": -5}),
    ("task_failed", {"error_class": "fatal"}),
    ("task_failed", {"retry_after_ms": -1}),
])
def test_task_event_rejects(name, change):
    data = {**copy.deepcopy(example(name)), **change}
    with pytest.raises(ValidationError):
        TaskEvent.model_validate(data)


def test_event_without_event_field_is_rejected():
    data = example("task_started")
    del data["event"]
    with pytest.raises(ValidationError):
        TaskEvent.model_validate(data)
