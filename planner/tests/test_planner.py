<<<<<<< HEAD
import json
from types import SimpleNamespace

import pytest

from planner import cli
from planner.export_schemas import stale_files
from planner.fake import FakePlanner, load_fixture_plan
from planner.llm import LLM_PLAN_SCHEMA, GroqPlanner, PlanRejected
from planner.validate import validate_plan_file


def test_committed_schemas_are_fresh():
    assert stale_files() == [], "run: python -m planner.export_schemas"


# --- FakePlanner -------------------------------------------------------------


def test_fake_planner_returns_fixture():
    plan = FakePlanner().generate_plan("anything")
    assert len(plan.tasks) == 6


def test_fake_planner_script_and_rejection():
    bad = load_fixture_plan("company_research")
    bad["tasks"][0]["depends_on"] = ["t6"]  # creates a cycle
    fake = FakePlanner(script=[bad])
    with pytest.raises(PlanRejected) as exc:
        fake.generate_plan("req")
    assert [i.code.value for i in exc.value.issues] == ["CYCLE"]
    assert fake.generate_plan("req").tasks[0].depends_on == []  # script exhausted -> default
    assert [c[0] for c in fake.calls] == ["req", "req"]


# --- Groq planner (fake client, no network) -----------------------------------


class FakeGroqClient:
    def __init__(self, content):
        self.content = content
        self.requests = []
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self._create))

    def _create(self, **kwargs):
        self.requests.append(kwargs)
        msg = SimpleNamespace(content=self.content)
        return SimpleNamespace(choices=[SimpleNamespace(message=msg, finish_reason="stop")], usage=None)


def llm_style_plan():
    """What strict mode returns: every hint key present, possibly null."""
    plan = load_fixture_plan("company_research")
    for t in plan["tasks"]:
        t["hints"] = {"estimated_seconds": t["hints"]["estimated_seconds"], "expected_fanout": None}
    plan["tasks"][0]["hints"] = {"estimated_seconds": None, "expected_fanout": None}
    return plan


def test_groq_planner_request_shape_and_normalization():
    client = FakeGroqClient(json.dumps(llm_style_plan()))
    plan = GroqPlanner(model="m", client=client).generate_plan("Compare Apple and NVIDIA")
    assert plan.tasks[0].hints is None
    assert plan.tasks[1].hints.estimated_seconds == 25
    assert plan.tasks[1].hints.expected_fanout is None

    [req] = client.requests
    assert req["model"] == "m"
    assert req["messages"][0]["role"] == "system" and "depends_on" in req["messages"][0]["content"]
    assert req["messages"][1] == {"role": "user", "content": "Compare Apple and NVIDIA"}
    fmt = req["response_format"]
    assert fmt["type"] == "json_schema" and fmt["json_schema"]["strict"] is True


def test_groq_planner_invalid_json_is_rejected():
    with pytest.raises(PlanRejected) as exc:
        GroqPlanner(client=FakeGroqClient("{not json")).generate_plan("x")
    assert exc.value.issues[0].code.value == "SCHEMA"


def test_groq_planner_semantic_errors_are_rejected():
    plan = llm_style_plan()
    plan["tasks"][2]["depends_on"] = ["t42"]
    with pytest.raises(PlanRejected) as exc:
        GroqPlanner(client=FakeGroqClient(json.dumps(plan))).generate_plan("x")
    assert [i.code.value for i in exc.value.issues] == ["UNKNOWN_DEPENDENCY"]


def _objects(schema):
    if isinstance(schema, dict):
        if schema.get("type") == "object":
            yield schema
        for v in schema.values():
            yield from _objects(v)
    elif isinstance(schema, list):
        for v in schema:
            yield from _objects(v)


def test_llm_schema_meets_groq_strict_rules():
    for obj in _objects(LLM_PLAN_SCHEMA):
        assert obj["additionalProperties"] is False
        assert sorted(obj["required"]) == sorted(obj["properties"])


# --- CLI ---------------------------------------------------------------------


def test_cli_writes_valid_plan_file(tmp_path, capsys):
    out = tmp_path / "plan.json"
    assert cli.main(["Compare Apple and NVIDIA", "-o", str(out), "--fake", "company_research"]) == 0
    doc = json.loads(out.read_text())
    assert doc["request_id"].startswith("req_") and doc["request_id"] != "req_2f9c1a7e"
    assert doc["created_at"].endswith("Z")
    assert validate_plan_file(doc).ok


def test_cli_prints_warnings(tmp_path, capsys):
    assert cli.main(["x", "-o", str(tmp_path / "p.json"), "--fake", "fork"]) == 0
    assert "MULTIPLE_SINKS" in capsys.readouterr().err
=======
import json
from types import SimpleNamespace

import pytest

from planner import cli
from planner.export_schemas import stale_files
from planner.fake import FakePlanner, load_fixture_plan
from planner.llm import LLM_PLAN_SCHEMA, GroqPlanner, PlanRejected
from planner.validate import validate_plan_file


def test_committed_schemas_are_fresh():
    assert stale_files() == [], "run: python -m planner.export_schemas"


# --- FakePlanner -------------------------------------------------------------


def test_fake_planner_returns_fixture():
    plan = FakePlanner().generate_plan("anything")
    assert len(plan.tasks) == 6


def cyclic_plan():
    bad = load_fixture_plan("company_research")
    bad["tasks"][0]["depends_on"] = ["t6"]  # creates a cycle
    return bad


# --- repair loop: one retry with the issue list, then reject ------------------


def test_valid_first_plan_needs_no_repair():
    fake = FakePlanner()
    fake.generate_plan("req")
    assert fake.calls == [("req", None)]
    assert fake.last_attempts == 1


def test_repair_succeeds_on_second_attempt():
    fake = FakePlanner(script=[cyclic_plan()])  # bad plan first, default (good) plan second
    plan = fake.generate_plan("req")
    assert plan.tasks[0].depends_on == []
    assert fake.last_attempts == 2
    [(req1, fb1), (req2, fb2)] = fake.calls
    assert (req1, fb1) == ("req", None)
    assert req2 == "req" and [i.code.value for i in fb2] == ["CYCLE"]


def test_repair_failure_rejects_with_second_attempts_issues():
    unknown_dep = load_fixture_plan("company_research")
    unknown_dep["tasks"][2]["depends_on"] = ["t42"]
    fake = FakePlanner(script=[cyclic_plan(), unknown_dep])
    with pytest.raises(PlanRejected) as exc:
        fake.generate_plan("req")
    assert [i.code.value for i in exc.value.issues] == ["UNKNOWN_DEPENDENCY"]
    assert exc.value.raw == unknown_dep
    assert len(fake.calls) == 2  # exactly one repair, never more


def test_repair_feedback_excludes_warnings():
    fork = load_fixture_plan("fork")  # valid, but MULTIPLE_SINKS warning
    fork["tasks"][1]["depends_on"].append("t1")  # DUPLICATE_DEPENDENCY error
    fake = FakePlanner(script=[fork])
    fake.generate_plan("req")
    assert [i.code.value for i in fake.calls[1][1]] == ["DUPLICATE_DEPENDENCY"]


# --- Groq planner (fake client, no network) -----------------------------------


class FakeGroqClient:
    """Returns `content` for every call, or the next item when given a list."""

    def __init__(self, content):
        self.contents = list(content) if isinstance(content, list) else None
        self.content = content
        self.requests = []
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self._create))

    def _create(self, **kwargs):
        self.requests.append(kwargs)
        content = self.contents.pop(0) if self.contents is not None else self.content
        msg = SimpleNamespace(content=content)
        return SimpleNamespace(choices=[SimpleNamespace(message=msg, finish_reason="stop")], usage=None)


def llm_style_plan():
    """What strict mode returns: every hint key present, possibly null."""
    plan = load_fixture_plan("company_research")
    for t in plan["tasks"]:
        t["hints"] = {"estimated_seconds": t["hints"]["estimated_seconds"], "expected_fanout": None}
    plan["tasks"][0]["hints"] = {"estimated_seconds": None, "expected_fanout": None}
    return plan


def test_groq_planner_request_shape_and_normalization():
    client = FakeGroqClient(json.dumps(llm_style_plan()))
    plan = GroqPlanner(model="m", client=client).generate_plan("Compare Apple and NVIDIA")
    assert plan.tasks[0].hints is None
    assert plan.tasks[1].hints.estimated_seconds == 25
    assert plan.tasks[1].hints.expected_fanout is None

    [req] = client.requests
    assert req["model"] == "m"
    assert req["messages"][0]["role"] == "system" and "depends_on" in req["messages"][0]["content"]
    assert req["messages"][1] == {"role": "user", "content": "Compare Apple and NVIDIA"}
    fmt = req["response_format"]
    assert fmt["type"] == "json_schema" and fmt["json_schema"]["strict"] is True


def test_groq_planner_invalid_json_is_rejected_after_one_repair():
    client = FakeGroqClient("{not json")
    with pytest.raises(PlanRejected) as exc:
        GroqPlanner(client=client).generate_plan("x")
    assert exc.value.issues[0].code.value == "SCHEMA"
    assert len(client.requests) == 2


def test_groq_planner_semantic_errors_are_rejected():
    plan = llm_style_plan()
    plan["tasks"][2]["depends_on"] = ["t42"]
    client = FakeGroqClient(json.dumps(plan))
    with pytest.raises(PlanRejected) as exc:
        GroqPlanner(client=client).generate_plan("x")
    assert [i.code.value for i in exc.value.issues] == ["UNKNOWN_DEPENDENCY"]
    assert len(client.requests) == 2


def test_groq_repair_request_replays_answer_and_issues():
    bad = llm_style_plan()
    bad["tasks"][2]["depends_on"] = ["t42"]
    client = FakeGroqClient([json.dumps(bad), json.dumps(llm_style_plan())])
    planner = GroqPlanner(client=client)
    assert len(planner.generate_plan("Compare Apple and NVIDIA").tasks) == 6
    assert planner.last_attempts == 2
    first, repair = client.requests
    assert len(first["messages"]) == 2
    system, user, assistant, feedback = repair["messages"]
    assert user == {"role": "user", "content": "Compare Apple and NVIDIA"}
    assert assistant == {"role": "assistant", "content": json.dumps(bad)}
    assert feedback["role"] == "user"
    assert "UNKNOWN_DEPENDENCY [t3]" in feedback["content"] and "t42" in feedback["content"]


def _objects(schema):
    if isinstance(schema, dict):
        if schema.get("type") == "object":
            yield schema
        for v in schema.values():
            yield from _objects(v)
    elif isinstance(schema, list):
        for v in schema:
            yield from _objects(v)


def test_llm_schema_meets_groq_strict_rules():
    for obj in _objects(LLM_PLAN_SCHEMA):
        assert obj["additionalProperties"] is False
        assert sorted(obj["required"]) == sorted(obj["properties"])


# --- CLI ---------------------------------------------------------------------


def test_cli_writes_valid_plan_file(tmp_path, capsys):
    out = tmp_path / "plan.json"
    assert cli.main(["Compare Apple and NVIDIA", "-o", str(out), "--fake", "company_research"]) == 0
    doc = json.loads(out.read_text())
    assert doc["request_id"].startswith("req_") and doc["request_id"] != "req_2f9c1a7e"
    assert doc["created_at"].endswith("Z")
    assert validate_plan_file(doc).ok


def test_cli_prints_warnings(tmp_path, capsys):
    assert cli.main(["x", "-o", str(tmp_path / "p.json"), "--fake", "fork"]) == 0
    assert "MULTIPLE_SINKS" in capsys.readouterr().err
>>>>>>> 5ebdf693386164bbcffe0b799e8fa8a6dc501faa
