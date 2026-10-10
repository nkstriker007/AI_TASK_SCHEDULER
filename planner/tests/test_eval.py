import json

import pytest

from planner import eval as ev
from planner.fake import FakePlanner, load_fixture_plan
from planner.schema import ExecutionPlan


def plan(name):
    return ExecutionPlan.model_validate(load_fixture_plan(name))


def tasks(*specs):
    """specs: (id, type, deps)"""
    return ExecutionPlan.model_validate({"summary": "s", "tasks": [
        {"id": i, "type": ty, "title": i, "instruction": i, "depends_on": list(d)} for i, ty, d in specs]})


@pytest.mark.parametrize("name,width,depth,sinks", [
    ("chain", 1, 3, ["t3"]),
    ("fork", 2, 2, ["t2", "t3"]),
    ("join", 2, 2, ["t3"]),
    ("diamond", 2, 3, ["t4"]),
    ("two_components", 2, 2, ["t2", "t4"]),
    ("company_research", 2, 4, ["t6"]),
])
def test_metrics_on_fixtures(name, width, depth, sinks):
    m = ev.plan_metrics(plan(name))
    assert (m.width, m.depth, m.sinks) == (width, depth, sinks)


def test_level_is_longest_path_not_shortest():
    # t3 depends on t1 directly and via t2, so it sits on level 2, not level 1.
    p = tasks(("t1", "research", []), ("t2", "summarize", ["t1"]), ("t3", "report", ["t1", "t2"]))
    assert ev.levels(p) == {"t1": 0, "t2": 1, "t3": 2}


def test_max_independent():
    assert ev.max_independent(plan("company_research"), "research") == 2
    assert ev.max_independent(plan("chain"), "research") == 1
    assert ev.max_independent(plan("chain"), "compare") == 0
    # Four researches, one of which builds on another: the largest independent set is 3.
    p = tasks(("t1", "research", []), ("t2", "research", []), ("t3", "research", []),
              ("t4", "research", ["t1"]), ("t5", "report", ["t2", "t3", "t4"]))
    assert ev.max_independent(p, "research") == 3
    # A greedy pick of t1 first would block both t2 and t3; the matching finds {t2, t3}.
    p = tasks(("t1", "research", []), ("t2", "research", ["t1"]), ("t3", "research", ["t1"]),
              ("t4", "report", ["t2", "t3"]))
    assert ev.max_independent(p, "research") == 2


def test_check_reports_each_violation():
    failures = ev.check({"min_width": 4, "max_depth": 3, "single_sink": True, "sink_type": "report",
                         "independent": {"type": "research", "min": 4}, "forbid_types": ["compare"]},
                        plan("company_research"))
    text = "\n".join(failures)
    assert "min_width: got 2" in text and "max_depth: got 4" in text
    assert "independent: 2 mutually independent research" in text
    assert "forbid_types: compare used by ['t5']" in text
    assert not any(f.startswith(("single_sink", "sink_type")) for f in failures)


def test_check_rejects_unknown_assertion():
    with pytest.raises(ValueError):
        ev.check({"min_widht": 2}, plan("chain"))


def test_eval_cases_file():
    cases = ev.load_cases()
    assert set(cases) == {"two_companies", "four_companies", "mostly_sequential", "document_batch", "trivial"}
    assert all(c["request"].strip() for c in cases.values())


def test_run_case_with_fake_planner():
    cases = ev.load_cases()
    ok = ev.run_case(FakePlanner("company_research"), "two_companies", cases["two_companies"])
    assert ok.passed and ok.attempts == 1 and ok.metrics["width"] == 2
    bad = ev.run_case(FakePlanner("company_research"), "four_companies", cases["four_companies"])
    assert not bad.passed and any("min_width" in f for f in bad.failures)


def test_run_case_records_repair_and_rejection():
    cyclic = load_fixture_plan("company_research")
    cyclic["tasks"][0]["depends_on"] = ["t6"]
    case = {"request": "r", "expect": {"single_sink": True}}
    repaired = ev.run_case(FakePlanner(script=[cyclic]), "c", case)
    assert repaired.passed and repaired.attempts == 2
    rejected = ev.run_case(FakePlanner(script=[cyclic, cyclic]), "c", case)
    assert not rejected.passed and rejected.failures[0].startswith("plan rejected: ERROR CYCLE")


def test_main_offline(tmp_path, capsys):
    out = tmp_path / "r.json"
    code = ev.main(["--fake", "company_research", "--case", "two_companies", "--case", "trivial", "--json", str(out)])
    assert code == 1  # trivial wants <= 2 tasks; the fixture has 6
    stdout = capsys.readouterr().out
    assert "PASS  two_companies" in stdout and "FAIL  trivial" in stdout and "1/2 cases passed" in stdout
    assert [r["name"] for r in json.loads(out.read_text())] == ["two_companies", "trivial"]
