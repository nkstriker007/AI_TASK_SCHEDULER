import copy
import json

import pytest

from planner.paths import EXPECTED_VALIDATION, FIXTURES_DIR
from planner.schema import MAX_SUBPLAN_TASKS, MAX_TASKS, IssueCode
from planner.validate import validate_plan, validate_plan_file, validate_subplan

EXPECTED = json.loads(EXPECTED_VALIDATION.read_text())


def codes(issues):
    return sorted({i.code.value for i in issues})


def load(name):
    return json.loads((FIXTURES_DIR / f"{name}.json").read_text())


def base_plan():
    return copy.deepcopy(load("company_research")["plan"])


def task(id, deps=(), type="research"):
    return {"id": id, "type": type, "title": id, "instruction": f"do {id}", "depends_on": list(deps)}


def test_every_fixture_is_listed():
    on_disk = {p.stem for p in FIXTURES_DIR.glob("*.json")}
    assert on_disk == set(EXPECTED)


@pytest.mark.parametrize("name", sorted(EXPECTED))
def test_fixture_matches_expected(name):
    result = validate_plan_file(load(name))
    exp = EXPECTED[name]
    assert result.ok == exp["valid"]
    assert codes(result.errors) == exp["errors"]
    assert codes(result.warnings) == exp["warnings"]


def test_valid_plan_returns_model():
    result = validate_plan(base_plan())
    assert result.ok
    assert [t.id for t in result.value.tasks] == ["t1", "t2", "t3", "t4", "t5", "t6"]


def test_schema_version():
    plan = base_plan()
    plan["schema_version"] = 2
    assert codes(validate_plan(plan).errors) == ["SCHEMA_VERSION"]


def test_empty_plan():
    plan = base_plan()
    plan["tasks"] = []
    assert codes(validate_plan(plan).errors) == ["EMPTY_PLAN"]


def test_too_many_tasks():
    plan = base_plan()
    plan["tasks"] = [task(f"t{i}") for i in range(1, MAX_TASKS + 2)]
    assert "TOO_MANY_TASKS" in codes(validate_plan(plan).errors)


def test_duplicate_dependency():
    plan = base_plan()
    plan["tasks"][2]["depends_on"] = ["t1", "t1"]
    result = validate_plan(plan)
    assert codes(result.errors) == ["DUPLICATE_DEPENDENCY"]
    assert result.errors[0].task_id == "t3"


@pytest.mark.parametrize(
    "mutate",
    [
        lambda t: t.update(id="task1"),  # bad pattern
        lambda t: t.update(type="search"),  # bad enum
        lambda t: t.update(title=""),  # length
        lambda t: t.update(instruction="x" * 2001),
        lambda t: t.update(extra="nope"),  # unknown field
        lambda t: t.update(hints={"estimated_seconds": -1}),
    ],
)
def test_schema_errors(mutate):
    plan = base_plan()
    mutate(plan["tasks"][0])
    assert "SCHEMA" in codes(validate_plan(plan).errors)


def test_reports_all_issues_at_once():
    plan = base_plan()
    del plan["tasks"][0]["instruction"]  # SCHEMA
    plan["tasks"][1]["depends_on"] = ["t2"]  # SELF_DEPENDENCY
    plan["tasks"][2]["depends_on"] = ["t42"]  # UNKNOWN_DEPENDENCY
    assert codes(validate_plan(plan).errors) == ["SCHEMA", "SELF_DEPENDENCY", "UNKNOWN_DEPENDENCY"]


def test_cycle_lists_unprocessed_tasks_including_downstream():
    plan = {"schema_version": 1, "summary": "s",
            "tasks": [task("t1", ["t2"]), task("t2", ["t1"]), task("t3", ["t2"]), task("t4")]}
    result = validate_plan(plan)
    [issue] = result.errors
    assert issue.code == IssueCode.CYCLE
    assert "t1, t2, t3" in issue.message and "t4" not in issue.message


def test_transitive_edge_is_a_warning_and_kept():
    plan = {"schema_version": 1, "summary": "s",
            "tasks": [task("t1"), task("t2", ["t1"]), task("t3", ["t1", "t2"], type="compare")]}
    result = validate_plan(plan)
    assert result.ok
    assert codes(result.warnings) == ["TRANSITIVE_EDGE"]
    assert result.value.tasks[2].depends_on == ["t1", "t2"]  # never silently removed


def test_non_dict_input():
    assert codes(validate_plan("not a plan").errors) == ["SCHEMA"]
    assert codes(validate_plan_file([]).errors) == ["SCHEMA"]


# --- subplans ----------------------------------------------------------------


def sub(id, deps=(), type="research"):
    return {"id": id, "type": type, "title": id, "instruction": f"do {id}", "depends_on": list(deps)}


def test_valid_subplan():
    result = validate_subplan({"tasks": [sub("s1"), sub("s2"), sub("s3", ["s1", "s2"], "compare")]})
    assert result.ok
    assert result.warnings == []  # multiple sinks are normal in a subplan


def test_subplan_rejects_nested_expand():
    result = validate_subplan({"tasks": [sub("s1", type="expand")]})
    assert codes(result.errors) == ["SCHEMA"]
    assert "nested expand" in result.errors[0].message


def test_subplan_rejects_non_local_dependency():
    result = validate_subplan({"tasks": [sub("s1", ["t1"])]})
    assert codes(result.errors) == ["UNKNOWN_DEPENDENCY"]


def test_subplan_rejects_t_ids_cycles_and_oversize():
    assert "SCHEMA" in codes(validate_subplan({"tasks": [sub("t1")]}).errors)
    assert codes(validate_subplan({"tasks": [sub("s1", ["s2"]), sub("s2", ["s1"])]}).errors) == ["CYCLE"]
    big = {"tasks": [sub(f"s{i}") for i in range(1, MAX_SUBPLAN_TASKS + 2)]}
    assert "TOO_MANY_TASKS" in codes(validate_subplan(big).errors)
    assert codes(validate_subplan({"tasks": [sub("s1"), sub("s2")]}, max_tasks=1).errors) == ["TOO_MANY_TASKS"]


def test_invalid_sim_subplan_invalidates_plan():
    doc = load("expand_canned")
    doc["plan"]["tasks"][1]["hints"]["sim_subplan"]["tasks"][0]["depends_on"] = ["s9"]
    result = validate_plan_file(doc)
    assert codes(result.errors) == ["UNKNOWN_DEPENDENCY"]
    assert result.errors[0].task_id == "t2"


def test_empty_sim_subplan_is_a_schema_error_only():
    # Same codes as the C++ loader: the subplan's length limit is a plan-level SCHEMA error.
    doc = load("expand_canned")
    doc["plan"]["tasks"][1]["hints"]["sim_subplan"]["tasks"] = []
    assert codes(validate_plan_file(doc).errors) == ["SCHEMA"]
