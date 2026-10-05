"""Schema + semantic validation for plans, plan files and subplans.

Validators never stop at the first problem and never repair anything: they
return every issue so a repair prompt (and a human) sees the whole picture.
Issue codes are shared with the C++ PlanLoader/Dag (docs/contract.md).

Semantic checks run on the raw JSON, so a plan with a schema error still gets
its dependency and cycle problems reported in the same pass.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ValidationError

from .schema import (
    MAX_SUBPLAN_TASKS,
    ExecutionPlan,
    IssueCode,
    PlanFile,
    Subplan,
    ValidationIssue,
)

M = TypeVar("M", bound=BaseModel)


@dataclass
class ValidationResult(Generic[M]):
    value: M | None
    issues: list[ValidationIssue] = field(default_factory=list)

    @property
    def errors(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == "error"]

    @property
    def warnings(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == "warning"]

    @property
    def ok(self) -> bool:
        return self.value is not None and not self.errors


def _error(code: IssueCode, message: str, task_id: str = "") -> ValidationIssue:
    return ValidationIssue(code=code, severity="error", task_id=task_id, message=message)


def _warning(code: IssueCode, message: str, task_id: str = "") -> ValidationIssue:
    return ValidationIssue(code=code, severity="warning", task_id=task_id, message=message)


# --- public entry points -----------------------------------------------------


def validate_plan(data: Any) -> ValidationResult[ExecutionPlan]:
    issues = _schema_issues(ExecutionPlan, data, plan_path=())
    tasks = data.get("tasks") if isinstance(data, dict) else None
    issues += _graph_issues(tasks, schema_ok=not issues)
    issues += _sim_subplan_issues(tasks)
    return _finish(ExecutionPlan, data, issues)


def validate_plan_file(data: Any) -> ValidationResult[PlanFile]:
    issues = _schema_issues(PlanFile, data, plan_path=("plan",))
    plan = data.get("plan") if isinstance(data, dict) else None
    tasks = plan.get("tasks") if isinstance(plan, dict) else None
    issues += _graph_issues(tasks, schema_ok=not issues)
    issues += _sim_subplan_issues(tasks)
    return _finish(PlanFile, data, issues)


def validate_subplan(data: Any, max_tasks: int = MAX_SUBPLAN_TASKS) -> ValidationResult[Subplan]:
    """Validate an expand task's result. Dependencies must be local (s1, s2, ...);
    the implicit dependency on the expand task is added by the scheduler on splice.
    Per-request limits (total tasks, number of expansions) are enforced by atsd."""
    issues = _schema_issues(Subplan, data, plan_path=(), max_tasks=max_tasks)
    tasks = data.get("tasks") if isinstance(data, dict) else None
    issues += _graph_issues(tasks, schema_ok=not issues, sinks_warning=False)
    return _finish(Subplan, data, issues)


def _finish(model: type[M], data: Any, issues: list[ValidationIssue]) -> ValidationResult[M]:
    if any(i.severity == "error" for i in issues):
        return ValidationResult(None, issues)
    return ValidationResult(model.model_validate(data), issues)


# --- schema (pydantic) -------------------------------------------------------


def _schema_issues(
    model: type[BaseModel],
    data: Any,
    plan_path: tuple[str, ...],
    max_tasks: int | None = None,
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    try:
        model.model_validate(data)
    except ValidationError as exc:
        for err in exc.errors():
            issues.append(_map_pydantic_error(err, data, plan_path, subplan=model is Subplan))
    # A caller-supplied limit lower than the model's own (subplan config).
    if max_tasks is not None:
        tasks = data.get("tasks") if isinstance(data, dict) else None
        if isinstance(tasks, list) and len(tasks) > max_tasks and not any(
            i.code == IssueCode.TOO_MANY_TASKS for i in issues
        ):
            issues.append(_too_many(len(tasks), max_tasks))
    return issues


def _too_many(n: int, limit: int) -> ValidationIssue:
    return _error(IssueCode.TOO_MANY_TASKS, f"plan has {n} tasks; the limit is {limit}")


def _map_pydantic_error(
    err: dict, data: Any, plan_path: tuple[str, ...], subplan: bool
) -> ValidationIssue:
    loc = tuple(err["loc"])
    rel = loc[len(plan_path) :] if loc[: len(plan_path)] == plan_path else None
    where = ".".join(str(p) for p in loc) or "<root>"

    if rel == ("schema_version",):
        return _error(IssueCode.SCHEMA_VERSION, f"schema_version must be 1 (got {err.get('input')!r})")
    if rel == ("tasks",) and err["type"] == "too_short":
        return _error(IssueCode.EMPTY_PLAN, "plan has no tasks")
    if rel == ("tasks",) and err["type"] == "too_long":
        return _too_many(len(err["input"]), err["ctx"]["max_length"])

    task_id = ""
    msg = f"{where}: {err['msg']}"
    if rel is not None and len(rel) >= 2 and rel[0] == "tasks" and isinstance(rel[1], int):
        task_id = _raw_task_id(data, plan_path, rel[1])
        if subplan and rel[2:] == ("type",) and err.get("input") == "expand":
            msg = f"{where}: nested expand is not allowed inside a subplan"
    return _error(IssueCode.SCHEMA, msg, task_id)


def _raw_task_id(data: Any, plan_path: tuple[str, ...], index: int) -> str:
    node = data
    for key in plan_path + ("tasks",):
        node = node.get(key) if isinstance(node, dict) else None
    if isinstance(node, list) and 0 <= index < len(node) and isinstance(node[index], dict):
        tid = node[index].get("id")
        return tid if isinstance(tid, str) else ""
    return ""


# --- graph (semantic) --------------------------------------------------------


def _graph_issues(
    tasks: Any,
    schema_ok: bool,
    sinks_warning: bool = True,
) -> list[ValidationIssue]:
    """Structural checks shared with C++, plus Python-only warnings.

    Contract details the C++ side mirrors (docs/contract.md):
      * tasks without a string id are skipped (already a SCHEMA error);
      * the cycle check is skipped when IDs are duplicated (graph is ambiguous);
      * Kahn runs over unique edges to known tasks, excluding self-edges
        (reported as SELF_DEPENDENCY, not CYCLE).
    """
    if not isinstance(tasks, list):
        return []
    issues: list[ValidationIssue] = []

    ids: list[str] = []
    seen: set[str] = set()
    duplicated: set[str] = set()
    for t in tasks:
        tid = t.get("id") if isinstance(t, dict) else None
        if not isinstance(tid, str):
            continue
        if tid in seen and tid not in duplicated:
            duplicated.add(tid)
            issues.append(_error(IssueCode.DUPLICATE_ID, f"task id {tid!r} is used more than once", tid))
        seen.add(tid)
        ids.append(tid)

    parents: dict[str, list[str]] = {tid: [] for tid in ids}
    for t in tasks:
        tid = t.get("id") if isinstance(t, dict) else None
        deps = t.get("depends_on", []) if isinstance(t, dict) else None
        if not isinstance(tid, str) or not isinstance(deps, list):
            continue
        local_seen: set[str] = set()
        for dep in deps:
            if not isinstance(dep, str):
                continue
            if dep in local_seen:
                issues.append(_error(IssueCode.DUPLICATE_DEPENDENCY, f"depends on {dep!r} more than once", tid))
                continue
            local_seen.add(dep)
            if dep == tid:
                issues.append(_error(IssueCode.SELF_DEPENDENCY, "task depends on itself", tid))
            elif dep not in seen:
                issues.append(_error(IssueCode.UNKNOWN_DEPENDENCY, f"depends on unknown task {dep!r}", tid))
            elif tid not in duplicated:
                parents[tid].append(dep)

    if not duplicated:
        unprocessed = _kahn_unprocessed(ids, parents)
        if unprocessed:
            issues.append(
                _error(IssueCode.CYCLE, "dependency cycle among tasks: " + ", ".join(unprocessed))
            )

    has_errors = any(i.severity == "error" for i in issues)
    if schema_ok and not has_errors:
        issues += _transitive_edge_warnings(ids, parents)
        if sinks_warning:
            issues += _multiple_sinks_warning(ids, parents)
    return issues


def _kahn_unprocessed(ids: list[str], parents: dict[str, list[str]]) -> list[str]:
    """Return tasks Kahn's algorithm could not emit, in plan order."""
    emitted = set(topological_order(ids, parents))
    return [tid for tid in ids if tid not in emitted]


def topological_order(ids: list[str], parents: dict[str, list[str]]) -> list[str]:
    """Kahn's algorithm; the result is shorter than ids if the graph has a cycle."""
    indegree = {tid: len(parents[tid]) for tid in ids}
    children: dict[str, list[str]] = {tid: [] for tid in ids}
    for tid in ids:
        for p in parents[tid]:
            children[p].append(tid)
    queue = deque(tid for tid in ids if indegree[tid] == 0)
    order: list[str] = []
    while queue:
        n = queue.popleft()
        order.append(n)
        for c in children[n]:
            indegree[c] -= 1
            if indegree[c] == 0:
                queue.append(c)
    return order


def _transitive_edge_warnings(ids: list[str], parents: dict[str, list[str]]) -> list[ValidationIssue]:
    ancestors: dict[str, set[str]] = {}
    for tid in topological_order(ids, parents):
        acc: set[str] = set()
        for p in parents[tid]:
            acc.add(p)
            acc |= ancestors[p]
        ancestors[tid] = acc
    issues = []
    for tid in ids:
        for p in parents[tid]:
            via = [q for q in parents[tid] if q != p and p in ancestors[q]]
            if via:
                issues.append(
                    _warning(
                        IssueCode.TRANSITIVE_EDGE,
                        f"edge {p} -> {tid} is also implied via {via[0]}; kept (data edge)",
                        tid,
                    )
                )
    return issues


def _multiple_sinks_warning(ids: list[str], parents: dict[str, list[str]]) -> list[ValidationIssue]:
    has_child = {p for tid in ids for p in parents[tid]}
    sinks = [tid for tid in ids if tid not in has_child]
    if len(sinks) > 1:
        return [
            _warning(
                IssueCode.MULTIPLE_SINKS,
                "more than one task has no dependents: " + ", ".join(sinks),
            )
        ]
    return []


# Inside a plan, an empty or oversized sim_subplan is a plain SCHEMA error (the
# Subplan model's length limits), matching the C++ loader.
_SUBPLAN_SCHEMA_CODES = {IssueCode.SCHEMA, IssueCode.EMPTY_PLAN, IssueCode.TOO_MANY_TASKS}


def _sim_subplan_issues(tasks: Any) -> list[ValidationIssue]:
    """Canned subplans in fixtures must pass the same subplan validation."""
    if not isinstance(tasks, list):
        return []
    issues = []
    for t in tasks:
        if not isinstance(t, dict) or not isinstance(t.get("hints"), dict):
            continue
        sub = t["hints"].get("sim_subplan")
        if sub is None:
            continue
        for issue in validate_subplan(sub).errors:
            if issue.code in _SUBPLAN_SCHEMA_CODES:
                continue  # already reported as SCHEMA by the plan-level schema check
            issues.append(issue.model_copy(update={
                "task_id": t.get("id", "") if isinstance(t.get("id"), str) else "",
                "message": f"sim_subplan: {issue.message}",
            }))
    return issues
