# Contract #1: plan schema (frozen end of Day 1)

Changing anything here takes a PR that updates `planner/planner/schema.py`, the generated `schemas/`, the fixtures, and both validators, reviewed by the other person.

## Sources of truth

| What | Where |
|---|---|
| Models (source of truth) | `planner/planner/schema.py` |
| Generated JSON Schema | `schemas/execution_plan.schema.json`, `schemas/plan_file.schema.json`, `schemas/subplan.schema.json` (`python -m planner.export_schemas`; a test fails if stale) |
| Fixtures | `examples/plans/*.json` (plan files, i.e. with the envelope) |
| Expected result per fixture | `examples/expected_validation.json`: `valid`, error codes, warning codes. The only expected-results file: the Python tests, the C++ fixture tests and `tests/parity.py` all read it. Every fixture must be listed. |

## Plan file (what C++ reads)

```jsonc
{
  "request_id": "req_2f9c1a7e",           // assigned by the runtime, never by the LLM
  "created_at": "2026-09-28T20:40:00Z",
  "plan": {
    "schema_version": 1,                   // optional, defaults to 1; any other value is SCHEMA_VERSION
    "summary": "…",                        // 1-300 characters
    "tasks": [                             // 1-50 tasks
      {
        "id": "t1",                        // ^t[0-9]+$, unique
        "type": "research",                // research | summarize | analyze | compare | report | expand
        "title": "…",                      // 1-120 characters
        "instruction": "…",                // 1-2000 characters
        "depends_on": ["…"],               // optional, defaults to []; ids whose OUTPUT this task consumes
        "hints": {                         // optional, may be null
          "estimated_seconds": 20,         // optional, >= 0 or null
          "expected_fanout": 3,            // optional, integer >= 1 or null (expand tasks)
          "sim_subplan": { "tasks": [] }   // optional, fixtures only (see below)
        }
      }
    ]
  }
}
```

`request_id` and `created_at` are non-empty strings assigned by the runtime. Unknown fields are errors at every level (Pydantic `extra="forbid"`). Lengths are counted in Unicode characters, not bytes.

## Additions beyond the design doc's section 4

- `hints.expected_fanout` (integer ≥ 1, optional): only meaningful on `expand` tasks.
- `hints.sim_subplan` (Subplan, optional): canned subplan for the simulator; appears only in fixtures and is never requested from the LLM. It must pass subplan validation on both sides: schema problems inside it (bad local ID, nested `expand`, unknown hint, 0 or more than 20 tasks) are `SCHEMA`, and its graph problems use the normal codes. All of these issues carry the owning expand task's ID.
- **Subplan** (`expand` result): `{ "tasks": [SubplanTask, ...] }`, 1–20 tasks. A SubplanTask has the same fields as a PlannedTask, but IDs match `^s[0-9]+$`, `type` excludes `expand` (no nesting), and `hints` has only `estimated_seconds`. `depends_on` may name only other subplan tasks. The implicit dependency on the expand task, the renaming to `t2.1`, `t2.2`, ..., and the per-request limits (≤ 100 tasks, ≤ 3 expansions) belong to atsd.

## Issue codes and the rules both sides implement

| Code | Rule |
|---|---|
| `SCHEMA` | Missing/unknown field, wrong type, bad enum, bad ID pattern, length or range violation |
| `SCHEMA_VERSION` | `schema_version` ≠ 1 |
| `EMPTY_PLAN` | `tasks` is empty |
| `TOO_MANY_TASKS` | more than 50 tasks (20 in a subplan) |
| `DUPLICATE_ID` | an ID is used more than once (reported once per ID) |
| `UNKNOWN_DEPENDENCY` | `depends_on` names a task that does not exist |
| `SELF_DEPENDENCY` | a task depends on itself |
| `DUPLICATE_DEPENDENCY` | the same ID appears twice in one `depends_on` (reported once per repeat) |
| `CYCLE` | Kahn's algorithm cannot emit every task; one issue lists the unemitted tasks in plan order |
| `TRANSITIVE_EDGE` | warning, Python only |
| `MULTIPLE_SINKS` | warning, Python only |

The details that decide parity:

1. Report every issue; don't stop at the first one.
   **Known difference (to settle before parity covers it):** Python still runs the graph checks when there are schema errors, using the tasks that have a string `id`. C++ runs `Dag::build` only after `PlanLoader` passes, so a plan with both kinds of error gets only `SCHEMA` from C++. No fixture mixes the two, so parity passes today.
2. Self-edges are reported only as `SELF_DEPENDENCY` and are left out of Kahn. Unknown dependencies and repeated dependencies are left out too, so `invalid_self_dep` has no `CYCLE`.
3. If any ID is duplicated, skip the cycle check because the graph is ambiguous.
4. The C++ side checks that text fields are non-empty. The length limits are enforced only in Python (design doc 3.8), and the fixtures don't depend on them.

Pydantic reports some of these as ordinary validation errors, so the Python validator maps them: `tasks` too short → `EMPTY_PLAN`, too long → `TOO_MANY_TASKS`, `schema_version` failing `Literal[1]` → `SCHEMA_VERSION`, everything else → `SCHEMA`. Parity is checked on codes only; messages may differ.

## Parity checks

- `make parity` (`tests/parity.py`): runs `scheduler/build/ats_validate <plan.json>` and `planner.check` over every fixture and compares both with `examples/expected_validation.json`. `ats_validate` prints `{"valid": bool, "issues": [{"code", "task_id", "message"}], ...}` and exits 0 when the plan is valid, 1 when it is invalid.
- `tests/test_parity.py`: the same comparison against `ats_sim` once it exists (Day 2), plus schema freshness.

## ats_sim expectations used by `tests/test_parity.py`

- Exit code 1 means the plan is invalid. Exit 0 (succeeded) and exit 2 (execution failed) both count as a valid plan.
- With `--json`, an invalid plan prints `{"valid": false, "issues": [{"code": "...", "task_id": "...", "message": "..."}]}`. The parity test then compares the set of error codes too.
- The binary is found at `scheduler/build/ats_sim`, or at `$ATS_SIM` if that is set.
