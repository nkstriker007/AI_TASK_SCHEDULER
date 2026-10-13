# Contract #2: task messages and events (frozen end of Day 2)

Agreed by Person A and Person B. Changing anything here takes a PR that updates `planner/planner/messages.py`, the generated `schemas/`, `examples/messages/`, and both sides, reviewed by the other person.

## Sources of truth

| What | Where |
|---|---|
| Models (source of truth) | `planner/planner/messages.py` |
| Generated JSON Schema | `schemas/task_message.schema.json`, `schemas/task_event.schema.json` (`python -m planner.export_schemas`; a test fails if stale) |
| Examples | `examples/messages/*.json` (each one is validated by `planner/tests/test_messages.py`) |

## Transport

| Stream | Writer → reader | Entry |
|---|---|---|
| `tasks:llm`, `tasks:sim` | atsd → workers (group `workers`) | `TaskMessage` |
| `task:events` | workers → atsd | `TaskStarted`, `TaskCompleted` or `TaskFailed` |

Each stream entry has **one field, `data`**, whose value is the message as a JSON string (`XADD tasks:llm * data '{...}'`). Unknown fields are errors at every level.

## IDs and keys

- `attempt_id` = `<request_id>:<task_id>:<attempt>`, e.g. `req_2f9c1a7e:t5:2`. So `request_id` must not contain `:`.
- `task_id` is a plan ID (`t5`) or a spliced subplan ID (`t2.1`). Subplan-local IDs (`s1`) never appear in messages. `t2.join` is completed inside atsd and never dispatched.
- Result key = `result:<request_id>:<task_id>`, written once by the worker with `SET NX`.

## TaskMessage (one dispatched attempt)

```json
{ "request_id": "req_2f9c1a7e", "task_id": "t5", "attempt": 2,
  "attempt_id": "req_2f9c1a7e:t5:2", "type": "compare",
  "instruction": "Compare financial performance and recent developments.",
  "inputs": { "t3": "result:req_2f9c1a7e:t3", "t4": "result:req_2f9c1a7e:t4" },
  "deadline_ms": 1759112400000, "estimated_seconds": 15 }
```

| Field | Type | Rule |
|---|---|---|
| `request_id` | string | non-empty, no `:` |
| `task_id` | string | `^t[0-9]+(\.[0-9]+)?$` |
| `attempt` | int | ≥ 1; a retry is a new message with `attempt + 1` |
| `attempt_id` | string | must equal `<request_id>:<task_id>:<attempt>` |
| `type` | enum | the plan task types (research, summarize, analyze, compare, report, expand) |
| `instruction` | string | non-empty |
| `inputs` | object | optional, default `{}`; dependency task ID → its result key in this request |
| `deadline_ms` | int | absolute Unix epoch ms; atsd owns it (8.7) |
| `estimated_seconds` | number or null | optional; the plan hint, used by the sim executor |

## Events on `task:events`

Every event has `event`, `attempt_id`, `worker_id` (non-empty) and `ts_ms` (worker wall clock, Unix epoch ms). `event` is required and selects the type.

| `event` | Extra fields |
|---|---|
| `task_started` | none |
| `task_completed` | `result_key` (must be this attempt's task slot), `committed` (bool), `result_kind` (`"output"` default, or `"subplan"` for an expand result to splice), `exec_ms` (int ≥ 0), `tokens_in` / `tokens_out` (int ≥ 0 or null, null for sim) |
| `task_failed` | `error_class` (`"retryable"` or `"permanent"`), `message` (1–2000 chars), `retry_after_ms` (int ≥ 0 or null) |

`committed: false` means `SET NX` found an earlier attempt's result, so this attempt's output was discarded. atsd treats the first `task_completed` for a task as the completion and records later ones (8.5).

## Request intake and status (proposed on Day 3, needs Person B's sign-off)

The design doc names these keys (8.3) but doesn't define their contents. The API and the worker side are built against the format below. Key and stream names are in `planner/planner/keys.py`. `worker/worker/minisched.py`, a Python stand-in for atsd used only for testing, writes exactly this format, so it can serve as a reference.

| Key | Type | Writer → reader | Content |
|---|---|---|---|
| `requests:new` | stream | API → atsd (group `scheduler`) | One field `data`: the plan file JSON (`{request_id, created_at, plan}`, contract #1), already validated by the API |
| `plan:<request_id>` | string | API → API | The same plan file JSON, written once (`SET NX`) before the `XADD`, so `GET /requests/{id}` works before atsd has picked the request up. atsd never needs to read it |
| `request:<request_id>` | hash | atsd → API | Fields below |

`request:<request_id>` fields, all strings:

| Field | Value |
|---|---|
| `status` | `RUNNING`, `SUCCEEDED`, `FAILED`, or `REJECTED` (the C++ loader rejected a plan the API accepted; see the known parity difference in `docs/contract.md`) |
| `sink` | ID of the single task with no dependents (empty if there are several); the API returns its result as the final output |
| `updated_ms` | Unix epoch ms of the last change |
| `task:<task_id>` | JSON `{"state": "<TaskState in upper case>", "attempt": <int, 0 before the first dispatch>}`, one field per task, including spliced tasks (`task:t2.1`) |
| `error` | optional: why the request failed or was rejected |

While the hash doesn't exist, the API reports the request as `SUBMITTED` with every task `WAITING`. Updating one task is a single `HSET request:<id> task:<tid> ...`, so atsd never rewrites the whole hash.

## How the scheduler applies messages and events

Person B's Day 2 `Scheduler` (`scheduler/include/ats/Scheduler.hpp`) already exposes one call per message or event, so atsd (Day 3) only has to translate between Redis entries and these calls. It never reads a clock or does I/O itself; deadlines and retry limits live in the service layer (design doc 8.5–8.7).

| Message or event | Scheduler call | State change |
|---|---|---|
| atsd writes a `TaskMessage` | `dispatchNext()` returns `{node, attempt}` | READY or RETRYING → QUEUED; `attempt` and `attempt_id` come from the returned attempt number |
| `task_started` | `started(n)` | QUEUED → RUNNING |
| first `task_completed` for the task | `complete(n)` | QUEUED or RUNNING → COMPLETED; children whose last dependency this was become READY. QUEUED is accepted because a `task_started` can be lost or arrive late |
| a later `task_completed` for the same task | none | recorded only (8.5), so children are never released twice |
| `task_failed` with `error_class: "retryable"` and attempts left | `retry(n)` | → RETRYING; the task re-enters the ready set and its next dispatch is a new message with `attempt + 1` |
| `task_failed` with `"permanent"`, or retryable with no attempts left | `fail(n)` | → FAILED; every unfinished descendant → CANCELLED, and the request fails |
| attempt deadline passes (Day 5) | same as a retryable `task_failed` | see 8.7 |

The simulator (`ats_sim --flaky` / `--fail`) drives the same calls today, so the retry and failure paths are already tested before Redis is involved.

## Additions beyond the design doc's section 8.4 (agreed)

1. `TaskMessage.estimated_seconds`: the sim executor sleeps for the task's hint (8.9), and the message is the only thing it reads.
2. Stream encoding: one `data` field holding JSON, rather than one stream field per message field.
3. `request_id` may not contain `:`, so `attempt_id` can be split unambiguously.
4. `TaskCompleted.result_kind` defaults to `"output"`; only expand results send `"subplan"`.
5. Events carry only `attempt_id` (as in 8.4); atsd recovers `request_id`, `task_id` and `attempt` by splitting it.
