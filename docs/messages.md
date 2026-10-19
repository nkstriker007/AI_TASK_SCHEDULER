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

## Result values

The value at a result key is always JSON (design doc 8.9):

| Task | Value | Example |
|---|---|---|
| any type except `expand` | an object with at least a `content` string; executors may add fields such as `key_points` or `sources` | `{"content": "Apple's revenue grew 8%…", "key_points": ["…"]}` |
| `expand` (`result_kind: "subplan"`) | the Subplan itself (contract #1) | `{"tasks": [{"id": "s1", "type": "research", …}]}` |

Helpers in `planner/planner/messages.py`: `encode_output(content, **extra)` builds the stored value, and `result_text(value)` turns a stored value back into text for a downstream prompt (`content` for an output, compact JSON for a Subplan). Readers tolerate a non-JSON value, from before this rule, by treating it as plain text; writers must always store JSON.

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

## Request intake and status (agreed on Day 3)

The design doc names these keys (8.3) but doesn't define their contents. This format was proposed on Day 3 and approved by Person B; the API reads it and atsd writes it. Key and stream names are in `planner/planner/keys.py`. `worker/worker/minisched.py`, a Python stand-in for atsd used only for testing, writes exactly this format, so it can serve as a reference.

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

atsd (`scheduler/src/Service.cpp`, run by `scheduler/tools/atsd.cpp`) translates between Redis entries and the Day 2 `Scheduler` calls. The `Scheduler` and `ExecutionState` never read a clock or do I/O; deadlines, backoff and retry limits live in the service (design doc 8.5–8.7, D33).

| Message or event | Scheduler call | State change |
|---|---|---|
| atsd writes a `TaskMessage` | `dispatchNext()` returns `{node, attempt}` | READY or RETRYING → QUEUED; `attempt` and `attempt_id` come from the returned attempt number; deadline = now + the capability's timeout |
| `task_started` for the current attempt | `started(n)` | QUEUED → RUNNING. A started event for an older attempt is ignored |
| first `task_completed` of **any** attempt | `complete(n)` | QUEUED or RUNNING → COMPLETED; children whose last dependency this was become READY. QUEUED is accepted because a `task_started` can be lost or arrive late; an older (lost) attempt's committed result is valid because tasks are pure (8.5 step 7) |
| a later `task_completed` for the same task | none | recorded as a duplicate, so children are never released twice |
| `task_failed` `retryable` for the current attempt, attempts left | after the backoff: `retry(n)` | the slot is freed at once; the task waits out the backoff (2 s, 4 s with jitter for llm, none for sim; at least `retry_after_ms`), then → RETRYING and is dispatched again with `attempt + 1` |
| `task_failed` `permanent`, or retryable with no attempts left (default 3) | `fail(n)` | → FAILED; every unfinished descendant → CANCELLED, and the request fails once nothing is left running |
| `task_failed` for an older attempt | none | that attempt was already handled when its deadline passed |
| the current attempt's deadline passes | as a retryable `task_failed` | the only lost-attempt detector (D10); no XAUTOCLAIM |

`atsd` writes `request:<id>` after every change (format above) and one JSON line per transition to `runs/<run_id>/events.jsonl` (8.12). Unit tests with a fake bus and a fake clock cover every row (`scheduler/tests/test_service.cpp`); `worker/tests/test_atsd_e2e.py` runs the real binary with real workers.

## Additions beyond the design doc's section 8.4 (agreed)

1. `TaskMessage.estimated_seconds`: the sim executor sleeps for the task's hint (8.9), and the message is the only thing it reads.
2. Stream encoding: one `data` field holding JSON, rather than one stream field per message field.
3. `request_id` may not contain `:`, so `attempt_id` can be split unambiguously.
4. `TaskCompleted.result_kind` defaults to `"output"`; only expand results send `"subplan"`.
5. Events carry only `attempt_id` (as in 8.4); atsd recovers `request_id`, `task_id` and `attempt` by splitting it.
6. Result values are JSON as described in "Result values": the design doc gives the output shape (8.9) but not how outputs and subplans share one key.
