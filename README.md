# AI Task Scheduler

A natural-language request is turned into a validated dependency graph of tasks by an LLM planner (Python).
A C++ runtime then schedules and executes it in parallel across workers over Redis Streams.
The design is in `docs/` (architecture document), the cross-language contract is in `docs/contract.md`,
and build-time decisions are in `docs/DECISIONS.md`.

## Quick start (macOS)

C++ runtime:

```bash
xcode-select --install          # Apple clang, if not already installed
brew install cmake
brew install uv                 # Python toolchain for the planner
make test                       # C++ unit tests + Python unit tests + schema freshness
make demo                       # ats_sim: simulated parallel runs with timeline and metrics
make parity                     # C++ and Python validators vs examples/messages/               JSON examples of every task message and event (contract #2)
examples/requests/eval_cases.yaml  planner evaluation cases
examples/expected_validation.json
./scheduler/build/ats_validate examples/plans/invalid_cycle.json
```

Python planner on its own:

```bash
cd planner
uv sync                         # create .venv with pydantic, groq, pytest
uv run pytest                   # schema, validator, FakePlanner, Groq planner (fake client), CLI
uv run python -m planner.cli "Compare Apple and NVIDIA" --fake company_research   # offline, no API key
uv run python -m planner.check ../examples/plans/*.json                            # validate fixtures
uv run python -m planner.export_schemas                                            # regenerate schemas/
cd .. && uv run --project planner pytest tests/test_parity.py                      # Python vs C++ parity
```

Live planning with Groq: create `.env` in the repo root (it is gitignored) with

```bash
GROQ_API_KEY=<your key>
ATS_PLANNER_MODEL=openai/gpt-oss-120b   # optional, this is the default
```

The planner does not load `.env` itself, so pass it with `--env-file`:

```bash
cd planner && uv run --env-file ../.env python -m planner.cli "Research Apple and NVIDIA and compare them" -o plan.json
```

If the first plan fails validation, the planner sends the issues back once and asks for a corrected plan
(the CLI says so on stderr); a second failure is a rejection.
CLI exit codes: 0 plan written, 1 plan rejected by the validator (after one repair attempt), 2 provider error.

Planner evaluation (5 structural cases in `examples/requests/eval_cases.yaml`, one Groq call each plus any repair):

```bash
cd planner && uv run --env-file ../.env python -m planner.eval
uv run python -m planner.eval --fake company_research   # offline plumbing check
```

Redis 7 in Docker (needs Docker Desktop):

```bash
make redis-up                   # docker compose up -d --wait redis
docker compose exec redis redis-cli ping
make redis-down
```

Workers (Day 3, Person A), with Redis running (`make redis-up`):

```bash
make worker-demo                # 3 sim workers + the dev runner on company_research.json
cd planner
uv run python -m planner.worker --stream tasks:sim -v                         # one sim worker
uv run --env-file ../.env python -m planner.worker --stream tasks:llm -v      # one Groq worker
uv run python -m planner.devrun ../examples/plans/company_research.json       # dev runner (not atsd)
```

`ATS_REDIS_URL` (default `redis://localhost:6379/0`) selects the Redis instance; `ATS_EXECUTOR_MODEL`
overrides the LLM worker's model (default `openai/gpt-oss-120b`). The worker tests start a throwaway
`redis-server` if one is installed, use `ATS_TEST_REDIS_URL` if set (CI), and are skipped otherwise.

Redis spike (Day 1 evening):

```bash
make redis-deps                 # brew install redis hiredis redis-plus-plus
brew services start redis
make redis-spike                # builds and runs tools/redis_ping.cpp (PING, XADD, XRANGE)
```

## Layout

```
docs/contract.md                 contract #1: plan file format, issue codes, parity rules
docs/messages.md                 contract #2 (draft): task messages and events on Redis Streams
docs/DECISIONS.md                decisions made during the build (D15, D17–D23)
schemas/                         JSON Schema generated from planner/planner/schema.py (do not edit by hand)
examples/plans/                  shared fixtures (plan files with the envelope)
examples/expected_validation.json  expected valid/errors/warnings per fixture (read by the C++ tests, the Python tests and tests/parity.py)
planner/                         Python planner (Person A)
  planner/schema.py              Pydantic models: ExecutionPlan, PlanFile, Subplan (source of truth)
  planner/messages.py            Pydantic models: TaskMessage and task events (contract #2)
  planner/eval.py                planner evaluation: width, depth, sinks, independence
  planner/validate.py            schema + semantic validation, all issue codes and warnings
  planner/llm.py                 Planner base class, GroqPlanner (strict structured output)
  planner/fake.py                FakePlanner: fixture plans, scripted outputs for tests
  planner/executors.py           SimExecutor and LLMExecutor (Groq): run one task, classify failures
  planner/worker.py              Redis Streams worker: consumer group, XAUTOCLAIM, SET NX results, events
  planner/devrun.py              development runner that drives a plan through workers (not atsd)
  planner/cli.py                 request -> validated plan file
  planner/check.py               validate plan files (used for parity)
  planner/export_schemas.py      writes / checks schemas/
  planner/prompts/               planner and executor system prompts
  tests/
scheduler/                       C++20 runtime (Person B)
  include/ats/                   Types, PlanLoader, Dag, ExecutionState, Analysis, Policy, Scheduler, Simulator
  src/
  tools/                         ats_validate (parity CLI), ats_sim (simulator), redis_ping (spike)
  tests/                         GoogleTest: loader, dag, state machine, fixtures, property test
tests/parity.py                  C++ and Python validators vs expected_validation.json (make parity)
tests/test_parity.py             Python validator vs C++ (ats_sim), schema freshness
```

## Day 1 status: Person A

- [x] Pydantic schema (`schema.py`): `ExecutionPlan`, `PlanFile` envelope, `expand` task type, `Subplan`/`SubplanTask` (local IDs `s1…`, no nested expand, ≤ 20 tasks), `hints.expected_fanout` and fixture-only `hints.sim_subplan`
- [x] JSON Schema export to `schemas/` with a staleness test
- [x] Validator (`validate.py`): every issue collected, shared issue codes, Pydantic errors mapped to `EMPTY_PLAN` / `TOO_MANY_TASKS` / `SCHEMA_VERSION`, `TRANSITIVE_EDGE` and `MULTIPLE_SINKS` warnings, subplan validation
- [x] `FakePlanner` returning fixture plans, with a script of raw outputs for testing the repair loop
- [x] Provider chosen (Groq, `openai/gpt-oss-120b`, D17) and research data source proposed (LLM-only, D15)
- [x] `GroqPlanner` (`llm.py`): one strict structured-output call against a relaxed strict-mode schema, nulls normalized, then the full validator (D18)
- [x] Planner CLI with a runtime-assigned `request_id`, plus `planner.check` for parity
- [x] `expand_canned` fixture and `expected_validation.json`; `docs/contract.md` and `docs/DECISIONS.md`

## Day 2 status: Person A

- [x] Repair loop: one retry with the validator's issues (the Groq call replays its previous answer), then reject
- [x] Message models (contract #2): `TaskMessage`, `task_started` / `task_completed` (incl. `result_kind: "subplan"`) / `task_failed`, exported to `schemas/`, examples in `examples/messages/`, draft in `docs/messages.md` for Person B's review
- [x] `docker-compose.yml` with Redis 7 (append-only persistence, health check); `make redis-up` / `make redis-down`
- [x] `eval_cases.yaml` + `planner.eval`; first Groq run: 5/5 cases pass, no repairs needed
- [ ] Message contract sign-off with Person B
- [x] `make demo` (runs `ats_sim` on the fixtures)

## Day 1 status: Person B

- [x] CMake project (C++20, nlohmann/json and GoogleTest via FetchContent), CI on Ubuntu + macOS
- [x] `PlanLoader`: schema checks on every field, unknown fields rejected, lengths counted in characters, all issues collected; `hints.expected_fanout` and `hints.sim_subplan` (with subplan checks) per contract #1
- [x] `Dag::build`: duplicate ids, unknown/self/duplicate dependencies, Kahn topological order, cycle reported as a path
- [x] `ExecutionState`: full state set (WAITING…RETRYING…CANCELLED), attempts, failure cancels descendants, illegal transitions throw
- [x] 48 tests incl. a randomized property test; verified with GCC, Clang (-Werror) and ASan/UBSan
- [x] `ats_validate` CLI + `tests/parity.py`
- [x] Redis client spike (`redis_ping`)

## Day 2 status: Person B

- [x] `Analysis`: duration estimates, bottom levels, critical path (with the path), plan shape (depth/width)
- [x] `SchedulingPolicy` interface with `FifoPolicy` and `CriticalPathPolicy`
- [x] `Scheduler`: ExecutionState + policy; dispatch/started/complete/retry/fail with in-flight tracking
- [x] `Simulator`: discrete-event, virtual time, N or unlimited workers, portable seeded jitter,
      permanent and one-shot (retry) failure injection; makespan, speedup, critical path, utilization, peak parallelism
- [x] `ats_sim` CLI (text timeline or `--json`), `make demo`, CLI exit-code tests
- [x] Loader fix: empty `created_at` rejected, no 200-character cap on `request_id` (contract #1); fixtures `invalid_empty_created_at` and `long_request_id`
- [x] 71 unit tests + 3 CLI tests. Property test: 1,800 simulations on random graphs checked against
      C <= makespan <= W, makespan = C (unlimited), = W (1 worker), and Graham's bound W/k + (1-1/k)C

## Day 3 status: Person A

- [x] Executors (`executors.py`): `SimExecutor` sleeps `estimated_seconds` × `--time-scale` and returns a canned output (an expand task returns a valid 2-task Subplan); `LLMExecutor` makes one Groq call per attempt with the dependency results in the prompt, time-boxed to the attempt's deadline, and expand tasks use a strict-mode subplan schema checked by `validate_subplan` (D20, D21)
- [x] Failure classification (8.6): rate limit, timeout, connection, 5xx and malformed model output are retryable (with `retry_after_ms` from `Retry-After`); bad request, auth and permission errors are permanent
- [x] Worker (`worker.py`, `python -m planner.worker --stream tasks:sim|tasks:llm`): consumer group `workers`, `task_started` → result → `task_completed`/`task_failed` on `task:events`, `SET NX` write-once results with `committed`, XACK last, XAUTOCLAIM takeover of entries idle > 30 s, deadline and invalid-message handling (D22, D23)
- [x] Development runner (`devrun.py`): dispatches a plan's tasks to the workers, retries retryable failures and missed deadlines with a new attempt, until done; for testing workers before atsd exists, not a replacement for it
- [x] 41 tests: executors with a fake Groq client, worker against a real Redis (exactly-once on duplicates and races, every failure path, takeover of a dead consumer's entry, `kill -9` of a worker process mid-task), end-to-end runs with retries; every published event is parsed with the contract #2 models
- [x] CI runs Redis 7 as a service for the planner job; `make worker-demo`
- [ ] Worker containers in `docker-compose.yml` (with atsd)
- [ ] Live Groq run of `tasks:llm` on the eval plans
