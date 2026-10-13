# AI Task Scheduler

A natural-language request is turned into a validated dependency graph of tasks by an LLM planner (Python).
A C++ runtime then schedules and executes it in parallel across workers over Redis Streams.
The design is in `docs/` (architecture document). The two cross-language contracts are `docs/contract.md`
(plan files) and `docs/messages.md` (task messages and events), and build-time decisions are in `docs/DECISIONS.md`.

## Quick start (macOS)

Tools:

```bash
xcode-select --install          # Apple clang, if not already installed
brew install cmake
brew install uv                 # Python toolchain for the planner
```

C++ runtime:

```bash
make build                      # configure + build scheduler/ into scheduler/build
make test                       # C++ tests (ctest) + planner, worker, API and parity tests (Python)
make cpp-test                   # C++ only
make py-test                    # Python only (worker/API tests start a throwaway redis-server)
make parity                     # C++ and Python validators vs examples/expected_validation.json
make demo                       # simulate examples/plans/company_research.json: 2 workers, unlimited workers, injected failure
./scheduler/build/ats_validate examples/plans/invalid_cycle.json
```

Simulator (`ats_sim`) on its own:

```bash
./scheduler/build/ats_sim examples/plans/company_research.json --workers 2
./scheduler/build/ats_sim examples/plans/company_research.json --workers 0                 # unlimited: makespan = critical path
./scheduler/build/ats_sim examples/plans/company_research.json --policy critical-path
./scheduler/build/ats_sim examples/plans/company_research.json --fail t2                   # permanent failure: descendants cancelled
./scheduler/build/ats_sim examples/plans/company_research.json --flaky t3                  # fails on attempt 1, retried
./scheduler/build/ats_sim examples/plans/company_research.json --jitter 0.2 --seed 7 --json
```

Exit codes: 0 every task completed, 1 invalid plan or arguments, 2 execution failed.

Python planner on its own:

```bash
cd planner
uv sync                         # create .venv with pydantic, groq, pyyaml, pytest
uv run pytest                   # schema, validator, messages, eval, FakePlanner, Groq planner (fake client), CLI
uv run python -m planner.cli "Compare Apple and NVIDIA" --fake company_research   # offline, no API key
uv run python -m planner.check ../examples/plans/*.json                            # validate fixtures
uv run python -m planner.export_schemas                                            # regenerate schemas/
cd .. && uv run --project planner pytest tests/test_parity.py                      # Python vs C++ (ats_sim) parity
```

If you move or copy the repo folder, delete and recreate the virtual environment
(`rm -rf planner/.venv && cd planner && uv sync`). Its scripts store absolute paths, so `uv run pytest`
otherwise runs the old location's Python and fails with `No module named 'planner'`.

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

Request → plan → simulated schedule (the Day 2 gate), end to end:

```bash
cd planner && uv run --env-file ../.env python -m planner.cli "Research Apple and NVIDIA, compare them, write a report" -o ../plan.json
cd .. && ./scheduler/build/ats_sim plan.json --workers 2
```

Planner evaluation (5 structural cases in `examples/requests/eval_cases.yaml`, one Groq call each plus any repair):

```bash
cd planner && uv run --env-file ../.env python -m planner.eval
uv run python -m planner.eval --fake company_research   # offline plumbing check
```

Distributed runtime in Docker (needs Docker Desktop; Day 3):

```bash
make up                                   # Redis + API on :8000 + 3 sim workers
docker compose --profile minisched up -d  # + Python stand-in for atsd (testing only, until atsd exists)
SIM_TIME_SCALE=0.1 make up                # sim tasks run 10x faster (65 s plan -> 6.5 s)
docker compose up -d --scale worker=5     # more workers
make down
```

Submit work and watch it:

```bash
# hand-written plan (no LLM), e.g. a fixture's "plan" object
python3 -c "import json;print(json.dumps(json.load(open('examples/plans/company_research.json'))['plan']))" \
  | curl -s -X POST localhost:8000/plans -H 'content-type: application/json' -d @-
# typed request, planned by Groq (needs GROQ_API_KEY in .env; ATS_PLANNER=fake make up for offline)
curl -s -X POST localhost:8000/requests -H 'content-type: application/json' -d '{"text": "Research Apple and NVIDIA and compare them"}'
curl -s localhost:8000/requests/<request_id>          # status, per-task states, final output
```

`POST` returns 202 with the `request_id`, 422 with the validator's issues if the plan is rejected,
and 502 if the LLM provider fails. `GET` shows `SUBMITTED` until the scheduler picks the request up.

Worker and API without Docker (needs a Redis on localhost:6379, e.g. `docker compose up -d redis`):

```bash
cd worker && uv sync && uv run python -m worker                       # one worker; env: WORKER_CAPABILITIES, WORKER_CONCURRENCY, SIM_TIME_SCALE
cd worker && uv run python -m worker.minisched ../examples/plans/diamond.json   # run one plan through the workers, print a timeline
cd api && uv sync && uv run uvicorn api.app:app --port 8000
```

Redis spike (Day 1 evening):

```bash
make redis-deps                 # brew install redis hiredis redis-plus-plus
brew services start redis
make redis-spike                # builds tools/redis_ping.cpp (PING, XADD, XRANGE) into scheduler/build-redis
```

## Layout

```
docs/contract.md                 contract #1: plan file format, issue codes, parity rules
docs/messages.md                 contract #2 (frozen): task messages and events on Redis Streams
docs/DECISIONS.md                decisions made during the build (D15, D17–D26)
schemas/                         JSON Schema generated from planner/planner/schema.py and messages.py (do not edit by hand)
examples/plans/                  shared fixtures (plan files with the envelope)
examples/messages/               JSON examples of every task message and event (contract #2)
examples/requests/eval_cases.yaml  planner evaluation cases
examples/expected_validation.json  expected valid/errors/warnings per fixture (read by the C++ tests, the Python tests and tests/parity.py)
planner/                         Python planner (Person A)
  planner/schema.py              Pydantic models: ExecutionPlan, PlanFile, Subplan (source of truth)
  planner/messages.py            Pydantic models: TaskMessage and task events (contract #2)
  planner/validate.py            schema + semantic validation, all issue codes and warnings
  planner/llm.py                 Planner base class with the repair loop, GroqPlanner (strict structured output)
  planner/fake.py                FakePlanner: fixture plans, scripted outputs for tests
  planner/eval.py                planner evaluation: width, depth, sinks, independence
  planner/cli.py                 request -> validated plan file
  planner/check.py               validate plan files (used for parity)
  planner/export_schemas.py      writes / checks schemas/
  planner/prompts/               planner system prompt
  tests/
scheduler/                       C++20 runtime (Person B)
  include/ats/                   Types, PlanLoader, Dag, ExecutionState (Day 1);
                                 Analysis, Policy, Scheduler, Simulator (Day 2)
  src/
  tools/                         ats_validate (parity CLI), ats_sim (simulator CLI), redis_ping (spike)
  tests/                         GoogleTest: loader, dag, state machine, fixtures, property test (Day 1);
                                 analysis, policies, scheduler, simulator invariants (Day 2); ats_sim CLI tests
tests/parity.py                  C++ and Python validators vs expected_validation.json (make parity)
tests/test_parity.py             Python validator vs C++ (ats_sim), schema freshness
worker/                          Python worker (Person A, Day 3)
  worker/main.py                 consumer loop: XREADGROUP, task_started, run, SET NX commit, task_completed/failed, XACK, heartbeat
  worker/registry.py             (capability, task type) -> executor
  worker/results.py              load dependency results, commit this task's result once
  worker/executors/              Executor interface + error classes; sim executor (LLM executors: Day 4)
  worker/minisched.py            Python stand-in for atsd, for testing only (D26)
  Dockerfile, tests/
api/                             FastAPI service (Person A, Day 3)
  api/app.py                     POST /requests, POST /plans, GET /requests/{id}, GET /healthz
  api/status.py                  merges plan:<id>, request:<id> and the sink's result
  Dockerfile, tests/
docker-compose.yml               redis, api, worker (3 replicas), minisched (profile)
```

## Day 1 status: Person A

- [x] Pydantic schema (`schema.py`): `ExecutionPlan`, `PlanFile` envelope, `expand` task type, `Subplan`/`SubplanTask` (local IDs `s1…`, no nested expand, ≤ 20 tasks), `hints.expected_fanout` and fixture-only `hints.sim_subplan`
- [x] JSON Schema export to `schemas/` with a staleness test
- [x] Validator (`validate.py`): every issue collected, shared issue codes, Pydantic errors mapped to `EMPTY_PLAN` / `TOO_MANY_TASKS` / `SCHEMA_VERSION`, `TRANSITIVE_EDGE` and `MULTIPLE_SINKS` warnings, subplan validation
- [x] `FakePlanner` returning fixture plans, with a script of raw outputs for testing the repair loop
- [x] Provider chosen (Groq, `openai/gpt-oss-120b`, D17) and research data source decided (LLM-only, D15)
- [x] `GroqPlanner` (`llm.py`): one strict structured-output call against a relaxed strict-mode schema, nulls normalized, then the full validator (D18)
- [x] Planner CLI with a runtime-assigned `request_id`, plus `planner.check` for parity
- [x] `expand_canned` fixture and `expected_validation.json`; `docs/contract.md` and `docs/DECISIONS.md`

## Day 1 status: Person B

- [x] CMake project (C++20, nlohmann/json and GoogleTest via FetchContent), CI on Ubuntu + macOS
- [x] `PlanLoader`: schema checks on every field, unknown fields rejected, lengths counted in characters, all issues collected; non-empty `request_id`/`created_at` with no upper length limit; `hints.expected_fanout` and `hints.sim_subplan` (with subplan checks) per contract #1
- [x] `Dag::build`: duplicate ids, unknown/self/duplicate dependencies, Kahn topological order, cycle reported as a path
- [x] `ExecutionState`: full state set (WAITING…RETRYING…CANCELLED), attempts, failure cancels descendants, illegal transitions throw
- [x] Randomized property test; verified with GCC, Clang (-Werror) and ASan/UBSan
- [x] `ats_validate` CLI + `tests/parity.py`
- [x] Redis client spike (`redis_ping`); needs `make redis-deps` first (redis-plus-plus is not installed by Homebrew's hiredis alone)

## Day 2 status: Person A

- [x] Repair loop: one retry with the validator's issues (the Groq call replays its previous answer), then reject
- [x] Message models (contract #2): `TaskMessage`, `task_started` / `task_completed` (incl. `result_kind: "subplan"`) / `task_failed`, exported to `schemas/`, examples in `examples/messages/`, `docs/messages.md`
- [x] `docker-compose.yml` with Redis 7 (append-only persistence, health check)
- [x] `eval_cases.yaml` + `planner.eval`; first Groq run: 5/5 cases pass, no repairs needed
- [x] Message contract (#2) signed off by Person A and Person B and frozen
- [x] Python validator made strict (no type conversion), matching the C++ loader
- [x] Request → live Groq plan → `ats_sim` works end to end (commands above)
- [ ] `make demo` covers the full request → plan → simulation path (today it simulates a fixture plan)

## Day 2 status: Person B

- [x] `SchedulingPolicy` interface (`push` / `pop`) with `FifoPolicy` and `CriticalPathPolicy` (largest bottom level first, ties in plan order), selected by name (D20)
- [x] `Scheduler`: glues `ExecutionState` to a policy; `dispatchNext()` returns the task and its attempt number, tracks tasks in flight; no clock, no I/O
- [x] `Analysis`: duration estimates (hint or default), bottom levels, critical path, plan shape (width, depth)
- [x] Discrete-event `Simulator`: virtual time, N or unlimited workers, seeded jitter, permanent (`--fail`) and flaky (`--flaky`) failure injection with retries (D21)
- [x] Metrics: makespan, sequential time, critical path, speedup and its upper bound, utilization, peak parallelism, retries (D22)
- [x] `ats_sim` CLI with text timeline and `--json` report; exit codes 0 / 1 / 2 (D23)
- [x] Invariant tests (design doc 10.2): one worker = sequential, unlimited workers = critical path, bounds on random graphs, determinism per seed, failure cancels only descendants
- [x] `make demo`; `tests/test_parity.py` runs against `ats_sim`
- [x] Message contract (#2) reviewed and signed off

Test counts at the end of Day 2: 81 C++ tests (ctest) and 95 Python tests, all passing; parity agrees on all 12 fixtures.

## Day 3 status: Person A

- [x] Worker (`worker/`): asyncio consumer group `workers` on `tasks:<capability>`, configurable concurrency, executor registry, sim executor (duration hint × `SIM_TIME_SCALE`), dependency inputs loaded by task id, `SET NX` commit (first commit wins), `task_started` / `task_completed` / `task_failed` events, acknowledge only after the event (D24), heartbeat to `worker:<id>` with a 10 s TTL, graceful stop on SIGTERM
- [x] Error classification: `RetryableError` (with `retry_after_ms`), `PermanentError`, unclassified → retryable; malformed messages reported as permanent failures
- [x] API (`api/`): `POST /plans`, `POST /requests` (Groq or fake planner, repair loop), `GET /requests/{id}` with per-task states, spliced tasks and the final output; runtime-assigned `request_id`
- [x] Request intake and status format proposed for atsd (`docs/messages.md`, D25), shared key names in `planner/planner/keys.py`
- [x] Dockerfiles for worker and API; `docker-compose.yml` with 3 worker replicas; `make up` / `make down`; CI job with a Redis service
- [x] Rehearsal of the Day 3 gate with the Python stand-in for atsd (D26): `company_research` submitted through the API ran on 3 worker containers in 6.54 s at `SIM_TIME_SCALE=0.1`, against 6.50 s from `ats_sim` (65 s × 0.1); every task started within 0.03 s of its simulated start time
- [ ] Day 3 gate with the real atsd (Person B): same plan, 3 workers, timeline matches `ats_sim` within overhead
