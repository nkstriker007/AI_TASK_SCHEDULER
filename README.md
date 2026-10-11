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
make test                       # C++ unit tests and CLI tests (ctest)
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

Redis 7 in Docker (needs Docker Desktop):

```bash
docker compose up -d --wait redis
docker compose exec redis redis-cli ping
docker compose down
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
docs/DECISIONS.md                decisions made during the build (D15, D17–D23)
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
