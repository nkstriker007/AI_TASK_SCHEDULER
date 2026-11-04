# AI Task Scheduler

A natural-language request is turned into a validated dependency graph of tasks by an LLM planner (Python).
A C++ runtime then schedules and executes it in parallel across workers over Redis Streams.
The design is in `docs/` (architecture document). The two cross-language contracts are `docs/contract.md`
(plan files) and `docs/messages.md` (task messages, events and result values), and build-time decisions are in `docs/DECISIONS.md`.

**Start here:** `docs/REPORT.md` (architecture diagram, decisions, results of E1–E8, limitations) and
`docs/DEMO.md` (the demo script). The fastest look at it running:

```bash
make build && make up-llm             # Docker: Redis + atsd + API + 3 Groq workers (GROQ_API_KEY in .env)
make demo-llm REQUEST="Research the top 3 AI chip companies and write a short report comparing them"
```

## Quick start (macOS)

Tools:

```bash
xcode-select --install          # Apple clang, if not already installed
brew install cmake
brew install uv                 # Python toolchain
brew install redis hiredis      # redis-server for the worker/API tests; hiredis to build atsd
```

Build and test everything:

```bash
make build                      # configure + build scheduler/ into scheduler/build (atsd too if hiredis is installed)
make test                       # C++ tests (ctest) + planner, worker, API and parity tests (Python)
make cpp-test                   # C++ only
make py-test                    # Python only
make e2e                        # atsd + workers end to end on a throwaway Redis
make chaos                      # Docker stack with injected faults; kills worker containers mid-run (needs Docker)
make parity                     # C++ and Python validators vs examples/expected_validation.json
make demo                       # request -> plan -> ats_sim (offline fake planner); LIVE=1 for Groq, REQUEST="..." for your own
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
uv run pytest                   # schema, validator, messages, eval, planner, CLI
uv run python -m planner.cli "Compare Apple and NVIDIA" --fake company_research   # offline, no API key
uv run python -m planner.check ../examples/plans/*.json                            # validate fixtures
uv run python -m planner.export_schemas                                            # regenerate schemas/
cd .. && uv run --project planner pytest tests/test_parity.py                      # Python vs C++ (ats_sim) parity
```

If you move or copy the repo folder, delete and recreate each virtual environment
(`rm -rf planner/.venv && cd planner && uv sync`, and the same for `worker/` and `api/`). Their scripts store
absolute paths, so `uv run pytest` otherwise runs the old location's Python and fails with `No module named ...`.

Live planning with Groq: create `.env` in the repo root (it is gitignored) with

```bash
GROQ_API_KEY=<your key>
ATS_PLANNER_MODEL=openai/gpt-oss-120b   # optional, this is the default
ATS_EXECUTOR_MODEL=openai/gpt-oss-120b  # optional, model for the LLM executor
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
make demo LIVE=1 REQUEST="Research AMD and Intel, compare them and write a report"   # one command
# or step by step:
cd planner && uv run --env-file ../.env python -m planner.cli "Research Apple and NVIDIA, compare them, write a report" -o ../plan.json
cd .. && ./scheduler/build/ats_sim plan.json --workers 2
```

Planner evaluation (5 structural cases in `examples/requests/eval_cases.yaml`, one Groq call each plus any repair):

```bash
cd planner && uv run --env-file ../.env python -m planner.eval
uv run python -m planner.eval --fake company_research   # offline plumbing check
```

## Distributed runtime (Day 3)

`atsd` (C++, `scheduler/tools/atsd.cpp`) is the scheduler service: it reads requests from the API,
dispatches tasks to the workers (`worker/`), handles their events, deadlines and retries, and writes
each request's status. It needs hiredis (`brew install hiredis`, or `apt install libhiredis-dev`);
`make build` builds it when hiredis is found.

In Docker (needs Docker Desktop):

```bash
make up                                   # Redis + atsd + API on :8000 + 3 sim workers
make up-llm                               # same, with real Groq tasks (GROQ_API_KEY in .env; D38 limits)
SIM_TIME_SCALE=0.1 make up                # sim tasks run 10x faster (65 s plan -> 6.5 s)
docker compose up -d --scale worker=5     # more workers
docker compose logs -f atsd worker
make down
```

atsd writes one JSON line per transition to `runs/<run_id>/events.jsonl`. `minisched`, the Python
stand-in used before atsd existed, is still available for testing workers on their own
(`docker compose --profile minisched up -d --scale atsd=0`); never run it alongside atsd.

Submit work and watch it:

```bash
# hand-written plan (no LLM), e.g. a fixture's "plan" object
python3 -c "import json;print(json.dumps(json.load(open('examples/plans/company_research.json'))['plan']))" \
  | curl -s -X POST localhost:8000/plans -H 'content-type: application/json' -d @-
# typed request, planned by Groq (needs GROQ_API_KEY in .env; ATS_PLANNER=fake make up for offline)
curl -s -X POST localhost:8000/requests -H 'content-type: application/json' -d '{"text": "Research Apple and NVIDIA and compare them"}'
curl -s localhost:8000/requests/<request_id>          # status, per-task states, token usage, final output
curl -s 'localhost:8000/requests/<request_id>?include_outputs=true'   # + every completed task's output
curl -s localhost:8000/requests/<request_id>/report   # the final report as Markdown (409 until it succeeds)
```

`POST` returns 202 with the `request_id`, 422 with the validator's issues if the plan is rejected,
and 502 if the LLM provider fails. `GET` shows `SUBMITTED` until atsd picks the request up, then
`RUNNING` and `SUCCEEDED`, `FAILED` or `REJECTED`. FastAPI's interactive docs are at http://localhost:8000/docs.

Without Docker (needs Redis on localhost:6379, e.g. `make redis-up`):

```bash
ATSD_CAPABILITY=sim ./scheduler/build/atsd                                   # the scheduler (env: see tools/atsd.cpp)
cd worker && uv sync && uv run python -m worker                              # one worker; env: WORKER_CAPABILITIES, WORKER_CONCURRENCY, SIM_TIME_SCALE
cd worker && uv run --env-file ../.env env WORKER_CAPABILITIES=llm python -m worker   # one Groq worker (tasks:llm)
cd api && uv sync && uv run uvicorn api.app:app --port 8000
```

atsd configuration (environment): `REDIS_URL`, `ATSD_CAPABILITY` (send every task to `sim` or `llm`;
default `llm`), `ATSD_POLICY` (`fair-share` | `critical-path` | `fifo`, D41), `ATSD_DRAIN_MS` (30000, D43),
`ATSD_JANITOR_INTERVAL_MS` (30000; 0 = off) and `ATSD_JANITOR_MIN_IDLE_MS` (60000) (D49), `ATSD_MAX_ATTEMPTS` (3),
`ATSD_LLM_MAX_INFLIGHT` (8), `ATSD_SIM_MAX_INFLIGHT` (0 = unlimited), `ATSD_LLM_TIMEOUT_MS` (120000),
`ATSD_SIM_TIMEOUT_MS` (300000), `ATSD_RUNS_DIR` (`runs`). Workers and the API use `REDIS_URL` too
(default `redis://localhost:6379/0`); the Groq worker uses `ATS_EXECUTOR_MODEL`. Tests that need Redis
start a throwaway `redis-server` if one is installed, use `ATS_TEST_REDIS_URL` if set (CI), and are
skipped otherwise. They flush that database, so never point `ATS_TEST_REDIS_URL` at a Redis holding
data you need.

Redis on its own:

```bash
make redis-up                   # docker compose up -d --wait redis
docker compose exec redis redis-cli ping
make redis-down
```

Redis spike (Day 1 evening):

```bash
make redis-deps                 # brew install redis hiredis redis-plus-plus
brew services start redis
make redis-spike                # builds and runs tools/redis_ping.cpp (PING, XADD, XRANGE)
```

## Layout

```
docs/contract.md                 contract #1: plan file format, issue codes, parity rules
docs/messages.md                 contract #2 (frozen): task messages, events and result values; request intake/status (proposed)
docs/REPORT.md                   architecture, decisions, results (E1–E8), limitations
docs/DEMO.md                     demo / video script
docs/DECISIONS.md                decisions made during the build (D15, D17–D56)
schemas/                         JSON Schema generated from planner/planner/schema.py and messages.py (do not edit by hand)
examples/plans/                  shared fixtures (plan files with the envelope)
examples/messages/               JSON examples of every task message and event (contract #2)
examples/requests/eval_cases.yaml  planner evaluation cases
examples/expected_validation.json  expected valid/errors/warnings per fixture (read by the C++ tests, the Python tests and tests/parity.py)
planner/                         Python planner and shared contracts (Person A)
  planner/schema.py              Pydantic models: ExecutionPlan, PlanFile, Subplan (source of truth)
  planner/messages.py            Pydantic models: TaskMessage, task events, result value helpers (contract #2)
  planner/keys.py                Redis key and stream names shared by API, workers and atsd
  planner/artifacts.py           artifact spill for results over 64 KB: pointer format, safe paths, read/write
  planner/validate.py            schema + semantic validation, all issue codes and warnings
  planner/llm.py                 Planner base class with the repair loop, GroqPlanner (strict structured output)
  planner/fake.py                FakePlanner: fixture plans, scripted outputs for tests
  planner/eval.py                planner evaluation: width, depth, sinks, independence
  planner/cli.py                 request -> validated plan file
  planner/check.py               validate plan files (used for parity)
  planner/export_schemas.py      writes / checks schemas/
  planner/prompts/               planner system prompt
  tests/
worker/                          Python worker (Person A, Day 3)
  worker/main.py                 consumer loop: XREADGROUP, task_started, run, SET NX commit, task_completed/failed, XACK, heartbeat
  worker/registry.py             (capability, task type) -> executor
  worker/results.py              load dependency results, commit once (spilling large results), token usage
  worker/redis_client.py         Redis connection that survives a Redis restart
  worker/faults.py               fault injection for sim tasks: error, 429, hang, crash, straggler (D46)
  worker/executors/              Executor interface + error classes; sim executor; Groq LLM executor (llm.py)
  worker/prompts/                executor prompts: common rules, one file per task type, output format, subplan (expand step 2)
  worker/minisched.py            Python stand-in for atsd, for testing only (D26)
  Dockerfile, tests/
api/                             FastAPI service (Person A, Day 3)
  api/app.py                     POST /requests, POST /plans, GET /requests/{id}, GET /healthz
  api/status.py                  merges plan:<id>, request:<id> and the sink's result
  Dockerfile, tests/
scheduler/                       C++20 runtime (Person B)
  include/ats/                   Types, PlanLoader, Dag, ExecutionState (Day 1);
                                 Analysis, Policy, Scheduler, Simulator (Day 2);
                                 Bus, Service (atsd logic), RedisBus (hiredis) (Day 3)
  src/
  tools/                         ats_validate (parity CLI), ats_sim (simulator CLI), atsd (scheduler service), redis_ping (spike)
  tests/                         GoogleTest: loader, dag, state machine, fixtures, property test (Day 1);
                                 analysis, policies, scheduler, simulator invariants (Day 2); ats_sim CLI tests;
                                 atsd service with a fake bus and clock (Day 3);
                                 policies, drain, resume (Day 4); test_redis_integration.cpp against a real Redis (Day 4)
  Dockerfile                     atsd image
tests/parity.py                  C++ and Python validators vs expected_validation.json (make parity)
tests/test_parity.py             Python validator vs C++ (ats_sim), schema freshness
bench/                           benchmarks, design doc 11 (Person A, Day 6)
  bench/gen.py                   workloads as plan files -> bench/workloads/
  bench/sim.py                   ats_sim sweep: workloads x workers x policies -> results/sim.csv
  bench/run.py                   E1-E3 with the real atsd + Redis + sim workers -> results/dist.csv
  bench/sim.py also runs E4      critical path vs FIFO on 100 random layered DAGs -> results/e4.csv
  bench/e5.py                    E5: speculation on a straggler fan-out, paired seeds; p50/p95/p99 latency profile
  bench/e7.py                    E7: real requests, distributed vs sequential agent vs one large call (Groq)
  bench/e8.py                    E8: open-ended requests, runtime expansion vs a static plan (Groq)
  bench/live.py                  shared by E7/E8: counting planner, stack API client, atsd event log, cost
  bench/analyze.py               plots and summary.md from the CSVs -> bench/results/
  tests/
scripts/demo.py                  live demo: request -> plan -> task states updating -> report (make demo-llm)
scripts/chaos.py                 chaos run against the Docker stack: submit, kill workers, check (make chaos)
docker-compose.yml               redis, atsd, api, worker (3 replicas), minisched (profile)
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
- [x] `PlanLoader`: schema checks on every field, unknown fields rejected, lengths counted in characters, all issues collected; `hints.expected_fanout` and `hints.sim_subplan` (with subplan checks) per contract #1
- [x] `Dag::build`: duplicate ids, unknown/self/duplicate dependencies, Kahn topological order, cycle reported as a path
- [x] `ExecutionState`: full state set (WAITING…RETRYING…CANCELLED), attempts, failure cancels descendants, illegal transitions throw
- [x] 48 tests incl. a randomized property test; verified with GCC, Clang (-Werror) and ASan/UBSan
- [x] `ats_validate` CLI + `tests/parity.py`
- [x] Redis client spike (`redis_ping`); needs `make redis-deps` first (redis-plus-plus is not installed by Homebrew's hiredis alone)

## Day 2 status: Person A

- [x] Repair loop: one retry with the validator's issues (the Groq call replays its previous answer), then reject
- [x] Message models (contract #2): `TaskMessage`, `task_started` / `task_completed` (incl. `result_kind: "subplan"`) / `task_failed`, exported to `schemas/`, examples in `examples/messages/`, `docs/messages.md`
- [x] `docker-compose.yml` with Redis 7 (append-only persistence, health check); `make redis-up` / `make redis-down`
- [x] `eval_cases.yaml` + `planner.eval`; first Groq run: 5/5 cases pass, no repairs needed
- [x] Message contract (#2) signed off by Person A and Person B and frozen
- [x] Python validator made strict (no type conversion), matching the C++ loader
- [x] Request → live Groq plan → `ats_sim` works end to end (commands above)
- [x] `make demo` covers the full request → plan → simulation path (offline by default, `LIVE=1` for Groq)

## Day 2 status: Person B

- [x] `Analysis`: duration estimates, bottom levels, critical path (with the path), plan shape (depth/width)
- [x] `SchedulingPolicy` interface with `FifoPolicy` and `CriticalPathPolicy` (largest bottom level first, ties in plan order), selected by name (D20)
- [x] `Scheduler`: ExecutionState + policy; dispatch/started/complete/retry/fail with in-flight tracking
- [x] `Simulator`: discrete-event, virtual time, N or unlimited workers, portable seeded jitter,
      permanent and one-shot (retry) failure injection (D21); makespan, speedup, critical path, utilization, peak parallelism (D22)
- [x] `ats_sim` CLI (text timeline or `--json`), `make demo`, CLI exit-code tests (D23)
- [x] Loader fix: empty `created_at` rejected, no 200-character cap on `request_id` (contract #1); fixtures `invalid_empty_created_at` and `long_request_id`
- [x] 71 unit tests + 3 CLI tests. Property test: 1,800 simulations on random graphs checked against
      C <= makespan <= W, makespan = C (unlimited), = W (1 worker), and Graham's bound W/k + (1-1/k)C
- [x] Message contract (#2) reviewed and signed off

## Day 3 status: Person A

- [x] Worker (`worker/`): asyncio consumer group `workers` on `tasks:<capability>`, configurable concurrency, executor registry, dependency inputs loaded by task id, `SET NX` commit (first commit wins), `task_started` / `task_completed` / `task_failed` events, acknowledge only after the event (D24), skip when the result already exists or the deadline has passed, heartbeat to `worker:<id>` with a 10 s TTL, graceful stop on SIGTERM
- [x] Executors: sim (duration hint × `SIM_TIME_SCALE`, canned 2-task subplan for expand, D28) and Groq LLM (`llm.py`: one async call per attempt, no SDK retries, time-boxed to the deadline, strict subplan schema for expand, D27)
- [x] Error classification: `RetryableError` (with `retry_after_ms`, e.g. 429 `Retry-After`), `PermanentError` (bad request, auth), unclassified → retryable; malformed messages reported as permanent failures
- [x] One worker implementation (D31): the `planner/` worker was merged into `worker/` and removed; result values are JSON everywhere (D30)
- [x] API (`api/`): `POST /plans`, `POST /requests` (Groq or fake planner, repair loop), `GET /requests/{id}` with per-task states, spliced tasks and the final output; runtime-assigned `request_id`
- [x] Request intake and status format (`docs/messages.md`, D25), approved by Person B; shared key names in `planner/planner/keys.py`
- [x] Dockerfiles for worker and API; `docker-compose.yml`; `make up` / `make down`; CI job with a Redis service
- [ ] Live Groq run of `tasks:llm` on the eval plans

## Day 3 status: Person B (atsd)

Written on Day 3 to the agreed contracts and Person B's Day 1–2 core, for Person B's review.

- [x] `Service` (`scheduler/src/Service.cpp`): ingests `requests:new` (re-validated with `PlanLoader` + `Dag::build`; invalid → `REJECTED`), one `Dag` + `Scheduler` per request, dispatch with per-capability in-flight caps, least-recently-served across requests, events applied idempotently (first completion of any attempt wins), attempt deadlines as the only lost-attempt detector (D10), retries with backoff and `retry_after_ms` up to 3 attempts, then FAILED + descendants CANCELLED (D33)
- [x] `request:<id>` written in the agreed format (D25); one JSON line per transition in `runs/<run_id>/events.jsonl` (8.12)
- [x] `RedisBus` over hiredis behind a `Bus` interface (D32); `atsd` binary configured by environment, clean stop on SIGTERM
- [x] 21 unit tests with a fake bus and a fake clock (design doc 10.3): backoff, `retry_after_ms`, max attempts, permanent failure, independent branch after a failure, deadline expiry, late committed result accepted once, late result during backoff, stale events, in-flight cap, rejection, duplicates, two concurrent requests, garbage events, acks, event log
- [x] 4 end-to-end tests with the real binary, real Redis and real workers (`worker/tests/test_atsd_e2e.py`), including a worker that dies holding a task: recovered by the deadline, one committed result per task
- [x] Docker image (`scheduler/Dockerfile`) and `atsd` service in `docker-compose.yml`; CI installs hiredis and builds atsd
- [x] Day 4: see "Day 4 status: Person B"
- [x] Day 5: janitor for dead pending entries (8.7, D49)

## Day 3 gate

- [x] **Passed.** `company_research` submitted through the API ran under atsd on 3 worker containers (`SIM_TIME_SCALE=0.1`) and SUCCEEDED. Every task started and finished within 0.03 s of `ats_sim --workers 3` × 0.1; makespan 6.53 s against 6.50 s (0.4 % overhead), work spread over all 3 workers.

## Day 4 status: Person A

- [x] LLM executors for all five types (and expand): one executor per type with its own prompt, Groq strict JSON output `{content, key_points}`, malformed output retryable, `reasoning_effort: low` for gpt-oss (D35, D38)
- [x] Input loading: dependency results by task id, artifact files resolved
- [x] Artifact spill: results over 64 KB written to `artifacts/<request>/<task>.<attempt>.json`, pointer in Redis, loser's file removed, paths confined to the directory (D36)
- [x] Token accounting: `usage:<request_id>` per request and per task, in `GET /requests/{id}` (D37)
- [x] Final output assembly in the API: artifacts resolved, several final tasks, `?include_outputs=true`, `GET /requests/{id}/report` as Markdown
- [x] `make up-llm`: the stack with real Groq tasks, with limits that fit the on_demand tier (D38)
- [x] Two problems found by the live gate and fixed: Groq tokens-per-minute 429s exhausted retries (in-flight cap, longer retry budget, low reasoning effort, and atsd pausing the whole LLM capability on `retry_after_ms`, D39); a stale Redis connection after a Redis restart made `POST /requests` return 500 (reconnecting clients, regression test, D40)

## Day 4 gate

- [x] **Passed.** Two typed requests submitted through `POST /requests` at the same time, planned by Groq (6 and 4 tasks), executed by Groq workers under atsd (`make up-llm`), both SUCCEEDED in 62 s and 20 s; `GET /requests/{id}/report` returned both reports. Rate-limited tasks recovered on attempts 2 and 3.
- [x] **Re-run after Person B's Day 4 (fair-share policy, drain, resume).** The first re-run failed one request: Groq's `retry-after` hints (2–4 s) were shorter than the full tokens-per-minute window, so 6 attempts were used up in under 40 s. With doubling LLM backoff (2, 4, 8, 16, 32 s, D45) both requests SUCCEEDED (79 s and 7 s), through 10 rate-limited retries (one task needed 5 attempts); the small request finished first under fair share.
- [x] atsd restarted mid-run in Docker (sim, `company_research`): it drained 2 attempts in flight, the next atsd resumed the request with 3 tasks already completed, and every task ran exactly once.

## Day 4 status: Person B

- [x] Cross-request policies in atsd (D41), `ATSD_POLICY`: `fair-share` (default: fewest attempts in flight first, critical path within a request), `critical-path` (largest bottom level across all requests), `fifo` (global ready order, the baseline). `SchedulingPolicy::peek()` and `priority()`, `Scheduler::peekNext()` and ready tickets shared across requests
- [x] A request waits only for its next task's capability, not every capability it uses (D42)
- [x] Clean shutdown (D43): SIGTERM stops intake and dispatch, waits up to `ATSD_DRAIN_MS` for attempts in flight to report, a second signal stops at once; 40 s `stop_grace_period` in `docker-compose.yml`
- [x] Restart and crash recovery (D44): a request's `requests:new` entry stays pending until it finishes; a new atsd resumes it, skips tasks whose result is committed, continues attempt numbers
- [x] 17 new unit tests (policies, peek, tickets, drain, resume); 5 C++ integration tests against a real Redis (`ats_redis_tests`: `RedisBus` round trip, two concurrent requests, crash and resume, drain); 3 new end-to-end tests with the real binary and workers (SIGTERM drain then resume with every task run once, `kill -9` then resume with one committed result per task, fair share vs FIFO under a cap of 2). GCC, Clang `-Werror`, ASan/UBSan clean
- [x] Person A: reviewed D41–D44, including the `requests:new` acknowledgement change: the API only `XADD`s to `requests:new` and never reads or acknowledges it, and minisched uses its own consumer, so nothing on Person A's side changes

Policies by hand (a cap makes the difference visible):

```bash
ATSD_CAPABILITY=sim ATSD_SIM_MAX_INFLIGHT=2 ATSD_POLICY=fifo ./scheduler/build/atsd      # or critical-path, fair-share
```

## Day 5 status: Person A

- [x] Fault injection (`worker/worker/faults.py`, D46): error, simulated 429 with `retry_after_ms`, hang, crash (the worker process exits mid-task), straggler, permanent; by rate or by rule (`SIM_FAULTS="t2@1=crash"`), deterministic per attempt; passed through `docker-compose.yml`
- [x] Graceful worker shutdown: the first SIGTERM finishes the attempts in progress (Day 3), a second one stops at once; attempts are stopped at their deadline so a hang cannot hold a worker (D47)
- [x] Error classification: injected and provider errors map to retryable (with `retry_after_ms`) or permanent; atsd pauses the capability on `retry_after_ms` (D39) and backs off 2–32 s (D45)
- [x] Chaos test (`worker/tests/test_chaos.py`, D48) with the real atsd: 4 requests through 20 % errors, 429s, hangs, stragglers, a crashed and a killed worker; all succeed, exactly one committed result per task, retries and lost attempts in atsd's log (passes 5 runs out of 5)
- [x] Chaos script (`scripts/chaos.py`, `make chaos`): Docker stack, `docker kill` of worker containers that are running a task, restart, report. Run: 6 requests, 3 workers killed, 20 retries and 4 lost attempts logged, all SUCCEEDED, 0 duplicate commits

## Day 5 status: Person B

- [x] Attempts, deadlines, retries with backoff, late-event handling and idempotent completion: in atsd since Day 3 (D10, D33), kept across restarts on Day 4 (D44)
- [x] Janitor for dead pending entries (D49, design doc 8.7): acknowledges `tasks:*` entries left pending by dead workers once their attempt is resolved; never retries; `janitor_acked` in the event log, a count in atsd's exit line
- [x] `Bus::pending` (`XPENDING ... IDLE`) and `Bus::entryData` (`XRANGE`) in `RedisBus`
- [x] 10 fake-clock unit tests (live attempt left alone until its deadline, failed / superseded / completed attempts, finished requests, requests from before a restart, unreadable and deleted entries, idle threshold, never changes state, interval, late completion still accepted), 2 integration tests against a real Redis, 1 end-to-end test (a worker dies holding t1: the deadline recovers it, the janitor acknowledges the dead entry, nothing left pending, one committed result per task)

## Day 5 gate

- [x] **Passed.** Real Groq run (`make up-llm`, `ATSD_LLM_TIMEOUT_MS=60000`): one typed request, and 4 s in `scripts/chaos.py` killed the worker container that was running t4 (not restarted). atsd's log: t4 attempt 1 `attempt_lost` at its deadline, `retry_scheduled`, attempt 2 on another worker; the request SUCCEEDED in 75 s with 0 duplicate commits and the report was served.

Open items:

- [ ] Person B: review the atsd changes made from Person A's side: D39 (capability pause on `retry_after_ms`) and D45 (doubling LLM backoff); notes in "Day 7 status: Person B"

## Day 6 status: Person A

- [x] Expand executor (D50): step 1 names the items, step 2 turns them into a `Subplan` (`worker/prompts/subplan.md`), checked with `validate_subplan` and an item-coverage check, repaired once, then a permanent failure. Live check: "top chip companies" → 5 independent research subtasks (NVIDIA, AMD, Intel, Qualcomm, Google)
- [x] Planner rule 2 and eval (D52): open-ended requests plan as expand → compare → report with no placeholder tasks; 3 open-ended eval cases, new assertions `require_types`, `expand_reaches_sink`, `expand_consumed_once`; eval 8/8 live
- [x] Benchmarks (D51): `bench/gen.py`, `sim.py`, `run.py`, `analyze.py`, tests in `bench/tests/` (part of `make py-test`)

```bash
make bench          # workloads -> ats_sim sweep -> bench/results/sim_scaling.png, summary.md (seconds, no Redis)
make bench-dist     # + E1-E3 with the real atsd, a throwaway redis-server and sim workers (~1 min)
```

First results (`bench/results/summary.md`, 1 repeat, 26 runs, all SUCCEEDED):

- E1, distributed vs simulator on 4 workers: +1.4 % (straggler-16) to +4.0 % (fanout-32)
- E2, speedup over a measured 1-worker run, tracks the simulator within 4 %: fanout-32 10.6x at 16 workers (sim 11.0x), layered-24 3.2x (bound 3.25x), chain-10 1.0x
- E3, 32 tasks + join on 8 workers: 3–6 ms overhead per step; efficiency 40 % at 2 ms tasks, 92 % at 50 ms, 99 % at 0.5 s; break-even about 3 ms
- Simulator: critical-path priority beats FIFO on straggler-16 (22 s vs 28 s on 4 workers) and layered-24 (18.4 s vs 19.5 s); identical on the others

## Day 6 status: Person B

- [x] Dynamic expansion in atsd (D53, design doc 8.11): the first committed expand result is read, validated (`parseSubplan` + `Dag::expand`) and spliced before the expand task completes: `t2.1…t2.n`, a synthetic `t2.join` that atsd completes itself, dependents rewired to it and given the spliced tasks' results as inputs; critical-path priorities recomputed; limits of 3 expansions and 100 tasks per request; an invalid subplan fails the expand task and cancels its dependents; re-spliced with the same ids after a restart
- [x] Graph growth in the core: `Dag::expand`, `ExecutionState::onExpanded`, `Scheduler::onExpanded`, `SchedulingPolicy::reprioritize`
- [x] `ats_sim` splices `hints.sim_subplan` (`expand_canned`: 8 tasks, makespan 60 s on unlimited workers along t2 -> t2.2 -> t2.join -> t3 -> t4); benchmark workloads unchanged (all 60 rows of `bench/results/sim.csv` reproduce exactly)
- [x] 26 new tests: 7 graph and subplan, 3 scheduler, 11 atsd with the fake bus (splice and inputs, duplicate and late results, invalid subplans, missing result, both limits, failed subtask, priorities, restart, janitor), 4 simulator, 1 end to end with the real atsd and sim workers
- [x] Stretch: speculative execution and overhead profiling (Day 7, see "Day 7 status: Person B")

```bash
./scheduler/build/ats_sim examples/plans/expand_canned.json --workers 0     # splice in the simulator
```

## Day 6 gate

- [x] `make bench` regenerates the sim plots
- [x] atsd splices expand results (D53): `expand_canned` runs end to end with the real atsd and sim workers (`worker/tests/test_atsd_e2e.py`)
- [x] **Passed.** Open-ended requests expand at runtime and complete on real Groq tasks (`make up-llm`, `POST /requests`), 3 out of 3:
  - "Research the top 3 AI chip companies and compare them in a short report": expand → NVIDIA, AMD, Intel spliced as `t1.1…t1.3` + `t1.join` → compare → report; SUCCEEDED in 44 s
  - "Find the largest cloud providers, research each, and write a comparison report": 5 providers spliced; SUCCEEDED in 109 s through Groq 429 retries (one task on attempt 5)
  - "Identify the currently trending databases and summarize each in a short report": 5 databases spliced; SUCCEEDED in 42 s
  - Each returned one final report. The first gate run found a planner placeholder ("Research each AI chip company" next to the compare task) that ran the research twice and left two final tasks; fixed by D54 (`EXPAND_FANOUT` + one repair). Live planner eval after the fix: 8/8 (one case used the repair)

## Day 7 status: Person B

- [x] Speculative execution in atsd (D56, design doc 8.8), `ATSD_SPECULATION=1`: one backup for a RUNNING attempt past 1.5x the p90 of its task type, only with spare capacity after normal dispatch; first committed result wins; a failed or lost primary hands over to the backup without a retry; backups outside the retry budget; attempt ids unique across restarts
- [x] E5 (`bench/e5.py`, `make bench-e5`): straggler fan-out on the real atsd, Redis and sim workers, 40 paired seeds. Median makespan 2.21 s -> 0.83 s (-63 %), p99 3.32 s -> 2.47 s (-26 %), p95 unchanged (2.22 s: a backup straggles too, one backup per task), median +6 % worker time; `bench/results/e5_tail.png`
- [x] Latency profile (overhead profiling) from atsd's event log and the workers' events: dispatch -> worker start p50 1 ms / p99 3 ms, worker completion -> atsd p50 1 ms / p99 3 ms, parent done -> child dispatched p50 2 ms / p99 3.2 ms (`summary.md`, `REPORT.md` E5)
- [x] 12 fake-clock unit tests (backup and first-result-wins, primary still wins, off by default, sample minimum, spare capacity only, primary failed / lost -> takeover, failed backup, retry budget and unique ids, permanent failure, drain, janitor), 1 end-to-end test (a forced straggler is backed up and the backup commits), 4 bench tests
- [ ] Review D39 and D45. Notes: both look correct as implemented; possible change, not made: cap `retry_after_ms` (e.g. at 60 s) so one bad hint cannot pause a whole capability for long

```bash
make bench-e5                                # E5 + latency profile (~3.5 min); REPEATS=10 for a quick run
ATSD_SPECULATION=1 ./scheduler/build/atsd    # speculation on (env as in tools/atsd.cpp)
```

## Day 7 status: Person A

- [x] E7, real requests (`bench/e7.py`, D55): 3 eval requests planned once, then run distributed, by a sequential agent and as one large call. On Groq's 8k tokens/min tier one large call is fastest (4–7 s) and cheapest; distributed beats the sequential agent when the rate limit allows parallel calls (two_companies 10.5 s vs 13.6 s) and both are dominated by 429 backoff when it does not (four_companies 44–50 s); the planner call is 31–52 % of latency on light requests
- [x] E8, open-ended requests (`bench/e8.py`, D55): runtime expansion vs a static plan with a guessed fan-out. The splice takes 1 ms and adds no planner calls (the expand task's 2 calls cost 1.8–4.7k tokens); with a requested count both modes cover it exactly; with no count expansion found 10 items against 5 guessed (2x tokens, and 3.5x latency through 429s); static plans added discovery tasks whose result cannot change the planned items
- [x] E4, critical path vs FIFO (`bench/sim.py`, 100 random layered DAGs, simulator): median 4.9–5.3 % shorter makespan with 2–3 workers, ties at 4 (the graph's width)
- [x] `docs/REPORT.md` (architecture diagram, decisions, E1–E8, limitations), `docs/DEMO.md` (video script), `scripts/demo.py` (`make demo-llm`), `make live-bench`
- [x] Fixes from the runs: expand subtask titles are item names, not sentences (the shared output format overrode the discovery prompt; expand now has `prompts/output_expand.md`); E7 closes its Groq clients
- [ ] Re-run one open-ended request live to confirm name-only titles (Groq's 200k tokens/day limit was reached on Day 7)
- [ ] Demo video (record with `docs/DEMO.md`)

```bash
make bench                                   # simulator sweep + E4 plots
make up-llm && make live-bench               # E7 + E8 on real Groq (~30 min on the 8k tokens/min tier)
make demo-llm REQUEST="Research the top 3 AI chip companies and write a short report comparing them"
```
