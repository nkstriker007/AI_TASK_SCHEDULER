# AI Task Scheduler

A natural-language request is turned into a validated dependency graph of tasks by an LLM planner (Python).
A C++ runtime then schedules and executes it in parallel across workers over Redis Streams.
The design is in `docs/` (architecture document) and the cross-language contract is in `docs/CONTRACT.md`.

## Quick start (macOS)

```bash
xcode-select --install          # Apple clang, if not already installed
brew install cmake
make test                       # configure, build, run the C++ unit tests
make parity                     # C++ validator vs examples/plans/expected.json (and Python once it exists)
./scheduler/build/ats_validate examples/plans/invalid_cycle.json
make demo                       # simulated parallel runs with timeline and metrics
./scheduler/build/ats_sim examples/plans/company_research.json --workers 2 --policy critical-path
```

`ats_sim` options: `--workers N` (0 = unlimited), `--policy fifo|critical-path`, `--default-seconds S`,
`--jitter J`, `--seed K`, `--fail t2,t5` (permanent failure), `--flaky t3` (fails once, then retried), `--json`.
Exit codes: 0 succeeded, 1 invalid plan or arguments, 2 execution failed.

Redis spike (Day 1 evening):

```bash
make redis-deps                 # brew install redis hiredis redis-plus-plus
brew services start redis
make redis-spike                # builds and runs tools/redis_ping.cpp (PING, XADD, XRANGE)
```

## Layout

```
examples/plans/      shared fixtures + expected.json (the contract both validators are tested against)
docs/CONTRACT.md     plan file format, issue codes, parity interface
scheduler/           C++20 runtime (Person B)
  include/ats/       Types, PlanLoader, Dag, ExecutionState, Analysis, Policy, Scheduler, Simulator
  src/
  tools/             ats_validate (parity CLI), ats_sim (simulator CLI), redis_ping (spike)
  tests/             GoogleTest: loader, dag, state machine, fixtures, analysis, policies,
                     scheduler, simulator (incl. scheduling-theory property tests)
planner/             Python planner (Person A)
tests/parity.py      cross-language parity check
```

## Day 1 status: Person B

- [x] CMake project (C++20, nlohmann/json and GoogleTest via FetchContent), CI on Ubuntu + macOS
- [x] `PlanLoader`: schema checks on every field, unknown fields rejected, lengths counted in characters, all issues collected
- [x] `Dag::build`: duplicate ids, unknown/self/duplicate dependencies, Kahn topological order, cycle reported as a path
- [x] `ExecutionState`: full state set (WAITING…RETRYING…CANCELLED), attempts, failure cancels descendants, illegal transitions throw
- [x] 44 tests incl. a randomized property test; verified with GCC, Clang (-Werror) and ASan/UBSan
- [x] `ats_validate` CLI + `tests/parity.py`
- [x] Redis client spike (`redis_ping`)

## Day 2 status: Person B

- [x] `Analysis`: duration estimates, bottom levels, critical path (with the path), plan shape (depth/width)
- [x] `SchedulingPolicy` interface with `FifoPolicy` and `CriticalPathPolicy`
- [x] `Scheduler`: ExecutionState + policy; dispatch/started/complete/retry/fail with in-flight tracking
- [x] `Simulator`: discrete-event, virtual time, N or unlimited workers, portable seeded jitter,
      permanent and one-shot (retry) failure injection; makespan, speedup, critical path, utilization, peak parallelism
- [x] `ats_sim` CLI (text timeline or `--json`), `make demo`, CLI exit-code tests
- [x] 71 unit tests + 3 CLI tests. Property test: 1,800 simulations on random graphs checked against
      C <= makespan <= W, makespan = C (unlimited), = W (1 worker), and Graham's bound W/k + (1-1/k)C
