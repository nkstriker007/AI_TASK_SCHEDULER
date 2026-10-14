# Scheduler Day 2: what to add

One package with everything on the C++ side for Day 2, tested together on commit 5ebdf69:

1. **`ats_sim` simulator**: `Simulator.hpp/.cpp`, `tools/ats_sim.cpp`, `tests/test_sim.cpp`, plus the `ats_sim` target in CMake and `make demo`.
2. **Loader fix**: `PlanLoader.cpp` now rejects an empty `created_at` and no longer caps `request_id` at 200 characters, plus 3 loader tests, 2 shared fixtures and one sentence in `docs/contract.md`.

## How to add it

From the repo root, either:

```bash
git checkout -b day2-scheduler
git apply scheduler-day2.patch
```

or copy everything under `files/` into the repo, keeping the folder paths.

## Check before pushing

```bash
make test     # 66 C++ tests + 89 Python tests + test_parity.py (29 passed, 0 skipped)
make parity   # 14/14 OK
make demo
```

Results on Linux: GCC 13 with ASan + UBSan and `-Werror`, and Clang 18 with `-Werror`, both 66/66. Not tested on macOS.

## Files changed (12)

| File | Change |
| --- | --- |
| `scheduler/include/ats/Simulator.hpp` | new |
| `scheduler/src/Simulator.cpp` | new |
| `scheduler/tools/ats_sim.cpp` | new |
| `scheduler/tests/test_sim.cpp` | new, 15 tests |
| `scheduler/CMakeLists.txt` | `Simulator.cpp`, `ats_sim` target, `test_sim.cpp` |
| `Makefile` | `make demo` |
| `scheduler/src/PlanLoader.cpp` | envelope fix |
| `scheduler/tests/test_loader.cpp` | 3 tests |
| `examples/plans/invalid_empty_created_at.json` | new fixture |
| `examples/plans/long_request_id.json` | new fixture |
| `examples/expected_validation.json` | lists the 2 fixtures |
| `docs/contract.md` | "no maximum length" |

Contract #1 says changes to it, and to the other person's code, need the other person's review: open this as a pull request rather than pushing to master.

## README: add under "Day 1 status: Person B"

```markdown
## Day 2 status: Person B

- [x] `Simulator`: simulated clock and N workers driving `ExecutionState` (no Redis, threads or wall clock); a dispatched attempt finishes after `estimated_seconds` (default 5 s)
- [x] Scheduling policies: FIFO and critical path (upward rank over `estimatedSeconds`, HEFT priority), ties broken by plan order
- [x] Retries: `maxAttempts` (default 3) with exponential backoff in RETRYING; the worker is freed during the wait; exhausted retries fail the task and cancel its descendants
- [x] Failure injection: deterministic `--fail ID:N` and seeded `--fail-rate P`
- [x] `ats_sim` CLI per contract #1 (exit 0 / 1 / 2, `--json`); `tests/test_parity.py` now runs against it
- [x] Loader fix: empty `created_at` rejected, no cap on `request_id` (contract #1), with fixtures `invalid_empty_created_at` and `long_request_id`
- [x] 15 tests incl. a 300-round randomized property test (worker limit, dependency order, every task terminal); GCC ASan/UBSan and Clang `-Werror` clean
- [x] `make demo`
- [ ] Message contract #2 review with Person A (`docs/messages.md`)
- [ ] Splicing `hints.sim_subplan` for `expand` tasks (atsd, see D21)
```

Also tick Person A's last item ("`make demo` once `ats_sim` exists") once you've both run it.

## docs/DECISIONS.md: add these rows

```markdown
| D20 | **Simulator time and policy.** `ats_sim` uses a simulated clock: an attempt starts when dispatched and finishes `estimated_seconds` later (default 5 s, `--default-seconds`). The default policy is critical path (upward rank = own duration + max child rank); FIFO is kept for comparison. Ties go to plan order. | Runs are instant and deterministic, so tests can assert exact makespans. Critical path is the policy the Redis scheduler will use; FIFO is the baseline for the benchmark. |
| D21 | **expand tasks are simulated as ordinary tasks on Day 2.** `hints.sim_subplan` is parsed and validated but not spliced into the graph. | Contract #1 puts splicing, renaming to `t2.1…` and the per-request limits in atsd. `Dag` is immutable, so splicing needs atsd's graph-growth logic, which doesn't exist yet. |
| D22 | **Retry rules in the simulator.** `maxAttempts` = 3 including the first; retry k waits `backoff * 2^(k-1)` seconds (default 1 s) in RETRYING with the worker released; every injected failure counts as retryable. `ats_sim` exits 64 on a usage error, since 2 already means "execution failed". | Mirrors the retry/backoff the Redis scheduler needs on Day 3, through the same `ExecutionState` transitions. `retryable` vs `permanent` (contract #2) arrives with real workers. |
```

The numbers D20 to D22 assume nobody has used them yet. Renumber if your partner has.
