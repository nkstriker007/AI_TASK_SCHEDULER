BUILD ?= scheduler/build

.PHONY: build test cpp-test py-test parity demo demo-llm e2e chaos bench bench-dist bench-e5 live-bench up up-llm down clean redis-deps redis-spike redis-up redis-down

build:
	cmake -S scheduler -B $(BUILD) -DCMAKE_BUILD_TYPE=Debug
	cmake --build $(BUILD) -j

test: cpp-test py-test

cpp-test: build
	cd $(BUILD) && ctest --output-on-failure

# Planner, worker/ (incl. atsd end to end), api/ and parity tests. Tests that need Redis start a
# throwaway redis-server (or use ATS_TEST_REDIS_URL); they are skipped if neither is available.
py-test:
	cd planner && uv run pytest
	cd worker && uv run pytest
	cd api && uv run pytest
	uv run --project planner pytest tests/test_parity.py
	cd bench && uv run pytest

parity: build
	uv run --project planner python tests/parity.py

# Day 2 gate: request -> plan -> simulated schedule.
#   make demo                                  offline: the fake planner returns the company_research plan
#   make demo LIVE=1                           real Groq planner (needs GROQ_API_KEY in .env)
#   make demo LIVE=1 REQUEST="Compare AMD and Intel and write a report"
REQUEST ?= Research Apple and NVIDIA, summarize the key developments for each, compare them and write a short report
DEMO_PLAN := $(CURDIR)/$(BUILD)/demo_plan.json

demo: build
	@echo "== 1. request: $(REQUEST)"
	@echo "== 2. plan ($(if $(LIVE),Groq planner,offline fake planner; LIVE=1 for Groq)) -> $(DEMO_PLAN)"
	cd planner && uv run $(if $(LIVE),--env-file ../.env,) python -m planner.cli "$(REQUEST)" -o $(DEMO_PLAN) $(if $(LIVE),,--fake company_research)
	@echo
	@echo "== 3. simulated schedule: FIFO, 2 workers =="
	./$(BUILD)/ats_sim $(DEMO_PLAN) --workers 2
	@echo
	@echo "== same plan, unlimited workers (makespan = critical path) =="
	./$(BUILD)/ats_sim $(DEMO_PLAN) --workers 0 | tail -3
	@echo
	@echo "== failure injected on t1: descendants cancelled, independent branches finish =="
	-./$(BUILD)/ats_sim $(DEMO_PLAN) --fail t1 | tail -3

# The real scheduler end to end: atsd (needs hiredis) + worker/ workers on a throwaway Redis.
# Also part of py-test; skipped there if atsd was not built.
e2e: build
	cd worker && uv run pytest -q tests/test_atsd_e2e.py

# Redis + atsd + API + 3 sim workers (worker/) in Docker.
up:
	docker compose up -d --build --wait

# Chaos (design doc 10.4, E6): sim stack with injected faults (20% errors, 5% simulated 429s,
# 5% hangs), short attempt deadlines, then scripts/chaos.py submits plans and kills worker
# containers while they run. Passes when every request succeeds with zero duplicate commits.
CHAOS_ENV = SIM_TIME_SCALE=0.2 SIM_FAULT_ERROR_RATE=0.2 SIM_FAULT_RATE_LIMIT_RATE=0.05 SIM_FAULT_HANG_RATE=0.05 \
	ATSD_SIM_TIMEOUT_MS=15000 ATSD_MAX_ATTEMPTS=8
chaos:
	$(CHAOS_ENV) docker compose up -d --build --wait
	cd worker && uv run python ../scripts/chaos.py --requests 6 --kills 3 --kill-every 4

# Same stack with real Groq tasks: atsd dispatches to tasks:llm, workers run the LLM executors.
# Defaults fit Groq's on_demand tier (8,000 tokens/minute for gpt-oss-120b): at most 2 calls in
# flight, and 6 attempts so a rate-limited task can wait out a full minute (D38). Override for a
# bigger tier, e.g. make up-llm LLM_INFLIGHT=8 LLM_ATTEMPTS=3
LLM_INFLIGHT ?= 2
LLM_ATTEMPTS ?= 6
up-llm:
	ATSD_CAPABILITY=llm WORKER_CAPABILITIES=llm WORKER_CONCURRENCY=2 \
	ATSD_LLM_MAX_INFLIGHT=$(LLM_INFLIGHT) ATSD_MAX_ATTEMPTS=$(LLM_ATTEMPTS) docker compose up -d --build --wait

# Benchmarks (design doc 11). make bench: workloads -> ats_sim sweep -> sim plots, in seconds,
# no Redis needed. make bench-dist: E1-E3 with the real atsd, a throwaway redis-server and
# in-process sim workers (about a minute), then the plots again with the measured results.
# Output: bench/results/ (CSVs, PNGs, summary.md).
bench: build
	cd bench && uv run python -m bench.gen && uv run python -m bench.sim && uv run python -m bench.analyze

bench-dist: bench
	cd bench && uv run python -m bench.run $(if $(REPEATS),--repeats $(REPEATS),) && uv run python -m bench.analyze

# E5: speculative execution on a straggler fan-out (real atsd + Redis + sim workers, ~3 min for
# 40 paired seeds), plus the p50/p95/p99 latency profile. make bench-e5 REPEATS=10 for a quick run.
bench-e5: build
	cd bench && uv run python -m bench.e5 $(if $(REPEATS),--repeats $(REPEATS),) && uv run python -m bench.analyze

# E7 and E8 on real Groq calls against the running LLM stack (make up-llm first). Slow on the 8k
# tokens/min tier: a cooldown before every mode keeps one mode's 429s out of the next (~30 min).
live-bench:
	cd bench && uv run --env-file ../.env python -m bench.e7 && uv run --env-file ../.env python -m bench.e8 && uv run python -m bench.analyze

# Live demo on the running stack (make up-llm first): plan, task states updating, final report.
#   make demo-llm REQUEST="Research the top 3 AI chip companies and write a short report comparing them"
demo-llm:
	python3 scripts/demo.py $(if $(REQUEST),"$(REQUEST)",)

down:
	docker compose --profile minisched down

redis-up:
	docker compose up -d --wait redis

redis-down:
	docker compose --profile minisched down

redis-deps:
	./scripts/install_redis_deps.sh

redis-spike:
	cmake -S scheduler -B scheduler/build-redis -DATS_WITH_REDIS=ON -DATS_BUILD_TESTS=OFF $(if $(wildcard deps),-DATS_DEPS_PREFIX=$(CURDIR)/deps,)
	cmake --build scheduler/build-redis -j
	./scheduler/build-redis/redis_ping

clean:
	rm -rf scheduler/build scheduler/build-*
