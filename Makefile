BUILD ?= scheduler/build

.PHONY: build test cpp-test py-test parity demo e2e up down clean redis-deps redis-spike redis-up redis-down

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

parity: build
	uv run --project planner python tests/parity.py

demo: build
	@echo "== company_research, FIFO, 2 workers =="
	./$(BUILD)/ats_sim examples/plans/company_research.json --workers 2
	@echo
	@echo "== same plan, unlimited workers (makespan = critical path) =="
	./$(BUILD)/ats_sim examples/plans/company_research.json --workers 0 | tail -3
	@echo
	@echo "== failure injected on t1: descendants cancelled, independent branch finishes =="
	-./$(BUILD)/ats_sim examples/plans/company_research.json --fail t1 | tail -3

# The real scheduler end to end: atsd (needs hiredis) + worker/ workers on a throwaway Redis.
# Also part of py-test; skipped there if atsd was not built.
e2e: build
	cd worker && uv run pytest -q tests/test_atsd_e2e.py

# Redis + atsd + API + 3 sim workers (worker/) in Docker.
up:
	docker compose up -d --build --wait

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
