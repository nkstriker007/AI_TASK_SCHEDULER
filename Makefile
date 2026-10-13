BUILD ?= scheduler/build

.PHONY: build test cpp-test py-test parity demo up down clean redis-deps redis-spike

build:
	cmake -S scheduler -B $(BUILD) -DCMAKE_BUILD_TYPE=Debug
	cmake --build $(BUILD) -j

test: cpp-test py-test

cpp-test: build
	cd $(BUILD) && ctest --output-on-failure

# Planner, worker and API unit tests. Worker and API tests start a throwaway redis-server
# (or use ATS_TEST_REDIS_URL); they are skipped if neither is available.
py-test:
	cd planner && uv run pytest -q
	cd worker && uv run pytest -q
	cd api && uv run pytest -q
	uv run --project planner pytest -q tests/test_parity.py

parity: build
	python3 tests/parity.py

demo: build
	@echo "== company_research, FIFO, 2 workers =="
	./$(BUILD)/ats_sim examples/plans/company_research.json --workers 2
	@echo
	@echo "== same plan, unlimited workers (makespan = critical path) =="
	./$(BUILD)/ats_sim examples/plans/company_research.json --workers 0 | tail -3
	@echo
	@echo "== failure injected on t1: descendants cancelled, independent branch finishes =="
	-./$(BUILD)/ats_sim examples/plans/company_research.json --fail t1 | tail -3

# Redis + API + 3 sim workers in Docker (Day 3). Add --profile minisched to run the Python
# stand-in for atsd until the C++ service exists.
up:
	docker compose up -d --build --wait

down:
	docker compose --profile minisched down

redis-deps:
	./scripts/install_redis_deps.sh

redis-spike:
	cmake -S scheduler -B scheduler/build-redis -DATS_WITH_REDIS=ON -DATS_BUILD_TESTS=OFF $(if $(wildcard deps),-DATS_DEPS_PREFIX=$(CURDIR)/deps,)
	cmake --build scheduler/build-redis -j
	./scheduler/build-redis/redis_ping

clean:
	rm -rf scheduler/build scheduler/build-*
