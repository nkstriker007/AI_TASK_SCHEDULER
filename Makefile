BUILD ?= scheduler/build

.PHONY: build test cpp-test py-test parity demo worker-demo clean redis-deps redis-spike redis-up redis-down

build:
	cmake -S scheduler -B $(BUILD) -DCMAKE_BUILD_TYPE=Debug
	cmake --build $(BUILD) -j

test: cpp-test py-test

cpp-test: build
	cd $(BUILD) && ctest --output-on-failure

py-test:
	cd planner && uv run pytest
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

# Needs Redis on localhost:6379 (make redis-up). Three sim workers + the dev runner on one plan.
worker-demo:
	cd planner && pids=""; \
	for i in 1 2 3; do uv run python -m planner.worker --stream tasks:sim --worker-id w$$i --time-scale 0.1 & pids="$$pids $$!"; done; \
	uv run python -m planner.devrun ../examples/plans/company_research.json; status=$$?; \
	kill $$pids 2>/dev/null; exit $$status

redis-up:
	docker compose up -d --wait redis

redis-down:
	docker compose down

redis-deps:
	./scripts/install_redis_deps.sh

redis-spike:
	cmake -S scheduler -B scheduler/build-redis -DATS_WITH_REDIS=ON -DATS_BUILD_TESTS=OFF $(if $(wildcard deps),-DATS_DEPS_PREFIX=$(CURDIR)/deps,)
	cmake --build scheduler/build-redis -j
	./scheduler/build-redis/redis_ping

clean:
	rm -rf scheduler/build scheduler/build-*
