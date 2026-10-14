BUILD ?= scheduler/build

.PHONY: build test cpp-test py-test parity demo clean redis-deps redis-spike redis-up redis-down

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
	@printf '== company_research, 4 workers, critical-path ==\n'
	./$(BUILD)/ats_sim examples/plans/company_research.json
	@printf '\n== same plan on 1 worker: no parallelism ==\n'
	@./$(BUILD)/ats_sim examples/plans/company_research.json --workers 1 | tail -1
	@printf '\n== t3 fails once, then succeeds on retry ==\n'
	@./$(BUILD)/ats_sim examples/plans/company_research.json --fail t3:1 | tail -1
	@printf '\n== t1 fails every attempt: its descendants are cancelled (exit 2) ==\n'
	-@./$(BUILD)/ats_sim examples/plans/company_research.json --fail t1:9 | tail -8
	@printf '\n== invalid plan is rejected (exit 1) ==\n'
	-@./$(BUILD)/ats_sim examples/plans/invalid_cycle.json

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
