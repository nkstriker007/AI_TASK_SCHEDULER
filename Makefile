BUILD ?= scheduler/build

.PHONY: build test parity demo clean redis-deps redis-spike

build:
	cmake -S scheduler -B $(BUILD) -DCMAKE_BUILD_TYPE=Debug
	cmake --build $(BUILD) -j

test: build
	cd $(BUILD) && ctest --output-on-failure

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

redis-deps:
	./scripts/install_redis_deps.sh

redis-spike:
	cmake -S scheduler -B scheduler/build-redis -DATS_WITH_REDIS=ON -DATS_BUILD_TESTS=OFF $(if $(wildcard deps),-DATS_DEPS_PREFIX=$(CURDIR)/deps,)
	cmake --build scheduler/build-redis -j
	./scheduler/build-redis/redis_ping

clean:
	rm -rf scheduler/build scheduler/build-*
