BUILD ?= scheduler/build

.PHONY: build test cpp-test py-test parity clean redis-deps redis-spike

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

redis-deps:
	./scripts/install_redis_deps.sh

redis-spike:
	cmake -S scheduler -B scheduler/build-redis -DATS_WITH_REDIS=ON -DATS_BUILD_TESTS=OFF $(if $(wildcard deps),-DATS_DEPS_PREFIX=$(CURDIR)/deps,)
	cmake --build scheduler/build-redis -j
	./scheduler/build-redis/redis_ping

clean:
	rm -rf scheduler/build scheduler/build-*
