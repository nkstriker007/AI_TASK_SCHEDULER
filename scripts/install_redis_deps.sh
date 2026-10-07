#!/usr/bin/env bash
# Installs hiredis + redis-plus-plus for the C++ scheduler (Day 1 evening spike).
# macOS: Homebrew packages. Linux: build from source into ./deps (FetchContent of both
# together does NOT work: redis++ cannot find <hiredis/hiredis.h>; install hiredis first).
set -euo pipefail
if [[ "$(uname)" == "Darwin" ]]; then
  brew install redis hiredis redis-plus-plus
  echo "Configure with: cmake -S scheduler -B scheduler/build-redis -DATS_WITH_REDIS=ON"
  exit 0
fi
PREFIX="$(pwd)/deps"
TMP="$(mktemp -d)"
git clone -q --depth 1 -b v1.2.0 https://github.com/redis/hiredis "$TMP/hiredis"
cmake -S "$TMP/hiredis" -B "$TMP/hb" -DCMAKE_INSTALL_PREFIX="$PREFIX" -DBUILD_SHARED_LIBS=OFF -DDISABLE_TESTS=ON >/dev/null
cmake --build "$TMP/hb" -j --target install >/dev/null
git clone -q --depth 1 -b 1.3.13 https://github.com/sewenew/redis-plus-plus "$TMP/redispp"
cmake -S "$TMP/redispp" -B "$TMP/rb" -DCMAKE_INSTALL_PREFIX="$PREFIX" -DCMAKE_PREFIX_PATH="$PREFIX" \
  -DREDIS_PLUS_PLUS_BUILD_TEST=OFF -DREDIS_PLUS_PLUS_BUILD_SHARED=OFF -DREDIS_PLUS_PLUS_CXX_STANDARD=17 >/dev/null
cmake --build "$TMP/rb" -j --target install >/dev/null
rm -rf "$TMP"
echo "Installed into $PREFIX"
echo "Configure with: cmake -S scheduler -B scheduler/build-redis -DATS_WITH_REDIS=ON -DATS_DEPS_PREFIX=$PREFIX"
