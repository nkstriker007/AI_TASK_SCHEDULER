cmake_minimum_required(VERSION 3.20)
project(ats LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

option(ATS_BUILD_TESTS "Build unit tests" ON)
option(ATS_WITH_REDIS "Build the Redis connectivity spike (needs hiredis + redis++)" OFF)

include(FetchContent)
FetchContent_Declare(nlohmann_json
  URL https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE)
FetchContent_MakeAvailable(nlohmann_json)

# ---- core library: no I/O beyond reading plan files, no threads, no clock ----
add_library(ats_core
  src/Types.cpp
  src/PlanLoader.cpp
  src/Dag.cpp
  src/ExecutionState.cpp
  src/Analysis.cpp
  src/Policy.cpp
  src/Scheduler.cpp
  src/Simulator.cpp)
target_include_directories(ats_core PUBLIC include)
target_link_libraries(ats_core PUBLIC nlohmann_json::nlohmann_json)
if(MSVC)
  target_compile_options(ats_core PRIVATE /W4)
else()
  target_compile_options(ats_core PRIVATE -Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wno-sign-conversion)
endif()

# ---- tools ----
add_executable(ats_validate tools/ats_validate.cpp)
target_link_libraries(ats_validate PRIVATE ats_core)
add_executable(ats_sim tools/ats_sim.cpp)
target_link_libraries(ats_sim PRIVATE ats_core)

if(ATS_WITH_REDIS)
  # macOS: brew install hiredis redis-plus-plus   Linux: scripts/install_redis_deps.sh
  set(ATS_DEPS_PREFIX "" CACHE PATH "Extra prefix where hiredis/redis++ are installed")
  find_path(REDISPP_INCLUDE sw/redis++/redis++.h HINTS ${ATS_DEPS_PREFIX}/include /opt/homebrew/include /usr/local/include REQUIRED)
  find_library(REDISPP_LIB NAMES redis++ HINTS ${ATS_DEPS_PREFIX}/lib /opt/homebrew/lib /usr/local/lib REQUIRED)
  find_library(HIREDIS_LIB NAMES hiredis HINTS ${ATS_DEPS_PREFIX}/lib /opt/homebrew/lib /usr/local/lib REQUIRED)
  find_package(Threads REQUIRED)
  add_executable(redis_ping tools/redis_ping.cpp)
  target_include_directories(redis_ping PRIVATE ${REDISPP_INCLUDE})
  target_link_libraries(redis_ping PRIVATE ${REDISPP_LIB} ${HIREDIS_LIB} Threads::Threads)
endif()

# ---- tests ----
if(ATS_BUILD_TESTS)
  FetchContent_Declare(googletest
    URL https://github.com/google/googletest/releases/download/v1.15.2/googletest-1.15.2.tar.gz
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE)
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)
  enable_testing()
  add_executable(ats_tests
    tests/test_smoke.cpp
    tests/test_loader.cpp
    tests/test_dag.cpp
    tests/test_state.cpp
    tests/test_fixtures.cpp
    tests/test_analysis.cpp
    tests/test_policy.cpp
    tests/test_scheduler.cpp
    tests/test_simulator.cpp)
  target_link_libraries(ats_tests PRIVATE ats_core GTest::gtest_main GTest::gmock)
  target_compile_definitions(ats_tests PRIVATE
    ATS_FIXTURES_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../examples/plans"
    ATS_EXPECTED_VALIDATION="${CMAKE_CURRENT_SOURCE_DIR}/../examples/expected_validation.json")
  include(GoogleTest)
  gtest_discover_tests(ats_tests)

  # End-to-end checks of the CLI: exit codes are part of its contract.
  set(PLANS ${CMAKE_CURRENT_SOURCE_DIR}/../examples/plans)
  add_test(NAME cli_sim_valid COMMAND ats_sim ${PLANS}/company_research.json --workers 2)
  set_tests_properties(cli_sim_valid PROPERTIES PASS_REGULAR_EXPRESSION "makespan 65.00s")
  add_test(NAME cli_sim_invalid_plan COMMAND ats_sim ${PLANS}/invalid_cycle.json)
  set_tests_properties(cli_sim_invalid_plan PROPERTIES WILL_FAIL TRUE)
  add_test(NAME cli_sim_injected_failure COMMAND ats_sim ${PLANS}/company_research.json --fail t2)
  set_tests_properties(cli_sim_injected_failure PROPERTIES WILL_FAIL TRUE)
endif()
