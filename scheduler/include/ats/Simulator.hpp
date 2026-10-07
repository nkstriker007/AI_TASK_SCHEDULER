#pragma once

#include <cstdint>
#include <string>
#include <unordered_set>
#include <vector>

#include "ats/Dag.hpp"
#include "ats/ExecutionState.hpp"

namespace ats {

struct SimConfig {
  int workers = 2;                    // 0 = unlimited
  std::string policy = "fifo";        // "fifo" | "critical-path"
  double defaultSeconds = 10.0;       // duration for tasks without an estimated_seconds hint
  double jitter = 0.0;                // each attempt runs for duration * U[1-jitter, 1+jitter]
  std::uint64_t seed = 42;
  std::unordered_set<std::string> failTaskIds;   // these tasks fail permanently
  std::unordered_set<std::string> flakyTaskIds;  // these tasks fail on attempt 1, then succeed
};

enum class SimEventKind { Start, Complete, Retry, Fail, Cancel };
const char* toString(SimEventKind k);

struct SimEvent {
  double time;
  SimEventKind kind;
  NodeId node;
  int worker;   // -1 for Cancel
  int attempt;  // 0 for Cancel
};

struct AttemptTiming {
  NodeId node;
  int worker;
  int attempt;
  double start;
  double end;
  SimEventKind outcome;  // Complete, Retry or Fail
};

struct SimReport {
  std::string policy;
  int workers = 0;                 // as configured (0 = unlimited)
  std::vector<SimEvent> events;    // in time order
  std::vector<AttemptTiming> attempts;
  double makespan = 0.0;           // time of the last event
  double sequentialSeconds = 0.0;  // sum of every task's first-attempt duration (1-worker baseline)
  double busySeconds = 0.0;        // worker time actually spent, including failed attempts
  double criticalPathSeconds = 0.0;
  std::vector<NodeId> criticalPath;
  double speedup = 0.0;            // sequentialSeconds / makespan (0 if the run failed)
  double maxSpeedup = 0.0;         // sequentialSeconds / criticalPathSeconds
  double utilization = 0.0;        // busySeconds / (workers x makespan)
  int peakParallelism = 0;
  std::size_t completed = 0, failed = 0, cancelled = 0, retries = 0;
  bool succeeded = false;
};

// Discrete-event simulation with virtual time: fully deterministic for a given config.
// Dispatch happens immediately after each batch of simultaneous events, so a task starts the
// moment its last dependency finishes and a worker is free; there is no waiting for whole levels.
SimReport simulate(const Dag& dag, const SimConfig& config);

}  // namespace ats
