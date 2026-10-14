#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "ats/Dag.hpp"
#include "ats/ExecutionState.hpp"

namespace ats {

// Which READY task a free worker takes next.
//   Fifo:         the task that became READY first (ties: plan order).
//   CriticalPath: the task with the longest chain of estimated work still behind it
//                 (upward rank, the HEFT priority). Ties: plan order.
enum class Policy { Fifo, CriticalPath };

std::optional<Policy> parsePolicy(std::string_view s);
std::string_view toString(Policy p);

// Upward rank of every task: its own duration plus the largest rank among its children.
// Computed once per plan over Dag::topologicalOrder() in reverse. Index = NodeId.
std::vector<double> criticalPathRank(const Dag& dag, double defaultSeconds);

struct SimOptions {
  int workers = 4;
  Policy policy = Policy::CriticalPath;
  int maxAttempts = 3;              // attempts per task, including the first
  double defaultSeconds = 5.0;      // duration when hints.estimated_seconds is absent
  double backoffSeconds = 1.0;      // retry k waits backoffSeconds * 2^(k-1) in RETRYING
  double failRate = 0.0;            // probability that any attempt fails (retryable)
  std::uint64_t seed = 1;           // for failRate; same seed => same run
  std::map<std::string, int> failFirst;  // task id -> its first N attempts fail (deterministic)
};

// One line of the run's log, in simulated time.
struct SimEvent {
  double t = 0;
  std::string taskId;
  std::string event;    // dispatched | completed | retrying | failed | cancelled
  int attempt = 0;      // 0 for cancelled
  int worker = -1;      // -1 when no worker is involved
};

struct SimTaskResult {
  std::string id;
  TaskState state = TaskState::Waiting;
  int attempts = 0;
  double start = -1;    // start of the last attempt, -1 if never dispatched
  double end = -1;      // end of the last attempt
  int worker = -1;      // worker of the last attempt
};

struct SimResult {
  bool succeeded = false;
  double makespan = 0;                // simulated seconds until nothing was left to do
  std::vector<SimTaskResult> tasks;   // plan order
  std::vector<SimEvent> events;       // time order
  std::size_t retries = 0;
};

// Days 1-2 runtime: drives ExecutionState with a simulated clock and N simulated workers,
// with no Redis, threads or real time. A dispatched attempt starts immediately and finishes
// after the task's estimated_seconds. Every state change goes through ExecutionState, so the
// same transitions are exercised that the Redis-driven scheduler will use from Day 3.
//
// expand tasks run as ordinary tasks of their own duration; splicing hints.sim_subplan into
// the graph is atsd's job (contract #1) and is not done here.
class Simulator {
 public:
  static SimResult run(const Dag& dag, const SimOptions& opts = {});
};

}  // namespace ats
