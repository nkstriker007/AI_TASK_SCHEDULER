#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <vector>

#include "ats/ExecutionState.hpp"
#include "ats/Policy.hpp"

namespace ats {

struct Dispatch {
  NodeId node;
  int attempt;  // 1 for the first try, 2 for the first retry, ...
};

// Glues the pure state machine to a scheduling policy. Driven by events from a simulator
// (Day 2) or Redis (Day 3+); it never sleeps, reads a clock or does I/O itself.
//
//   start()          initially ready tasks enter the policy's ready set
//   dispatchNext()   policy picks the next task; it becomes QUEUED with a new attempt number
//   started(n)       worker began executing it               QUEUED -> RUNNING
//   complete(n)      newly ready children enter the ready set
//   retry(n)         task goes back into the ready set as RETRYING
//   fail(n)          task FAILED; returns the descendants that were CANCELLED
class Scheduler {
 public:
  Scheduler(const Dag& dag, std::unique_ptr<SchedulingPolicy> policy);

  void start();
  std::optional<Dispatch> dispatchNext();
  void started(NodeId n);
  std::vector<NodeId> complete(NodeId n);
  void retry(NodeId n);
  std::vector<NodeId> fail(NodeId n);

  bool hasReady() const { return !policy_->empty(); }
  std::size_t readyCount() const { return policy_->size(); }
  std::size_t inFlight() const { return inFlight_; }
  bool isDone() const { return state_.isDone(); }
  bool succeeded() const { return state_.succeeded(); }
  const ExecutionState& state() const { return state_; }
  const SchedulingPolicy& policy() const { return *policy_; }
  const Dag& dag() const { return *dag_; }

 private:
  void leaveFlight(NodeId n);

  const Dag* dag_;
  ExecutionState state_;
  std::unique_ptr<SchedulingPolicy> policy_;
  std::size_t inFlight_ = 0;  // QUEUED + RUNNING
};

}  // namespace ats
