#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <vector>

#include "ats/ExecutionState.hpp"
#include "ats/Policy.hpp"

namespace ats {

struct Dispatch {
  NodeId task;
  int attempt;  // 1 for the first try, 2 for the first retry, ...
};

// Glues the pure state machine to a scheduling policy. Driven by events from a simulator
// (Day 2) or Redis (Day 3+); it never sleeps, reads a clock or does I/O itself.
//
//   start()               initially ready tasks enter the policy's ready set
//   dispatch_next()       policy picks the next task; it becomes QUEUED with a new attempt number
//   mark_started(task)    worker began executing it                    QUEUED -> RUNNING
//   mark_completed(task)  newly ready children enter the ready set
//   mark_for_retry(task)  task goes back into the ready set as RETRYING
//   mark_failed(task)     task FAILED; returns the descendants that were CANCELLED
class Scheduler {
 public:
  Scheduler(const Dag& dag, std::unique_ptr<SchedulingPolicy> policy);

  void start();
  std::optional<Dispatch> dispatch_next();
  void mark_started(NodeId task);
  std::vector<NodeId> mark_completed(NodeId task);
  void mark_for_retry(NodeId task);
  std::vector<NodeId> mark_failed(NodeId task);

  bool has_ready_tasks() const { return !policy_->empty(); }
  std::size_t ready_count() const { return policy_->size(); }
  std::size_t in_flight_count() const { return in_flight_count_; }
  bool is_done() const { return execution_state_.isDone(); }
  bool succeeded() const { return execution_state_.succeeded(); }
  const ExecutionState& execution_state() const { return execution_state_; }
  const SchedulingPolicy& policy() const { return *policy_; }
  const Dag& dag() const { return *dag_; }

 private:
  void remove_from_flight(NodeId task);

  const Dag* dag_;
  ExecutionState execution_state_;
  std::unique_ptr<SchedulingPolicy> policy_;
  std::size_t in_flight_count_ = 0;  // tasks that are QUEUED or RUNNING
};

}  // namespace ats
