#pragma once

#include <cstddef>
#include <cstdint>
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
//
// Every time a task enters the ready set it gets a ticket from `tickets`, a counter that
// several Schedulers may share (atsd shares one across all requests, so tickets order ready
// tasks globally: the cross-request FIFO policy, D41). Without one, the Scheduler keeps its own.
class Scheduler {
 public:
  Scheduler(const Dag& dag, std::unique_ptr<SchedulingPolicy> policy,
            std::shared_ptr<std::uint64_t> tickets = nullptr);

  void start();
  std::optional<Dispatch> dispatchNext();
  // The task dispatchNext() would pick, without dispatching it. Drops entries that are no
  // longer dispatchable (e.g. cancelled), so it is not const.
  std::optional<NodeId> peekNext();
  std::uint64_t readyTicket(NodeId n) const { return tickets_.at(n); }
  void started(NodeId n);
  std::vector<NodeId> complete(NodeId n);
  void retry(NodeId n);
  std::vector<NodeId> fail(NodeId n);
  // After Dag::expand(e) on this scheduler's graph (e QUEUED or RUNNING): grow the state and
  // re-rank the ready set with `priorities` (one per task). Then complete(e) releases the subplan.
  void onExpanded(NodeId e, std::vector<double> priorities);

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
  void push(NodeId n);
  bool dispatchable(NodeId n) const;

  const Dag* dag_;
  ExecutionState state_;
  std::unique_ptr<SchedulingPolicy> policy_;
  std::size_t inFlight_ = 0;  // QUEUED + RUNNING
  std::shared_ptr<std::uint64_t> counter_;
  std::vector<std::uint64_t> tickets_;  // per task: ticket of its latest entry into the ready set
};

}  // namespace ats
