#pragma once

#include <cstddef>
#include <initializer_list>
#include <string_view>
#include <vector>

#include "ats/Dag.hpp"

namespace ats {

enum class TaskState { Waiting, Ready, Queued, Running, Retrying, Completed, Failed, Cancelled };

std::string_view toString(TaskState s);
bool isTerminal(TaskState s);

// Runtime state for one request. A pure, event-driven state machine:
// no I/O, no clock, no threads and no policy (which task goes next, retry limits and
// backoff live in the Scheduler/service layer). The same object is driven by the simulator
// on Days 1-2 and by Redis events from Day 3.
//
// Transitions (anything else throws std::logic_error — it is a programming error):
//   start()            WAITING -> READY                  for every task with no dependencies
//   onDispatched(n)    READY | RETRYING -> QUEUED        increments the attempt counter
//   onStarted(n)       QUEUED -> RUNNING
//   onCompleted(n)     QUEUED | RUNNING -> COMPLETED     children whose last dependency this was -> READY
//   onRetry(n)         QUEUED | RUNNING -> RETRYING      the service decides whether retries remain
//   onFailed(n)        QUEUED | RUNNING | RETRYING -> FAILED; every unfinished descendant -> CANCELLED
//
// onCompleted accepts QUEUED because a worker's "started" event can be lost or arrive late.
class ExecutionState {
 public:
  explicit ExecutionState(const Dag& dag);

  std::vector<NodeId> start();
  int onDispatched(NodeId n);                  // returns the new attempt number (1-based)
  void onStarted(NodeId n);
  std::vector<NodeId> onCompleted(NodeId n);   // returns tasks that became READY, in plan order
  void onRetry(NodeId n);
  std::vector<NodeId> onFailed(NodeId n);      // returns tasks that became CANCELLED

  TaskState state(NodeId n) const { return states_.at(n); }
  std::size_t unresolved(NodeId n) const { return unresolved_.at(n); }
  int attempts(NodeId n) const { return attempts_.at(n); }
  std::size_t count(TaskState s) const;

  bool started() const { return started_; }
  bool isDone() const;      // nothing is WAITING, READY, QUEUED, RUNNING or RETRYING
  bool succeeded() const;   // every task COMPLETED

  const Dag& dag() const { return *dag_; }

 private:
  void require(NodeId n, std::initializer_list<TaskState> allowed, std::string_view event) const;

  const Dag* dag_;
  std::vector<TaskState> states_;
  std::vector<std::size_t> unresolved_;
  std::vector<int> attempts_;
  bool started_ = false;
};

}  // namespace ats
