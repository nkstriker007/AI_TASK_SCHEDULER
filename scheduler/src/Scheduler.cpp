#include "ats/Scheduler.hpp"

#include <stdexcept>
#include <utility>

namespace ats {

Scheduler::Scheduler(const Dag& dag, std::unique_ptr<SchedulingPolicy> policy)
    : dag_(&dag), execution_state_(dag), policy_(std::move(policy)) {
  if (!policy_) throw std::invalid_argument("Scheduler needs a policy");
}

void Scheduler::start() {
  for (NodeId ready_task : execution_state_.start()) policy_->push(ready_task);
}

std::optional<Dispatch> Scheduler::dispatch_next() {
  while (auto next_task = policy_->pop()) {
    // Defensive: anything not dispatchable any more (e.g. cancelled) is dropped from the ready set.
    const TaskState current_state = execution_state_.state(*next_task);
    if (current_state != TaskState::Ready && current_state != TaskState::Retrying) continue;
    const int attempt_number = execution_state_.onDispatched(*next_task);
    ++in_flight_count_;
    return Dispatch{*next_task, attempt_number};
  }
  return std::nullopt;
}

void Scheduler::mark_started(NodeId task) { execution_state_.onStarted(task); }

void Scheduler::remove_from_flight(NodeId task) {
  if (in_flight_count_ == 0) throw std::logic_error("in-flight count underflow at task " + dag_->id(task));
  --in_flight_count_;
}

std::vector<NodeId> Scheduler::mark_completed(NodeId task) {
  std::vector<NodeId> newly_ready_tasks = execution_state_.onCompleted(task);
  remove_from_flight(task);
  for (NodeId child_task : newly_ready_tasks) policy_->push(child_task);
  return newly_ready_tasks;
}

void Scheduler::mark_for_retry(NodeId task) {
  execution_state_.onRetry(task);
  remove_from_flight(task);
  policy_->push(task);
}

std::vector<NodeId> Scheduler::mark_failed(NodeId task) {
  // A RETRYING task already left flight when it was put back in the ready set.
  const TaskState current_state = execution_state_.state(task);
  const bool was_in_flight = current_state == TaskState::Queued || current_state == TaskState::Running;
  std::vector<NodeId> cancelled_tasks = execution_state_.onFailed(task);
  if (was_in_flight) remove_from_flight(task);
  return cancelled_tasks;
}

}  // namespace ats
