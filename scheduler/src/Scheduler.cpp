#include "ats/Scheduler.hpp"

#include <stdexcept>
#include <utility>

namespace ats {

Scheduler::Scheduler(const Dag& dag, std::unique_ptr<SchedulingPolicy> policy)
    : dag_(&dag), state_(dag), policy_(std::move(policy)) {
  if (!policy_) throw std::invalid_argument("Scheduler needs a policy");
}

void Scheduler::start() {
  for (NodeId n : state_.start()) policy_->push(n);
}

std::optional<Dispatch> Scheduler::dispatchNext() {
  while (auto n = policy_->pop()) {
    // Defensive: anything not dispatchable any more (e.g. cancelled) is dropped from the ready set.
    const TaskState s = state_.state(*n);
    if (s != TaskState::Ready && s != TaskState::Retrying) continue;
    const int attempt = state_.onDispatched(*n);
    ++inFlight_;
    return Dispatch{*n, attempt};
  }
  return std::nullopt;
}

void Scheduler::started(NodeId n) { state_.onStarted(n); }

void Scheduler::leaveFlight(NodeId n) {
  if (inFlight_ == 0) throw std::logic_error("in-flight count underflow at task " + dag_->id(n));
  --inFlight_;
}

std::vector<NodeId> Scheduler::complete(NodeId n) {
  std::vector<NodeId> ready = state_.onCompleted(n);
  leaveFlight(n);
  for (NodeId c : ready) policy_->push(c);
  return ready;
}

void Scheduler::retry(NodeId n) {
  state_.onRetry(n);
  leaveFlight(n);
  policy_->push(n);
}

std::vector<NodeId> Scheduler::fail(NodeId n) {
  // A RETRYING task already left flight when it was put back in the ready set.
  const bool wasInFlight = state_.state(n) == TaskState::Queued || state_.state(n) == TaskState::Running;
  std::vector<NodeId> cancelled = state_.onFailed(n);
  if (wasInFlight) leaveFlight(n);
  return cancelled;
}

}  // namespace ats
