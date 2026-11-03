#include "ats/Scheduler.hpp"

#include <stdexcept>
#include <utility>

namespace ats {

Scheduler::Scheduler(const Dag& dag, std::unique_ptr<SchedulingPolicy> policy,
                     std::shared_ptr<std::uint64_t> tickets)
    : dag_(&dag), state_(dag), policy_(std::move(policy)),
      counter_(tickets ? std::move(tickets) : std::make_shared<std::uint64_t>(0)), tickets_(dag.size(), 0) {
  if (!policy_) throw std::invalid_argument("Scheduler needs a policy");
}

void Scheduler::push(NodeId n) {
  tickets_.at(n) = ++*counter_;
  policy_->push(n);
}

bool Scheduler::dispatchable(NodeId n) const {
  const TaskState s = state_.state(n);
  return s == TaskState::Ready || s == TaskState::Retrying;
}

void Scheduler::start() {
  for (NodeId n : state_.start()) push(n);
}

std::optional<NodeId> Scheduler::peekNext() {
  while (auto n = policy_->peek()) {
    if (dispatchable(*n)) return n;
    policy_->pop();  // stale: drop it so the next head can be seen
  }
  return std::nullopt;
}

std::optional<Dispatch> Scheduler::dispatchNext() {
  while (auto n = policy_->pop()) {
    // Defensive: anything not dispatchable any more (e.g. cancelled) is dropped from the ready set.
    if (!dispatchable(*n)) continue;
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
  for (NodeId c : ready) push(c);
  return ready;
}

void Scheduler::retry(NodeId n) {
  state_.onRetry(n);
  leaveFlight(n);
  push(n);
}

void Scheduler::onExpanded(NodeId e, std::vector<double> priorities) {
  state_.onExpanded(e);
  tickets_.resize(dag_->size(), 0);
  policy_->reprioritize(std::move(priorities));
}

std::vector<NodeId> Scheduler::fail(NodeId n) {
  // A RETRYING task already left flight when it was put back in the ready set.
  const bool wasInFlight = state_.state(n) == TaskState::Queued || state_.state(n) == TaskState::Running;
  std::vector<NodeId> cancelled = state_.onFailed(n);
  if (wasInFlight) leaveFlight(n);
  return cancelled;
}

}  // namespace ats
