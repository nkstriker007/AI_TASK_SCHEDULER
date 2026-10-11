#include "ats/ExecutionState.hpp"

#include <deque>
#include <stdexcept>
#include <string>

namespace ats {

std::string_view toString(TaskState s) {
  switch (s) {
    case TaskState::Waiting: return "WAITING";
    case TaskState::Ready: return "READY";
    case TaskState::Queued: return "QUEUED";
    case TaskState::Running: return "RUNNING";
    case TaskState::Retrying: return "RETRYING";
    case TaskState::Completed: return "COMPLETED";
    case TaskState::Failed: return "FAILED";
    case TaskState::Cancelled: return "CANCELLED";
  }
  return "UNKNOWN";
}

bool isTerminal(TaskState s) {
  return s == TaskState::Completed || s == TaskState::Failed || s == TaskState::Cancelled;
}

ExecutionState::ExecutionState(const Dag& dag)
    : dag_(&dag), states_(dag.size(), TaskState::Waiting), unresolved_(dag.size()), attempts_(dag.size(), 0) {
  for (NodeId n = 0; n < dag.size(); ++n) unresolved_[n] = dag.indegree(n);
}

void ExecutionState::require(NodeId n, std::initializer_list<TaskState> allowed, std::string_view event) const {
  if (!started_) throw std::logic_error(std::string(event) + " before start()");
  const TaskState s = states_.at(n);
  for (TaskState a : allowed) {
    if (s == a) return;
  }
  throw std::logic_error(std::string(event) + " is illegal for task " + dag_->id(n) + " in state " +
                         std::string(toString(s)));
}

std::vector<NodeId> ExecutionState::start() {
  if (started_) throw std::logic_error("start() called twice");
  started_ = true;
  std::vector<NodeId> ready;
  for (NodeId n = 0; n < states_.size(); ++n) {
    if (unresolved_[n] == 0) {
      states_[n] = TaskState::Ready;
      ready.push_back(n);
    }
  }
  return ready;
}

int ExecutionState::onDispatched(NodeId n) {
  require(n, {TaskState::Ready, TaskState::Retrying}, "onDispatched");
  states_[n] = TaskState::Queued;
  return ++attempts_[n];
}

void ExecutionState::onStarted(NodeId n) {
  require(n, {TaskState::Queued}, "onStarted");
  states_[n] = TaskState::Running;
}

std::vector<NodeId> ExecutionState::onCompleted(NodeId n) {
  require(n, {TaskState::Queued, TaskState::Running}, "onCompleted");
  states_[n] = TaskState::Completed;
  std::vector<NodeId> ready;
  for (NodeId c : dag_->children(n)) {
    if (unresolved_[c] == 0) throw std::logic_error("dependency count underflow for task " + dag_->id(c));
    --unresolved_[c];
    // A child cancelled by a failed sibling branch stays cancelled.
    if (unresolved_[c] == 0 && states_[c] == TaskState::Waiting) {
      states_[c] = TaskState::Ready;
      ready.push_back(c);
    }
  }
  return ready;
}

void ExecutionState::onRetry(NodeId n) {
  require(n, {TaskState::Queued, TaskState::Running}, "onRetry");
  states_[n] = TaskState::Retrying;
}

std::vector<NodeId> ExecutionState::onFailed(NodeId n) {
  require(n, {TaskState::Queued, TaskState::Running, TaskState::Retrying}, "onFailed");
  states_[n] = TaskState::Failed;
  std::vector<NodeId> cancelled;
  std::vector<bool> visited(states_.size(), false);
  std::deque<NodeId> queue{n};
  while (!queue.empty()) {
    NodeId v = queue.front();
    queue.pop_front();
    for (NodeId c : dag_->children(v)) {
      if (visited[c]) continue;
      visited[c] = true;
      if (states_[c] == TaskState::Waiting || states_[c] == TaskState::Ready) {
        states_[c] = TaskState::Cancelled;
        cancelled.push_back(c);
      }
      queue.push_back(c);
    }
  }
  return cancelled;
}

std::size_t ExecutionState::count(TaskState s) const {
  std::size_t k = 0;
  for (TaskState x : states_) k += (x == s);
  return k;
}

bool ExecutionState::isDone() const {
  if (!started_) return false;
  for (TaskState s : states_) {
    if (!isTerminal(s)) return false;
  }
  return true;
}

bool ExecutionState::succeeded() const {
  return started_ && count(TaskState::Completed) == states_.size();
}

}  // namespace ats
