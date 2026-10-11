#include "ats/Policy.hpp"

#include <stdexcept>

#include "ats/Analysis.hpp"

namespace ats {

std::optional<NodeId> FifoPolicy::pop() {
  if (queue_.empty()) return std::nullopt;
  NodeId n = queue_.front();
  queue_.pop_front();
  return n;
}

void CriticalPathPolicy::push(NodeId n) {
  if (n >= bottom_.size()) throw std::out_of_range("CriticalPathPolicy: unknown task");
  heap_.push(n);
}

std::optional<NodeId> CriticalPathPolicy::pop() {
  if (heap_.empty()) return std::nullopt;
  NodeId n = heap_.top();
  heap_.pop();
  return n;
}

std::unique_ptr<SchedulingPolicy> makePolicy(std::string_view name, const Dag& dag,
                                             const std::vector<double>& durations) {
  if (name == "fifo") return std::make_unique<FifoPolicy>();
  if (name == "critical-path") return std::make_unique<CriticalPathPolicy>(bottomLevels(dag, durations));
  throw std::invalid_argument("unknown scheduling policy \"" + std::string(name) + "\" (use fifo or critical-path)");
}

std::vector<std::string> policyNames() { return {"fifo", "critical-path"}; }

}  // namespace ats
