#pragma once

#include <cstddef>
#include <deque>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "ats/Dag.hpp"

namespace ats {

// Decides which READY task is dispatched next. This is the only place scheduling policy lives:
// ExecutionState says WHAT is ready, the policy says which ready task goes FIRST.
class SchedulingPolicy {
 public:
  virtual ~SchedulingPolicy() = default;
  virtual std::string_view name() const = 0;
  virtual void push(NodeId n) = 0;
  virtual std::optional<NodeId> pop() = 0;
  virtual std::size_t size() const = 0;
  bool empty() const { return size() == 0; }
};

// Baseline: first ready, first dispatched.
class FifoPolicy final : public SchedulingPolicy {
 public:
  std::string_view name() const override { return "fifo"; }
  void push(NodeId n) override { queue_.push_back(n); }
  std::optional<NodeId> pop() override;
  std::size_t size() const override { return queue_.size(); }

 private:
  std::deque<NodeId> queue_;
};

// Dispatches the ready task with the largest bottom level (its duration plus the longest path
// to a sink) first, so long chains start early. Ties go to the earlier task in plan order.
class CriticalPathPolicy final : public SchedulingPolicy {
 public:
  explicit CriticalPathPolicy(std::vector<double> bottomLevels) : bottom_(std::move(bottomLevels)) {}
  // The heap's comparator points at bottom_, so the object must never be copied or moved.
  CriticalPathPolicy(const CriticalPathPolicy&) = delete;
  CriticalPathPolicy& operator=(const CriticalPathPolicy&) = delete;
  std::string_view name() const override { return "critical-path"; }
  void push(NodeId n) override;
  std::optional<NodeId> pop() override;
  std::size_t size() const override { return heap_.size(); }
  double priority(NodeId n) const { return bottom_.at(n); }

 private:
  struct Before {
    const std::vector<double>* bottom;
    bool operator()(NodeId a, NodeId b) const {  // true if a has LOWER priority than b
      if ((*bottom)[a] != (*bottom)[b]) return (*bottom)[a] < (*bottom)[b];
      return a > b;
    }
  };
  std::vector<double> bottom_;
  std::priority_queue<NodeId, std::vector<NodeId>, Before> heap_{Before{&bottom_}};
};

// "fifo" | "critical-path". `durations` feeds priority-based policies. Throws std::invalid_argument.
std::unique_ptr<SchedulingPolicy> makePolicy(std::string_view name, const Dag& dag,
                                             const std::vector<double>& durations);
std::vector<std::string> policyNames();

}  // namespace ats
