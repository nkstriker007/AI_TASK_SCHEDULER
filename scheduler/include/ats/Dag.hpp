#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "ats/Types.hpp"

namespace ats {

// Dense index for a task inside one request's graph. String ids exist only at the boundary.
using NodeId = std::size_t;

struct DagBuildResult;

// Immutable dependency graph for one plan. Edge parent -> child means "child consumes parent's output".
class Dag {
 public:
  using BuildResult = DagBuildResult;

  // Structural validation + construction: duplicate ids, unknown/self/duplicate dependencies,
  // and cycles (Kahn's algorithm, with the offending cycle reported as a path).
  static BuildResult build(const Plan& plan);

  Dag(const Dag&) = default;
  Dag(Dag&&) noexcept = default;
  Dag& operator=(const Dag&) = default;
  Dag& operator=(Dag&&) noexcept = default;

  std::size_t size() const { return tasks_.size(); }
  const std::string& requestId() const { return requestId_; }
  const TaskSpec& task(NodeId n) const { return tasks_.at(n); }
  const std::string& id(NodeId n) const { return tasks_.at(n).id; }
  std::optional<NodeId> find(std::string_view id) const;

  const std::vector<NodeId>& children(NodeId n) const { return children_.at(n); }
  const std::vector<NodeId>& parents(NodeId n) const { return parents_.at(n); }
  std::size_t indegree(NodeId n) const { return parents_.at(n).size(); }
  std::vector<NodeId> roots() const;
  std::vector<NodeId> sinks() const;

  // A valid topological order computed once during build. Used for analysis
  // (e.g. critical path), never as an execution order.
  const std::vector<NodeId>& topologicalOrder() const { return topo_; }

 private:
  Dag() = default;

  std::string requestId_;
  std::vector<TaskSpec> tasks_;
  std::unordered_map<std::string, NodeId> index_;
  std::vector<std::vector<NodeId>> children_;
  std::vector<std::vector<NodeId>> parents_;
  std::vector<NodeId> topo_;
};

struct DagBuildResult {
  std::optional<Dag> dag;              // set only when issues is empty
  std::vector<ValidationIssue> issues;
};

}  // namespace ats
