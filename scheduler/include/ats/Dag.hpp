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
struct DagExpandResult;

// Dependency graph for one plan. Edge parent -> child means "child consumes parent's output".
// Built once by build(); afterwards it only grows, through expand() (dynamic expansion, 8.11).
class Dag {
 public:
  using BuildResult = DagBuildResult;

  // Structural validation + construction: duplicate ids, unknown/self/duplicate dependencies,
  // and cycles (Kahn's algorithm, with the offending cycle reported as a path).
  static BuildResult build(const Plan& plan);

  // Dynamic expansion (design doc 8.11, D53). Splices `subplan` (local ids s1, s2, ...) under the
  // expand task `e`, which must not have completed yet:
  //   - subplan task i becomes "<e>.<i>" (in subplan order) and depends on e plus its renamed local deps;
  //   - a synthetic "<e>.join" node depends on every subplan sink;
  //   - every former child of e now depends on "<e>.join" instead of e.
  // Checks first (e is an expand task that was never expanded, 1-20 tasks, no nested expand,
  // local dependencies only, no duplicate/self/unknown dependency, no cycle) and changes nothing
  // if any check fails. New node ids are appended, so existing NodeIds stay valid.
  DagExpandResult expand(NodeId e, const std::vector<TaskSpec>& subplan);
  static std::string joinId(const std::string& expandId) { return expandId + ".join"; }
  bool isJoin(NodeId n) const;  // a synthetic join node: completed by the runtime, never dispatched

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

struct DagExpandResult {
  std::vector<NodeId> added;           // the subplan tasks, in subplan order
  std::optional<NodeId> join;          // the join node
  std::vector<NodeId> rewired;         // former children of the expand task, now children of the join
  std::vector<ValidationIssue> issues; // nothing was changed when non-empty
  bool ok() const { return issues.empty(); }
};

}  // namespace ats
