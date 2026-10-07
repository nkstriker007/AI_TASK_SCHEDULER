#include "ats/Dag.hpp"

#include <algorithm>
#include <deque>
#include <unordered_set>

namespace ats {

namespace {

// Every node left over after Kahn's algorithm has at least one parent that is also left over,
// so walking parents from any leftover node must revisit a node: that loop is a cycle.
std::vector<NodeId> findCycle(const std::vector<std::vector<NodeId>>& parents, const std::vector<bool>& leftover) {
  NodeId start = 0;
  while (!leftover[start]) ++start;
  std::vector<NodeId> walk;
  std::vector<std::ptrdiff_t> seenAt(parents.size(), -1);
  NodeId cur = start;
  while (seenAt[cur] < 0) {
    seenAt[cur] = static_cast<std::ptrdiff_t>(walk.size());
    walk.push_back(cur);
    for (NodeId p : parents[cur]) {
      if (leftover[p]) {
        cur = p;
        break;
      }
    }
  }
  // walk[seenAt[cur]..] follows child -> parent; reverse it to follow edges forward.
  std::vector<NodeId> cycle(walk.begin() + seenAt[cur], walk.end());
  std::reverse(cycle.begin(), cycle.end());
  // Rotate so the earliest task in plan order comes first (deterministic messages).
  std::rotate(cycle.begin(), std::min_element(cycle.begin(), cycle.end()), cycle.end());
  return cycle;
}

}  // namespace

Dag::BuildResult Dag::build(const Plan& plan) {
  BuildResult result;
  Dag g;
  g.requestId_ = plan.requestId;

  // 1. Nodes. A duplicate id is reported and the later copy is dropped.
  std::vector<const TaskSpec*> kept;
  for (const TaskSpec& t : plan.tasks) {
    if (g.index_.count(t.id)) {
      result.issues.push_back({codes::kDuplicateId, t.id, "task id \"" + t.id + "\" is used more than once"});
      continue;
    }
    g.index_.emplace(t.id, g.tasks_.size());
    g.tasks_.push_back(t);
    kept.push_back(&t);
  }
  const std::size_t n = g.tasks_.size();
  g.children_.assign(n, {});
  g.parents_.assign(n, {});

  // 2. Edges parent -> child, with per-edge validation.
  for (NodeId child = 0; child < n; ++child) {
    const TaskSpec& t = *kept[child];
    std::unordered_set<std::string> seen;
    for (const std::string& dep : t.dependsOn) {
      if (dep == t.id) {
        result.issues.push_back({codes::kSelfDependency, t.id, "task \"" + t.id + "\" depends on itself"});
        continue;
      }
      if (!seen.insert(dep).second) {
        result.issues.push_back(
            {codes::kDuplicateDependency, t.id, "task \"" + t.id + "\" lists \"" + dep + "\" more than once"});
        continue;
      }
      auto it = g.index_.find(dep);
      if (it == g.index_.end()) {
        result.issues.push_back(
            {codes::kUnknownDependency, t.id, "task \"" + t.id + "\" depends on unknown task \"" + dep + "\""});
        continue;
      }
      g.parents_[child].push_back(it->second);
      g.children_[it->second].push_back(child);
    }
  }

  // 3. Kahn's algorithm: topological order + cycle detection.
  std::vector<std::size_t> indeg(n);
  std::deque<NodeId> queue;
  for (NodeId v = 0; v < n; ++v) {
    indeg[v] = g.parents_[v].size();
    if (indeg[v] == 0) queue.push_back(v);
  }
  while (!queue.empty()) {
    NodeId v = queue.front();
    queue.pop_front();
    g.topo_.push_back(v);
    for (NodeId c : g.children_[v]) {
      if (--indeg[c] == 0) queue.push_back(c);
    }
  }
  if (g.topo_.size() != n) {
    std::vector<bool> leftover(n);
    for (NodeId v = 0; v < n; ++v) leftover[v] = indeg[v] > 0;
    const std::vector<NodeId> cycle = findCycle(g.parents_, leftover);
    std::string path;
    for (NodeId v : cycle) path += g.tasks_[v].id + " -> ";
    path += g.tasks_[cycle.front()].id;
    const std::size_t blocked = (n - g.topo_.size()) - cycle.size();
    std::string msg = "dependency cycle: " + path;
    if (blocked > 0) msg += " (" + std::to_string(blocked) + " more task(s) blocked downstream)";
    result.issues.push_back({codes::kCycle, g.tasks_[cycle.front()].id, msg});
  }

  // 4. Canned subplans (hints.sim_subplan) get the same structural checks on their local ids.
  //    Issues are attributed to the expand task that owns the subplan.
  for (const TaskSpec& t : plan.tasks) {
    if (t.simSubplan.empty()) continue;
    Plan sub;
    sub.tasks = t.simSubplan;
    for (const ValidationIssue& i : build(sub).issues) {
      result.issues.push_back({i.code, t.id, "sim_subplan: " + i.message});
    }
  }

  if (result.issues.empty()) result.dag = std::move(g);
  return result;
}

std::optional<NodeId> Dag::find(std::string_view id) const {
  auto it = index_.find(std::string(id));
  if (it == index_.end()) return std::nullopt;
  return it->second;
}

std::vector<NodeId> Dag::roots() const {
  std::vector<NodeId> r;
  for (NodeId v = 0; v < size(); ++v) {
    if (parents_[v].empty()) r.push_back(v);
  }
  return r;
}

std::vector<NodeId> Dag::sinks() const {
  std::vector<NodeId> r;
  for (NodeId v = 0; v < size(); ++v) {
    if (children_[v].empty()) r.push_back(v);
  }
  return r;
}

}  // namespace ats
