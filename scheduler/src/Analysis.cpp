#include "ats/Analysis.hpp"

#include <algorithm>
#include <stdexcept>

namespace ats {

std::vector<double> estimatedDurations(const Dag& dag, double defaultSeconds) {
  if (defaultSeconds < 0.0) throw std::invalid_argument("defaultSeconds must be >= 0");
  std::vector<double> d(dag.size());
  for (NodeId n = 0; n < dag.size(); ++n) d[n] = dag.task(n).estimatedSeconds.value_or(defaultSeconds);
  return d;
}

namespace {
void checkSize(const Dag& dag, const std::vector<double>& durations) {
  if (durations.size() != dag.size()) throw std::invalid_argument("durations.size() != dag.size()");
}
}  // namespace

std::vector<double> bottomLevels(const Dag& dag, const std::vector<double>& durations) {
  checkSize(dag, durations);
  std::vector<double> bl(dag.size(), 0.0);
  const auto& topo = dag.topologicalOrder();
  // Reverse topological order: every child is finished before its parents.
  for (auto it = topo.rbegin(); it != topo.rend(); ++it) {
    const NodeId n = *it;
    double longestChild = 0.0;
    for (NodeId c : dag.children(n)) longestChild = std::max(longestChild, bl[c]);
    bl[n] = durations[n] + longestChild;
  }
  return bl;
}

CriticalPath criticalPath(const Dag& dag, const std::vector<double>& durations) {
  checkSize(dag, durations);
  CriticalPath cp;
  if (dag.size() == 0) return cp;
  // finish[n] = earliest finish time with unlimited workers; pred[n] = parent that determines it.
  std::vector<double> finish(dag.size(), 0.0);
  std::vector<std::ptrdiff_t> pred(dag.size(), -1);
  for (NodeId n : dag.topologicalOrder()) {
    double start = 0.0;
    for (NodeId p : dag.parents(n)) {
      // Latest-finishing parent decides the start; on ties the first parent listed wins.
      if (pred[n] < 0 || finish[p] > start) {
        start = finish[p];
        pred[n] = static_cast<std::ptrdiff_t>(p);
      }
    }
    finish[n] = start + durations[n];
  }
  NodeId end = 0;
  for (NodeId n = 1; n < dag.size(); ++n)
    if (finish[n] > finish[end]) end = n;
  cp.seconds = finish[end];
  for (std::ptrdiff_t v = static_cast<std::ptrdiff_t>(end); v >= 0; v = pred[static_cast<NodeId>(v)])
    cp.path.push_back(static_cast<NodeId>(v));
  std::reverse(cp.path.begin(), cp.path.end());
  return cp;
}

PlanShape planShape(const Dag& dag) {
  PlanShape s;
  s.tasks = dag.size();
  s.level.assign(dag.size(), 0);
  for (NodeId n : dag.topologicalOrder()) {
    s.edges += dag.parents(n).size();
    for (NodeId p : dag.parents(n)) s.level[n] = std::max(s.level[n], s.level[p] + 1);
  }
  if (dag.size() == 0) return s;
  s.depth = *std::max_element(s.level.begin(), s.level.end()) + 1;
  std::vector<std::size_t> perLevel(s.depth, 0);
  for (std::size_t l : s.level) ++perLevel[l];
  s.width = *std::max_element(perLevel.begin(), perLevel.end());
  return s;
}

}  // namespace ats
