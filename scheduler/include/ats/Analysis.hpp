#pragma once

#include <cstddef>
#include <vector>

#include "ats/Dag.hpp"

namespace ats {

// Pure graph analysis. No state, no I/O. Used by scheduling policies, the simulator,
// the metrics report and (later) the planner evaluation.

// Duration estimate per task: the planner's hint when present, otherwise `defaultSeconds`.
std::vector<double> estimatedDurations(const Dag& dag, double defaultSeconds);

// Bottom level of each task: its own duration plus the longest duration-weighted path
// from it to any sink. The critical-path scheduling priority.
std::vector<double> bottomLevels(const Dag& dag, const std::vector<double>& durations);

struct CriticalPath {
  double seconds = 0.0;        // lower bound on makespan with unlimited workers
  std::vector<NodeId> path;    // tasks on one longest path, in execution order
};
CriticalPath criticalPath(const Dag& dag, const std::vector<double>& durations);

// Structural shape, independent of durations. Level of a task = 1 + max level of its parents.
struct PlanShape {
  std::size_t tasks = 0;
  std::size_t edges = 0;
  std::size_t depth = 0;   // number of levels (longest path in tasks)
  std::size_t width = 0;   // largest number of tasks on one level
  std::vector<std::size_t> level;  // level of each task, 0-based
};
PlanShape planShape(const Dag& dag);

}  // namespace ats
