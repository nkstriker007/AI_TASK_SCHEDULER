#pragma once

#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "ats/Types.hpp"

namespace ats {

struct LoadResult {
  std::optional<Plan> plan;             // set only when issues is empty
  std::vector<ValidationIssue> issues;  // every schema problem found, not just the first
  bool ok() const { return plan.has_value(); }
};

// Parses and schema-checks a plan file ({request_id, created_at, plan}).
// Never trusts the input: every field is checked for presence, type, enum value and length,
// and unknown fields are rejected. Structural checks (ids, dependencies, cycles) are Dag::build's job.
LoadResult parsePlanFile(std::string_view json);

// An expand task's committed result (contract #1 Subplan: {"tasks": [SubplanTask, ...]}, 1-20 tasks,
// local ids s1..., no nested expand, hints limited to estimated_seconds), schema-checked like
// hints.sim_subplan. Issues are attributed to `ownerId`. Structural checks are Dag::expand's job.
struct SubplanResult {
  std::vector<TaskSpec> tasks;          // set only when issues is empty
  std::vector<ValidationIssue> issues;
  bool ok() const { return issues.empty(); }
};
SubplanResult parseSubplan(std::string_view json, const std::string& ownerId);
LoadResult loadPlanFile(const std::filesystem::path& path);

}  // namespace ats
