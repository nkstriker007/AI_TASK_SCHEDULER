#pragma once

#include <filesystem>
#include <optional>
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
LoadResult loadPlanFile(const std::filesystem::path& path);

}  // namespace ats
