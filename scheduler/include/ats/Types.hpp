#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace ats {

inline constexpr int kSchemaVersion = 1;
inline constexpr std::size_t kMaxTasks = 50;
inline constexpr std::size_t kMaxSubplanTasks = 20;
inline constexpr std::size_t kMaxSummaryChars = 300;
inline constexpr std::size_t kMaxTitleChars = 120;
inline constexpr std::size_t kMaxInstructionChars = 2000;

enum class TaskType { Research, Summarize, Analyze, Compare, Report, Expand };

std::optional<TaskType> parseTaskType(std::string_view s);
std::string_view toString(TaskType t);

// Static task description exactly as the planner produced it. Never mutated by the runtime.
struct TaskSpec {
  std::string id;
  TaskType type = TaskType::Research;
  std::string title;
  std::string instruction;
  std::vector<std::string> dependsOn;
  std::optional<double> estimatedSeconds;
  // expand tasks only: expected number of spawned tasks, for priority estimates.
  std::optional<int> expectedFanout;
  // expand tasks only, fixtures only: canned subplan the simulator splices instead of calling
  // an LLM (hints.sim_subplan). Local ids s1, s2, ...; empty when absent.
  std::vector<TaskSpec> simSubplan;
};

struct Plan {
  std::string requestId;
  std::string createdAt;
  int schemaVersion = kSchemaVersion;
  std::string summary;
  std::vector<TaskSpec> tasks;
};

// Issue codes are part of the cross-language contract (docs/contract.md).
namespace codes {
inline constexpr const char* kSchema = "SCHEMA";
inline constexpr const char* kSchemaVersion = "SCHEMA_VERSION";
inline constexpr const char* kEmptyPlan = "EMPTY_PLAN";
inline constexpr const char* kTooManyTasks = "TOO_MANY_TASKS";
inline constexpr const char* kDuplicateId = "DUPLICATE_ID";
inline constexpr const char* kUnknownDependency = "UNKNOWN_DEPENDENCY";
inline constexpr const char* kSelfDependency = "SELF_DEPENDENCY";
inline constexpr const char* kDuplicateDependency = "DUPLICATE_DEPENDENCY";
inline constexpr const char* kCycle = "CYCLE";
}  // namespace codes

struct ValidationIssue {
  std::string code;
  std::string taskId;  // empty when the issue is not tied to one task
  std::string message;
};

// Number of Unicode code points in a UTF-8 string (Pydantic counts characters, not bytes).
std::size_t utf8Length(std::string_view s);

}  // namespace ats
