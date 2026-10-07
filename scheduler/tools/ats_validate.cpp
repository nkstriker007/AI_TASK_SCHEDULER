// ats_validate <plan.json>
// Runs the C++ side of validation (schema checks + structural checks) and prints
// {"valid": bool, "request_id": ..., "tasks": N, "roots": [...], "issues": [{code, task_id, message}]}.
// Exit code: 0 valid, 1 invalid, 2 usage error. Used by the cross-language parity test.
#include <iostream>

#include <nlohmann/json.hpp>

#include "ats/Dag.hpp"
#include "ats/PlanLoader.hpp"

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "usage: ats_validate <plan.json>\n";
    return 2;
  }
  nlohmann::json out;
  nlohmann::json issues = nlohmann::json::array();
  auto addIssues = [&](const std::vector<ats::ValidationIssue>& v) {
    for (const auto& i : v) issues.push_back({{"code", i.code}, {"task_id", i.taskId}, {"message", i.message}});
  };

  ats::LoadResult loaded = ats::loadPlanFile(argv[1]);
  addIssues(loaded.issues);
  if (loaded.ok()) {
    out["request_id"] = loaded.plan->requestId;
    out["tasks"] = loaded.plan->tasks.size();
    ats::Dag::BuildResult built = ats::Dag::build(*loaded.plan);
    addIssues(built.issues);
    if (built.dag) {
      nlohmann::json roots = nlohmann::json::array();
      for (ats::NodeId r : built.dag->roots()) roots.push_back(built.dag->id(r));
      out["roots"] = roots;
    }
  }
  out["valid"] = issues.empty();
  out["issues"] = issues;
  std::cout << out.dump(2) << "\n";
  return issues.empty() ? 0 : 1;
}
