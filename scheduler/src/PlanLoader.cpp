#include "ats/PlanLoader.hpp"

#include <fstream>
#include <initializer_list>
#include <limits>
#include <regex>
#include <sstream>
#include <string>

#include <nlohmann/json.hpp>

namespace ats {

namespace {

using nlohmann::json;

class Checker {
 public:
  std::vector<ValidationIssue> issues;

  void schema(const std::string& path, const std::string& msg, const std::string& taskId = {}) {
    issues.push_back({codes::kSchema, taskId, path + ": " + msg});
  }

  // Rejects any key not in `allowed` (mirrors Pydantic extra="forbid").
  void onlyKeys(const json& obj, const std::string& path, std::initializer_list<const char*> allowed,
                const std::string& taskId = {}) {
    for (const auto& item : obj.items()) {
      bool known = false;
      for (const char* k : allowed) known = known || item.key() == k;
      if (!known) schema(path + "." + item.key(), "unknown field", taskId);
    }
  }

  // Required string with a character-length range. Returns the value when valid.
  std::optional<std::string> requiredString(const json& obj, const char* key, const std::string& path,
                                            std::size_t minChars, std::size_t maxChars,
                                            const std::string& taskId = {}) {
    const std::string p = path + "." + key;
    auto it = obj.find(key);
    if (it == obj.end()) {
      schema(p, "field required", taskId);
      return std::nullopt;
    }
    if (!it->is_string()) {
      schema(p, "must be a string", taskId);
      return std::nullopt;
    }
    std::string v = it->get<std::string>();
    const std::size_t n = utf8Length(v);
    if (n < minChars || n > maxChars) {
      std::ostringstream m;
      m << "must be " << minChars << "-" << maxChars << " characters (got " << n << ")";
      schema(p, m.str(), taskId);
      return std::nullopt;
    }
    return v;
  }
};

const std::regex& idPattern() {
  static const std::regex re("^t[0-9]+$");
  return re;
}

const std::regex& subplanIdPattern() {
  static const std::regex re("^s[0-9]+$");
  return re;
}

std::optional<TaskSpec> parseTask(Checker& c, const json& jt, const std::string& path, bool subplan,
                                  const std::string& ownerId);

// hints.sim_subplan: {"tasks": [SubplanTask, ...]}, 1-kMaxSubplanTasks tasks. Issues are attributed
// to the expand task that owns the subplan (ownerId), as in the Python validator.
void parseSimSubplan(Checker& c, const json& js, const std::string& path, const std::string& ownerId,
                     TaskSpec& owner) {
  if (!js.is_object()) {
    c.schema(path, "must be an object or null", ownerId);
    return;
  }
  c.onlyKeys(js, path, {"tasks"}, ownerId);
  auto it = js.find("tasks");
  if (it == js.end()) {
    c.schema(path + ".tasks", "field required", ownerId);
  } else if (!it->is_array()) {
    c.schema(path + ".tasks", "must be an array", ownerId);
  } else if (it->empty() || it->size() > kMaxSubplanTasks) {
    c.schema(path + ".tasks", "must have 1-" + std::to_string(kMaxSubplanTasks) + " tasks (got " +
                                  std::to_string(it->size()) + ")",
             ownerId);
  } else {
    for (std::size_t i = 0; i < it->size(); ++i) {
      const std::string p = path + ".tasks[" + std::to_string(i) + "]";
      if (auto t = parseTask(c, (*it)[i], p, /*subplan=*/true, ownerId)) owner.simSubplan.push_back(std::move(*t));
    }
  }
}

// Parses one plan task, or one subplan task when `subplan` is set: ids ^s[0-9]+$, no nested
// expand, and hints limited to estimated_seconds. ownerId attributes subplan issues to the
// expand task; it is empty for plan tasks.
std::optional<TaskSpec> parseTask(Checker& c, const json& jt, const std::string& path, bool subplan,
                                  const std::string& ownerId) {
  if (!jt.is_object()) {
    c.schema(path, "must be an object", ownerId);
    return std::nullopt;
  }
  const std::size_t before = c.issues.size();

  // Resolve the id first so every later issue can be attributed to the task.
  std::string taskId;
  if (auto it = jt.find("id"); it != jt.end() && it->is_string()) taskId = it->get<std::string>();
  const std::string issueId = subplan ? ownerId : taskId;

  c.onlyKeys(jt, path, {"id", "type", "title", "instruction", "depends_on", "hints"}, issueId);

  TaskSpec t;
  const char* idRule = subplan ? "^s[0-9]+$" : "^t[0-9]+$";
  if (auto it = jt.find("id"); it == jt.end()) {
    c.schema(path + ".id", "field required", ownerId);
  } else if (!it->is_string()) {
    c.schema(path + ".id", "must be a string", ownerId);
  } else if (!std::regex_match(taskId, subplan ? subplanIdPattern() : idPattern())) {
    c.schema(path + ".id", std::string("must match ") + idRule + " (got \"" + taskId + "\")", issueId);
  } else {
    t.id = taskId;
  }

  if (auto it = jt.find("type"); it == jt.end()) {
    c.schema(path + ".type", "field required", issueId);
  } else if (!it->is_string()) {
    c.schema(path + ".type", "must be a string", issueId);
  } else if (auto type = parseTaskType(it->get<std::string>())) {
    if (subplan && *type == TaskType::Expand) {
      c.schema(path + ".type", "nested expand is not allowed inside a subplan", issueId);
    } else {
      t.type = *type;
    }
  } else {
    c.schema(path + ".type", "unsupported task type \"" + it->get<std::string>() + "\"", issueId);
  }

  if (auto v = c.requiredString(jt, "title", path, 1, kMaxTitleChars, issueId)) t.title = *v;
  if (auto v = c.requiredString(jt, "instruction", path, 1, kMaxInstructionChars, issueId)) t.instruction = *v;

  if (auto it = jt.find("depends_on"); it != jt.end()) {  // optional, defaults to []
    if (!it->is_array()) {
      c.schema(path + ".depends_on", "must be an array of task ids", issueId);
    } else {
      for (std::size_t i = 0; i < it->size(); ++i) {
        const json& d = (*it)[i];
        if (!d.is_string()) {
          c.schema(path + ".depends_on[" + std::to_string(i) + "]", "must be a string", issueId);
        } else {
          t.dependsOn.push_back(d.get<std::string>());
        }
      }
    }
  }

  if (auto it = jt.find("hints"); it != jt.end() && !it->is_null()) {  // optional, may be null
    if (!it->is_object()) {
      c.schema(path + ".hints", "must be an object or null", issueId);
    } else {
      if (subplan) {
        c.onlyKeys(*it, path + ".hints", {"estimated_seconds"}, issueId);
      } else {
        c.onlyKeys(*it, path + ".hints", {"estimated_seconds", "expected_fanout", "sim_subplan"}, issueId);
      }
      if (auto es = it->find("estimated_seconds"); es != it->end() && !es->is_null()) {
        if (!es->is_number()) {
          c.schema(path + ".hints.estimated_seconds", "must be a number or null", issueId);
        } else if (es->get<double>() < 0.0) {
          c.schema(path + ".hints.estimated_seconds", "must be >= 0", issueId);
        } else {
          t.estimatedSeconds = es->get<double>();
        }
      }
      if (!subplan) {
        if (auto ef = it->find("expected_fanout"); ef != it->end() && !ef->is_null()) {
          if (!ef->is_number_integer()) {
            c.schema(path + ".hints.expected_fanout", "must be an integer or null", issueId);
          } else if (ef->get<long long>() < 1 || ef->get<long long>() > std::numeric_limits<int>::max()) {
            c.schema(path + ".hints.expected_fanout", "must be >= 1", issueId);
          } else {
            t.expectedFanout = static_cast<int>(ef->get<long long>());
          }
        }
        if (auto ss = it->find("sim_subplan"); ss != it->end() && !ss->is_null()) {
          parseSimSubplan(c, *ss, path + ".hints.sim_subplan", taskId, t);
        }
      }
    }
  }

  if (c.issues.size() != before) return std::nullopt;
  return t;
}

}  // namespace

LoadResult parsePlanFile(std::string_view text) {
  Checker c;
  LoadResult result;

  const json doc = json::parse(text.begin(), text.end(), /*cb=*/nullptr, /*allow_exceptions=*/false);
  if (doc.is_discarded()) {
    c.schema("$", "invalid JSON");
    result.issues = std::move(c.issues);
    return result;
  }
  if (!doc.is_object()) {
    c.schema("$", "plan file must be a JSON object");
    result.issues = std::move(c.issues);
    return result;
  }

  Plan plan;
  c.onlyKeys(doc, "$", {"request_id", "created_at", "plan"});
  if (auto v = c.requiredString(doc, "request_id", "$", 1, 200)) plan.requestId = *v;
  if (auto it = doc.find("created_at"); it == doc.end()) {
    c.schema("$.created_at", "field required");
  } else if (!it->is_string()) {
    c.schema("$.created_at", "must be a string");
  } else {
    plan.createdAt = it->get<std::string>();
  }

  auto pit = doc.find("plan");
  if (pit == doc.end()) {
    c.schema("$.plan", "field required");
  } else if (!pit->is_object()) {
    c.schema("$.plan", "must be an object");
  } else {
    const json& jp = *pit;
    c.onlyKeys(jp, "plan", {"schema_version", "summary", "tasks"});

    if (auto it = jp.find("schema_version"); it != jp.end()) {  // optional, defaults to 1
      if (!it->is_number_integer() || it->get<long long>() != kSchemaVersion) {
        c.issues.push_back({codes::kSchemaVersion, {}, "plan.schema_version: must be " +
                                                            std::to_string(kSchemaVersion) + " (got " +
                                                            it->dump() + ")"});
      }
    }
    if (auto v = c.requiredString(jp, "summary", "plan", 1, kMaxSummaryChars)) plan.summary = *v;

    if (auto it = jp.find("tasks"); it == jp.end()) {
      c.schema("plan.tasks", "field required");
    } else if (!it->is_array()) {
      c.schema("plan.tasks", "must be an array");
    } else if (it->empty()) {
      c.issues.push_back({codes::kEmptyPlan, {}, "plan.tasks: plan has no tasks"});
    } else if (it->size() > kMaxTasks) {
      c.issues.push_back({codes::kTooManyTasks, {},
                          "plan.tasks: " + std::to_string(it->size()) + " tasks exceeds the limit of " +
                              std::to_string(kMaxTasks)});
    } else {
      for (std::size_t i = 0; i < it->size(); ++i) {
        const std::string path = "plan.tasks[" + std::to_string(i) + "]";
        if (auto t = parseTask(c, (*it)[i], path, /*subplan=*/false, {})) plan.tasks.push_back(std::move(*t));
      }
    }
  }

  if (c.issues.empty()) result.plan = std::move(plan);
  result.issues = std::move(c.issues);
  return result;
}

LoadResult loadPlanFile(const std::filesystem::path& path) {
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    LoadResult r;
    r.issues.push_back({codes::kSchema, {}, "$: cannot open " + path.string()});
    return r;
  }
  std::ostringstream buf;
  buf << in.rdbuf();
  return parsePlanFile(buf.str());
}

}  // namespace ats
