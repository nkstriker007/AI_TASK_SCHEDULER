// ats_sim <plan.json> [options]
// Validates a plan file (PlanLoader + Dag::build), then runs it in the simulator.
//
//   --json                 machine-readable output (contract #1, "ats_sim expectations")
//   --workers N            simulated workers (default 4)
//   --policy P             fifo | critical (default critical)
//   --max-attempts N       attempts per task incl. the first (default 3)
//   --default-seconds S    duration when a task has no estimated_seconds (default 5)
//   --backoff S            first retry wait in seconds, doubled each retry (default 1)
//   --fail ID:N            task ID fails its first N attempts (repeatable)
//   --fail-rate P          every attempt fails with probability P (default 0)
//   --seed N               random seed for --fail-rate (default 1)
//
// Exit codes (contract #1): 0 plan valid and every task completed, 1 plan invalid,
// 2 plan valid but execution failed, 64 usage error.
#include <algorithm>
#include <charconv>
#include <cstdio>
#include <iomanip>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include <nlohmann/json.hpp>

#include "ats/Dag.hpp"
#include "ats/PlanLoader.hpp"
#include "ats/Simulator.hpp"

namespace {

constexpr int kExitOk = 0;
constexpr int kExitInvalid = 1;
constexpr int kExitFailed = 2;
constexpr int kExitUsage = 64;

int usage(const std::string& why) {
  std::cerr << "ats_sim: " << why << "\n"
            << "usage: ats_sim <plan.json> [--json] [--workers N] [--policy fifo|critical]\n"
            << "               [--max-attempts N] [--default-seconds S] [--backoff S]\n"
            << "               [--fail ID:N]... [--fail-rate P] [--seed N]\n";
  return kExitUsage;
}

template <typename T>
bool parseNum(std::string_view s, T& out) {
  if constexpr (std::is_floating_point_v<T>) {
    try {
      std::size_t used = 0;
      out = static_cast<T>(std::stod(std::string(s), &used));
      return used == s.size();
    } catch (...) {
      return false;
    }
  } else {
    auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), out);
    return ec == std::errc() && p == s.data() + s.size();
  }
}

std::string fmt(double v) {
  std::ostringstream o;
  o << std::fixed << std::setprecision(1) << v;
  return o.str();
}

nlohmann::json issuesJson(const std::vector<ats::ValidationIssue>& v) {
  nlohmann::json a = nlohmann::json::array();
  for (const auto& i : v) a.push_back({{"code", i.code}, {"task_id", i.taskId}, {"message", i.message}});
  return a;
}

void printHuman(const ats::Dag& dag, const ats::SimOptions& o, const ats::SimResult& r) {
  std::cout << "request " << dag.requestId() << ": " << dag.size() << " tasks, " << o.workers
            << " workers, policy " << ats::toString(o.policy) << "\n\n";
  for (const auto& e : r.events) {
    std::cout << "[" << std::setw(7) << fmt(e.t) << "s] " << std::left << std::setw(11) << e.event
              << std::right << std::setw(6) << e.taskId;
    if (e.attempt > 0) std::cout << "  attempt " << e.attempt;
    if (e.worker >= 0) std::cout << "  worker " << e.worker;
    std::cout << "\n";
  }

  // Text timeline, one row per task (last attempt only).
  constexpr int kWidth = 50;
  double total = r.makespan > 0 ? r.makespan : 1.0;
  std::cout << "\n";
  for (const auto& t : r.tasks) {
    std::string bar(kWidth, ' ');
    if (t.start >= 0 && t.end >= 0) {
      int a = static_cast<int>(t.start / total * kWidth);
      int b = std::max(a + 1, static_cast<int>(t.end / total * kWidth));
      char c = t.state == ats::TaskState::Completed ? '#' : 'x';
      for (int i = a; i < b && i < kWidth; ++i) bar[static_cast<std::size_t>(i)] = c;
    }
    std::cout << std::left << std::setw(6) << t.id << std::right << " |" << bar << "| "
              << ats::toString(t.state);
    if (t.attempts > 1) std::cout << " after " << t.attempts << " attempts";
    std::cout << "\n";
  }
  std::cout << "\n"
            << (r.succeeded ? "succeeded" : "FAILED") << ": makespan " << fmt(r.makespan) << "s, "
            << r.retries << " retries\n";
}

nlohmann::json resultJson(const ats::Dag& dag, const ats::SimOptions& o, const ats::SimResult& r) {
  nlohmann::json tasks = nlohmann::json::array();
  for (const auto& t : r.tasks) {
    tasks.push_back({{"id", t.id}, {"state", ats::toString(t.state)}, {"attempts", t.attempts},
                     {"start", t.start}, {"end", t.end}, {"worker", t.worker}});
  }
  nlohmann::json events = nlohmann::json::array();
  for (const auto& e : r.events) {
    events.push_back({{"t", e.t}, {"task_id", e.taskId}, {"event", e.event},
                      {"attempt", e.attempt}, {"worker", e.worker}});
  }
  return {{"valid", true},
          {"request_id", dag.requestId()},
          {"issues", nlohmann::json::array()},
          {"workers", o.workers},
          {"policy", ats::toString(o.policy)},
          {"succeeded", r.succeeded},
          {"makespan_s", r.makespan},
          {"retries", r.retries},
          {"tasks", tasks},
          {"events", events}};
}

}  // namespace

int main(int argc, char** argv) {
  std::string path;
  bool json = false;
  ats::SimOptions opts;

  for (int i = 1; i < argc; ++i) {
    std::string_view a = argv[i];
    auto value = [&](std::string_view& out) {
      if (i + 1 >= argc) return false;
      out = argv[++i];
      return true;
    };
    std::string_view v;
    if (a == "--json") {
      json = true;
    } else if (a == "--workers") {
      if (!value(v) || !parseNum(v, opts.workers) || opts.workers < 1) return usage("--workers needs an integer >= 1");
    } else if (a == "--policy") {
      if (!value(v)) return usage("--policy needs fifo or critical");
      auto p = ats::parsePolicy(v);
      if (!p) return usage("unknown policy '" + std::string(v) + "'");
      opts.policy = *p;
    } else if (a == "--max-attempts") {
      if (!value(v) || !parseNum(v, opts.maxAttempts) || opts.maxAttempts < 1) return usage("--max-attempts needs an integer >= 1");
    } else if (a == "--default-seconds") {
      if (!value(v) || !parseNum(v, opts.defaultSeconds) || opts.defaultSeconds < 0) return usage("--default-seconds needs a number >= 0");
    } else if (a == "--backoff") {
      if (!value(v) || !parseNum(v, opts.backoffSeconds) || opts.backoffSeconds < 0) return usage("--backoff needs a number >= 0");
    } else if (a == "--fail-rate") {
      if (!value(v) || !parseNum(v, opts.failRate) || opts.failRate < 0 || opts.failRate > 1) return usage("--fail-rate needs a number in [0, 1]");
    } else if (a == "--seed") {
      if (!value(v) || !parseNum(v, opts.seed)) return usage("--seed needs a non-negative integer");
    } else if (a == "--fail") {
      if (!value(v)) return usage("--fail needs ID:N");
      auto colon = v.rfind(':');
      int n = 0;
      if (colon == std::string_view::npos || colon == 0 || !parseNum(v.substr(colon + 1), n) || n < 1)
        return usage("--fail needs ID:N with N >= 1, e.g. --fail t3:2");
      opts.failFirst[std::string(v.substr(0, colon))] = n;
    } else if (!a.empty() && a.front() == '-') {
      return usage("unknown option " + std::string(a));
    } else if (path.empty()) {
      path = a;
    } else {
      return usage("only one plan file");
    }
  }
  if (path.empty()) return usage("missing plan file");

  // 1. Validate exactly like ats_validate: schema first, then structure.
  ats::LoadResult loaded = ats::loadPlanFile(path);
  std::vector<ats::ValidationIssue> issues = loaded.issues;
  std::optional<ats::Dag> dag;
  if (loaded.ok()) {
    ats::Dag::BuildResult built = ats::Dag::build(*loaded.plan);
    issues.insert(issues.end(), built.issues.begin(), built.issues.end());
    dag = std::move(built.dag);
  }
  if (!issues.empty() || !dag) {
    if (json) {
      std::cout << nlohmann::json{{"valid", false}, {"issues", issuesJson(issues)}}.dump(2) << "\n";
    } else {
      std::cout << "plan rejected (" << issues.size() << " issue" << (issues.size() == 1 ? "" : "s") << "):\n";
      for (const auto& i : issues)
        std::cout << "  " << i.code << (i.taskId.empty() ? "" : " [" + i.taskId + "]") << ": " << i.message << "\n";
    }
    return kExitInvalid;
  }

  for (const auto& [id, n] : opts.failFirst) {
    if (!dag->find(id)) return usage("--fail names unknown task '" + id + "'");
  }

  // 2. Run it.
  ats::SimResult result = ats::Simulator::run(*dag, opts);
  if (json) {
    std::cout << resultJson(*dag, opts, result).dump(2) << "\n";
  } else {
    printHuman(*dag, opts, result);
  }
  return result.succeeded ? kExitOk : kExitFailed;
}
