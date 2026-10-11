// ats_sim: load a plan, validate it, and simulate its parallel execution.
//
//   ats_sim <plan.json> [--workers N] [--policy fifo|critical-path] [--default-seconds S]
//                       [--jitter J] [--seed K] [--fail t2,t5] [--flaky t3] [--json]
//
// Exit codes: 0 every task completed, 1 invalid plan or arguments, 2 execution failed.
#include <cstdio>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <unordered_set>

#include <nlohmann/json.hpp>

#include "ats/Dag.hpp"
#include "ats/PlanLoader.hpp"
#include "ats/Simulator.hpp"

namespace {

void usage() {
  std::cerr << "usage: ats_sim <plan.json> [--workers N (0=unlimited)] [--policy fifo|critical-path]\n"
               "               [--default-seconds S] [--jitter J] [--seed K]\n"
               "               [--fail t2,t5] [--flaky t3] [--json]\n";
}

std::unordered_set<std::string> splitIds(const std::string& s) {
  std::unordered_set<std::string> out;
  std::stringstream ss(s);
  std::string item;
  while (std::getline(ss, item, ','))
    if (!item.empty()) out.insert(item);
  return out;
}

std::string fmt(double v, int precision = 2) {
  char buf[64];
  std::snprintf(buf, sizeof buf, "%.*f", precision, v);
  return buf;
}

nlohmann::json toJson(const ats::Dag& g, const ats::SimReport& r) {
  using nlohmann::json;
  json events = json::array();
  for (const auto& e : r.events)
    events.push_back({{"time", e.time}, {"event", ats::toString(e.kind)}, {"task", g.id(e.node)},
                      {"worker", e.worker}, {"attempt", e.attempt}});
  json attempts = json::array();
  for (const auto& a : r.attempts)
    attempts.push_back({{"task", g.id(a.node)}, {"worker", a.worker}, {"attempt", a.attempt},
                        {"start", a.start}, {"end", a.end}, {"outcome", ats::toString(a.outcome)}});
  json cp = json::array();
  for (auto n : r.criticalPath) cp.push_back(g.id(n));
  return {{"request_id", g.requestId()},
          {"tasks", g.size()},
          {"policy", r.policy},
          {"workers", r.workers},
          {"makespan", r.makespan},
          {"sequential_seconds", r.sequentialSeconds},
          {"busy_seconds", r.busySeconds},
          {"critical_path_seconds", r.criticalPathSeconds},
          {"critical_path", cp},
          {"speedup", r.speedup},
          {"max_speedup", r.maxSpeedup},
          {"utilization", r.utilization},
          {"peak_parallelism", r.peakParallelism},
          {"completed", r.completed},
          {"failed", r.failed},
          {"cancelled", r.cancelled},
          {"retries", r.retries},
          {"succeeded", r.succeeded},
          {"events", events},
          {"attempts", attempts}};
}

void printText(const ats::Dag& g, const ats::SimReport& r) {
  std::cout << "plan " << g.requestId() << ": " << g.size() << " tasks | policy " << r.policy << " | workers "
            << (r.workers == 0 ? std::string("unlimited") : std::to_string(r.workers)) << "\n";
  std::map<double, std::string> lines;  // one line per instant, events in order
  for (const auto& e : r.events) {
    std::string& line = lines[e.time];
    if (!line.empty()) line += " | ";
    line += std::string(ats::toString(e.kind)) + " " + g.id(e.node);
    if (e.kind == ats::SimEventKind::Start) {
      line += " " + g.task(e.node).title + " (w" + std::to_string(e.worker) + ")";
      if (e.attempt > 1) line += " attempt " + std::to_string(e.attempt);
    }
  }
  for (const auto& [t, line] : lines) {
    std::string ts = fmt(t);
    std::cout << "t=" << std::string(ts.size() < 8 ? 8 - ts.size() : 0, ' ') << ts << "  " << line << "\n";
  }
  std::string path;
  for (auto n : r.criticalPath) path += (path.empty() ? "" : " -> ") + g.id(n);
  std::cout << "makespan " << fmt(r.makespan) << "s | sequential " << fmt(r.sequentialSeconds)
            << "s | critical path " << fmt(r.criticalPathSeconds) << "s (" << path << ")\n"
            << "speedup " << (r.succeeded ? fmt(r.speedup) + "x" : std::string("n/a (run failed)")) << " (max "
            << fmt(r.maxSpeedup) << "x) | peak parallelism "
            << r.peakParallelism << " | utilization " << fmt(100.0 * r.utilization, 0) << "% | retries "
            << r.retries << "\n"
            << "outcome " << (r.succeeded ? "SUCCEEDED" : "FAILED") << " (" << r.completed << " completed, "
            << r.failed << " failed, " << r.cancelled << " cancelled)\n";
}

}  // namespace

int main(int argc, char** argv) {
  std::string path;
  ats::SimConfig cfg;
  bool asJson = false;
  try {
    for (int i = 1; i < argc; ++i) {
      const std::string a = argv[i];
      auto value = [&]() -> std::string {
        if (i + 1 >= argc) throw std::invalid_argument("missing value for " + a);
        return argv[++i];
      };
      if (a == "--workers") cfg.workers = std::stoi(value());
      else if (a == "--policy") cfg.policy = value();
      else if (a == "--default-seconds") cfg.defaultSeconds = std::stod(value());
      else if (a == "--jitter") cfg.jitter = std::stod(value());
      else if (a == "--seed") cfg.seed = std::stoull(value());
      else if (a == "--fail") cfg.failTaskIds = splitIds(value());
      else if (a == "--flaky") cfg.flakyTaskIds = splitIds(value());
      else if (a == "--json") asJson = true;
      else if (a == "-h" || a == "--help") { usage(); return 0; }
      else if (!a.empty() && a[0] == '-') throw std::invalid_argument("unknown option " + a);
      else if (path.empty()) path = a;
      else throw std::invalid_argument("more than one plan file given");
    }
    if (path.empty()) throw std::invalid_argument("no plan file given");
  } catch (const std::exception& e) {
    std::cerr << "ats_sim: " << e.what() << "\n";
    usage();
    return 1;
  }

  ats::LoadResult loaded = ats::loadPlanFile(path);
  std::vector<ats::ValidationIssue> issues = loaded.issues;
  std::optional<ats::Dag> dag;
  if (loaded.ok()) {
    ats::Dag::BuildResult built = ats::Dag::build(*loaded.plan);
    issues = built.issues;
    dag = std::move(built.dag);
  }
  if (!dag) {
    if (asJson) {
      nlohmann::json out = {{"valid", false}, {"issues", nlohmann::json::array()}};
      for (const auto& i : issues) out["issues"].push_back({{"code", i.code}, {"task_id", i.taskId}, {"message", i.message}});
      std::cout << out.dump(2) << "\n";
    } else {
      std::cerr << "invalid plan " << path << ":\n";
      for (const auto& i : issues) std::cerr << "  " << i.code << ": " << i.message << "\n";
    }
    return 1;
  }

  ats::SimReport report;
  try {
    report = ats::simulate(*dag, cfg);
  } catch (const std::invalid_argument& e) {
    std::cerr << "ats_sim: " << e.what() << "\n";
    return 1;
  }
  if (asJson) {
    nlohmann::json out = toJson(*dag, report);
    out["valid"] = true;
    std::cout << out.dump(2) << "\n";
  } else {
    printText(*dag, report);
  }
  return report.succeeded ? 0 : 2;
}
