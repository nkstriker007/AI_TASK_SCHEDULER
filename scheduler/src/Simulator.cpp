#include "ats/Simulator.hpp"

#include <algorithm>
#include <queue>
#include <set>
#include <stdexcept>
#include <tuple>

#include "ats/Analysis.hpp"
#include "ats/Policy.hpp"
#include "ats/Scheduler.hpp"

namespace ats {

const char* toString(SimEventKind k) {
  switch (k) {
    case SimEventKind::Start: return "start";
    case SimEventKind::Complete: return "done";
    case SimEventKind::Retry: return "retry";
    case SimEventKind::Fail: return "FAIL";
    case SimEventKind::Cancel: return "cancel";
  }
  return "?";
}

namespace {

// SplitMix64: tiny, well-distributed and identical on every compiler and standard library
// (std::uniform_real_distribution is not, so Linux and macOS would disagree).
std::uint64_t mix(std::uint64_t x) {
  x += 0x9E3779B97F4A7C15ULL;
  x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
  x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
  return x ^ (x >> 31);
}

// Deterministic factor in [1-j, 1+j] for one (task, attempt).
double jitterFactor(std::uint64_t seed, NodeId n, int attempt, double j) {
  if (j <= 0.0) return 1.0;
  const std::uint64_t h = mix(seed ^ mix(static_cast<std::uint64_t>(n) * 1000003ULL + static_cast<std::uint64_t>(attempt)));
  const double u = static_cast<double>(h >> 11) * 0x1.0p-53;  // [0, 1)
  return 1.0 - j + 2.0 * j * u;
}

struct Running {
  double end;
  NodeId node;
  int worker;
  int attempt;
  double start;
  // Min-heap on (end, node): simultaneous completions are processed in plan order.
  bool operator>(const Running& o) const { return std::tie(end, node) > std::tie(o.end, o.node); }
};

}  // namespace

SimReport simulate(const Dag& dag, const SimConfig& cfg) {
  if (cfg.workers < 0) throw std::invalid_argument("workers must be >= 0 (0 = unlimited)");
  if (cfg.jitter < 0.0 || cfg.jitter >= 1.0) throw std::invalid_argument("jitter must be in [0, 1)");
  for (const auto& set : {cfg.failTaskIds, cfg.flakyTaskIds})
    for (const auto& id : set)
      if (!dag.find(id)) throw std::invalid_argument("unknown task id in fault injection: " + id);

  const std::vector<double> base = estimatedDurations(dag, cfg.defaultSeconds);
  auto duration = [&](NodeId n, int attempt) { return base[n] * jitterFactor(cfg.seed, n, attempt, cfg.jitter); };

  std::vector<double> firstAttempt(dag.size());
  for (NodeId n = 0; n < dag.size(); ++n) firstAttempt[n] = duration(n, 1);

  Scheduler scheduler(dag, makePolicy(cfg.policy, dag, firstAttempt));
  SimReport r;
  r.policy = std::string(scheduler.policy().name());
  r.workers = cfg.workers;
  for (double d : firstAttempt) r.sequentialSeconds += d;
  const CriticalPath cp = criticalPath(dag, firstAttempt);
  r.criticalPathSeconds = cp.seconds;
  r.criticalPath = cp.path;

  const bool unlimited = cfg.workers == 0;
  std::set<int> freeWorkers;  // lowest id first, so worker numbering is stable
  for (int w = 0; w < cfg.workers; ++w) freeWorkers.insert(w);
  int nextUnlimitedWorker = 0;
  std::priority_queue<Running, std::vector<Running>, std::greater<>> running;
  double now = 0.0;

  scheduler.start();
  while (true) {
    // 1. Fill every free worker with the policy's next choice.
    while (scheduler.has_ready_tasks() && (unlimited || !freeWorkers.empty())) {
      auto dispatch = scheduler.dispatch_next();
      if (!dispatch) break;
      int worker;
      if (unlimited) {
        worker = nextUnlimitedWorker++;
      } else {
        worker = *freeWorkers.begin();
        freeWorkers.erase(freeWorkers.begin());
      }
      scheduler.mark_started(dispatch->task);
      running.push({now + duration(dispatch->task, dispatch->attempt), dispatch->task, worker, dispatch->attempt, now});
      r.events.push_back({now, SimEventKind::Start, dispatch->task, worker, dispatch->attempt});
      r.peakParallelism = std::max(r.peakParallelism, static_cast<int>(running.size()));
    }
    if (running.empty()) break;

    // 2. Advance virtual time to the next completion and process every event at that instant.
    now = running.top().end;
    while (!running.empty() && running.top().end == now) {
      Running e = running.top();
      running.pop();
      if (!unlimited) freeWorkers.insert(e.worker);
      r.busySeconds += e.end - e.start;
      const std::string& id = dag.id(e.node);

      SimEventKind outcome;
      if (cfg.failTaskIds.count(id)) {
        outcome = SimEventKind::Fail;
        r.events.push_back({now, outcome, e.node, e.worker, e.attempt});
        for (NodeId c : scheduler.mark_failed(e.node)) r.events.push_back({now, SimEventKind::Cancel, c, -1, 0});
      } else if (cfg.flakyTaskIds.count(id) && e.attempt == 1) {
        outcome = SimEventKind::Retry;
        r.events.push_back({now, outcome, e.node, e.worker, e.attempt});
        scheduler.mark_for_retry(e.node);
        ++r.retries;
      } else {
        outcome = SimEventKind::Complete;
        r.events.push_back({now, outcome, e.node, e.worker, e.attempt});
        scheduler.mark_completed(e.node);
      }
      r.attempts.push_back({e.node, e.worker, e.attempt, e.start, e.end, outcome});
    }
  }

  if (!scheduler.is_done()) throw std::logic_error("simulation stalled with unfinished tasks");
  const ExecutionState& st = scheduler.execution_state();
  r.completed = st.count(TaskState::Completed);
  r.failed = st.count(TaskState::Failed);
  r.cancelled = st.count(TaskState::Cancelled);
  r.succeeded = st.succeeded();
  r.makespan = now;
  if (r.makespan > 0.0) {
    // Speedup only means something if all the work was done: a failed run "finishes early"
    // because cancelled tasks never ran.
    if (r.succeeded) r.speedup = r.sequentialSeconds / r.makespan;
    const int capacity = unlimited ? r.peakParallelism : cfg.workers;
    r.utilization = r.busySeconds / (capacity * r.makespan);
  }
  if (r.criticalPathSeconds > 0.0) r.maxSpeedup = r.sequentialSeconds / r.criticalPathSeconds;
  return r;
}

}  // namespace ats
