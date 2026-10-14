#include "ats/Simulator.hpp"

#include <algorithm>
#include <cmath>
#include <queue>
#include <random>
#include <stdexcept>
#include <tuple>

namespace ats {

std::optional<Policy> parsePolicy(std::string_view s) {
  if (s == "fifo") return Policy::Fifo;
  if (s == "critical" || s == "critical-path") return Policy::CriticalPath;
  return std::nullopt;
}

std::string_view toString(Policy p) {
  return p == Policy::Fifo ? "fifo" : "critical-path";
}

namespace {

double durationOf(const TaskSpec& t, double defaultSeconds) {
  return t.estimatedSeconds.value_or(defaultSeconds);
}

// Something that happens at a simulated time: an attempt finishing, or a retry wait ending.
struct Timer {
  double t;
  std::uint64_t seq;  // insertion order, so equal times pop deterministically
  NodeId node;
  bool attemptEnds;   // true: attempt finished; false: RETRYING backoff is over
  bool success;
  int worker;
};
struct TimerLater {
  bool operator()(const Timer& a, const Timer& b) const {
    return std::tie(a.t, a.seq) > std::tie(b.t, b.seq);
  }
};

// The ready set, ordered by the policy. Key: (priority, tiebreak). Lowest key pops first.
class ReadyQueue {
 public:
  ReadyQueue(Policy p, const std::vector<double>& rank) : policy_(p), rank_(rank) {}

  void push(NodeId n) {
    double primary = policy_ == Policy::Fifo ? static_cast<double>(arrival_++) : -rank_.at(n);
    q_.push({primary, n});
  }
  bool empty() const { return q_.empty(); }
  NodeId pop() {
    NodeId n = q_.top().second;
    q_.pop();
    return n;
  }

 private:
  using Item = std::pair<double, NodeId>;  // (primary key, plan order)
  Policy policy_;
  const std::vector<double>& rank_;
  std::uint64_t arrival_ = 0;
  std::priority_queue<Item, std::vector<Item>, std::greater<Item>> q_;
};

}  // namespace

std::vector<double> criticalPathRank(const Dag& dag, double defaultSeconds) {
  std::vector<double> rank(dag.size(), 0.0);
  const auto& topo = dag.topologicalOrder();
  for (auto it = topo.rbegin(); it != topo.rend(); ++it) {
    double below = 0.0;
    for (NodeId c : dag.children(*it)) below = std::max(below, rank[c]);
    rank[*it] = durationOf(dag.task(*it), defaultSeconds) + below;
  }
  return rank;
}

SimResult Simulator::run(const Dag& dag, const SimOptions& opts) {
  if (opts.workers < 1) throw std::invalid_argument("workers must be >= 1");
  if (opts.maxAttempts < 1) throw std::invalid_argument("maxAttempts must be >= 1");

  ExecutionState state(dag);
  const std::vector<double> rank = criticalPathRank(dag, opts.defaultSeconds);
  ReadyQueue ready(opts.policy, rank);
  std::priority_queue<Timer, std::vector<Timer>, TimerLater> timers;
  std::uint64_t seq = 0;

  std::mt19937_64 rng(opts.seed);
  std::bernoulli_distribution coin(std::clamp(opts.failRate, 0.0, 1.0));

  SimResult result;
  result.tasks.resize(dag.size());
  for (NodeId n = 0; n < dag.size(); ++n) result.tasks[n].id = dag.id(n);

  std::vector<int> freeWorkers;  // kept sorted descending so back() is the lowest id
  for (int w = opts.workers - 1; w >= 0; --w) freeWorkers.push_back(w);

  double now = 0.0;
  auto log = [&](NodeId n, std::string ev, int attempt, int worker) {
    result.events.push_back({now, dag.id(n), std::move(ev), attempt, worker});
  };

  for (NodeId n : state.start()) ready.push(n);

  while (true) {
    // 1. Give every free worker a READY task, in policy order.
    while (!freeWorkers.empty() && !ready.empty()) {
      NodeId n = ready.pop();
      int w = freeWorkers.back();
      freeWorkers.pop_back();

      int attempt = state.onDispatched(n);
      state.onStarted(n);  // the simulated worker starts at once

      auto ff = opts.failFirst.find(dag.id(n));
      bool fails = (ff != opts.failFirst.end() && attempt <= ff->second) || coin(rng);
      double end = now + durationOf(dag.task(n), opts.defaultSeconds);
      timers.push({end, seq++, n, true, !fails, w});

      SimTaskResult& r = result.tasks[n];
      r.attempts = attempt;
      r.start = now;
      r.worker = w;
      log(n, "dispatched", attempt, w);
    }

    if (timers.empty()) break;  // nothing running, nothing waiting to retry

    // 2. Jump the clock to the next thing that happens.
    Timer tm = timers.top();
    timers.pop();
    now = tm.t;

    if (!tm.attemptEnds) {  // a retry's backoff is over: back in the ready set
      ready.push(tm.node);
      continue;
    }

    freeWorkers.push_back(tm.worker);
    std::sort(freeWorkers.rbegin(), freeWorkers.rend());
    SimTaskResult& r = result.tasks[tm.node];
    r.end = now;
    int attempt = state.attempts(tm.node);

    if (tm.success) {
      log(tm.node, "completed", attempt, tm.worker);
      for (NodeId c : state.onCompleted(tm.node)) ready.push(c);
    } else if (attempt < opts.maxAttempts) {
      state.onRetry(tm.node);
      ++result.retries;
      log(tm.node, "retrying", attempt, tm.worker);
      double wait = opts.backoffSeconds * std::pow(2.0, attempt - 1);
      timers.push({now + wait, seq++, tm.node, false, false, -1});
    } else {
      log(tm.node, "failed", attempt, tm.worker);
      for (NodeId c : state.onFailed(tm.node)) log(c, "cancelled", 0, -1);
    }
  }

  if (!state.isDone()) throw std::logic_error("simulator stopped with unfinished tasks");
  for (NodeId n = 0; n < dag.size(); ++n) result.tasks[n].state = state.state(n);
  result.succeeded = state.succeeded();
  result.makespan = now;
  return result;
}

}  // namespace ats
