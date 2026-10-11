#include <algorithm>
#include <cmath>
#include <map>
#include <random>
#include <stdexcept>
#include <string>

#include "Helpers.hpp"
#include "ats/Analysis.hpp"
#include "ats/Simulator.hpp"

using namespace ats;
using namespace ats::test;
using nlohmann::json;
using ::testing::ElementsAre;
using ::testing::UnorderedElementsAre;

namespace {
json timed(const std::string& id, std::vector<std::string> deps, double seconds) {
  json t = task(id, std::move(deps));
  t["hints"] = json::object();
  t["hints"]["estimated_seconds"] = seconds;
  return t;
}

Dag companyResearch() {
  return mustBuild(planFile({timed("t1", {}, 20), timed("t2", {}, 25), timed("t3", {"t1"}, 10),
                             timed("t4", {"t2"}, 10), timed("t5", {"t3", "t4"}, 15), timed("t6", {"t5"}, 15)}));
}

// 15 short tasks + one 10x straggler, all feeding a join.
Dag straggler() {
  json tasks = json::array();
  std::vector<std::string> all;
  for (int i = 1; i <= 15; ++i) {
    tasks.push_back(timed("t" + std::to_string(i), {}, 0.3));
    all.push_back("t" + std::to_string(i));
  }
  tasks.push_back(timed("t16", {}, 3.0));
  all.push_back("t16");
  tasks.push_back(timed("t17", all, 0.3));
  return mustBuild(planFile(tasks));
}

SimConfig config(int workers, const std::string& policy = "fifo") {
  SimConfig c;
  c.workers = workers;
  c.policy = policy;
  return c;
}

const AttemptTiming& attemptOf(const Dag& g, const SimReport& r, const std::string& id, int attempt = 1) {
  for (const auto& a : r.attempts)
    if (g.id(a.node) == id && a.attempt == attempt) return a;
  throw std::runtime_error("no attempt " + std::to_string(attempt) + " for " + id);
}

constexpr double kEps = 1e-9;
}  // namespace

// ---------- worked example from the design document ----------

TEST(Simulator, CompanyResearchTwoWorkersMatchesTheDesignDoc) {
  Dag g = companyResearch();
  SimReport r = simulate(g, config(2));
  EXPECT_TRUE(r.succeeded);
  EXPECT_DOUBLE_EQ(r.makespan, 65.0);
  EXPECT_DOUBLE_EQ(r.sequentialSeconds, 95.0);
  EXPECT_DOUBLE_EQ(r.criticalPathSeconds, 65.0);
  EXPECT_NEAR(r.speedup, 95.0 / 65.0, kEps);
  EXPECT_NEAR(r.utilization, 95.0 / 130.0, kEps);
  EXPECT_EQ(r.peakParallelism, 2);
  EXPECT_THAT(ids(g, r.criticalPath), ElementsAre("t2", "t4", "t5", "t6"));

  // t3 starts the moment t1 finishes, while t2 is still running: no waiting for whole levels.
  EXPECT_DOUBLE_EQ(attemptOf(g, r, "t3").start, 20.0);
  EXPECT_DOUBLE_EQ(attemptOf(g, r, "t4").start, 25.0);
  EXPECT_DOUBLE_EQ(attemptOf(g, r, "t5").start, 35.0);
  EXPECT_DOUBLE_EQ(attemptOf(g, r, "t6").start, 50.0);
}

TEST(Simulator, OneWorkerIsSequential) {
  SimReport r = simulate(companyResearch(), config(1));
  EXPECT_DOUBLE_EQ(r.makespan, 95.0);
  EXPECT_DOUBLE_EQ(r.speedup, 1.0);
  EXPECT_DOUBLE_EQ(r.utilization, 1.0);
}

TEST(Simulator, UnlimitedWorkersHitTheCriticalPath) {
  SimReport r = simulate(companyResearch(), config(0));
  EXPECT_DOUBLE_EQ(r.makespan, 65.0);
  EXPECT_EQ(r.peakParallelism, 2);
}

// ---------- policies ----------

TEST(Simulator, CriticalPathPolicyBeatsFifoOnAStraggler) {
  // FIFO starts the straggler last; critical-path starts it first and hides it behind short tasks.
  Dag g = straggler();
  SimReport f = simulate(g, config(4, "fifo"));
  SimReport c = simulate(g, config(4, "critical-path"));
  EXPECT_NEAR(f.makespan, 4.2, 1e-9);
  EXPECT_NEAR(c.makespan, 3.3, 1e-9);
  EXPECT_NEAR(c.makespan, c.criticalPathSeconds, 1e-9);  // optimal here
  EXPECT_EQ(f.policy, "fifo");
  EXPECT_EQ(c.policy, "critical-path");
}

// ---------- fault injection ----------

TEST(Simulator, FailureCancelsDescendantsButIndependentWorkFinishes) {
  Dag g = companyResearch();
  SimConfig cfg = config(2);
  cfg.failTaskIds = {"t1"};
  SimReport r = simulate(g, cfg);
  EXPECT_FALSE(r.succeeded);
  EXPECT_EQ(r.failed, 1u);
  EXPECT_EQ(r.cancelled, 3u);   // t3, t5, t6
  EXPECT_EQ(r.completed, 2u);   // t2, t4
  std::vector<std::string> cancelled;
  for (const auto& e : r.events)
    if (e.kind == SimEventKind::Cancel) cancelled.push_back(g.id(e.node));
  EXPECT_THAT(cancelled, UnorderedElementsAre("t3", "t5", "t6"));
  EXPECT_DOUBLE_EQ(r.makespan, 35.0);  // t2 (25) + t4 (10) still run to completion
  EXPECT_EQ(r.speedup, 0.0);           // not reported: the run skipped cancelled work
}

TEST(Simulator, FlakyTaskIsRetriedAndTheRunStillSucceeds) {
  Dag g = companyResearch();
  SimConfig cfg = config(2);
  cfg.flakyTaskIds = {"t2"};
  SimReport r = simulate(g, cfg);
  EXPECT_TRUE(r.succeeded);
  EXPECT_EQ(r.retries, 1u);
  const auto& first = attemptOf(g, r, "t2", 1);
  const auto& second = attemptOf(g, r, "t2", 2);
  EXPECT_EQ(first.outcome, SimEventKind::Retry);
  EXPECT_EQ(second.outcome, SimEventKind::Complete);
  EXPECT_GE(second.start, first.end);
  EXPECT_DOUBLE_EQ(r.busySeconds, 95.0 + 25.0);  // the failed attempt cost real worker time
  EXPECT_GT(r.makespan, 65.0);
}

TEST(Simulator, RejectsBadConfiguration) {
  Dag g = companyResearch();
  SimConfig cfg = config(2);
  cfg.failTaskIds = {"t99"};
  EXPECT_THROW(simulate(g, cfg), std::invalid_argument);
  EXPECT_THROW(simulate(g, config(-1)), std::invalid_argument);
  EXPECT_THROW(simulate(g, config(2, "random")), std::invalid_argument);
  SimConfig j = config(2);
  j.jitter = 1.0;
  EXPECT_THROW(simulate(g, j), std::invalid_argument);
}

// ---------- jitter and determinism ----------

TEST(Simulator, JitterIsDeterministicPerSeedAndBounded) {
  Dag g = companyResearch();
  SimConfig a = config(2);
  a.jitter = 0.3;
  a.seed = 7;
  SimReport r1 = simulate(g, a), r2 = simulate(g, a);
  EXPECT_EQ(r1.makespan, r2.makespan);
  ASSERT_EQ(r1.attempts.size(), r2.attempts.size());
  for (std::size_t i = 0; i < r1.attempts.size(); ++i) EXPECT_EQ(r1.attempts[i].end, r2.attempts[i].end);

  SimConfig b = a;
  b.seed = 8;
  EXPECT_NE(simulate(g, b).makespan, r1.makespan);

  const auto base = estimatedDurations(g, 10);
  for (const auto& at : r1.attempts) {
    const double d = at.end - at.start;
    EXPECT_GE(d, base[at.node] * 0.7 - kEps);
    EXPECT_LE(d, base[at.node] * 1.3 + kEps);
  }
}

// ---------- property test: scheduling theory on random graphs ----------
//
// For list scheduling with k identical workers, total work W and critical path C:
//   C <= makespan <= W                        (any schedule)
//   makespan == C            when k is unlimited
//   makespan == W            when k == 1
//   makespan <= W/k + (1 - 1/k) * C           (Graham's bound for greedy list scheduling)
TEST(Simulator, RandomGraphsRespectSchedulingBounds) {
  std::mt19937 rng(2026);
  int runs = 0;
  for (int trial = 0; trial < 150; ++trial) {
    const int n = 1 + static_cast<int>(rng() % 30);
    const int density = 2 + static_cast<int>(rng() % 6);
    json tasks = json::array();
    for (int i = 1; i <= n; ++i) {
      std::vector<std::string> deps;
      for (int j = 1; j < i; ++j)
        if (rng() % static_cast<unsigned>(density) == 0) deps.push_back("t" + std::to_string(j));
      tasks.push_back(timed("t" + std::to_string(i), deps, 0.25 * static_cast<double>(1 + rng() % 40)));
    }
    Dag g = mustBuild(planFile(tasks));

    for (const std::string policy : {"fifo", "critical-path"}) {
      for (int k : {0, 1, 2, 3, 5, 8}) {
        SimConfig cfg = config(k, policy);
        cfg.jitter = (trial % 3 == 0) ? 0.25 : 0.0;
        cfg.seed = static_cast<std::uint64_t>(trial);
        SimReport r = simulate(g, cfg);
        ++runs;
        const double W = r.sequentialSeconds, C = r.criticalPathSeconds, T = r.makespan;
        const double tol = 1e-9 * std::max(1.0, W);
        SCOPED_TRACE("trial " + std::to_string(trial) + " policy " + policy + " workers " + std::to_string(k));

        ASSERT_TRUE(r.succeeded);
        ASSERT_EQ(r.completed, g.size());
        EXPECT_GE(T, C - tol);
        EXPECT_LE(T, W + tol);
        if (k == 0) EXPECT_NEAR(T, C, tol);
        if (k == 1) EXPECT_NEAR(T, W, tol);
        if (k >= 1) {
          EXPECT_LE(T, W / k + (1.0 - 1.0 / k) * C + tol) << "Graham bound violated";
          EXPECT_LE(r.peakParallelism, k);
          EXPECT_LE(r.utilization, 1.0 + 1e-9);
        }

        // Dependencies: every task starts no earlier than each parent's completion.
        std::map<NodeId, double> doneAt;
        for (const auto& a : r.attempts)
          if (a.outcome == SimEventKind::Complete) doneAt[a.node] = a.end;
        for (const auto& a : r.attempts)
          for (NodeId p : g.parents(a.node)) EXPECT_GE(a.start, doneAt.at(p) - tol);

        // A worker never runs two attempts at once.
        if (k >= 1) {
          std::map<int, std::vector<std::pair<double, double>>> byWorker;
          for (const auto& a : r.attempts) byWorker[a.worker].push_back({a.start, a.end});
          for (auto& [w, spans] : byWorker) {
            EXPECT_LT(w, k);
            std::sort(spans.begin(), spans.end());
            for (std::size_t i = 1; i < spans.size(); ++i) EXPECT_GE(spans[i].first, spans[i - 1].second - tol);
          }
        }
      }
    }
  }
  EXPECT_EQ(runs, 150 * 2 * 6);
}
