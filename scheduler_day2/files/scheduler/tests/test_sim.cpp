#include <algorithm>
#include <filesystem>
#include <fstream>
#include <map>
#include <random>
#include <string>

#include "Helpers.hpp"
#include "ats/Simulator.hpp"

using namespace ats;
using namespace ats::test;
using nlohmann::json;
using ::testing::DoubleEq;
using ::testing::ElementsAre;

namespace {

json timed(const std::string& id, double seconds, std::vector<std::string> deps = {}) {
  json t = task(id, std::move(deps));
  t["hints"] = {{"estimated_seconds", seconds}};
  return t;
}

const SimTaskResult& result(const SimResult& r, const std::string& id) {
  auto it = std::find_if(r.tasks.begin(), r.tasks.end(), [&](const SimTaskResult& t) { return t.id == id; });
  if (it == r.tasks.end()) throw std::runtime_error("no task " + id);
  return *it;
}

SimOptions withWorkers(int n, Policy p = Policy::CriticalPath) {
  SimOptions o;
  o.workers = n;
  o.policy = p;
  return o;
}

// Largest number of attempts running at the same simulated instant.
int peakConcurrency(const SimResult& r) {
  std::vector<std::pair<double, int>> edges;  // (time, +1 start / -1 end); ends sort before starts
  for (const auto& e : r.events) {
    if (e.event == "dispatched") edges.push_back({e.t, +1});
    if (e.event == "completed" || e.event == "retrying" || e.event == "failed") edges.push_back({e.t, -1});
  }
  std::sort(edges.begin(), edges.end());
  int now = 0, peak = 0;
  for (auto [t, d] : edges) peak = std::max(peak, now += d);
  return peak;
}

}  // namespace

TEST(Simulator, ChainRunsInOrderAndTakesTheSumOfDurations) {
  Dag g = mustBuild(planFile({timed("t1", 2), timed("t2", 3, {"t1"}), timed("t3", 4, {"t2"})}));
  SimResult r = Simulator::run(g, withWorkers(4));
  EXPECT_TRUE(r.succeeded);
  EXPECT_THAT(r.makespan, DoubleEq(9));
  EXPECT_THAT(result(r, "t2").start, DoubleEq(2));
  EXPECT_THAT(result(r, "t3").start, DoubleEq(5));
}

TEST(Simulator, IndependentTasksRunInParallel) {
  Dag g = mustBuild(planFile({timed("t1", 5), timed("t2", 7), timed("t3", 3), timed("t4", 1, {"t1", "t2", "t3"})}));
  SimResult r = Simulator::run(g, withWorkers(3));
  EXPECT_THAT(r.makespan, DoubleEq(8));  // max(5, 7, 3) + 1
  EXPECT_EQ(peakConcurrency(r), 3);
}

TEST(Simulator, NeverRunsMoreTasksThanWorkers) {
  Dag g = mustBuild(planFile({timed("t1", 2), timed("t2", 2), timed("t3", 2), timed("t4", 2)}));
  SimResult r = Simulator::run(g, withWorkers(1));
  EXPECT_THAT(r.makespan, DoubleEq(8));
  EXPECT_EQ(peakConcurrency(r), 1);
}

TEST(Simulator, ChildStartsOnlyAfterEveryParentCompleted) {
  Dag g = mustBuild(planFile({timed("t1", 1), timed("t2", 6), timed("t3", 1, {"t1", "t2"})}));
  SimResult r = Simulator::run(g, withWorkers(4));
  EXPECT_THAT(result(r, "t3").start, DoubleEq(6));
}

TEST(Simulator, MissingEstimateUsesDefaultSeconds) {
  Dag g = mustBuild(planFile({task("t1"), task("t2", {"t1"})}));
  SimOptions o = withWorkers(2);
  o.defaultSeconds = 2.5;
  EXPECT_THAT(Simulator::run(g, o).makespan, DoubleEq(5));
}

TEST(Simulator, CriticalPathBeatsFifoWhenTheLongChainIsListedLast) {
  // Two workers. FIFO starts the three short roots first and reaches the long chain late.
  Dag g = mustBuild(planFile({timed("t1", 1), timed("t2", 1), timed("t3", 1), timed("t4", 1),
                              timed("t5", 10, {"t4"})}));
  SimResult fifo = Simulator::run(g, withWorkers(2, Policy::Fifo));
  SimResult cp = Simulator::run(g, withWorkers(2, Policy::CriticalPath));
  EXPECT_THAT(fifo.makespan, DoubleEq(12));
  EXPECT_THAT(cp.makespan, DoubleEq(11));
  EXPECT_THAT(result(cp, "t4").start, DoubleEq(0));
}

TEST(Simulator, FifoTakesTasksInTheOrderTheyBecameReady) {
  Dag g = mustBuild(planFile({timed("t1", 1), timed("t2", 1), timed("t3", 1)}));
  SimResult r = Simulator::run(g, withWorkers(1, Policy::Fifo));
  std::vector<std::string> order;
  for (const auto& e : r.events)
    if (e.event == "dispatched") order.push_back(e.taskId);
  EXPECT_THAT(order, ElementsAre("t1", "t2", "t3"));
}

TEST(Simulator, CriticalPathRankIsLongestRemainingChain) {
  Dag g = mustBuild(planFile({timed("t1", 1), timed("t2", 4, {"t1"}), timed("t3", 2, {"t1"}), timed("t4", 3, {"t2", "t3"})}));
  std::vector<double> rank = criticalPathRank(g, 5);
  EXPECT_THAT(rank[node(g, "t4")], DoubleEq(3));
  EXPECT_THAT(rank[node(g, "t2")], DoubleEq(7));
  EXPECT_THAT(rank[node(g, "t3")], DoubleEq(5));
  EXPECT_THAT(rank[node(g, "t1")], DoubleEq(8));
}

TEST(Simulator, RetriesWithExponentialBackoffThenSucceeds) {
  Dag g = mustBuild(planFile({timed("t1", 1)}));
  SimOptions o = withWorkers(1);
  o.failFirst["t1"] = 2;
  o.maxAttempts = 3;
  o.backoffSeconds = 1;
  SimResult r = Simulator::run(g, o);
  EXPECT_TRUE(r.succeeded);
  EXPECT_EQ(result(r, "t1").attempts, 3);
  EXPECT_EQ(r.retries, 2u);
  // attempt 1: 0-1, wait 1, attempt 2: 2-3, wait 2, attempt 3: 5-6
  EXPECT_THAT(result(r, "t1").start, DoubleEq(5));
  EXPECT_THAT(r.makespan, DoubleEq(6));
}

TEST(Simulator, AWorkerIsFreeDuringBackoff) {
  Dag g = mustBuild(planFile({timed("t1", 1), timed("t2", 1)}));
  SimOptions o = withWorkers(1);
  o.failFirst["t1"] = 1;
  o.backoffSeconds = 10;
  SimResult r = Simulator::run(g, o);
  EXPECT_THAT(result(r, "t2").start, DoubleEq(1));   // runs while t1 waits
  EXPECT_THAT(result(r, "t1").start, DoubleEq(11));
}

TEST(Simulator, ExhaustedRetriesFailTheTaskAndCancelOnlyItsDescendants) {
  Dag g = mustBuild(planFile({timed("t1", 1), timed("t2", 1, {"t1"}), timed("t3", 1, {"t2"}),
                              timed("t4", 1), timed("t5", 1, {"t4"})}));
  SimOptions o = withWorkers(2);
  o.failFirst["t1"] = 99;
  o.maxAttempts = 2;
  SimResult r = Simulator::run(g, o);
  EXPECT_FALSE(r.succeeded);
  EXPECT_EQ(result(r, "t1").state, TaskState::Failed);
  EXPECT_EQ(result(r, "t1").attempts, 2);
  EXPECT_EQ(result(r, "t2").state, TaskState::Cancelled);
  EXPECT_EQ(result(r, "t3").state, TaskState::Cancelled);
  EXPECT_EQ(result(r, "t5").state, TaskState::Completed);
  EXPECT_EQ(result(r, "t2").attempts, 0);   // never dispatched
}

TEST(Simulator, SameSeedGivesTheSameRun) {
  Dag g = mustBuild(planFile({timed("t1", 1), timed("t2", 2), timed("t3", 1, {"t1", "t2"}), timed("t4", 3, {"t3"})}));
  SimOptions o = withWorkers(2);
  o.failRate = 0.5;
  o.seed = 42;
  SimResult a = Simulator::run(g, o), b = Simulator::run(g, o);
  ASSERT_EQ(a.events.size(), b.events.size());
  for (std::size_t i = 0; i < a.events.size(); ++i) {
    EXPECT_EQ(a.events[i].taskId, b.events[i].taskId);
    EXPECT_EQ(a.events[i].event, b.events[i].event);
    EXPECT_THAT(a.events[i].t, DoubleEq(b.events[i].t));
  }
}

TEST(Simulator, RejectsBadOptions) {
  Dag g = mustBuild(planFile({task("t1")}));
  SimOptions o;
  o.workers = 0;
  EXPECT_THROW(Simulator::run(g, o), std::invalid_argument);
  o = SimOptions{};
  o.maxAttempts = 0;
  EXPECT_THROW(Simulator::run(g, o), std::invalid_argument);
}

TEST(Simulator, EveryValidFixtureRunsToCompletion) {
  std::ifstream in(ATS_EXPECTED_VALIDATION);
  json expected = json::parse(in);
  int checked = 0;
  for (const auto& [name, exp] : expected.items()) {
    if (!exp.at("valid").get<bool>()) continue;
    LoadResult l = loadPlanFile(std::filesystem::path(ATS_FIXTURES_DIR) / (name + ".json"));
    ASSERT_TRUE(l.ok()) << name;
    Dag::BuildResult b = Dag::build(*l.plan);
    ASSERT_TRUE(b.dag) << name;
    for (Policy p : {Policy::Fifo, Policy::CriticalPath}) {
      SimResult r = Simulator::run(*b.dag, withWorkers(2, p));
      EXPECT_TRUE(r.succeeded) << name;
      EXPECT_LE(peakConcurrency(r), 2) << name;
    }
    ++checked;
  }
  EXPECT_GE(checked, 7);
}

// Property test: random DAGs, worker counts and failure rates. Whatever happens, the run
// ends with every task terminal, never exceeds the worker count, never starts a task before
// all its parents completed.
TEST(Simulator, RandomPlansKeepEveryInvariant) {
  std::mt19937 rng(7);
  for (int round = 0; round < 300; ++round) {
    int n = std::uniform_int_distribution<int>(1, 25)(rng);
    json tasks = json::array();
    for (int i = 1; i <= n; ++i) {
      std::vector<std::string> deps;
      for (int j = 1; j < i; ++j)
        if (std::bernoulli_distribution(0.2)(rng)) deps.push_back("t" + std::to_string(j));
      tasks.push_back(timed("t" + std::to_string(i), std::uniform_int_distribution<int>(0, 9)(rng), deps));
    }
    Dag g = mustBuild(planFile(tasks));
    SimOptions o = withWorkers(std::uniform_int_distribution<int>(1, 5)(rng),
                               round % 2 ? Policy::Fifo : Policy::CriticalPath);
    o.failRate = std::uniform_real_distribution<double>(0, 0.5)(rng);
    o.maxAttempts = std::uniform_int_distribution<int>(1, 3)(rng);
    o.seed = static_cast<std::uint64_t>(round);
    SimResult r = Simulator::run(g, o);

    ASSERT_LE(peakConcurrency(r), o.workers) << "round " << round;
    bool allCompleted = true;
    for (NodeId v = 0; v < g.size(); ++v) {
      const SimTaskResult& t = r.tasks[v];
      ASSERT_TRUE(isTerminal(t.state)) << "round " << round;
      allCompleted = allCompleted && t.state == TaskState::Completed;
      if (t.start >= 0) {
        for (NodeId p : g.parents(v)) {
          ASSERT_EQ(r.tasks[p].state, TaskState::Completed) << "round " << round;
          ASSERT_LE(r.tasks[p].end, t.start) << "round " << round;
        }
      }
    }
    ASSERT_EQ(r.succeeded, allCompleted) << "round " << round;
  }
}
