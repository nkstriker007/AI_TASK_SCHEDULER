#include <algorithm>
#include <random>
#include <stdexcept>
#include <string>

#include "Helpers.hpp"
#include "ats/ExecutionState.hpp"

using namespace ats;
using namespace ats::test;
using nlohmann::json;
using ::testing::ElementsAre;
using ::testing::IsEmpty;
using ::testing::UnorderedElementsAre;

namespace {
// Dispatch + start + complete in one go; returns ids of newly ready tasks.
std::vector<std::string> runToCompletion(ExecutionState& s, const Dag& g, const std::string& id) {
  NodeId n = node(g, id);
  s.onDispatched(n);
  s.onStarted(n);
  return ids(g, s.onCompleted(n));
}
}  // namespace

TEST(ExecutionState, ChainUnlocksOneAtATime) {
  Dag g = mustBuild(planFile({task("t1"), task("t2", {"t1"}), task("t3", {"t2"})}));
  ExecutionState s(g);
  EXPECT_THAT(ids(g, s.start()), ElementsAre("t1"));
  EXPECT_EQ(s.state(node(g, "t2")), TaskState::Waiting);
  EXPECT_THAT(runToCompletion(s, g, "t1"), ElementsAre("t2"));
  EXPECT_THAT(runToCompletion(s, g, "t2"), ElementsAre("t3"));
  EXPECT_FALSE(s.isDone());
  EXPECT_THAT(runToCompletion(s, g, "t3"), IsEmpty());
  EXPECT_TRUE(s.isDone());
  EXPECT_TRUE(s.succeeded());
}

TEST(ExecutionState, ForkReleasesBothChildrenTogether) {
  Dag g = mustBuild(planFile({task("t1"), task("t2", {"t1"}), task("t3", {"t1"})}));
  ExecutionState s(g);
  s.start();
  EXPECT_THAT(runToCompletion(s, g, "t1"), ElementsAre("t2", "t3"));
}

TEST(ExecutionState, JoinFiresExactlyOnceAfterLastParent) {
  Dag g = mustBuild(planFile({task("t1"), task("t2"), task("t3", {"t1", "t2"})}));
  ExecutionState s(g);
  EXPECT_THAT(ids(g, s.start()), ElementsAre("t1", "t2"));
  EXPECT_THAT(runToCompletion(s, g, "t2"), IsEmpty());   // never after just one parent
  EXPECT_EQ(s.unresolved(node(g, "t3")), 1u);
  EXPECT_EQ(s.state(node(g, "t3")), TaskState::Waiting);
  EXPECT_THAT(runToCompletion(s, g, "t1"), ElementsAre("t3"));
  EXPECT_EQ(s.unresolved(node(g, "t3")), 0u);
}

TEST(ExecutionState, DiamondDoesNotUnlockEarly) {
  Dag g = mustBuild(planFile({task("t1"), task("t2", {"t1"}), task("t3", {"t1"}), task("t4", {"t2", "t3"})}));
  ExecutionState s(g);
  s.start();
  runToCompletion(s, g, "t1");
  EXPECT_THAT(runToCompletion(s, g, "t3"), IsEmpty());
  EXPECT_THAT(runToCompletion(s, g, "t2"), ElementsAre("t4"));
}

TEST(ExecutionState, CompletionFromQueuedIsAllowed) {
  // The worker's "started" event may be lost or arrive after "completed".
  Dag g = mustBuild(planFile({task("t1"), task("t2", {"t1"})}));
  ExecutionState s(g);
  s.start();
  s.onDispatched(node(g, "t1"));
  EXPECT_THAT(ids(g, s.onCompleted(node(g, "t1"))), ElementsAre("t2"));
}

TEST(ExecutionState, RetryIncrementsAttempts) {
  Dag g = mustBuild(planFile({task("t1"), task("t2", {"t1"})}));
  ExecutionState s(g);
  s.start();
  NodeId t1 = node(g, "t1");
  EXPECT_EQ(s.onDispatched(t1), 1);
  s.onStarted(t1);
  s.onRetry(t1);
  EXPECT_EQ(s.state(t1), TaskState::Retrying);
  EXPECT_EQ(s.onDispatched(t1), 2);
  EXPECT_EQ(s.attempts(t1), 2);
  s.onRetry(t1);                         // lost from QUEUED (e.g. deadline expired before start)
  EXPECT_EQ(s.onDispatched(t1), 3);
  s.onStarted(t1);
  EXPECT_THAT(ids(g, s.onCompleted(t1)), ElementsAre("t2"));
}

TEST(ExecutionState, FailureCancelsAllDescendantsButNotIndependentBranches) {
  //   t1 -> t3 -> t5
  //   t2 -> t4 --^      t6 independent
  Dag g = mustBuild(planFile({task("t1"), task("t2"), task("t3", {"t1"}), task("t4", {"t2"}),
                              task("t5", {"t3", "t4"}), task("t6")}));
  ExecutionState s(g);
  s.start();
  NodeId t1 = node(g, "t1");
  s.onDispatched(t1);
  s.onStarted(t1);
  EXPECT_THAT(ids(g, s.onFailed(t1)), UnorderedElementsAre("t3", "t5"));
  EXPECT_EQ(s.state(t1), TaskState::Failed);
  EXPECT_FALSE(s.isDone());

  // The independent branch keeps going; t4 finishing must not revive cancelled t5.
  runToCompletion(s, g, "t2");
  EXPECT_THAT(runToCompletion(s, g, "t4"), IsEmpty());
  EXPECT_EQ(s.state(node(g, "t5")), TaskState::Cancelled);
  runToCompletion(s, g, "t6");
  EXPECT_TRUE(s.isDone());
  EXPECT_FALSE(s.succeeded());
  EXPECT_EQ(s.count(TaskState::Completed), 3u);
  EXPECT_EQ(s.count(TaskState::Cancelled), 2u);
}

TEST(ExecutionState, FailAfterRetriesExhausted) {
  Dag g = mustBuild(planFile({task("t1"), task("t2", {"t1"})}));
  ExecutionState s(g);
  s.start();
  NodeId t1 = node(g, "t1");
  s.onDispatched(t1);
  s.onRetry(t1);
  EXPECT_THAT(ids(g, s.onFailed(t1)), ElementsAre("t2"));   // RETRYING -> FAILED is allowed
  EXPECT_TRUE(s.isDone());
}

TEST(ExecutionState, IllegalTransitionsThrow) {
  Dag g = mustBuild(planFile({task("t1"), task("t2", {"t1"})}));
  ExecutionState s(g);
  NodeId t1 = node(g, "t1"), t2 = node(g, "t2");
  EXPECT_THROW(s.onDispatched(t1), std::logic_error);   // before start()
  s.start();
  EXPECT_THROW(s.start(), std::logic_error);
  EXPECT_THROW(s.onDispatched(t2), std::logic_error);   // still WAITING
  EXPECT_THROW(s.onStarted(t1), std::logic_error);      // READY, not QUEUED
  EXPECT_THROW(s.onCompleted(t1), std::logic_error);    // never dispatched
  EXPECT_THROW(s.onRetry(t1), std::logic_error);
  s.onDispatched(t1);
  EXPECT_THROW(s.onDispatched(t1), std::logic_error);   // double dispatch
  s.onStarted(t1);
  s.onCompleted(t1);
  EXPECT_THROW(s.onCompleted(t1), std::logic_error);    // duplicate completion: the service must dedupe
  EXPECT_THROW(s.onFailed(t1), std::logic_error);
}

TEST(ExecutionState, NotDoneBeforeStart) {
  Dag g = mustBuild(planFile({task("t1")}));
  ExecutionState s(g);
  EXPECT_FALSE(s.isDone());
  EXPECT_FALSE(s.succeeded());
}

// Property test: random DAGs executed in random order. A task becomes READY exactly once,
// only after every parent is COMPLETED, and every task completes.
TEST(ExecutionState, RandomDagsRandomCompletionOrder) {
  std::mt19937 rng(12345);
  for (int trial = 0; trial < 300; ++trial) {
    const int n = 1 + static_cast<int>(rng() % 40);
    json tasks = json::array();
    for (int i = 1; i <= n; ++i) {
      std::vector<std::string> deps;
      for (int j = 1; j < i; ++j)
        if (rng() % 5 == 0) deps.push_back("t" + std::to_string(j));
      tasks.push_back(task("t" + std::to_string(i), deps));
    }
    Dag g = mustBuild(planFile(tasks));
    ExecutionState s(g);
    std::vector<NodeId> ready = s.start();
    std::vector<int> timesReady(g.size(), 0);
    for (NodeId r : ready) ++timesReady[r];
    std::vector<NodeId> running;
    while (!ready.empty() || !running.empty()) {
      // Randomly either dispatch a ready task or complete a running one.
      if (!ready.empty() && (running.empty() || rng() % 2 == 0)) {
        std::size_t k = rng() % ready.size();
        NodeId v = ready[k];
        ready.erase(ready.begin() + static_cast<std::ptrdiff_t>(k));
        s.onDispatched(v);
        s.onStarted(v);
        running.push_back(v);
      } else {
        std::size_t k = rng() % running.size();
        NodeId v = running[k];
        running.erase(running.begin() + static_cast<std::ptrdiff_t>(k));
        for (NodeId c : s.onCompleted(v)) {
          ++timesReady[c];
          for (NodeId p : g.parents(c)) ASSERT_EQ(s.state(p), TaskState::Completed);
          ready.push_back(c);
        }
      }
    }
    ASSERT_TRUE(s.succeeded()) << "trial " << trial;
    for (NodeId v = 0; v < g.size(); ++v) ASSERT_EQ(timesReady[v], 1) << g.id(v);
  }
}
