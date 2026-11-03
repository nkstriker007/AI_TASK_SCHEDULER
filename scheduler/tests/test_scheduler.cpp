#include <stdexcept>

#include "Helpers.hpp"
#include "ats/Analysis.hpp"
#include "ats/Scheduler.hpp"

using namespace ats;
using namespace ats::test;
using nlohmann::json;
using ::testing::ElementsAre;
using ::testing::IsEmpty;

namespace {
Scheduler fifo(const Dag& g) { return Scheduler(g, std::make_unique<FifoPolicy>()); }
std::string next(Scheduler& s, const Dag& g) {
  auto d = s.dispatchNext();
  return d ? g.id(d->node) : "";
}
}  // namespace

TEST(Scheduler, DispatchesReadyTasksInPolicyOrder) {
  Dag g = mustBuild(planFile({task("t1"), task("t2"), task("t3", {"t1", "t2"})}));
  Scheduler s = fifo(g);
  s.start();
  EXPECT_EQ(s.readyCount(), 2u);
  EXPECT_EQ(next(s, g), "t1");
  EXPECT_EQ(next(s, g), "t2");
  EXPECT_EQ(s.inFlight(), 2u);
  EXPECT_FALSE(s.dispatchNext().has_value());  // t3 still waiting
}

TEST(Scheduler, CompletionReleasesChildrenIntoReadySet) {
  Dag g = mustBuild(planFile({task("t1"), task("t2", {"t1"}), task("t3", {"t1"})}));
  Scheduler s = fifo(g);
  s.start();
  auto d = s.dispatchNext();
  s.started(d->node);
  EXPECT_THAT(ids(g, s.complete(d->node)), ElementsAre("t2", "t3"));
  EXPECT_EQ(s.inFlight(), 0u);
  EXPECT_EQ(s.readyCount(), 2u);
}

TEST(Scheduler, RetryRequeuesWithNextAttempt) {
  Dag g = mustBuild(planFile({task("t1"), task("t2", {"t1"})}));
  Scheduler s = fifo(g);
  s.start();
  auto d1 = s.dispatchNext();
  EXPECT_EQ(d1->attempt, 1);
  s.started(d1->node);
  s.retry(d1->node);
  EXPECT_EQ(s.inFlight(), 0u);
  auto d2 = s.dispatchNext();
  ASSERT_TRUE(d2.has_value());
  EXPECT_EQ(g.id(d2->node), "t1");
  EXPECT_EQ(d2->attempt, 2);
  s.started(d2->node);
  EXPECT_THAT(ids(g, s.complete(d2->node)), ElementsAre("t2"));
}

TEST(Scheduler, FailFromRetryingDoesNotUnderflowInFlight) {
  Dag g = mustBuild(planFile({task("t1"), task("t2", {"t1"})}));
  Scheduler s = fifo(g);
  s.start();
  auto d = s.dispatchNext();
  s.retry(d->node);                       // back in the ready set, not in flight
  EXPECT_THAT(ids(g, s.fail(d->node)), ElementsAre("t2"));
  EXPECT_EQ(s.inFlight(), 0u);
  EXPECT_FALSE(s.dispatchNext().has_value());  // the failed task is skipped, not dispatched
  EXPECT_TRUE(s.isDone());
  EXPECT_FALSE(s.succeeded());
}

TEST(Scheduler, CriticalPathPolicyStartsTheLongChainFirst) {
  // t1 is a short leaf; t2 heads a long chain. FIFO would start t1 first, critical-path picks t2.
  json a = task("t1");
  a["hints"] = json::object({{"estimated_seconds", 1}});
  json b = task("t2");
  b["hints"] = json::object({{"estimated_seconds", 1}});
  json c = task("t3", {"t2"});
  c["hints"] = json::object({{"estimated_seconds", 10}});
  Dag g = mustBuild(planFile({a, b, c}));
  Scheduler s(g, makePolicy("critical-path", g, estimatedDurations(g, 1)));
  s.start();
  EXPECT_EQ(next(s, g), "t2");
  EXPECT_EQ(next(s, g), "t1");
}

TEST(Scheduler, RunsToCompletion) {
  Dag g = mustBuild(planFile({task("t1"), task("t2", {"t1"})}));
  Scheduler s = fifo(g);
  s.start();
  while (auto d = s.dispatchNext()) {
    s.started(d->node);
    s.complete(d->node);
  }
  EXPECT_TRUE(s.isDone());
  EXPECT_TRUE(s.succeeded());
  EXPECT_EQ(s.state().attempts(*g.find("t2")), 1);
}

TEST(Scheduler, RejectsNullPolicy) {
  Dag g = mustBuild(planFile({task("t1")}));
  EXPECT_THROW(Scheduler(g, nullptr), std::invalid_argument);
}

TEST(Scheduler, PeekNextMatchesDispatch) {
  using namespace ats::test;
  ats::Dag g = mustBuild(planFile({task("t1"), task("t2", {"t1"}), task("t3")}));
  ats::Scheduler s(g, std::make_unique<ats::FifoPolicy>());
  s.start();
  auto a = s.dispatchNext();
  ASSERT_TRUE(a);
  EXPECT_EQ(g.id(a->node), "t1");
  EXPECT_EQ(s.peekNext(), node(g, "t3"));
  EXPECT_EQ(s.dispatchNext()->node, node(g, "t3"));
  EXPECT_FALSE(s.peekNext());
}

TEST(Scheduler, SharedTicketsOrderReadyTasksAcrossSchedulers) {
  using namespace ats::test;
  ats::Dag g1 = mustBuild(planFile({task("t1"), task("t2", {"t1"})}));
  ats::Dag g2 = mustBuild(planFile({task("t1")}));
  auto tickets = std::make_shared<std::uint64_t>(0);
  ats::Scheduler s1(g1, std::make_unique<ats::FifoPolicy>(), tickets);
  ats::Scheduler s2(g2, std::make_unique<ats::FifoPolicy>(), tickets);
  s1.start();
  s2.start();
  EXPECT_LT(s1.readyTicket(node(g1, "t1")), s2.readyTicket(node(g2, "t1")));
  auto d = s1.dispatchNext();
  s1.complete(d->node);  // b becomes ready after x
  EXPECT_GT(s1.readyTicket(node(g1, "t2")), s2.readyTicket(node(g2, "t1")));
}

// ------------------------------------------------------------------------------------------
// Dynamic expansion (Day 6, D53)

namespace {

TaskSpec subTask(const std::string& id, std::vector<std::string> deps, double secs) {
  TaskSpec t;
  t.id = id;
  t.type = TaskType::Research;
  t.title = id;
  t.instruction = id;
  t.dependsOn = std::move(deps);
  t.estimatedSeconds = secs;
  return t;
}

}  // namespace

TEST(SchedulerExpand, SubplanRunsBetweenTheExpandTaskAndItsDependents) {
  Dag g = mustBuild(planFile({task("t1"), task("t2", {}, "expand"), task("t3", {"t1", "t2"}, "compare")}));
  Scheduler s(g, std::make_unique<FifoPolicy>());
  s.start();
  auto a = s.dispatchNext();
  auto b = s.dispatchNext();
  ASSERT_EQ(g.id(b->node), "t2");
  s.complete(a->node);  // t1
  auto r = g.expand(b->node, {subTask("s1", {}, 1), subTask("s2", {}, 1)});
  ASSERT_TRUE(r.ok());
  s.onExpanded(b->node, std::vector<double>(g.size(), 0.0));
  EXPECT_EQ(s.state().state(node(g, "t3")), TaskState::Waiting);
  EXPECT_THAT(ids(g, s.complete(b->node)), ElementsAre("t2.1", "t2.2"));
  auto x = s.dispatchNext();
  auto y = s.dispatchNext();
  EXPECT_THAT(s.complete(x->node), IsEmpty());
  EXPECT_THAT(ids(g, s.complete(y->node)), ElementsAre("t2.join"));
  auto j = s.dispatchNext();
  EXPECT_THAT(ids(g, s.complete(j->node)), ElementsAre("t3"));
  auto t3 = s.dispatchNext();
  s.complete(t3->node);
  EXPECT_TRUE(s.isDone());
  EXPECT_TRUE(s.succeeded());
}

TEST(SchedulerExpand, AFailedSubtaskCancelsTheJoinAndEverythingAfterIt) {
  Dag g = mustBuild(planFile({task("t1", {}, "expand"), task("t2", {"t1"}, "report")}));
  Scheduler s(g, std::make_unique<FifoPolicy>());
  s.start();
  auto e = s.dispatchNext();
  ASSERT_TRUE(g.expand(e->node, {subTask("s1", {}, 1), subTask("s2", {}, 1)}).ok());
  s.onExpanded(e->node, std::vector<double>(g.size(), 0.0));
  s.complete(e->node);
  auto x = s.dispatchNext();
  s.dispatchNext();
  EXPECT_THAT(ids(g, s.fail(x->node)), ElementsAre("t1.join", "t2"));
}

TEST(SchedulerExpand, CriticalPathReranksTheReadySetAfterASplice) {
  Dag g = mustBuild(planFile({task("t1", {}, "expand"), task("t2"), task("t3")}));
  std::vector<double> d(g.size(), 1.0);
  auto policy = std::make_unique<CriticalPathPolicy>(bottomLevels(g, d));
  CriticalPathPolicy* cp = policy.get();
  Scheduler s(g, std::move(policy));
  s.start();
  auto e = s.dispatchNext();  // t1 (ties in plan order); t2 and t3 ready
  ASSERT_EQ(g.id(e->node), "t1");
  ASSERT_TRUE(g.expand(e->node, {subTask("s1", {}, 30)}).ok());
  std::vector<double> durations = estimatedDurations(g, 1.0);
  durations[node(g, "t3")] = 50;  // pretend t3 became the longest
  s.onExpanded(e->node, bottomLevels(g, durations));
  EXPECT_DOUBLE_EQ(cp->priority(node(g, "t1.1")), 30.0);
  EXPECT_EQ(s.peekNext(), node(g, "t3"));  // re-ranked
  s.complete(e->node);
  EXPECT_EQ(g.id(s.dispatchNext()->node), "t3");
  EXPECT_EQ(g.id(s.dispatchNext()->node), "t1.1");  // 30 + join 0 beats t2's 1
}
