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
  auto d = s.dispatch_next();
  return d ? g.id(d->task) : "";
}
}  // namespace

TEST(Scheduler, DispatchesReadyTasksInPolicyOrder) {
  Dag g = mustBuild(planFile({task("t1"), task("t2"), task("t3", {"t1", "t2"})}));
  Scheduler s = fifo(g);
  s.start();
  EXPECT_EQ(s.ready_count(), 2u);
  EXPECT_EQ(next(s, g), "t1");
  EXPECT_EQ(next(s, g), "t2");
  EXPECT_EQ(s.in_flight_count(), 2u);
  EXPECT_FALSE(s.dispatch_next().has_value());  // t3 still waiting
}

TEST(Scheduler, CompletionReleasesChildrenIntoReadySet) {
  Dag g = mustBuild(planFile({task("t1"), task("t2", {"t1"}), task("t3", {"t1"})}));
  Scheduler s = fifo(g);
  s.start();
  auto d = s.dispatch_next();
  s.mark_started(d->task);
  EXPECT_THAT(ids(g, s.mark_completed(d->task)), ElementsAre("t2", "t3"));
  EXPECT_EQ(s.in_flight_count(), 0u);
  EXPECT_EQ(s.ready_count(), 2u);
}

TEST(Scheduler, RetryRequeuesWithNextAttempt) {
  Dag g = mustBuild(planFile({task("t1"), task("t2", {"t1"})}));
  Scheduler s = fifo(g);
  s.start();
  auto d1 = s.dispatch_next();
  EXPECT_EQ(d1->attempt, 1);
  s.mark_started(d1->task);
  s.mark_for_retry(d1->task);
  EXPECT_EQ(s.in_flight_count(), 0u);
  auto d2 = s.dispatch_next();
  ASSERT_TRUE(d2.has_value());
  EXPECT_EQ(g.id(d2->task), "t1");
  EXPECT_EQ(d2->attempt, 2);
  s.mark_started(d2->task);
  EXPECT_THAT(ids(g, s.mark_completed(d2->task)), ElementsAre("t2"));
}

TEST(Scheduler, FailFromRetryingDoesNotUnderflowInFlight) {
  Dag g = mustBuild(planFile({task("t1"), task("t2", {"t1"})}));
  Scheduler s = fifo(g);
  s.start();
  auto d = s.dispatch_next();
  s.mark_for_retry(d->task);                       // back in the ready set, not in flight
  EXPECT_THAT(ids(g, s.mark_failed(d->task)), ElementsAre("t2"));
  EXPECT_EQ(s.in_flight_count(), 0u);
  EXPECT_FALSE(s.dispatch_next().has_value());  // the failed task is skipped, not dispatched
  EXPECT_TRUE(s.is_done());
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
  while (auto d = s.dispatch_next()) {
    s.mark_started(d->task);
    s.mark_completed(d->task);
  }
  EXPECT_TRUE(s.is_done());
  EXPECT_TRUE(s.succeeded());
  EXPECT_EQ(s.execution_state().attempts(*g.find("t2")), 1);
}

TEST(Scheduler, RejectsNullPolicy) {
  Dag g = mustBuild(planFile({task("t1")}));
  EXPECT_THROW(Scheduler(g, nullptr), std::invalid_argument);
}
