#include <stdexcept>
#include <string>

#include "Helpers.hpp"
#include "ats/Analysis.hpp"

using namespace ats;
using namespace ats::test;
using nlohmann::json;
using ::testing::DoubleEq;
using ::testing::ElementsAre;

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
}  // namespace

TEST(Analysis, EstimatedDurationsUseHintsThenDefault) {
  Dag g = mustBuild(planFile({timed("t1", {}, 3.5), task("t2", {"t1"})}));
  EXPECT_THAT(estimatedDurations(g, 7.0), ElementsAre(3.5, 7.0));
  EXPECT_THROW(estimatedDurations(g, -1.0), std::invalid_argument);
}

TEST(Analysis, CriticalPathOfCompanyResearch) {
  Dag g = companyResearch();
  CriticalPath cp = criticalPath(g, estimatedDurations(g, 10));
  EXPECT_DOUBLE_EQ(cp.seconds, 65.0);  // t2 25 + t4 10 + t5 15 + t6 15
  EXPECT_THAT(ids(g, cp.path), ElementsAre("t2", "t4", "t5", "t6"));
}

TEST(Analysis, BottomLevels) {
  Dag g = companyResearch();
  EXPECT_THAT(bottomLevels(g, estimatedDurations(g, 10)), ElementsAre(60, 65, 40, 40, 30, 15));
}

TEST(Analysis, CriticalPathTieIsDeterministic) {
  Dag g = mustBuild(planFile({timed("t1", {}, 5), timed("t2", {}, 5), timed("t3", {"t1", "t2"}, 1)}));
  CriticalPath cp = criticalPath(g, estimatedDurations(g, 1));
  EXPECT_DOUBLE_EQ(cp.seconds, 6.0);
  EXPECT_THAT(ids(g, cp.path), ElementsAre("t1", "t3"));  // first listed parent wins the tie
}

TEST(Analysis, DisconnectedGraphPicksLongestComponent) {
  Dag g = mustBuild(planFile({timed("t1", {}, 1), timed("t2", {"t1"}, 1), timed("t3", {}, 5)}));
  CriticalPath cp = criticalPath(g, estimatedDurations(g, 1));
  EXPECT_DOUBLE_EQ(cp.seconds, 5.0);
  EXPECT_THAT(ids(g, cp.path), ElementsAre("t3"));
}

TEST(Analysis, PlanShape) {
  PlanShape chain = planShape(mustBuild(planFile({task("t1"), task("t2", {"t1"}), task("t3", {"t2"})})));
  EXPECT_EQ(chain.depth, 3u);
  EXPECT_EQ(chain.width, 1u);
  EXPECT_EQ(chain.edges, 2u);

  PlanShape company = planShape(companyResearch());
  EXPECT_EQ(company.depth, 4u);
  EXPECT_EQ(company.width, 2u);
  EXPECT_EQ(company.edges, 5u);
  EXPECT_THAT(company.level, ElementsAre(0u, 0u, 1u, 1u, 2u, 3u));
}

TEST(Analysis, SizeMismatchThrows) {
  Dag g = companyResearch();
  EXPECT_THROW(bottomLevels(g, {1.0}), std::invalid_argument);
  EXPECT_THROW(criticalPath(g, {}), std::invalid_argument);
}
