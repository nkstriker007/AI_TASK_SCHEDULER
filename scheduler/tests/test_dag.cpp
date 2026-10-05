#include <algorithm>
#include <string>

#include "Helpers.hpp"

using namespace ats;
using namespace ats::test;
using nlohmann::json;
using ::testing::ElementsAre;
using ::testing::UnorderedElementsAre;

namespace {
// Every edge goes forward in the topological order.
void expectValidTopo(const Dag& g) {
  const auto& order = g.topologicalOrder();
  ASSERT_EQ(order.size(), g.size());
  std::vector<std::size_t> pos(g.size());
  for (std::size_t i = 0; i < order.size(); ++i) pos[order[i]] = i;
  for (NodeId v = 0; v < g.size(); ++v)
    for (NodeId c : g.children(v)) EXPECT_LT(pos[v], pos[c]) << g.id(v) << " -> " << g.id(c);
}
}  // namespace

TEST(Dag, Chain) {
  Dag g = mustBuild(planFile({task("t1"), task("t2", {"t1"}), task("t3", {"t2"})}));
  EXPECT_THAT(ids(g, g.roots()), ElementsAre("t1"));
  EXPECT_THAT(ids(g, g.sinks()), ElementsAre("t3"));
  EXPECT_EQ(g.indegree(node(g, "t3")), 1u);
  expectValidTopo(g);
}

TEST(Dag, ForkJoinDiamondIndegrees) {
  Dag g = mustBuild(planFile({task("t1"), task("t2", {"t1"}), task("t3", {"t1"}), task("t4", {"t2", "t3"})}));
  EXPECT_THAT(ids(g, g.children(node(g, "t1"))), ElementsAre("t2", "t3"));
  EXPECT_THAT(ids(g, g.parents(node(g, "t4"))), ElementsAre("t2", "t3"));
  EXPECT_EQ(g.indegree(node(g, "t4")), 2u);
  expectValidTopo(g);
}

TEST(Dag, DisconnectedComponentsAndOutOfOrderDeclaration) {
  // t2 is declared before its dependency t3; order in the file must not matter.
  Dag g = mustBuild(planFile({task("t1"), task("t2", {"t3"}), task("t3"), task("t4", {"t1"})}));
  EXPECT_THAT(ids(g, g.roots()), ElementsAre("t1", "t3"));
  EXPECT_THAT(ids(g, g.sinks()), ElementsAre("t2", "t4"));
  expectValidTopo(g);
}

TEST(Dag, TransitiveEdgesAreAllowed) {
  // Compare consumes both raw research and the summary: a legitimate data edge (warning in Python only).
  Dag g = mustBuild(planFile({task("t1"), task("t2", {"t1"}), task("t3", {"t1", "t2"})}));
  EXPECT_EQ(g.indegree(node(g, "t3")), 2u);
}

TEST(Dag, UnknownDependency) {
  auto issues = buildIssues(planFile({task("t1"), task("t2", {"t9"})}));
  ASSERT_THAT(codesOf(issues), ElementsAre("UNKNOWN_DEPENDENCY"));
  EXPECT_EQ(issues[0].taskId, "t2");
  EXPECT_NE(issues[0].message.find("t9"), std::string::npos);
}

TEST(Dag, SelfDependency) {
  EXPECT_THAT(codesOf(buildIssues(planFile({task("t1", {"t1"})}))), ElementsAre("SELF_DEPENDENCY"));
}

TEST(Dag, DuplicateDependency) {
  EXPECT_THAT(codesOf(buildIssues(planFile({task("t1"), task("t2", {"t1", "t1"})}))),
              ElementsAre("DUPLICATE_DEPENDENCY"));
}

TEST(Dag, DuplicateId) {
  auto issues = buildIssues(planFile({task("t1"), task("t1", {}, "summarize")}));
  ASSERT_THAT(codesOf(issues), ElementsAre("DUPLICATE_ID"));
  EXPECT_EQ(issues[0].taskId, "t1");
}

TEST(Dag, CycleReportsTheCycleNotDownstreamTasks) {
  auto issues = buildIssues(planFile({task("t1", {"t3"}), task("t2", {"t1"}), task("t3", {"t2"}), task("t4", {"t3"})}));
  ASSERT_THAT(codesOf(issues), ElementsAre("CYCLE"));
  EXPECT_EQ(issues[0].taskId, "t1");
  EXPECT_NE(issues[0].message.find("t1 -> t2 -> t3 -> t1"), std::string::npos) << issues[0].message;
  EXPECT_NE(issues[0].message.find("1 more task(s) blocked"), std::string::npos) << issues[0].message;
}

TEST(Dag, TwoNodeCycleAndCycleBehindValidPrefix) {
  auto issues = buildIssues(planFile({task("t1"), task("t2", {"t1", "t3"}), task("t3", {"t2"})}));
  ASSERT_THAT(codesOf(issues), ElementsAre("CYCLE"));
  EXPECT_NE(issues[0].message.find("t2 -> t3 -> t2"), std::string::npos) << issues[0].message;
}

TEST(Dag, ReportsEveryStructuralProblemAtOnce) {
  auto issues = buildIssues(planFile({task("t1", {"t1"}), task("t2", {"t9"}), task("t2"),
                                      task("t3", {"t4"}), task("t4", {"t3"})}));
  EXPECT_THAT(codesOf(issues),
              UnorderedElementsAre("DUPLICATE_ID", "SELF_DEPENDENCY", "UNKNOWN_DEPENDENCY", "CYCLE"));
}

TEST(Dag, FindAndAccessors) {
  Dag g = mustBuild(planFile({task("t1"), task("t2", {"t1"}, "report")}));
  EXPECT_EQ(g.requestId(), "req_test");
  EXPECT_FALSE(g.find("t9").has_value());
  EXPECT_EQ(g.task(node(g, "t2")).type, TaskType::Report);
}

TEST(Dag, LargeWideGraph) {
  json tasks = json::array();
  std::vector<std::string> all;
  for (int i = 1; i <= 49; ++i) {
    tasks.push_back(task("t" + std::to_string(i)));
    all.push_back("t" + std::to_string(i));
  }
  tasks.push_back(task("t50", all, "report"));
  Dag g = mustBuild(planFile(tasks));
  EXPECT_EQ(g.roots().size(), 49u);
  EXPECT_EQ(g.indegree(node(g, "t50")), 49u);
  expectValidTopo(g);
}
