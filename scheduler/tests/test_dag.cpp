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

// ------------------------------------------------------------------------------------------
// Dynamic expansion (Day 6, design doc 8.11, D53)

namespace {

ats::TaskSpec sub(const std::string& id, std::vector<std::string> deps = {}, double secs = 5,
                  ats::TaskType type = ats::TaskType::Research) {
  ats::TaskSpec t;
  t.id = id;
  t.type = type;
  t.title = "Sub " + id;
  t.instruction = "Do " + id;
  t.dependsOn = std::move(deps);
  t.estimatedSeconds = secs;
  return t;
}

// t1 research, t2 expand, t3 compare(t1, t2), t4 report(t3)
ats::Dag expandPlan() {
  return mustBuild(planFile({task("t1"), task("t2", {}, "expand"), task("t3", {"t1", "t2"}, "compare"),
                             task("t4", {"t3"}, "report")}));
}

std::vector<std::string> depsOf(const ats::Dag& g, const std::string& id) {
  return ids(g, g.parents(node(g, id)));
}

}  // namespace

TEST(DagExpand, RenamesAddsAJoinAndRewiresTheDependents) {
  ats::Dag g = expandPlan();
  const NodeId t2 = node(g, "t2");
  auto r = g.expand(t2, {sub("s1"), sub("s2"), sub("s3", {"s1", "s2"})});
  ASSERT_TRUE(r.ok());
  EXPECT_EQ(ids(g, r.added), (std::vector<std::string>{"t2.1", "t2.2", "t2.3"}));
  ASSERT_TRUE(r.join);
  EXPECT_EQ(g.id(*r.join), "t2.join");
  EXPECT_TRUE(g.isJoin(*r.join));
  EXPECT_FALSE(g.isJoin(t2));
  EXPECT_EQ(ids(g, r.rewired), (std::vector<std::string>{"t3"}));

  EXPECT_EQ(ids(g, g.children(t2)), (std::vector<std::string>{"t2.1", "t2.2", "t2.3"}));
  EXPECT_EQ(depsOf(g, "t2.1"), (std::vector<std::string>{"t2"}));
  EXPECT_EQ(depsOf(g, "t2.3"), (std::vector<std::string>{"t2", "t2.1", "t2.2"}));
  EXPECT_EQ(depsOf(g, "t2.join"), (std::vector<std::string>{"t2.3"}));  // only the subplan's sink
  EXPECT_EQ(depsOf(g, "t3"), (std::vector<std::string>{"t1", "t2.join"}));
  EXPECT_EQ(g.task(node(g, "t3")).dependsOn, (std::vector<std::string>{"t1", "t2.join"}));
  EXPECT_EQ(g.task(node(g, "t2.3")).dependsOn, (std::vector<std::string>{"t2", "t2.1", "t2.2"}));
  EXPECT_EQ(g.task(*r.join).estimatedSeconds, 0.0);
  EXPECT_EQ(g.size(), 8u);
  EXPECT_EQ(g.topologicalOrder().size(), 8u);
  EXPECT_EQ(ids(g, g.sinks()), (std::vector<std::string>{"t4"}));
  // Existing node ids are unchanged.
  EXPECT_EQ(g.id(0), "t1");
  EXPECT_EQ(g.id(3), "t4");
}

TEST(DagExpand, IndependentSubtasksAllFeedTheJoin) {
  ats::Dag g = expandPlan();
  auto r = g.expand(node(g, "t2"), {sub("s1"), sub("s2"), sub("s3")});
  ASSERT_TRUE(r.ok());
  EXPECT_EQ(depsOf(g, "t2.join"), (std::vector<std::string>{"t2.1", "t2.2", "t2.3"}));
}

TEST(DagExpand, AnExpandTaskWithNoDependentsGetsAJoinThatIsASink) {
  ats::Dag g = mustBuild(planFile({task("t1", {}, "expand")}));
  auto r = g.expand(0, {sub("s1")});
  ASSERT_TRUE(r.ok());
  EXPECT_TRUE(r.rewired.empty());
  EXPECT_EQ(ids(g, g.sinks()), (std::vector<std::string>{"t1.join"}));
}

TEST(DagExpand, InvalidSubplansChangeNothing) {
  struct Case {
    const char* name;
    std::vector<ats::TaskSpec> subplan;
    const char* code;
  };
  std::vector<ats::TaskSpec> tooMany;
  for (int i = 1; i <= 21; ++i) tooMany.push_back(sub("s" + std::to_string(i)));
  const std::vector<Case> cases = {
      {"empty", {}, "TOO_MANY_TASKS"},
      {"over 20", tooMany, "TOO_MANY_TASKS"},
      {"nested expand", {sub("s1", {}, 5, ats::TaskType::Expand)}, "SCHEMA"},
      {"unknown local dep", {sub("s1", {"s9"})}, "UNKNOWN_DEPENDENCY"},
      {"dep outside the subplan", {sub("s1", {"t1"})}, "UNKNOWN_DEPENDENCY"},
      {"self dep", {sub("s1", {"s1"})}, "SELF_DEPENDENCY"},
      {"duplicate id", {sub("s1"), sub("s1")}, "DUPLICATE_ID"},
      {"cycle", {sub("s1", {"s2"}), sub("s2", {"s1"})}, "CYCLE"},
  };
  for (const auto& c : cases) {
    ats::Dag g = expandPlan();
    auto r = g.expand(node(g, "t2"), c.subplan);
    EXPECT_FALSE(r.ok()) << c.name;
    EXPECT_THAT(codesOf(r.issues), ::testing::Contains(c.code)) << c.name;
    for (const auto& i : r.issues) EXPECT_EQ(i.taskId, "t2") << c.name;
    EXPECT_EQ(g.size(), 4u) << c.name;
    EXPECT_EQ(depsOf(g, "t3"), (std::vector<std::string>{"t1", "t2"})) << c.name;
  }
}

TEST(DagExpand, OnlyExpandTasksAndOnlyOnce) {
  ats::Dag g = expandPlan();
  EXPECT_THAT(codesOf(g.expand(node(g, "t1"), {sub("s1")}).issues), ::testing::Contains("SCHEMA"));
  ASSERT_TRUE(g.expand(node(g, "t2"), {sub("s1")}).ok());
  auto again = g.expand(node(g, "t2"), {sub("s1")});
  EXPECT_FALSE(again.ok());
  EXPECT_EQ(g.size(), 6u);
  EXPECT_FALSE(g.expand(node(g, "t2.join"), {sub("s1")}).ok());  // a join is never expandable
}

TEST(Subplan, ParsesAValidSubplan) {
  auto r = ats::parseSubplan(
      R"({"tasks":[{"id":"s1","type":"research","title":"A","instruction":"Do A","depends_on":[],"hints":{"estimated_seconds":4}},
                   {"id":"s2","type":"summarize","title":"B","instruction":"Do B","depends_on":["s1"]}]})",
      "t2");
  ASSERT_TRUE(r.ok());
  ASSERT_EQ(r.tasks.size(), 2u);
  EXPECT_EQ(r.tasks[0].estimatedSeconds, 4.0);
  EXPECT_EQ(r.tasks[1].dependsOn, (std::vector<std::string>{"s1"}));
}

TEST(Subplan, RejectsSchemaProblemsAttributedToTheExpandTask) {
  const std::vector<std::string> bad = {
      "not json",
      R"([])",
      R"({"tasks":[]})",
      R"({"tasks":[{"id":"t1","type":"research","title":"A","instruction":"x"}]})",
      R"({"tasks":[{"id":"s1","type":"expand","title":"A","instruction":"x"}]})",
      R"({"tasks":[{"id":"s1","type":"research","title":"A","instruction":"x","hints":{"expected_fanout":2}}]})",
      R"({"tasks":[{"id":"s1","type":"research","title":"A","instruction":"x"}],"extra":1})",
  };
  for (const auto& text : bad) {
    auto r = ats::parseSubplan(text, "t2");
    EXPECT_FALSE(r.ok()) << text;
    EXPECT_TRUE(r.tasks.empty()) << text;
    for (const auto& i : r.issues) EXPECT_EQ(i.taskId, "t2") << text;
  }
}
