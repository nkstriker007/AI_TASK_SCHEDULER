#include <string>

#include "Helpers.hpp"

using namespace ats;
using namespace ats::test;
using nlohmann::json;
using ::testing::ElementsAre;

namespace {
LoadResult load(const json& j) { return parsePlanFile(j.dump()); }
}  // namespace

TEST(PlanLoader, LoadsValidPlanWithAllFields) {
  json t = task("t1");
  t["hints"] = {{"estimated_seconds", 12.5}};
  json f = planFile({t, task("t2", {"t1"}, "summarize")});
  LoadResult r = load(f);
  ASSERT_TRUE(r.ok());
  EXPECT_EQ(r.plan->requestId, "req_test");
  ASSERT_EQ(r.plan->tasks.size(), 2u);
  EXPECT_EQ(r.plan->tasks[0].estimatedSeconds, 12.5);
  EXPECT_EQ(r.plan->tasks[1].type, TaskType::Summarize);
  EXPECT_THAT(r.plan->tasks[1].dependsOn, ElementsAre("t1"));
}

TEST(PlanLoader, OptionalFieldsDefault) {
  json t = task("t1");
  t.erase("depends_on");
  json f = planFile({t});
  f["plan"].erase("schema_version");
  LoadResult r = load(f);
  ASSERT_TRUE(r.ok());
  EXPECT_TRUE(r.plan->tasks[0].dependsOn.empty());
  EXPECT_EQ(r.plan->schemaVersion, 1);
  EXPECT_FALSE(r.plan->tasks[0].estimatedSeconds.has_value());
}

TEST(PlanLoader, NullHintsAndNullEstimateAreAllowed) {
  json a = task("t1");
  a["hints"] = nullptr;
  json b = task("t2");
  b["hints"] = {{"estimated_seconds", nullptr}};
  EXPECT_TRUE(load(planFile({a, b})).ok());
}

TEST(PlanLoader, InvalidJson) {
  LoadResult r = parsePlanFile("{\"request_id\": ");
  EXPECT_FALSE(r.ok());
  EXPECT_THAT(codesOf(r.issues), ElementsAre("SCHEMA"));
}

TEST(PlanLoader, TopLevelMustBeObject) {
  EXPECT_THAT(codesOf(parsePlanFile("[1,2]").issues), ElementsAre("SCHEMA"));
}

TEST(PlanLoader, MissingEnvelopeFields) {
  json f = planFile({task("t1")});
  f.erase("request_id");
  f.erase("created_at");
  EXPECT_THAT(codesOf(load(f).issues), ElementsAre("SCHEMA", "SCHEMA"));
}

TEST(PlanLoader, UnknownFieldsRejectedEverywhere) {
  json t = task("t1");
  t["priority"] = 5;                         // runtime concern, not allowed from the planner
  t["hints"] = {{"estimated_seconds", 1}, {"worker", "w7"}};
  json f = planFile({t});
  f["plan"]["retry_policy"] = "always";
  f["extra"] = true;
  LoadResult r = load(f);
  EXPECT_FALSE(r.ok());
  EXPECT_EQ(r.issues.size(), 4u);
  for (const auto& i : r.issues) EXPECT_EQ(i.code, "SCHEMA") << i.message;
}

TEST(PlanLoader, SchemaVersionMustBeOne) {
  json f = planFile({task("t1")});
  f["plan"]["schema_version"] = 2;
  EXPECT_THAT(codesOf(load(f).issues), ElementsAre("SCHEMA_VERSION"));
  f["plan"]["schema_version"] = "1";
  EXPECT_THAT(codesOf(load(f).issues), ElementsAre("SCHEMA_VERSION"));
}

TEST(PlanLoader, EmptyAndOversizedPlans) {
  EXPECT_THAT(codesOf(load(planFile(json::array())).issues), ElementsAre("EMPTY_PLAN"));
  json many = json::array();
  for (int i = 1; i <= 51; ++i) many.push_back(task("t" + std::to_string(i)));
  EXPECT_THAT(codesOf(load(planFile(many)).issues), ElementsAre("TOO_MANY_TASKS"));
  many.erase(many.end() - 1);
  EXPECT_TRUE(load(planFile(many)).ok());   // exactly 50 is allowed
}

TEST(PlanLoader, IdPattern) {
  for (const char* bad : {"task1", "t", "T1", "t1a", " t1", "t-1", ""}) {
    LoadResult r = load(planFile({task(bad)}));
    EXPECT_THAT(codesOf(r.issues), ElementsAre("SCHEMA")) << "id=\"" << bad << "\"";
  }
  json numeric = task("t1");
  numeric["id"] = 1;
  EXPECT_THAT(codesOf(load(planFile({numeric})).issues), ElementsAre("SCHEMA"));
}

TEST(PlanLoader, TaskTypeMustBeSupported) {
  LoadResult r = load(planFile({task("t1", {}, "shell_command")}));
  ASSERT_THAT(codesOf(r.issues), ElementsAre("SCHEMA"));
  EXPECT_EQ(r.issues[0].taskId, "t1");
  EXPECT_NE(r.issues[0].message.find("shell_command"), std::string::npos);
}

TEST(PlanLoader, StringLengthsCountCharactersNotBytes) {
  json t = task("t1");
  std::string title;
  for (int i = 0; i < 120; ++i) title += "\xC3\xA9";  // 120 characters, 240 bytes
  t["title"] = title;
  EXPECT_TRUE(load(planFile({t})).ok());
  t["title"] = title + "e";                           // 121 characters
  EXPECT_THAT(codesOf(load(planFile({t})).issues), ElementsAre("SCHEMA"));
  t["title"] = "";
  EXPECT_THAT(codesOf(load(planFile({t})).issues), ElementsAre("SCHEMA"));
}

TEST(PlanLoader, FieldTypes) {
  json a = task("t1");
  a["depends_on"] = "t0";
  json b = task("t2");
  b["depends_on"] = json::array({1});
  json c = task("t3");
  c["hints"] = {{"estimated_seconds", -1}};
  json d = task("t4");
  d["hints"] = {{"estimated_seconds", "10"}};
  json e = task("t5");
  e["instruction"] = 42;
  LoadResult r = load(planFile({a, b, c, d, e}));
  EXPECT_EQ(r.issues.size(), 5u);
  for (const auto& i : r.issues) EXPECT_EQ(i.code, "SCHEMA");
}

TEST(PlanLoader, CollectsEveryIssueAndAttributesTasks) {
  json a = task("t1");
  a.erase("title");
  json b = task("t2", {}, "nope");
  LoadResult r = load(planFile({a, b}));
  ASSERT_EQ(r.issues.size(), 2u);
  EXPECT_EQ(r.issues[0].taskId, "t1");
  EXPECT_EQ(r.issues[1].taskId, "t2");
  EXPECT_FALSE(r.plan.has_value());
}

TEST(PlanLoader, MissingFileIsAnIssueNotACrash) {
  LoadResult r = loadPlanFile("/definitely/not/here.json");
  EXPECT_THAT(codesOf(r.issues), ElementsAre("SCHEMA"));
}

// --- expand hints (contract #1: hints.expected_fanout, hints.sim_subplan) ---

namespace {
json expandTask(json subTasks, json fanout = 2) {
  json t = task("t1", {}, "expand");
  json sub = json::object();
  sub["tasks"] = std::move(subTasks);
  t["hints"] = {{"estimated_seconds", 5}, {"expected_fanout", std::move(fanout)}, {"sim_subplan", std::move(sub)}};
  return t;
}

json subTask(const std::string& id, std::vector<std::string> deps = {}, const std::string& type = "research") {
  json t = task(id, std::move(deps), type);
  t["hints"] = {{"estimated_seconds", 3}};
  return t;
}

json array(std::initializer_list<json> items) {
  json a = json::array();
  for (const auto& i : items) a.push_back(i);
  return a;
}
}  // namespace

TEST(PlanLoader, ParsesExpandHintsAndSimSubplan) {
  LoadResult r = load(planFile({expandTask(array({subTask("s1"), subTask("s2", {"s1"})}), 3)}));
  ASSERT_TRUE(r.ok()) << r.issues.front().message;
  const TaskSpec& t = r.plan->tasks[0];
  EXPECT_EQ(t.expectedFanout, 3);
  ASSERT_EQ(t.simSubplan.size(), 2u);
  EXPECT_EQ(t.simSubplan[1].id, "s2");
  EXPECT_THAT(t.simSubplan[1].dependsOn, ElementsAre("s1"));
  EXPECT_EQ(t.simSubplan[1].estimatedSeconds, 3.0);
}

TEST(PlanLoader, ExpectedFanoutMustBePositiveInteger) {
  EXPECT_THAT(codesOf(load(planFile({expandTask(array({subTask("s1")}), 0)})).issues), ElementsAre("SCHEMA"));
  EXPECT_THAT(codesOf(load(planFile({expandTask(array({subTask("s1")}), 1.5)})).issues), ElementsAre("SCHEMA"));
}

TEST(PlanLoader, SimSubplanSchemaRules) {
  // ids must be local (s1...), no nested expand, hints limited to estimated_seconds, 1-20 tasks.
  json badHints = subTask("s1");
  badHints["hints"]["expected_fanout"] = 2;
  for (const json& sub : {array({subTask("t9")}), array({subTask("s1", {}, "expand")}), array({badHints}),
                          json::array()}) {
    LoadResult r = load(planFile({expandTask(sub)}));
    ASSERT_THAT(codesOf(r.issues), ElementsAre("SCHEMA")) << sub.dump();
    EXPECT_EQ(r.issues[0].taskId, "t1") << "attributed to the expand task";
  }
  json big = json::array();
  for (std::size_t i = 1; i <= kMaxSubplanTasks + 1; ++i) big.push_back(subTask("s" + std::to_string(i)));
  EXPECT_THAT(codesOf(load(planFile({expandTask(big)})).issues), ElementsAre("SCHEMA"));
}

TEST(PlanLoader, SimSubplanStructureIsCheckedByDagBuild) {
  json f = planFile({expandTask(array({subTask("s1", {"s9"}), subTask("s2", {"s2"})}))});
  auto issues = buildIssues(f);
  EXPECT_THAT(codesOf(issues), ElementsAre("UNKNOWN_DEPENDENCY", "SELF_DEPENDENCY"));
  for (const auto& i : issues) EXPECT_EQ(i.taskId, "t1");
}
