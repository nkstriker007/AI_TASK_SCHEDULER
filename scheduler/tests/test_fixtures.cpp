// The C++ half of the cross-language contract: every fixture in examples/plans must produce
// exactly the error codes listed in examples/expected_validation.json (shared with the Python tests;
// keys are fixture names without .json, warnings are Python-only and ignored here).
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <set>
#include <string>

#include "Helpers.hpp"

namespace fs = std::filesystem;
using namespace ats;

namespace {
const fs::path kDir = ATS_FIXTURES_DIR;

nlohmann::json expectedValidation() {
  std::ifstream in(ATS_EXPECTED_VALIDATION);
  return nlohmann::json::parse(in);
}

std::vector<std::string> validate(const fs::path& file) {
  std::set<std::string> codes;
  LoadResult l = loadPlanFile(file);
  for (const auto& i : l.issues) codes.insert(i.code);
  if (l.ok())
    for (const auto& i : Dag::build(*l.plan).issues) codes.insert(i.code);
  return {codes.begin(), codes.end()};
}
}  // namespace

TEST(Fixtures, EveryFixtureMatchesTheContract) {
  const auto expected = expectedValidation();
  ASSERT_FALSE(expected.empty());
  for (const auto& [name, entry] : expected.items()) {
    auto want = entry.at("errors").get<std::vector<std::string>>();
    std::sort(want.begin(), want.end());
    const auto got = validate(kDir / (name + ".json"));
    EXPECT_EQ(got, want) << name;
    EXPECT_EQ(got.empty(), entry.at("valid").get<bool>()) << name;
  }
}

TEST(Fixtures, EveryFixtureFileIsListed) {
  const auto expected = expectedValidation();
  for (const auto& entry : fs::directory_iterator(kDir)) {
    if (entry.path().extension() != ".json") continue;
    const std::string name = entry.path().stem().string();
    EXPECT_TRUE(expected.contains(name)) << name << " is missing from expected_validation.json";
  }
}

TEST(Fixtures, CompanyResearchShape) {
  LoadResult l = loadPlanFile(kDir / "company_research.json");
  ASSERT_TRUE(l.ok());
  Dag g = *Dag::build(*l.plan).dag;
  EXPECT_THAT(test::ids(g, g.roots()), ::testing::ElementsAre("t1", "t2"));
  EXPECT_THAT(test::ids(g, g.sinks()), ::testing::ElementsAre("t6"));
  EXPECT_EQ(g.task(*g.find("t2")).estimatedSeconds, 25.0);
}
