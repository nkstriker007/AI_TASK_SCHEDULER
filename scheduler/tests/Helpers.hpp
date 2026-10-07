#pragma once

#include <initializer_list>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "ats/Dag.hpp"
#include "ats/PlanLoader.hpp"

namespace ats::test {

// Builds a minimal valid task JSON object.
inline nlohmann::json task(const std::string& id, std::vector<std::string> deps = {},
                           const std::string& type = "research") {
  nlohmann::json t = nlohmann::json::object();
  t["id"] = id;
  t["type"] = type;
  t["title"] = "Task " + id;
  t["instruction"] = "Do " + id;
  t["depends_on"] = deps;
  return t;
}

// Takes an explicit array. Never brace-initialise a json from a single json element ({t}):
// GCC and Clang disagree on whether that copies t or wraps it in an array.
inline nlohmann::json planFile(nlohmann::json tasks) {
  if (!tasks.is_array()) throw std::invalid_argument("planFile expects a JSON array of tasks");
  nlohmann::json plan = nlohmann::json::object();
  plan["schema_version"] = 1;
  plan["summary"] = "test plan";
  plan["tasks"] = std::move(tasks);
  nlohmann::json file = nlohmann::json::object();
  file["request_id"] = "req_test";
  file["created_at"] = "2026-09-28T00:00:00Z";
  file["plan"] = std::move(plan);
  return file;
}

inline nlohmann::json planFile(std::initializer_list<nlohmann::json> tasks) {
  nlohmann::json arr = nlohmann::json::array();
  for (const auto& t : tasks) arr.push_back(t);
  return planFile(std::move(arr));
}

inline std::vector<std::string> codesOf(const std::vector<ValidationIssue>& issues) {
  std::vector<std::string> c;
  for (const auto& i : issues) c.push_back(i.code);
  return c;
}

// Loads + builds; fails the test if either stage reports issues.
// Throws (failing the test cleanly) instead of dereferencing an empty result.
inline Dag mustBuild(const nlohmann::json& file) {
  LoadResult l = parsePlanFile(file.dump());
  if (!l.ok()) throw std::runtime_error("plan rejected by loader: " + l.issues.front().message);
  Dag::BuildResult b = Dag::build(*l.plan);
  if (!b.dag) throw std::runtime_error("plan rejected by Dag::build: " + b.issues.front().message);
  return std::move(*b.dag);
}

// Structural issues for a plan that passes the schema.
inline std::vector<ValidationIssue> buildIssues(const nlohmann::json& file) {
  LoadResult l = parsePlanFile(file.dump());
  if (!l.ok()) throw std::runtime_error("plan rejected by loader: " + l.issues.front().message);
  return Dag::build(*l.plan).issues;
}

inline std::vector<std::string> ids(const Dag& g, const std::vector<NodeId>& nodes) {
  std::vector<std::string> out;
  for (NodeId n : nodes) out.push_back(g.id(n));
  return out;
}

inline NodeId node(const Dag& g, const std::string& id) {
  auto n = g.find(id);
  EXPECT_TRUE(n.has_value()) << id;
  return n.value_or(0);
}

}  // namespace ats::test
