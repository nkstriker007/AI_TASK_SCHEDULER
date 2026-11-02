#include <stdexcept>

#include "Helpers.hpp"
#include "ats/Policy.hpp"

using namespace ats;
using ::testing::ElementsAre;

namespace {
std::vector<NodeId> drain(SchedulingPolicy& p) {
  std::vector<NodeId> out;
  while (auto n = p.pop()) out.push_back(*n);
  return out;
}
}  // namespace

TEST(Policy, FifoKeepsArrivalOrder) {
  FifoPolicy p;
  for (NodeId n : {3, 1, 2}) p.push(n);
  EXPECT_EQ(p.size(), 3u);
  EXPECT_THAT(drain(p), ElementsAre(3u, 1u, 2u));
  EXPECT_TRUE(p.empty());
  EXPECT_FALSE(p.pop().has_value());
}

TEST(Policy, CriticalPathPopsLongestRemainingWorkFirst) {
  CriticalPathPolicy p({1.0, 9.0, 5.0, 9.0});
  for (NodeId n : {0, 1, 2, 3}) p.push(n);
  EXPECT_THAT(drain(p), ElementsAre(1u, 3u, 2u, 0u));  // 9 (plan order breaks the tie), 9, 5, 1
}

TEST(Policy, CriticalPathInterleavedPushPop) {
  CriticalPathPolicy p({2.0, 4.0, 8.0});
  p.push(0);
  p.push(1);
  EXPECT_EQ(p.pop(), 1u);
  p.push(2);
  EXPECT_EQ(p.pop(), 2u);
  EXPECT_EQ(p.pop(), 0u);
  EXPECT_THROW(p.push(7), std::out_of_range);
}

TEST(Policy, Factory) {
  ats::Dag g = ats::test::mustBuild(ats::test::planFile({ats::test::task("t1")}));
  EXPECT_EQ(makePolicy("fifo", g, {1.0})->name(), "fifo");
  EXPECT_EQ(makePolicy("critical-path", g, {1.0})->name(), "critical-path");
  EXPECT_THROW(makePolicy("random", g, {1.0}), std::invalid_argument);
  EXPECT_THAT(policyNames(), ElementsAre("fifo", "critical-path"));
}

TEST(Policy, PeekReturnsWhatPopWouldWithoutRemovingIt) {
  ats::FifoPolicy fifo;
  EXPECT_FALSE(fifo.peek());
  fifo.push(2);
  fifo.push(0);
  EXPECT_EQ(fifo.peek(), 2u);
  EXPECT_EQ(fifo.size(), 2u);
  EXPECT_EQ(fifo.pop(), 2u);
  EXPECT_DOUBLE_EQ(fifo.priority(0), 0.0);

  ats::CriticalPathPolicy cp({1.0, 9.0, 4.0});
  cp.push(0);
  cp.push(2);
  cp.push(1);
  EXPECT_EQ(cp.peek(), 1u);
  EXPECT_EQ(cp.size(), 3u);
  EXPECT_EQ(cp.pop(), 1u);
  EXPECT_EQ(cp.peek(), 2u);
  const ats::SchedulingPolicy& base = cp;
  EXPECT_DOUBLE_EQ(base.priority(2), 4.0);  // virtual: visible through the interface
}
