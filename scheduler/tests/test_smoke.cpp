#include <gtest/gtest.h>

#include "ats/Types.hpp"

TEST(Smoke, TaskTypeRoundTrip) {
  for (const char* name : {"research", "summarize", "analyze", "compare", "report", "expand"}) {
    auto t = ats::parseTaskType(name);
    ASSERT_TRUE(t.has_value()) << name;
    EXPECT_EQ(ats::toString(*t), name);
  }
  EXPECT_FALSE(ats::parseTaskType("shell").has_value());
  EXPECT_FALSE(ats::parseTaskType("Research").has_value());  // enum values are case-sensitive
}

TEST(Smoke, Utf8LengthCountsCharacters) {
  EXPECT_EQ(ats::utf8Length(""), 0u);
  EXPECT_EQ(ats::utf8Length("abc"), 3u);
  EXPECT_EQ(ats::utf8Length("caf\xC3\xA9"), 4u);         // café
  EXPECT_EQ(ats::utf8Length("\xE6\x97\xA5\xE6\x9C\xAC"), 2u);  // 日本
}
