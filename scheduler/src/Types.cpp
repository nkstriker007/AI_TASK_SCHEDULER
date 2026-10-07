#include "ats/Types.hpp"

#include <array>
#include <utility>

namespace ats {

namespace {
constexpr std::array<std::pair<std::string_view, TaskType>, 6> kTypes{{
    {"research", TaskType::Research},
    {"summarize", TaskType::Summarize},
    {"analyze", TaskType::Analyze},
    {"compare", TaskType::Compare},
    {"report", TaskType::Report},
    {"expand", TaskType::Expand},
}};
}  // namespace

std::optional<TaskType> parseTaskType(std::string_view s) {
  for (const auto& [name, type] : kTypes) {
    if (name == s) return type;
  }
  return std::nullopt;
}

std::string_view toString(TaskType t) {
  for (const auto& [name, type] : kTypes) {
    if (type == t) return name;
  }
  return "unknown";
}

std::size_t utf8Length(std::string_view s) {
  std::size_t n = 0;
  for (unsigned char c : s) {
    if ((c & 0xC0) != 0x80) ++n;  // count every byte that is not a continuation byte
  }
  return n;
}

}  // namespace ats
