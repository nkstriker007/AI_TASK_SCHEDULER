#pragma once

#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace ats {

// One stream entry. Every entry in this system has a single field, `data`, holding JSON
// (contract #2), so only that field is surfaced.
struct BusEntry {
  std::string stream;
  std::string id;
  std::string data;
};

// The Redis operations atsd needs, as an interface so the service logic can be unit-tested
// with a fake (design doc 10.3). RedisBus implements it with hiredis.
class Bus {
 public:
  virtual ~Bus() = default;

  // XGROUP CREATE <stream> <group> 0 MKSTREAM; an existing group is not an error.
  virtual void createGroup(const std::string& stream, const std::string& group) = 0;

  // XREADGROUP GROUP <group> <consumer> COUNT <count> [BLOCK <blockMs>] STREAMS <streams...> > ...
  // blockMs <= 0 means do not block.
  virtual std::vector<BusEntry> readGroup(const std::string& group, const std::string& consumer,
                                          const std::vector<std::string>& streams, int count, int blockMs) = 0;

  virtual void ack(const std::string& stream, const std::string& group, const std::string& id) = 0;

  // XADD <stream> * data <data>; returns the entry id.
  virtual std::string add(const std::string& stream, const std::string& data) = 0;

  // HSET <key> f1 v1 f2 v2 ...
  virtual void hset(const std::string& key, const std::vector<std::pair<std::string, std::string>>& fields) = 0;

  // ---- used when atsd restarts and resumes unfinished requests (D44) ----

  // XREADGROUP GROUP <group> <consumer> COUNT <count> STREAMS <stream> <afterId>: entries this
  // consumer read earlier but has not acknowledged, with ids greater than afterId ("0" = all).
  virtual std::vector<BusEntry> readPending(const std::string& stream, const std::string& group,
                                            const std::string& consumer, const std::string& afterId, int count) = 0;

  // HGET <key> <field>; nullopt if the key or field does not exist.
  virtual std::optional<std::string> hget(const std::string& key, const std::string& field) = 0;

  // EXISTS <key>
  virtual bool exists(const std::string& key) = 0;
};

}  // namespace ats
