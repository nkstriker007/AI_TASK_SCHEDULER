#pragma once

#include <string>
#include <utility>
#include <vector>

#include "ats/Bus.hpp"

struct redisContext;
struct redisReply;

namespace ats {

struct RedisAddress {
  std::string host = "127.0.0.1";
  int port = 6379;
  int db = 0;
};

// Parses redis://host[:port][/db]. Throws std::invalid_argument on anything else.
RedisAddress parseRedisUrl(const std::string& url);

// Bus over one synchronous hiredis connection. atsd is single-threaded, so one connection
// is enough. Any Redis error or lost connection throws std::runtime_error; atsd exits and
// its supervisor (Docker) restarts it.
//
// hiredis is used directly, behind this interface, instead of redis-plus-plus: the design
// doc's fallback (12.7), and the only C client needed on macOS, Linux and in the image.
class RedisBus final : public Bus {
 public:
  explicit RedisBus(const RedisAddress& address, int connectTimeoutMs = 5000);
  ~RedisBus() override;
  RedisBus(const RedisBus&) = delete;
  RedisBus& operator=(const RedisBus&) = delete;

  void ping();
  void createGroup(const std::string& stream, const std::string& group) override;
  std::vector<BusEntry> readGroup(const std::string& group, const std::string& consumer,
                                  const std::vector<std::string>& streams, int count, int blockMs) override;
  void ack(const std::string& stream, const std::string& group, const std::string& id) override;
  std::string add(const std::string& stream, const std::string& data) override;
  void hset(const std::string& key, const std::vector<std::pair<std::string, std::string>>& fields) override;

 private:
  // Runs one command; the caller frees the reply. Throws on connection or Redis errors
  // unless the error text contains `allowedError`.
  redisReply* command(const std::vector<std::string>& args, const char* allowedError = nullptr);

  redisContext* ctx_ = nullptr;
};

}  // namespace ats
