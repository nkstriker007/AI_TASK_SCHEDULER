#include "ats/RedisBus.hpp"

#include <hiredis/hiredis.h>

#include <memory>
#include <stdexcept>
#include <string>

namespace ats {

namespace {

struct ReplyDeleter {
  void operator()(redisReply* r) const { freeReplyObject(r); }
};
using ReplyPtr = std::unique_ptr<redisReply, ReplyDeleter>;

// Runs a command whose reply is not needed.
void run(redisReply* reply) { freeReplyObject(reply); }

std::string str(const redisReply* r) {
  if (!r || (r->type != REDIS_REPLY_STRING && r->type != REDIS_REPLY_STATUS)) return {};
  return std::string(r->str, r->len);
}

}  // namespace

RedisAddress parseRedisUrl(const std::string& url) {
  const std::string scheme = "redis://";
  if (url.rfind(scheme, 0) != 0) throw std::invalid_argument("Redis URL must start with redis:// (got " + url + ")");
  std::string rest = url.substr(scheme.size());
  RedisAddress a;
  if (auto slash = rest.find('/'); slash != std::string::npos) {
    const std::string db = rest.substr(slash + 1);
    if (!db.empty()) a.db = std::stoi(db);
    rest = rest.substr(0, slash);
  }
  if (auto colon = rest.rfind(':'); colon != std::string::npos) {
    a.port = std::stoi(rest.substr(colon + 1));
    rest = rest.substr(0, colon);
  }
  if (!rest.empty()) a.host = rest;
  return a;
}

RedisBus::RedisBus(const RedisAddress& address, int connectTimeoutMs) {
  timeval tv{connectTimeoutMs / 1000, (connectTimeoutMs % 1000) * 1000};
  ctx_ = redisConnectWithTimeout(address.host.c_str(), address.port, tv);
  if (!ctx_ || ctx_->err) {
    const std::string why = ctx_ ? ctx_->errstr : "cannot allocate redis context";
    if (ctx_) redisFree(ctx_);
    ctx_ = nullptr;
    throw std::runtime_error("cannot connect to Redis at " + address.host + ":" + std::to_string(address.port) +
                             ": " + why);
  }
  if (address.db != 0) run(command({"SELECT", std::to_string(address.db)}));
}

RedisBus::~RedisBus() {
  if (ctx_) redisFree(ctx_);
}

redisReply* RedisBus::command(const std::vector<std::string>& args, const char* allowedError) {
  std::vector<const char*> argv;
  std::vector<size_t> lens;
  for (const auto& a : args) {
    argv.push_back(a.data());
    lens.push_back(a.size());
  }
  auto* reply = static_cast<redisReply*>(redisCommandArgv(ctx_, static_cast<int>(argv.size()), argv.data(), lens.data()));
  if (!reply) throw std::runtime_error(std::string("Redis connection error: ") + ctx_->errstr);
  if (reply->type == REDIS_REPLY_ERROR) {
    const std::string err(reply->str, reply->len);
    if (allowedError && err.find(allowedError) != std::string::npos) return reply;
    freeReplyObject(reply);
    throw std::runtime_error("Redis " + args.front() + " failed: " + err);
  }
  return reply;
}

void RedisBus::ping() { run(command({"PING"})); }

void RedisBus::createGroup(const std::string& stream, const std::string& group) {
  run(command({"XGROUP", "CREATE", stream, group, "0", "MKSTREAM"}, "BUSYGROUP"));
}

std::vector<BusEntry> RedisBus::readGroup(const std::string& group, const std::string& consumer,
                                          const std::vector<std::string>& streams, int count, int blockMs) {
  std::vector<std::string> args{"XREADGROUP", "GROUP", group, consumer, "COUNT", std::to_string(count)};
  if (blockMs > 0) {
    args.emplace_back("BLOCK");
    args.push_back(std::to_string(blockMs));
  }
  args.emplace_back("STREAMS");
  for (const auto& s : streams) args.push_back(s);
  for (std::size_t i = 0; i < streams.size(); ++i) args.emplace_back(">");

  ReplyPtr reply(command(args));
  std::vector<BusEntry> out;
  if (reply->type == REDIS_REPLY_NIL) return out;  // BLOCK timed out
  // RESP2 reply: [[stream, [[id, [field, value, ...]], ...]], ...]
  for (std::size_t s = 0; s < reply->elements; ++s) {
    const redisReply* streamReply = reply->element[s];
    if (streamReply->type != REDIS_REPLY_ARRAY || streamReply->elements != 2) continue;
    const std::string stream = str(streamReply->element[0]);
    const redisReply* entries = streamReply->element[1];
    for (std::size_t e = 0; e < entries->elements; ++e) {
      const redisReply* entry = entries->element[e];
      if (entry->type != REDIS_REPLY_ARRAY || entry->elements != 2) continue;
      BusEntry be{stream, str(entry->element[0]), ""};
      const redisReply* fields = entry->element[1];
      if (fields->type == REDIS_REPLY_ARRAY) {
        for (std::size_t f = 0; f + 1 < fields->elements; f += 2)
          if (str(fields->element[f]) == "data") be.data = str(fields->element[f + 1]);
      }
      out.push_back(std::move(be));
    }
  }
  return out;
}

void RedisBus::ack(const std::string& stream, const std::string& group, const std::string& id) {
  run(command({"XACK", stream, group, id}));
}

std::string RedisBus::add(const std::string& stream, const std::string& data) {
  ReplyPtr reply(command({"XADD", stream, "*", "data", data}));
  return str(reply.get());
}

void RedisBus::hset(const std::string& key, const std::vector<std::pair<std::string, std::string>>& fields) {
  if (fields.empty()) return;
  std::vector<std::string> args{"HSET", key};
  for (const auto& [f, v] : fields) {
    args.push_back(f);
    args.push_back(v);
  }
  run(command(args));
}

}  // namespace ats
