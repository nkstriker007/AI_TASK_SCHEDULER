// Integration tests against a real Redis (Day 4): RedisBus commands, and the Service driven over
// RedisBus with a minimal in-test worker. Uses ATS_TEST_REDIS_URL if set (CI); otherwise starts a
// throwaway redis-server on a free port; skipped if neither is available. The database is
// flushed, so never point ATS_TEST_REDIS_URL at a Redis holding data you need.
#include <hiredis/hiredis.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include "Helpers.hpp"
#include "ats/RedisBus.hpp"
#include "ats/Service.hpp"

namespace {

using nlohmann::json;
using ats::test::planFile;
using ats::test::task;

// A throwaway redis-server (or the one in ATS_TEST_REDIS_URL), plus a raw hiredis connection
// the tests use to play the worker's part (SET NX results, XADD events).
class RedisFixture : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    if (const char* url = std::getenv("ATS_TEST_REDIS_URL"); url && *url) {
      address_ = ats::parseRedisUrl(url);
      return;
    }
    if (std::system("command -v redis-server >/dev/null 2>&1") != 0) return;
    const int port = 20000 + static_cast<int>(::getpid() % 20000);
    const std::string cmd = "redis-server --port " + std::to_string(port) +
                            " --save '' --appendonly no --daemonize yes --logfile /dev/null";
    if (std::system(cmd.c_str()) != 0) return;
    ats::RedisAddress a;
    a.port = port;
    for (int i = 0; i < 100; ++i) {  // wait until it accepts connections
      redisContext* c = redisConnect(a.host.c_str(), a.port);
      const bool ok = c && !c->err;
      if (c) redisFree(c);
      if (ok) {
        address_ = a;
        started_ = true;
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
  }

  static void TearDownTestSuite() {
    if (!started_ || !address_) return;
    if (redisContext* c = redisConnect(address_->host.c_str(), address_->port)) {
      if (auto* r = static_cast<redisReply*>(redisCommand(c, "SHUTDOWN NOSAVE"))) freeReplyObject(r);
      redisFree(c);
    }
  }

  void SetUp() override {
    if (!address_) GTEST_SKIP() << "no Redis: install redis-server or set ATS_TEST_REDIS_URL";
    raw_ = redisConnect(address_->host.c_str(), address_->port);
    ASSERT_TRUE(raw_ && !raw_->err);
    cmd({"SELECT", std::to_string(address_->db)});
    cmd({"FLUSHDB"});
  }

  void TearDown() override {
    if (raw_) redisFree(raw_);
  }

  // Runs a command on the raw connection; returns the reply as a string (or "" / "(nil)").
  std::string cmd(const std::vector<std::string>& args) {
    std::vector<const char*> argv;
    std::vector<size_t> lens;
    for (const auto& a : args) {
      argv.push_back(a.data());
      lens.push_back(a.size());
    }
    auto* r = static_cast<redisReply*>(redisCommandArgv(raw_, static_cast<int>(argv.size()), argv.data(), lens.data()));
    if (!r) throw std::runtime_error("redis connection lost");
    std::string out;
    if (r->type == REDIS_REPLY_STRING || r->type == REDIS_REPLY_STATUS || r->type == REDIS_REPLY_ERROR)
      out.assign(r->str, r->len);
    else if (r->type == REDIS_REPLY_INTEGER)
      out = std::to_string(r->integer);
    else if (r->type == REDIS_REPLY_NIL)
      out = "(nil)";
    freeReplyObject(r);
    return out;
  }

  std::unique_ptr<ats::RedisBus> bus() { return std::make_unique<ats::RedisBus>(*address_); }

  static inline std::optional<ats::RedisAddress> address_;
  static inline bool started_ = false;
  redisContext* raw_ = nullptr;
};

// Plays the workers: takes every TaskMessage from tasks:sim, optionally completes it (SET NX the
// result, XADD task_completed). Returns the attempt_ids it took.
struct InTestWorker {
  ats::RedisBus& bus;
  std::vector<std::string> taken;

  std::vector<json> take() {
    bus.createGroup("tasks:sim", "workers");
    std::vector<json> out;
    for (const auto& e : bus.readGroup("workers", "w-test", {"tasks:sim"}, 100, 0)) {
      json m = json::parse(e.data);
      taken.push_back(m["attempt_id"]);
      out.push_back(m);
      bus.ack("tasks:sim", "workers", e.id);
    }
    return out;
  }
};

json completedEvent(const json& m, bool committed) {
  return {{"event", "task_completed"}, {"attempt_id", m["attempt_id"]}, {"worker_id", "w-test"},
          {"ts_ms", 1}, {"result_key", "result:" + m["request_id"].get<std::string>() + ":" + m["task_id"].get<std::string>()},
          {"committed", committed}, {"result_kind", "output"}, {"exec_ms", 1}};
}

ats::ServiceConfig simConfig(const std::string& policy = "fair-share", std::size_t cap = 0) {
  ats::ServiceConfig c;
  c.policy = policy;
  c.defaultCapability = "sim";
  c.capabilities["sim"] = ats::CapabilityConfig{cap, 60'000, {}, 0.0};
  return c;
}

json named(json file, const std::string& rid) {
  file["request_id"] = rid;
  return file;
}

std::int64_t wallMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch()).count();
}

}  // namespace

TEST_F(RedisFixture, BusCommandsRoundTrip) {
  auto b = bus();
  b->createGroup("s", "g");
  b->createGroup("s", "g");  // BUSYGROUP is not an error
  const std::string id1 = b->add("s", R"({"n":1})");
  const std::string id2 = b->add("s", R"({"n":2})");
  auto got = b->readGroup("g", "c1", {"s"}, 10, 0);
  ASSERT_EQ(got.size(), 2u);
  EXPECT_EQ(got[0].stream, "s");
  EXPECT_EQ(got[0].id, id1);
  EXPECT_EQ(got[1].data, R"({"n":2})");
  EXPECT_TRUE(b->readGroup("g", "c1", {"s"}, 10, 0).empty());  // nothing new

  b->ack("s", "g", id1);
  auto pending = b->readPending("s", "g", "c1", "0", 10);  // read but not acknowledged
  ASSERT_EQ(pending.size(), 1u);
  EXPECT_EQ(pending[0].id, id2);
  EXPECT_EQ(pending[0].data, R"({"n":2})");
  EXPECT_TRUE(b->readPending("s", "g", "c1", id2, 10).empty());
  EXPECT_TRUE(b->readPending("s", "g", "other-consumer", "0", 10).empty());

  b->hset("h", {{"a", "1"}, {"b", "2"}});
  EXPECT_EQ(b->hget("h", "b"), "2");
  EXPECT_FALSE(b->hget("h", "zzz"));
  EXPECT_FALSE(b->hget("nokey", "a"));
  EXPECT_TRUE(b->exists("h"));
  EXPECT_FALSE(b->exists("nokey"));
}

TEST_F(RedisFixture, BlockingReadTimesOutEmpty) {
  auto b = bus();
  b->createGroup("s", "g");
  const auto t0 = wallMs();
  EXPECT_TRUE(b->readGroup("g", "c", {"s"}, 10, 100).empty());
  EXPECT_GE(wallMs() - t0, 90);
}

TEST_F(RedisFixture, TwoRequestsRunConcurrentlyOverRedis) {
  auto b = bus();
  ats::Service svc(*b, simConfig("fair-share", /*cap=*/2), wallMs);
  svc.setup();
  InTestWorker w{*b};
  cmd({"XADD", "requests:new", "*", "data", named(planFile({task("t1"), task("t2"), task("t3", {"t1", "t2"})}), "req_a").dump()});
  cmd({"XADD", "requests:new", "*", "data", named(planFile({task("t1"), task("t2", {"t1"})}), "req_b").dump()});

  std::map<std::string, int> firstRoundPerRequest;
  for (int round = 0; round < 50 && svc.stats().succeeded < 2; ++round) {
    svc.pollOnce(20);
    auto msgs = w.take();
    if (round == 0)
      for (const auto& m : msgs) ++firstRoundPerRequest[m["request_id"].get<std::string>()];
    for (const auto& m : msgs) {
      cmd({"SET", "result:" + m["request_id"].get<std::string>() + ":" + m["task_id"].get<std::string>(),
           R"({"content":"x"})", "NX"});
      cmd({"XADD", "task:events", "*", "data", completedEvent(m, true).dump()});
    }
  }
  EXPECT_EQ(cmd({"HGET", "request:req_a", "status"}), "SUCCEEDED");
  EXPECT_EQ(cmd({"HGET", "request:req_b", "status"}), "SUCCEEDED");
  EXPECT_EQ(firstRoundPerRequest["req_a"], 1);  // fair share: cap 2 split one each at the start
  EXPECT_EQ(firstRoundPerRequest["req_b"], 1);
  EXPECT_EQ(w.taken.size(), 5u);                // every task exactly once
  // Both finished, so both requests:new entries are acknowledged and nothing is left pending.
  EXPECT_TRUE(b->readPending("requests:new", "scheduler", "atsd", "0", 10).empty());
}

TEST_F(RedisFixture, ARestartedServiceResumesWithoutRerunningCommittedTasks) {
  auto b = bus();
  InTestWorker w{*b};
  cmd({"XADD", "requests:new", "*", "data",
       planFile({task("t1"), task("t2", {"t1"}), task("t3", {"t2"})}).dump()});
  {
    ats::Service first(*b, simConfig(), wallMs);
    first.setup();
    first.pollOnce(20);
    auto msgs = w.take();
    ASSERT_EQ(msgs.size(), 1u);  // t1
    cmd({"SET", "result:req_test:t1", R"({"content":"one"})", "NX"});
    cmd({"XADD", "task:events", "*", "data", completedEvent(msgs[0], true).dump()});
    first.pollOnce(20);
    msgs = w.take();
    ASSERT_EQ(msgs.size(), 1u);  // t2 dispatched...
    EXPECT_EQ(msgs[0]["attempt_id"], "req_test:t2:1");
    // ...and atsd dies here (no drain, no ack): the Service is simply destroyed.
  }
  EXPECT_EQ(cmd({"HGET", "request:req_test", "status"}), "RUNNING");

  ats::Service second(*b, simConfig(), wallMs);
  second.setup();
  EXPECT_EQ(second.recoverPending(), 1u);
  EXPECT_EQ(second.stats().restoredTasks, 1u);
  auto msgs = w.take();
  ASSERT_EQ(msgs.size(), 1u);
  EXPECT_EQ(msgs[0]["attempt_id"], "req_test:t2:2");  // not t1, and a new attempt number

  for (int round = 0; round < 20 && second.activeRequests() > 0; ++round) {
    for (const auto& m : msgs) {
      const std::string key = "result:req_test:" + m["task_id"].get<std::string>();
      const bool committed = cmd({"SET", key, R"({"content":"x"})", "NX"}) == "OK";
      cmd({"XADD", "task:events", "*", "data", completedEvent(m, committed).dump()});
    }
    second.pollOnce(20);
    msgs = w.take();
  }
  EXPECT_EQ(cmd({"HGET", "request:req_test", "status"}), "SUCCEEDED");
  EXPECT_EQ(cmd({"GET", "result:req_test:t1"}), R"({"content":"one"})");  // never rewritten
  EXPECT_EQ(std::count(w.taken.begin(), w.taken.end(), "req_test:t1:1"), 1);
  EXPECT_TRUE(b->readPending("requests:new", "scheduler", "atsd", "0", 10).empty());
}

TEST_F(RedisFixture, DrainingLeavesNewRequestsForTheNextService) {
  auto b = bus();
  InTestWorker w{*b};
  {
    ats::Service svc(*b, simConfig(), wallMs);
    svc.setup();
    svc.beginDrain();
    cmd({"XADD", "requests:new", "*", "data", planFile({task("t1")}).dump()});
    svc.pollOnce(20);
    EXPECT_EQ(svc.activeRequests(), 0u);
    EXPECT_TRUE(w.take().empty());
  }
  ats::Service next(*b, simConfig(), wallMs);
  next.setup();
  next.pollOnce(20);  // never read by the draining one, so it arrives as a new entry
  EXPECT_EQ(next.activeRequests(), 1u);
  EXPECT_EQ(w.take().size(), 1u);
}

// ------------------------------------------------------------------------------------------
// Day 5: the janitor (D49) against real XPENDING / XRANGE

TEST_F(RedisFixture, PendingAndEntryDataRoundTrip) {
  auto b = bus();
  EXPECT_TRUE(b->pending("tasks:none", "workers", 0, "", 10).empty());  // no stream or group: not an error
  b->createGroup("tasks:sim", "workers");
  const std::string id1 = b->add("tasks:sim", R"({"attempt_id":"r:t1:1"})");
  const std::string id2 = b->add("tasks:sim", R"({"attempt_id":"r:t2:1"})");
  ASSERT_EQ(b->readGroup("workers", "w-dead", {"tasks:sim"}, 10, 0).size(), 2u);
  std::this_thread::sleep_for(std::chrono::milliseconds(60));

  auto all = b->pending("tasks:sim", "workers", 50, "", 10);
  ASSERT_EQ(all.size(), 2u);
  EXPECT_EQ(all[0].id, id1);
  EXPECT_EQ(all[0].consumer, "w-dead");
  EXPECT_GE(all[0].idleMs, 50);
  EXPECT_EQ(all[0].deliveries, 1);
  auto rest = b->pending("tasks:sim", "workers", 50, id1, 10);  // exclusive start
  ASSERT_EQ(rest.size(), 1u);
  EXPECT_EQ(rest[0].id, id2);
  EXPECT_TRUE(b->pending("tasks:sim", "workers", 60'000, "", 10).empty());  // not idle long enough

  EXPECT_EQ(b->entryData("tasks:sim", id2), R"({"attempt_id":"r:t2:1"})");
  cmd({"XDEL", "tasks:sim", id2});
  EXPECT_FALSE(b->entryData("tasks:sim", id2));
  EXPECT_EQ(b->pending("tasks:sim", "workers", 0, "", 10).size(), 2u);  // still pending after XDEL
}

TEST_F(RedisFixture, JanitorAcksADeadWorkersEntryOnlyAfterItsAttemptIsLost) {
  auto b = bus();
  ats::ServiceConfig c = simConfig();
  c.capabilities["sim"].timeoutMs = 100;
  c.janitorIntervalMs = 0;  // run by hand
  c.janitorMinIdleMs = 50;
  ats::Service svc(*b, c, wallMs);
  svc.setup();
  cmd({"XADD", "requests:new", "*", "data", planFile({task("t1")}).dump()});
  svc.pollOnce(20);
  b->createGroup("tasks:sim", "workers");
  auto taken = b->readGroup("workers", "w-dead", {"tasks:sim"}, 10, 0);  // the worker that dies
  ASSERT_EQ(taken.size(), 1u);
  std::this_thread::sleep_for(std::chrono::milliseconds(60));
  EXPECT_EQ(svc.runJanitor(), 0u);  // attempt 1 is still live
  EXPECT_EQ(b->pending("tasks:sim", "workers", 0, "", 10).size(), 1u);

  std::this_thread::sleep_for(std::chrono::milliseconds(60));
  svc.pollOnce(10);                 // deadline passed: attempt 1 lost, attempt 2 dispatched (no backoff for sim)
  EXPECT_EQ(svc.stats().lostAttempts, 1u);
  EXPECT_EQ(svc.runJanitor(), 1u);
  EXPECT_TRUE(b->pending("tasks:sim", "workers", 0, "", 10).empty());  // attempt 2 was never read
  auto next = b->readGroup("workers", "w-live", {"tasks:sim"}, 10, 0);
  ASSERT_EQ(next.size(), 1u);
  EXPECT_NE(next[0].data.find("req_test:t1:2"), std::string::npos);
}
