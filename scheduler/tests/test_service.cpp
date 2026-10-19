// atsd service logic with a fake bus and a fake clock (design doc 10.3).
#include <cstdint>
#include <map>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "Helpers.hpp"
#include "ats/Service.hpp"

namespace {

using nlohmann::json;
using ats::test::planFile;
using ats::test::task;

class FakeBus final : public ats::Bus {
 public:
  void createGroup(const std::string& stream, const std::string& group) override { groups.emplace_back(stream, group); }
  std::vector<ats::BusEntry> readGroup(const std::string&, const std::string&, const std::vector<std::string>&, int,
                                       int) override {
    auto out = std::move(inbox);
    inbox.clear();
    return out;
  }
  void ack(const std::string& stream, const std::string&, const std::string& id) override { acked.emplace_back(stream, id); }
  std::string add(const std::string& stream, const std::string& data) override {
    added.emplace_back(stream, data);
    return std::to_string(added.size()) + "-0";
  }
  void hset(const std::string& key, const std::vector<std::pair<std::string, std::string>>& fields) override {
    for (const auto& [f, v] : fields) hashes[key][f] = v;
  }

  // Messages dispatched to a task stream, parsed.
  std::vector<json> messages() const {
    std::vector<json> out;
    for (const auto& [s, d] : added)
      if (s.rfind("tasks:", 0) == 0) out.push_back(json::parse(d));
    return out;
  }
  std::string taskState(const std::string& rid, const std::string& tid) {
    return json::parse(hashes["request:" + rid]["task:" + tid])["state"];
  }

  std::vector<std::pair<std::string, std::string>> groups, acked, added;
  std::vector<ats::BusEntry> inbox;
  std::map<std::string, std::map<std::string, std::string>> hashes;
};

ats::ServiceConfig simConfig(std::size_t simCap = 0) {
  ats::ServiceConfig c;
  c.defaultCapability = "sim";
  c.capabilities["sim"] = ats::CapabilityConfig{simCap, 10'000, {1'000, 2'000}, 0.0};
  return c;
}

struct Fixture {
  explicit Fixture(ats::ServiceConfig cfg = simConfig()) : svc(bus, std::move(cfg), [this] { return now; }, &log) {}
  void submit(const json& file) { svc.submitPlanFile(file.dump()); svc.tick(); }
  void event(const json& ev) { svc.handleEvent(ev.dump()); svc.tick(); }
  void started(const std::string& rid, const std::string& tid, int attempt, const std::string& w = "w1") {
    event({{"event", "task_started"}, {"attempt_id", rid + ":" + tid + ":" + std::to_string(attempt)},
           {"worker_id", w}, {"ts_ms", now}});
  }
  void completed(const std::string& rid, const std::string& tid, int attempt, bool committed = true) {
    event({{"event", "task_completed"}, {"attempt_id", rid + ":" + tid + ":" + std::to_string(attempt)},
           {"worker_id", "w1"}, {"ts_ms", now}, {"result_key", "result:" + rid + ":" + tid},
           {"committed", committed}, {"result_kind", "output"}, {"exec_ms", 5}});
  }
  void failed(const std::string& rid, const std::string& tid, int attempt, const std::string& cls,
              std::optional<std::int64_t> retryAfter = std::nullopt) {
    json ev = {{"event", "task_failed"}, {"attempt_id", rid + ":" + tid + ":" + std::to_string(attempt)},
               {"worker_id", "w1"}, {"ts_ms", now}, {"error_class", cls}, {"message", "boom"}};
    ev["retry_after_ms"] = retryAfter ? json(*retryAfter) : json(nullptr);
    event(ev);
  }
  void advance(std::int64_t ms) { now += ms; svc.tick(); }
  std::vector<std::string> dispatched() const {
    std::vector<std::string> ids;
    for (const auto& m : bus.messages()) ids.push_back(m["attempt_id"]);
    return ids;
  }

  std::int64_t now = 1'000'000;
  FakeBus bus;
  std::ostringstream log;
  ats::Service svc;
};

json chain() { return planFile({task("t1"), task("t2", {"t1"}), task("t3", {"t2"})}); }

}  // namespace

TEST(Service, SetupCreatesSchedulerGroups) {
  Fixture f;
  f.svc.setup();
  EXPECT_THAT(f.bus.groups, ::testing::ElementsAre(std::pair<std::string, std::string>{"requests:new", "scheduler"},
                                                   std::pair<std::string, std::string>{"task:events", "scheduler"}));
}

TEST(Service, IngestDispatchesRootsWithAContractMessage) {
  Fixture f;
  json file = planFile({task("t1"), task("t2"), task("t3", {"t1", "t2"}, "compare")});
  file["plan"]["tasks"][0]["hints"] = {{"estimated_seconds", 20}};
  f.submit(file);

  ASSERT_EQ(f.dispatched(), (std::vector<std::string>{"req_test:t1:1", "req_test:t2:1"}));
  json m = f.bus.messages()[0];
  EXPECT_EQ(f.bus.added[0].first, "tasks:sim");
  EXPECT_EQ(m["request_id"], "req_test");
  EXPECT_EQ(m["task_id"], "t1");
  EXPECT_EQ(m["attempt"], 1);
  EXPECT_EQ(m["type"], "research");
  EXPECT_EQ(m["inputs"], json::object());
  EXPECT_EQ(m["deadline_ms"], f.now + 10'000);
  EXPECT_EQ(m["estimated_seconds"], 20);
  EXPECT_FALSE(f.bus.messages()[1].contains("estimated_seconds"));  // no hint, no field

  auto& h = f.bus.hashes["request:req_test"];
  EXPECT_EQ(h["status"], "RUNNING");
  EXPECT_EQ(h["sink"], "t3");
  EXPECT_EQ(json::parse(h["task:t1"]), (json{{"state", "QUEUED"}, {"attempt", 1}}));
  EXPECT_EQ(json::parse(h["task:t3"]), (json{{"state", "WAITING"}, {"attempt", 0}}));
}

TEST(Service, CompletionReleasesChildrenWithInputKeysAndFinishes) {
  Fixture f;
  f.submit(planFile({task("t1"), task("t2"), task("t3", {"t1", "t2"}, "compare")}));
  f.started("req_test", "t1", 1);
  EXPECT_EQ(f.bus.taskState("req_test", "t1"), "RUNNING");
  f.completed("req_test", "t1", 1);
  EXPECT_EQ(f.dispatched().size(), 2u);  // t3 still waits for t2
  f.completed("req_test", "t2", 1);      // completion without a started event is accepted
  ASSERT_EQ(f.dispatched().back(), "req_test:t3:1");
  EXPECT_EQ(f.bus.messages().back()["inputs"],
            (json{{"t1", "result:req_test:t1"}, {"t2", "result:req_test:t2"}}));
  f.completed("req_test", "t3", 1);
  EXPECT_EQ(f.bus.hashes["request:req_test"]["status"], "SUCCEEDED");
  EXPECT_EQ(f.svc.activeRequests(), 0u);
  EXPECT_EQ(f.svc.stats().succeeded, 1u);
}

TEST(Service, DuplicateCompletionNeverReleasesChildrenTwice) {
  Fixture f;
  f.submit(chain());
  f.completed("req_test", "t1", 1);
  f.completed("req_test", "t1", 1, /*committed=*/false);
  EXPECT_EQ(f.dispatched(), (std::vector<std::string>{"req_test:t1:1", "req_test:t2:1"}));
  EXPECT_EQ(f.svc.stats().duplicateCompletions, 1u);
}

TEST(Service, RetryableFailureRedispatchesAfterBackoff) {
  Fixture f;
  f.submit(chain());
  f.failed("req_test", "t1", 1, "retryable");
  EXPECT_EQ(f.bus.taskState("req_test", "t1"), "RETRYING");
  EXPECT_EQ(f.dispatched().size(), 1u);  // backoff 1 s not over yet
  f.advance(999);
  EXPECT_EQ(f.dispatched().size(), 1u);
  f.advance(1);
  EXPECT_EQ(f.dispatched().back(), "req_test:t1:2");
  EXPECT_EQ(f.bus.messages().back()["attempt"], 2);
}

TEST(Service, RetryAfterHintIsHonoured) {
  Fixture f;
  f.submit(chain());
  f.failed("req_test", "t1", 1, "retryable", 5'000);  // longer than the 1 s backoff
  f.advance(4'999);
  EXPECT_EQ(f.dispatched().size(), 1u);
  f.advance(1);
  EXPECT_EQ(f.dispatched().back(), "req_test:t1:2");
}

TEST(Service, MaxAttemptsFailsTaskAndCancelsDescendants) {
  Fixture f;
  f.submit(chain());
  f.failed("req_test", "t1", 1, "retryable");
  f.advance(1'000);
  f.failed("req_test", "t1", 2, "retryable");
  f.advance(2'000);
  f.failed("req_test", "t1", 3, "retryable");
  EXPECT_EQ(f.dispatched(), (std::vector<std::string>{"req_test:t1:1", "req_test:t1:2", "req_test:t1:3"}));
  auto& h = f.bus.hashes["request:req_test"];
  EXPECT_EQ(h["status"], "FAILED");
  EXPECT_THAT(h["error"], ::testing::HasSubstr("after 3 attempts"));
  EXPECT_EQ(f.bus.taskState("req_test", "t1"), "FAILED");
  EXPECT_EQ(f.bus.taskState("req_test", "t2"), "CANCELLED");
  EXPECT_EQ(f.bus.taskState("req_test", "t3"), "CANCELLED");
}

TEST(Service, PermanentFailureFailsImmediately) {
  Fixture f;
  f.submit(chain());
  f.failed("req_test", "t1", 1, "permanent");
  EXPECT_EQ(f.dispatched().size(), 1u);
  EXPECT_EQ(f.bus.hashes["request:req_test"]["status"], "FAILED");
}

TEST(Service, IndependentBranchFinishesAfterASiblingFails) {
  Fixture f;
  f.submit(planFile({task("t1"), task("t2", {"t1"}), task("t3"), task("t4", {"t3"})}));
  f.failed("req_test", "t1", 1, "permanent");
  EXPECT_EQ(f.bus.taskState("req_test", "t2"), "CANCELLED");
  EXPECT_EQ(f.bus.hashes["request:req_test"]["status"], "RUNNING");  // t3 is still running
  f.completed("req_test", "t3", 1);
  f.completed("req_test", "t4", 1);
  EXPECT_EQ(f.bus.hashes["request:req_test"]["status"], "FAILED");
  EXPECT_EQ(f.bus.taskState("req_test", "t4"), "COMPLETED");
}

TEST(Service, DeadlineExpiryDispatchesANewAttempt) {
  Fixture f;
  f.submit(chain());
  f.started("req_test", "t1", 1);
  f.advance(10'000);  // deadline: lost attempt, then backoff 1 s
  EXPECT_EQ(f.svc.stats().lostAttempts, 1u);
  EXPECT_EQ(f.dispatched().size(), 1u);
  f.advance(1'000);
  EXPECT_EQ(f.dispatched().back(), "req_test:t1:2");
}

TEST(Service, LateCommittedResultFromALostAttemptIsAcceptedOnce) {
  Fixture f;
  f.submit(chain());
  f.advance(10'000);                 // attempt 1 lost
  f.advance(1'000);                  // attempt 2 dispatched
  f.completed("req_test", "t1", 1);  // the "lost" worker was just slow
  EXPECT_EQ(f.bus.taskState("req_test", "t1"), "COMPLETED");
  EXPECT_EQ(f.dispatched().back(), "req_test:t2:1");
  f.completed("req_test", "t1", 2, /*committed=*/false);  // attempt 2 finds the result already there
  EXPECT_EQ(f.svc.stats().duplicateCompletions, 1u);
  EXPECT_EQ(f.dispatched().size(), 3u);  // t2 released exactly once
}

TEST(Service, LateResultDuringBackoffCancelsTheRetry) {
  Fixture f;
  f.submit(chain());
  f.advance(10'000);                 // attempt 1 lost, retry due in 1 s
  f.completed("req_test", "t1", 1);  // arrives before the retry starts
  f.advance(5'000);
  EXPECT_EQ(f.dispatched(), (std::vector<std::string>{"req_test:t1:1", "req_test:t2:1"}));
}

TEST(Service, StaleEventsFromALostAttemptAreIgnored) {
  Fixture f;
  f.submit(chain());
  f.advance(10'000);
  f.advance(1'000);  // attempt 2 in flight
  f.started("req_test", "t1", 1);
  f.failed("req_test", "t1", 1, "permanent");  // a lost attempt cannot fail the task
  EXPECT_EQ(f.bus.taskState("req_test", "t1"), "QUEUED");
  EXPECT_EQ(f.bus.hashes["request:req_test"]["status"], "RUNNING");
}

TEST(Service, InFlightCapIsNeverExceeded) {
  Fixture f(simConfig(/*simCap=*/2));
  f.submit(planFile({task("t1"), task("t2"), task("t3"), task("t4"), task("t5", {"t1", "t2", "t3", "t4"})}));
  EXPECT_EQ(f.dispatched().size(), 2u);
  EXPECT_EQ(f.svc.inFlight("sim"), 2u);
  f.completed("req_test", "t1", 1);
  EXPECT_EQ(f.dispatched().size(), 3u);
  EXPECT_EQ(f.svc.inFlight("sim"), 2u);
  f.failed("req_test", "t2", 1, "retryable");  // a failed attempt frees its slot during backoff
  EXPECT_EQ(f.dispatched().size(), 4u);
  EXPECT_EQ(f.svc.inFlight("sim"), 2u);
}

TEST(Service, InvalidPlanIsRejectedAndNothingRuns) {
  Fixture f;
  f.submit(planFile({task("t1", {"t2"}), task("t2", {"t1"})}));
  auto& h = f.bus.hashes["request:req_test"];
  EXPECT_EQ(h["status"], "REJECTED");
  EXPECT_THAT(h["error"], ::testing::HasSubstr("CYCLE"));
  EXPECT_TRUE(f.bus.messages().empty());
  EXPECT_EQ(f.svc.stats().rejected, 1u);
}

TEST(Service, RequestIdWithAColonIsRejected) {
  Fixture f;
  json file = chain();
  file["request_id"] = "req:bad";
  f.submit(file);
  EXPECT_EQ(f.bus.hashes["request:req:bad"]["status"], "REJECTED");
  EXPECT_TRUE(f.bus.messages().empty());
}

TEST(Service, DuplicateSubmissionIsIgnored) {
  Fixture f;
  f.submit(chain());
  f.submit(chain());
  EXPECT_EQ(f.dispatched().size(), 1u);
  EXPECT_EQ(f.svc.stats().ingested, 1u);
}

TEST(Service, TwoRequestsRunConcurrentlyWithoutMixing) {
  Fixture f(simConfig(/*simCap=*/1));
  json a = chain();
  json b = chain();
  b["request_id"] = "req_other";
  f.submit(a);
  f.submit(b);
  EXPECT_EQ(f.dispatched(), (std::vector<std::string>{"req_test:t1:1"}));  // cap 1
  f.completed("req_test", "t1", 1);
  // Round-robin: the other request gets the free slot before req_test's next task.
  EXPECT_EQ(f.dispatched().back(), "req_other:t1:1");
  f.completed("req_other", "t1", 1);
  EXPECT_EQ(f.dispatched().back(), "req_test:t2:1");
  EXPECT_EQ(f.bus.taskState("req_other", "t2"), "READY");
  EXPECT_EQ(f.bus.taskState("req_test", "t2"), "QUEUED");
}

TEST(Service, GarbageAndUnknownEventsAreIgnored) {
  Fixture f;
  f.submit(chain());
  f.svc.handleEvent("not json");
  f.event({{"event", "task_completed"}, {"attempt_id", "nonsense"}});
  f.event({{"event", "task_completed"}, {"attempt_id", "req_nobody:t1:1"}});
  f.event({{"event", "task_completed"}, {"attempt_id", "req_test:t9:1"}});
  f.event({{"event", "task_completed"}, {"attempt_id", "req_test:t1:7"}});  // attempt never dispatched
  f.event({{"event", "task_teleported"}, {"attempt_id", "req_test:t1:1"}});
  EXPECT_EQ(f.svc.stats().ignoredEvents, 6u);
  EXPECT_EQ(f.bus.taskState("req_test", "t1"), "QUEUED");
}

TEST(Service, PollOnceHandlesBothStreamsAndAcks) {
  Fixture f;
  f.bus.inbox.push_back({"requests:new", "1-0", chain().dump()});
  f.svc.pollOnce(0);
  ASSERT_EQ(f.dispatched().size(), 1u);
  f.bus.inbox.push_back({"task:events", "2-0",
                         json{{"event", "task_completed"}, {"attempt_id", "req_test:t1:1"}, {"worker_id", "w"},
                              {"ts_ms", 1}, {"result_key", "result:req_test:t1"}, {"committed", true},
                              {"exec_ms", 1}}.dump()});
  f.svc.pollOnce(0);
  EXPECT_EQ(f.dispatched().back(), "req_test:t2:1");
  EXPECT_EQ(f.bus.acked, (std::vector<std::pair<std::string, std::string>>{{"requests:new", "1-0"}, {"task:events", "2-0"}}));
}

TEST(Service, EventLogHasOneJsonLinePerTransition) {
  Fixture f;
  f.submit(chain());
  f.completed("req_test", "t1", 1);
  std::istringstream lines(f.log.str());
  std::vector<std::string> events;
  for (std::string line; std::getline(lines, line);) events.push_back(json::parse(line)["event"]);
  EXPECT_THAT(events, ::testing::ElementsAre("request_accepted", "dispatched", "completed", "dispatched"));
}
