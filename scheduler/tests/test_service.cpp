// atsd service logic with a fake bus and a fake clock (design doc 10.3).
#include <algorithm>
#include <cstdint>
#include <optional>
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
  std::vector<ats::BusEntry> readGroup(const std::string&, const std::string&, const std::vector<std::string>& streams,
                                       int, int) override {
    lastStreams = streams;
    std::vector<ats::BusEntry> out, keep;
    for (auto& e : inbox)
      (std::find(streams.begin(), streams.end(), e.stream) != streams.end() ? out : keep).push_back(std::move(e));
    inbox = std::move(keep);
    return out;
  }
  std::vector<ats::BusEntry> readPending(const std::string&, const std::string&, const std::string&,
                                         const std::string& afterId, int) override {
    std::vector<ats::BusEntry> out;
    for (const auto& e : pending)
      if (afterId == "0" || e.id > afterId) out.push_back(e);  // ids in these tests compare as strings
    return out;
  }
  std::optional<std::string> hget(const std::string& key, const std::string& field) override {
    auto k = hashes.find(key);
    if (k == hashes.end()) return std::nullopt;
    auto f = k->second.find(field);
    if (f == k->second.end()) return std::nullopt;
    return f->second;
  }
  bool exists(const std::string& key) override { return strings.count(key) || hashes.count(key); }
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
  std::vector<ats::BusEntry> inbox, pending;
  std::vector<std::string> lastStreams;
  std::map<std::string, std::string> strings;  // e.g. committed results
  std::map<std::string, std::map<std::string, std::string>> hashes;
};

// The Day 3 tests below were written for FIFO order; the Day 4 policy tests set their own.
ats::ServiceConfig simConfig(std::size_t simCap = 0, const std::string& policy = "fifo") {
  ats::ServiceConfig c;
  c.policy = policy;
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
  // Events are acknowledged at once; the request's entry stays pending until it finishes (D44).
  EXPECT_EQ(f.bus.acked, (std::vector<std::pair<std::string, std::string>>{{"task:events", "2-0"}}));
  f.completed("req_test", "t2", 1);
  f.completed("req_test", "t3", 1);
  EXPECT_EQ(f.bus.acked.back(), (std::pair<std::string, std::string>{"requests:new", "1-0"}));
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

TEST(Service, RetryAfterPausesTheWholeCapability) {
  Fixture f;
  f.submit(planFile({task("t1"), task("t2"), task("t3"), task("t4", {"t1"})}));
  ASSERT_EQ(f.dispatched().size(), 3u);
  f.failed("req_test", "t2", 1, "retryable", 5'000);  // "try again in 5 s": a shared provider limit
  f.completed("req_test", "t1", 1);                   // t4 becomes ready...
  EXPECT_EQ(f.dispatched().size(), 3u);               // ...but nothing is dispatched while paused
  f.advance(4'999);
  EXPECT_EQ(f.dispatched().size(), 3u);
  f.advance(1);
  // FIFO: t4 has been ready since t1 finished; t2 re-enters the ready set when its retry is due.
  EXPECT_EQ(f.dispatched(), (std::vector<std::string>{"req_test:t1:1", "req_test:t2:1", "req_test:t3:1",
                                                      "req_test:t4:1", "req_test:t2:2"}));
}

TEST(Service, FailureWithoutRetryAfterDoesNotPause) {
  Fixture f;
  f.submit(planFile({task("t1"), task("t2"), task("t3", {"t1"})}));
  f.failed("req_test", "t2", 1, "retryable");
  f.completed("req_test", "t1", 1);
  EXPECT_EQ(f.dispatched().back(), "req_test:t3:1");  // dispatched straight away
}

// ------------------------------------------------------------------------------------------
// Day 4: cross-request policies (D41), per-capability heads (D42), drain (D43), resume (D44)

namespace {

json named(json file, const std::string& rid) {
  file["request_id"] = rid;
  return file;
}

json wide(int n, double seconds = 10) {  // n independent tasks
  json tasks = json::array();
  for (int i = 1; i <= n; ++i) {
    json t = task("t" + std::to_string(i));
    t["hints"] = {{"estimated_seconds", seconds}};
    tasks.push_back(t);
  }
  return planFile(tasks);
}

}  // namespace

TEST(ServicePolicy, UnknownPolicyIsRejectedAtConstruction) {
  FakeBus bus;
  ats::ServiceConfig c = simConfig(0, "round-robin");
  EXPECT_THROW(ats::Service(bus, c, [] { return std::int64_t{0}; }), std::invalid_argument);
  EXPECT_EQ(ats::servicePolicyNames(), (std::vector<std::string>{"fair-share", "critical-path", "fifo"}));
}

TEST(ServicePolicy, DefaultIsFairShare) { EXPECT_EQ(ats::ServiceConfig{}.policy, "fair-share"); }

TEST(ServicePolicy, FairShareGivesAFreedSlotToTheRequestHoldingFewer) {
  Fixture f(simConfig(/*simCap=*/2, "fair-share"));
  f.svc.submitPlanFile(named(wide(3), "req_a").dump());  // both arrive before the next tick
  f.svc.submitPlanFile(named(wide(3), "req_b").dump());
  f.svc.tick();
  EXPECT_EQ(f.dispatched(), (std::vector<std::string>{"req_a:t1:1", "req_b:t1:1"}));  // one slot each
  f.completed("req_b", "t1", 1);
  // req_b now holds 0 slots and req_a 1, so req_b gets the slot back, although req_a was served
  // less recently (a plain round-robin would have given it to req_a and let it hold both).
  EXPECT_EQ(f.dispatched().back(), "req_b:t2:1");
  f.completed("req_a", "t1", 1);
  EXPECT_EQ(f.dispatched().back(), "req_a:t2:1");
}

TEST(ServicePolicy, FairShareUsesCriticalPathWithinARequest) {
  Fixture f(simConfig(/*simCap=*/1, "fair-share"));
  json file = planFile({task("t1"), task("t2"), task("t3", {"t2"})});
  file["plan"]["tasks"][0]["hints"] = {{"estimated_seconds", 5}};
  file["plan"]["tasks"][1]["hints"] = {{"estimated_seconds", 5}};
  file["plan"]["tasks"][2]["hints"] = {{"estimated_seconds", 30}};
  f.submit(file);
  EXPECT_EQ(f.dispatched(), (std::vector<std::string>{"req_test:t2:1"}));  // bottom level 35 beats 5
}

TEST(ServicePolicy, FifoServesTasksInTheOrderTheyBecameReadyAcrossRequests) {
  Fixture f(simConfig(/*simCap=*/1, "fifo"));
  f.submit(named(wide(3), "req_a"));
  f.submit(named(wide(1), "req_b"));
  f.completed("req_a", "t1", 1);
  // All of req_a's tasks became ready before req_b's: the baseline lets req_a go first.
  EXPECT_EQ(f.dispatched().back(), "req_a:t2:1");
  f.completed("req_a", "t2", 1);
  EXPECT_EQ(f.dispatched().back(), "req_a:t3:1");
  f.completed("req_a", "t3", 1);
  EXPECT_EQ(f.dispatched().back(), "req_b:t1:1");
}

TEST(ServicePolicy, CriticalPathPicksTheLongestRemainingWorkAcrossRequests) {
  Fixture f(simConfig(/*simCap=*/1, "critical-path"));
  f.submit(named(wide(2, /*seconds=*/5), "req_small"));  // arrives first, short tasks
  json chainB = planFile({task("t1"), task("t2", {"t1"})});
  chainB["plan"]["tasks"][0]["hints"] = {{"estimated_seconds", 20}};
  chainB["plan"]["tasks"][1]["hints"] = {{"estimated_seconds", 20}};
  f.submit(named(chainB, "req_long"));
  ASSERT_EQ(f.dispatched().size(), 1u);
  f.completed("req_small", "t1", 1);
  EXPECT_EQ(f.dispatched().back(), "req_long:t1:1");  // bottom level 40 beats 5
  f.completed("req_long", "t1", 1);
  EXPECT_EQ(f.dispatched().back(), "req_long:t2:1");  // 20 still beats 5
  f.completed("req_long", "t2", 1);
  EXPECT_EQ(f.dispatched().back(), "req_small:t2:1");
}

TEST(ServicePolicy, AFullCapabilityOnlyHoldsBackRequestsWhoseNextTaskNeedsIt) {
  ats::ServiceConfig c = simConfig(0, "critical-path");
  c.capabilityByType = {{"research", "llm"}, {"summarize", "sim"}};
  c.capabilities["llm"] = ats::CapabilityConfig{1, 10'000, {}, 0.0};
  Fixture f(c);
  f.submit(named(planFile({task("t1")}), "req_llm"));  // takes the only llm slot
  json mixed = planFile({task("t1"), task("t2", {}, "summarize")});
  mixed["plan"]["tasks"][1]["hints"] = {{"estimated_seconds", 60}};  // the sim task is the head
  f.submit(named(mixed, "req_mixed"));
  // Day 3 held req_mixed back entirely because one of its capabilities (llm) was full.
  EXPECT_EQ(f.dispatched(), (std::vector<std::string>{"req_llm:t1:1", "req_mixed:t2:1"}));
  EXPECT_EQ(f.bus.added.back().first, "tasks:sim");
  f.completed("req_llm", "t1", 1);
  EXPECT_EQ(f.dispatched().back(), "req_mixed:t1:1");
}

TEST(ServiceDrain, DrainingStopsIntakeAndDispatchButTakesEvents) {
  Fixture f(simConfig(/*simCap=*/1));
  f.submit(chain());
  f.svc.beginDrain();
  EXPECT_TRUE(f.svc.draining());
  f.bus.inbox.push_back({"requests:new", "9-0", named(chain(), "req_late").dump()});
  f.bus.inbox.push_back({"task:events", "8-0",
                         json{{"event", "task_completed"}, {"attempt_id", "req_test:t1:1"}, {"worker_id", "w"},
                              {"ts_ms", 1}, {"result_key", "result:req_test:t1"}, {"committed", true},
                              {"exec_ms", 1}}.dump()});
  f.svc.pollOnce(0);
  EXPECT_EQ(f.bus.lastStreams, (std::vector<std::string>{"task:events"}));
  EXPECT_EQ(f.bus.inbox.size(), 1u);                       // the new request stays unread
  EXPECT_EQ(f.bus.taskState("req_test", "t1"), "COMPLETED");
  EXPECT_EQ(f.dispatched().size(), 1u);                     // t2 is ready but not dispatched
  EXPECT_EQ(f.bus.taskState("req_test", "t2"), "READY");
  EXPECT_EQ(f.svc.inFlightTotal(), 0u);                     // drained
}

TEST(ServiceDrain, RetriesDueWhileDrainingAreNotDispatched) {
  Fixture f;
  f.submit(chain());
  f.svc.beginDrain();
  f.failed("req_test", "t1", 1, "retryable");
  f.advance(10'000);
  EXPECT_EQ(f.dispatched().size(), 1u);
  EXPECT_EQ(f.bus.taskState("req_test", "t1"), "RETRYING");
}

TEST(ServiceResume, RestartSkipsCommittedTasksAndContinuesAttemptNumbers) {
  Fixture f;
  // What the previous atsd left behind: t1 committed, t2 on its second attempt, t3 waiting.
  f.bus.strings["result:req_test:t1"] = R"({"content":"done"})";
  f.bus.hashes["request:req_test"] = {{"status", "RUNNING"},
                                      {"task:t1", R"({"state":"COMPLETED","attempt":1})"},
                                      {"task:t2", R"({"state":"RUNNING","attempt":2})"},
                                      {"task:t3", R"({"state":"WAITING","attempt":0})"}};
  f.bus.pending.push_back({"requests:new", "5-0", chain().dump()});

  EXPECT_EQ(f.svc.recoverPending(), 1u);
  EXPECT_EQ(f.svc.stats().resumed, 1u);
  EXPECT_EQ(f.svc.stats().restoredTasks, 1u);
  EXPECT_EQ(f.dispatched(), (std::vector<std::string>{"req_test:t2:3"}));  // t1 not run again
  EXPECT_EQ(f.bus.taskState("req_test", "t1"), "COMPLETED");
  EXPECT_TRUE(f.bus.acked.empty());  // still pending until the request finishes

  // A completion from the attempt that was running before the restart is still accepted.
  f.completed("req_test", "t2", 2);
  EXPECT_EQ(f.dispatched().back(), "req_test:t3:1");
  f.completed("req_test", "t2", 3, /*committed=*/false);
  EXPECT_EQ(f.svc.stats().duplicateCompletions, 1u);
  f.completed("req_test", "t3", 1);
  EXPECT_EQ(f.bus.hashes["request:req_test"]["status"], "SUCCEEDED");
  EXPECT_EQ(f.bus.acked, (std::vector<std::pair<std::string, std::string>>{{"requests:new", "5-0"}}));
}

TEST(ServiceResume, ARestartGivesAFreshRetryBudget) {
  Fixture f;
  f.bus.hashes["request:req_test"] = {{"status", "RUNNING"}, {"task:t1", R"({"state":"RETRYING","attempt":2})"}};
  f.bus.pending.push_back({"requests:new", "5-0", chain().dump()});
  f.svc.recoverPending();
  f.failed("req_test", "t1", 3, "retryable");
  f.advance(1'000);
  f.failed("req_test", "t1", 4, "retryable");
  f.advance(2'000);
  EXPECT_EQ(f.dispatched(), (std::vector<std::string>{"req_test:t1:3", "req_test:t1:4", "req_test:t1:5"}));
  f.failed("req_test", "t1", 5, "retryable");  // 3 attempts in this run: the budget is spent
  EXPECT_EQ(f.bus.hashes["request:req_test"]["status"], "FAILED");
  EXPECT_THAT(f.bus.hashes["request:req_test"]["error"], ::testing::HasSubstr("after 5 attempts"));  // in total
}

TEST(ServiceResume, AResultArrivingBeforeTheRedispatchCompletesWithoutAMessage) {
  Fixture f(simConfig(/*simCap=*/1));
  f.bus.hashes["request:req_other"] = {{"status", "RUNNING"}, {"task:t1", R"({"state":"RUNNING","attempt":1})"}};
  f.submit(named(chain(), "req_busy"));  // holds the only slot
  f.bus.pending.push_back({"requests:new", "5-0", named(chain(), "req_other").dump()});
  f.svc.recoverPending();
  EXPECT_EQ(f.bus.taskState("req_other", "t1"), "READY");
  f.completed("req_other", "t1", 1);  // the pre-restart attempt finishes
  f.completed("req_busy", "t1", 1);   // frees the slot
  // req_other's t1 completed without being sent again; its t2 follows.
  for (const auto& id : f.dispatched()) EXPECT_NE(id.rfind("req_other:t1:", 0), 0u) << id;
  EXPECT_EQ(f.bus.taskState("req_other", "t1"), "COMPLETED");
}

TEST(ServiceResume, FinishedButUnacknowledgedRequestsAreOnlyAcknowledged) {
  Fixture f;
  f.bus.hashes["request:req_test"] = {{"status", "SUCCEEDED"}};
  f.bus.pending.push_back({"requests:new", "5-0", chain().dump()});
  EXPECT_EQ(f.svc.recoverPending(), 0u);
  EXPECT_TRUE(f.bus.messages().empty());
  EXPECT_EQ(f.bus.acked, (std::vector<std::pair<std::string, std::string>>{{"requests:new", "5-0"}}));
  EXPECT_EQ(f.bus.hashes["request:req_test"]["status"], "SUCCEEDED");
}

TEST(ServiceResume, InvalidPendingEntryIsRejectedAndAcknowledged) {
  Fixture f;
  f.bus.pending.push_back({"requests:new", "5-0", planFile({task("t1", {"t1"})}).dump()});
  EXPECT_EQ(f.svc.recoverPending(), 0u);
  EXPECT_EQ(f.bus.hashes["request:req_test"]["status"], "REJECTED");
  EXPECT_EQ(f.bus.acked.size(), 1u);
}
