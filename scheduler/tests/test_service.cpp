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
    for (const auto& e : pendingRequests)
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
  std::optional<std::string> get(const std::string& key) override {
    auto it = strings.find(key);
    if (it == strings.end()) return std::nullopt;
    return it->second;
  }
  // Janitor: `workerPending` plays the workers group's pending list on tasks:* streams.
  std::vector<ats::PendingEntry> pending(const std::string& stream, const std::string& group, std::int64_t minIdleMs,
                                         const std::string& afterId, int count) override {
    ++pendingCalls;
    std::vector<ats::PendingEntry> out;
    if (group != "workers") return out;
    for (const auto& [key, p] : workerPending) {
      if (key.first != stream || p.idleMs < minIdleMs) continue;
      if (!afterId.empty() && !(afterId < p.id)) continue;
      out.push_back(p);
      if (static_cast<int>(out.size()) == count) break;
    }
    return out;
  }
  std::optional<std::string> entryData(const std::string& stream, const std::string& id) override {
    for (std::size_t i = 0; i < added.size(); ++i)
      if (added[i].first == stream && std::to_string(i + 1) + "-0" == id) return added[i].second;
    return std::nullopt;
  }
  void ack(const std::string& stream, const std::string&, const std::string& id) override {
    acked.emplace_back(stream, id);
    workerPending.erase({stream, id});
  }
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
  std::vector<ats::BusEntry> inbox, pendingRequests;
  std::vector<std::string> lastStreams;
  std::map<std::pair<std::string, std::string>, ats::PendingEntry> workerPending;  // (stream, id) -> entry
  int pendingCalls = 0;
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
  f.bus.pendingRequests.push_back({"requests:new", "5-0", chain().dump()});

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
  f.bus.pendingRequests.push_back({"requests:new", "5-0", chain().dump()});
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
  f.bus.pendingRequests.push_back({"requests:new", "5-0", named(chain(), "req_other").dump()});
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
  f.bus.pendingRequests.push_back({"requests:new", "5-0", chain().dump()});
  EXPECT_EQ(f.svc.recoverPending(), 0u);
  EXPECT_TRUE(f.bus.messages().empty());
  EXPECT_EQ(f.bus.acked, (std::vector<std::pair<std::string, std::string>>{{"requests:new", "5-0"}}));
  EXPECT_EQ(f.bus.hashes["request:req_test"]["status"], "SUCCEEDED");
}

TEST(ServiceResume, InvalidPendingEntryIsRejectedAndAcknowledged) {
  Fixture f;
  f.bus.pendingRequests.push_back({"requests:new", "5-0", planFile({task("t1", {"t1"})}).dump()});
  EXPECT_EQ(f.svc.recoverPending(), 0u);
  EXPECT_EQ(f.bus.hashes["request:req_test"]["status"], "REJECTED");
  EXPECT_EQ(f.bus.acked.size(), 1u);
}

TEST(Service, LlmBackoffDoublesSoSixAttemptsOutlastAMinute) {
  ats::ServiceConfig c;  // atsd's defaults
  const auto& backoff = c.capabilities.at("llm").backoffMs;
  EXPECT_EQ(backoff, (std::vector<std::int64_t>{2'000, 4'000, 8'000, 16'000, 32'000}));
  std::int64_t total = 0;
  for (auto b : backoff) total += b;  // waits before attempts 2..6
  EXPECT_GT(total, 60'000);
}

// ------------------------------------------------------------------------------------------
// Day 5: the janitor for dead pending entries (D49, design doc 8.7)

namespace {

ats::ServiceConfig janitorConfig(std::int64_t interval = 30'000) {
  ats::ServiceConfig c = simConfig();
  c.janitorIntervalMs = interval;
  c.janitorMinIdleMs = 60'000;
  return c;
}

// The n-th message dispatched has stream id "<n>-0" in FakeBus.
void leavePending(FakeBus& bus, int n, std::int64_t idleMs = 70'000, const std::string& consumer = "w-dead") {
  const std::string id = std::to_string(n) + "-0";
  bus.workerPending[{"tasks:sim", id}] = ats::PendingEntry{id, consumer, idleMs, 1};
}

bool ackedTask(const FakeBus& bus, int n) {
  const std::pair<std::string, std::string> key{"tasks:sim", std::to_string(n) + "-0"};
  return std::find(bus.acked.begin(), bus.acked.end(), key) != bus.acked.end();
}

}  // namespace

TEST(ServiceJanitor, LeavesTheLiveAttemptAndAcksItOnceItIsLost) {
  Fixture f(janitorConfig(0));  // run by hand
  f.submit(chain());
  leavePending(f.bus, 1);       // t1 attempt 1: its worker died holding it
  EXPECT_EQ(f.svc.runJanitor(), 0u);  // still the live attempt: only its deadline may act (D10)
  EXPECT_FALSE(ackedTask(f.bus, 1));

  f.advance(10'000);            // deadline: attempt 1 lost, retry in 1 s
  EXPECT_EQ(f.svc.runJanitor(), 1u);
  EXPECT_TRUE(ackedTask(f.bus, 1));

  f.advance(1'000);             // attempt 2 dispatched as message 2
  ASSERT_EQ(f.dispatched().back(), "req_test:t1:2");
  leavePending(f.bus, 2);
  EXPECT_EQ(f.svc.runJanitor(), 0u);  // the new live attempt is left alone
  EXPECT_EQ(f.svc.stats().janitorAcked, 1u);
}

TEST(ServiceJanitor, AcksEntriesOfFailedSupersededAndCompletedAttempts) {
  Fixture f(janitorConfig(0));
  f.submit(planFile({task("t1"), task("t2"), task("t3", {"t1"})}));  // messages 1 (t1), 2 (t2)
  f.failed("req_test", "t2", 1, "retryable");                      // waiting out its backoff
  f.completed("req_test", "t1", 1);                                // message 3 = t3
  f.advance(1'000);                                                // message 4 = t2 attempt 2
  ASSERT_EQ(f.dispatched().back(), "req_test:t2:2");
  for (int n : {1, 2, 3, 4}) leavePending(f.bus, n);
  EXPECT_EQ(f.svc.runJanitor(), 2u);
  EXPECT_TRUE(ackedTask(f.bus, 1));   // t1 COMPLETED (worker died after reporting, before XACK)
  EXPECT_TRUE(ackedTask(f.bus, 2));   // t2 attempt 1, superseded by attempt 2
  EXPECT_FALSE(ackedTask(f.bus, 3));  // t3 attempt 1 is live
  EXPECT_FALSE(ackedTask(f.bus, 4));  // t2 attempt 2 is live
}

TEST(ServiceJanitor, AcksEverythingOfAFinishedRequest) {
  Fixture f(janitorConfig(0));
  f.submit(planFile({task("t1"), task("t2")}));
  f.failed("req_test", "t1", 1, "permanent");
  f.completed("req_test", "t2", 1);
  ASSERT_EQ(f.bus.hashes["request:req_test"]["status"], "FAILED");
  leavePending(f.bus, 1);
  leavePending(f.bus, 2);
  EXPECT_EQ(f.svc.runJanitor(), 2u);
}

TEST(ServiceJanitor, RequestsFromBeforeARestartAreJudgedByTheirStatus) {
  Fixture f(janitorConfig(0));
  // Messages this atsd never sent: an earlier atsd's, for requests it does not track.
  f.bus.added.emplace_back("tasks:sim", json{{"attempt_id", "req_done:t1:1"}}.dump());     // 1-0
  f.bus.added.emplace_back("tasks:sim", json{{"attempt_id", "req_unknown:t1:1"}}.dump());  // 2-0
  f.bus.hashes["request:req_done"]["status"] = "SUCCEEDED";
  leavePending(f.bus, 1);
  leavePending(f.bus, 2);
  EXPECT_EQ(f.svc.runJanitor(), 1u);
  EXPECT_TRUE(ackedTask(f.bus, 1));
  EXPECT_FALSE(ackedTask(f.bus, 2));  // not known to be finished: left alone
}

TEST(ServiceJanitor, AcksEntriesThatCannotBeRead) {
  Fixture f(janitorConfig(0));
  f.bus.added.emplace_back("tasks:sim", "not json");  // 1-0
  leavePending(f.bus, 1);
  leavePending(f.bus, 9);  // 9-0 was deleted from the stream (XDEL/XTRIM)
  EXPECT_EQ(f.svc.runJanitor(), 2u);
  EXPECT_THAT(f.log.str(), ::testing::HasSubstr("(entry deleted)"));
}

TEST(ServiceJanitor, OnlyLongIdleEntriesAreConsidered) {
  Fixture f(janitorConfig(0));
  f.submit(chain());
  f.failed("req_test", "t1", 1, "retryable");  // attempt 1 resolved...
  leavePending(f.bus, 1, /*idleMs=*/5'000);    // ...but its worker may still be about to ack it
  EXPECT_EQ(f.svc.runJanitor(), 0u);
}

TEST(ServiceJanitor, NeverDispatchesRetriesOrChangesState) {
  Fixture f(janitorConfig(0));
  f.submit(chain());
  f.advance(10'000);  // t1 attempt 1 lost; retry due in 1 s
  leavePending(f.bus, 1);
  const auto before = f.dispatched();
  const auto states = f.bus.hashes["request:req_test"];
  f.svc.runJanitor();
  EXPECT_EQ(f.dispatched(), before);
  EXPECT_EQ(f.bus.hashes["request:req_test"], states);
  EXPECT_EQ(f.svc.stats().retries, 1u);
}

TEST(ServiceJanitor, RunsFromTickOncePerInterval) {
  Fixture f(janitorConfig(30'000));
  f.svc.tick();
  const int perRun = f.bus.pendingCalls;  // one XPENDING per capability stream
  EXPECT_GE(perRun, 1);
  EXPECT_EQ(f.svc.stats().janitorRuns, 1u);
  f.advance(29'999);
  EXPECT_EQ(f.svc.stats().janitorRuns, 1u);
  f.advance(1);
  EXPECT_EQ(f.svc.stats().janitorRuns, 2u);
  EXPECT_EQ(f.bus.pendingCalls, 2 * perRun);
}

TEST(ServiceJanitor, IntervalZeroDisablesIt) {
  Fixture f(janitorConfig(0));
  f.submit(chain());
  f.advance(120'000);
  EXPECT_EQ(f.svc.stats().janitorRuns, 0u);
  EXPECT_EQ(f.bus.pendingCalls, 0);
}

TEST(ServiceJanitor, ALateCompletionIsStillAcceptedAfterTheJanitorAckedItsEntry) {
  Fixture f(janitorConfig(0));
  f.submit(chain());
  f.advance(10'000);                  // attempt 1 lost
  leavePending(f.bus, 1);
  ASSERT_EQ(f.svc.runJanitor(), 1u);
  f.completed("req_test", "t1", 1);   // the "dead" worker was only slow (8.5)
  EXPECT_EQ(f.bus.taskState("req_test", "t1"), "COMPLETED");
  EXPECT_EQ(f.dispatched().back(), "req_test:t2:1");
}

// ------------------------------------------------------------------------------------------
// Day 6: dynamic expansion in atsd (D53, design doc 8.11)

namespace {

json subtask(const std::string& id, std::vector<std::string> deps = {}) {
  return {{"id", id}, {"type", "research"}, {"title", "Sub " + id}, {"instruction", "Do " + id},
          {"depends_on", deps}, {"hints", {{"estimated_seconds", 5}}}};
}

json subplan(std::vector<json> tasks) {
  json arr = json::array();
  for (auto& t : tasks) arr.push_back(t);
  return {{"tasks", arr}};
}

// t1 research, t2 expand, t3 compare(t1, t2), t4 report(t3)
json expandFile() {
  return planFile({task("t1"), task("t2", {}, "expand"), task("t3", {"t1", "t2"}, "compare"),
                   task("t4", {"t3"}, "report")});
}

// The worker side of an expand attempt: commit the subplan, report it.
void commitSubplan(Fixture& f, const std::string& rid, const std::string& tid, int attempt, const json& sp,
                   bool committed = true) {
  if (committed) f.bus.strings["result:" + rid + ":" + tid] = sp.dump();
  f.event({{"event", "task_completed"}, {"attempt_id", rid + ":" + tid + ":" + std::to_string(attempt)},
           {"worker_id", "w1"}, {"ts_ms", f.now}, {"result_key", "result:" + rid + ":" + tid},
           {"committed", committed}, {"result_kind", "subplan"}, {"exec_ms", 5}});
}

json messageFor(const Fixture& f, const std::string& attemptId) {
  for (const auto& m : f.bus.messages())
    if (m["attempt_id"] == attemptId) return m;
  return json();
}

std::vector<std::string> loggedEvents(const Fixture& f, const std::string& kind) {
  std::vector<std::string> out;
  std::istringstream lines(f.log.str());
  for (std::string line; std::getline(lines, line);) {
    json j = json::parse(line);
    if (j["event"] == kind) out.push_back(j.value("detail", ""));
  }
  return out;
}

}  // namespace

TEST(ServiceExpand, SpliceDispatchesTheSubplanAndTheJoinFeedsTheDependents) {
  Fixture f;
  f.submit(expandFile());
  ASSERT_EQ(f.dispatched(), (std::vector<std::string>{"req_test:t1:1", "req_test:t2:1"}));
  EXPECT_EQ(messageFor(f, "req_test:t2:1")["type"], "expand");
  f.completed("req_test", "t1", 1);
  commitSubplan(f, "req_test", "t2", 1, subplan({subtask("s1"), subtask("s2"), subtask("s3", {"s1"})}));

  // The new tasks get the expand task's result as input (contract #2 example).
  EXPECT_EQ(f.dispatched(), (std::vector<std::string>{"req_test:t1:1", "req_test:t2:1", "req_test:t2.1:1",
                                                      "req_test:t2.2:1"}));
  json m = messageFor(f, "req_test:t2.1:1");
  EXPECT_EQ(m["task_id"], "t2.1");
  EXPECT_EQ(m["inputs"], (json{{"t2", "result:req_test:t2"}}));
  EXPECT_EQ(m["estimated_seconds"], 5);
  EXPECT_EQ(f.bus.taskState("req_test", "t2"), "COMPLETED");
  EXPECT_EQ(f.bus.taskState("req_test", "t2.3"), "WAITING");
  EXPECT_EQ(f.bus.taskState("req_test", "t2.join"), "WAITING");
  EXPECT_EQ(f.svc.stats().expansions, 1u);
  EXPECT_EQ(f.svc.stats().splicedTasks, 3u);
  EXPECT_THAT(loggedEvents(f, "graph_expanded"),
              ::testing::ElementsAre("3 tasks: t2.1, t2.2, t2.3; t2.join -> t3"));

  f.completed("req_test", "t2.1", 1);
  EXPECT_EQ(messageFor(f, "req_test:t2.3:1")["inputs"],
            (json{{"t2", "result:req_test:t2"}, {"t2.1", "result:req_test:t2.1"}}));
  f.completed("req_test", "t2.2", 1);
  f.completed("req_test", "t2.3", 1);
  // t2.join completes inside atsd: never dispatched. t3 receives t1 and the subplan's sinks.
  EXPECT_EQ(f.bus.taskState("req_test", "t2.join"), "COMPLETED");
  for (const auto& id : f.dispatched()) EXPECT_EQ(id.find("t2.join"), std::string::npos) << id;
  json t3 = messageFor(f, "req_test:t3:1");
  ASSERT_FALSE(t3.is_null());
  EXPECT_EQ(t3["inputs"], (json{{"t1", "result:req_test:t1"}, {"t2.2", "result:req_test:t2.2"},
                                {"t2.3", "result:req_test:t2.3"}}));
  f.completed("req_test", "t3", 1);
  f.completed("req_test", "t4", 1);
  EXPECT_EQ(f.bus.hashes["request:req_test"]["status"], "SUCCEEDED");
  EXPECT_EQ(f.bus.hashes["request:req_test"]["sink"], "t4");
}

TEST(ServiceExpand, ADuplicateOrLateExpandResultNeverSplicesTwice) {
  Fixture f;
  f.submit(expandFile());
  commitSubplan(f, "req_test", "t2", 1, subplan({subtask("s1"), subtask("s2")}));
  const auto before = f.dispatched();
  commitSubplan(f, "req_test", "t2", 1, subplan({subtask("s1")}), /*committed=*/false);
  EXPECT_EQ(f.dispatched(), before);
  EXPECT_EQ(f.svc.stats().expansions, 1u);
  EXPECT_EQ(f.svc.stats().duplicateCompletions, 1u);
  EXPECT_EQ(loggedEvents(f, "graph_expanded").size(), 1u);
}

TEST(ServiceExpand, ALostExpandAttemptsLateResultIsSplicedOnce) {
  Fixture f;
  f.submit(expandFile());
  f.advance(10'000);  // t1 and t2 attempt 1 lost
  f.advance(1'000);   // attempt 2 of both dispatched
  commitSubplan(f, "req_test", "t2", 1, subplan({subtask("s1")}));  // the slow attempt 1 wins SET NX
  EXPECT_EQ(f.bus.taskState("req_test", "t2"), "COMPLETED");
  EXPECT_EQ(f.dispatched().back(), "req_test:t2.1:1");
  commitSubplan(f, "req_test", "t2", 2, subplan({subtask("s1")}), /*committed=*/false);
  EXPECT_EQ(f.svc.stats().expansions, 1u);
}

TEST(ServiceExpand, AnInvalidSubplanFailsTheExpandTaskAndCancelsItsDependents) {
  const std::vector<std::pair<std::string, json>> bad = {
      {"cycle", subplan({subtask("s1", {"s2"}), subtask("s2", {"s1"})})},
      {"unknown local dep", subplan({subtask("s1", {"s9"})})},
      {"nested expand", subplan({json{{"id", "s1"}, {"type", "expand"}, {"title", "x"}, {"instruction", "x"}}})},
      {"wrong id", subplan({subtask("t9")})},
      {"empty", json{{"tasks", json::array()}}},
      {"artifact pointer", json{{"artifact", "req_test/t2.1.json"}, {"bytes", 90000}}},
  };
  for (const auto& [name, sp] : bad) {
    Fixture f;
    f.submit(expandFile());
    f.completed("req_test", "t1", 1);
    commitSubplan(f, "req_test", "t2", 1, sp);
    EXPECT_EQ(f.bus.taskState("req_test", "t2"), "FAILED") << name;
    EXPECT_EQ(f.bus.taskState("req_test", "t3"), "CANCELLED") << name;
    EXPECT_EQ(f.bus.taskState("req_test", "t4"), "CANCELLED") << name;
    EXPECT_EQ(f.bus.hashes["request:req_test"]["status"], "FAILED") << name;
    EXPECT_THAT(f.bus.hashes["request:req_test"]["error"], ::testing::HasSubstr("t2 expansion rejected")) << name;
    EXPECT_EQ(f.svc.stats().rejectedExpansions, 1u) << name;
    EXPECT_EQ(f.dispatched().size(), 2u) << name;
  }
}

TEST(ServiceExpand, AMissingResultFailsTheExpandTask) {
  Fixture f;
  f.submit(expandFile());
  f.completed("req_test", "t2", 1);  // event without a committed key
  EXPECT_EQ(f.bus.taskState("req_test", "t2"), "FAILED");
  EXPECT_THAT(loggedEvents(f, "expansion_rejected"), ::testing::ElementsAre(::testing::HasSubstr("no committed result")));
}

TEST(ServiceExpand, TheRequestTaskLimitIsEnforced) {
  ats::ServiceConfig c = simConfig();
  c.maxRequestTasks = 6;  // 4 planned + 2 spliced allowed
  Fixture f(c);
  f.submit(expandFile());
  commitSubplan(f, "req_test", "t2", 1, subplan({subtask("s1"), subtask("s2"), subtask("s3")}));
  EXPECT_EQ(f.bus.taskState("req_test", "t2"), "FAILED");
  EXPECT_THAT(loggedEvents(f, "expansion_rejected"), ::testing::ElementsAre(::testing::HasSubstr("exceed 6 tasks")));
}

TEST(ServiceExpand, AtMostMaxExpansionsPerRequest) {
  ats::ServiceConfig c = simConfig();
  c.maxExpansions = 1;
  Fixture f(c);
  f.submit(planFile({task("t1", {}, "expand"), task("t2", {}, "expand"), task("t3", {"t1", "t2"}, "report")}));
  commitSubplan(f, "req_test", "t1", 1, subplan({subtask("s1")}));
  commitSubplan(f, "req_test", "t2", 1, subplan({subtask("s1")}));
  EXPECT_EQ(f.bus.taskState("req_test", "t1"), "COMPLETED");
  EXPECT_EQ(f.bus.taskState("req_test", "t2"), "FAILED");
  EXPECT_THAT(loggedEvents(f, "expansion_rejected"), ::testing::ElementsAre(::testing::HasSubstr("limit 1")));
  EXPECT_EQ(f.bus.taskState("req_test", "t1.1"), "QUEUED");  // the first expansion still runs
}

TEST(ServiceExpand, AFailedSubtaskCancelsTheJoinAndTheDependents) {
  Fixture f;
  f.submit(expandFile());
  f.completed("req_test", "t1", 1);
  commitSubplan(f, "req_test", "t2", 1, subplan({subtask("s1"), subtask("s2")}));
  f.failed("req_test", "t2.1", 1, "permanent");
  EXPECT_EQ(f.bus.taskState("req_test", "t2.join"), "CANCELLED");
  EXPECT_EQ(f.bus.taskState("req_test", "t3"), "CANCELLED");
  f.completed("req_test", "t2.2", 1);
  EXPECT_EQ(f.bus.hashes["request:req_test"]["status"], "FAILED");
}

TEST(ServiceExpand, CriticalPathPrioritiesAreRecomputedAfterTheSplice) {
  ats::ServiceConfig c = simConfig(/*simCap=*/1, "critical-path");
  Fixture f(c);
  json file = planFile({task("t1", {}, "expand"), task("t2")});
  file["plan"]["tasks"][0]["hints"] = {{"estimated_seconds", 1}};
  file["plan"]["tasks"][1]["hints"] = {{"estimated_seconds", 20}};
  f.submit(file);  // t2 (20) before t1 (1)
  ASSERT_EQ(f.dispatched(), (std::vector<std::string>{"req_test:t2:1"}));
  f.completed("req_test", "t2", 1);
  json longSub = subtask("s2");
  longSub["hints"]["estimated_seconds"] = 50;
  commitSubplan(f, "req_test", "t1", 1, subplan({subtask("s1"), longSub, subtask("s3")}));
  EXPECT_EQ(f.dispatched().back(), "req_test:t1.2:1");  // the 50 s subtask first
}

TEST(ServiceExpand, ARestartReSplicesWithTheSameIdsAndSkipsCommittedSubtasks) {
  Fixture f;
  // What the previous atsd left: t1, t2 (the subplan) and t2.1 committed; t2.2 on attempt 1.
  const json sp = subplan({subtask("s1"), subtask("s2")});
  f.bus.strings["result:req_test:t1"] = R"({"content":"m"})";
  f.bus.strings["result:req_test:t2"] = sp.dump();
  f.bus.strings["result:req_test:t2.1"] = R"({"content":"a"})";
  f.bus.hashes["request:req_test"] = {{"status", "RUNNING"},
                                      {"task:t1", R"({"state":"COMPLETED","attempt":1})"},
                                      {"task:t2", R"({"state":"COMPLETED","attempt":1})"},
                                      {"task:t2.1", R"({"state":"COMPLETED","attempt":1})"},
                                      {"task:t2.2", R"({"state":"RUNNING","attempt":1})"}};
  f.bus.pendingRequests.push_back({"requests:new", "5-0", expandFile().dump()});
  f.svc.recoverPending();
  EXPECT_EQ(f.svc.stats().expansions, 1u);
  EXPECT_EQ(f.dispatched(), (std::vector<std::string>{"req_test:t2.2:2"}));  // only the unfinished one
  EXPECT_EQ(f.bus.taskState("req_test", "t2.1"), "COMPLETED");
  f.completed("req_test", "t2.2", 2);
  EXPECT_EQ(f.bus.taskState("req_test", "t2.join"), "COMPLETED");
  EXPECT_EQ(f.dispatched().back(), "req_test:t3:1");
}

TEST(ServiceExpand, TheJanitorKnowsSplicedTasks) {
  Fixture f(janitorConfig(0));
  f.submit(expandFile());
  commitSubplan(f, "req_test", "t2", 1, subplan({subtask("s1")}));  // message 3 = t2.1
  leavePending(f.bus, 3);
  EXPECT_EQ(f.svc.runJanitor(), 0u);  // t2.1 attempt 1 is live
  f.completed("req_test", "t2.1", 1);
  EXPECT_EQ(f.svc.runJanitor(), 1u);
}
