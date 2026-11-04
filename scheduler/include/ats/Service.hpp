#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <deque>
#include <map>
#include <memory>
#include <optional>
#include <ostream>
#include <random>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "ats/Bus.hpp"
#include "ats/Dag.hpp"
#include "ats/Scheduler.hpp"

namespace ats {

// Stream, group and key names: the same literals as planner/planner/keys.py.
namespace keys {
inline constexpr const char* kRequestsStream = "requests:new";
inline constexpr const char* kEventsStream = "task:events";
inline constexpr const char* kSchedulerGroup = "scheduler";
inline constexpr const char* kWorkersGroup = "workers";
inline std::string taskStream(const std::string& capability) { return "tasks:" + capability; }
inline std::string requestKey(const std::string& requestId) { return "request:" + requestId; }
inline std::string resultKey(const std::string& requestId, const std::string& taskId) {
  return "result:" + requestId + ":" + taskId;
}
}  // namespace keys

struct CapabilityConfig {
  std::size_t maxInFlight = 0;          // attempts in flight at once across all requests; 0 = unlimited (D13)
  std::int64_t timeoutMs = 120'000;     // attempt deadline = dispatch time + timeoutMs (8.6)
  std::vector<std::int64_t> backoffMs;  // delay before attempt 2, 3, ...; the last value repeats; empty = none
  double jitter = 0.0;                  // backoff * U[1-jitter, 1+jitter]
};

struct ServiceConfig {
  std::string consumer = "atsd";        // keep it stable: a restarted atsd resumes this consumer's requests (D44)
  // Across requests (D41):
  //   "fair-share"     the request with the fewest attempts in flight goes next (then the one served
  //                    least recently, then arrival); critical path within the request. Default.
  //   "critical-path"  the ready task with the largest bottom level, whichever request it is in
  //   "fifo"           the task that became ready first, whichever request it is in (baseline)
  std::string policy = "fair-share";
  int maxAttempts = 3;
  double defaultSeconds = 10.0;         // duration estimate without a hint (critical-path priorities)
  std::string defaultCapability = "llm";
  std::map<std::string, std::string> capabilityByType;  // task type name -> capability; else defaultCapability
  std::map<std::string, CapabilityConfig> capabilities{
      // 2 s, 4 s as in design doc 8.6, then doubling: with 6 attempts (make up-llm, D38) the waits add
      // up to more than a minute, enough to outlast a tokens-per-minute window (D45).
      {"llm", CapabilityConfig{8, 120'000, {2'000, 4'000, 8'000, 16'000, 32'000}, 0.2}},
      {"sim", CapabilityConfig{0, 300'000, {}, 0.0}},
  };
  std::uint64_t seed = 42;              // backoff jitter
  // Janitor (D49): every janitorIntervalMs, acknowledge entries in tasks:<capability> (group
  // "workers") that have been pending for at least janitorMinIdleMs and whose attempt is already
  // resolved. Hygiene only: it never retries or changes a task. 0 disables it.
  // Dynamic expansion limits (design doc 8.11, D53); an expand result over them fails the expand task.
  int maxExpansions = 3;                // expansions per request
  std::size_t maxRequestTasks = 100;    // tasks per request after all expansions (join nodes not counted)
  // Speculative execution (design doc 8.8, D56). Off by default. When on, a RUNNING attempt that has
  // run longer than speculationFactor x the speculationQuantile of completed attempts of its task
  // type (at least speculationMinSamples of them) gets one backup attempt, but only with capacity
  // left after normal dispatch. The first committed result wins (SET NX, pure tasks).
  bool speculation = false;
  double speculationFactor = 1.5;
  double speculationQuantile = 0.9;
  std::size_t speculationMinSamples = 5;
  std::int64_t janitorIntervalMs = 30'000;
  std::int64_t janitorMinIdleMs = 60'000;
};

// atsd's logic, with no I/O of its own: the Bus does Redis, the clock is injected, and the
// optional stream receives one JSON line per transition (design doc 8.12). Single-threaded.
//
// Per request it owns a Dag and a Scheduler (ExecutionState + policy). It:
//   - ingests plan files from requests:new (re-validated with PlanLoader + Dag::build);
//   - dispatches READY tasks as TaskMessages within per-capability in-flight caps (D13);
//   - applies task:events: started, the first completion of any attempt (8.5), failures;
//   - is the only detector of lost attempts: an attempt with no completion by its deadline is
//     treated as a retryable failure (D10); a late committed completion is still accepted;
//   - retries retryable failures with backoff (honouring retry_after_ms) up to maxAttempts,
//     then fails the task, which cancels its descendants (D5);
//   - writes request:<id> in the agreed format (D25).
class Service {
 public:
  using Clock = std::function<std::int64_t()>;  // Unix epoch milliseconds

  struct Stats {
    std::size_t ingested = 0, rejected = 0, succeeded = 0, failed = 0;
    std::size_t resumed = 0, restoredTasks = 0;  // requests picked up again after a restart (D44)
    std::size_t janitorRuns = 0, janitorAcked = 0;  // D49
    std::size_t expansions = 0, splicedTasks = 0, rejectedExpansions = 0;  // D53
    std::size_t speculativeAttempts = 0, speculativeWins = 0, backupTakeovers = 0;  // D56
    std::size_t dispatched = 0, retries = 0, lostAttempts = 0;
    std::size_t duplicateCompletions = 0, ignoredEvents = 0;
  };

  Service(Bus& bus, ServiceConfig config, Clock now, std::ostream* eventLog = nullptr,
          std::ostream* diagnostics = nullptr);
  ~Service();
  Service(const Service&) = delete;
  Service& operator=(const Service&) = delete;

  void setup();                                  // consumer groups on requests:new and task:events
  // Re-ingests the requests:new entries this consumer read but never acknowledged: requests a
  // previous atsd accepted and did not finish (D44). Call once after setup(). Returns how many.
  std::size_t recoverPending();
  std::size_t pollOnce(int maxBlockMs);          // read both streams, handle, ack, tick
  // One requests:new entry. With an entryId, an accepted request keeps that entry pending until
  // it finishes, and the call returns true: the caller must not acknowledge it (D44).
  bool submitPlanFile(std::string_view json, const std::string& entryId = "", bool resume = false);
  void handleEvent(std::string_view json);       // one task:events entry
  void tick();                                   // deadlines, due retries, dispatch

  // Clean shutdown (D43): stop taking requests and dispatching; keep handling events so the
  // attempts in flight can finish. Unfinished requests stay pending for the next atsd.
  void beginDrain();

  // One janitor pass over every capability stream (D49); tick() runs it every janitorIntervalMs.
  // Returns how many dead pending entries it acknowledged.
  std::size_t runJanitor();
  bool draining() const { return draining_; }
  std::size_t inFlightTotal() const;

  std::optional<std::int64_t> nextTimerMs() const;  // earliest deadline or retry
  std::size_t activeRequests() const { return active_.size(); }
  std::size_t inFlight(const std::string& capability) const;
  const Stats& stats() const { return stats_; }
  const ServiceConfig& config() const { return config_; }

 private:
  struct NodeRun;
  struct RequestRun;

  const CapabilityConfig& capabilityConfig(const std::string& capability) const;
  std::string capabilityFor(TaskType type) const;
  bool hasRoom(const std::string& capability) const;
  std::optional<NodeId> eligibleHead(RequestRun& req);
  bool before(const RequestRun& a, NodeId ha, const RequestRun& b, NodeId hb) const;
  void dispatchAll();
  void dispatch(RequestRun& req, const Dispatch& d);
  void expireDeadlines();
  void startDueRetries();
  void scheduleRetry(RequestRun& req, NodeId n, std::optional<std::int64_t> retryAfterMs, const std::string& why);
  void completeNode(RequestRun& req, NodeId n, const std::string& worker, int attempt);
  void failNode(RequestRun& req, NodeId n, const std::string& reason);
  void releaseSlot(RequestRun& req, NodeId n);
  void checkDone(RequestRun& req);
  void sweepFinished();
  void reject(const std::string& requestId, const std::string& error);
  void writeTask(RequestRun& req, NodeId n, const std::string& state);
  void log(const std::string& event, const std::string& request, const std::string& task = "", int attempt = 0,
           const std::string& worker = "", const std::string& detail = "");
  void diag(const std::string& message);
  std::int64_t backoffFor(const std::string& capability, int attempt);
  bool attemptResolved(const std::string& data);
  bool restoreNode(RequestRun& req, NodeId n);
  void speculate();
  void dispatchBackup(RequestRun& req, NodeId n, std::int64_t runningMs, double thresholdMs);
  void releaseBackup(RequestRun& req, NodeId n);
  bool promoteBackup(RequestRun& req, NodeId n);
  void recordDuration(const std::string& type, std::int64_t ms);
  std::optional<double> durationQuantile(const std::string& type) const;
  std::string taskMessage(const RequestRun& req, NodeId n, int attempt, std::int64_t deadlineMs) const;
  bool spliceExpansion(RequestRun& req, NodeId e, const std::string& worker, int attempt);

  Bus& bus_;
  ServiceConfig config_;
  Clock now_;
  std::ostream* eventLog_;
  std::ostream* diag_;
  std::mt19937_64 rng_;
  std::vector<std::string> order_;  // active request ids in arrival order (round-robin dispatch)
  std::unordered_map<std::string, std::unique_ptr<RequestRun>> active_;
  std::unordered_set<std::string> finished_;  // done or rejected: later events and duplicates are ignored
  std::map<std::string, std::size_t> inFlight_;
  std::map<std::string, std::int64_t> pausedUntil_;  // capability -> no dispatch before this time (provider throttling)
  std::uint64_t serveCounter_ = 0;  // dispatch sequence, for least-recently-served order
  std::uint64_t arrivalCounter_ = 0;
  std::shared_ptr<std::uint64_t> readyTickets_ = std::make_shared<std::uint64_t>(0);  // shared by every Scheduler
  bool draining_ = false;
  std::int64_t nextJanitorMs_ = 0;
  std::map<std::string, std::deque<std::int64_t>> durations_;  // task type -> recent attempt durations (D56)
  Stats stats_;
};

// Names accepted by ServiceConfig::policy.
std::vector<std::string> servicePolicyNames();

}  // namespace ats
