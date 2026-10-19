#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
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
  std::string consumer = "atsd";
  std::string policy = "fifo";          // "fifo" | "critical-path"
  int maxAttempts = 3;
  double defaultSeconds = 10.0;         // duration estimate without a hint (critical-path priorities)
  std::string defaultCapability = "llm";
  std::map<std::string, std::string> capabilityByType;  // task type name -> capability; else defaultCapability
  std::map<std::string, CapabilityConfig> capabilities{
      {"llm", CapabilityConfig{8, 120'000, {2'000, 4'000}, 0.2}},
      {"sim", CapabilityConfig{0, 300'000, {}, 0.0}},
  };
  std::uint64_t seed = 42;              // backoff jitter
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
    std::size_t dispatched = 0, retries = 0, lostAttempts = 0;
    std::size_t duplicateCompletions = 0, ignoredEvents = 0;
  };

  Service(Bus& bus, ServiceConfig config, Clock now, std::ostream* eventLog = nullptr,
          std::ostream* diagnostics = nullptr);
  ~Service();
  Service(const Service&) = delete;
  Service& operator=(const Service&) = delete;

  void setup();                                  // consumer groups on requests:new and task:events
  std::size_t pollOnce(int maxBlockMs);          // read both streams, handle, ack, tick
  void submitPlanFile(std::string_view json);    // one requests:new entry
  void handleEvent(std::string_view json);       // one task:events entry
  void tick();                                   // deadlines, due retries, dispatch

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
  bool canDispatch(const RequestRun& req) const;
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
  std::uint64_t serveCounter_ = 0;  // dispatch sequence, for least-recently-served order
  Stats stats_;
};

}  // namespace ats
