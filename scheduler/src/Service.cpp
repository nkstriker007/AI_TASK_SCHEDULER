#include "ats/Service.hpp"

#include <algorithm>
#include <exception>
#include <functional>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <utility>

#include <nlohmann/json.hpp>

#include "ats/Analysis.hpp"
#include "ats/PlanLoader.hpp"
#include "ats/Policy.hpp"

namespace ats {

using nlohmann::json;

// Service-layer view of one task. ExecutionState holds the task's state; this holds what
// only the service knows: the current attempt's deadline, a pending retry, the capability.
struct Service::NodeRun {
  std::string capability;
  int attempt = 0;                       // latest dispatched attempt (0 = never dispatched)
  int attemptBase = 0;                   // attempts made before atsd restarted; numbering continues (D44)
  bool inFlight = false;                 // the latest attempt holds a capability slot and a deadline
  std::int64_t deadlineMs = 0;
  std::optional<std::int64_t> retryAtMs;  // set while waiting out a backoff
  bool completeOnDispatch = false;       // a late result arrived while RETRYING (see handleEvent)
  // Speculative execution (D56). `attempt` stays the primary; a backup is a second live attempt
  // with its own slot and deadline. Attempt numbers stay unique: a backup takes highest + 1, and
  // later primaries are shifted by `extraAttempts`, which also keeps backups out of the retry budget.
  std::int64_t startedAtMs = 0;          // when the primary reported task_started (0 = not yet)
  int highest = 0;                       // highest attempt number dispatched for this task
  int extraAttempts = 0;                 // backups dispatched so far
  int backupAttempt = 0;
  bool backupInFlight = false;
  std::int64_t backupDeadlineMs = 0;
  bool speculated = false;               // at most one backup per primary attempt
};

struct Service::RequestRun {
  explicit RequestRun(Dag d) : dag(std::move(d)) {}
  Dag dag;                               // must not move once `sched` points at it
  std::unique_ptr<Scheduler> sched;
  std::vector<NodeRun> nodes;
  std::vector<std::string> capabilities;  // distinct capabilities used by this request
  std::string sink;                      // the single task with no dependents, or ""
  std::string error;                     // first failure, for request:<id>.error
  std::uint64_t lastServed = 0;          // serveCounter_ at its last dispatch; 0 = never
  std::uint64_t arrival = 0;             // ingest order
  std::size_t inFlight = 0;              // attempts holding a capability slot (fair share, D41)
  int expansions = 0;                    // expand results spliced so far (D53)
  bool resumed = false;                  // picked up again after a restart (D44)
  std::string entryId;                   // requests:new entry, acknowledged when the request finishes (D44)
  bool finished = false;
};

namespace {

std::string taskStateJson(const std::string& state, int attempt) {
  return json{{"state", state}, {"attempt", attempt}}.dump();
}

// attempt_id = <request_id>:<task_id>:<attempt> (contract #2).
bool splitAttemptId(const std::string& value, std::string& request, std::string& task, int& attempt) {
  const auto a = value.find(':');
  if (a == std::string::npos) return false;
  const auto b = value.find(':', a + 1);
  if (b == std::string::npos || value.find(':', b + 1) != std::string::npos) return false;
  request = value.substr(0, a);
  task = value.substr(a + 1, b - a - 1);
  try {
    std::size_t used = 0;
    attempt = std::stoi(value.substr(b + 1), &used);
    if (used != value.size() - b - 1) return false;
  } catch (const std::exception&) {
    return false;
  }
  return !request.empty() && !task.empty() && attempt >= 1;
}

}  // namespace

Service::Service(Bus& bus, ServiceConfig config, Clock now, std::ostream* eventLog, std::ostream* diagnostics)
    : bus_(bus), config_(std::move(config)), now_(std::move(now)), eventLog_(eventLog), diag_(diagnostics),
      rng_(config_.seed) {
  if (config_.maxAttempts < 1) throw std::invalid_argument("maxAttempts must be >= 1");
  const auto names = servicePolicyNames();
  if (std::find(names.begin(), names.end(), config_.policy) == names.end())
    throw std::invalid_argument("unknown scheduling policy \"" + config_.policy +
                                "\" (use fair-share, critical-path or fifo)");
  if (!now_) throw std::invalid_argument("Service needs a clock");
}

Service::~Service() = default;

void Service::setup() {
  bus_.createGroup(keys::kRequestsStream, keys::kSchedulerGroup);
  bus_.createGroup(keys::kEventsStream, keys::kSchedulerGroup);
}

std::size_t Service::recoverPending() {
  std::size_t resumed = 0;
  std::string after = "0";
  while (true) {
    auto entries = bus_.readPending(keys::kRequestsStream, keys::kSchedulerGroup, config_.consumer, after, 100);
    if (entries.empty()) break;
    for (const auto& e : entries) {
      after = e.id;
      bool held = false;
      try {
        held = submitPlanFile(e.data, e.id, /*resume=*/true);
      } catch (const std::exception& ex) {
        diag("error resuming " + e.id + ": " + ex.what());
      }
      if (held) ++resumed;
      else bus_.ack(keys::kRequestsStream, keys::kSchedulerGroup, e.id);
    }
  }
  tick();
  return resumed;
}

std::size_t Service::pollOnce(int maxBlockMs) {
  int block = maxBlockMs;
  if (auto next = nextTimerMs()) {
    const std::int64_t wait = *next - now_();
    block = static_cast<int>(std::clamp<std::int64_t>(wait, 0, maxBlockMs));
  }
  // While draining, new requests stay unread in requests:new for the next atsd (D43).
  std::vector<std::string> streams{keys::kEventsStream};
  if (!draining_) streams.insert(streams.begin(), keys::kRequestsStream);
  auto entries = bus_.readGroup(keys::kSchedulerGroup, config_.consumer, streams, 100, block);
  for (const auto& e : entries) {
    bool held = false;
    try {
      if (e.stream == keys::kRequestsStream) held = submitPlanFile(e.data, e.id);
      else handleEvent(e.data);
    } catch (const std::exception& ex) {  // a bad entry or a bug must not stop the service
      diag("error handling " + e.stream + " " + e.id + ": " + ex.what());
    }
    if (!held) bus_.ack(e.stream, keys::kSchedulerGroup, e.id);
  }
  tick();
  return entries.size();
}

void Service::beginDrain() {
  if (draining_) return;
  draining_ = true;
  diag("draining: no new requests or dispatches; waiting for " + std::to_string(inFlightTotal()) +
       " attempts in flight");
}

std::size_t Service::inFlightTotal() const {
  std::size_t total = 0;
  for (const auto& [cap, n] : inFlight_) total += n;
  return total;
}

// ------------------------------------------------------------------------------- intake

bool Service::submitPlanFile(std::string_view text, const std::string& entryId, bool resume) {
  std::string requestId;
  try {
    json doc = json::parse(text);
    if (doc.is_object() && doc.contains("request_id") && doc["request_id"].is_string())
      requestId = doc["request_id"].get<std::string>();
  } catch (const json::exception&) {
  }
  if (!requestId.empty() && (active_.count(requestId) || finished_.count(requestId))) {
    diag("duplicate request " + requestId + " ignored");
    return false;
  }

  LoadResult loaded = parsePlanFile(text);
  std::vector<ValidationIssue> issues = loaded.issues;
  std::optional<Dag> dag;
  if (loaded.ok()) {
    auto built = Dag::build(*loaded.plan);
    issues = built.issues;
    dag = std::move(built.dag);
  }
  if (!issues.empty() || !dag) {
    std::ostringstream err;
    for (std::size_t i = 0; i < issues.size(); ++i) {
      if (i) err << "; ";
      err << issues[i].code << (issues[i].taskId.empty() ? "" : " [" + issues[i].taskId + "]") << ": "
          << issues[i].message;
    }
    if (requestId.empty()) {
      ++stats_.rejected;
      diag("rejected plan file without a usable request_id: " + err.str());
    } else {
      reject(requestId, err.str());
    }
    return false;
  }
  if (requestId.find(':') != std::string::npos) {  // attempt_id must split unambiguously (contract #2)
    reject(requestId, "SCHEMA: request_id must not contain ':'");
    return false;
  }

  if (resume) {
    // Finished, but atsd stopped before acknowledging the entry: nothing to run again.
    const auto status = bus_.hget(keys::requestKey(requestId), "status");
    if (status && (*status == "SUCCEEDED" || *status == "FAILED" || *status == "REJECTED")) {
      finished_.insert(requestId);
      diag("request " + requestId + " already " + *status + "; not resumed");
      return false;
    }
  }

  auto req = std::make_unique<RequestRun>(std::move(*dag));
  const Dag& g = req->dag;
  // Within a request: FIFO for the fifo baseline, critical path otherwise (D41).
  const std::string within = config_.policy == "fifo" ? "fifo" : "critical-path";
  req->sched = std::make_unique<Scheduler>(g, makePolicy(within, g, estimatedDurations(g, config_.defaultSeconds)),
                                           readyTickets_);
  req->nodes.resize(g.size());
  req->entryId = entryId;
  req->arrival = ++arrivalCounter_;
  std::size_t restored = 0;
  req->resumed = resume;
  for (NodeId n = 0; n < g.size(); ++n) {
    req->nodes[n].capability = capabilityFor(g.task(n).type);
    if (resume && restoreNode(*req, n)) ++restored;
    if (std::find(req->capabilities.begin(), req->capabilities.end(), req->nodes[n].capability) ==
        req->capabilities.end())
      req->capabilities.push_back(req->nodes[n].capability);
  }
  const auto sinks = g.sinks();
  req->sink = sinks.size() == 1 ? g.id(sinks.front()) : "";

  std::vector<std::pair<std::string, std::string>> fields{
      {"status", "RUNNING"}, {"sink", req->sink}, {"updated_ms", std::to_string(now_())}};
  for (NodeId n = 0; n < g.size(); ++n)
    fields.emplace_back("task:" + g.id(n), taskStateJson("WAITING", req->nodes[n].attempt));
  bus_.hset(keys::requestKey(requestId), fields);

  RequestRun& r = *req;
  active_.emplace(requestId, std::move(req));
  order_.push_back(requestId);
  ++stats_.ingested;
  if (resume) {
    ++stats_.resumed;
    stats_.restoredTasks += restored;
    log("request_resumed", requestId, "", 0, "",
        std::to_string(g.size()) + " tasks, " + std::to_string(restored) + " already completed");
  } else {
    log("request_accepted", requestId, "", 0, "", std::to_string(g.size()) + " tasks");
  }

  r.sched->start();
  for (NodeId n = 0; n < g.size(); ++n)
    if (r.sched->state().state(n) == TaskState::Ready) writeTask(r, n, "READY");
  return !entryId.empty();
}

// D44: after a restart, numbering continues after the attempts recorded in request:<id>, so events
// from those attempts are never mistaken for new ones, and a committed result means the task is
// done: it completes when the policy reaches it, with no message. Returns true for the latter.
bool Service::restoreNode(RequestRun& req, NodeId n) {
  const std::string& rid = req.dag.requestId();
  NodeRun& nr = req.nodes[n];
  if (auto prev = bus_.hget(keys::requestKey(rid), "task:" + req.dag.id(n))) {
    try {
      json p = json::parse(*prev);
      if (p.is_object() && p.contains("attempt") && p["attempt"].is_number_integer())
        nr.attemptBase = std::max(0, p["attempt"].get<int>());
    } catch (const json::exception&) {
    }
  }
  nr.attempt = nr.attemptBase;
  nr.highest = nr.attemptBase;
  if (!bus_.exists(keys::resultKey(rid, req.dag.id(n)))) return false;
  nr.completeOnDispatch = true;
  return true;
}

void Service::reject(const std::string& requestId, const std::string& error) {
  ++stats_.rejected;
  finished_.insert(requestId);
  bus_.hset(keys::requestKey(requestId),
            {{"status", "REJECTED"}, {"error", error}, {"updated_ms", std::to_string(now_())}});
  log("request_rejected", requestId, "", 0, "", error);
}

// ------------------------------------------------------------------------------- events

void Service::handleEvent(std::string_view text) {
  json ev;
  try {
    ev = json::parse(text);
  } catch (const json::exception&) {
    ++stats_.ignoredEvents;
    diag("event is not JSON");
    return;
  }
  const std::string kind = ev.value("event", "");
  const std::string attemptId = ev.contains("attempt_id") && ev["attempt_id"].is_string()
                                    ? ev["attempt_id"].get<std::string>() : "";
  const std::string worker = ev.contains("worker_id") && ev["worker_id"].is_string()
                                 ? ev["worker_id"].get<std::string>() : "";
  std::string rid, tid;
  int attempt = 0;
  if (!splitAttemptId(attemptId, rid, tid, attempt)) {
    ++stats_.ignoredEvents;
    diag("event without a valid attempt_id");
    return;
  }
  auto it = active_.find(rid);
  if (it == active_.end()) {  // a finished request, or one this atsd never saw
    ++stats_.ignoredEvents;
    return;
  }
  RequestRun& req = *it->second;
  auto node = req.dag.find(tid);
  if (!node || attempt > req.nodes[*node].highest) {
    ++stats_.ignoredEvents;
    diag("event for an unknown task or attempt: " + attemptId);
    return;
  }
  const NodeId n = *node;
  NodeRun& nr = req.nodes[n];
  const TaskState st = req.sched->state().state(n);

  if (kind == "task_started") {
    if (attempt == nr.attempt && nr.inFlight && st == TaskState::Queued) {
      req.sched->started(n);
      nr.startedAtMs = now_();
      writeTask(req, n, "RUNNING");
      log("started", rid, tid, attempt, worker);
    } else {
      ++stats_.ignoredEvents;  // stale attempt, or already running/finished
    }
  } else if (kind == "task_completed") {
    // The first completion of ANY attempt completes the task: results are written once with
    // SET NX and tasks are pure, so a late result from a lost attempt is valid (8.5 step 7).
    if (st == TaskState::Completed) {
      ++stats_.duplicateCompletions;
      log("duplicate_completion", rid, tid, attempt, worker);
    } else if (isTerminal(st)) {
      ++stats_.ignoredEvents;  // FAILED or CANCELLED: the result is unused
    } else if (st == TaskState::Retrying || st == TaskState::Ready) {
      // Already back in the ready set (RETRYING, or READY after a restart, D44), which the state
      // machine cannot complete from: finish it the moment the policy dispatches it again,
      // without sending a message.
      nr.completeOnDispatch = true;
      log("late_result", rid, tid, attempt, worker, "completes on next dispatch");
    } else {  // QUEUED or RUNNING, possibly waiting out a backoff
      const bool backupWon = nr.backupAttempt != 0 && attempt == nr.backupAttempt;
      if (ev.contains("exec_ms") && ev["exec_ms"].is_number_integer())
        recordDuration(std::string(toString(req.dag.task(n).type)), ev["exec_ms"].get<std::int64_t>());
      if (nr.inFlight) releaseSlot(req, n);
      releaseBackup(req, n);  // the other attempt may still finish; its result is a duplicate (8.5)
      nr.retryAtMs.reset();
      if (backupWon) {
        ++stats_.speculativeWins;
        log("speculation_won", rid, tid, attempt, worker);
      }
      completeNode(req, n, worker, attempt);  // splices first if this is an expand task (D53)
    }
  } else if (kind == "task_failed") {
    const bool live = st == TaskState::Queued || st == TaskState::Running;
    if (live && nr.backupInFlight && attempt == nr.backupAttempt && ev.value("error_class", "") != "permanent") {
      releaseBackup(req, n);  // a failed backup changes nothing: the primary is still running
      log("backup_failed", rid, tid, attempt, worker, ev.value("message", std::string{}));
    } else if (attempt == nr.attempt && nr.inFlight && live && ev.value("error_class", "") != "permanent" &&
               promoteBackup(req, n)) {
      log("backup_took_over", rid, tid, nr.attempt, worker, "primary failed: " + ev.value("message", std::string{}));
    } else if (live && ((attempt == nr.attempt && nr.inFlight) || (attempt == nr.backupAttempt && nr.backupInFlight))) {
      releaseSlot(req, n);
      releaseBackup(req, n);
      const std::string message = ev.value("message", std::string{});
      if (ev.value("error_class", "") == "permanent") {
        failNode(req, n, tid + " failed permanently: " + message);
      } else {
        std::optional<std::int64_t> retryAfter;
        if (ev.contains("retry_after_ms") && ev["retry_after_ms"].is_number_integer())
          retryAfter = ev["retry_after_ms"].get<std::int64_t>();
        if (retryAfter && *retryAfter > 0) {
          // The provider said "try again in N ms" (e.g. a tokens-per-minute 429). That limit is
          // shared by every task on this capability, so pause all of them, not just this one;
          // otherwise the others keep using up the window and this task never gets through.
          auto& until = pausedUntil_[nr.capability];
          until = std::max(until, now_() + *retryAfter);
          log("capability_paused", rid, tid, attempt, worker,
              nr.capability + " for " + std::to_string(*retryAfter) + " ms");
        }
        scheduleRetry(req, n, retryAfter, message);
      }
    } else {
      ++stats_.ignoredEvents;  // a lost attempt's late failure: already handled at its deadline
    }
  } else {
    ++stats_.ignoredEvents;
    diag("unknown event type '" + kind + "'");
  }
  checkDone(req);
  sweepFinished();
}

// ------------------------------------------------------------------------------- timers

void Service::tick() {
  expireDeadlines();
  startDueRetries();
  dispatchAll();
  speculate();
  sweepFinished();
  if (config_.janitorIntervalMs > 0 && now_() >= nextJanitorMs_) {
    nextJanitorMs_ = now_() + config_.janitorIntervalMs;
    try {
      runJanitor();
    } catch (const std::exception& ex) {  // hygiene must never stop the scheduler
      diag(std::string("janitor: ") + ex.what());
    }
  }
}

// ------------------------------------------------------------------------------- janitor

// True if the TaskMessage `data` belongs to an attempt atsd no longer waits for (D49): its request
// is finished, its task is terminal, a newer attempt has been dispatched, or this attempt was
// already declared lost or failed. Anything atsd cannot place is left alone, except entries that
// cannot be read at all, which no worker could run either.
bool Service::attemptResolved(const std::string& data) {
  std::string attemptId;
  try {
    json m = json::parse(data);
    if (m.is_object() && m.contains("attempt_id") && m["attempt_id"].is_string())
      attemptId = m["attempt_id"].get<std::string>();
  } catch (const json::exception&) {
    return true;
  }
  std::string rid, tid;
  int attempt = 0;
  if (!splitAttemptId(attemptId, rid, tid, attempt)) return true;

  auto it = active_.find(rid);
  if (it == active_.end()) {
    if (finished_.count(rid)) return true;
    const auto status = bus_.hget(keys::requestKey(rid), "status");  // finished by an earlier atsd
    return status && (*status == "SUCCEEDED" || *status == "FAILED" || *status == "REJECTED");
  }
  const RequestRun& req = *it->second;
  auto node = req.dag.find(tid);
  if (!node) return true;
  const NodeRun& nr = req.nodes[*node];
  if (isTerminal(req.sched->state().state(*node))) return true;
  if (attempt == nr.attempt && nr.inFlight) return false;              // the live primary
  if (attempt == nr.backupAttempt && nr.backupInFlight) return false;  // the live backup (D56)
  return attempt <= nr.highest;  // superseded, lost or failed; newer than atsd knows: leave it
}

std::size_t Service::runJanitor() {
  ++stats_.janitorRuns;
  std::size_t acked = 0;
  constexpr int kPage = 100;
  constexpr std::size_t kMaxPerRun = 1000;  // bounded work per tick
  for (const auto& [capability, cfg] : config_.capabilities) {
    const std::string stream = keys::taskStream(capability);
    std::string after;
    std::size_t seen = 0;
    while (seen < kMaxPerRun) {
      auto page = bus_.pending(stream, keys::kWorkersGroup, config_.janitorMinIdleMs, after, kPage);
      if (page.empty()) break;
      for (const auto& p : page) {
        after = p.id;
        ++seen;
        const auto data = bus_.entryData(stream, p.id);
        if (data && !attemptResolved(*data)) continue;  // still the live attempt: its deadline owns it
        bus_.ack(stream, keys::kWorkersGroup, p.id);
        ++acked;
        std::string rid, tid;
        int attempt = 0;
        if (data) {
          try {
            json m = json::parse(*data);
            if (m.is_object() && m.contains("attempt_id") && m["attempt_id"].is_string())
              splitAttemptId(m["attempt_id"].get<std::string>(), rid, tid, attempt);
          } catch (const json::exception&) {
          }
        }
        log("janitor_acked", rid, tid, attempt, p.consumer,
            stream + " " + p.id + " idle " + std::to_string(p.idleMs) + " ms" + (data ? "" : " (entry deleted)"));
      }
      if (page.size() < static_cast<std::size_t>(kPage)) break;
    }
  }
  stats_.janitorAcked += acked;
  return acked;
}

std::optional<std::int64_t> Service::nextTimerMs() const {
  std::optional<std::int64_t> next;
  const std::int64_t now = now_();
  for (const auto& [cap, until] : pausedUntil_)
    if (until > now && (!next || until < *next)) next = until;
  for (const auto& [id, req] : active_) {
    for (const auto& nr : req->nodes) {
      if (nr.inFlight && (!next || nr.deadlineMs < *next)) next = nr.deadlineMs;
      if (nr.backupInFlight && (!next || nr.backupDeadlineMs < *next)) next = nr.backupDeadlineMs;
      if (nr.retryAtMs && (!next || *nr.retryAtMs < *next)) next = *nr.retryAtMs;
    }
  }
  return next;
}

void Service::expireDeadlines() {
  const std::int64_t now = now_();
  for (const auto& id : std::vector<std::string>(order_)) {
    auto it = active_.find(id);
    if (it == active_.end()) continue;
    RequestRun& req = *it->second;
    for (NodeId n = 0; n < req.nodes.size(); ++n) {
      NodeRun& nr = req.nodes[n];
      if (nr.backupInFlight && nr.backupDeadlineMs <= now) {
        ++stats_.lostAttempts;
        log("attempt_lost", id, req.dag.id(n), nr.backupAttempt, "", "backup's deadline passed");
        releaseBackup(req, n);
      }
      if (!nr.inFlight || nr.deadlineMs > now) continue;
      // The only lost-attempt detector (D10): no XAUTOCLAIM, no heartbeat dependence.
      ++stats_.lostAttempts;
      log("attempt_lost", id, req.dag.id(n), nr.attempt, "", "deadline passed");
      if (promoteBackup(req, n)) {
        log("backup_took_over", id, req.dag.id(n), nr.attempt, "", "primary's deadline passed");
        continue;
      }
      releaseSlot(req, n);
      scheduleRetry(req, n, std::nullopt, "deadline passed");
    }
    checkDone(req);
  }
}

void Service::startDueRetries() {
  const std::int64_t now = now_();
  for (const auto& id : order_) {
    auto it = active_.find(id);
    if (it == active_.end()) continue;
    RequestRun& req = *it->second;
    for (NodeId n = 0; n < req.nodes.size(); ++n) {
      NodeRun& nr = req.nodes[n];
      if (!nr.retryAtMs || *nr.retryAtMs > now) continue;
      nr.retryAtMs.reset();
      req.sched->retry(n);  // QUEUED|RUNNING -> RETRYING, back into the policy's ready set
      writeTask(req, n, "RETRYING");
    }
  }
}

void Service::scheduleRetry(RequestRun& req, NodeId n, std::optional<std::int64_t> retryAfterMs, const std::string& why) {
  NodeRun& nr = req.nodes[n];
  const std::string& rid = req.dag.requestId();
  const int tries = nr.attempt - nr.attemptBase - nr.extraAttempts;  // backups do not count (D56)
  if (tries >= config_.maxAttempts) {  // a restart starts a fresh budget (D44)
    failNode(req, n, req.dag.id(n) + " failed after " + std::to_string(nr.attempt) + " attempts: " + why);
    return;
  }
  std::int64_t delay = backoffFor(nr.capability, tries);
  if (retryAfterMs) delay = std::max(delay, *retryAfterMs);  // honour the provider's hint (8.6)
  nr.retryAtMs = now_() + delay;
  ++stats_.retries;
  writeTask(req, n, "RETRYING");
  log("retry_scheduled", rid, req.dag.id(n), nr.attempt, "", why + " (in " + std::to_string(delay) + " ms)");
}

std::int64_t Service::backoffFor(const std::string& capability, int attempt) {
  const CapabilityConfig& cap = capabilityConfig(capability);
  if (cap.backoffMs.empty()) return 0;
  const std::size_t i = std::min<std::size_t>(static_cast<std::size_t>(std::max(attempt, 1) - 1), cap.backoffMs.size() - 1);
  double delay = static_cast<double>(cap.backoffMs[i]);
  if (cap.jitter > 0) delay *= std::uniform_real_distribution<double>(1.0 - cap.jitter, 1.0 + cap.jitter)(rng_);
  return static_cast<std::int64_t>(delay);
}

// ------------------------------------------------------------------------------- dispatch

const CapabilityConfig& Service::capabilityConfig(const std::string& capability) const {
  auto it = config_.capabilities.find(capability);
  if (it == config_.capabilities.end()) throw std::invalid_argument("no configuration for capability '" + capability + "'");
  return it->second;
}

std::string Service::capabilityFor(TaskType type) const {
  auto it = config_.capabilityByType.find(std::string(toString(type)));
  return it != config_.capabilityByType.end() ? it->second : config_.defaultCapability;
}

std::size_t Service::inFlight(const std::string& capability) const {
  auto it = inFlight_.find(capability);
  return it == inFlight_.end() ? 0 : it->second;
}

bool Service::hasRoom(const std::string& capability) const {
  if (auto p = pausedUntil_.find(capability); p != pausedUntil_.end() && now_() < p->second) return false;
  const std::size_t cap = capabilityConfig(capability).maxInFlight;
  return cap == 0 || inFlight(capability) < cap;
}

// The request's next task, if it could be dispatched now: its own capability has room (D42),
// or it completes without a message (a late or restored result). Only the head is considered,
// so a head waiting for its capability holds back the request's other ready tasks.
std::optional<NodeId> Service::eligibleHead(RequestRun& req) {
  if (req.finished) return std::nullopt;
  auto head = req.sched->peekNext();
  if (!head) return std::nullopt;
  const NodeRun& nr = req.nodes[*head];
  if (nr.completeOnDispatch || hasRoom(nr.capability)) return head;
  return std::nullopt;
}

// Cross-request order (D41): true if request a's head should be dispatched before b's.
bool Service::before(const RequestRun& a, NodeId ha, const RequestRun& b, NodeId hb) const {
  if (config_.policy == "fair-share") {
    // The request with the smallest share of the capacity right now goes first, so a request
    // with long tasks cannot hold more than its share of slots once others are waiting.
    if (a.inFlight != b.inFlight) return a.inFlight < b.inFlight;
    if (a.lastServed != b.lastServed) return a.lastServed < b.lastServed;
    return a.arrival < b.arrival;
  }
  const std::uint64_t ta = a.sched->readyTicket(ha), tb = b.sched->readyTicket(hb);
  if (config_.policy == "critical-path") {
    const double pa = a.sched->policy().priority(ha), pb = b.sched->policy().priority(hb);
    if (pa != pb) return pa > pb;
  }
  return ta < tb;  // tickets are global, so this is "became ready first" across requests
}

void Service::dispatchAll() {
  if (draining_) return;  // D43
  // One task at a time: every request offers its head, the cross-request policy picks one.
  while (true) {
    RequestRun* next = nullptr;
    NodeId nextHead = 0;
    for (const auto& id : order_) {
      auto it = active_.find(id);
      if (it == active_.end()) continue;
      RequestRun& r = *it->second;
      auto head = eligibleHead(r);
      if (!head) continue;
      if (!next || before(r, *head, *next, nextHead)) {
        next = &r;
        nextHead = *head;
      }
    }
    if (!next) break;
    auto d = next->sched->dispatchNext();  // pops exactly the head peeked above
    if (!d) continue;
    next->lastServed = ++serveCounter_;
    dispatch(*next, *d);
  }
  sweepFinished();
}

void Service::dispatch(RequestRun& req, const Dispatch& d) {
  const Dag& g = req.dag;
  const std::string& rid = g.requestId();
  const TaskSpec& t = g.task(d.node);
  NodeRun& nr = req.nodes[d.node];

  if (nr.completeOnDispatch) {  // a late result arrived while RETRYING/READY, or a result restored after a restart
    nr.completeOnDispatch = false;
    completeNode(req, d.node, "", nr.attempt);
    checkDone(req);
    return;
  }
  nr.attempt = std::max(nr.attemptBase + d.attempt + nr.extraAttempts, nr.highest + 1);
  nr.highest = nr.attempt;
  nr.startedAtMs = 0;
  nr.speculated = false;

  const std::int64_t deadline = now_() + capabilityConfig(nr.capability).timeoutMs;
  nr.inFlight = true;
  nr.deadlineMs = deadline;
  ++inFlight_[nr.capability];
  ++req.inFlight;
  bus_.add(keys::taskStream(nr.capability), taskMessage(req, d.node, nr.attempt, deadline));
  ++stats_.dispatched;
  writeTask(req, d.node, "QUEUED");
  log("dispatched", rid, t.id, nr.attempt, "", keys::taskStream(nr.capability));
}

// One TaskMessage (contract #2). A join has no result of its own: a dependent of t2.join receives
// the subplan sinks' results (design doc 8.11), keyed by their spliced ids.
std::string Service::taskMessage(const RequestRun& req, NodeId n, int attempt, std::int64_t deadlineMs) const {
  const Dag& g = req.dag;
  const std::string& rid = g.requestId();
  const TaskSpec& t = g.task(n);
  json inputs = json::object();
  std::function<void(NodeId)> addInput = [&](NodeId p) {
    if (g.isJoin(p)) {
      for (NodeId pp : g.parents(p)) addInput(pp);
    } else {
      inputs[g.id(p)] = keys::resultKey(rid, g.id(p));
    }
  };
  for (NodeId p : g.parents(n)) addInput(p);
  json msg = {
      {"request_id", rid},
      {"task_id", t.id},
      {"attempt", attempt},
      {"attempt_id", rid + ":" + t.id + ":" + std::to_string(attempt)},
      {"type", std::string(toString(t.type))},
      {"instruction", t.instruction},
      {"inputs", inputs},
      {"deadline_ms", deadlineMs},
  };
  if (t.estimatedSeconds) msg["estimated_seconds"] = *t.estimatedSeconds;
  return msg.dump();
}

// ------------------------------------------------------------------------------- speculation (D56)

void Service::recordDuration(const std::string& type, std::int64_t ms) {
  if (ms < 0) return;
  auto& window = durations_[type];
  window.push_back(ms);
  constexpr std::size_t kWindow = 200;  // recent attempts only
  if (window.size() > kWindow) window.pop_front();
}

std::optional<double> Service::durationQuantile(const std::string& type) const {
  auto it = durations_.find(type);
  if (it == durations_.end() || it->second.size() < std::max<std::size_t>(config_.speculationMinSamples, 1))
    return std::nullopt;
  std::vector<std::int64_t> v(it->second.begin(), it->second.end());
  std::sort(v.begin(), v.end());
  const double pos = config_.speculationQuantile * static_cast<double>(v.size() - 1);  // linear interpolation
  const std::size_t lo = static_cast<std::size_t>(pos);
  const std::size_t hi = std::min(lo + 1, v.size() - 1);
  return static_cast<double>(v[lo]) + (pos - static_cast<double>(lo)) * static_cast<double>(v[hi] - v[lo]);
}

// Runs after dispatchAll, so backups only use capacity that no ready task wanted.
void Service::speculate() {
  if (!config_.speculation || draining_) return;
  const std::int64_t now = now_();
  for (const auto& id : order_) {
    auto it = active_.find(id);
    if (it == active_.end() || it->second->finished) continue;
    RequestRun& req = *it->second;
    for (NodeId n = 0; n < req.nodes.size(); ++n) {
      NodeRun& nr = req.nodes[n];
      if (!nr.inFlight || nr.speculated || nr.backupInFlight || nr.startedAtMs == 0) continue;
      if (req.sched->state().state(n) != TaskState::Running || req.dag.isJoin(n)) continue;
      const auto q = durationQuantile(std::string(toString(req.dag.task(n).type)));
      if (!q) continue;
      const double threshold = config_.speculationFactor * *q;
      const std::int64_t running = now - nr.startedAtMs;
      if (static_cast<double>(running) <= threshold || !hasRoom(nr.capability)) continue;
      dispatchBackup(req, n, running, threshold);
    }
  }
}

void Service::dispatchBackup(RequestRun& req, NodeId n, std::int64_t runningMs, double thresholdMs) {
  NodeRun& nr = req.nodes[n];
  nr.speculated = true;
  nr.backupAttempt = nr.highest + 1;
  nr.highest = nr.backupAttempt;
  ++nr.extraAttempts;
  nr.backupInFlight = true;
  nr.backupDeadlineMs = now_() + capabilityConfig(nr.capability).timeoutMs;
  ++inFlight_[nr.capability];
  ++req.inFlight;
  bus_.add(keys::taskStream(nr.capability), taskMessage(req, n, nr.backupAttempt, nr.backupDeadlineMs));
  ++stats_.dispatched;
  ++stats_.speculativeAttempts;
  writeTask(req, n, "RUNNING");  // records the new highest attempt number, for a restart (D44)
  log("speculated", req.dag.requestId(), req.dag.id(n), nr.backupAttempt, "",
      "attempt " + std::to_string(nr.attempt) + " running " + std::to_string(runningMs) + " ms > " +
          std::to_string(static_cast<std::int64_t>(thresholdMs)) + " ms");
}

void Service::releaseBackup(RequestRun& req, NodeId n) {
  NodeRun& nr = req.nodes[n];
  if (!nr.backupInFlight) return;
  nr.backupInFlight = false;
  auto& count = inFlight_[nr.capability];
  if (count > 0) --count;
  if (req.inFlight > 0) --req.inFlight;
}

// The primary failed or was lost while a backup runs: the backup becomes the primary, with its
// slot and deadline. No retry, and the retry budget is unchanged. False if there is no backup.
bool Service::promoteBackup(RequestRun& req, NodeId n) {
  NodeRun& nr = req.nodes[n];
  if (!nr.backupInFlight) return false;
  releaseSlot(req, n);  // the primary's slot
  nr.attempt = nr.backupAttempt;
  nr.deadlineMs = nr.backupDeadlineMs;
  nr.inFlight = true;   // takes over the backup's slot (already counted)
  nr.backupInFlight = false;
  ++stats_.backupTakeovers;
  return true;
}

void Service::releaseSlot(RequestRun& req, NodeId n) {
  NodeRun& nr = req.nodes[n];
  if (!nr.inFlight) return;
  nr.inFlight = false;
  auto& count = inFlight_[nr.capability];
  if (count > 0) --count;
  if (req.inFlight > 0) --req.inFlight;
}

// ------------------------------------------------------------------------------- outcomes

void Service::completeNode(RequestRun& req, NodeId n, const std::string& worker, int attempt) {
  // An expand task's first committed result is spliced before the task completes, so completing
  // it releases the new tasks (D53). Only reached once per task, so a duplicate or late result can
  // never splice twice. If the subplan is rejected, the expand task fails instead.
  if (req.dag.task(n).type == TaskType::Expand && !req.dag.isJoin(n) && !spliceExpansion(req, n, worker, attempt))
    return;
  const std::vector<NodeId> ready = req.sched->complete(n);
  writeTask(req, n, "COMPLETED");
  log(req.dag.isJoin(n) ? "join_completed" : "completed", req.dag.requestId(), req.dag.id(n), attempt, worker);
  for (NodeId c : ready) writeTask(req, c, "READY");
}

bool Service::spliceExpansion(RequestRun& req, NodeId e, const std::string& worker, int attempt) {
  Dag& g = req.dag;
  const std::string rid = g.requestId();
  const std::string eid = g.id(e);
  auto reject = [&](const std::string& why) {
    ++stats_.rejectedExpansions;
    log("expansion_rejected", rid, eid, attempt, worker, why);
    failNode(req, e, eid + " expansion rejected: " + why);  // D5: its dependents are cancelled
    return false;
  };
  auto describe = [](const std::vector<ValidationIssue>& issues) {
    std::string out;
    for (std::size_t i = 0; i < issues.size(); ++i) out += (i ? "; " : "") + issues[i].code + ": " + issues[i].message;
    return out;
  };

  if (req.expansions >= config_.maxExpansions)
    return reject("the request already has " + std::to_string(req.expansions) + " expansions (limit " +
                  std::to_string(config_.maxExpansions) + ")");
  const std::string key = keys::resultKey(rid, eid);
  const auto raw = bus_.get(key);
  if (!raw) return reject("no committed result at " + key);
  try {
    json v = json::parse(*raw);
    if (v.is_object() && v.contains("artifact"))
      return reject("the subplan was stored as an artifact (over 64 KB), which atsd cannot read");
  } catch (const json::exception&) {
  }
  SubplanResult sub = parseSubplan(*raw, eid);
  if (!sub.ok()) return reject("invalid subplan: " + describe(sub.issues));
  const std::size_t realTasks = g.size() - static_cast<std::size_t>(req.expansions);  // joins excluded
  if (realTasks + sub.tasks.size() > config_.maxRequestTasks)
    return reject(std::to_string(sub.tasks.size()) + " more tasks would exceed " +
                  std::to_string(config_.maxRequestTasks) + " tasks in the request");
  DagExpandResult r = g.expand(e, sub.tasks);
  if (!r.ok()) return reject("invalid subplan: " + describe(r.issues));

  ++req.expansions;
  ++stats_.expansions;
  stats_.splicedTasks += r.added.size();
  req.nodes.resize(g.size());
  std::vector<NodeId> fresh = r.added;
  fresh.push_back(*r.join);
  std::vector<std::pair<std::string, std::string>> fields;
  std::string names;
  for (NodeId n : fresh) {
    NodeRun& nr = req.nodes[n];
    nr.capability = capabilityFor(g.task(n).type);
    if (n == *r.join) {
      nr.completeOnDispatch = true;  // completed inside atsd the moment it is ready; never dispatched
    } else {
      if (std::find(req.capabilities.begin(), req.capabilities.end(), nr.capability) == req.capabilities.end())
        req.capabilities.push_back(nr.capability);
      if (req.resumed) restoreNode(req, n);  // re-splicing after a restart: same ids, keep progress
      names += (names.empty() ? "" : ", ") + g.id(n);
    }
    fields.emplace_back("task:" + g.id(n), taskStateJson("WAITING", nr.attempt));
  }
  fields.emplace_back("updated_ms", std::to_string(now_()));
  bus_.hset(keys::requestKey(rid), fields);
  req.sched->onExpanded(e, bottomLevels(g, estimatedDurations(g, config_.defaultSeconds)));

  std::string rewired;
  for (NodeId c : r.rewired) rewired += (rewired.empty() ? "" : ", ") + g.id(c);
  log("graph_expanded", rid, eid, attempt, worker,
      std::to_string(r.added.size()) + " tasks: " + names + "; " + g.id(*r.join) + " -> " +
          (rewired.empty() ? "(none)" : rewired));
  return true;
}

void Service::failNode(RequestRun& req, NodeId n, const std::string& reason) {
  NodeRun& nr = req.nodes[n];
  nr.retryAtMs.reset();
  const std::vector<NodeId> cancelled = req.sched->fail(n);  // D5: descendants are cancelled
  if (req.error.empty()) req.error = reason;
  writeTask(req, n, "FAILED");
  log("failed", req.dag.requestId(), req.dag.id(n), nr.attempt, "", reason);
  for (NodeId c : cancelled) {
    req.nodes[c].retryAtMs.reset();
    writeTask(req, c, "CANCELLED");
    log("cancelled", req.dag.requestId(), req.dag.id(c));
  }
}

void Service::checkDone(RequestRun& req) {
  if (req.finished || !req.sched->isDone()) return;
  req.finished = true;
  const bool ok = req.sched->succeeded();
  const std::string& rid = req.dag.requestId();
  std::vector<std::pair<std::string, std::string>> fields{{"status", ok ? "SUCCEEDED" : "FAILED"},
                                                          {"updated_ms", std::to_string(now_())}};
  if (!ok) fields.emplace_back("error", req.error);
  bus_.hset(keys::requestKey(rid), fields);
  if (!req.entryId.empty()) bus_.ack(keys::kRequestsStream, keys::kSchedulerGroup, req.entryId);  // D44
  ++(ok ? stats_.succeeded : stats_.failed);
  log(ok ? "request_succeeded" : "request_failed", rid, "", 0, "", req.error);
}

void Service::sweepFinished() {
  for (auto it = order_.begin(); it != order_.end();) {
    auto a = active_.find(*it);
    if (a != active_.end() && a->second->finished) {
      finished_.insert(*it);
      active_.erase(a);
      it = order_.erase(it);
    } else {
      ++it;
    }
  }
}

// ------------------------------------------------------------------------------- output

void Service::writeTask(RequestRun& req, NodeId n, const std::string& state) {
  bus_.hset(keys::requestKey(req.dag.requestId()),
            {{"task:" + req.dag.id(n), taskStateJson(state, std::max(req.nodes[n].attempt, req.nodes[n].highest))},
             {"updated_ms", std::to_string(now_())}});
}

void Service::log(const std::string& event, const std::string& request, const std::string& task, int attempt,
                  const std::string& worker, const std::string& detail) {
  if (!eventLog_) return;
  json line = {{"ts_ms", now_()}, {"event", event}, {"request", request}};
  if (!task.empty()) line["task"] = task;
  if (attempt) line["attempt"] = attempt;
  if (!worker.empty()) line["worker"] = worker;
  if (!detail.empty()) line["detail"] = detail;
  *eventLog_ << line.dump() << '\n';
  eventLog_->flush();
}

void Service::diag(const std::string& message) {
  if (diag_) *diag_ << "atsd: " << message << '\n';
}

std::vector<std::string> servicePolicyNames() { return {"fair-share", "critical-path", "fifo"}; }

}  // namespace ats
