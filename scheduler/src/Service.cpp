#include "ats/Service.hpp"

#include <algorithm>
#include <exception>
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
  bool inFlight = false;                 // the latest attempt holds a capability slot and a deadline
  std::int64_t deadlineMs = 0;
  std::optional<std::int64_t> retryAtMs;  // set while waiting out a backoff
  bool completeOnDispatch = false;       // a late result arrived while RETRYING (see handleEvent)
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
  if (!now_) throw std::invalid_argument("Service needs a clock");
}

Service::~Service() = default;

void Service::setup() {
  bus_.createGroup(keys::kRequestsStream, keys::kSchedulerGroup);
  bus_.createGroup(keys::kEventsStream, keys::kSchedulerGroup);
}

std::size_t Service::pollOnce(int maxBlockMs) {
  int block = maxBlockMs;
  if (auto next = nextTimerMs()) {
    const std::int64_t wait = *next - now_();
    block = static_cast<int>(std::clamp<std::int64_t>(wait, 0, maxBlockMs));
  }
  auto entries = bus_.readGroup(keys::kSchedulerGroup, config_.consumer,
                                {keys::kRequestsStream, keys::kEventsStream}, 100, block);
  for (const auto& e : entries) {
    try {
      if (e.stream == keys::kRequestsStream) submitPlanFile(e.data);
      else handleEvent(e.data);
    } catch (const std::exception& ex) {  // a bad entry or a bug must not stop the service
      diag("error handling " + e.stream + " " + e.id + ": " + ex.what());
    }
    bus_.ack(e.stream, keys::kSchedulerGroup, e.id);
  }
  tick();
  return entries.size();
}

// ------------------------------------------------------------------------------- intake

void Service::submitPlanFile(std::string_view text) {
  std::string requestId;
  try {
    json doc = json::parse(text);
    if (doc.is_object() && doc.contains("request_id") && doc["request_id"].is_string())
      requestId = doc["request_id"].get<std::string>();
  } catch (const json::exception&) {
  }
  if (!requestId.empty() && (active_.count(requestId) || finished_.count(requestId))) {
    diag("duplicate request " + requestId + " ignored");
    return;
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
    return;
  }
  if (requestId.find(':') != std::string::npos) {  // attempt_id must split unambiguously (contract #2)
    reject(requestId, "SCHEMA: request_id must not contain ':'");
    return;
  }

  auto req = std::make_unique<RequestRun>(std::move(*dag));
  const Dag& g = req->dag;
  req->sched = std::make_unique<Scheduler>(g, makePolicy(config_.policy, g, estimatedDurations(g, config_.defaultSeconds)));
  req->nodes.resize(g.size());
  for (NodeId n = 0; n < g.size(); ++n) {
    req->nodes[n].capability = capabilityFor(g.task(n).type);
    if (std::find(req->capabilities.begin(), req->capabilities.end(), req->nodes[n].capability) ==
        req->capabilities.end())
      req->capabilities.push_back(req->nodes[n].capability);
  }
  const auto sinks = g.sinks();
  req->sink = sinks.size() == 1 ? g.id(sinks.front()) : "";

  std::vector<std::pair<std::string, std::string>> fields{
      {"status", "RUNNING"}, {"sink", req->sink}, {"updated_ms", std::to_string(now_())}};
  for (NodeId n = 0; n < g.size(); ++n) fields.emplace_back("task:" + g.id(n), taskStateJson("WAITING", 0));
  bus_.hset(keys::requestKey(requestId), fields);

  RequestRun& r = *req;
  active_.emplace(requestId, std::move(req));
  order_.push_back(requestId);
  ++stats_.ingested;
  log("request_accepted", requestId, "", 0, "", std::to_string(g.size()) + " tasks");

  r.sched->start();
  for (NodeId n = 0; n < g.size(); ++n)
    if (r.sched->state().state(n) == TaskState::Ready) writeTask(r, n, "READY");
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
  if (!node || attempt > req.nodes[*node].attempt) {
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
    } else if (st == TaskState::Retrying) {
      // Already back in the ready set, which the state machine cannot complete from: finish it
      // the moment the policy dispatches it again, without sending a message.
      nr.completeOnDispatch = true;
      log("late_result", rid, tid, attempt, worker, "completes on next dispatch");
    } else {  // QUEUED or RUNNING, possibly waiting out a backoff
      if (nr.inFlight) releaseSlot(req, n);
      nr.retryAtMs.reset();
      completeNode(req, n, worker, attempt);
      if (ev.value("result_kind", "output") == "subplan")
        log("subplan_not_spliced", rid, tid, attempt, worker, "dynamic expansion is Day 6");
    }
  } else if (kind == "task_failed") {
    if (attempt == nr.attempt && nr.inFlight && (st == TaskState::Queued || st == TaskState::Running)) {
      releaseSlot(req, n);
      const std::string message = ev.value("message", std::string{});
      if (ev.value("error_class", "") == "permanent") {
        failNode(req, n, tid + " failed permanently: " + message);
      } else {
        std::optional<std::int64_t> retryAfter;
        if (ev.contains("retry_after_ms") && ev["retry_after_ms"].is_number_integer())
          retryAfter = ev["retry_after_ms"].get<std::int64_t>();
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
  sweepFinished();
}

std::optional<std::int64_t> Service::nextTimerMs() const {
  std::optional<std::int64_t> next;
  for (const auto& [id, req] : active_) {
    for (const auto& nr : req->nodes) {
      if (nr.inFlight && (!next || nr.deadlineMs < *next)) next = nr.deadlineMs;
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
      if (!nr.inFlight || nr.deadlineMs > now) continue;
      // The only lost-attempt detector (D10): no XAUTOCLAIM, no heartbeat dependence.
      releaseSlot(req, n);
      ++stats_.lostAttempts;
      log("attempt_lost", id, req.dag.id(n), nr.attempt, "", "deadline passed");
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
  if (nr.attempt >= config_.maxAttempts) {
    failNode(req, n, req.dag.id(n) + " failed after " + std::to_string(nr.attempt) + " attempts: " + why);
    return;
  }
  std::int64_t delay = backoffFor(nr.capability, nr.attempt);
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
  const std::size_t cap = capabilityConfig(capability).maxInFlight;
  return cap == 0 || inFlight(capability) < cap;
}

// The policy pops the next task without saying its capability first, so a request is only
// asked for a task while every capability it uses has room. With one capability per request
// (all-sim or all-llm, as configured today) this is exact; mixed requests are held back
// conservatively, never over the cap.
bool Service::canDispatch(const RequestRun& req) const {
  if (!req.sched->hasReady()) return false;
  for (const auto& c : req.capabilities)
    if (!hasRoom(c)) return false;
  return true;
}

void Service::dispatchAll() {
  // One task at a time, always from the eligible request served least recently (a new request
  // first, ties by arrival), so a large request cannot starve a small one while capacity is
  // short. Within a request the policy decides. (FairShare proper is Day 4.)
  while (true) {
    RequestRun* next = nullptr;
    for (const auto& id : order_) {
      auto it = active_.find(id);
      if (it == active_.end() || it->second->finished || !canDispatch(*it->second)) continue;
      if (!next || it->second->lastServed < next->lastServed) next = it->second.get();
    }
    if (!next) break;
    auto d = next->sched->dispatchNext();
    if (!d) continue;  // the policy held only stale entries and is now empty, so this request drops out
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
  nr.attempt = d.attempt;

  if (nr.completeOnDispatch) {  // a late result arrived while this task was RETRYING
    nr.completeOnDispatch = false;
    completeNode(req, d.node, "", d.attempt - 1);
    checkDone(req);
    return;
  }

  const std::int64_t deadline = now_() + capabilityConfig(nr.capability).timeoutMs;
  json inputs = json::object();
  for (NodeId p : g.parents(d.node)) inputs[g.id(p)] = keys::resultKey(rid, g.id(p));
  json msg = {
      {"request_id", rid},
      {"task_id", t.id},
      {"attempt", d.attempt},
      {"attempt_id", rid + ":" + t.id + ":" + std::to_string(d.attempt)},
      {"type", std::string(toString(t.type))},
      {"instruction", t.instruction},
      {"inputs", inputs},
      {"deadline_ms", deadline},
  };
  if (t.estimatedSeconds) msg["estimated_seconds"] = *t.estimatedSeconds;

  nr.inFlight = true;
  nr.deadlineMs = deadline;
  ++inFlight_[nr.capability];
  bus_.add(keys::taskStream(nr.capability), msg.dump());
  ++stats_.dispatched;
  writeTask(req, d.node, "QUEUED");
  log("dispatched", rid, t.id, d.attempt, "", keys::taskStream(nr.capability));
}

void Service::releaseSlot(RequestRun& req, NodeId n) {
  NodeRun& nr = req.nodes[n];
  if (!nr.inFlight) return;
  nr.inFlight = false;
  auto& count = inFlight_[nr.capability];
  if (count > 0) --count;
}

// ------------------------------------------------------------------------------- outcomes

void Service::completeNode(RequestRun& req, NodeId n, const std::string& worker, int attempt) {
  const std::vector<NodeId> ready = req.sched->complete(n);
  writeTask(req, n, "COMPLETED");
  log("completed", req.dag.requestId(), req.dag.id(n), attempt, worker);
  for (NodeId c : ready) writeTask(req, c, "READY");
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
            {{"task:" + req.dag.id(n), taskStateJson(state, req.nodes[n].attempt)},
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

}  // namespace ats
