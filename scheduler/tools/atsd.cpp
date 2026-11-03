// atsd: the scheduler service (design doc 8.1). Runs until SIGTERM / SIGINT, then drains.
//
// Reads validated plan files from requests:new and worker events from task:events (consumer
// group "scheduler"), dispatches TaskMessages to tasks:<capability>, owns attempt deadlines and
// retries, and writes request:<id> (D25). All scheduling logic is in ats::Service; this file is
// configuration, the Redis connection, the event log file and the main loop.
//
// Configuration (environment):
//   REDIS_URL               redis://host:port/db            default redis://127.0.0.1:6379/0
//   ATSD_CAPABILITY         send every task to this capability (e.g. "sim"); default: per type, all "llm"
//   ATSD_POLICY             fair-share | critical-path | fifo   default fair-share (D41)
//   ATSD_MAX_ATTEMPTS       attempts per task               default 3
//   ATSD_LLM_MAX_INFLIGHT   llm attempts in flight          default 8   (0 = unlimited)
//   ATSD_SIM_MAX_INFLIGHT   sim attempts in flight          default 0   (unlimited)
//   ATSD_LLM_TIMEOUT_MS     llm attempt deadline            default 120000
//   ATSD_SIM_TIMEOUT_MS     sim attempt deadline            default 300000
//   ATSD_RUNS_DIR           event log: <dir>/<run_id>/events.jsonl; empty disables   default runs
//   ATSD_CONSUMER           consumer name in the group      default atsd  (keep it stable: D44)
//   ATSD_DRAIN_MS           on SIGTERM, how long to wait for attempts in flight   default 30000
//   ATSD_JANITOR_INTERVAL_MS  how often to acknowledge dead pending task entries (D49; 0 = off)   default 30000
//   ATSD_JANITOR_MIN_IDLE_MS  only entries pending at least this long             default 60000
//
// Shutdown (D43): the first SIGTERM/SIGINT stops intake and dispatch and waits up to
// ATSD_DRAIN_MS for the attempts in flight to report; a second signal stops at once. Requests
// that are not finished stay pending in requests:new, and the next atsd with the same consumer
// name resumes them, skipping tasks whose result is already committed (D44). A crash (kill -9)
// is recovered the same way.
//
// Exit codes: 0 clean shutdown, 1 configuration or Redis error.
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <ctime>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>

#include "ats/RedisBus.hpp"
#include "ats/Service.hpp"

namespace {

std::atomic<int> g_signals{0};
void onSignal(int) { g_signals.fetch_add(1); }

std::string env(const char* name, const std::string& fallback) {
  const char* v = std::getenv(name);
  return v ? std::string(v) : fallback;
}

long long envInt(const char* name, long long fallback) {
  const char* v = std::getenv(name);
  if (!v || !*v) return fallback;
  try {
    std::size_t used = 0;
    long long n = std::stoll(v, &used);
    if (used == std::string(v).size() && n >= 0) return n;
  } catch (const std::exception&) {
  }
  throw std::invalid_argument(std::string(name) + " must be a non-negative integer (got '" + v + "')");
}

std::int64_t nowMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch()).count();
}

std::string runId() {
  std::time_t t = std::time(nullptr);
  char buf[32];
  std::strftime(buf, sizeof buf, "%Y%m%dT%H%M%SZ", std::gmtime(&t));
  return buf;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc > 1) {
    std::cerr << "usage: atsd   (configured through the environment; see the header of tools/atsd.cpp)\n";
    return std::string(argv[1]) == "--help" || std::string(argv[1]) == "-h" ? 0 : 1;
  }
  try {
    ats::ServiceConfig cfg;
    cfg.consumer = env("ATSD_CONSUMER", "atsd");
    cfg.policy = env("ATSD_POLICY", "fair-share");
    cfg.maxAttempts = static_cast<int>(envInt("ATSD_MAX_ATTEMPTS", 3));
    if (const std::string cap = env("ATSD_CAPABILITY", ""); !cap.empty()) cfg.defaultCapability = cap;
    cfg.capabilities["llm"].maxInFlight = static_cast<std::size_t>(envInt("ATSD_LLM_MAX_INFLIGHT", 8));
    cfg.capabilities["sim"].maxInFlight = static_cast<std::size_t>(envInt("ATSD_SIM_MAX_INFLIGHT", 0));
    cfg.capabilities["llm"].timeoutMs = envInt("ATSD_LLM_TIMEOUT_MS", 120'000);
    cfg.capabilities["sim"].timeoutMs = envInt("ATSD_SIM_TIMEOUT_MS", 300'000);
    if (!cfg.capabilities.count(cfg.defaultCapability))
      throw std::invalid_argument("ATSD_CAPABILITY must be llm or sim (got '" + cfg.defaultCapability + "')");
    const std::int64_t drainMs = envInt("ATSD_DRAIN_MS", 30'000);
    cfg.janitorIntervalMs = envInt("ATSD_JANITOR_INTERVAL_MS", 30'000);
    cfg.janitorMinIdleMs = envInt("ATSD_JANITOR_MIN_IDLE_MS", 60'000);

    const std::string url = env("REDIS_URL", "redis://127.0.0.1:6379/0");
    ats::RedisBus bus(ats::parseRedisUrl(url));
    bus.ping();

    std::unique_ptr<std::ofstream> eventLog;
    const std::string runsDir = env("ATSD_RUNS_DIR", "runs");
    std::string logPath;
    if (!runsDir.empty()) {
      const auto dir = std::filesystem::path(runsDir) / runId();
      std::filesystem::create_directories(dir);
      logPath = (dir / "events.jsonl").string();
      eventLog = std::make_unique<std::ofstream>(logPath, std::ios::app);
    }

    ats::Service service(bus, cfg, nowMs, eventLog.get(), &std::cerr);  // throws on an unknown ATSD_POLICY
    service.setup();
    std::signal(SIGTERM, onSignal);
    std::signal(SIGINT, onSignal);
    if (const std::size_t resumed = service.recoverPending())
      std::cerr << "atsd: resumed " << resumed << " unfinished request(s) from a previous run ("
                << service.stats().restoredTasks << " tasks already completed)\n";

    std::cerr << "atsd: up | redis " << url << " | capability " << cfg.defaultCapability << " | policy "
              << cfg.policy << " | max attempts " << cfg.maxAttempts << " | llm cap "
              << cfg.capabilities["llm"].maxInFlight << " | events " << (logPath.empty() ? "off" : logPath) << "\n";

    while (g_signals == 0) service.pollOnce(200);  // short block so a stop signal is noticed quickly

    // Clean shutdown (D43): let the attempts in flight report, so their results are recorded
    // and the next atsd does not run them again.
    service.beginDrain();
    const std::int64_t drainUntil = nowMs() + drainMs;
    while (service.inFlightTotal() > 0 && g_signals < 2 && nowMs() < drainUntil) service.pollOnce(100);
    if (service.inFlightTotal() == 0)
      std::cerr << "atsd: drained\n";
    else
      std::cerr << "atsd: stopping with " << service.inFlightTotal()
                << " attempts in flight; the next atsd resumes them\n";

    const auto& s = service.stats();
    std::cerr << "atsd: stopped | requests " << s.ingested << " (" << s.succeeded << " succeeded, " << s.failed
              << " failed, " << s.rejected << " rejected, " << service.activeRequests() << " unfinished) | "
              << s.dispatched << " dispatches, " << s.retries << " retries, " << s.lostAttempts << " lost attempts, "
              << s.duplicateCompletions << " duplicate completions, " << s.janitorAcked
              << " dead pending entries acknowledged\n";
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "atsd: " << e.what() << "\n";
    return 1;
  }
}
