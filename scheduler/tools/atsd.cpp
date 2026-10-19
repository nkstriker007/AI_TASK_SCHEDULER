// atsd: the scheduler service (design doc 8.1). Runs until SIGTERM / SIGINT.
//
// Reads validated plan files from requests:new and worker events from task:events (consumer
// group "scheduler"), dispatches TaskMessages to tasks:<capability>, owns attempt deadlines and
// retries, and writes request:<id> (D25). All scheduling logic is in ats::Service; this file is
// configuration, the Redis connection, the event log file and the main loop.
//
// Configuration (environment):
//   REDIS_URL               redis://host:port/db            default redis://127.0.0.1:6379/0
//   ATSD_CAPABILITY         send every task to this capability (e.g. "sim"); default: per type, all "llm"
//   ATSD_POLICY             fifo | critical-path            default fifo
//   ATSD_MAX_ATTEMPTS       attempts per task               default 3
//   ATSD_LLM_MAX_INFLIGHT   llm attempts in flight          default 8   (0 = unlimited)
//   ATSD_SIM_MAX_INFLIGHT   sim attempts in flight          default 0   (unlimited)
//   ATSD_LLM_TIMEOUT_MS     llm attempt deadline            default 120000
//   ATSD_SIM_TIMEOUT_MS     sim attempt deadline            default 300000
//   ATSD_RUNS_DIR           event log: <dir>/<run_id>/events.jsonl; empty disables   default runs
//   ATSD_CONSUMER           consumer name in the group      default atsd
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

std::atomic<bool> g_stop{false};
void onSignal(int) { g_stop = true; }

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
    cfg.policy = env("ATSD_POLICY", "fifo");
    cfg.maxAttempts = static_cast<int>(envInt("ATSD_MAX_ATTEMPTS", 3));
    if (const std::string cap = env("ATSD_CAPABILITY", ""); !cap.empty()) cfg.defaultCapability = cap;
    cfg.capabilities["llm"].maxInFlight = static_cast<std::size_t>(envInt("ATSD_LLM_MAX_INFLIGHT", 8));
    cfg.capabilities["sim"].maxInFlight = static_cast<std::size_t>(envInt("ATSD_SIM_MAX_INFLIGHT", 0));
    cfg.capabilities["llm"].timeoutMs = envInt("ATSD_LLM_TIMEOUT_MS", 120'000);
    cfg.capabilities["sim"].timeoutMs = envInt("ATSD_SIM_TIMEOUT_MS", 300'000);
    if (!cfg.capabilities.count(cfg.defaultCapability))
      throw std::invalid_argument("ATSD_CAPABILITY must be llm or sim (got '" + cfg.defaultCapability + "')");
    // makePolicy would reject an unknown name per request; fail fast at startup instead.
    bool knownPolicy = false;
    for (const auto& p : ats::policyNames()) knownPolicy |= (p == cfg.policy);
    if (!knownPolicy) throw std::invalid_argument("ATSD_POLICY must be fifo or critical-path (got '" + cfg.policy + "')");

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

    ats::Service service(bus, cfg, nowMs, eventLog.get(), &std::cerr);
    service.setup();

    std::signal(SIGTERM, onSignal);
    std::signal(SIGINT, onSignal);
    std::cerr << "atsd: up | redis " << url << " | capability " << cfg.defaultCapability << " | policy "
              << cfg.policy << " | max attempts " << cfg.maxAttempts << " | llm cap "
              << cfg.capabilities["llm"].maxInFlight << " | events " << (logPath.empty() ? "off" : logPath) << "\n";

    while (!g_stop) service.pollOnce(200);  // short block so a stop signal is noticed quickly

    const auto& s = service.stats();
    std::cerr << "atsd: stopped | requests " << s.ingested << " (" << s.succeeded << " succeeded, " << s.failed
              << " failed, " << s.rejected << " rejected, " << service.activeRequests() << " unfinished) | "
              << s.dispatched << " dispatches, " << s.retries << " retries, " << s.lostAttempts << " lost attempts, "
              << s.duplicateCompletions << " duplicate completions\n";
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "atsd: " << e.what() << "\n";
    return 1;
  }
}
