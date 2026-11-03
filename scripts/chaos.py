"""Chaos run against the Docker stack (design doc 10.4, E6): submit requests through the API,
kill worker containers while they run, then check the outcome.

    make chaos                                    # sim stack with injected faults + this script
    cd worker && uv run python ../scripts/chaos.py --kills 3 --kill-every 4
    cd worker && uv run python ../scripts/chaos.py --text "Compare AMD and Intel" --kills 1   # real run (make up-llm)

Each kill is `docker kill` (SIGKILL: no cleanup, no event, no XACK), so atsd's attempt
deadline is what recovers the attempt. The killed container is started again after
--restart-after seconds, as a supervisor would. Passes (exit 0) when every request
SUCCEEDED and no task has more than one committed result.

Run it with the worker environment (it needs the redis package) from the repo root or worker/.
"""

from __future__ import annotations

import argparse
import json
import random
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import Counter
from pathlib import Path

import redis

REPO = Path(__file__).resolve().parents[1]
FIXTURES = ["company_research", "diamond", "chain", "join", "fork"]


def call(api: str, method: str, path: str, body: dict | None = None) -> tuple[int, dict | str]:
    req = urllib.request.Request(api + path, method=method, data=json.dumps(body).encode() if body else None,
                                 headers={"content-type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            text = r.read().decode()
            code = r.status
    except urllib.error.HTTPError as e:
        text, code = e.read().decode(), e.code
    try:
        return code, json.loads(text)
    except json.JSONDecodeError:
        return code, text


def compose(*args: str) -> str:
    return subprocess.run(["docker", "compose", *args], cwd=REPO, capture_output=True, text=True, check=True).stdout


def worker_containers() -> list[str]:
    return [c for c in compose("ps", "-q", "worker").split() if c]


def busy_containers(r: redis.Redis, running: list[str]) -> list[str]:
    """Containers whose worker reports an attempt in progress. A worker's id is
    w-<hostname>-<pid> and a container's hostname is its short id (heartbeat: worker:<id>)."""
    busy = []
    for key in r.scan_iter("worker:*"):
        entry = r.hgetall(key)
        if int(entry.get("in_flight", 0)) > 0:
            host = key.split(":", 1)[1].removeprefix("w-").rsplit("-", 1)[0]
            busy += [c for c in running if c.startswith(host)]
    return busy


def killer(stop: threading.Event, kills: int, every: float, restart_after: float, log: list[str],
           r: redis.Redis, busy_only: bool) -> None:
    rng = random.Random()
    for _ in range(kills):
        if stop.wait(every):
            return
        running = worker_containers()
        candidates = running
        if busy_only:  # kill a worker in the middle of a task, so there is something to recover
            for _ in range(50):
                candidates = busy_containers(r, running)
                if candidates or stop.wait(0.2):
                    break
        if not candidates:
            continue
        victim = rng.choice(candidates)
        subprocess.run(["docker", "kill", victim], capture_output=True)
        log.append(f"{time.strftime('%H:%M:%S')} killed worker {victim[:12]}")
        print(f"  chaos: killed worker container {victim[:12]}", flush=True)
        if restart_after >= 0:
            def restart(cid=victim):
                time.sleep(restart_after)
                subprocess.run(["docker", "start", cid], capture_output=True)
                print(f"  chaos: restarted {cid[:12]}", flush=True)
            threading.Thread(target=restart, daemon=True).start()


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--api", default="http://localhost:8000")
    p.add_argument("--redis", default="redis://localhost:6379/0")
    p.add_argument("--requests", type=int, default=5, help="hand-written fixture plans to submit (sim)")
    p.add_argument("--text", action="append", default=[], help="typed request(s) instead, planned by the LLM")
    p.add_argument("--kills", type=int, default=3)
    p.add_argument("--kill-every", type=float, default=3.0, help="seconds between kills")
    p.add_argument("--restart-after", type=float, default=2.0, help="restart a killed worker after N s (-1: never)")
    p.add_argument("--any-worker", action="store_true", help="kill any worker, not only one running a task")
    p.add_argument("--timeout", type=float, default=600)
    a = p.parse_args()

    code, health = call(a.api, "GET", "/healthz")
    if code != 200:
        print(f"API not reachable at {a.api} ({code}); start the stack first", file=sys.stderr)
        return 2
    print(f"workers running: {len(worker_containers())}")

    t0 = time.time()
    rids = []
    submissions = [("POST", "/requests", {"text": t}) for t in a.text] or [
        ("POST", "/plans", json.loads((REPO / f"examples/plans/{FIXTURES[i % len(FIXTURES)]}.json").read_text())["plan"])
        for i in range(a.requests)]
    for method, path, body in submissions:
        code, resp = call(a.api, method, path, body)
        if code != 202:
            print(f"submit failed: {code} {resp}", file=sys.stderr)
            return 1
        rids.append(resp["request_id"])
    print(f"submitted {len(rids)} request(s): {', '.join(rids)}")

    r = redis.Redis.from_url(a.redis, decode_responses=True)
    stop, kill_log = threading.Event(), []
    k = threading.Thread(target=killer, args=(stop, a.kills, a.kill_every, a.restart_after, kill_log, r,
                                              not a.any_worker), daemon=True)
    k.start()

    final: dict[str, dict] = {}
    while len(final) < len(rids) and time.time() - t0 < a.timeout:
        for rid in rids:
            if rid not in final:
                code, d = call(a.api, "GET", f"/requests/{rid}")
                if code == 200 and isinstance(d, dict) and d["status"] in ("SUCCEEDED", "FAILED", "REJECTED"):
                    final[rid] = d
        time.sleep(1)
    stop.set()
    k.join(timeout=5)
    elapsed = time.time() - t0

    # Outcome from Redis: commits and completions per task, from task:events.
    committed, completions = Counter(), Counter()
    for _, fields in r.xrange("task:events"):
        ev = json.loads(fields["data"])
        req, task, _ = ev["attempt_id"].split(":")
        if req in rids and ev["event"] == "task_completed":
            completions[(req, task)] += 1
            committed[(req, task)] += bool(ev["committed"])
    duplicate_commits = sum(n - 1 for n in committed.values() if n > 1)
    duplicate_executions = sum(n - 1 for n in completions.values() if n > 1)
    retried_tasks = sum(t["attempt"] > 1 for d in final.values() for t in d["tasks"])

    # Retries and lost attempts as atsd logged them.
    logged = Counter()
    for log_file in sorted((REPO / "runs").glob("*/events.jsonl")):
        for line in log_file.read_text().splitlines():
            ev = json.loads(line)
            if ev.get("request") in rids:
                logged[ev["event"]] += 1

    print(f"\n== chaos report ({elapsed:.0f}s) ==")
    for rid in rids:
        d = final.get(rid)
        print(f"  {rid}: {d['status'] if d else 'TIMED OUT'}"
              + (f"  ({d['error'][:80]})" if d and d.get('error') else ""))
    print(f"  workers killed:            {len(kill_log)}")
    print(f"  tasks retried:             {retried_tasks}")
    print(f"  atsd retries / lost:       {logged['retry_scheduled']} / {logged['attempt_lost']}")
    print(f"  duplicate executions:      {duplicate_executions}  (extra completions, harmless)")
    print(f"  duplicate commits:         {duplicate_commits}  (must be 0)")
    ok = len(final) == len(rids) and all(d["status"] == "SUCCEEDED" for d in final.values()) and duplicate_commits == 0
    print("  RESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
