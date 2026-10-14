"""Worker against a real Redis (see conftest.redis_client): contract #2 end to end."""

import json
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from planner.devrun import run_plan
from planner.executors import ExecResult, ExecutionError, Executor, SimExecutor
from planner.messages import STREAM_FIELD, TaskEvent, TaskMessage, result_key
from planner.paths import FIXTURES_DIR
from planner.worker import EVENTS_STREAM, GROUP, Worker

STREAM = "tasks:sim"
REQ = "req_w"
PLANNER_DIR = Path(__file__).resolve().parents[1]


def message(task_id="t1", attempt=1, inputs=(), type="research", deadline_s=60, **extra):
    m = dict(request_id=REQ, task_id=task_id, attempt=attempt, attempt_id=f"{REQ}:{task_id}:{attempt}",
             type=type, instruction=f"Do {task_id}.", inputs={d: result_key(REQ, d) for d in inputs},
             deadline_ms=int((time.time() + deadline_s) * 1000), estimated_seconds=0)
    m.update(extra)
    return TaskMessage(**m)


def send(r, msg, stream=STREAM):
    raw = msg if isinstance(msg, str) else msg.model_dump_json()
    return r.xadd(stream, {STREAM_FIELD: raw})


def events(r):
    """Every event on task:events, parsed with the contract's own model (fails on a bad event)."""
    return [TaskEvent.model_validate_json(f[STREAM_FIELD]).root for _, f in r.xrange(EVENTS_STREAM)]


def pending(r, stream=STREAM):
    return r.xpending(stream, GROUP)["pending"]


class Recording(Executor):
    name = "recording"

    def __init__(self, output="out", error=None, kind="output", side_effect=None):
        self.output, self.error, self.kind, self.side_effect = output, error, kind, side_effect
        self.calls = []

    def execute(self, msg, inputs):
        self.calls.append((msg.attempt_id, dict(inputs)))
        if self.side_effect:
            self.side_effect(msg)
        if self.error:
            raise self.error
        return ExecResult(self.output, self.kind, 10, 5)


def worker(r, executor, **kw):
    w = Worker(r, STREAM, executor, worker_id=kw.pop("worker_id", "w-test"), block_ms=50, **kw)
    w.ensure_group()
    return w


# --- happy path ----------------------------------------------------------------


def test_completes_a_task_and_commits_the_result_once(redis_client):
    r, ex = redis_client, Recording("Apple facts")
    w = worker(r, ex)
    send(r, message("t1"))
    assert w.poll_once() == 1

    assert r.get(result_key(REQ, "t1")) == "Apple facts"
    started, done = events(r)
    assert started.event == "task_started" and started.attempt_id == f"{REQ}:t1:1"
    assert done.event == "task_completed" and done.committed is True and done.result_kind == "output"
    assert done.result_key == result_key(REQ, "t1") and (done.tokens_in, done.tokens_out) == (10, 5)
    assert pending(r) == 0                                  # acknowledged


def test_dependency_results_are_passed_to_the_executor(redis_client):
    r, ex = redis_client, Recording()
    r.set(result_key(REQ, "t3"), "A")
    r.set(result_key(REQ, "t4"), "B")
    w = worker(r, ex)
    send(r, message("t5", inputs=["t3", "t4"], type="compare"))
    w.poll_once()
    assert ex.calls == [(f"{REQ}:t5:1", {"t3": "A", "t4": "B"})]


def test_expand_result_is_reported_as_subplan(redis_client):
    r = redis_client
    w = worker(r, SimExecutor(time_scale=0))
    send(r, message("t2", type="expand"))
    w.poll_once()
    done = events(r)[-1]
    assert done.result_kind == "subplan"
    assert json.loads(r.get(result_key(REQ, "t2")))["tasks"][0]["id"] == "s1"


# --- exactly-once results --------------------------------------------------------


def test_existing_result_means_committed_false_and_no_rerun(redis_client):
    r, ex = redis_client, Recording("second")
    r.set(result_key(REQ, "t1"), "first")
    w = worker(r, ex)
    send(r, message("t1", attempt=2))
    w.poll_once()
    assert ex.calls == []
    assert r.get(result_key(REQ, "t1")) == "first"
    done = events(r)[-1]
    assert done.event == "task_completed" and done.committed is False and done.exec_ms == 0
    assert w.stats.duplicates == 1


def test_losing_a_race_keeps_the_winners_result(redis_client):
    r = redis_client
    # While this attempt runs, another attempt commits first.
    ex = Recording("loser", side_effect=lambda m: r.set(result_key(REQ, "t1"), "winner"))
    w = worker(r, ex)
    send(r, message("t1", attempt=2))
    w.poll_once()
    assert r.get(result_key(REQ, "t1")) == "winner"
    assert events(r)[-1].committed is False


def test_expand_duplicate_still_reports_subplan(redis_client):
    r = redis_client
    r.set(result_key(REQ, "t2"), json.dumps({"tasks": []}))
    w = worker(r, Recording())
    send(r, message("t2", attempt=2, type="expand"))
    w.poll_once()
    done = events(r)[-1]
    assert done.committed is False and done.result_kind == "subplan"


# --- failures ------------------------------------------------------------------


@pytest.mark.parametrize("retryable, retry_after", [(True, 1500), (False, None)])
def test_executor_errors_become_task_failed_with_their_class(redis_client, retryable, retry_after):
    r = redis_client
    w = worker(r, Recording(error=ExecutionError("provider said no", retryable, retry_after)))
    send(r, message("t1"))
    w.poll_once()
    failed = events(r)[-1]
    assert failed.event == "task_failed"
    assert failed.error_class == ("retryable" if retryable else "permanent")
    assert failed.retry_after_ms == retry_after and "provider said no" in failed.message
    assert not r.exists(result_key(REQ, "t1")) and pending(r) == 0


def test_executor_bug_is_reported_and_the_worker_keeps_going(redis_client):
    r = redis_client
    w = worker(r, Recording(error=ZeroDivisionError("oops")))
    send(r, message("t1"))
    send(r, message("t2"))
    w.poll_once()
    w.poll_once()
    failed = [e for e in events(r) if e.event == "task_failed"]
    assert len(failed) == 2 and all(e.error_class == "retryable" for e in failed)
    assert "ZeroDivisionError" in failed[0].message


def test_missing_input_is_retryable(redis_client):
    r, ex = redis_client, Recording()
    w = worker(r, ex)
    send(r, message("t5", inputs=["t3"]))
    w.poll_once()
    failed = events(r)[-1]
    assert failed.event == "task_failed" and failed.error_class == "retryable" and "t3" in failed.message
    assert ex.calls == []


def test_expired_deadline_skips_the_work(redis_client):
    r, ex = redis_client, Recording()
    w = worker(r, ex)
    send(r, message("t1", deadline_s=-5))
    w.poll_once()
    assert ex.calls == []
    assert events(r)[-1].error_class == "retryable"


def test_invalid_message_with_usable_attempt_id_is_failed_permanently(redis_client):
    r = redis_client
    w = worker(r, Recording())
    bad = message("t1").model_dump()
    bad["type"] = "dance"
    send(r, json.dumps(bad))
    w.poll_once()
    (failed,) = events(r)
    assert failed.event == "task_failed" and failed.error_class == "permanent"
    assert failed.attempt_id == f"{REQ}:t1:1" and pending(r) == 0


def test_garbage_message_is_dropped_without_an_event(redis_client):
    r = redis_client
    w = worker(r, Recording())
    send(r, "{not json")
    r.xadd(STREAM, {"wrong_field": "x"})
    w.poll_once()
    w.poll_once()
    assert events(r) == [] and pending(r) == 0 and w.stats.invalid == 2


# --- crash recovery --------------------------------------------------------------


def test_entry_of_a_dead_consumer_is_claimed_and_finished(redis_client):
    r = redis_client
    send(r, message("t1"))
    r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
    r.xreadgroup(GROUP, "dead-worker", {STREAM: ">"}, count=1)    # read, then "crash": never acked
    assert pending(r) == 1

    w = worker(r, Recording("rescued"), worker_id="w-alive", claim_idle_ms=0, claim_every_s=0)
    assert w.poll_once() == 1
    assert w.stats.claimed == 1
    assert r.get(result_key(REQ, "t1")) == "rescued" and pending(r) == 0


def test_killed_worker_process_is_recovered_with_one_result(redis_url, redis_client):
    """kill -9 a real worker mid-task; a second worker claims the entry; one result, committed once."""
    r = redis_client
    send(r, message("t1", estimated_seconds=30))      # sim sleeps 30 s * scale 1: long enough to kill
    env = {**os.environ, "ATS_REDIS_URL": redis_url}
    cmd = [sys.executable, "-m", "planner.worker", "--stream", STREAM]
    victim = subprocess.Popen(cmd + ["--worker-id", "victim"], cwd=PLANNER_DIR, env=env,
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(100):                            # wait until it has started the task
            if any(e.event == "task_started" for e in events(r)):
                break
            time.sleep(0.05)
        else:
            pytest.fail("worker never started the task")
    finally:
        victim.send_signal(signal.SIGKILL)
        victim.wait(timeout=5)
    assert pending(r) == 1 and not r.exists(result_key(REQ, "t1"))

    rescuer = subprocess.run(cmd + ["--worker-id", "rescuer", "--claim-idle-ms", "100", "--time-scale", "0",
                                    "--max-messages", "1"], cwd=PLANNER_DIR, env=env, timeout=30,
                             capture_output=True, text=True)
    assert rescuer.returncode == 0, rescuer.stderr
    assert r.exists(result_key(REQ, "t1")) and pending(r) == 0
    done = [e for e in events(r) if e.event == "task_completed"]
    assert len(done) == 1 and done[0].worker_id == "rescuer" and done[0].committed is True


# --- end to end with the development runner --------------------------------------


def run_with_workers(r, executor_factory, n_workers=3, **run_kw):
    stop = threading.Event()
    workers = [worker(r, executor_factory(), worker_id=f"w{i}") for i in range(n_workers)]

    def loop(w):
        while not stop.is_set():
            w.poll_once()
    threads = [threading.Thread(target=loop, args=(w,), daemon=True) for w in workers]
    for t in threads:
        t.start()
    try:
        plan = json.loads((FIXTURES_DIR / "company_research.json").read_text())
        return run_plan(r, plan, STREAM, verbose=False, timeout_s=30, **run_kw)
    finally:
        stop.set()
        for t in threads:
            t.join(timeout=5)


def test_fixture_plan_runs_end_to_end(redis_client):
    report = run_with_workers(redis_client, lambda: SimExecutor(time_scale=0.005))
    assert report.ok
    assert all(redis_client.exists(result_key(report.request_id, t)) for t in report.tasks)
    for e in events(redis_client):           # every event on the wire matches contract #2
        assert e.worker_id in {"w0", "w1", "w2"}


class FlakyOnce(Executor):
    """Fails the first attempt of every task (retryable), succeeds after."""
    name = "flaky"

    def execute(self, msg, inputs):
        if msg.attempt == 1:
            raise ExecutionError("transient", retryable=True, retry_after_ms=10)
        return ExecResult(f"ok {msg.task_id}")


def test_retryable_failures_are_retried_to_success(redis_client):
    report = run_with_workers(redis_client, FlakyOnce)
    assert report.ok
    assert all(t.attempt == 2 and t.committed_by == 2 for t in report.tasks.values())


def test_permanent_failure_stops_the_run(redis_client):
    report = run_with_workers(redis_client, lambda: Recording(error=ExecutionError("bad request", retryable=False)))
    assert not report.ok
    assert sum(t.state == "failed" for t in report.tasks.values()) >= 1
