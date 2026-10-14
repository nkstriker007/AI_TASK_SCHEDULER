"""Worker: consumes TaskMessages from a Redis stream and runs them (contract #2).

    python -m planner.worker --stream tasks:sim
    python -m planner.worker --stream tasks:llm --worker-id w-llm-1

One worker = one consumer in the `workers` group of one stream. Per message:

    1. parse the TaskMessage            invalid -> task_failed (permanent) if the attempt_id is usable, then XACK
    2. publish task_started
    3. deadline already passed?         -> task_failed (retryable), XACK
    4. result key already set?          -> task_completed committed=false (an earlier attempt won), XACK
    5. read the dependency results      missing -> task_failed (retryable)
    6. run the executor                 ExecutionError -> task_failed with its class
    7. SET result:<req>:<task> NX       committed = whether this attempt's write won
    8. publish task_completed, then XACK

XACK always comes last. If the worker dies anywhere before it, the entry stays in
the group's pending list and another worker takes it over with XAUTOCLAIM once it
has been idle for --claim-idle-ms. Steps 4 and 7 make that safe: an attempt can
run twice, but a result is recorded exactly once (8.5).
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import socket
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from pydantic import ValidationError

from .executors import ExecutionError, Executor, LLMExecutor, SimExecutor
from .messages import (
    STREAM_FIELD,
    TaskCompleted,
    TaskFailed,
    TaskMessage,
    TaskStarted,
    parse_attempt_id,
    result_key,
)
from .schema import TaskType

GROUP = "workers"
EVENTS_STREAM = "task:events"
TASK_STREAMS = ("tasks:llm", "tasks:sim")

log = logging.getLogger("ats.worker")


def _kind_for(msg: TaskMessage) -> str:
    """An expand task's result is always a Subplan (8.11); everything else is an output."""
    return "subplan" if msg.type == TaskType.expand else "output"


def now_ms() -> int:
    return int(time.time() * 1000)


@dataclass
class WorkerStats:
    processed: int = 0
    completed: int = 0
    duplicates: int = 0      # task_completed with committed=false
    failed: int = 0
    claimed: int = 0         # entries taken over from dead consumers
    invalid: int = 0


@dataclass
class Worker:
    redis: Any                               # redis.Redis(decode_responses=True)
    stream: str
    executor: Executor
    worker_id: str = field(default_factory=lambda: f"{socket.gethostname()}-{os.getpid()}")
    group: str = GROUP
    events_stream: str = EVENTS_STREAM
    block_ms: int = 2000                     # XREADGROUP wait when idle
    claim_idle_ms: int = 30_000              # take over entries pending longer than this
    claim_every_s: float = 5.0
    clock_ms: Callable[[], int] = now_ms
    stats: WorkerStats = field(default_factory=WorkerStats)

    def __post_init__(self) -> None:
        self._stop = False
        self._last_claim = 0.0

    # ---------------------------------------------------------------- setup
    def ensure_group(self) -> None:
        """Create the consumer group (and the stream) if needed. Safe to call from every worker."""
        try:
            self.redis.xgroup_create(self.stream, self.group, id="0", mkstream=True)
        except Exception as exc:  # BUSYGROUP: another worker created it first
            if "BUSYGROUP" not in str(exc):
                raise

    def stop(self, *_: Any) -> None:
        self._stop = True

    # ----------------------------------------------------------------- loop
    def run(self, max_messages: int | None = None) -> WorkerStats:
        self.ensure_group()
        log.info("worker %s consuming %s (group %s)", self.worker_id, self.stream, self.group)
        while not self._stop and (max_messages is None or self.stats.processed < max_messages):
            self.poll_once()
        return self.stats

    def poll_once(self) -> int:
        """Claim abandoned entries if due, else read new ones. Returns entries handled."""
        handled = 0
        if time.monotonic() - self._last_claim >= self.claim_every_s:
            self._last_claim = time.monotonic()
            for entry_id, fields in self.claim_abandoned():
                self.stats.claimed += 1
                self.handle(entry_id, fields)
                handled += 1
        if handled:
            return handled
        resp = self.redis.xreadgroup(self.group, self.worker_id, {self.stream: ">"},
                                     count=1, block=self.block_ms)
        for _stream, entries in resp or []:
            for entry_id, fields in entries:
                self.handle(entry_id, fields)
                handled += 1
        return handled

    def claim_abandoned(self) -> list[tuple[str, dict]]:
        resp = self.redis.xautoclaim(self.stream, self.group, self.worker_id,
                                     min_idle_time=self.claim_idle_ms, start_id="0-0", count=10)
        entries = resp[1] if len(resp) > 1 else []
        # Entries deleted from the stream come back as (id, None); just acknowledge them.
        out = []
        for entry_id, fields in entries:
            if fields is None:
                self.redis.xack(self.stream, self.group, entry_id)
            else:
                out.append((entry_id, fields))
        return out

    # ------------------------------------------------------------ one entry
    def handle(self, entry_id: str, fields: dict) -> None:
        self.stats.processed += 1
        try:
            self._handle(fields)
        finally:
            self.redis.xack(self.stream, self.group, entry_id)  # always last

    def _handle(self, fields: dict) -> None:
        raw = (fields or {}).get(STREAM_FIELD)
        try:
            msg = TaskMessage.model_validate_json(raw or "")
        except ValidationError as exc:
            self.stats.invalid += 1
            self._report_invalid(raw, exc)
            return

        self._publish(TaskStarted(event="task_started", attempt_id=msg.attempt_id,
                                  worker_id=self.worker_id, ts_ms=self.clock_ms()))
        key = result_key(msg.request_id, msg.task_id)

        if self.clock_ms() >= msg.deadline_ms:
            self._fail(msg, "deadline passed before the attempt started", retryable=True)
            return
        if self.redis.exists(key):
            self.stats.duplicates += 1
            self._complete(msg, key, committed=False, kind=_kind_for(msg), exec_ms=0)
            return

        try:
            inputs = self._read_inputs(msg)
            t0 = time.monotonic()
            result = self.executor.execute(msg, inputs)
            exec_ms = int((time.monotonic() - t0) * 1000)
        except ExecutionError as err:
            self._fail(msg, err.message, retryable=err.retryable, retry_after_ms=err.retry_after_ms)
            return
        except Exception as exc:  # a bug in an executor must not kill the worker
            log.exception("executor crashed on %s", msg.attempt_id)
            self._fail(msg, f"executor error: {type(exc).__name__}: {exc}", retryable=True)
            return

        committed = bool(self.redis.set(key, result.output, nx=True))
        if not committed:
            self.stats.duplicates += 1
        self._complete(msg, key, committed=committed, kind=result.kind, exec_ms=exec_ms,
                       tokens_in=result.tokens_in, tokens_out=result.tokens_out)

    # -------------------------------------------------------------- helpers
    def _read_inputs(self, msg: TaskMessage) -> dict[str, str]:
        if not msg.inputs:
            return {}
        deps = list(msg.inputs)
        values = self.redis.mget([msg.inputs[d] for d in deps])
        missing = [d for d, v in zip(deps, values) if v is None]
        if missing:
            raise ExecutionError(f"input results not found for {missing}", retryable=True)
        return dict(zip(deps, values))

    def _complete(self, msg: TaskMessage, key: str, committed: bool, kind: str, exec_ms: int,
                  tokens_in: int | None = None, tokens_out: int | None = None) -> None:
        self.stats.completed += 1
        self._publish(TaskCompleted(event="task_completed", attempt_id=msg.attempt_id,
                                    worker_id=self.worker_id, ts_ms=self.clock_ms(),
                                    result_key=key, committed=committed, result_kind=kind,
                                    exec_ms=exec_ms, tokens_in=tokens_in, tokens_out=tokens_out))
        log.info("%s completed (committed=%s, %d ms)", msg.attempt_id, committed, exec_ms)

    def _fail(self, msg: TaskMessage, message: str, retryable: bool, retry_after_ms: int | None = None) -> None:
        self.stats.failed += 1
        self._publish(TaskFailed(event="task_failed", attempt_id=msg.attempt_id, worker_id=self.worker_id,
                                 ts_ms=self.clock_ms(), error_class="retryable" if retryable else "permanent",
                                 message=(message or "failed")[:2000], retry_after_ms=retry_after_ms))
        log.warning("%s failed (%s): %s", msg.attempt_id, "retryable" if retryable else "permanent", message)

    def _report_invalid(self, raw: Any, exc: ValidationError) -> None:
        """A malformed message can't run. Tell atsd if we can recover its attempt_id."""
        import json

        log.error("invalid task message: %s", exc.errors()[:3])
        try:
            attempt = json.loads(raw)["attempt_id"]
            parse_attempt_id(attempt)
            event = TaskFailed(event="task_failed", attempt_id=attempt, worker_id=self.worker_id,
                               ts_ms=self.clock_ms(), error_class="permanent",
                               message=f"invalid task message: {exc.errors()[0]['msg']}"[:2000])
        except Exception:
            return  # nothing addressable to report; the entry is acknowledged and dropped
        self._publish(event)

    def _publish(self, event: Any) -> None:
        self.redis.xadd(self.events_stream, {STREAM_FIELD: event.model_dump_json()})


# --------------------------------------------------------------------- CLI


def make_executor(stream: str, time_scale: float) -> Executor:
    if stream.endswith(":sim"):
        return SimExecutor(time_scale=time_scale)
    return LLMExecutor()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Run one ATS worker.")
    p.add_argument("--stream", required=True, choices=TASK_STREAMS)
    p.add_argument("--worker-id", default=None)
    p.add_argument("--redis-url", default=os.environ.get("ATS_REDIS_URL", "redis://localhost:6379/0"))
    p.add_argument("--time-scale", type=float, default=1.0, help="sim only: multiply estimated_seconds")
    p.add_argument("--claim-idle-ms", type=int, default=30_000)
    p.add_argument("--max-messages", type=int, default=None, help="exit after N messages (tests, demos)")
    p.add_argument("-v", "--verbose", action="store_true")
    a = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO if a.verbose else logging.WARNING,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    import redis

    if a.stream == "tasks:llm" and not os.environ.get("GROQ_API_KEY"):
        print("GROQ_API_KEY is not set; run with: uv run --env-file ../.env python -m planner.worker "
              "--stream tasks:llm", file=sys.stderr)
        return 2
    client = redis.Redis.from_url(a.redis_url, decode_responses=True)
    worker = Worker(client, a.stream, make_executor(a.stream, a.time_scale), claim_idle_ms=a.claim_idle_ms)
    if a.worker_id:
        worker.worker_id = a.worker_id
    signal.signal(signal.SIGTERM, worker.stop)
    signal.signal(signal.SIGINT, worker.stop)
    try:
        stats = worker.run(max_messages=a.max_messages)
    except redis.ConnectionError as exc:
        print(f"cannot reach Redis at {a.redis_url}: {exc}", file=sys.stderr)
        return 2
    print(f"{worker.worker_id}: {stats}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
