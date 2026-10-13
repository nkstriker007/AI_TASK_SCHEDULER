"""Worker process: pull attempts from capability streams, run them, commit, report.

Lifecycle of one attempt on the worker side (design doc 8.5):

    XREADGROUP tasks:<cap>  ->  task_started  ->  load inputs  ->  run executor
      ->  SET result:{req}:{task} NX  ->  task_completed (committed true/false)  ->  XACK

Failures become task_failed with error_class retryable or permanent; atsd decides
whether to retry. The worker never retries on its own (D12) and never claims other
consumers' pending entries (D10): a lost attempt is detected by atsd's deadline.
A message is acknowledged only after its event is on task:events, so a crash before
that leaves it pending, which is harmless because atsd owns the retry.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import socket
import time
from collections.abc import Callable
from dataclasses import dataclass, field

from pydantic import ValidationError
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from planner import keys
from planner.messages import (
    ATTEMPT_ID_PATTERN,
    STREAM_FIELD,
    TaskCompleted,
    TaskFailed,
    TaskMessage,
    TaskStarted,
    result_key,
)

from .executors import ExecutionContext, PermanentError, RetryableError
from .registry import ExecutorRegistry, default_registry
from .results import commit_result, load_inputs

log = logging.getLogger("worker")

MAX_MESSAGE_CHARS = 2000  # TaskFailed.message limit (contract #2)


def _default_worker_id() -> str:
    return f"w-{socket.gethostname()}-{os.getpid()}"


@dataclass
class WorkerConfig:
    redis_url: str = "redis://localhost:6379/0"
    worker_id: str = field(default_factory=_default_worker_id)
    capabilities: tuple[str, ...] = ("sim",)
    concurrency: int = 1  # attempts this worker runs at once
    block_ms: int = 1000  # XREADGROUP block; also bounds how fast a stop request is noticed
    heartbeat_s: float = 3.0
    heartbeat_ttl_s: int = 10
    sim_time_scale: float = 1.0
    sim_default_seconds: float = 10.0

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> WorkerConfig:
        env = dict(os.environ if env is None else env)
        cfg = cls()
        cfg.redis_url = env.get("REDIS_URL", cfg.redis_url)
        cfg.worker_id = env.get("WORKER_ID") or cfg.worker_id
        caps = env.get("WORKER_CAPABILITIES")
        if caps:
            cfg.capabilities = tuple(c.strip() for c in caps.split(",") if c.strip())
        cfg.concurrency = int(env.get("WORKER_CONCURRENCY", cfg.concurrency))
        cfg.sim_time_scale = float(env.get("SIM_TIME_SCALE", cfg.sim_time_scale))
        cfg.sim_default_seconds = float(env.get("SIM_DEFAULT_SECONDS", cfg.sim_default_seconds))
        if cfg.concurrency < 1:
            raise ValueError("WORKER_CONCURRENCY must be >= 1")
        if not cfg.capabilities:
            raise ValueError("WORKER_CAPABILITIES must name at least one capability")
        return cfg


def _now_ms() -> int:
    return int(time.time() * 1000)


class Worker:
    def __init__(
        self,
        redis: Redis,
        config: WorkerConfig,
        registry: ExecutorRegistry | None = None,
        clock_ms: Callable[[], int] = _now_ms,
    ):
        self.redis = redis
        self.config = config
        self.registry = registry or default_registry(config.sim_time_scale, config.sim_default_seconds)
        self.clock_ms = clock_ms
        self.streams = {keys.task_stream(c): c for c in config.capabilities}
        self._inflight: set[asyncio.Task] = set()
        self._started_ms = clock_ms()
        self.processed = 0

    # ------------------------------------------------------------------ setup

    async def ensure_groups(self) -> None:
        """Create the `workers` group on each stream if needed. Id 0 so entries added
        before the group existed are still delivered. atsd may create it too."""
        for stream in self.streams:
            try:
                await self.redis.xgroup_create(stream, keys.WORKERS_GROUP, id="0", mkstream=True)
            except ResponseError as exc:
                if "BUSYGROUP" not in str(exc):
                    raise

    # --------------------------------------------------------------- main loop

    async def run(self, stop: asyncio.Event) -> None:
        await self.ensure_groups()
        heartbeat = asyncio.create_task(self._heartbeat_loop(stop))
        log.info("worker %s up: streams=%s concurrency=%d", self.config.worker_id,
                 ",".join(self.streams), self.config.concurrency)
        try:
            while not stop.is_set():
                free = self.config.concurrency - len(self._inflight)
                if free <= 0:
                    await asyncio.wait(self._inflight, return_when=asyncio.FIRST_COMPLETED)
                    continue
                batches = await self.redis.xreadgroup(
                    keys.WORKERS_GROUP,
                    self.config.worker_id,
                    {s: ">" for s in self.streams},
                    count=free,
                    block=self.config.block_ms,
                )
                for stream, entries in batches or []:
                    for entry_id, fields in entries:
                        task = asyncio.create_task(self.handle(stream, entry_id, fields))
                        self._inflight.add(task)
                        task.add_done_callback(self._inflight.discard)
        finally:
            # Graceful stop: finish what we already took, then leave the registry.
            if self._inflight:
                await asyncio.gather(*self._inflight, return_exceptions=True)
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
            await self.redis.delete(keys.worker_key(self.config.worker_id))
            log.info("worker %s stopped after %d attempts", self.config.worker_id, self.processed)

    # ------------------------------------------------------------- one attempt

    async def handle(self, stream: str, entry_id: str, fields: dict[str, str]) -> None:
        raw = fields.get(STREAM_FIELD, "")
        try:
            message = TaskMessage.model_validate_json(raw)
        except ValidationError as exc:
            await self._reject_malformed(raw, exc)
            await self.redis.xack(stream, keys.WORKERS_GROUP, entry_id)
            return

        await self._emit(TaskStarted(event="task_started", **self._base(message)))
        started = time.monotonic()
        try:
            executor = self.registry.get(self.streams[stream], message.type)
            inputs = await load_inputs(self.redis, message)
            result = await executor.run(ExecutionContext(message=message, inputs=inputs))
            key = result_key(message.request_id, message.task_id)
            committed = await commit_result(self.redis, key, result.output)
            event = TaskCompleted(
                event="task_completed",
                **self._base(message),
                result_key=key,
                committed=committed,
                result_kind=result.result_kind,
                exec_ms=int((time.monotonic() - started) * 1000),
                tokens_in=result.tokens_in,
                tokens_out=result.tokens_out,
            )
        except RetryableError as exc:
            event = self._failed(message, "retryable", str(exc), exc.retry_after_ms)
        except PermanentError as exc:
            event = self._failed(message, "permanent", str(exc))
        except asyncio.CancelledError:
            raise  # not acked: stays pending, atsd's deadline handles it
        except Exception as exc:  # unclassified bug or library error: let atsd retry, bounded by max attempts
            log.exception("attempt %s raised", message.attempt_id)
            event = self._failed(message, "retryable", f"{type(exc).__name__}: {exc}")

        await self._emit(event)
        await self.redis.xack(stream, keys.WORKERS_GROUP, entry_id)
        self.processed += 1

    def _base(self, message: TaskMessage) -> dict:
        return {"attempt_id": message.attempt_id, "worker_id": self.config.worker_id, "ts_ms": self.clock_ms()}

    def _failed(self, message: TaskMessage, error_class: str, text: str,
                retry_after_ms: int | None = None) -> TaskFailed:
        return TaskFailed(
            event="task_failed",
            **self._base(message),
            error_class=error_class,
            message=(text or error_class)[:MAX_MESSAGE_CHARS],
            retry_after_ms=retry_after_ms,
        )

    async def _reject_malformed(self, raw: str, exc: ValidationError) -> None:
        """A message that violates contract #2 cannot run. If its attempt_id is readable,
        report a permanent failure so atsd does not wait for the deadline."""
        log.error("malformed task message: %s", exc.errors()[:3])
        try:
            attempt = json.loads(raw).get("attempt_id")
        except (json.JSONDecodeError, AttributeError):
            attempt = None
        if isinstance(attempt, str) and re.fullmatch(ATTEMPT_ID_PATTERN, attempt):
            await self._emit(TaskFailed(
                event="task_failed",
                attempt_id=attempt,
                worker_id=self.config.worker_id,
                ts_ms=self.clock_ms(),
                error_class="permanent",
                message=f"malformed task message: {exc.error_count()} validation error(s)",
            ))

    async def _emit(self, event) -> None:
        await self.redis.xadd(keys.EVENTS_STREAM, {STREAM_FIELD: event.model_dump_json()})

    # --------------------------------------------------------------- heartbeat

    async def beat(self) -> None:
        key = keys.worker_key(self.config.worker_id)
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.hset(key, mapping={
                "capabilities": ",".join(self.config.capabilities),
                "concurrency": self.config.concurrency,
                "in_flight": len(self._inflight),
                "processed": self.processed,
                "started_ms": self._started_ms,
                "last_seen_ms": self.clock_ms(),
            })
            pipe.expire(key, self.config.heartbeat_ttl_s)
            await pipe.execute()

    async def _heartbeat_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self.beat()
            except Exception:  # liveness only (8.7): never let it kill the worker
                log.warning("heartbeat failed", exc_info=True)
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.config.heartbeat_s)
            except asyncio.TimeoutError:
                pass
