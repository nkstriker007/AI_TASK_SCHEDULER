"""Sim executor: sleeps for the task's duration hint instead of calling an LLM (8.9).

It powers the distributed-vs-simulator comparison (E1) and the benchmarks, so its
timing must follow the same rule as the C++ simulator: estimated_seconds when the
plan has a hint, otherwise a default (10 s). `time_scale` shrinks every duration
by the same factor so a 65-second plan can run in 6.5 s during development.
Faults (error, simulated 429, hang, crash, straggler) are injected per attempt from a
FaultConfig (worker/faults.py); with none configured the executor behaves as before.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Callable

from planner.schema import TaskType

from ..faults import CRASH_EXIT_CODE, Fault, FaultConfig
from .base import ExecutionContext, ExecutionResult, Executor, PermanentError, RetryableError

log = logging.getLogger("worker.faults")


def _crash_process() -> None:
    log.error("injected fault: crashing the worker process (exit %d)", CRASH_EXIT_CODE)
    logging.shutdown()
    os._exit(CRASH_EXIT_CODE)  # no cleanup, no event, no XACK: like `docker kill`

DEFAULT_SIM_FANOUT = 2


class SimExecutor(Executor):
    capability = "sim"

    def __init__(self, time_scale: float = 1.0, default_seconds: float = 10.0, fanout: int = DEFAULT_SIM_FANOUT,
                 faults: FaultConfig | None = None, crash: Callable[[], None] = _crash_process):
        if time_scale < 0 or default_seconds < 0 or fanout < 1:
            raise ValueError("time_scale and default_seconds must be >= 0, fanout >= 1")
        self.time_scale = time_scale
        self.default_seconds = default_seconds
        self.fanout = fanout
        self.faults = faults or FaultConfig()
        self.crash = crash  # tests replace it; in a real worker it ends the process

    def duration(self, ctx: ExecutionContext) -> float:
        hint = ctx.message.estimated_seconds
        return (self.default_seconds if hint is None else hint) * self.time_scale

    async def run(self, ctx: ExecutionContext) -> ExecutionResult:
        msg = ctx.message
        seconds = self.duration(ctx)
        fault = self.faults.decide(msg.task_id, msg.attempt, msg.attempt_id)
        if fault != Fault.none:
            log.warning("injected fault %s on %s", fault.value, msg.attempt_id)
        if fault == Fault.rate_limit:
            raise RetryableError("injected fault: 429 rate limited", retry_after_ms=self.faults.retry_after_ms)
        if fault == Fault.permanent:
            raise PermanentError("injected fault: permanent failure")
        if fault == Fault.straggler:
            seconds *= self.faults.straggler_factor
        if fault in (Fault.error, Fault.crash):
            await asyncio.sleep(seconds / 2)  # fail part-way through, as real work would
            if fault == Fault.crash:
                self.crash()
                await asyncio.Event().wait()  # a test's crash() returns: behave like a dead process
            raise RetryableError("injected fault: transient error")
        if fault == Fault.hang:
            await asyncio.Event().wait()  # never finishes; the worker stops at the deadline
        await asyncio.sleep(seconds)
        if msg.type == TaskType.expand:
            # A canned subplan of `fanout` research tasks, so atsd's splice path can run
            # without an LLM (D28).
            return ExecutionResult(result_kind="subplan", output={"tasks": [
                {"id": f"s{i}", "type": "research", "title": f"Item {i} from {msg.task_id}",
                 "instruction": f"Research item {i} found by {msg.task_id}.", "depends_on": []}
                for i in range(1, self.fanout + 1)
            ]})
        return ExecutionResult(
            output={
                "content": f"[sim] {msg.type.value} {msg.task_id}: {msg.instruction[:200]}",
                "inputs": sorted(ctx.inputs),
                "simulated_seconds": seconds,
            }
        )
