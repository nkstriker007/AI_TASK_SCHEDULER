"""Sim executor: sleeps for the task's duration hint instead of calling an LLM (8.9).

It powers the distributed-vs-simulator comparison (E1) and the benchmarks, so its
timing must follow the same rule as the C++ simulator: estimated_seconds when the
plan has a hint, otherwise a default (10 s). `time_scale` shrinks every duration
by the same factor so a 65-second plan can run in 6.5 s during development.
Fault injection (crash, hang, error rate) is Day 5.
"""

from __future__ import annotations

import asyncio

from .base import ExecutionContext, ExecutionResult, Executor


class SimExecutor(Executor):
    capability = "sim"

    def __init__(self, time_scale: float = 1.0, default_seconds: float = 10.0):
        if time_scale < 0 or default_seconds < 0:
            raise ValueError("time_scale and default_seconds must be >= 0")
        self.time_scale = time_scale
        self.default_seconds = default_seconds

    def duration(self, ctx: ExecutionContext) -> float:
        hint = ctx.message.estimated_seconds
        return (self.default_seconds if hint is None else hint) * self.time_scale

    async def run(self, ctx: ExecutionContext) -> ExecutionResult:
        seconds = self.duration(ctx)
        await asyncio.sleep(seconds)
        msg = ctx.message
        return ExecutionResult(
            output={
                "content": f"[sim] {msg.type.value} {msg.task_id}: {msg.instruction[:200]}",
                "inputs": sorted(ctx.inputs),
                "simulated_seconds": seconds,
            }
        )
