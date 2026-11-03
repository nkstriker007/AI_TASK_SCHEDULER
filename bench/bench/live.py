"""Helpers shared by the real-LLM experiments (E7, E8): the Groq planner with token accounting,
the running stack's API, atsd's event log, cost.

These need GROQ_API_KEY and a running LLM stack (`make up-llm`, API on localhost:8000).
"""

from __future__ import annotations

import asyncio
import json
import subprocess
import time
import urllib.request
from dataclasses import dataclass, field
from typing import Any

from planner.llm import GroqPlanner
from planner.schema import ExecutionPlan

# Groq list price for openai/gpt-oss-120b, USD per million tokens (input, output), Nov 2026.
PRICE_IN, PRICE_OUT = 0.15, 0.60
API = "http://localhost:8000"
FINISHED = ("SUCCEEDED", "FAILED", "REJECTED")


def cost(tokens_in: int, tokens_out: int) -> float:
    return (tokens_in * PRICE_IN + tokens_out * PRICE_OUT) / 1e6


def cooldown(seconds: float) -> None:
    """Let Groq's tokens-per-minute window empty, so one mode's 429s do not slow the next."""
    if seconds > 0:
        print(f"  (cooldown {seconds:.0f}s)", flush=True)
        time.sleep(seconds)


@dataclass
class Planned:
    plan: ExecutionPlan
    seconds: float
    tokens_in: int
    tokens_out: int
    calls: int
    attempts: int


class CountingPlanner(GroqPlanner):
    """GroqPlanner that sums tokens over every call (the repair call included) and waits out 429s."""

    def __init__(self, system_prompt: str | None = None, **kw):
        super().__init__(max_retries=0, **kw)
        if system_prompt is not None:
            self.system_prompt = system_prompt
        self.usage = [0, 0, 0]

    def propose(self, request, feedback=None):
        for _ in range(8):
            try:
                raw = super().propose(request, feedback)
                break
            except Exception as exc:
                if type(exc).__name__ != "RateLimitError":
                    raise
                time.sleep(_retry_after(exc, 10.0))
        else:
            raise RuntimeError("planner: rate limited 8 times")
        u = self.last_usage
        self.usage[0] += getattr(u, "prompt_tokens", 0) or 0
        self.usage[1] += getattr(u, "completion_tokens", 0) or 0
        self.usage[2] += 1
        return raw


def plan(request: str, system_prompt: str | None = None) -> Planned:
    p = CountingPlanner(system_prompt)
    t0 = time.monotonic()
    result = p.generate_plan(request)
    return Planned(result, time.monotonic() - t0, p.usage[0], p.usage[1], p.usage[2], p.last_attempts)


def _retry_after(exc: Exception, default: float) -> float:
    headers = getattr(getattr(exc, "response", None), "headers", None) or {}
    try:
        return max(1.0, float(headers.get("retry-after", default)))
    except (TypeError, ValueError):
        return default


def http(method: str, path: str, body: Any = None, api: str = API) -> Any:
    req = urllib.request.Request(api + path, method=method, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"content-type": "application/json"})
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read())


@dataclass
class Run:
    request_id: str
    status: str
    seconds: float  # POST /plans -> finished
    tokens_in: int
    tokens_out: int
    llm_calls: int
    retries: int  # attempts beyond the first, over all tasks
    tasks: list[dict[str, Any]] = field(default_factory=list)
    output: Any = None


def run_on_stack(p: ExecutionPlan, api: str = API, timeout_s: float = 1800) -> Run:
    """Submit a plan to the running stack and wait for it (the planner's time is measured separately)."""
    t0 = time.monotonic()
    rid = http("POST", "/plans", p.model_dump(mode="json", exclude_none=True), api)["request_id"]
    while time.monotonic() - t0 < timeout_s:
        st = http("GET", f"/requests/{rid}", api=api)
        if st["status"] in FINISHED:
            break
        time.sleep(0.5)
    seconds = time.monotonic() - t0
    usage = st.get("usage") or {}
    return Run(rid, st["status"], seconds, usage.get("tokens_in", 0), usage.get("tokens_out", 0),
               usage.get("llm_calls", 0), sum(max(0, t["attempt"] - 1) for t in st["tasks"]), st["tasks"], st["output"])


def atsd_events(request_id: str) -> list[dict[str, Any]]:
    """atsd's event log lines for one request, read from the atsd container."""
    out = subprocess.run(["docker", "compose", "exec", "-T", "atsd", "sh", "-c", "cat /runs/*/events.jsonl"],
                         capture_output=True, text=True, check=False)
    events = []
    for line in out.stdout.splitlines():
        try:
            e = json.loads(line)
        except json.JSONDecodeError:
            continue
        if e.get("request") == request_id:
            events.append(e)
    return events


def output_text(output: Any) -> str:
    if isinstance(output, dict) and isinstance(output.get("content"), str):
        return output["content"] + "\n" + "\n".join(output.get("key_points") or [])
    return json.dumps(output) if not isinstance(output, str) else output


def arun(coro):
    return asyncio.run(coro)
