"""Redis key and stream names shared by the API, the workers and atsd (design doc 8.3).

The C++ side uses the same literal names; docs/messages.md lists them.
"""

from __future__ import annotations

# API -> atsd: validated plan files to execute. One field `data` holding the PlanFile JSON.
REQUESTS_STREAM = "requests:new"
SCHEDULER_GROUP = "scheduler"  # atsd's consumer group on requests:new and task:events

# atsd -> workers: one stream per capability (tasks:llm, tasks:sim, ...).
WORKERS_GROUP = "workers"

# workers -> atsd: task_started / task_completed / task_failed.
EVENTS_STREAM = "task:events"

# research, summarize, analyze, compare, report, expand -> llm. Synthetic work -> sim.
# Runtime config, not part of the plan (8.3).
CAPABILITIES = ("llm", "sim")


def task_stream(capability: str) -> str:
    return f"tasks:{capability}"


def request_key(request_id: str) -> str:
    """Hash written by atsd: request status and per-task states (read by the API)."""
    return f"request:{request_id}"


def plan_key(request_id: str) -> str:
    """String written once by the API: the submitted PlanFile JSON, so status can be
    served before atsd has picked the request up."""
    return f"plan:{request_id}"


def worker_key(worker_id: str) -> str:
    """Hash written by each worker every few seconds, with a short TTL (liveness only)."""
    return f"worker:{worker_id}"
