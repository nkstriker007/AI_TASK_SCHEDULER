"""HTTP API (design doc 8.10).

    POST /requests {"text": ...}   plan with the LLM, validate, enqueue
    POST /plans    {ExecutionPlan} validate and enqueue a hand-written plan (benchmarks, tests)
    GET  /requests/{request_id}    status, per-task states, final output once the sink completes
    GET  /healthz                  Redis reachable

Enqueueing writes plan:{req} (so status works immediately) and then appends the
plan file to requests:new for atsd. The runtime, never the LLM, assigns request_id.

Configuration (environment):
    REDIS_URL            default redis://localhost:6379/0
    ATS_PLANNER          "groq" (default; needs GROQ_API_KEY) or "fake"
    ATS_FAKE_FIXTURE     fixture returned by the fake planner (default company_research)
"""

from __future__ import annotations

import os
import re
from collections.abc import Callable
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Body, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from groq import GroqError
from pydantic import BaseModel, Field
from redis.asyncio import Redis
from starlette.concurrency import run_in_threadpool

from planner import keys
from planner.cli import make_plan_file
from planner.llm import Planner, PlanRejected
from planner.messages import REQUEST_ID_PATTERN, STREAM_FIELD
from planner.schema import ExecutionPlan, ValidationIssue
from planner.validate import validate_plan

from .status import SUBMITTED, request_status

MAX_REQUEST_CHARS = 4000


class CreateRequest(BaseModel):
    text: str = Field(min_length=1, max_length=MAX_REQUEST_CHARS)


def planner_from_env() -> Callable[[], Planner]:
    kind = os.environ.get("ATS_PLANNER", "groq")
    if kind == "fake":
        from planner.fake import FakePlanner

        fixture = os.environ.get("ATS_FAKE_FIXTURE", "company_research")
        return lambda: FakePlanner(default=fixture)
    if kind == "groq":
        from planner.llm import GroqPlanner

        return GroqPlanner
    raise ValueError(f"ATS_PLANNER must be 'groq' or 'fake' (got {kind!r})")


def _issues(issues: list[ValidationIssue]) -> list[dict[str, Any]]:
    return [i.model_dump(mode="json") for i in issues]


def _rejected(issues: list[ValidationIssue]) -> JSONResponse:
    return JSONResponse(status_code=422, content={"error": "plan_rejected", "issues": _issues(issues)})


async def enqueue(redis: Redis, plan: ExecutionPlan) -> str:
    plan_file = make_plan_file(plan)
    body = plan_file.model_dump_json(exclude_none=True)
    # Claim the id first and enqueue only if the claim succeeded. (In one MULTI the XADD
    # would run even when SET NX fails, queueing a plan under another request's id.)
    if not await redis.set(keys.plan_key(plan_file.request_id), body, nx=True):
        # 32 random bits collided with an existing request: refuse rather than overwrite
        raise HTTPException(status_code=503, detail="request id collision, retry")
    await redis.xadd(keys.REQUESTS_STREAM, {STREAM_FIELD: body})
    return plan_file.request_id


def _accepted(request_id: str, plan: ExecutionPlan, warnings: list[ValidationIssue], **extra) -> JSONResponse:
    return JSONResponse(status_code=202, content={
        "request_id": request_id,
        "status": SUBMITTED,
        "plan": plan.model_dump(mode="json", exclude_none=True),
        "warnings": _issues(warnings),
        **extra,
    })


def create_app(redis_url: str | None = None, planner_factory: Callable[[], Planner] | None = None) -> FastAPI:
    redis_url = redis_url or os.environ.get("REDIS_URL", "redis://localhost:6379/0")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.redis = Redis.from_url(redis_url, decode_responses=True)
        app.state.planner_factory = planner_factory or planner_from_env()
        try:
            yield
        finally:
            await app.state.redis.aclose()

    app = FastAPI(title="AI Task Scheduler", version="0.1.0", lifespan=lifespan)

    @app.get("/healthz")
    async def healthz(request: Request):
        await request.app.state.redis.ping()
        return {"ok": True}

    @app.post("/plans", status_code=202)
    async def submit_plan(request: Request, plan: Any = Body(...)):
        result = validate_plan(plan)
        if not result.ok:
            return _rejected(result.issues)
        request_id = await enqueue(request.app.state.redis, result.value)
        return _accepted(request_id, result.value, result.warnings)

    @app.post("/requests", status_code=202)
    async def submit_request(request: Request, body: CreateRequest):
        planner = request.app.state.planner_factory()
        try:
            # The Groq SDK is synchronous; keep it off the event loop.
            plan = await run_in_threadpool(planner.generate_plan, body.text)
        except PlanRejected as exc:
            return _rejected(exc.issues)
        except GroqError as exc:
            raise HTTPException(status_code=502, detail=f"planner provider error: {exc}") from exc
        warnings = validate_plan(plan.model_dump(mode="json", exclude_none=True)).warnings
        request_id = await enqueue(request.app.state.redis, plan)
        return _accepted(request_id, plan, warnings, planner_attempts=planner.last_attempts)

    @app.get("/requests/{request_id}")
    async def get_request(request: Request, request_id: str):
        if not re.fullmatch(REQUEST_ID_PATTERN, request_id):
            raise HTTPException(status_code=404, detail="unknown request")
        status = await request_status(request.app.state.redis, request_id)
        if status is None:
            raise HTTPException(status_code=404, detail="unknown request")
        return status

    return app


app = create_app()  # uvicorn api.app:app
