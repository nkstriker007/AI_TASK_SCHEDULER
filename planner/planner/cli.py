"""Planner CLI: request text -> validated plan file.

    python -m planner.cli "<request>" -o plan.json
    python -m planner.cli "<request>" -o plan.json --fake company_research

Exit codes: 0 plan written, 1 plan rejected, 2 provider error.
"""

from __future__ import annotations

import argparse
import json
import secrets
import sys
from datetime import datetime, timezone
from pathlib import Path

from .llm import PlanRejected, Planner
from .schema import ExecutionPlan, PlanFile
from .validate import validate_plan


def new_request_id() -> str:
    return f"req_{secrets.token_hex(4)}"


def make_plan_file(plan: ExecutionPlan, request_id: str | None = None) -> PlanFile:
    """Wrap a plan in the runtime envelope. The LLM never produces request_id."""
    return PlanFile(
        request_id=request_id or new_request_id(),
        created_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        plan=plan,
    )


def build_planner(args: argparse.Namespace) -> Planner:
    if args.fake:
        from .fake import FakePlanner

        return FakePlanner(default=args.fake)
    from .llm import GroqPlanner

    return GroqPlanner(model=args.model)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Turn a request into a validated plan file.")
    parser.add_argument("request", help="natural-language request")
    parser.add_argument("-o", "--output", type=Path, help="plan file to write (default: stdout)")
    parser.add_argument("--model", help="Groq model id (default: $ATS_PLANNER_MODEL or openai/gpt-oss-120b)")
    parser.add_argument("--fake", metavar="FIXTURE", help="use FakePlanner with examples/plans/FIXTURE.json")
    args = parser.parse_args(argv)

    from groq import GroqError

    try:
        plan = build_planner(args).generate_plan(args.request)
    except PlanRejected as exc:
        print(exc, file=sys.stderr)
        if exc.raw is not None:
            print("raw output:\n" + (exc.raw if isinstance(exc.raw, str) else json.dumps(exc.raw, indent=2)),
                  file=sys.stderr)
        return 1
    except GroqError as exc:  # missing key, auth, rate limit, network
        print(f"provider error: {exc}", file=sys.stderr)
        return 2

    for warning in validate_plan(plan.model_dump(mode="json", exclude_none=True)).warnings:
        print(warning, file=sys.stderr)

    text = make_plan_file(plan).model_dump_json(indent=2, exclude_none=True) + "\n"
    if args.output:
        args.output.write_text(text)
        print(f"wrote {args.output} ({len(plan.tasks)} tasks)", file=sys.stderr)
    else:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
