"""Generate schemas/*.schema.json from the Pydantic models.

    python -m planner.export_schemas          # write
    python -m planner.export_schemas --check  # exit 1 if the committed files are stale
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pydantic import BaseModel

from .messages import TaskEvent, TaskMessage
from .paths import SCHEMAS_DIR
from .schema import ExecutionPlan, PlanFile, Subplan

SCHEMAS: dict[str, type[BaseModel]] = {
    "execution_plan.schema.json": ExecutionPlan,
    "plan_file.schema.json": PlanFile,
    "subplan.schema.json": Subplan,
    "task_message.schema.json": TaskMessage,
    "task_event.schema.json": TaskEvent,
}


def render(model: type[BaseModel]) -> str:
    schema = model.model_json_schema()
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    return json.dumps(schema, indent=2, sort_keys=True) + "\n"


def stale_files(directory: Path = SCHEMAS_DIR) -> list[str]:
    stale = []
    for name, model in SCHEMAS.items():
        path = directory / name
        if not path.exists() or path.read_text() != render(model):
            stale.append(name)
    return stale


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--check", action="store_true", help="fail if committed schemas are stale")
    parser.add_argument("--out", type=Path, default=SCHEMAS_DIR)
    args = parser.parse_args(argv)

    if args.check:
        stale = stale_files(args.out)
        if stale:
            print("stale schema files (run python -m planner.export_schemas): " + ", ".join(stale), file=sys.stderr)
            return 1
        return 0

    args.out.mkdir(parents=True, exist_ok=True)
    for name, model in SCHEMAS.items():
        (args.out / name).write_text(render(model))
        print(f"wrote {args.out / name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
