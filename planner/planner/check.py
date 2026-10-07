"""Validate plan files (used by the parity check against the C++ loader).

    python -m planner.check examples/plans/*.json          # human-readable
    python -m planner.check --json examples/plans/*.json   # {file: {valid, errors, warnings}}

Exit code: 0 if every file is valid, 1 otherwise.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .validate import validate_plan_file


def check_file(path: Path) -> dict:
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        return {"valid": False, "errors": ["SCHEMA"], "warnings": [], "issues": [f"invalid JSON: {exc}"]}
    result = validate_plan_file(data)
    return {
        "valid": result.ok,
        "errors": sorted({i.code.value for i in result.errors}),
        "warnings": sorted({i.code.value for i in result.warnings}),
        "issues": [str(i) for i in result.issues],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate plan files.")
    parser.add_argument("files", nargs="+", type=Path)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    results = {str(p): check_file(p) for p in args.files}
    if args.json:
        print(json.dumps(results, indent=2))
    else:
        for name, r in results.items():
            print(f"{'valid  ' if r['valid'] else 'INVALID'} {name}")
            for issue in r["issues"]:
                print(f"    {issue}")
    return 0 if all(r["valid"] for r in results.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
