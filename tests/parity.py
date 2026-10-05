#!/usr/bin/env python3
"""Cross-language parity check.

Runs the C++ validator (ats_validate) and the Python validator (planner.check) over every
fixture in examples/plans and compares both with examples/expected_validation.json
(accept/reject and error codes; warnings are Python-only). Exit code 0 when everything agrees.

Needs the planner's dependencies (pydantic): run it with `make parity`, or
`uv run --project planner python tests/parity.py`.
"""
import json, subprocess, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "examples" / "plans"
EXPECTED = ROOT / "examples" / "expected_validation.json"
ATS_VALIDATE = ROOT / "scheduler" / "build" / "ats_validate"

sys.path.insert(0, str(ROOT / "planner"))
from planner.check import check_file  # noqa: E402


def cpp_result(path):
    out = subprocess.run([str(ATS_VALIDATE), str(path)], capture_output=True, text=True)
    report = json.loads(out.stdout)
    return report["valid"], sorted({i["code"] for i in report["issues"]})


def main():
    if not ATS_VALIDATE.exists():
        sys.exit(f"{ATS_VALIDATE} not found: run `make build` first")
    expected = json.loads(EXPECTED.read_text())
    on_disk = {p.stem for p in FIXTURES.glob("*.json")}
    ok = on_disk == set(expected)
    if not ok:
        print(f"BAD fixtures and {EXPECTED.name} differ: "
              f"unlisted={sorted(on_disk - set(expected))} missing={sorted(set(expected) - on_disk)}")

    for name, exp in sorted(expected.items()):
        path = FIXTURES / f"{name}.json"
        want = (exp["valid"], sorted(exp["errors"]))
        cpp = cpp_result(path)
        py_report = check_file(path)
        py = (py_report["valid"], py_report["errors"])
        row_ok = cpp == want and py == want
        ok &= row_ok
        print(f"{'OK ' if row_ok else 'BAD'} {name:24} expected={want[1]} cpp={cpp[1]} python={py[1]}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
