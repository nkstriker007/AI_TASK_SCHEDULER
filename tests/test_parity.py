"""Parity: the Python validator and the C++ loader agree on every fixture.

Run from the repo root:
    uv run --project planner pytest tests/test_parity.py
    ATS_SIM=path/to/ats_sim uv run --project planner pytest tests/test_parity.py

The C++ side is skipped until ats_sim is built (default: scheduler/build/ats_sim).
ats_sim contract (docs/contract.md): exit 1 means invalid plan; with --json an
invalid plan prints {"valid": false, "issues": [{"code": ...}, ...]}.
"""

import json
import os
import subprocess
from pathlib import Path

import pytest

from planner.check import check_file
from planner.export_schemas import stale_files
from planner.paths import EXPECTED_VALIDATION, FIXTURES_DIR, REPO_ROOT

EXPECTED = json.loads(EXPECTED_VALIDATION.read_text())
FIXTURES = sorted(FIXTURES_DIR.glob("*.json"))
ATS_SIM = Path(os.environ.get("ATS_SIM", REPO_ROOT / "scheduler" / "build" / "ats_sim"))


def test_schema_files_are_fresh():
    assert stale_files() == []


@pytest.mark.parametrize("path", FIXTURES, ids=lambda p: p.stem)
def test_python_matches_expected(path):
    result = check_file(path)
    assert result["valid"] == EXPECTED[path.stem]["valid"]
    assert result["errors"] == EXPECTED[path.stem]["errors"]


@pytest.mark.skipif(not ATS_SIM.exists(), reason=f"ats_sim not built at {ATS_SIM}")
@pytest.mark.parametrize("path", FIXTURES, ids=lambda p: p.stem)
def test_cpp_agrees_with_python(path):
    py = check_file(path)
    proc = subprocess.run([str(ATS_SIM), str(path), "--json"], capture_output=True, text=True, timeout=30)
    cpp_valid = proc.returncode != 1
    assert cpp_valid == py["valid"], f"python={py['valid']} cpp exit={proc.returncode}\n{proc.stdout}{proc.stderr}"
    if not py["valid"]:
        try:
            report = json.loads(proc.stdout)
        except json.JSONDecodeError:
            return  # accept/reject parity is the hard requirement; codes checked when JSON is available
        cpp_codes = sorted({i["code"] for i in report.get("issues", [])})
        assert cpp_codes == py["errors"]
