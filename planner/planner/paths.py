"""Repository paths used by the planner tools (development layout)."""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMAS_DIR = REPO_ROOT / "schemas"
FIXTURES_DIR = REPO_ROOT / "examples" / "plans"
# Expected accept/reject + issue codes per fixture; shared with the C++ tests.
EXPECTED_VALIDATION = REPO_ROOT / "examples" / "expected_validation.json"
