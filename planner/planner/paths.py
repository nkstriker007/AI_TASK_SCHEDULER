<<<<<<< HEAD
"""Repository paths used by the planner tools (development layout)."""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMAS_DIR = REPO_ROOT / "schemas"
FIXTURES_DIR = REPO_ROOT / "examples" / "plans"
# Expected accept/reject + issue codes per fixture; shared with the C++ tests.
EXPECTED_VALIDATION = REPO_ROOT / "examples" / "expected_validation.json"
=======
"""Repository paths used by the planner tools (development layout)."""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMAS_DIR = REPO_ROOT / "schemas"
FIXTURES_DIR = REPO_ROOT / "examples" / "plans"
# Expected accept/reject + issue codes per fixture; shared with the C++ tests.
EXPECTED_VALIDATION = REPO_ROOT / "examples" / "expected_validation.json"
# JSON examples for contract #2 (task messages and events).
MESSAGES_DIR = REPO_ROOT / "examples" / "messages"
# Planner evaluation cases (design doc section 6).
EVAL_CASES = REPO_ROOT / "examples" / "requests" / "eval_cases.yaml"
>>>>>>> 5ebdf693386164bbcffe0b799e8fa8a6dc501faa
