"""Artifact spill for large results (design doc 8.1, contract #2 "Result values").

A result whose JSON is larger than ARTIFACT_LIMIT_BYTES is written to a file under the
artifacts directory (a volume shared by workers and the API), and the result key holds a
pointer instead:  {"artifact": "<request_id>/<task_id>.<attempt>.json", "bytes": <size>}.

The file name includes the attempt: if two attempts of one task both finish, each writes
its own file and only the attempt whose SET NX wins is referenced; the loser deletes its
file. A shared name would let the loser overwrite the winner's file after the pointer was
committed.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

ARTIFACT_LIMIT_BYTES = 64 * 1024
ARTIFACTS_DIR_ENV = "ATS_ARTIFACTS_DIR"
DEFAULT_ARTIFACTS_DIR = "artifacts"


def artifacts_root(env: dict[str, str] | None = None) -> Path:
    env = os.environ if env is None else env
    return Path(env.get(ARTIFACTS_DIR_ENV) or DEFAULT_ARTIFACTS_DIR)


def artifact_name(request_id: str, task_id: str, attempt: int) -> str:
    return f"{request_id}/{task_id}.{attempt}.json"


def make_pointer(name: str, size: int) -> dict[str, Any]:
    return {"artifact": name, "bytes": size}


def is_pointer(value: Any) -> bool:
    return (isinstance(value, dict) and set(value) == {"artifact", "bytes"}
            and isinstance(value["artifact"], str) and isinstance(value["bytes"], int))


def artifact_path(root: Path, name: str) -> Path:
    """The file for an artifact name, refusing anything that would leave the root."""
    if not name or Path(name).is_absolute() or ".." in Path(name).parts:
        raise ValueError(f"invalid artifact name {name!r}")
    root = root.resolve()
    path = (root / name).resolve()
    if root not in path.parents:
        raise ValueError(f"artifact {name!r} is outside {root}")
    return path


def write_artifact(root: Path, name: str, text: str) -> Path:
    """Write atomically (temp file + rename), so a reader never sees half a file."""
    path = artifact_path(root, name)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)
    return path


def read_artifact(root: Path, pointer: dict[str, Any]) -> Any:
    return json.loads(artifact_path(root, pointer["artifact"]).read_text(encoding="utf-8"))


def resolve(value: Any, root: Path) -> Any:
    """A parsed result value, with an artifact pointer replaced by the file's content."""
    return read_artifact(root, value) if is_pointer(value) else value
