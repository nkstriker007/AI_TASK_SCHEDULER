"""Simulator sweep: every workload x worker count x policy through ats_sim (no Redis needed).

    python -m bench.sim                      # -> bench/results/sim.csv

This is the reference the distributed runs are compared against (E1) and the source of the
sim plots (`make bench`). ats_sim is deterministic, so one run per configuration is enough.
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
from pathlib import Path

from .gen import WORKLOADS_DIR

REPO = Path(__file__).resolve().parents[2]
ATS_SIM = REPO / "scheduler" / "build" / "ats_sim"
RESULTS_DIR = Path(__file__).resolve().parents[1] / "results"
WORKERS = (1, 2, 4, 8, 16, 0)  # 0 = unlimited
POLICIES = ("fifo", "critical-path")
FIELDS = ("workload", "tasks", "workers", "policy", "makespan", "sequential_seconds", "critical_path_seconds",
          "speedup", "max_speedup", "utilization", "peak_parallelism")


def simulate(plan: Path, workers: int, policy: str, ats_sim: Path = ATS_SIM) -> dict:
    out = subprocess.run([str(ats_sim), str(plan), "--workers", str(workers), "--policy", policy, "--json"],
                         capture_output=True, text=True, check=False)
    if out.returncode != 0:
        raise RuntimeError(f"ats_sim failed on {plan.name}: {out.stderr or out.stdout}")
    return json.loads(out.stdout)


def sweep(workloads_dir: Path = WORKLOADS_DIR, ats_sim: Path = ATS_SIM) -> list[dict]:
    rows = []
    for plan in sorted(workloads_dir.glob("*.json")):
        for policy in POLICIES:
            for workers in WORKERS:
                r = simulate(plan, workers, policy, ats_sim)
                rows.append({"workload": plan.stem, "workers": workers, "policy": policy,
                             **{k: r[k] for k in FIELDS if k in r and k not in ("workload", "workers", "policy")}})
    return rows


def write_csv(rows: list[dict], path: Path, fields=FIELDS) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", type=Path, default=RESULTS_DIR / "sim.csv")
    p.add_argument("--ats-sim", type=Path, default=ATS_SIM)
    a = p.parse_args(argv)
    if not a.ats_sim.exists():
        raise SystemExit(f"ats_sim not found at {a.ats_sim}: run `make build` first")
    rows = sweep(ats_sim=a.ats_sim)
    write_csv(rows, a.out)
    print(f"wrote {a.out} ({len(rows)} simulations)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
