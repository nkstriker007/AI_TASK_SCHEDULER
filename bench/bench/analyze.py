"""Plots and tables from bench/results/*.csv.

    python -m bench.analyze            # -> bench/results/*.png and bench/results/summary.md

  sim_scaling.png   simulator: speedup vs workers per workload, FIFO and critical path,
                    with the critical-path bound (sum / critical path)       [make bench]
  e1_overhead.png   distributed vs simulator makespan per workload (E1)    [if dist.csv has E1]
  e2_scaling.png    measured vs simulated speedup per workload (E2)        [if dist.csv has E2]
  e3_efficiency.png efficiency vs task duration, break-even (E3)           [if dist.csv has E3]

Chart conventions (dataviz guidance): one y-axis per chart, thin 2 px lines, at most two
series per chart in fixed palette order (blue, then orange; validated for colour-vision
deficiency), legends whenever there are two series, text in ink colours, light gridlines.
summary.md carries every number as a table, so nothing depends on colour alone.
"""

from __future__ import annotations

import argparse
import csv
import itertools
import math
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from .sim import RESULTS_DIR

# Reference palette (light): categorical slots 1-2 and the chart chrome.
SERIES = ("#2a78d6", "#eb6834")
SURFACE, INK, INK_2, MUTED, GRID, AXIS = "#fcfcfb", "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"
WORKLOAD_ORDER = ("chain-10", "layered-24", "straggler-16", "fanout-32", "tiny-48")


def _style() -> None:
    plt.rcParams.update({
        "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
        "axes.edgecolor": AXIS, "axes.labelcolor": INK_2, "axes.titlecolor": INK, "text.color": INK,
        "xtick.color": INK_2, "ytick.color": INK_2, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6,
        "axes.spines.top": False, "axes.spines.right": False, "font.size": 9, "axes.titlesize": 10,
        "axes.titleweight": "bold", "lines.linewidth": 2, "lines.markersize": 5, "legend.frameon": False,
    })


def _read(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open() as f:
        return list(csv.DictReader(f))


def _num(v: str) -> float:
    return float(v) if v not in ("", None) else math.nan


def _save(fig, path: Path) -> Path:
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def _workers_axis(ax, ticks) -> None:
    ax.set_xscale("log", base=2)
    ax.set_xticks(ticks, [str(t) for t in ticks])
    ax.minorticks_off()
    ax.set_xlabel("workers")


# --- simulator ---------------------------------------------------------------------------


def sim_scaling(rows: list[dict], out: Path) -> Path:
    by = defaultdict(dict)
    bound = {}
    for r in rows:
        w = int(r["workers"])
        if w == 0:
            continue  # unlimited: shown as the bound instead
        by[(r["workload"], r["policy"])][w] = _num(r["speedup"])
        bound[r["workload"]] = _num(r["max_speedup"])
    workloads = [w for w in WORKLOAD_ORDER if w in bound] + sorted(set(bound) - set(WORKLOAD_ORDER))
    fig, axes = plt.subplots(1, len(workloads), figsize=(3.0 * len(workloads), 2.9), squeeze=False)
    for ax, wl in zip(axes[0], workloads):
        # Critical path dashed with square markers over solid FIFO: where the policies tie, both
        # stay visible (shape and dash are a second cue besides colour).
        for color, policy, label, style, marker in ((SERIES[0], "fifo", "FIFO", "-", "o"),
                                                    (SERIES[1], "critical-path", "critical path", (0, (3, 2)), "s")):
            pts = sorted(by[(wl, policy)].items())
            ax.plot([p[0] for p in pts], [p[1] for p in pts], color=color, linestyle=style, marker=marker, label=label)
        if by[(wl, "fifo")] == by[(wl, "critical-path")]:
            ax.annotate("policies tie", (0.98, 0.04), xycoords="axes fraction", ha="right", color=INK_2, fontsize=8)
        ax.axhline(bound[wl], color=MUTED, linestyle=(0, (4, 3)), linewidth=1)
        ax.annotate(f"bound {bound[wl]:.1f}x", (1, bound[wl]), xytext=(2, 3), textcoords="offset points",
                    color=INK_2, fontsize=8)
        ax.set_title(wl, loc="left")
        _workers_axis(ax, [1, 2, 4, 8, 16])
        ax.set_ylim(0, max(bound[wl], max(by[(wl, "fifo")].values())) * 1.18)
    axes[0][0].set_ylabel("speedup over 1 worker")
    handles, labels = axes[0][0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper right", ncol=2, bbox_to_anchor=(1.0, 1.04))
    fig.suptitle("Simulated speedup by worker count (ats_sim)", x=0.01, ha="left", y=1.04, fontsize=11, fontweight="bold")
    fig.tight_layout()
    return _save(fig, out / "sim_scaling.png")


# --- distributed ----------------------------------------------------------------------------


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else math.nan


def e1_overhead(rows: list[dict], out: Path) -> Path | None:
    e1 = [r for r in rows if r["experiment"] == "E1"]
    if not e1:
        return None
    groups = defaultdict(list)
    for r in e1:
        groups[r["workload"]].append(_num(r["overhead_pct"]))
    names = [w for w in WORKLOAD_ORDER if w in groups] + sorted(set(groups) - set(WORKLOAD_ORDER))
    values = [_mean(groups[n]) for n in names]
    fig, ax = plt.subplots(figsize=(5.2, 2.8))
    bars = ax.bar(names, values, color=SERIES[0], width=0.55)
    for b, v in zip(bars, values):
        ax.annotate(f"{v:+.1f}%", (b.get_x() + b.get_width() / 2, v), xytext=(0, 3), textcoords="offset points",
                    ha="center", color=INK, fontsize=8)
    ax.axhline(0, color=AXIS, linewidth=1)
    ax.set_ylabel("extra makespan vs simulator (%)")
    ax.set_title("E1: distributed overhead over the simulator, 4 workers", loc="left")
    ax.set_ylim(min(0, min(values)) * 1.3, max(values) * 1.35 if max(values) > 0 else 1)
    ax.grid(axis="x", visible=False)
    fig.tight_layout()
    return _save(fig, out / "e1_overhead.png")


def e2_scaling(rows: list[dict], out: Path) -> Path | None:
    e2 = [r for r in rows if r["experiment"] == "E2"]
    if not e2:
        return None
    measured, simulated = defaultdict(lambda: defaultdict(list)), defaultdict(lambda: defaultdict(list))
    for r in e2:
        measured[r["workload"]][int(r["workers"])].append(_num(r["makespan"]))
        simulated[r["workload"]][int(r["workers"])].append(_num(r["sim_makespan"]))
    names = [w for w in WORKLOAD_ORDER if w in measured]
    fig, axes = plt.subplots(1, len(names), figsize=(3.2 * len(names), 2.9), squeeze=False)
    for ax, wl in zip(axes[0], names):
        ws = sorted(measured[wl])
        base_m, base_s = _mean(measured[wl][1]), _mean(simulated[wl][1])  # measured 1-worker run (3.10)
        ax.plot(ws, [base_m / _mean(measured[wl][w]) for w in ws], color=SERIES[0], marker="o", label="measured")
        # dashed with square markers, so a near-perfect match does not hide the measured line
        ax.plot(ws, [base_s / _mean(simulated[wl][w]) for w in ws], color=SERIES[1], linestyle=(0, (3, 2)),
                marker="s", label="simulator")
        ax.set_title(wl, loc="left")
        _workers_axis(ax, ws)
        ax.set_ylim(0, None)
    axes[0][0].set_ylabel("speedup over 1 worker")
    handles, labels = axes[0][0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper right", ncol=2, bbox_to_anchor=(1.0, 1.04))
    fig.suptitle("E2: measured speedup vs the simulator", x=0.01, ha="left", y=1.04, fontsize=11, fontweight="bold")
    fig.tight_layout()
    return _save(fig, out / "e2_scaling.png")


def _break_even(points: list[tuple[float, float]], level: float = 50.0) -> float | None:
    """Task duration where efficiency crosses `level` %, interpolated on a log scale."""
    for (d0, e0), (d1, e1) in itertools.pairwise(points):
        if (e0 - level) * (e1 - level) <= 0 and e0 != e1:
            t = (level - e0) / (e1 - e0)
            return 10 ** (math.log10(d0) + t * (math.log10(d1) - math.log10(d0)))
    return None


def e3_efficiency(rows: list[dict], out: Path) -> tuple[Path | None, float | None]:
    e3 = [r for r in rows if r["experiment"] == "E3"]
    if not e3:
        return None, None
    groups = defaultdict(list)
    for r in e3:
        groups[_num(r["task_seconds"])].append(100 * _num(r["sim_makespan"]) / _num(r["makespan"]))
    points = sorted((d, _mean(v)) for d, v in groups.items())
    be = _break_even(points)
    fig, ax = plt.subplots(figsize=(5.2, 2.9))
    ax.plot([p[0] * 1000 for p in points], [p[1] for p in points], color=SERIES[0], marker="o")
    ax.axhline(50, color=MUTED, linestyle=(0, (4, 3)), linewidth=1)
    ax.annotate("50% efficiency", (points[-1][0] * 1000, 50), xytext=(-2, 4), textcoords="offset points",
                ha="right", color=INK_2, fontsize=8)
    if be:
        ax.axvline(be * 1000, color=MUTED, linestyle=":", linewidth=1)
        ax.annotate(f"break-even ~{be * 1000:.0f} ms", (be * 1000, 8), xytext=(4, 0), textcoords="offset points",
                    color=INK, fontsize=8)
    ax.set_xscale("log")
    ticks = [p[0] * 1000 for p in points]
    ax.set_xticks(ticks, [f"{t:g}" for t in ticks])
    ax.minorticks_off()
    ax.set_xlabel("task duration (ms, log scale)")
    ax.set_ylabel("efficiency (simulated / measured)")
    ax.set_ylim(0, 105)
    ax.set_title("E3: where parallelism stops paying (32 tasks + join, 8 workers)", loc="left")
    fig.tight_layout()
    return _save(fig, out / "e3_efficiency.png"), be


# --- summary -------------------------------------------------------------------------------


def _table(headers: list[str], rows: list[list]) -> str:
    lines = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    lines += ["| " + " | ".join(str(c) for c in row) + " |" for row in rows]
    return "\n".join(lines)


def summary(sim: list[dict], dist: list[dict], break_even: float | None) -> str:
    out = ["# Benchmark summary", "", "Generated by `python -m bench.analyze` from `results/sim.csv`"
           + (" and `results/dist.csv`." if dist else "."), ""]
    if sim:
        out += ["## Simulator (ats_sim), FIFO", ""]
        rows = []
        for wl in [w for w in WORKLOAD_ORDER if any(r["workload"] == w for r in sim)]:
            s = {int(r["workers"]): r for r in sim if r["workload"] == wl and r["policy"] == "fifo"}
            cp = {int(r["workers"]): r for r in sim if r["workload"] == wl and r["policy"] == "critical-path"}
            rows.append([wl, s[1]["tasks"], f"{_num(s[1]['makespan']):.2f}", f"{_num(s[1]['critical_path_seconds']):.2f}",
                         *(f"{_num(s[w]['speedup']):.2f}x" for w in (2, 4, 8, 16)), f"{_num(s[1]['max_speedup']):.2f}x",
                         f"{_num(cp[4]['makespan']):.2f} / {_num(s[4]['makespan']):.2f}"])
        out += [_table(["workload", "tasks", "sequential s", "critical path s", "2 w", "4 w", "8 w", "16 w", "bound",
                        "4 w makespan CP / FIFO"], rows), ""]
    e1 = [r for r in dist if r["experiment"] == "E1"]
    if e1:
        out += ["## E1: distributed vs simulator (4 workers)", "",
                _table(["workload", "measured s", "simulated s", "overhead"],
                       [[r["workload"], r["makespan"], r["sim_makespan"], f"{_num(r['overhead_pct']):+.1f}%"] for r in e1]), ""]
    e2 = [r for r in dist if r["experiment"] == "E2"]
    if e2:
        out += ["## E2: scaling (speedup over a measured 1-worker run)", ""]
        rows = []
        for wl in [w for w in WORKLOAD_ORDER if any(r["workload"] == w for r in e2)]:
            m = {int(r["workers"]): _num(r["makespan"]) for r in e2 if r["workload"] == wl}
            s = {int(r["workers"]): _num(r["sim_makespan"]) for r in e2 if r["workload"] == wl}
            rows.append([wl] + [f"{m[1] / m[w]:.2f}x ({s[1] / s[w]:.2f}x)" for w in sorted(m)])
        ws = sorted({int(r["workers"]) for r in e2})
        out += [_table(["workload"] + [f"{w} w measured (sim)" for w in ws], rows), ""]
    e3 = [r for r in dist if r["experiment"] == "E3"]
    if e3:
        out += ["## E3: overhead vs task duration (32 tasks + join, 8 workers)", "",
                _table(["task duration", "measured s", "simulated s", "efficiency", "overhead per step"],
                       [[f"{_num(r['task_seconds']) * 1000:g} ms", r["makespan"], r["sim_makespan"],
                         f"{100 * _num(r['sim_makespan']) / _num(r['makespan']):.0f}%", f"{_num(r['per_step_overhead_ms']):.1f} ms"]
                        for r in sorted(e3, key=lambda r: _num(r["task_seconds"]))]), ""]
        if break_even:
            out += [f"Efficiency falls below 50% for tasks shorter than about **{break_even * 1000:.0f} ms**.", ""]
    return "\n".join(out)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--results", type=Path, default=RESULTS_DIR)
    a = p.parse_args(argv)
    _style()
    sim, dist = _read(a.results / "sim.csv"), _read(a.results / "dist.csv")
    if not sim and not dist:
        raise SystemExit(f"no results in {a.results}: run `python -m bench.sim` (and `python -m bench.run`) first")
    written = []
    if sim:
        written.append(sim_scaling(sim, a.results))
    for fn in (e1_overhead, e2_scaling):
        if (p_ := fn(dist, a.results)):
            written.append(p_)
    e3_path, be = e3_efficiency(dist, a.results)
    if e3_path:
        written.append(e3_path)
    (a.results / "summary.md").write_text(summary(sim, dist, be) + "\n")
    for w in written + [a.results / "summary.md"]:
        print(f"wrote {w}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
