"""Plots and tables from bench/results/*.csv.

    python -m bench.analyze            # -> bench/results/*.png and bench/results/summary.md

  sim_scaling.png   simulator: speedup vs workers per workload, FIFO and critical path,
                    with the critical-path bound (sum / critical path)       [make bench]
  e1_overhead.png   distributed vs simulator makespan per workload (E1)    [if dist.csv has E1]
  e2_scaling.png    measured vs simulated speedup per workload (E2)        [if dist.csv has E2]
  e3_efficiency.png efficiency vs task duration, break-even (E3)           [if dist.csv has E3]
  e4_policy.png     critical-path gain over FIFO on layered-24, 100 seeds (E4, simulator)   [make bench]
  e7_requests.png   real requests: latency per mode, planner share (E7)    [if e7.csv]
  e8_expansion.png  open-ended requests: expand vs static plan (E8)        [if e8.csv]
  e9_accuracy.png   public datasets: accuracy of the workflow vs one LLM call (E9)  [if e9.csv]
  e5_tail.png       straggler fan-out: makespan p50/p95/p99 with and without speculation (E5)  [if e5.csv]

Chart conventions (dataviz guidance): one y-axis per chart, thin 2 px lines, at most three
series per chart in fixed palette order (blue, orange, aqua; validated for colour-vision
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

# Reference palette (light): categorical slots 1-3 and the chart chrome. Slot 3 is below 3:1
# contrast on the surface, so every chart that uses it labels its bars with their values.
SERIES = ("#2a78d6", "#eb6834", "#1baf7a")
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


# --- E4, E7, E8 --------------------------------------------------------------------------------


def e4_policy(rows: list[dict], out: Path) -> Path | None:
    """One dot per seed (jittered), with the median marked, per worker count."""
    if not rows:
        return None
    ws = sorted({int(r["workers"]) for r in rows})
    top = max(_num(r["gain_pct"]) for r in rows)
    fig, ax = plt.subplots(figsize=(5.2, 3.1))
    for i, w in enumerate(ws):
        gains = sorted(_num(r["gain_pct"]) for r in rows if int(r["workers"]) == w)
        xs = [i + ((k * 0.6180339) % 1 - 0.5) * 0.5 for k in range(len(gains))]
        ax.scatter(xs, gains, s=10, color=SERIES[0], alpha=0.45, linewidths=0)
        med = gains[len(gains) // 2]
        ax.plot([i - 0.3, i + 0.3], [med, med], color=INK, linewidth=2)
        wins = sum(g > 0.01 for g in gains)
        ax.annotate(f"median {med:+.1f}%\nbetter in {wins}/{len(gains)}", (i, top), xytext=(0, 4),
                    textcoords="offset points", ha="center", va="bottom", fontsize=8, color=INK)
    ax.axhline(0, color=AXIS, linewidth=1)
    ax.set_xticks(range(len(ws)), [f"{w} workers" for w in ws])
    ax.set_xlim(-0.5, len(ws) - 0.5)
    ax.set_ylim(top=top * 1.3)
    ax.set_ylabel("makespan saved vs FIFO (%)")
    ax.set_title("E4: critical-path priority vs FIFO, layered-24, 100 random DAGs", loc="left")
    ax.grid(axis="x", visible=False)
    fig.tight_layout()
    return _save(fig, out / "e4_policy.png")


E7_MODES = (("distributed", "distributed (plan + parallel)"), ("sequential", "sequential agent (plan + 1 at a time)"),
            ("single", "single large call"))


def e7_requests(rows: list[dict], out: Path) -> Path | None:
    if not rows:
        return None
    cases = list(dict.fromkeys(r["case"] for r in rows))
    by = {(r["case"], r["mode"]): r for r in rows}
    fig, ax = plt.subplots(figsize=(6.4, 3.1))
    width = 0.26
    for j, (mode, label) in enumerate(E7_MODES):
        xs = [i + (j - 1) * (width + 0.02) for i in range(len(cases))]
        vals = [_num(by[(c, mode)]["latency_s"]) if (c, mode) in by else math.nan for c in cases]
        bars = ax.bar(xs, vals, width=width, color=SERIES[j], label=label)
        for b, v in zip(bars, vals):
            if not math.isnan(v):
                ax.annotate(f"{v:.0f}s", (b.get_x() + b.get_width() / 2, v), xytext=(0, 2), textcoords="offset points",
                            ha="center", fontsize=7, color=INK)
        if mode != "single":  # the planner call is part of both plan-based latencies
            planner = [_num(by[(c, mode)]["planner_s"]) if (c, mode) in by else 0 for c in cases]
            ax.bar(xs, planner, width=width, color=INK_2, alpha=0.55,
                   label="planner call (included)" if mode == "distributed" else None)
    ax.set_xticks(range(len(cases)), [c.replace("_", " ") for c in cases])
    ax.set_ylabel("end-to-end latency (s)")
    ax.set_title("E7: real requests on Groq (8k tokens/min tier)", loc="left")
    ax.grid(axis="x", visible=False)
    ax.legend(loc="upper left", fontsize=7, ncol=2)
    ax.set_ylim(0, max(_num(r["latency_s"]) for r in rows) * 1.35)
    fig.tight_layout()
    return _save(fig, out / "e7_requests.png")


def e8_expansion(rows: list[dict], out: Path) -> Path | None:
    if not rows:
        return None
    cases = list(dict.fromkeys(r["case"] for r in rows))
    by = {(r["case"], r["mode"]): r for r in rows}
    fig, ax = plt.subplots(figsize=(5.6, 2.9))
    width = 0.36
    for j, (mode, label) in enumerate((("static", "static plan (guessed fan-out)"), ("expand", "runtime expansion"))):
        xs = [i + (j - 0.5) * (width + 0.03) for i in range(len(cases))]
        vals = [_num(by[(c, mode)]["latency_s"]) if (c, mode) in by else math.nan for c in cases]
        bars = ax.bar(xs, vals, width=width, color=SERIES[j], label=label)
        for b, c in zip(bars, cases):
            r = by.get((c, mode))
            if r:
                ax.annotate(f"{_num(r['latency_s']):.0f}s, {r['items']} items", (b.get_x() + b.get_width() / 2,
                            b.get_height()), xytext=(0, 2), textcoords="offset points", ha="center", fontsize=7, color=INK)
    ax.set_xticks(range(len(cases)), [c.replace("_", " ") for c in cases])
    ax.set_ylabel("end-to-end latency (s)")
    ax.set_title("E8: open-ended requests, static plan vs runtime expansion", loc="left")
    ax.grid(axis="x", visible=False)
    ax.legend(loc="upper left", fontsize=7)
    ax.set_ylim(0, max(_num(r["latency_s"]) for r in rows) * 1.35)
    fig.tight_layout()
    return _save(fig, out / "e8_expansion.png")


def _pct(values: list[float], q: float) -> float:
    """Percentile with linear interpolation between closest ranks (numpy's default)."""
    if not values:
        return math.nan
    v = sorted(values)
    pos = q / 100 * (len(v) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(v) - 1)
    return v[lo] + (pos - lo) * (v[hi] - v[lo])


E5_MODES = (("baseline", "no speculation"), ("speculation", "speculation (D56)"))
E5_QS = (50, 95, 99)


def _e5_makespans(rows: list[dict], mode: str) -> list[float]:
    return [_num(r["makespan"]) for r in rows if r["mode"] == mode and r["status"] == "SUCCEEDED"]


def e5_tail(rows: list[dict], out: Path) -> Path | None:
    """Grouped bars: makespan at p50 / p95 / p99, without and with speculation."""
    if not rows:
        return None
    fig, ax = plt.subplots(figsize=(5.2, 3.1))
    width = 0.36
    for k, (mode, label) in enumerate(E5_MODES):
        m = _e5_makespans(rows, mode)
        vals = [_pct(m, q) for q in E5_QS]
        xs = [i + (k - 0.5) * width for i in range(len(E5_QS))]
        bars = ax.bar(xs, vals, width=width * 0.92, color=SERIES[k], label=label)
        for b, v in zip(bars, vals):
            ax.annotate(f"{v:.2f}", (b.get_x() + b.get_width() / 2, v), xytext=(0, 2), textcoords="offset points",
                        ha="center", va="bottom", fontsize=8, color=INK)
    n = len({r["seed"] for r in rows})
    ax.set_xticks(range(len(E5_QS)), [f"p{q}" for q in E5_QS])
    ax.set_ylabel("request makespan (s)")
    ax.set_title(f"E5: speculative execution on a straggler fan-out, {n} paired seeds", loc="left")
    ax.grid(axis="x", visible=False)
    ax.legend(loc="upper left")
    fig.tight_layout()
    return _save(fig, out / "e5_tail.png")


def summary_e5(rows: list[dict], latency: list[dict]) -> str:
    if not rows:
        return ""
    out = ["## E5: speculative execution (16 tasks of 2 s + join, 10 % of attempts straggle 10x, 1/10 time scale)", ""]
    table = []
    for mode, label in E5_MODES:
        m = _e5_makespans(rows, mode)
        runs = [r for r in rows if r["mode"] == mode]
        table.append([label, len(m), *[f"{_pct(m, q):.3f}" for q in E5_QS], f"{max(m):.3f}" if m else "-",
                      sum(int(r["backups"]) for r in runs), sum(int(r["backups_won"]) for r in runs)])
    out += [_table(["mode", "runs", "p50 s", "p95 s", "p99 s", "max s", "backups", "backups won"], table), ""]
    # Extra worker time, paired by seed: (useful + wasted) with speculation / without, - 1.
    by_seed: dict[str, dict[str, float]] = defaultdict(dict)
    for r in rows:
        by_seed[r["seed"]][r["mode"]] = _num(r["useful_ms"]) + _num(r["wasted_ms"])
    extra = [100 * (v["speculation"] / v["baseline"] - 1) for v in by_seed.values()
             if "speculation" in v and "baseline" in v and v["baseline"] > 0]
    if extra:
        out += [f"Extra worker time with speculation, paired by seed: median {_pct(extra, 50):+.1f} %, "
                f"p95 {_pct(extra, 95):+.1f} %. Losing attempts run to completion (no cancellation), "
                "so the cost is the backups' own run time.", ""]
    if latency:
        out += ["## Latency profile (overhead per scheduling step, all E5 runs)", "",
                "pickup: atsd dispatch -> worker task_started; report: worker task_completed -> atsd records it; "
                "handoff: last parent's completion -> child dispatched.", ""]
        prof = []
        for metric in ("pickup", "report", "handoff"):
            v = [_num(r["ms"]) for r in latency if r["metric"] == metric]
            if v:
                prof.append([metric, len(v), *[f"{_pct(v, q):.1f}" for q in (50, 95, 99)], f"{max(v):.1f}"])
        out += [_table(["step", "samples", "p50 ms", "p95 ms", "p99 ms", "max ms"], prof), ""]
    return "\n".join(out)


E9_DATASETS = (("parallelqa", "ParallelQA"), ("hotpotqa_comparison", "HotpotQA comparison"),
               ("movie_recommendation", "Movie recommendation"))
E9_MODES = (("workflow", "workflow (plan + parallel tasks)"), ("single", "single LLM call"))


def _e9_rows(rows: list[dict], dataset: str, mode: str) -> list[dict]:
    return [r for r in rows if r["dataset"] == dataset and r["mode"] == mode]


def e9_accuracy(rows: list[dict], out: Path) -> Path | None:
    """Grouped bars: accuracy per dataset, workflow vs single call, n per bar in the label."""
    if not rows:
        return None
    present = [(d, label) for d, label in E9_DATASETS if any(r["dataset"] == d for r in rows)]
    fig, ax = plt.subplots(figsize=(5.6, 3.1))
    width = 0.36
    for k, (mode, label) in enumerate(E9_MODES):
        vals, ns = [], []
        for d, _ in present:
            rs = _e9_rows(rows, d, mode)
            ns.append(len(rs))
            vals.append(100 * sum(int(r["correct"]) for r in rs) / len(rs) if rs else 0.0)
        xs = [i + (k - 0.5) * width for i in range(len(present))]
        bars = ax.bar(xs, vals, width=width * 0.92, color=SERIES[k], label=label)
        for b, v, n in zip(bars, vals, ns):
            ax.annotate(f"{v:.0f}%\nn={n}", (b.get_x() + b.get_width() / 2, v), xytext=(0, 2),
                        textcoords="offset points", ha="center", va="bottom", fontsize=7.5, color=INK)
    ax.set_xticks(range(len(present)), [label for _, label in present])
    ax.set_ylim(0, 118)
    ax.set_ylabel("correct answers (%)")
    ax.set_title("E9: accuracy on public datasets, workflow vs one LLM call", loc="left")
    ax.grid(axis="x", visible=False)
    ax.legend(loc="upper right", fontsize=8)
    fig.tight_layout()
    return _save(fig, out / "e9_accuracy.png")


def summary_e9(rows: list[dict]) -> str:
    if not rows:
        return ""
    out = ["## E9: accuracy on public datasets, workflow vs one LLM call", "",
           "Correct: a numeric reference within 10 %, otherwise exact match after normalising (LLMCompiler's rule); "
           "F1: token overlap (HotpotQA). Latency includes the planner for the workflow.", ""]
    table = []
    for d, dlabel in E9_DATASETS:
        for mode, mlabel in E9_MODES:
            rs = _e9_rows(rows, d, mode)
            if not rs:
                continue
            ok = [r for r in rs if r["status"] == "SUCCEEDED"]
            lat = [_num(r["latency_s"]) for r in ok]
            tok = [_num(r["tokens_in"]) + _num(r["tokens_out"]) for r in ok]
            table.append([dlabel, mlabel, len(rs), f"{100 * sum(int(r['correct']) for r in rs) / len(rs):.0f}%",
                          f"{sum(_num(r['f1']) for r in rs) / len(rs):.2f}",
                          f"{_pct(lat, 50):.1f}" if lat else "-", f"{_pct(lat, 95):.1f}" if lat else "-",
                          f"{_pct(tok, 50):,.0f}" if tok else "-",
                          f"{sum(_num(r['cost_usd']) for r in rs):.4f}", len(rs) - len(ok),
                          sum(r["extracted"] == "llm" for r in rs)])
    out += [_table(["dataset", "mode", "questions", "correct", "mean F1", "p50 latency s", "p95 latency s",
                    "median tokens", "cost $", "failed runs", "answer extracted by LLM"], table), ""]
    by = []
    for d, dlabel in E9_DATASETS:
        branches = sorted({int(r["branch"]) for r in rows if r["dataset"] == d and str(r["branch"]).isdigit()})
        if len(branches) < 2:
            continue
        for b in branches:
            cells = [dlabel, b]
            for mode, _ in E9_MODES:
                rs = [r for r in _e9_rows(rows, d, mode) if str(r["branch"]) == str(b)]
                lat = [_num(r["latency_s"]) for r in rs if r["status"] == "SUCCEEDED"]
                cells += [f"{100 * sum(int(r['correct']) for r in rs) / len(rs):.0f}% ({len(rs)})" if rs else "-",
                          f"{_pct(lat, 50):.1f}" if lat else "-"]
            by.append(cells)
    if by:
        out += ["By number of entities (branches):", "",
                _table(["dataset", "branches", "workflow correct (n)", "workflow p50 s", "single correct (n)",
                        "single p50 s"], by), ""]
    return "\n".join(out)


def summary_live(e4: list[dict], e7: list[dict], e8: list[dict]) -> str:
    out: list[str] = []
    if e4:
        out += ["## E4: critical-path priority vs FIFO (simulator, layered-24, 100 seeds)", ""]
        rows = []
        for w in sorted({int(r["workers"]) for r in e4}):
            g = sorted(_num(r["gain_pct"]) for r in e4 if int(r["workers"]) == w)
            rows.append([w, f"{g[len(g) // 2]:+.1f}%", f"{_mean(g):+.1f}%", f"{g[-1]:+.1f}%", f"{g[0]:+.1f}%",
                         f"{sum(x > 0.01 for x in g)} / {sum(abs(x) <= 0.01 for x in g)} / {sum(x < -0.01 for x in g)}"])
        out += [_table(["workers", "median saved", "mean", "best", "worst", "better / tie / worse"], rows), ""]
    if e7:
        out += ["## E7: real requests (Groq, openai/gpt-oss-120b)", "",
                _table(["case", "mode", "latency s", "planner s", "planner share", "LLM calls", "tokens in / out",
                        "cost $", "retries", "answer chars"],
                       [[r["case"], r["mode"], r["latency_s"], r["planner_s"],
                         f"{100 * _num(r['planner_share']):.0f}%" if r["mode"] != "single" else "-", r["llm_calls"],
                         f"{r['tokens_in']} / {r['tokens_out']}", f"{_num(r['cost_usd']):.4f}", r["retries"],
                         r["output_chars"]] for r in e7]), ""]
    if e8:
        out += ["## E8: open-ended requests, static plan vs runtime expansion", "",
                _table(["case", "mode", "latency s", "items (requested)", "items", "expand task tokens", "splice delay ms",
                        "tokens in / out", "cost $", "retries"],
                       [[r["case"], r["mode"], r["latency_s"], f"{r['items']} ({r['requested_items'] or '-'})",
                         r["item_names"], r.get("expand_tokens", ""), r["splice_delay_ms"] or "-",
                         f"{r['tokens_in']} / {r['tokens_out']}", f"{_num(r['cost_usd']):.4f}", r["retries"]] for r in e8]), ""]
    return "\n".join(out)


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
    e4, e7, e8 = _read(a.results / "e4.csv"), _read(a.results / "e7.csv"), _read(a.results / "e8.csv")
    e5, e5_latency = _read(a.results / "e5.csv"), _read(a.results / "e5_latency.csv")
    e9 = _read(a.results / "e9.csv")
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
    for fn, rows in ((e4_policy, e4), (e5_tail, e5), (e7_requests, e7), (e8_expansion, e8), (e9_accuracy, e9)):
        if (p_ := fn(rows, a.results)):
            written.append(p_)
    (a.results / "summary.md").write_text(summary(sim, dist, be) + "\n" + summary_live(e4, e7, e8) + "\n"
                                          + summary_e5(e5, e5_latency) + "\n" + summary_e9(e9) + "\n")
    for w in written + [a.results / "summary.md"]:
        print(f"wrote {w}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
