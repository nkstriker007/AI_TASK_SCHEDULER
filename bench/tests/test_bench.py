import asyncio
import csv
import json
import shutil
import subprocess
import time

import pytest
from planner.validate import validate_plan_file

from bench import analyze, gen, run, sim


def test_workloads_are_valid_and_shaped_as_specified():
    wl = gen.workloads()
    assert set(wl) == {"chain-10", "fanout-32", "layered-24", "straggler-16", "tiny-48"}
    for doc in wl.values():
        assert validate_plan_file(doc).ok
    tasks = {name: doc["plan"]["tasks"] for name, doc in wl.items()}
    assert [t["depends_on"] for t in tasks["chain-10"]][1] == ["t1"]
    assert len(tasks["fanout-32"]) == 33 and len(tasks["fanout-32"][-1]["depends_on"]) == 32
    secs = sorted(t["hints"]["estimated_seconds"] for t in tasks["straggler-16"][:-1])
    assert secs[-1] == 10 * secs[0]
    assert gen.layered() == gen.layered()  # seeded: identical every time
    assert all(t["hints"]["estimated_seconds"] == 0.01 for t in tasks["tiny-48"])


needs_sim = pytest.mark.skipif(not sim.ATS_SIM.exists(), reason="ats_sim not built")


@needs_sim
def test_sim_sweep_and_analysis(tmp_path):
    for name, doc in gen.workloads().items():
        (tmp_path / f"{name}.json").write_text(json.dumps(doc))
    rows = sim.sweep(tmp_path)
    assert len(rows) == 5 * len(sim.WORKERS) * len(sim.POLICIES)
    chain = {int(r["workers"]): r for r in rows if r["workload"] == "chain-10" and r["policy"] == "fifo"}
    assert all(abs(chain[w]["speedup"] - 1.0) < 1e-9 for w in (1, 16))  # a chain never speeds up
    fan = {int(r["workers"]): r for r in rows if r["workload"] == "fanout-32" and r["policy"] == "fifo"}
    assert fan[16]["speedup"] > 10 and fan[0]["max_speedup"] == pytest.approx(16.5)

    results = tmp_path / "results"
    sim.write_csv(rows, results / "sim.csv")
    analyze._style()
    assert analyze.sim_scaling(analyze._read(results / "sim.csv"), results).exists()
    text = analyze.summary(analyze._read(results / "sim.csv"), [], None)
    assert "| fanout-32 | 33 |" in text


def test_break_even_interpolates_on_a_log_scale():
    points = [(0.001, 20.0), (0.01, 80.0)]
    assert analyze._break_even(points) == pytest.approx(10 ** -2.5)
    assert analyze._break_even([(0.1, 90.0), (1.0, 99.0)]) is None


@needs_sim
@pytest.mark.skipif(not run.ATSD.exists() or not shutil.which("redis-server"), reason="needs atsd and redis-server")
def test_one_distributed_run_matches_the_simulator():
    port = run._free_port()
    proc = subprocess.Popen(["redis-server", "--port", str(port), "--save", "", "--appendonly", "no"],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        time.sleep(0.3)
        doc = gen.fanout(8, seconds=1.0, name="test-fanout")
        m = asyncio.run(run.run_once(f"redis://127.0.0.1:{port}/0", doc, 4, 0.05))
        sim_makespan, steps = run._simulate(doc, 4, 0.05, sim.ATS_SIM)
        assert m["status"] == "SUCCEEDED"
        assert steps == 3 and sim_makespan == pytest.approx(0.15)  # 2 rounds of 4, then the join
        assert sim_makespan <= m["makespan"] < sim_makespan + 0.5
    finally:
        proc.terminate()
        proc.wait()


def test_results_csv_fields_round_trip(tmp_path):
    path = tmp_path / "dist.csv"
    sim.write_csv([{f: 1 for f in run.FIELDS}], path, run.FIELDS)
    with path.open() as f:
        assert next(iter(csv.DictReader(f)))["per_step_overhead_ms"] == "1"
