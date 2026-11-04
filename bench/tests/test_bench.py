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


@needs_sim
def test_e4_compares_policies_per_seed():
    rows = sim.e4(seeds=range(1, 4))
    assert len(rows) == 3 * len(sim.E4_WORKERS)
    for r in rows:
        assert r["critical_path"] <= r["fifo"] * 1.05  # never much worse
        assert r["gain_pct"] == pytest.approx(100 * (r["fifo"] - r["critical_path"]) / r["fifo"], abs=1e-3)


def test_e7_runs_tasks_in_dependency_order():
    from planner.fake import load_fixture_plan
    from planner.schema import ExecutionPlan

    from bench import e7

    plan = ExecutionPlan.model_validate(load_fixture_plan("company_research"))
    order = e7._order(plan)
    pos = {t: i for i, t in enumerate(order)}
    assert sorted(order) == sorted(t.id for t in plan.tasks)
    assert all(pos[d] < pos[t.id] for t in plan.tasks for d in t.depends_on)


def test_e8_static_prompt_forbids_expand_and_reads_requested_counts():
    from bench import e8

    prompt = e8.static_prompt()
    assert "Never use the expand type" in prompt and "Use expand only" not in prompt
    assert "expand." not in prompt.splitlines()[[l.startswith("1. ") for l in prompt.splitlines()].index(True)]
    assert e8.requested_items("Research the top 5 AI chip companies") == 5
    assert e8.requested_items("Find the three largest cloud providers") == 3
    assert e8.requested_items("Identify the most widely used vector databases") is None
    tasks = [{"id": "t1", "type": "research", "title": "Identify the three largest cloud providers", "depends_on": []},
             {"id": "t2", "type": "summarize", "title": "Summarize AWS AI strategy", "depends_on": ["t1"]},
             {"id": "t3", "type": "compare", "title": "Compare", "depends_on": ["t2"]}]
    assert [t["id"] for t in e8.static_items(tasks)] == ["t2"]


def test_live_charts_and_tables_from_rows(tmp_path):
    analyze._style()
    e7_rows = [{"case": "two_companies", "mode": m, "latency_s": str(v), "planner_s": "4", "planner_share": "0.3",
                "llm_calls": "7", "tokens_in": "100", "tokens_out": "50", "cost_usd": "0.0001", "retries": "0",
                "output_chars": "900", "status": "SUCCEEDED"} for m, v in (("distributed", 10), ("sequential", 14),
                                                                            ("single", 6))]
    e8_rows = [{"case": "top_chip_companies", "mode": m, "latency_s": "40", "items": "5", "requested_items": "5",
                "item_names": "A; B", "expand_tokens": c, "splice_delay_ms": d, "tokens_in": "1", "tokens_out": "1",
                "cost_usd": "0", "retries": "0"} for m, c, d in (("expand", "2", "3"), ("static", "0", ""))]
    assert analyze.e7_requests(e7_rows, tmp_path).exists()
    assert analyze.e8_expansion(e8_rows, tmp_path).exists()
    text = analyze.summary_live([], e7_rows, e8_rows)
    assert "| two_companies | single | 6 |" in text and "| top_chip_companies | expand | 40 | 5 (5) |" in text


# --------------------------------------------------------------------------------- E5 (Person B)

from bench import e5  # noqa: E402


def test_e5_plan_is_a_valid_fanout_with_a_join():
    doc = e5.plan_doc("req_e5_t", tasks=4, seconds=1.0)
    assert validate_plan_file(doc).ok
    tasks = doc["plan"]["tasks"]
    assert len(tasks) == 5 and tasks[-1]["depends_on"] == ["t1", "t2", "t3", "t4"]


def test_e5_percentile_interpolates_like_numpy():
    assert e5.percentile([1, 2, 3, 4], 50) == 2.5
    assert e5.percentile([10], 99) == 10
    assert e5.percentile(list(range(101)), 99) == 99


def test_e5_analyse_counts_backups_waste_and_latency_steps():
    doc = e5.plan_doc("req_x", tasks=2, seconds=1.0)  # t1, t2 -> t3
    log = [
        {"ts_ms": 1000, "event": "dispatched", "request": "req_x", "task": "t1", "attempt": 1},
        {"ts_ms": 1000, "event": "dispatched", "request": "req_x", "task": "t2", "attempt": 1},
        {"ts_ms": 1300, "event": "completed", "request": "req_x", "task": "t1", "attempt": 1},
        {"ts_ms": 1400, "event": "speculated", "request": "req_x", "task": "t2", "attempt": 2},
        {"ts_ms": 1650, "event": "speculation_won", "request": "req_x", "task": "t2", "attempt": 2},
        {"ts_ms": 1650, "event": "completed", "request": "req_x", "task": "t2", "attempt": 2},
        {"ts_ms": 1652, "event": "dispatched", "request": "req_x", "task": "t3", "attempt": 1},
        {"ts_ms": 1900, "event": "completed", "request": "req_x", "task": "t3", "attempt": 1},
    ]
    worker = [
        {"event": "task_started", "attempt_id": "req_x:t1:1", "ts_ms": 1002},
        {"event": "task_started", "attempt_id": "req_x:t2:1", "ts_ms": 1003},
        {"event": "task_completed", "attempt_id": "req_x:t1:1", "ts_ms": 1298, "committed": True, "exec_ms": 296},
        {"event": "task_started", "attempt_id": "req_x:t2:2", "ts_ms": 1404},
        {"event": "task_completed", "attempt_id": "req_x:t2:2", "ts_ms": 1648, "committed": True, "exec_ms": 244},
        {"event": "task_started", "attempt_id": "req_x:t3:1", "ts_ms": 1655},
        {"event": "task_completed", "attempt_id": "req_x:t3:1", "ts_ms": 1899, "committed": True, "exec_ms": 244},
        {"event": "task_completed", "attempt_id": "req_x:t2:1", "ts_ms": 3000, "committed": False, "exec_ms": 1997},
    ]
    out = e5.analyse(doc, log, worker, True, 7, "SUCCEEDED", e5.FaultConfig())
    row = out["row"]
    assert row["mode"] == "speculation" and row["makespan"] == 0.9
    assert (row["backups"], row["backups_won"]) == (1, 1)
    assert (row["useful_ms"], row["wasted_ms"]) == (784, 1997)
    by = {}
    for s in out["samples"]:
        by.setdefault(s["metric"], []).append(s["ms"])
    assert sorted(by["pickup"]) == [2, 3, 3, 4]       # four dispatched attempts all started
    assert sorted(by["report"]) == [1, 2, 2]          # worker completion -> atsd records it
    assert by["handoff"] == [4]                        # t2's winning completion (1648) -> t3 dispatched (1652)


def test_e5_summary_reports_percentiles_and_paired_extra_work(tmp_path):
    rows = []
    for seed in range(1, 11):
        rows.append({"mode": "baseline", "seed": str(seed), "status": "SUCCEEDED", "makespan": str(2.2 if seed % 2 else 0.4),
                     "backups": "0", "backups_won": "0", "useful_ms": "7000", "wasted_ms": "0", "stragglers": "1"})
        rows.append({"mode": "speculation", "seed": str(seed), "status": "SUCCEEDED", "makespan": "0.8",
                     "backups": "1", "backups_won": "1", "useful_ms": "3400", "wasted_ms": "4000", "stragglers": "1"})
    latency = [{"mode": "baseline", "seed": "1", "metric": "pickup", "ms": str(v)} for v in (1, 2, 3, 40)]
    text = analyze.summary_e5(rows, latency)
    assert "## E5" in text and "| no speculation | 10 | 1.300 | 2.200 | 2.200 | 2.200 | 0 | 0 |" in text
    assert "| speculation (D56) | 10 | 0.800 |" in text
    assert "median +5.7 %" in text              # (3400 + 4000) / 7000 - 1
    assert "| pickup | 4 | 2.5 |" in text
    assert analyze.e5_tail(rows, tmp_path).exists()


# --------------------------------------------------------------------------------- E9

from bench import e9  # noqa: E402


def test_e9_datasets_load_with_reference_answers():
    sizes = {name: len(e9.load(name)) for name in e9.DATASETS}
    assert sizes == {"parallelqa": 113, "hotpotqa_comparison": 200, "movie_recommendation": 100}
    for name in e9.DATASETS:
        for item in e9.load(name):
            assert item["id"] and item["question"] and item["answer"]
    for item in e9.load("movie_recommendation"):
        assert item["answer"] in item["options"] and len(item["options"]) == 4


def test_e9_sample_covers_every_branch_count():
    picked = e9.sample(e9.load("parallelqa"), 8)
    assert sorted(int(p["branch"]) for p in picked) == [2, 2, 3, 3, 4, 4, 5, 5]
    assert e9.sample(e9.load("parallelqa"), 8) == picked  # deterministic


def test_e9_scoring_follows_llmcompiler():
    assert e9.correct("about 540 million", "522")          # within 10 %
    assert not e9.correct("600", "522")
    assert e9.correct("1.1", "1.125") and not e9.correct("n/a", "1.125")
    assert e9.correct("Yes.", "yes") and not e9.correct("no", "yes")
    assert e9.correct("The Mariana Trench", "Mariana Trench")
    assert not e9.correct("Robert Erskine Childers", "Robert Erskine Childers DSC")  # exact match only...
    assert e9.f1("Robert Erskine Childers", "Robert Erskine Childers DSC") > 0.8  # ...F1 gives partial credit
    opts = ["Forrest Gump", "Righteous Kill", "Rosetta", "In Cold Blood"]
    assert e9.correct("I would pick Forrest Gump", "Forrest Gump", opts)
    assert not e9.correct("Forrest Gump or Rosetta", "Forrest Gump", opts)  # names two options
    assert not e9.correct(None, "yes")


def test_e9_final_line_reads_the_last_marker():
    assert e9.final_line("Reasoning...\n**Final answer:** Forrest Gump.") == "Forrest Gump"
    assert e9.final_line("Final answer: 1\nmore\nFinal answer: 2") == "2"
    assert e9.final_line("no marker here") is None


def _fake_modes(calls):
    async def single(request):
        calls.append(("single", request))
        return {"status": "SUCCEEDED", "seconds": 1.0, "planner_s": 0.0, "tasks": 0, "text": "Final answer: yes",
                "tokens_in": 100, "tokens_out": 50, "llm_calls": 1, "retries": 0, "request_id": ""}

    def workflow(request, api):
        calls.append(("workflow", request))
        return {"status": "SUCCEEDED", "seconds": 3.0, "planner_s": 1.0, "tasks": 3, "text": "Final answer: no",
                "tokens_in": 1000, "tokens_out": 500, "llm_calls": 4, "retries": 1, "request_id": "req_x"}

    async def extract(text, question):
        return e9.final_line(text), "line", (0, 0, 0)

    return single, workflow, extract


def _yes_no_dataset(tmp_path, n):
    d = tmp_path / "datasets"
    d.mkdir()
    with (d / "hotpotqa_comparison.jsonl").open("w") as f:
        for i in range(n):
            f.write(json.dumps({"id": f"q{i}", "question": f"Is {i} even?", "answer": "yes", "branch": 2}) + "\n")
    return d


def test_e9_loop_scores_both_modes_alternates_order_and_resumes(tmp_path):
    d, out, calls = _yes_no_dataset(tmp_path, 3), tmp_path / "e9.csv", []
    single, workflow, extract = _fake_modes(calls)
    rows = e9.run(["hotpotqa_comparison"], 3, out, "http://x", 10**9, 0, single=single, workflow=workflow,
                  extract=extract, datasets_dir=d)
    assert len(rows) == 6
    by = {(r["id"], r["mode"]): r for r in rows}
    assert by[("q0", "single")]["correct"] == 1 and by[("q0", "workflow")]["correct"] == 0
    assert by[("q0", "workflow")]["tokens_in"] == 1000 and by[("q0", "workflow")]["planner_s"] == 1.0
    assert [m for m, _ in calls] == ["workflow", "single", "single", "workflow", "workflow", "single"]
    assert all(e9.ANSWER_FORMAT in req for _, req in calls)  # same instruction for both modes
    with out.open() as f:
        assert len(list(csv.DictReader(f))) == 6

    calls.clear()  # a rerun skips everything that already succeeded
    rows = e9.run(["hotpotqa_comparison"], 3, out, "http://x", 10**9, 0, single=single, workflow=workflow,
                  extract=extract, datasets_dir=d)
    assert calls == [] and len(rows) == 6


def test_e9_prefers_the_workflows_structured_final_answer(tmp_path):
    d, out, calls = _yes_no_dataset(tmp_path, 1), tmp_path / "e9.csv", []
    single, workflow, extract = _fake_modes(calls)

    def structured(request, api):  # the text says "no", the structured field says "yes" (D58)
        return {**workflow(request, api), "final_answer": "Yes"}

    extracted = []

    async def tracking_extract(text, question):
        extracted.append(text)
        return await extract(text, question)

    rows = e9.run(["hotpotqa_comparison"], 1, out, "http://x", 10**9, 0, single=single, workflow=structured,
                  extract=tracking_extract, datasets_dir=d)
    by = {r["mode"]: r for r in rows}
    assert by["workflow"]["extracted"] == "field" and by["workflow"]["correct"] == 1
    assert by["single"]["extracted"] == "line" and extracted == ["Final answer: yes"]  # only single was parsed


def test_e9_loop_stops_at_the_token_budget_and_continues_later(tmp_path):
    d, out, calls = _yes_no_dataset(tmp_path, 4), tmp_path / "e9.csv", []
    single, workflow, extract = _fake_modes(calls)
    rows = e9.run(["hotpotqa_comparison"], 4, out, "http://x", 1500, 0, single=single, workflow=workflow,
                  extract=extract, datasets_dir=d)
    assert len(rows) == 2  # one question (1,650 tokens) reaches the 1,500 budget
    rows = e9.run(["hotpotqa_comparison"], 4, out, "http://x", 10**9, 0, single=single, workflow=workflow,
                  extract=extract, datasets_dir=d)
    assert len(rows) == 8


def test_e9_summary_and_chart(tmp_path):
    rows = []
    for i in range(4):
        for mode, ok in (("workflow", i % 2), ("single", 1)):
            rows.append({"dataset": "parallelqa", "id": f"p{i}", "branch": str(2 + i % 2), "mode": mode,
                         "status": "SUCCEEDED", "correct": str(ok), "f1": "1.0" if ok else "0.0", "latency_s": "3.0",
                         "planner_s": "1.0", "tasks": "3", "llm_calls": "4", "tokens_in": "900", "tokens_out": "100",
                         "cost_usd": "0.0002", "retries": "0", "extracted": "line", "prediction": "x", "gold": "y",
                         "request_id": "", "error": ""})
    text = analyze.summary_e9(rows)
    assert "| ParallelQA | workflow (plan + parallel tasks) | 4 | 50% |" in text
    assert "| ParallelQA | single LLM call | 4 | 100% |" in text
    assert "By number of entities" in text and "| ParallelQA | 3 | 100% (2) |" in text
    assert analyze.e9_accuracy(rows, tmp_path).exists()


# --------------------------------------------------------------------------------- latency breakdown

from bench import breakdown  # noqa: E402


def _ev(ts, event, task=None, attempt=0):
    e = {"ts_ms": ts, "event": event, "request": "req_b"}
    if task:
        e["task"] = task
    if attempt:
        e["attempt"] = attempt
    return e


def test_breakdown_walks_the_critical_path_and_splits_each_task():
    # t1 and t2 start together; t2 fails once (429, 3 s pause), is retried, finishes last; t3 needs both.
    events = [
        _ev(1000, "request_accepted"),
        _ev(1000, "dispatched", "t1", 1), _ev(1000, "dispatched", "t2", 1),
        _ev(1010, "started", "t1", 1), _ev(1010, "started", "t2", 1),
        _ev(2000, "completed", "t1", 1),
        _ev(2500, "capability_paused", "t2", 1), _ev(2500, "retry_scheduled", "t2", 1),
        _ev(5500, "dispatched", "t2", 2), _ev(5520, "started", "t2", 2), _ev(7520, "completed", "t2", 2),
        _ev(9000, "dispatched", "t3", 1),  # slot freed late
        _ev(9030, "started", "t3", 1), _ev(12030, "completed", "t3", 1),
    ]
    deps = {"t1": [], "t2": [], "t3": ["t1", "t2"]}
    b = breakdown.breakdown(events, deps)
    assert b["path"] == ["t2", "t3"]                      # t1 finished first: not on the path
    assert b["retries"] == 4.5                            # t2: first dispatch 1.0 s -> retry dispatch 5.5 s
    assert b["slot_wait"] == 1.48                         # t3: ready 7.52 s -> dispatched 9.0 s
    assert round(b["pickup"], 3) == 0.05                  # 0.02 + 0.03
    assert b["llm_call"] == 5.0                           # t2 attempt 2: 2.0 s, t3: 3.0 s
    assert b["atsd_s"] == 11.03 and b["pauses_429"] == 1

    r = breakdown.row("req_b", events, deps, planner_s=3.0, latency_s=15.0)
    assert r["submit_poll_s"] == 0.97                     # 15 - 3 - (4.5 + 1.48 + 0.05 + 5.0)
    text = breakdown.report([r])
    assert "| retries + backoff | 4.5 | 30% |" in text and "| planner | 3.0 | 20% |" in text


def test_breakdown_knows_spliced_tasks_and_joins():
    tasks = [{"id": "t1", "depends_on": []}, {"id": "t2", "depends_on": ["t1"]},
             {"id": "t1.1", "depends_on": None}, {"id": "t1.2", "depends_on": None}, {"id": "t1.join", "depends_on": None}]
    deps = breakdown.dependencies(tasks)
    assert deps == {"t1": [], "t2": ["t1.join"], "t1.1": ["t1"], "t1.2": ["t1"], "t1.join": ["t1.1", "t1.2"]}
    events = [_ev(0, "request_accepted"), _ev(0, "dispatched", "t1", 1), _ev(1000, "completed", "t1", 1),
              _ev(1000, "dispatched", "t1.1", 1), _ev(1000, "dispatched", "t1.2", 1),
              _ev(2000, "completed", "t1.1", 1), _ev(4000, "completed", "t1.2", 1),
              _ev(4000, "join_completed", "t1.join"), _ev(4001, "dispatched", "t2", 1), _ev(6001, "completed", "t2", 1)]
    b = breakdown.breakdown(events, deps)
    assert b["path"] == ["t1", "t1.2", "t1.join", "t2"]
    assert b["llm_call"] == 6.0 and b["slot_wait"] == 0.001


def test_breakdown_reads_the_runs_directory(tmp_path):
    d = tmp_path / "runs" / "20261103T000000Z"
    d.mkdir(parents=True)
    (d / "events.jsonl").write_text(json.dumps(_ev(1, "request_accepted")) + "\nnot json\n"
                                    + json.dumps({"ts_ms": 2, "event": "x", "request": "other"}) + "\n")
    by = breakdown.read_events(tmp_path / "runs")
    assert set(by) == {"req_b", "other"}
