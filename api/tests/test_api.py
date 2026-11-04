import json

import groq
import pytest
import redis
from fastapi.testclient import TestClient

from api.app import create_app
from planner import keys
from planner.fake import FakePlanner, load_fixture_plan
from planner.messages import STREAM_FIELD, result_key
from planner.schema import PlanFile


@pytest.fixture
def r(redis_url):
    return redis.Redis.from_url(redis_url, decode_responses=True)


def client(redis_url, planner_factory=lambda: FakePlanner(), artifacts_dir=None, direct_executor=None, fast_path=False):
    return TestClient(create_app(redis_url=redis_url, planner_factory=planner_factory, artifacts_dir=artifacts_dir,
                                 direct_executor_factory=direct_executor, fast_path=fast_path))


def queued(r):
    return [PlanFile.model_validate_json(f[STREAM_FIELD]) for _, f in r.xrange(keys.REQUESTS_STREAM)]


def test_healthz(redis_url):
    with client(redis_url) as c:
        assert c.get("/healthz").json() == {"ok": True}


def test_post_plan_enqueues_plan_file(redis_url, r):
    with client(redis_url) as c:
        resp = c.post("/plans", json=load_fixture_plan("company_research"))
    assert resp.status_code == 202
    body = resp.json()
    rid = body["request_id"]
    assert rid.startswith("req_") and body["status"] == "SUBMITTED" and body["warnings"] == []
    [pf] = queued(r)
    assert pf.request_id == rid and len(pf.plan.tasks) == 6
    assert PlanFile.model_validate_json(r.get(keys.plan_key(rid))) == pf


def test_post_plan_reports_warnings(redis_url):
    with client(redis_url) as c:
        body = c.post("/plans", json=load_fixture_plan("fork")).json()
    assert [w["code"] for w in body["warnings"]] == ["MULTIPLE_SINKS"]


@pytest.mark.parametrize("mutate, code", [
    (lambda p: p["tasks"][0].update(depends_on=["t6"]), "CYCLE"),
    (lambda p: p.update(request_id="req_from_client"), "SCHEMA"),  # the runtime assigns ids
    (lambda p: p["tasks"][0].pop("instruction"), "SCHEMA"),
])
def test_post_plan_rejects_invalid_plans(redis_url, r, mutate, code):
    plan = load_fixture_plan("company_research")
    mutate(plan)
    with client(redis_url) as c:
        resp = c.post("/plans", json=plan)
    assert resp.status_code == 422
    assert code in [i["code"] for i in resp.json()["issues"]]
    assert queued(r) == []


def test_request_id_collision_is_refused_and_not_queued(redis_url, r, monkeypatch):
    import api.app as app_module

    fixed = PlanFile(request_id="req_taken", created_at="2026-10-13T00:00:00Z",
                     plan=load_fixture_plan("chain"))
    monkeypatch.setattr(app_module, "make_plan_file", lambda plan: fixed)
    r.set(keys.plan_key("req_taken"), "existing request")
    with client(redis_url) as c:
        resp = c.post("/plans", json=load_fixture_plan("chain"))
    assert resp.status_code == 503
    assert queued(r) == []                                   # nothing handed to the scheduler
    assert r.get(keys.plan_key("req_taken")) == "existing request"  # nothing overwritten


def test_post_request_plans_and_enqueues(redis_url, r):
    with client(redis_url) as c:
        resp = c.post("/requests", json={"text": "Research Apple and NVIDIA and compare them"})
    assert resp.status_code == 202
    assert resp.json()["planner_attempts"] == 1
    [pf] = queued(r)
    assert pf.request_id == resp.json()["request_id"]


def test_post_request_rejected_after_repair(redis_url, r):
    bad = load_fixture_plan("company_research")
    bad["tasks"][0]["depends_on"] = ["t42"]
    with client(redis_url, lambda: FakePlanner(script=[bad, bad])) as c:
        resp = c.post("/requests", json={"text": "x"})
    assert resp.status_code == 422
    assert [i["code"] for i in resp.json()["issues"]] == ["UNKNOWN_DEPENDENCY"]
    assert queued(r) == []


def test_post_request_provider_error_is_502(redis_url):
    class Down(FakePlanner):
        def propose(self, request, feedback=None):
            raise groq.GroqError("no api key")

    with client(redis_url, Down) as c:
        resp = c.post("/requests", json={"text": "x"})
    assert resp.status_code == 502 and "no api key" in resp.json()["detail"]


def test_post_request_validates_body(redis_url):
    with client(redis_url) as c:
        assert c.post("/requests", json={"text": ""}).status_code == 422
        assert c.post("/requests", json={}).status_code == 422


def test_get_unknown_request_is_404(redis_url):
    with client(redis_url) as c:
        assert c.get("/requests/req_nope").status_code == 404
        assert c.get("/requests/a:b").status_code == 404


def test_get_submitted_request_before_atsd(redis_url):
    with client(redis_url) as c:
        rid = c.post("/plans", json=load_fixture_plan("company_research")).json()["request_id"]
        body = c.get(f"/requests/{rid}").json()
    assert body["status"] == "SUBMITTED" and body["sink"] == "t6" and body["output"] is None
    assert [(t["id"], t["state"]) for t in body["tasks"]][:2] == [("t1", "WAITING"), ("t2", "WAITING")]


def test_get_running_and_finished_request(redis_url, r):
    with client(redis_url) as c:
        rid = c.post("/plans", json=load_fixture_plan("expand_canned")).json()["request_id"]
        # What atsd writes (docs/messages.md, "Request intake and status").
        r.hset(keys.request_key(rid), mapping={
            "status": "RUNNING", "sink": "t4", "updated_ms": 1760000000000,
            "task:t1": json.dumps({"state": "RUNNING", "attempt": 2}),
            "task:t2.1": json.dumps({"state": "QUEUED", "attempt": 1}),  # spliced by expand
        })
        running = c.get(f"/requests/{rid}").json()
        r.hset(keys.request_key(rid), mapping={"status": "SUCCEEDED",
                                               "task:t4": json.dumps({"state": "COMPLETED", "attempt": 1})})
        r.set(result_key(rid, "t4"), json.dumps({"content": "final report"}))
        finished = c.get(f"/requests/{rid}").json()

    states = {t["id"]: (t["state"], t["attempt"]) for t in running["tasks"]}
    assert running["status"] == "RUNNING" and running["updated_ms"] == 1760000000000
    assert states["t1"] == ("RUNNING", 2) and states["t3"] == ("WAITING", 0) and states["t2.1"] == ("QUEUED", 1)
    assert running["output"] is None
    assert finished["status"] == "SUCCEEDED" and finished["output"] == {"content": "final report"}


def test_spliced_tasks_are_described_from_the_subplan(redis_url, r):
    def sub(i, deps=()):
        return {"id": f"s{i}", "type": "research", "title": f"Company {i}", "instruction": "x", "depends_on": list(deps)}

    with client(redis_url) as c:
        rid = c.post("/plans", json=load_fixture_plan("expand_canned")).json()["request_id"]
        # t2 is the expand task; 11 subtasks, s11 depends on s10 (so s10 is not a sink)
        r.set(result_key(rid, "t2"), json.dumps({"tasks": [sub(i) for i in range(1, 11)] + [sub(11, ["s10"])]}))
        r.hset(keys.request_key(rid), mapping={"status": "RUNNING", **{
            f"task:{tid}": json.dumps({"state": "WAITING", "attempt": 0})
            for tid in ["t2.join", "t2.10", "t2.2", "t2.1", "t2.11"]}})
        tasks = c.get(f"/requests/{rid}").json()["tasks"]

    spliced = [t for t in tasks if "." in t["id"]]
    assert [t["id"] for t in spliced] == ["t2.1", "t2.2", "t2.10", "t2.11", "t2.join"]  # numeric order
    assert (spliced[0]["type"], spliced[0]["title"]) == ("research", "Company 1")
    assert spliced[3]["depends_on"] == ["t2", "t2.10"]
    join = spliced[-1]
    assert join["type"] == "join" and "t2.10" not in join["depends_on"] and "t2.11" in join["depends_on"]


def test_plain_text_result_is_returned_not_a_500(redis_url, r):
    with client(redis_url) as c:
        rid = c.post("/plans", json=load_fixture_plan("chain")).json()["request_id"]
        r.hset(keys.request_key(rid), mapping={"status": "SUCCEEDED", "sink": "t3",
                                               "task:t3": json.dumps({"state": "COMPLETED", "attempt": 1})})
        r.set(result_key(rid, "t3"), "[sim] report t3 using t2")  # pre-JSON result value
        resp = c.get(f"/requests/{rid}")
    assert resp.status_code == 200 and resp.json()["output"] == "[sim] report t3 using t2"


# --- Day 4: final output assembly ---------------------------------------------------


def finish(r, rid, tasks, status="SUCCEEDED", sink="t3"):
    r.hset(keys.request_key(rid), mapping={"status": status, "sink": sink, **{
        f"task:{t}": json.dumps({"state": "COMPLETED", "attempt": 1}) for t in tasks}})


def test_spilled_final_output_is_read_from_its_artifact(redis_url, r, tmp_path):
    report = {"content": "# Report\n" + "y" * 70_000, "key_points": ["k"]}
    with client(redis_url, artifacts_dir=tmp_path) as c:
        rid = c.post("/plans", json=load_fixture_plan("chain")).json()["request_id"]
        (tmp_path / rid).mkdir()
        (tmp_path / rid / "t3.1.json").write_text(json.dumps(report))
        r.set(result_key(rid, "t3"), json.dumps({"artifact": f"{rid}/t3.1.json", "bytes": 70_020}))
        finish(r, rid, ["t1", "t2", "t3"])
        assert c.get(f"/requests/{rid}").json()["output"] == report


def test_missing_artifact_is_reported_not_a_500(redis_url, r, tmp_path):
    with client(redis_url, artifacts_dir=tmp_path) as c:
        rid = c.post("/plans", json=load_fixture_plan("chain")).json()["request_id"]
        r.set(result_key(rid, "t3"), json.dumps({"artifact": f"{rid}/t3.1.json", "bytes": 5}))
        finish(r, rid, ["t3"])
        resp = c.get(f"/requests/{rid}")
    assert resp.status_code == 200 and "cannot read artifact" in resp.json()["output"]["error"]


def test_include_outputs_and_usage(redis_url, r):
    with client(redis_url) as c:
        rid = c.post("/plans", json=load_fixture_plan("chain")).json()["request_id"]
        for t in ("t1", "t2"):
            r.set(result_key(rid, t), json.dumps({"content": f"out {t}", "key_points": []}))
        r.hset(keys.request_key(rid), mapping={"status": "RUNNING", "sink": "t3",
               "task:t1": json.dumps({"state": "COMPLETED", "attempt": 1}),
               "task:t2": json.dumps({"state": "COMPLETED", "attempt": 2})})
        r.hset(keys.usage_key(rid), mapping={"tokens_in": 300, "tokens_out": 50, "llm_calls": 3,
                                             "t1:tokens_in": 100, "t1:tokens_out": 20, "t1:llm_calls": 1})
        plain = c.get(f"/requests/{rid}").json()
        full = c.get(f"/requests/{rid}?include_outputs=true").json()
    assert "outputs" not in plain and plain["output"] is None
    assert full["outputs"] == {"t1": {"content": "out t1", "key_points": []}, "t2": {"content": "out t2", "key_points": []}}
    assert full["usage"] == {"tokens_in": 300, "tokens_out": 50, "llm_calls": 3,
                             "by_task": {"t1": {"tokens_in": 100, "tokens_out": 20, "llm_calls": 1}}}


def test_report_is_409_until_success_then_markdown(redis_url, r):
    with client(redis_url) as c:
        rid = c.post("/plans", json=load_fixture_plan("chain")).json()["request_id"]
        assert c.get(f"/requests/{rid}/report").status_code == 409
        r.set(result_key(rid, "t3"), json.dumps({"content": "Apple and NVIDIA compared.", "key_points": ["NVIDIA grew faster"]}))
        finish(r, rid, ["t1", "t2", "t3"])
        resp = c.get(f"/requests/{rid}/report")
        assert c.get("/requests/req_nope/report").status_code == 404
    assert resp.status_code == 200 and resp.headers["content-type"].startswith("text/markdown")
    assert resp.text.startswith("# A -> B -> C") and "Apple and NVIDIA compared." in resp.text
    assert "- NVIDIA grew faster" in resp.text


def test_several_final_tasks_are_all_returned(redis_url, r):
    with client(redis_url) as c:
        rid = c.post("/plans", json=load_fixture_plan("two_components")).json()["request_id"]
        r.set(result_key(rid, "t2"), json.dumps({"content": "B done", "key_points": []}))
        r.set(result_key(rid, "t4"), json.dumps({"content": "D done", "key_points": []}))
        finish(r, rid, ["t1", "t2", "t3", "t4"], sink="")
        body = c.get(f"/requests/{rid}").json()
        report = c.get(f"/requests/{rid}/report").text
    assert body["output"] == {"t2": {"content": "B done", "key_points": []}, "t4": {"content": "D done", "key_points": []}}
    assert "## Task t2\n\nB done" in report and "## Task t4\n\nD done" in report  # one section per final task, by title


def test_api_survives_a_redis_restart():
    """Regression (Day 4 gate run): a request after Redis restarted used a stale pooled
    connection and returned 500. Uses its own redis-server so it can restart it."""
    import shutil
    import socket
    import subprocess
    import time

    exe = shutil.which("redis-server")
    if not exe:
        pytest.skip("needs redis-server")
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]

    def start():
        proc = subprocess.Popen([exe, "--port", str(port), "--save", "", "--appendonly", "no"],
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(100):
            try:
                redis.Redis(port=port).ping()
                return proc
            except redis.ConnectionError:
                time.sleep(0.05)
        raise RuntimeError("redis-server did not start")

    proc = start()
    try:
        with client(f"redis://127.0.0.1:{port}/0") as c:
            assert c.post("/plans", json=load_fixture_plan("chain")).status_code == 202
            proc.terminate()
            proc.wait()
            proc = start()
            assert c.post("/plans", json=load_fixture_plan("chain")).status_code == 202
    finally:
        proc.terminate()
        proc.wait()


# --- fast path (D58) -----------------------------------------------------------------------------


class FakeDirect:
    """Stands in for the direct LLM executor: answers, or fails `fail` times with a 429 first."""

    def __init__(self, fail=0, permanent=False):
        self.fail, self.permanent, self.calls = fail, permanent, 0

    async def run(self, ctx):
        from worker.executors.base import ExecutionResult, PermanentError, RetryableError

        self.calls += 1
        if self.permanent:
            raise PermanentError("bad request")
        if self.calls <= self.fail:
            raise RetryableError("429", retry_after_ms=1)
        return ExecutionResult(output={"content": f"answer to: {ctx.message.instruction}", "key_points": ["k"],
                                       "final_answer": "Paris"}, tokens_in=10, tokens_out=5)


def wait_done(c, rid, timeout=10):
    import time

    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        body = c.get(f"/requests/{rid}").json()
        if body["status"] in ("SUCCEEDED", "FAILED"):
            return body
        time.sleep(0.02)
    raise AssertionError("request did not finish")


@pytest.mark.parametrize("text, simple", [
    ("What is the capital of France?", True),
    ("Write a short explanation of what a DAG is.", True),
    ("Research AMD and Intel", False),
    ("Compare Apple, NVIDIA", False),
    ("Who is older, Federer or Nadal?", False),
    ("List the top 5 AI chip companies", False),
    (" ".join(["word"] * 31), False),
])
def test_simple_requests_are_recognised(text, simple):
    from api.direct import is_simple

    assert is_simple(text) is simple


def test_simple_request_is_answered_directly_without_planner_or_atsd(redis_url, r):
    fake = FakeDirect()
    planner = FakePlanner()
    with client(redis_url, lambda: planner, direct_executor=lambda: fake, fast_path=True) as c:
        resp = c.post("/requests", json={"text": "What is the capital of France?"})
        assert resp.status_code == 202
        sub = resp.json()
        body = wait_done(c, sub["request_id"])
        report = c.get(f"/requests/{sub['request_id']}/report").text
    assert sub["mode"] == "direct" and sub["planner_attempts"] == 0 and [t["id"] for t in sub["plan"]["tasks"]] == ["t1"]
    assert planner.calls == [] and queued(r) == []  # no planner call, nothing for atsd
    assert body["status"] == "SUCCEEDED" and body["sink"] == "t1" and body["tasks"][0]["state"] == "COMPLETED"
    assert body["output"]["final_answer"] == "Paris" and body["usage"]["tokens_in"] == 10
    assert "**Answer:** Paris" in report


def test_auto_sends_multi_part_requests_to_the_workflow(redis_url, r):
    with client(redis_url, direct_executor=lambda: FakeDirect(), fast_path=True) as c:
        sub = c.post("/requests", json={"text": "Research Apple and NVIDIA and compare them"}).json()
        forced = c.post("/requests", json={"text": "What is a DAG?", "mode": "workflow"}).json()
    assert sub["mode"] == "workflow" and forced["mode"] == "workflow" and len(queued(r)) == 2


def test_mode_direct_works_even_with_the_fast_path_off(redis_url, r):
    with client(redis_url, direct_executor=lambda: FakeDirect(), fast_path=False) as c:
        auto = c.post("/requests", json={"text": "What is a DAG?"}).json()
        sub = c.post("/requests", json={"text": "Compare A and B", "mode": "direct"}).json()
        body = wait_done(c, sub["request_id"])
    assert auto["mode"] == "workflow" and sub["mode"] == "direct" and body["status"] == "SUCCEEDED"


def test_direct_answer_retries_rate_limits_then_fails_permanently(redis_url, monkeypatch):
    import api.direct as direct_module

    async def no_sleep(_):
        return None

    monkeypatch.setattr(direct_module.asyncio, "sleep", no_sleep)
    flaky = FakeDirect(fail=2)
    with client(redis_url, direct_executor=lambda: flaky) as c:
        ok = wait_done(c, c.post("/requests", json={"text": "x", "mode": "direct"}).json()["request_id"])
        again = wait_done(c, c.post("/requests", json={"text": "x", "mode": "direct"}).json()["request_id"])
    assert ok["status"] == "SUCCEEDED" and ok["tasks"][0]["attempt"] == 3  # two 429s, then the answer
    assert again["status"] == "SUCCEEDED" and again["tasks"][0]["attempt"] == 1 and flaky.calls == 4
    with client(redis_url, direct_executor=lambda: FakeDirect(permanent=True)) as c:
        failed = wait_done(c, c.post("/requests", json={"text": "x", "mode": "direct"}).json()["request_id"])
    assert failed["status"] == "FAILED" and "bad request" in failed["error"]
    with client(redis_url, direct_executor=lambda: FakeDirect(fail=99)) as c:
        limited = wait_done(c, c.post("/requests", json={"text": "x", "mode": "direct"}).json()["request_id"])
    assert limited["status"] == "FAILED" and limited["tasks"][0]["attempt"] == 4

