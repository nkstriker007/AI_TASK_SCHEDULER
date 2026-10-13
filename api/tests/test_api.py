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


def client(redis_url, planner_factory=lambda: FakePlanner()):
    return TestClient(create_app(redis_url=redis_url, planner_factory=planner_factory))


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
