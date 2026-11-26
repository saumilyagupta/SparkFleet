"""Evidence view API tests. No SSH: facts / job history / the report generator are fakes. Fake IPs only."""

from __future__ import annotations

import json
import os
import sys
import time
import types

import pytest

pytest.importorskip("httpx")
from fastapi.testclient import TestClient  # noqa: E402

from clusterctl import cluster_ops, facts  # noqa: E402
from clusterctl import inventory as inv  # noqa: E402
from clusterctl.dashboard.actions import default_actions  # noqa: E402
from clusterctl.dashboard.app import create_app  # noqa: E402
from clusterctl.paths import ProjectPaths  # noqa: E402

TOKEN = "test-token-0123456789abcdef"
AUTH = {"X-Token": TOKEN}


def _write_json(path, data: dict, mtime: float) -> None:
    path.write_text(json.dumps(data), encoding="utf-8")
    os.utime(path, (mtime, mtime))


@pytest.fixture
def paths(tmp_path, monkeypatch, sample_inventory) -> ProjectPaths:
    monkeypatch.setenv("CLUSTERCTL_HOME", str(tmp_path))
    p = ProjectPaths(tmp_path)
    inv.save(sample_inventory, p.inventory)
    res = p.results_dir
    res.mkdir()
    now = time.time()
    for i in range(12):                                  # 12 runs: list is trimmed to 10
        _write_json(res / f"wordcount-2026010{i % 10}-{i:06d}.json",
                    {"tag": f"run-{i}", "summary": {"total_words": 100 + i},
                     "executors": [{"ip": "10.0.0.2", "cores": 4}]}, now - 1000 + i)
    _write_json(res / "demo-old.json", {"stamp": "old", "identical": True}, now - 500)
    _write_json(res / "demo-new.json", {"stamp": "new", "identical": True,
                                        "victim": {"name": "w1", "ip": "10.0.0.2"}}, now - 100)
    (res / "demo-new.md").write_text("# demo report\n", encoding="utf-8")
    (res / "demo-new.log").write_text("Lost executor 0 on 10.0.0.2\n", encoding="utf-8")
    (res / "wordcount-x.tsv").write_text("the\t3\n", encoding="utf-8")
    (res / "evidence-20260101-000000.html").write_text("<h1>evidence</h1>", encoding="utf-8")
    (res / ".vault-pass").write_text("never-served", encoding="utf-8")
    (tmp_path / "secrets.vault").write_text("never-served", encoding="utf-8")
    (tmp_path / "evidence-x.html").write_text("outside results", encoding="utf-8")
    return p


@pytest.fixture
def client(paths) -> TestClient:
    return TestClient(create_app(paths, TOKEN, actions=default_actions()))


# ------------------------------------------------------------------ auth

@pytest.mark.parametrize("url", ["/api/evidence/facts", "/api/evidence/results", "/api/evidence/jobs",
                                 "/api/evidence/report/evidence-20260101-000000.html",
                                 "/api/evidence/file/demo-new.md"])
def test_evidence_routes_require_token(client, url):
    assert client.get(url).status_code == 401
    assert client.get(url, headers={"X-Token": "wrong-token-value-xxxx"}).status_code == 401


def test_json_routes_reject_query_token(client):
    assert client.get("/api/evidence/results", params={"token": TOKEN}).status_code == 401


# ------------------------------------------------------------------ data routes

def test_results_newest_first_and_trimmed(client):
    data = client.get("/api/evidence/results", headers=AUTH).json()
    assert len(data["wordcount"]) == 10
    assert data["wordcount"][0]["tag"] == "run-11" and data["wordcount"][-1]["tag"] == "run-2"
    assert [d["stamp"] for d in data["demos"]] == ["new", "old"]
    assert [r["name"] for r in data["reports"]] == ["evidence-20260101-000000.html"]


def test_facts_route_uses_collect_facts(client, monkeypatch):
    nodes = [{"name": "m1", "ip": "10.0.0.1", "role": "master", "virt": "none",
              "is_physical": True, "reachable": True},
             {"name": "w1", "ip": "10.0.0.2", "role": "worker", "reachable": False, "error": "timeout"}]
    monkeypatch.setattr(facts, "collect_facts", lambda ctx: nodes)
    r = client.get("/api/evidence/facts", headers=AUTH)
    assert r.status_code == 200 and r.json() == {"nodes": nodes}


def test_jobs_route_ok_and_502(client, monkeypatch):
    history = {"master": "spark://10.0.0.1:7077",
               "apps": [{"id": "app-1", "name": "wc", "cores": 8, "state": "FINISHED"}]}
    monkeypatch.setattr(cluster_ops, "job_history", lambda ctx: history)
    assert client.get("/api/evidence/jobs", headers=AUTH).json() == history

    def boom(ctx):
        raise RuntimeError("master unreachable")
    monkeypatch.setattr(cluster_ops, "job_history", boom)
    r = client.get("/api/evidence/jobs", headers=AUTH)
    assert r.status_code == 502 and "master unreachable" in r.json()["error"]


# ------------------------------------------------------------------ file routes

def test_report_served_as_html_with_query_token(client):
    r = client.get("/api/evidence/report/evidence-20260101-000000.html", params={"token": TOKEN})
    assert r.status_code == 200 and "<h1>evidence</h1>" in r.text
    assert r.headers["content-type"].startswith("text/html")
    assert "script-src" not in r.headers["content-security-policy"]


@pytest.mark.parametrize("name", ["demo-new.md", "demo-new.log", "wordcount-x.tsv"])
def test_raw_files_served_as_text(client, name):
    r = client.get(f"/api/evidence/file/{name}", headers=AUTH)
    assert r.status_code == 200 and r.headers["content-type"].startswith("text/plain")
    assert client.get(f"/api/evidence/file/{name}", params={"token": TOKEN}).status_code == 200


@pytest.mark.parametrize("name", ["../x", "..%2Fx", "..%2F..%2Fsecrets.vault", "secrets.vault", ".vault-pass",
                                  "evidence-x.html", "demo-new.json", "evidence-missing.html",
                                  "..%5Cevidence-x.html", "inventory.yaml"])
def test_report_and_file_routes_reject_bad_names(client, name):
    for route in ("report", "file"):
        r = client.get(f"/api/evidence/{route}/{name}", params={"token": TOKEN})
        assert r.status_code == 404, (route, name, r.status_code)
        assert "never-served" not in r.text and "outside results" not in r.text


def test_file_route_does_not_serve_report_and_vice_versa(client):
    assert client.get("/api/evidence/file/evidence-20260101-000000.html", headers=AUTH).status_code == 404
    assert client.get("/api/evidence/report/demo-new.md", headers=AUTH).status_code == 404


# ------------------------------------------------------------------ evidence action

def test_evidence_action_registered():
    assert "evidence" in default_actions()
    assert default_actions()["evidence"].needs_vault is False


def test_evidence_action_runs_fake_generator(client, paths, monkeypatch):
    def fake_generate(ctx):
        out = ctx.paths.results_dir / "evidence-20260102-000000.html"
        out.write_text("<p>fake report</p>", encoding="utf-8")
        return out

    fake = types.ModuleType("clusterctl.evidence")
    fake.generate_report = fake_generate
    monkeypatch.setitem(sys.modules, "clusterctl.evidence", fake)

    r = client.post("/api/actions/evidence", json={}, headers=AUTH)
    assert r.status_code == 202, r.text
    job_id = r.json()["job_id"]
    deadline = time.time() + 5
    while (job := client.get(f"/api/jobs/{job_id}", headers=AUTH).json())["state"] == "running":
        assert time.time() < deadline, "job did not finish"
        time.sleep(0.05)
    assert job["state"] == "done", job["error"]
    assert any("evidence-20260102-000000.html" in e["msg"] for e in job["history"])
    names = [x["name"] for x in client.get("/api/evidence/results", headers=AUTH).json()["reports"]]
    assert "evidence-20260102-000000.html" in names
    served = client.get("/api/evidence/report/evidence-20260102-000000.html", params={"token": TOKEN})
    assert served.status_code == 200 and "fake report" in served.text


def test_evidence_action_rejects_params(client):
    assert client.post("/api/actions/evidence", json={"x": 1}, headers=AUTH).status_code == 422
