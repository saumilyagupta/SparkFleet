"""Dashboard API tests. No SSH, no real cluster: actions are injected fakes. All IPs/passwords are fake."""

from __future__ import annotations

import json
import threading
import time

import pytest

pytest.importorskip("httpx")
from fastapi.testclient import TestClient  # noqa: E402

from clusterctl import inventory as inv  # noqa: E402
from clusterctl.cli import ENV_PASS, ENV_PASS_FILE  # noqa: E402
from clusterctl.dashboard.actions import Action, LimitParams, NoParams, resolve_data_path  # noqa: E402
from clusterctl.dashboard.app import create_app  # noqa: E402
from clusterctl.dashboard.state import stored_master_password  # noqa: E402
from clusterctl.paths import ProjectPaths  # noqa: E402
from clusterctl.vault import Vault  # noqa: E402

TOKEN = "test-token-0123456789abcdef"
MASTER_PW = "correct-horse-battery"
HOST_PW = "s3cret-host-pw-XYZ"
AUTH = {"X-Token": TOKEN}


@pytest.fixture
def paths(tmp_path, monkeypatch, sample_inventory) -> ProjectPaths:
    monkeypatch.delenv(ENV_PASS, raising=False)
    monkeypatch.delenv(ENV_PASS_FILE, raising=False)
    monkeypatch.setenv("CLUSTERCTL_HOME", str(tmp_path))
    p = ProjectPaths(tmp_path)
    inv.save(sample_inventory, p.inventory)
    Vault.create(p.vault, MASTER_PW)
    return p


class Gate:
    """A fake action that blocks until released, logging through the job's reporter."""

    def __init__(self) -> None:
        self.release = threading.Event()
        self.calls: list[tuple[str, ...]] = []

    def __call__(self, ctx, params) -> None:
        self.calls.append(tuple(params.limit or ()))
        ctx.reporter.log("m1", "fake step running", "info")
        assert self.release.wait(5), "test never released the gate"
        ctx.reporter.log("-", "fake step complete", "ok")


def _boom(ctx, _params) -> None:
    raise RuntimeError("simulated failure")


@pytest.fixture
def gate() -> Gate:
    return Gate()


@pytest.fixture
def client(paths, gate) -> TestClient:
    actions = {
        "verify": Action(gate, LimitParams),
        "boom": Action(_boom, NoParams),
        "locked_only": Action(gate, LimitParams, needs_vault=True),
    }
    return TestClient(create_app(paths, TOKEN, actions=actions))


def _unlock(client: TestClient) -> None:
    r = client.post("/api/vault/unlock", json={"password": MASTER_PW}, headers=AUTH)
    assert r.status_code == 200, r.text


def _wait_job(client: TestClient, job_id: str, timeout: float = 5.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        job = client.get(f"/api/jobs/{job_id}", headers=AUTH).json()
        if job["state"] != "running":
            return job
        time.sleep(0.05)
    raise AssertionError("job did not finish")


# ------------------------------------------------------------------ auth

def test_api_requires_token(client):
    assert client.get("/api/inventory").status_code == 401
    assert client.get("/api/inventory", headers={"X-Token": "wrong-token-value-xxxx"}).status_code == 401
    assert client.get("/api/inventory", params={"token": TOKEN}).status_code == 401  # query only for SSE
    r = client.get("/api/vault")
    assert r.status_code == 401 and r.json() == {"error": "missing or invalid token"}
    assert client.get("/api/inventory", headers=AUTH).status_code == 200


def test_static_page_loads_without_token_and_sets_security_headers(client):
    r = client.get("/")
    assert r.status_code == 200 and "clusterctl" in r.text
    assert "default-src 'self'" in r.headers["content-security-policy"]
    assert r.headers["referrer-policy"] == "no-referrer"
    assert client.get("/static/app.js").status_code == 200


def test_stream_requires_token(client):
    assert client.get("/api/jobs/abc/stream").status_code == 401
    assert client.get("/api/jobs/abc/stream", params={"token": TOKEN}).status_code == 404


def test_untrusted_host_header_rejected(client):
    assert client.get("/api/vault", headers={**AUTH, "Host": "evil.example"}).status_code == 400


def test_create_app_rejects_short_token(paths):
    with pytest.raises(ValueError):
        create_app(paths, "short")


# ------------------------------------------------------------------ vault

def test_vault_unlock_wrong_password(client):
    r = client.post("/api/vault/unlock", json={"password": "not-the-password"}, headers=AUTH)
    assert r.status_code in (400, 401)
    assert "wrong master password" in r.json()["error"]
    assert client.get("/api/vault", headers=AUTH).json()["unlocked"] is False


def test_vault_unlock_and_lock(client):
    _unlock(client)
    info = client.get("/api/vault", headers=AUTH).json()
    assert info == {"exists": True, "unlocked": True, "names": []}
    assert client.post("/api/vault/lock", headers=AUTH).json()["unlocked"] is False


def test_vault_create_when_missing(tmp_path, monkeypatch):
    monkeypatch.delenv(ENV_PASS, raising=False)
    c = TestClient(create_app(ProjectPaths(tmp_path), TOKEN, actions={}))
    assert c.post("/api/vault/unlock", json={"password": MASTER_PW}, headers=AUTH).status_code == 404
    r = c.post("/api/vault/unlock", json={"password": MASTER_PW, "create": True}, headers=AUTH)
    assert r.status_code == 200 and r.json()["unlocked"] is True
    assert (tmp_path / "secrets.vault").exists()


def test_auto_unlock_from_env(paths, monkeypatch):
    monkeypatch.setenv(ENV_PASS, MASTER_PW)
    c = TestClient(create_app(paths, TOKEN, actions={}, auto_unlock=True))
    assert c.get("/api/vault", headers=AUTH).json()["unlocked"] is True


def test_stored_master_password_reads_file_never_prompts(paths, monkeypatch):
    assert stored_master_password(paths) is None
    pass_file = paths.root / "pw.txt"
    pass_file.write_text(MASTER_PW + "\n", encoding="utf-8")
    monkeypatch.setenv(ENV_PASS_FILE, str(pass_file))
    assert stored_master_password(paths) == MASTER_PW


# ------------------------------------------------------------------ hosts

def test_get_inventory(client):
    data = client.get("/api/inventory", headers=AUTH).json()
    assert data["exists"] is True
    assert [h["name"] for h in data["hosts"]] == ["m1", "w1", "w2"]
    assert data["hosts"][0]["ip"] == "10.0.0.1"
    assert all(h["has_password"] is None for h in data["hosts"])   # locked: unknown


def test_add_host_persists_and_password_is_write_only(client, paths):
    _unlock(client)
    body = {"name": "w3", "ip": "10.0.0.4", "user": "ubuntu", "role": "worker", "password": HOST_PW}
    r = client.post("/api/hosts", json=body, headers=AUTH)
    assert r.status_code == 201, r.text
    assert r.json()["password_stored"] is True

    assert inv.load(paths.inventory).get("w3").ip == "10.0.0.4"
    assert HOST_PW not in paths.inventory.read_text(encoding="utf-8")
    assert Vault.open(paths.vault, MASTER_PW).get("w3") == HOST_PW

    responses = [r, client.get("/api/inventory", headers=AUTH), client.get("/api/vault", headers=AUTH)]
    for resp in responses:
        assert HOST_PW not in resp.text
    hosts = {h["name"]: h for h in responses[1].json()["hosts"]}
    assert hosts["w3"]["has_password"] is True and hosts["w1"]["has_password"] is False


def test_add_host_with_password_while_locked_is_409(client, paths):
    body = {"name": "w3", "ip": "10.0.0.4", "user": "ubuntu", "password": HOST_PW}
    r = client.post("/api/hosts", json=body, headers=AUTH)
    assert r.status_code == 409
    with pytest.raises(inv.InventoryError):
        inv.load(paths.inventory).get("w3")


def test_add_host_without_password_while_locked(client, paths):
    r = client.post("/api/hosts", json={"name": "w3", "ip": "10.0.0.4", "user": "ubuntu"}, headers=AUTH)
    assert r.status_code == 201 and r.json()["password_stored"] is False


@pytest.mark.parametrize("bad", [
    {"name": "W3", "ip": "10.0.0.4", "user": "ubuntu"},
    {"name": "w3", "ip": "10.0.0.999", "user": "ubuntu"},
    {"name": "w3", "ip": "10.0.0.2", "user": "ubuntu"},          # duplicate IP of w1
    {"name": "w3", "ip": "10.0.0.4", "user": "ubuntu", "role": "master"},  # second master
])
def test_add_host_validation(client, bad):
    r = client.post("/api/hosts", json=bad, headers=AUTH)
    assert r.status_code == 400 and r.json()["error"]


def test_validation_errors_never_echo_password(client):
    r = client.post("/api/hosts", json={"name": "w3", "ip": "10.0.0.4", "user": 5,
                                        "password": HOST_PW, "bogus": HOST_PW}, headers=AUTH)
    assert r.status_code == 422
    assert HOST_PW not in r.text


def test_delete_host(client, paths):
    assert client.delete("/api/hosts/w2", headers=AUTH).status_code == 200
    assert [h.name for h in inv.load(paths.inventory).hosts] == ["m1", "w1"]
    assert client.delete("/api/hosts/nope", headers=AUTH).status_code == 404


def test_set_password(client, paths):
    assert client.post("/api/hosts/w1/password", json={"password": HOST_PW}, headers=AUTH).status_code == 409
    _unlock(client)
    r = client.post("/api/hosts/w1/password", json={"password": HOST_PW}, headers=AUTH)
    assert r.status_code == 200 and HOST_PW not in r.text
    assert Vault.open(paths.vault, MASTER_PW).get("w1") == HOST_PW
    assert client.post("/api/hosts/nope/password", json={"password": "x"}, headers=AUTH).status_code == 404


def test_import_systems_upload(client, paths):
    _unlock(client)
    systems = {"systems": [
        {"system": "Node A", "ip_address": "10.0.0.21", "username": "alice", "password": HOST_PW},
        {"system": "Node B", "ip_address": "10.0.0.22", "username": "bob", "password": "other-pw-1"},
    ]}
    files = {"file": ("systems.json", json.dumps(systems).encode(), "application/json")}
    r = client.post("/api/import-systems", files=files, data={"master": "10.0.0.22", "jump": "bastion"},
                    headers=AUTH)
    assert r.status_code == 200, r.text
    assert r.json()["master"] == "node-b" and r.json()["passwords_stored"] == 2
    assert HOST_PW not in r.text
    loaded = inv.load(paths.inventory)
    assert loaded.cluster.jump_host == "bastion"
    assert Vault.open(paths.vault, MASTER_PW).get("node-a") == HOST_PW
    assert not any(p.name == "systems.json" for p in paths.root.iterdir())


def test_import_systems_rejects_bad_json_and_locked_vault(client):
    bad = {"file": ("systems.json", b"{not json", "application/json")}
    assert client.post("/api/import-systems", files=bad, headers=AUTH).status_code == 400
    ok = {"systems": [{"system": "A", "ip_address": "10.0.0.31", "username": "u", "password": "p"}]}
    files = {"file": ("systems.json", json.dumps(ok).encode(), "application/json")}
    assert client.post("/api/import-systems", files=files, headers=AUTH).status_code == 409


# ------------------------------------------------------------------ keys

def test_keys_and_audit_empty(client):
    assert client.get("/api/keys", headers=AUTH).json() == {"keys": []}
    assert client.get("/api/audit", headers=AUTH).json() == {"entries": []}
    r = client.post("/api/keys/revoke", json={"fingerprint": "SHA256:" + "A" * 43}, headers=AUTH)
    assert r.status_code == 404


# ------------------------------------------------------------------ jobs

def test_unknown_action_rejected(client):
    r = client.post("/api/actions/rm-rf", json={}, headers=AUTH)
    assert r.status_code == 404 and "unknown action" in r.json()["error"]


def test_action_param_validation(client):
    assert client.post("/api/actions/verify", json={"limit": "m1"}, headers=AUTH).status_code == 422
    assert client.post("/api/actions/verify", json={"extra": 1}, headers=AUTH).status_code == 422
    assert client.post("/api/actions/verify", json={"limit": ["ghost"]}, headers=AUTH).status_code == 400


def test_action_needing_vault_is_409_when_locked(client):
    assert client.post("/api/actions/locked_only", json={}, headers=AUTH).status_code == 409


def test_second_concurrent_job_is_409(client, gate):
    r = client.post("/api/actions/verify", json={"limit": ["w1"]}, headers=AUTH)
    assert r.status_code == 202
    job_id = r.json()["job_id"]
    second = client.post("/api/actions/verify", json={}, headers=AUTH)
    assert second.status_code == 409 and "still running" in second.json()["error"]

    gate.release.set()
    job = _wait_job(client, job_id)
    assert job["state"] == "done" and job["error"] == ""
    assert gate.calls == [("w1",)]
    msgs = [e["msg"] for e in job["history"]]
    assert "fake step running" in msgs and "fake step complete" in msgs
    assert client.post("/api/actions/verify", json={}, headers=AUTH).status_code == 202


def test_failed_job_reports_error(client):
    job_id = client.post("/api/actions/boom", headers=AUTH).json()["job_id"]
    job = _wait_job(client, job_id)
    assert job["state"] == "failed" and "simulated failure" in job["error"]
    assert job["history"][-1]["level"] == "error"


def test_job_stream_sse(client, gate):
    gate.release.set()
    job_id = client.post("/api/actions/verify", json={}, headers=AUTH).json()["job_id"]
    _wait_job(client, job_id)
    with client.stream("GET", f"/api/jobs/{job_id}/stream", params={"token": TOKEN}) as r:
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("text/event-stream")
        body = "".join(r.iter_text())
    assert "event: log" in body and "fake step complete" in body
    assert 'event: end\ndata: {"state": "done"' in body


def test_unknown_job_404(client):
    assert client.get("/api/jobs/nope", headers=AUTH).status_code == 404


# ------------------------------------------------------------------ helpers

def test_resolve_data_path_stays_inside_project(tmp_path):
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "in.txt").write_text("a b a", encoding="utf-8")
    assert resolve_data_path(tmp_path, "data/in.txt") == (tmp_path / "data" / "in.txt").resolve()
    with pytest.raises(ValueError):
        resolve_data_path(tmp_path, "../outside.txt")
    with pytest.raises(ValueError):
        resolve_data_path(tmp_path, "data/missing.txt")
