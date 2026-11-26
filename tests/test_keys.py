import base64
import hashlib
import json

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from clusterctl.keys import KeyError_, KeyRegistry, fingerprint, generate_control_key


def _pub_line(comment="test") -> str:
    key = Ed25519PrivateKey.generate().public_key()
    raw = key.public_bytes(serialization.Encoding.OpenSSH, serialization.PublicFormat.OpenSSH)
    return raw.decode("ascii") + f" {comment}"


@pytest.fixture
def registry(tmp_path):
    return KeyRegistry(tmp_path / "keys" / "registry.json", tmp_path / "keys" / "audit.log")


def _register(registry, line, owner="w1"):
    return registry.register(public_key=line, kind="node", owner=owner, user="ubuntu", ip="10.0.0.2")


class TestFingerprint:
    def test_matches_openssh_format_computed_independently(self):
        line = _pub_line()
        blob = base64.b64decode(line.split()[1])
        expected = "SHA256:" + base64.b64encode(hashlib.sha256(blob).digest()).decode().rstrip("=")
        assert fingerprint(line) == expected

    def test_no_base64_padding(self):
        assert not fingerprint(_pub_line()).endswith("=")

    def test_comment_does_not_change_fingerprint(self):
        line = _pub_line("a")
        assert fingerprint(line) == fingerprint(line.rsplit(" ", 1)[0] + " other")

    @pytest.mark.parametrize("bad", ["", "   ", "ssh-ed25519"])
    def test_malformed_line_rejected(self, bad):
        with pytest.raises(KeyError_):
            fingerprint(bad)


class TestControlKey:
    def test_creates_files_and_returns_public_line(self, tmp_path):
        priv, pub = tmp_path / "k" / "id", tmp_path / "k" / "id.pub"
        line = generate_control_key(priv, pub)
        assert priv.exists() and pub.exists()
        assert line.startswith("ssh-ed25519 ") and line.endswith("clusterctl-control")
        assert pub.read_text(encoding="utf-8").strip() == line

    def test_idempotent_returns_same_key(self, tmp_path):
        priv, pub = tmp_path / "id", tmp_path / "id.pub"
        first = generate_control_key(priv, pub)
        private_before = priv.read_bytes()
        second = generate_control_key(priv, pub)
        assert second == first
        assert priv.read_bytes() == private_before


class TestRegistry:
    def test_register_records_key(self, registry):
        line = _pub_line()
        rec = _register(registry, line)
        assert rec.fingerprint == fingerprint(line)
        assert rec.status == "active" and rec.key_type == "ssh-ed25519"
        assert registry.find(rec.fingerprint) == rec

    def test_register_dedupes(self, registry):
        line = _pub_line()
        first = _register(registry, line)
        second = _register(registry, line, owner="other")
        assert second == first
        assert len(registry.records()) == 1
        assert len(registry.audit_entries()) == 1

    def test_records_empty_when_no_file(self, registry):
        assert registry.records() == ()
        assert registry.find("SHA256:x") is None

    def test_mark_installed_adds_host_once(self, registry):
        rec = _register(registry, _pub_line())
        registry.mark_installed(rec.fingerprint, "w1")
        registry.mark_installed(rec.fingerprint, "w1")
        registry.mark_installed(rec.fingerprint, "w2")
        assert registry.find(rec.fingerprint).installed_on == ("w1", "w2")
        events = [e["event"] for e in registry.audit_entries()]
        assert events.count("key_installed") == 2

    def test_mark_installed_unknown_fp_is_noop(self, registry):
        registry.mark_installed("SHA256:nope", "w1")
        assert registry.audit_entries() == []

    def test_mark_revoked(self, registry):
        rec = _register(registry, _pub_line())
        registry.mark_installed(rec.fingerprint, "w1")
        registry.mark_installed(rec.fingerprint, "w2")
        updated = registry.mark_revoked(rec.fingerprint, ["w1"])
        assert updated.status == "revoked"
        assert updated.installed_on == ("w2",)
        assert registry.find(rec.fingerprint).status == "revoked"

    def test_mark_revoked_unknown_raises(self, registry):
        with pytest.raises(KeyError_, match="no key with fingerprint"):
            registry.mark_revoked("SHA256:nope", [])

    def test_audit_log_has_one_json_line_per_event(self, registry):
        rec = _register(registry, _pub_line())
        registry.mark_installed(rec.fingerprint, "w1")
        registry.mark_revoked(rec.fingerprint, ["w1"])
        lines = registry.audit_path.read_text(encoding="utf-8").splitlines()
        parsed = [json.loads(line) for line in lines]
        assert [p["event"] for p in parsed] == ["key_registered", "key_installed", "key_revoked"]
        assert all("at" in p and p["fingerprint"] == rec.fingerprint for p in parsed)

    def test_audit_entries_limit(self, registry):
        for i in range(5):
            registry.audit("e", n=i)
        assert [e["n"] for e in registry.audit_entries(limit=2)] == [3, 4]

    def test_audit_entries_empty_without_file(self, registry):
        assert registry.audit_entries() == []
