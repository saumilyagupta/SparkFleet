import json

import pytest

from clusterctl.vault import MIN_MASTER_PASSWORD, Vault, VaultError

MASTER = "correct-horse-battery"
SECRET = "S3cr3t-fake-pw-XYZ"


@pytest.fixture
def vault_path(tmp_path):
    return tmp_path / "secrets.vault"


def test_create_then_open_round_trip(vault_path):
    Vault.create(vault_path, MASTER).with_secret("w1", SECRET).save()
    reopened = Vault.open(vault_path, MASTER)
    assert reopened.get("w1") == SECRET
    assert reopened.names() == ("w1",)


def test_wrong_master_password_raises(vault_path):
    Vault.create(vault_path, MASTER)
    with pytest.raises(VaultError, match="wrong master password"):
        Vault.open(vault_path, "not-the-password")


def test_short_master_password_rejected(vault_path):
    with pytest.raises(VaultError, match="at least"):
        Vault.create(vault_path, "x" * (MIN_MASTER_PASSWORD - 1))
    assert not vault_path.exists()


def test_create_refuses_to_overwrite(vault_path):
    Vault.create(vault_path, MASTER)
    with pytest.raises(VaultError, match="already exists"):
        Vault.create(vault_path, MASTER)


def test_open_missing_file(vault_path):
    with pytest.raises(VaultError, match="not found"):
        Vault.open(vault_path, MASTER)


def test_open_corrupt_file(vault_path):
    vault_path.write_text("{not json", encoding="utf-8")
    with pytest.raises(VaultError, match="corrupt"):
        Vault.open(vault_path, MASTER)


def test_open_file_missing_keys(vault_path):
    vault_path.write_text("{}", encoding="utf-8")
    with pytest.raises(VaultError, match="corrupt"):
        Vault.open(vault_path, MASTER)


def test_tampered_data_detected(vault_path):
    Vault.create(vault_path, MASTER).with_secret("w1", SECRET).save()
    doc = json.loads(vault_path.read_text(encoding="utf-8"))
    data = doc["data"]
    doc["data"] = data[:-4] + ("AAAA" if not data.endswith("AAAA") else "BBBB")
    vault_path.write_text(json.dumps(doc), encoding="utf-8")
    with pytest.raises(VaultError):
        Vault.open(vault_path, MASTER)


def test_with_secret_returns_new_vault_and_original_unchanged(vault_path):
    original = Vault.create(vault_path, MASTER)
    updated = original.with_secret("w1", SECRET)
    assert updated is not original
    assert original.get("w1") is None
    assert updated.get("w1") == SECRET


def test_with_secret_empty_password_rejected(vault_path):
    with pytest.raises(VaultError, match="empty"):
        Vault.create(vault_path, MASTER).with_secret("w1", "")


def test_without_secret_returns_new_vault_and_original_unchanged(vault_path):
    original = Vault.create(vault_path, MASTER).with_secret("w1", SECRET)
    removed = original.without_secret("w1")
    assert removed.get("w1") is None
    assert original.get("w1") == SECRET


def test_without_secret_unknown_name_is_noop(vault_path):
    original = Vault.create(vault_path, MASTER).with_secret("w1", SECRET)
    assert original.without_secret("zzz").names() == ("w1",)


def test_secrets_mapping_is_read_only(vault_path):
    vault = Vault.create(vault_path, MASTER).with_secret("w1", SECRET)
    with pytest.raises(TypeError):
        vault._secrets["w1"] = "changed"


def test_file_on_disk_does_not_contain_plaintext(vault_path):
    Vault.create(vault_path, MASTER).with_secret("w1", SECRET).save()
    raw = vault_path.read_bytes()
    assert SECRET.encode() not in raw
    assert MASTER.encode() not in raw


def test_names_sorted(vault_path):
    vault = Vault.create(vault_path, MASTER).with_secret("b", "x").with_secret("a", "y")
    assert vault.names() == ("a", "b")


def test_unicode_password_round_trip(vault_path):
    pw = "pässwörd-日本語"
    Vault.create(vault_path, MASTER).with_secret("w1", pw).save()
    assert Vault.open(vault_path, MASTER).get("w1") == pw
