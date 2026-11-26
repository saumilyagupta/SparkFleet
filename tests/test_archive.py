from clusterctl.archive import archive, wanted
from clusterctl.paths import ProjectPaths


def test_wanted_selects_results_and_skips_fake_previews():
    assert wanted("bench-20261127-101500.json")
    assert wanted("demo-shuffle-20261127-101500.md")
    assert wanted("dashboard-live.png")
    assert not wanted("bench-20261127-101500-100mb-w1-r1.json")   # per-run detail
    assert not wanted("scaling-preview-dashboard.png")             # fake data
    assert not wanted("wordcount-20261127-101500.tsv")


def test_archive_copies_and_refuses_secrets(tmp_path):
    paths = ProjectPaths(tmp_path)
    paths.results_dir.mkdir()
    (tmp_path / ".vault-pass").write_text("vault-master-pw")
    (paths.results_dir / "demo-a.md").write_text("all good")
    (paths.results_dir / "demo-b.md").write_text("oops hunter2 leaked")
    (paths.results_dir / "demo-c.log").write_text("x vault-master-pw y")
    (paths.results_dir / "demo-d.log").write_bytes(b"-----BEGIN OPENSSH PRIVATE KEY-----\n")

    result = archive(paths, vault_secrets=("hunter2",))

    assert result.copied == ("demo-a.md",)
    assert len(result.refused) == 3
    assert (tmp_path / "docs" / "results" / "demo-a.md").read_text() == "all good"
    assert not (tmp_path / "docs" / "results" / "demo-b.md").exists()


def test_archive_is_idempotent(tmp_path):
    paths = ProjectPaths(tmp_path)
    paths.results_dir.mkdir()
    (paths.results_dir / "demo-a.md").write_text("x")
    archive(paths)

    assert archive(paths).unchanged == ("demo-a.md",)
