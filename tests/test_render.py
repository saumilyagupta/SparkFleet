import pytest

from clusterctl.paths import TEMPLATES_DIR
from clusterctl.provision import render_values
from clusterctl.render import TemplateError, render, render_text

TEMPLATES = sorted(p.name for p in TEMPLATES_DIR.iterdir() if p.is_file())


def test_render_text_substitutes_values():
    assert render_text("a={{x}} b={{ y }}", {"x": 1, "y": "two"}) == "a=1 b=two"


def test_render_text_repeated_placeholder():
    assert render_text("{{x}}-{{x}}", {"x": "q"}) == "q-q"


def test_render_text_missing_value_raises():
    with pytest.raises(TemplateError, match="missing_key"):
        render_text("{{missing_key}}", {})


def test_render_text_no_placeholders_unchanged():
    assert render_text("plain ${SHELL} text", {}) == "plain ${SHELL} text"


def test_render_normalises_crlf(tmp_path):
    (tmp_path / "t").write_bytes(b"a={{x}}\r\nb\r\n")
    assert render("t", {"x": 1}, templates_dir=tmp_path) == "a=1\nb\n"


def test_template_set_is_not_empty():
    assert TEMPLATES


@pytest.mark.parametrize("host_name", ["m1", "w1", "w2"])
@pytest.mark.parametrize("name", TEMPLATES)
def test_real_template_renders_without_leftover_placeholders(name, host_name, sample_inventory):
    values = render_values(sample_inventory, sample_inventory.get(host_name))
    out = render(name, values)
    assert "{{" not in out and "}}" not in out


def test_render_values_includes_worker_resources_only_when_set(sample_inventory):
    with_res = render_values(sample_inventory, sample_inventory.get("w1"))
    without = render_values(sample_inventory, sample_inventory.get("w2"))
    assert "SPARK_WORKER_CORES=4" in with_res["worker_resources"]
    assert "SPARK_WORKER_MEMORY=4g" in with_res["worker_resources"]
    assert without["worker_resources"] == ""
    assert with_res["master_ip"] == "10.0.0.1"
