import hashlib

import pytest

from clusterctl.inventory import ClusterConfig
from clusterctl.spark_dist import DistError, parse_sha512, sha512_file, tarball_name

FILENAME = "spark-3.5.10-bin-hadoop3.tgz"
HEX = hashlib.sha512(b"fake").hexdigest()


def test_bsd_style_hex_then_filename():
    assert parse_sha512(f"{HEX}  {FILENAME}\n", FILENAME) == HEX


def test_uppercase_hex_is_lowercased():
    assert parse_sha512(f"{HEX.upper()}  {FILENAME}", FILENAME) == HEX


def test_gpg_style_split_across_lines():
    groups = [HEX[i:i + 8].upper() for i in range(0, 128, 8)]
    text = f"{FILENAME}: " + " ".join(groups[:8]) + "\n        " + " ".join(groups[8:]) + "\n"
    assert parse_sha512(text, FILENAME) == HEX


def test_bare_hex_without_filename():
    assert parse_sha512(HEX, FILENAME) == HEX


@pytest.mark.parametrize("text", ["", "garbage", f"{FILENAME}: ABCD", HEX[:100], HEX + "ab"])
def test_bad_input_raises(text):
    with pytest.raises(DistError, match="could not parse SHA-512"):
        parse_sha512(text, FILENAME)


def test_tarball_name_from_config():
    assert tarball_name(ClusterConfig()) == FILENAME


def test_sha512_file_matches_hashlib(tmp_path):
    f = tmp_path / "x.bin"
    f.write_bytes(b"fake")
    assert sha512_file(f) == HEX
