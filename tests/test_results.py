import json
import os
import time

import pytest

from clusterctl.facts import parse_facts
from clusterctl.inventory import Host
from clusterctl.paths import ProjectPaths
from clusterctl.results import parse_driver_log, results_index, safe_result_file

DRIVER_LOG = """\
26/11/26 22:45:27 INFO StandaloneSchedulerBackend: Granted executor ID app-1/0 on hostPort 10.0.0.2:4001 with 24 core(s), 1024.0 MiB RAM
26/11/26 22:45:27 INFO StandaloneSchedulerBackend: Granted executor ID app-1/1 on hostPort 10.0.0.3:4001 with 24 core(s), 1024.0 MiB RAM
26/11/26 22:45:41 ERROR TaskSchedulerImpl: Lost executor 0 on 10.0.0.2: Worker shutting down
26/11/26 22:45:41 WARN TaskSetManager: Lost task 23.0 in stage 0.0 (TID 23) (10.0.0.2 executor 0): ExecutorLostFailure
26/11/26 22:45:41 WARN TaskSetManager: Lost task 8.0 in stage 0.0 (TID 8) (10.0.0.2 executor 0): ExecutorLostFailure
26/11/26 22:45:42 INFO TaskSetManager: Starting task 23.1 in stage 0.0 (TID 48) (10.0.0.3, executor 1, partition 23, PROCESS_LOCAL, 9045 bytes)
26/11/26 22:45:42 INFO TaskSetManager: Starting task 8.1 in stage 0.0 (TID 49) (10.0.0.3, executor 1, partition 8, PROCESS_LOCAL, 9045 bytes)
26/11/26 22:46:59 INFO DAGScheduler: ShuffleMapStage 0 (reduceByKey at /opt/x/wordcount.py:64) finished in 78.718 s
============================================================
WORDCOUNT_DONE app_id=app-1 driver=node1
input=file:///data/corpus.txt partitions=288 executors=1
total_words=1000 distinct_words=3 elapsed_s=84.5
output=/data/out.tsv
top 20:
         600  the
         300  of
         100  and
============================================================
"""


def test_parse_driver_log_extracts_summary_and_recovery():
    parsed = parse_driver_log(DRIVER_LOG.splitlines())

    assert parsed["summary"]["total_words"] == 1000
    assert parsed["summary"]["partitions"] == 288
    assert parsed["summary"]["elapsed_s"] == 84.5
    assert parsed["top"][0] == {"word": "the", "count": 600}
    assert len(parsed["top"]) == 3
    assert [x["ip"] for x in parsed["executors"]] == ["10.0.0.2", "10.0.0.3"]
    assert parsed["lost_executors"] == [{"id": "0", "ip": "10.0.0.2", "reason": "Worker shutting down"}]
    assert parsed["lost_tasks"] == 2
    assert parsed["retried_tasks"] == 2
    assert parsed["stages"][0] == {"stage": "ShuffleMapStage 0", "op": "reduceByKey", "seconds": 78.718}


def test_parse_driver_log_handles_reporter_prefix():
    prefixed = [f"[node1] {line}" for line in DRIVER_LOG.splitlines()]

    assert parse_driver_log(prefixed)["summary"]["total_words"] == 1000


def _write(path, data, age):
    path.write_text(json.dumps(data), encoding="utf-8")
    stamp = time.time() - age
    os.utime(path, (stamp, stamp))


def test_results_index_is_newest_first(tmp_path):
    paths = ProjectPaths(tmp_path)
    paths.results_dir.mkdir()
    _write(paths.results_dir / "wordcount-a.json", {"tag": "old"}, 100)
    _write(paths.results_dir / "wordcount-b.json", {"tag": "new"}, 1)
    _write(paths.results_dir / "demo-a.json", {"stamp": "d"}, 5)

    index = results_index(paths)

    assert [r["tag"] for r in index["wordcount"]] == ["new", "old"]
    assert index["demos"][0]["stamp"] == "d"


def test_results_index_without_folder(tmp_path):
    assert results_index(ProjectPaths(tmp_path))["wordcount"] == []


@pytest.mark.parametrize("name", ["../secrets.vault", "..", ".vault-pass", "a/b.txt", "x\\y", ""])
def test_safe_result_file_rejects_traversal(tmp_path, name):
    paths = ProjectPaths(tmp_path)
    paths.results_dir.mkdir()
    (tmp_path / "secrets.vault").write_text("x")

    with pytest.raises((ValueError, FileNotFoundError)):
        safe_result_file(paths, name)


def test_safe_result_file_returns_existing_file(tmp_path):
    paths = ProjectPaths(tmp_path)
    paths.results_dir.mkdir()
    (paths.results_dir / "demo-1.log").write_text("log")

    assert safe_result_file(paths, "demo-1.log").read_text() == "log"


def test_parse_facts_detects_bare_metal():
    host = Host(name="node1", ip="10.0.0.1", user="u", role="master")
    output = "hostname=pc1\nvirt=none\ncpu_cores=24\nmem_kb=16000000\nvendor=HP\n" \
             "svc_spark-master=active|Thu 2026-11-26 17:57:02 IST|0|enabled\n"

    facts = parse_facts(host, output)

    assert facts.is_physical
    assert facts.cpu_cores == 24
    assert facts.mem_gb == 15.3
    assert facts.services[0].name == "spark-master"
    assert facts.services[0].enabled == "enabled"


def test_parse_facts_flags_virtual_machine():
    host = Host(name="node1", ip="10.0.0.1", user="u")

    assert not parse_facts(host, "virt=kvm\n").is_physical
