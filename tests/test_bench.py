from clusterctl.bench import RunPoint, corpus_name, speedup_table


def _p(size, k, s):
    return RunPoint(size_mb=size, workers=k, cores=24 * k, seconds=s, sha256="x")


def test_speedup_and_efficiency_use_median_against_one_worker():
    points = [_p(1000, 1, 90), _p(1000, 1, 100), _p(1000, 1, 110),
              _p(1000, 2, 50), _p(1000, 2, 52), _p(1000, 2, 48),
              _p(1000, 3, 40), _p(1000, 3, 40), _p(1000, 3, 41)]

    rows = {r["workers"]: r for r in speedup_table(points)}

    assert rows[1]["median_s"] == 100 and rows[1]["speedup"] == 1.0
    assert rows[2]["speedup"] == 2.0 and rows[2]["efficiency"] == 1.0
    assert rows[3]["speedup"] == 2.5 and rows[3]["efficiency"] == 0.83
    assert rows[3]["throughput_mb_s"] == 25.0


def test_speedup_table_groups_by_size():
    rows = speedup_table([_p(100, 1, 10), _p(100, 2, 8), _p(500, 1, 40), _p(500, 2, 21)])

    assert [(r["size_mb"], r["workers"]) for r in rows] == [(100, 1), (100, 2), (500, 1), (500, 2)]


def test_corpus_name():
    assert corpus_name(2000) == "corpus-2000mb.txt"
