from clusterctl.monitor import CpuTimes, cpu_percent, mem_percent, parse_probe

PROBE_OUT = """\
cpu  100 0 50 800 50 0 0 0 0 0
MemTotal:       16000000 kB
MemAvailable:   12000000 kB
0.39 0.31 0.20 1/900 12345
"""


def test_parse_probe_reads_cpu_memory_and_load():
    sample = parse_probe(PROBE_OUT)

    assert sample.cpu == CpuTimes(busy=150, total=1000)
    assert sample.mem_total_kb == 16000000
    assert sample.load1 == 0.39


def test_cpu_percent_uses_deltas():
    assert cpu_percent(CpuTimes(150, 1000), CpuTimes(650, 2000)) == 50.0


def test_cpu_percent_handles_counter_reset():
    assert cpu_percent(CpuTimes(500, 1000), CpuTimes(10, 1000)) == 0.0


def test_mem_percent():
    assert mem_percent(parse_probe(PROBE_OUT)) == 25.0
