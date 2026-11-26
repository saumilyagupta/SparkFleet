"""Read-only hardware/software facts per node: evidence that these are real, physical machines."""

from __future__ import annotations

from dataclasses import asdict, dataclass

from clusterctl.context import Context
from clusterctl.inventory import Host
from clusterctl.provision import services_for
from clusterctl.runner import run_parallel

# One round trip per host. Every line is KEY=value; nothing here needs sudo.
_FACTS_SCRIPT = r"""
. /etc/os-release 2>/dev/null
echo "hostname=$(hostname)"
echo "os=${PRETTY_NAME:-unknown}"
echo "kernel=$(uname -r)"
echo "arch=$(uname -m)"
echo "cpu_model=$(lscpu 2>/dev/null | sed -n 's/^Model name:[[:space:]]*//p' | head -1)"
echo "cpu_cores=$(nproc 2>/dev/null)"
echo "mem_kb=$(awk '/MemTotal/ {print $2}' /proc/meminfo)"
echo "virt=$(systemd-detect-virt 2>/dev/null || echo none)"
echo "vendor=$(cat /sys/class/dmi/id/sys_vendor 2>/dev/null)"
echo "product=$(cat /sys/class/dmi/id/product_name 2>/dev/null)"
echo "board=$(cat /sys/class/dmi/id/board_name 2>/dev/null)"
echo "java=$(java -version 2>&1 | head -1)"
echo "spark=$(head -1 {spark_home}/RELEASE 2>/dev/null)"
echo "boot_time=$(uptime -s 2>/dev/null)"
for svc in {services}; do
  echo "svc_$svc=$(systemctl show -p ActiveState --value $svc)|$(systemctl show -p ActiveEnterTimestamp --value $svc)|$(systemctl show -p NRestarts --value $svc)|$(systemctl is-enabled $svc 2>/dev/null)"
done
"""


@dataclass(frozen=True)
class ServiceFact:
    name: str
    state: str
    since: str
    restarts: str
    enabled: str


@dataclass(frozen=True)
class NodeFacts:
    name: str
    ip: str
    role: str
    hostname: str
    os: str
    kernel: str
    arch: str
    cpu_model: str
    cpu_cores: int
    mem_gb: float
    virt: str
    vendor: str
    product: str
    board: str
    java: str
    spark: str
    boot_time: str
    services: tuple[ServiceFact, ...]

    @property
    def is_physical(self) -> bool:
        return self.virt == "none"

    def to_dict(self) -> dict:
        data = asdict(self)
        data["services"] = [asdict(s) for s in self.services]
        data["is_physical"] = self.is_physical
        return data


def parse_facts(host: Host, output: str) -> NodeFacts:
    kv: dict[str, str] = {}
    for line in output.splitlines():
        key, sep, value = line.partition("=")
        if sep:
            kv[key.strip()] = value.strip()
    services = []
    for key, value in kv.items():
        if key.startswith("svc_"):
            state, since, restarts, enabled = (value.split("|") + ["", "", "", ""])[:4]
            services.append(ServiceFact(key[4:], state, since, restarts, enabled))
    mem_kb = int(kv.get("mem_kb") or 0)
    return NodeFacts(
        name=host.name, ip=host.ip, role=host.role,
        hostname=kv.get("hostname", ""), os=kv.get("os", ""), kernel=kv.get("kernel", ""),
        arch=kv.get("arch", ""), cpu_model=kv.get("cpu_model", ""),
        cpu_cores=int(kv.get("cpu_cores") or 0), mem_gb=round(mem_kb / 1024 / 1024, 1),
        virt=kv.get("virt", "unknown") or "unknown", vendor=kv.get("vendor", ""),
        product=kv.get("product", ""), board=kv.get("board", ""), java=kv.get("java", ""),
        spark=kv.get("spark", ""), boot_time=kv.get("boot_time", ""), services=tuple(services),
    )


def node_facts(ctx: Context, host: Host) -> NodeFacts:
    script = (_FACTS_SCRIPT.replace("{spark_home}", ctx.inventory.cluster.spark_home)
              .replace("{services}", " ".join(services_for(ctx.inventory, host))))
    with ctx.session(host) as s:
        return parse_facts(host, s.run(script, check=False).out)


def collect_facts(ctx: Context) -> list[dict]:
    """Facts for every host; unreachable hosts are reported, not raised."""
    results = run_parallel(ctx.inventory.hosts, lambda h: node_facts(ctx, h), _Quiet(), "facts")
    out = []
    for host, result in zip(ctx.inventory.hosts, results):
        if result.ok:
            out.append({**result.value.to_dict(), "reachable": True})
        else:
            out.append({"name": host.name, "ip": host.ip, "role": host.role,
                        "reachable": False, "error": result.detail})
    return out


class _Quiet:
    def log(self, *args, **kwargs) -> None:
        pass
