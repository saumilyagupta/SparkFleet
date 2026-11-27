"""Dashboard actions: each one composes the SAME library calls the CLI commands use."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from clusterctl import bootstrap as boot
from clusterctl import cluster_ops, provision, workload
from clusterctl.context import Context
from clusterctl.inventory import Host
from clusterctl.runner import all_ok, failed_names

INPUT_NAME = r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$"
HOST_NAME = r"^[a-z][a-z0-9-]{0,62}$"


class ActionFailed(RuntimeError):
    """An action ran but some hosts failed (CLI would exit 1)."""


class _Params(BaseModel):
    model_config = ConfigDict(extra="forbid")


class NoParams(_Params):
    pass


class LimitParams(_Params):
    limit: list[str] | None = Field(default=None, max_length=256)


class PushParams(_Params):
    path: str = Field(min_length=1, max_length=1024)


class RunParams(_Params):
    input: str = Field(pattern=INPUT_NAME)
    partitions: int = Field(default=0, ge=0, le=100_000)
    top: int = Field(default=20, ge=1, le=10_000)


class DemoParams(_Params):
    input: str = Field(pattern=INPUT_NAME)
    mode: Literal["worker", "executor"] = "worker"
    victim: str | None = Field(default=None, pattern=HOST_NAME)
    waves: int = Field(default=4, ge=1, le=100)
    delay_ms: int = Field(default=12000, ge=0, le=600_000)


class ScenarioParams(_Params):
    scenario: Literal["shuffle", "straggler", "cascade"]


class FailoverParams(_Params):
    input: str = Field(default="corpus.txt", pattern=INPUT_NAME)


# Guard rails for a browser button: the defaults (100/500/1000/2000 MB x 3 repeats) already take
# about 25 minutes on the lab cluster, so the UI cannot ask for an all-day run by accident.
BENCH_MAX_SIZES = 8
BENCH_MAX_MB = 2000
BENCH_MAX_REPEATS = 5


def parse_sizes(raw: str) -> tuple[int, ...]:
    """"100,500" -> (100, 500). Rejects empty/odd values and anything over BENCH_MAX_MB."""
    parts = [p.strip() for p in raw.split(",")]
    if not parts or any(not p.isdigit() for p in parts):
        raise ValueError("sizes must be comma-separated whole MB values, e.g. 100,500,1000")
    sizes = sorted({int(p) for p in parts})
    if len(sizes) > BENCH_MAX_SIZES:
        raise ValueError(f"at most {BENCH_MAX_SIZES} sizes per benchmark")
    if sizes[0] < 1 or sizes[-1] > BENCH_MAX_MB:
        raise ValueError(f"sizes must be between 1 and {BENCH_MAX_MB} MB")
    return tuple(sizes)


class BenchParams(_Params):
    """Same knobs as `clusterctl bench`; sizes are normalized so the action sees a clean tuple."""
    sizes: str = "100,500,1000,2000"
    repeats: int = Field(default=3, ge=1, le=BENCH_MAX_REPEATS)
    max_workers: int | None = Field(default=None, ge=1, le=1000)

    @field_validator("sizes")
    @classmethod
    def _check_sizes(cls, value: str) -> str:
        return ",".join(str(n) for n in parse_sizes(value))

    def size_list(self) -> tuple[int, ...]:
        return parse_sizes(self.sizes)


@dataclass(frozen=True)
class Action:
    fn: Callable[[Context, BaseModel], object]
    params: type[BaseModel] = NoParams
    needs_vault: bool = False


def _hosts(ctx: Context, params: BaseModel) -> tuple[Host, ...]:
    return ctx.inventory.select(getattr(params, "limit", None))


def _check(results, label: str) -> None:
    if not all_ok(results):
        raise ActionFailed(f"{label} failed on: {', '.join(failed_names(results))}")


def do_bootstrap(ctx: Context, p: LimitParams) -> None:
    _check(boot.bootstrap(ctx, _hosts(ctx, p)), "bootstrap")


def do_provision(ctx: Context, p: LimitParams) -> None:
    provision.provision(ctx, _hosts(ctx, p))


def do_up(ctx: Context, p: LimitParams) -> None:
    """Same sequence as `clusterctl up`: bootstrap -> provision -> start -> verify."""
    hosts = _hosts(ctx, p)
    _check(boot.bootstrap(ctx, hosts), "bootstrap")
    provision.provision(ctx, hosts)
    _check(cluster_ops.start(ctx, hosts), "start")
    cluster_ops.verify(ctx)


def _service(fn: Callable, label: str, then_verify: bool) -> Callable[[Context, LimitParams], None]:
    """Same as `clusterctl start|stop|restart`: verify after a successful start/restart."""
    def run(ctx: Context, p: LimitParams) -> None:
        _check(fn(ctx, _hosts(ctx, p)), label)
        if then_verify:
            cluster_ops.verify(ctx)
    return run


def do_verify(ctx: Context, _p: NoParams) -> None:
    cluster_ops.verify(ctx)


def resolve_data_path(root: Path, raw: str) -> Path:
    """Data files must live inside the project directory (relative paths resolve there)."""
    root = root.resolve()
    candidate = Path(raw)
    path = (candidate if candidate.is_absolute() else root / candidate).resolve()
    if path != root and root not in path.parents:
        raise ValueError("data file must be inside the project directory")
    if not path.is_file():
        raise ValueError(f"data file not found: {path.relative_to(root)}")
    return path


def do_push_data(ctx: Context, p: PushParams) -> None:
    remote = workload.push_data(ctx, resolve_data_path(ctx.paths.root, p.path))
    ctx.reporter.log("-", f"data available on every node at {remote}", "ok")


def do_run_wordcount(ctx: Context, p: RunParams) -> None:
    workload.run_wordcount(ctx, p.input, partitions=p.partitions, top=p.top)


def do_demo_failure(ctx: Context, p: DemoParams) -> None:
    workload.demo_failure(ctx, p.input, mode=p.mode, victim_name=p.victim,
                          waves=p.waves, delay_ms=p.delay_ms)


def do_demo_scenario(ctx: Context, p: ScenarioParams) -> None:
    from clusterctl.scenarios import run_scenario

    run_scenario(ctx, p.scenario)


def do_demo_failover(ctx: Context, p: FailoverParams) -> None:
    """Kill the active master mid-job; ZooKeeper must elect the standby and the job must finish."""
    from clusterctl.ha import demo_master_failover  # noqa: PLC0415 - import on use, like the CLI

    d = demo_master_failover(ctx, p.input)
    ctx.reporter.log("-", f"new master {d['new_master']['name']}; results identical={d['identical']}",
                     "ok" if d["identical"] else "error")


def do_ha(ctx: Context, _p: NoParams) -> None:
    """Read-only: masters (ALIVE/STANDBY) and ZooKeeper modes, same output as `clusterctl ha`."""
    from clusterctl.ha import master_states, zookeeper_status  # noqa: PLC0415

    cfg = ctx.inventory.cluster
    ctx.reporter.log("-", f"master HA: {'ENABLED' if cfg.ha_enabled else 'disabled'}; "
                          f"master URL {ctx.inventory.spark_master_url}")
    for m in master_states(ctx):
        extra = f"workers={m['alive_workers']}" if "alive_workers" in m else m.get("error", "")
        ctx.reporter.log(m["name"], f"master {m['ip']} {m['status']} {extra}")
    for z in zookeeper_status(ctx):
        ctx.reporter.log(z["name"], f"zookeeper {z['ip']} {z['mode']}")


def do_bench(ctx: Context, p: BenchParams) -> None:
    from clusterctl.bench import run_benchmark  # noqa: PLC0415 - import on use, like the CLI

    report = run_benchmark(ctx, sizes_mb=p.size_list(), repeats=p.repeats, max_workers=p.max_workers)
    for r in report["table"]:
        ctx.reporter.log("-", f"{r['size_mb']} MB on {r['workers']} worker(s): {r['median_s']} s, "
                              f"speedup {r['speedup']}, efficiency {r['efficiency']}, {r['throughput_mb_s']} MB/s")
    ctx.reporter.log("-", f"correct vs single-machine reference: {report['correct_vs_reference']}",
                     "ok" if all(report["correct_vs_reference"].values()) else "warn")


def do_archive(ctx: Context, _p: NoParams) -> None:
    """Secret-scan results/ into docs/results/ exactly like `clusterctl archive` (secrets from the vault)."""
    from clusterctl.archive import archive  # noqa: PLC0415 - import on use, like the CLI

    vault = ctx.vault
    secrets = tuple(s for s in (vault.get(n) for n in vault.names()) if s) if vault else ()
    result = archive(ctx.paths, secrets)
    for name in result.copied:
        ctx.reporter.log("-", f"copied docs/results/{name}", "ok")
    for name in result.refused:
        ctx.reporter.log("-", f"REFUSED {name}", "error")
    ctx.reporter.log("-", f"{len(result.copied)} copied, {len(result.unchanged)} unchanged, "
                          f"{len(result.refused)} refused")
    if result.refused:
        raise ActionFailed(f"{len(result.refused)} result file(s) refused by the secret scan")


def do_evidence(ctx: Context, _p: NoParams) -> None:
    """Write results/evidence-<ts>.html (facts + latest runs) for the evaluator."""
    from clusterctl.evidence import generate_report  # noqa: PLC0415 - optional module, import on use

    path = Path(generate_report(ctx))
    ctx.reporter.log("-", f"evidence report written: {path.name}", "ok")


def default_actions() -> dict[str, Action]:
    """Name -> action. `tunnel` is handled by the app (it keeps servers alive)."""
    return {
        "bootstrap": Action(do_bootstrap, LimitParams, needs_vault=True),
        "provision": Action(do_provision, LimitParams, needs_vault=True),
        "up": Action(do_up, LimitParams, needs_vault=True),
        "start": Action(_service(cluster_ops.start, "start", True), LimitParams, needs_vault=True),
        "stop": Action(_service(cluster_ops.stop, "stop", False), LimitParams, needs_vault=True),
        "restart": Action(_service(cluster_ops.restart, "restart", True), LimitParams, needs_vault=True),
        "verify": Action(do_verify),
        "push_data": Action(do_push_data, PushParams),
        "run_wordcount": Action(do_run_wordcount, RunParams),
        "demo_failure": Action(do_demo_failure, DemoParams, needs_vault=True),
        "demo_scenario": Action(do_demo_scenario, ScenarioParams, needs_vault=True),
        # Kills a master with sudo, then restores it: the most disruptive action in the UI.
        "demo_failover": Action(do_demo_failover, FailoverParams, needs_vault=True),
        "ha": Action(do_ha),
        "bench": Action(do_bench, BenchParams),
        "archive": Action(do_archive),
        "evidence": Action(do_evidence),
    }
