"""clusterctl command-line interface.  Run `python -m clusterctl --help`."""

from __future__ import annotations

import argparse
import dataclasses
import getpass
import json
import os
import sys
import time
from pathlib import Path

from clusterctl import bootstrap as boot
from clusterctl import cluster_ops, provision, workload
from clusterctl import inventory as inv
from clusterctl.context import Context
from clusterctl.keys import generate_control_key
from clusterctl.paths import ProjectPaths
from clusterctl.runner import ConsoleReporter, all_ok
from clusterctl.systems_import import load_systems
from clusterctl.vault import Vault, VaultError

ENV_PASS = "CLUSTERCTL_VAULT_PASSWORD"
ENV_PASS_FILE = "CLUSTERCTL_VAULT_PASSWORD_FILE"


# ------------------------------------------------------------------ helpers

def master_password(paths: ProjectPaths, confirm: bool = False) -> str:
    """Env var, then password file (.vault-pass, gitignored), then interactive prompt."""
    if os.environ.get(ENV_PASS):
        return os.environ[ENV_PASS]
    pass_file = Path(os.environ.get(ENV_PASS_FILE) or paths.root / ".vault-pass")
    if pass_file.is_file():
        return pass_file.read_text(encoding="utf-8").strip()
    first = getpass.getpass("Vault master password: ")
    if confirm and getpass.getpass("Repeat master password: ") != first:
        raise VaultError("passwords do not match")
    return first


def open_vault(paths: ProjectPaths) -> Vault:
    return Vault.open(paths.vault, master_password(paths))


def open_or_create_vault(paths: ProjectPaths) -> Vault:
    if paths.vault.exists():
        return open_vault(paths)
    vault = Vault.create(paths.vault, master_password(paths, confirm=True))
    print(f"created encrypted vault {paths.vault}")
    return vault


def make_context(paths: ProjectPaths, *, with_vault: bool = False) -> Context:
    inventory = inv.load(paths.inventory)
    vault = open_vault(paths) if with_vault else None
    return Context.build(paths, inventory, ConsoleReporter(), vault)


def limited(ctx: Context, args: argparse.Namespace) -> tuple[inv.Host, ...]:
    return ctx.inventory.select(args.limit.split(",") if getattr(args, "limit", None) else None)


def exit_code(results) -> int:
    return 0 if all_ok(results) else 1


# ------------------------------------------------------------------ commands

def cmd_init(paths: ProjectPaths, args: argparse.Namespace) -> int:
    generate_control_key(paths.control_key, paths.control_pub)
    print(f"control key: {paths.control_pub}")
    vault = open_or_create_vault(paths)
    if args.systems:
        return _import_systems(paths, vault, args)
    if not paths.inventory.exists():
        inv.save(inv.Inventory(cluster=inv.ClusterConfig(jump_host=args.jump)), paths.inventory)
        print(f"created empty {paths.inventory}; add hosts with 'inventory add' or 'import-systems'")
    return 0


def _import_systems(paths: ProjectPaths, vault: Vault, args: argparse.Namespace) -> int:
    base = inv.load(paths.inventory).cluster if paths.inventory.exists() else inv.ClusterConfig()
    if args.jump is not None:
        base = dataclasses.replace(base, jump_host=args.jump or None)
    result = load_systems(Path(args.systems), master=args.master, cluster=base)
    inv.save(result.inventory, paths.inventory)
    for name, password in result.passwords.items():
        vault = vault.with_secret(name, password)
    vault.save()
    for h in result.inventory.hosts:
        print(f"  {h.name:<12} {h.ip:<16} {h.user:<16} {h.role}")
    print(f"wrote {paths.inventory}; stored {len(result.passwords)} passwords in {paths.vault}")
    print(f"master: {result.inventory.master.name} ({result.inventory.master.ip})")
    print(f"NOTE: {args.systems} still holds plaintext passwords. It is gitignored; delete it once imported.")
    return 0


def cmd_import_systems(paths: ProjectPaths, args: argparse.Namespace) -> int:
    return _import_systems(paths, open_or_create_vault(paths), args)


def cmd_inventory(paths: ProjectPaths, args: argparse.Namespace) -> int:
    if args.action == "list":
        inventory = inv.load(paths.inventory)
        print(f"jump host: {inventory.cluster.jump_host or '(direct)'}")
        for h in inventory.hosts:
            print(f"  {h.name:<12} {h.ip:<16} {h.user:<16} {h.role}")
        return 0
    if args.action == "add":
        inventory = inv.load(paths.inventory).with_host(
            inv.Host(name=args.name, ip=args.ip, user=args.user, role=args.role))
        inv.save(inventory, paths.inventory)
        vault = open_or_create_vault(paths)
        vault.with_secret(args.name, getpass.getpass(f"password for {args.user}@{args.ip}: ")).save()
        print(f"added {args.name}; next: clusterctl up --limit {args.name}")
        return 0
    if args.action == "remove":
        inv.save(inv.load(paths.inventory).without_host(args.name), paths.inventory)
        print(f"removed {args.name} from inventory (vault entry kept; 'vault remove' to delete)")
        return 0
    if args.action == "import":
        rows = inv.read_csv(Path(args.csv))
        inventory = inv.load(paths.inventory)
        vault = open_or_create_vault(paths)
        for row in rows:
            inventory = inventory.with_host(row.host)
            if row.password:
                vault = vault.with_secret(row.host.name, row.password)
        inv.save(inventory, paths.inventory)
        vault.save()
        print(f"imported {len(rows)} hosts")
        if args.delete_after:
            Path(args.csv).unlink()
            print(f"deleted {args.csv}")
        return 0
    return 2


def cmd_vault(paths: ProjectPaths, args: argparse.Namespace) -> int:
    if args.action == "init":
        open_or_create_vault(paths)
        return 0
    vault = open_vault(paths)
    if args.action == "list":
        for name in vault.names():
            print(f"  {name}  (password stored)")
        return 0
    if args.action == "set":
        vault.with_secret(args.name, getpass.getpass(f"password for {args.name}: ")).save()
        print(f"stored password for {args.name}")
        return 0
    if args.action == "remove":
        vault.without_secret(args.name).save()
        return 0
    return 2


def cmd_keys(paths: ProjectPaths, args: argparse.Namespace) -> int:
    ctx = make_context(paths)
    if args.action == "list":
        for r in ctx.registry.records():
            print(f"{r.status:<8} {r.kind:<8} {r.owner:<12} {r.fingerprint}  on: {', '.join(r.installed_on) or '-'}")
        return 0
    if args.action == "audit":
        for entry in ctx.registry.audit_entries():
            print(json.dumps(entry))
        return 0
    if args.action == "revoke":
        removed = boot.revoke(ctx, args.fingerprint)
        print(f"revoked {args.fingerprint}; removed from: {', '.join(removed) or 'none'}")
        return 0
    return 2


def cmd_bootstrap(paths: ProjectPaths, args: argparse.Namespace) -> int:
    ctx = make_context(paths, with_vault=True)
    return exit_code(boot.bootstrap(ctx, limited(ctx, args)))


def cmd_provision(paths: ProjectPaths, args: argparse.Namespace) -> int:
    ctx = make_context(paths, with_vault=True)
    provision.provision(ctx, limited(ctx, args))
    return 0


def cmd_service(paths: ProjectPaths, args: argparse.Namespace) -> int:
    ctx = make_context(paths, with_vault=True)
    fn = {"start": cluster_ops.start, "stop": cluster_ops.stop, "restart": cluster_ops.restart}[args.command]
    code = exit_code(fn(ctx, limited(ctx, args)))
    if args.command != "stop" and code == 0:
        cluster_ops.verify(ctx)
    return code


def cmd_status(paths: ProjectPaths, args: argparse.Namespace) -> int:
    ctx = make_context(paths)
    data = cluster_ops.status(ctx)
    if args.json:
        print(json.dumps(data, indent=2))
        return 0
    m = data["master"]
    print(f"master: {m.get('status')}  workers alive: {len(m.get('alive_workers', []))}  "
          f"cores: {m.get('cores')}  memory: {m.get('memory_mb')} MB  {m.get('error', '')}")
    for h in data["hosts"]:
        svc = " ".join(f"{k}={v}" for k, v in h["services"].items())
        state = "up" if h["reachable"] else f"DOWN ({h['error']})"
        worker = ("ALIVE" if h["worker_alive"] else "not registered") if "spark-worker" in h["services"] else "-"
        print(f"  {h['name']:<12} {h['ip']:<16} {h['role']:<7} {state:<6} {svc:<26} "
              f"worker={worker:<15} {h['spark'][:12]}")
    return 0


def cmd_up(paths: ProjectPaths, args: argparse.Namespace) -> int:
    """One shot: bootstrap -> provision -> start -> verify."""
    ctx = make_context(paths, with_vault=True)
    hosts = limited(ctx, args)
    if not all_ok(boot.bootstrap(ctx, hosts)):
        return 1
    provision.provision(ctx, hosts)
    if not all_ok(cluster_ops.start(ctx, hosts)):
        return 1
    cluster_ops.verify(ctx)
    return 0


def cmd_data(paths: ProjectPaths, args: argparse.Namespace) -> int:
    ctx = make_context(paths)
    if args.action == "push":
        print(workload.push_data(ctx, Path(args.file)))
    elif args.action == "generate":
        from clusterctl.bench import generate_on_cluster

        info = generate_on_cluster(ctx, args.size_mb, with_reference=not args.no_reference)
        print(f"{info['name']} on every node; use: clusterctl run --input {info['name']}")
    elif args.action == "clean":
        from clusterctl.bench import clean_generated

        clean_generated(ctx)
    return 0


def cmd_bench(paths: ProjectPaths, args: argparse.Namespace) -> int:
    from clusterctl.bench import run_benchmark

    sizes = tuple(int(x) for x in args.sizes.split(","))
    report = run_benchmark(make_context(paths), sizes_mb=sizes, repeats=args.repeats,
                           max_workers=args.max_workers)
    print(f"{'size':>7} {'workers':>7} {'median':>8} {'speedup':>8} {'effic.':>7} {'MB/s':>7}")
    for r in report["table"]:
        print(f"{r['size_mb']:>5}MB {r['workers']:>7} {r['median_s']:>7}s {r['speedup']:>8} "
              f"{r['efficiency']:>7} {r['throughput_mb_s']:>7}")
    print(f"correct vs single-machine reference: {report['correct_vs_reference']}")
    return 0


def cmd_run(paths: ProjectPaths, args: argparse.Namespace) -> int:
    ctx = make_context(paths)
    workload.run_wordcount(ctx, args.input, partitions=args.partitions, top=args.top)
    return 0


def cmd_demo(paths: ProjectPaths, args: argparse.Namespace) -> int:
    ctx = make_context(paths, with_vault=True)
    if args.scenario == "failure":
        workload.demo_failure(ctx, args.input, mode=args.mode, victim_name=args.victim,
                              waves=args.waves, delay_ms=args.delay_ms)
        return 0
    if args.scenario == "master-failover":
        from clusterctl.ha import demo_master_failover

        d = demo_master_failover(ctx, args.input)
        print(f"master failover: new master {d['new_master']['name']}, identical={d['identical']}")
        return 0
    from clusterctl.scenarios import run_scenario

    d = run_scenario(ctx, args.scenario)
    print(f"{args.scenario}: identical={d['identical']} lost_tasks={d['lost_tasks']} "
          f"fetch_failed={d['fetch_failed']} resubmitted={d['resubmit_count']} "
          f"speculatable={d['speculatable']} speculative_wins={d['speculative_wins']}")
    return 0


def cmd_facts(paths: ProjectPaths, args: argparse.Namespace) -> int:
    from clusterctl.facts import collect_facts

    facts = collect_facts(make_context(paths))
    if args.json:
        print(json.dumps(facts, indent=2))
        return 0
    for f in facts:
        if not f["reachable"]:
            print(f"  {f['name']:<11} {f['ip']:<15} UNREACHABLE: {f['error']}")
            continue
        print(f"  {f['name']:<11} {f['ip']:<15} {f['role']:<7} host={f['hostname']:<12} "
              f"virt={f['virt']:<5} {f['vendor']} {f['product']}")
        print(f"  {'':<11} {f['cpu_model']} x{f['cpu_cores']} | {f['mem_gb']} GB | {f['os']} | {f['java']}")
    physical = sum(1 for f in facts if f.get("is_physical"))
    print(f"{physical}/{len(facts)} nodes report virtualization 'none' (bare metal)")
    return 0


def cmd_history(paths: ProjectPaths, args: argparse.Namespace) -> int:
    history = cluster_ops.job_history(make_context(paths))
    print(f"applications on {history['master']}:")
    for a in history["apps"]:
        print(f"  {a['id']:<26} {a['state']:<9} cores={a['cores']:<3} {a['duration_s']:>7}s  {a['name']}")
    return 0


def cmd_evidence(paths: ProjectPaths, args: argparse.Namespace) -> int:
    from clusterctl.evidence import generate_report

    report = generate_report(make_context(paths))
    print(f"evidence report: {report}")
    return 0


def cmd_monitor(paths: ProjectPaths, args: argparse.Namespace) -> int:
    from clusterctl.monitor import Sampler

    sampler = Sampler(make_context(paths)).start()
    print("live load every 3 s; Ctrl+C to stop")
    try:
        while True:
            time.sleep(sampler.interval_s)
            rows = []
            for n in sampler.snapshot()["nodes"]:
                p = n["latest"] or {}
                cpu = "-" if p.get("cpu") is None else f"{p['cpu']:5.1f}%"
                mem = "-" if p.get("mem") is None else f"{p['mem']:5.1f}%"
                rows.append(f"  {n['name']:<11} {n['role']:<7} cpu {cpu:>7}  mem {mem:>7}  "
                            f"load {p.get('load', '-')!s:<5} spark cores {p.get('spark_cores', 0):>3}  {n['status'][:40]}")
            print(time.strftime("%H:%M:%S"), *rows, sep="\n", flush=True)
    except KeyboardInterrupt:
        sampler.stop()
        return 0


def cmd_ha(paths: ProjectPaths, args: argparse.Namespace) -> int:
    from clusterctl.ha import master_states, zookeeper_status

    ctx = make_context(paths)
    cfg = ctx.inventory.cluster
    print(f"master HA: {'ENABLED' if cfg.ha_enabled else 'disabled'}   master URL: {ctx.inventory.spark_master_url}")
    for m in master_states(ctx):
        extra = f"workers={m['alive_workers']}" if "alive_workers" in m else m.get("error", "")
        print(f"  master     {m['name']:<11} {m['ip']:<15} {m['status']:<11} {extra}")
    for z in zookeeper_status(ctx):
        print(f"  zookeeper  {z['name']:<11} {z['ip']:<15} {z['mode']}")
    return 0


def cmd_tunnel(paths: ProjectPaths, args: argparse.Namespace) -> int:
    from clusterctl.tunnel import open_tunnels

    ctx = make_context(paths)
    open_tunnels(ctx)
    print("tunnels open; Ctrl+C to stop")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        return 0


def cmd_dashboard(paths: ProjectPaths, args: argparse.Namespace) -> int:
    from clusterctl.dashboard.app import serve

    return serve(paths, host="127.0.0.1", port=args.port)


# ------------------------------------------------------------------ parser

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="clusterctl", description="Provision and run an Apache Spark cluster")
    sub = p.add_subparsers(dest="command", required=True)

    def with_limit(sp: argparse.ArgumentParser) -> argparse.ArgumentParser:
        sp.add_argument("--limit", help="comma-separated host names (default: all)")
        return sp

    sp = sub.add_parser("init", help="create control key, vault, and inventory")
    sp.add_argument("--systems", help="import a systems.json credentials file")
    sp.add_argument("--master", help="master by name, IP, label or username (default: first system)")
    sp.add_argument("--jump", help="~/.ssh/config alias of a jump host")
    sp.set_defaults(fn=cmd_init)

    sp = sub.add_parser("import-systems", help="import systems.json into inventory + vault")
    sp.add_argument("systems")
    sp.add_argument("--master")
    sp.add_argument("--jump")
    sp.set_defaults(fn=cmd_import_systems)

    sp = sub.add_parser("inventory", help="list/add/remove/import hosts")
    isub = sp.add_subparsers(dest="action", required=True)
    isub.add_parser("list")
    a = isub.add_parser("add")
    a.add_argument("name"); a.add_argument("ip"); a.add_argument("user")
    a.add_argument("--role", default="worker", choices=inv.ROLES)
    r = isub.add_parser("remove"); r.add_argument("name")
    c = isub.add_parser("import"); c.add_argument("csv")
    c.add_argument("--delete-after", action="store_true", help="delete the CSV after import")
    sp.set_defaults(fn=cmd_inventory)

    sp = sub.add_parser("vault", help="manage encrypted passwords")
    vsub = sp.add_subparsers(dest="action", required=True)
    vsub.add_parser("init"); vsub.add_parser("list")
    s = vsub.add_parser("set"); s.add_argument("name")
    r = vsub.add_parser("remove"); r.add_argument("name")
    sp.set_defaults(fn=cmd_vault)

    sp = sub.add_parser("keys", help="SSH key registry")
    ksub = sp.add_subparsers(dest="action", required=True)
    ksub.add_parser("list"); ksub.add_parser("audit")
    r = ksub.add_parser("revoke"); r.add_argument("fingerprint")
    sp.set_defaults(fn=cmd_keys)

    for name, fn, text in (("bootstrap", cmd_bootstrap, "install SSH keys using vault passwords"),
                           ("provision", cmd_provision, "install Java + Spark + services"),
                           ("up", cmd_up, "bootstrap + provision + start + verify")):
        with_limit(sub.add_parser(name, help=text)).set_defaults(fn=fn)
    for name in ("start", "stop", "restart"):
        with_limit(sub.add_parser(name, help=f"{name} Spark services")).set_defaults(fn=cmd_service)

    sp = sub.add_parser("status", help="cluster health")
    sp.add_argument("--json", action="store_true")
    sp.set_defaults(fn=cmd_status)

    sp = sub.add_parser("data", help="copy an input file to every node")
    dsub = sp.add_subparsers(dest="action", required=True)
    d = dsub.add_parser("push"); d.add_argument("file")
    g = dsub.add_parser("generate", help="create corpus-<N>mb.txt on the master and copy to workers")
    g.add_argument("--size-mb", type=int, required=True)
    g.add_argument("--no-reference", action="store_true", help="skip the single-machine reference count")
    dsub.add_parser("clean", help="delete generated corpus-*mb.txt files from every node")
    sp.set_defaults(fn=cmd_data)

    sp = sub.add_parser("run", help="run Word Count on the cluster")
    sp.add_argument("--input", required=True, help="file name previously pushed with 'data push'")
    sp.add_argument("--partitions", type=int, default=0)
    sp.add_argument("--top", type=int, default=20)
    sp.set_defaults(fn=cmd_run)

    sp = sub.add_parser("bench", help="scaling benchmark: time vs workers and input size")
    sp.add_argument("--sizes", default="100,500,1000,2000", help="comma-separated MB sizes")
    sp.add_argument("--repeats", type=int, default=3)
    sp.add_argument("--max-workers", type=int)
    sp.set_defaults(fn=cmd_bench)

    sp = sub.add_parser("demo", help="failure-recovery demo")
    sp.add_argument("scenario", choices=["failure", "shuffle", "straggler", "cascade", "master-failover"],
                    help="failure: kill a busy worker | shuffle: kill during the shuffle read | "
                         "straggler: throttle a worker, speculative execution | cascade: two workers die")
    sp.add_argument("--input", default="corpus.txt", help="input for the 'failure' scenario")
    sp.add_argument("--mode", choices=["worker", "executor"], default="worker")
    sp.add_argument("--victim", help="worker name to kill (default: first busy worker)")
    sp.add_argument("--waves", type=int, default=4, help="partitions = cores x waves")
    sp.add_argument("--delay-ms", type=int, default=12000)
    sp.set_defaults(fn=cmd_demo)

    sub.add_parser("ha", help="master HA status: masters (ALIVE/STANDBY) and ZooKeeper modes"
                   ).set_defaults(fn=cmd_ha)

    sp = sub.add_parser("facts", help="hardware/OS facts per node (physical-machine proof)")
    sp.add_argument("--json", action="store_true")
    sp.set_defaults(fn=cmd_facts)
    sub.add_parser("monitor", help="live CPU/memory/Spark cores per node").set_defaults(fn=cmd_monitor)
    sub.add_parser("history", help="applications the master has run").set_defaults(fn=cmd_history)
    sub.add_parser("evidence", help="write an offline HTML evidence report to results/").set_defaults(fn=cmd_evidence)

    sub.add_parser("tunnel", help="forward Spark web UIs to localhost").set_defaults(fn=cmd_tunnel)

    sp = sub.add_parser("dashboard", help="local web dashboard")
    sp.add_argument("--port", type=int, default=8765)
    sp.set_defaults(fn=cmd_dashboard)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    paths = ProjectPaths.discover()
    try:
        return args.fn(paths, args)
    except (inv.InventoryError, VaultError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130
    except Exception as exc:  # noqa: BLE001 - top-level: show a clean message
        print(f"error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
