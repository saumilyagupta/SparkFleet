"""Offline evidence report: one self-contained HTML file proving the cluster works.

Sections: summary, topology, physical nodes, Word Count result, scaling benchmark, failure recovery,
job history, setup proof. No JavaScript, no external assets; prints cleanly to PDF.
Contains NO secrets: no passwords, no private keys (fingerprints only).
"""

from __future__ import annotations

import math
from datetime import datetime
from html import escape
from pathlib import Path

from clusterctl import scaling
from clusterctl.cluster_ops import job_history, status
from clusterctl.context import Context
from clusterctl.facts import collect_facts
from clusterctl.results import results_index
from clusterctl.paths import TEMPLATES_DIR


def e(value: object) -> str:
    return escape(str(value), quote=True)


def _safe(fn, fallback):
    try:
        return fn()
    except Exception as exc:  # noqa: BLE001 - a report section must not kill the whole report
        return fallback(exc)


# ------------------------------------------------------------------ sections

def _summary(st: dict, nodes: list[dict]) -> str:
    m = st.get("master", {})
    alive = len(m.get("alive_workers", []))
    workers = sum(1 for n in nodes if n.get("role") == "worker")
    physical = sum(1 for n in nodes if n.get("is_physical"))
    mem_gb = round((m.get("memory_mb") or 0) / 1024, 1)
    ok = m.get("status") == "ALIVE" and alive == workers
    chips = [
        ("Master", m.get("status", "?"), ok),
        ("Workers alive", f"{alive}/{workers}", alive == workers),
        ("Cores", m.get("cores", "?"), True),
        ("Memory", f"{mem_gb} GB", True),
        ("Physical machines", f"{physical}/{len(nodes)}", physical == len(nodes)),
    ]
    return "<div class='chips'>" + "".join(
        f"<div class='chip {'good' if good else 'bad'}'><span>{e(k)}</span><b>{e(v)}</b></div>"
        for k, v, good in chips) + "</div>"


def _topology(ctx: Context) -> str:
    inv = ctx.inventory
    master = inv.master
    workers = list(inv.workers)
    width, wx = 900, 640
    height = max(260, 90 + 80 * len(workers))
    jump = inv.cluster.jump_host
    parts = [f"<svg viewBox='0 0 {width} {height}' role='img' aria-label='Cluster topology'>"]

    def box(x: int, y: int, w: int, title: str, sub: str, cls: str) -> None:
        parts.append(f"<rect x='{x}' y='{y}' width='{w}' height='56' rx='8' class='{cls}'/>"
                     f"<text x='{x + 12}' y='{y + 24}' class='t1'>{e(title)}</text>"
                     f"<text x='{x + 12}' y='{y + 43}' class='t2'>{e(sub)}</text>")

    my = height // 2 - 28
    box(10, my, 150, "Laptop (control)", "clusterctl", "n-ctl")
    if jump:
        box(190, my, 160, "Jump host", jump, "n-jump")
        parts.append(f"<line x1='160' y1='{my + 28}' x2='190' y2='{my + 28}' class='l-ssh'/>")
    mx = 390
    box(mx, my, 200, f"{master.name} · MASTER", f"{master.ip}:7077 + driver", "n-master")
    parts.append(f"<line x1='{350 if jump else 160}' y1='{my + 28}' x2='{mx}' y2='{my + 28}' class='l-ssh'/>")
    for i, w in enumerate(workers):
        y = 30 + i * 80
        box(wx, y, 240, f"{w.name} · worker", w.ip, "n-worker")
        parts.append(f"<line x1='{mx + 200}' y1='{my + 28}' x2='{wx}' y2='{y + 28}' class='l-spark'/>")
    parts.append("</svg>")
    return "".join(parts)


def _nodes(nodes: list[dict]) -> str:
    rows = []
    for n in nodes:
        if not n.get("reachable"):
            rows.append(f"<tr><td>{e(n['name'])}</td><td>{e(n['ip'])}</td><td colspan='7' class='bad'>"
                        f"unreachable: {e(n.get('error', ''))}</td></tr>")
            continue
        svc = "; ".join(f"{s['name']} {s['state']} since {s['since']} (restarts {s['restarts']}, {s['enabled']})"
                        for s in n.get("services", []))
        virt = f"<span class='{'pill good' if n['is_physical'] else 'pill bad'}'>{e(n['virt'])}</span>"
        rows.append(
            f"<tr><td><b>{e(n['name'])}</b><br><small>{e(n['role'])}</small></td><td>{e(n['ip'])}<br>"
            f"<small>{e(n['hostname'])}</small></td><td>{e(n['vendor'])} {e(n['product'])}</td>"
            f"<td>{e(n['cpu_model'])}<br><small>{e(n['cpu_cores'])} cores</small></td><td>{e(n['mem_gb'])} GB</td>"
            f"<td>{virt}</td><td>{e(n['os'])}<br><small>kernel {e(n['kernel'])}</small></td>"
            f"<td><small>{e(n['java'])}<br>{e(n['spark'])}</small></td><td><small>{e(svc)}</small></td></tr>")
    return ("<table><thead><tr><th>Node</th><th>IP / hostname</th><th>Hardware</th><th>CPU</th><th>RAM</th>"
            "<th>Virtualization</th><th>OS</th><th>Java / Spark</th><th>Service</th></tr></thead><tbody>"
            + "".join(rows) + "</tbody></table>"
            "<p class='note'><code>systemd-detect-virt</code> prints <b>none</b> only on bare metal; a VM or "
            "container would report kvm, vmware, oracle, docker, etc. Hardware identity is read from DMI "
            "(<code>/sys/class/dmi/id</code>).</p>")


def _bars(top: list[dict]) -> str:
    if not top:
        return "<p>No word counts recorded.</p>"
    top = top[:20]
    peak = max(t["count"] for t in top) or 1
    row, label_w, bar_w = 22, 90, 560
    parts = [f"<svg viewBox='0 0 {label_w + bar_w + 110} {row * len(top) + 6}' role='img' "
             "aria-label='Top 20 words by count'>"]
    for i, t in enumerate(top):
        y, w = i * row + 3, max(2, int(bar_w * t["count"] / peak))
        parts.append(f"<text x='{label_w - 8}' y='{y + 14}' class='bl' text-anchor='end'>{e(t['word'])}</text>"
                     f"<rect x='{label_w}' y='{y}' width='{w}' height='{row - 6}' rx='3' class='bar'/>"
                     f"<text x='{label_w + w + 6}' y='{y + 14}' class='bv'>{t['count']:,}</text>")
    parts.append("</svg>")
    return "".join(parts)


def _wordcount(run: dict | None) -> str:
    if not run:
        return "<p>No Word Count run recorded yet. Run <code>clusterctl run --input corpus.txt</code>.</p>"
    s = run.get("summary", {})
    match = run.get("matches_local_reference")
    verdict = ("<div class='verdict good'>✓ Cluster output is byte-identical to a single-machine count "
               f"(SHA-256 {e(run['result_sha256'][:16])}…)</div>") if match else (
        "<div class='verdict bad'>✗ Output differs from the single-machine count</div>" if match is False
        else "<div class='verdict'>No single-machine reference available.</div>")
    execs = "".join(f"<tr><td>{e(x['ip'])}</td><td>{e(x['id'])}</td><td>{e(x['cores'])}</td></tr>"
                    for x in run.get("executors", []))
    stages = "".join(f"<tr><td>{e(x['stage'])}</td><td>{e(x['op'])}</td><td>{e(x['seconds'])} s</td></tr>"
                     for x in run.get("stages", []))
    return (f"<p>Run <code>{e(run['tag'])}</code> started {e(run.get('started', ''))}, Spark application "
            f"<code>{e(s.get('app_id', ''))}</code>, driver on <code>{e(s.get('driver', ''))}</code>.</p>"
            "<div class='chips'>"
            f"<div class='chip'><span>Total words</span><b>{s.get('total_words', 0):,}</b></div>"
            f"<div class='chip'><span>Distinct words</span><b>{e(s.get('distinct_words', '?'))}</b></div>"
            f"<div class='chip'><span>Partitions</span><b>{e(s.get('partitions', '?'))}</b></div>"
            f"<div class='chip'><span>Executors</span><b>{e(s.get('executors', '?'))}</b></div>"
            f"<div class='chip'><span>Elapsed</span><b>{e(s.get('elapsed_s', '?'))} s</b></div></div>"
            + verdict + "<h3>Top 20 words</h3>" + _bars(run.get("top", []))
            + "<div class='two'><div><h3>Executors (one per worker)</h3><table><thead><tr><th>Worker IP</th>"
            f"<th>Executor</th><th>Cores</th></tr></thead><tbody>{execs}</tbody></table></div>"
            "<div><h3>Stages</h3><table><thead><tr><th>Stage</th><th>Operation</th><th>Time</th></tr></thead>"
            f"<tbody>{stages}</tbody></table></div></div>")


def _demo(demo: dict | None) -> str:
    if not demo:
        return "<p>No failure demo recorded yet. Run <code>clusterctl demo failure --input corpus.txt</code>.</p>"
    v = demo.get("victim", {})
    same = demo.get("identical")
    steps = "".join(
        f"<li class='{'kill' if ('stopped' in t['event'] or 'injecting' in t['event']) else ''}'>"
        f"<span>+{e(t['t'])} s</span>{e(t['event'])}</li>" for t in demo.get("timeline", []))
    lines = "\n".join(demo.get("key_log_lines", []))
    return (f"<p class='lead'>Worker <b>{e(v.get('name'))} ({e(v.get('ip'))})</b> was killed while running "
            f"tasks. Spark lost <b>{e(demo.get('lost_tasks'))}</b> tasks, re-ran <b>{e(demo.get('retried_tasks'))}"
            "</b> of them on the surviving workers, and the job finished.</p>"
            + (f"<div class='verdict good'>✓ Result with the failure is identical to the clean baseline "
               f"(SHA-256 {e(demo['recovered']['sha256'][:16])}… = {e(demo['baseline']['sha256'][:16])}…)</div>"
               if same else "<div class='verdict bad'>✗ Result differs from baseline</div>")
            + f"<h3>Timeline (demo {e(demo.get('stamp'))})</h3><ol class='steps'>{steps}</ol>"
            f"<h3>Driver log: detection and recovery</h3><pre>{e(lines)}</pre>"
            "<p class='note'>How: executor JVM death closes its RPC connection → the scheduler marks the "
            "executor lost → each running task fails with <code>ExecutorLostFailure</code> and is retried as "
            "attempt <code>.1</code> on a live executor. RDD lineage lets Spark recompute those partitions "
            "from the input, which every node holds. Word Count is deterministic, so the answer is unchanged.</p>")


def _speedup_chart(bench: dict) -> str:
    """Inline SVG: speedup vs workers, one polyline per input size plus the dashed ideal y = x."""
    ks = scaling.worker_counts(bench) or [1]
    k0, k1 = min(ks), max(ks)
    peak = max([k1, 1.0] + [p[1] for s in scaling.sizes(bench) for p in scaling.series(bench, s)])
    y_max = max(1, math.ceil(peak))
    width, height, left, right, top, bottom = 640, 300, 52, 20, 18, 46
    plot_w, plot_h = width - left - right, height - top - bottom

    def x(k: float) -> float:
        return left + plot_w / 2 if k1 == k0 else left + (k - k0) / (k1 - k0) * plot_w

    def y(v: float) -> float:
        return top + (1 - min(v, y_max) / y_max) * plot_h

    parts = [f"<svg viewBox='0 0 {width} {height}' class='sc-chart' role='img' "
             "aria-label='Speedup versus number of workers, one line per input size'>"]
    step = 1 if y_max <= 6 else 2
    for v in range(0, y_max + 1, step):
        parts.append(f"<line x1='{left}' y1='{y(v):.1f}' x2='{width - right}' y2='{y(v):.1f}' class='sc-grid'/>"
                     f"<text x='{left - 8}' y='{y(v) + 4:.1f}' class='sc-tick' text-anchor='end'>{v}×</text>")
    for k in ks:
        parts.append(f"<text x='{x(k):.1f}' y='{height - bottom + 18}' class='sc-tick' "
                     f"text-anchor='middle'>{e(k)}</text>")
    parts.append(f"<text x='{left + plot_w / 2:.1f}' y='{height - 6}' class='sc-axis' text-anchor='middle'>"
                 "workers</text><text x='14' y='{0:.1f}' class='sc-axis' text-anchor='middle' "
                 "transform='rotate(-90 14 {0:.1f})'>speedup vs 1 worker</text>".format(top + plot_h / 2))
    parts.append(f"<line x1='{x(k0):.1f}' y1='{y(k0):.1f}' x2='{x(k1):.1f}' y2='{y(k1):.1f}' class='sc-ideal'/>")
    for i, size in enumerate(scaling.sizes(bench)):
        pts = scaling.series(bench, size)
        coords = " ".join(f"{x(k):.1f},{y(v):.1f}" for k, v in pts)
        parts.append(f"<polyline points='{coords}' class='sc-line s{i % 6}'/>")
        parts.extend(f"<circle cx='{x(k):.1f}' cy='{y(v):.1f}' r='3.5' class='sc-dot s{i % 6}'>"
                     f"<title>{e(size)} MB, {e(k)} workers: {v:.2f}×</title></circle>" for k, v in pts)
    legend = [(f"{size} MB", f"sc-line s{i % 6}") for i, size in enumerate(scaling.sizes(bench))]
    legend.append(("ideal (linear)", "sc-ideal"))
    for i, (label, cls) in enumerate(legend):
        ly = top + 12 + i * 18
        parts.append(f"<line x1='{left + 12}' y1='{ly}' x2='{left + 34}' y2='{ly}' class='{cls}'/>"
                     f"<text x='{left + 40}' y='{ly + 4}' class='sc-legend'>{e(label)}</text>")
    parts.append("</svg>")
    return "".join(parts)


def _scaling(bench: dict | None) -> str:
    if not bench:
        return ("<p>Scaling benchmark not run yet. Run <code>clusterctl bench</code> to time Word Count on "
                "1, 2 and 3 workers for several input sizes.</p>")
    ks = scaling.worker_counts(bench)
    ok = scaling.all_correct(bench)
    verdict = ("<div class='verdict good'>✓ Every run matched the single-machine count</div>" if ok else
               "<div class='verdict'>Correctness not checked against a single-machine reference.</div>"
               if ok is None else "<div class='verdict bad'>✗ Some runs differed from the single-machine count "
               f"(input sizes: {e(', '.join(scaling.failed_sizes(bench)))} MB)</div>")
    head = "".join(f"<th>{e(k)} worker{'s' if k != 1 else ''}</th>" for k in ks)
    rows = []
    for size in scaling.sizes(bench):
        cells = []
        for k in ks:
            r = scaling.row(bench, size, k)
            cells.append("<td>–</td>" if r is None else
                         f"<td><b>{float(r['median_s']):.1f} s</b><br><small>{float(r['speedup']):.2f}× · "
                         f"{round(100 * float(r['efficiency']))}% · {float(r['throughput_mb_s']):.1f} MB/s</small></td>")
        rows.append(f"<tr><td><b>{e(size)} MB</b></td>{''.join(cells)}</tr>")
    return (f"<p class='lead'>{e(scaling.interpretation(bench))}</p>" + verdict
            + "<h3>Speedup vs workers</h3>" + _speedup_chart(bench)
            + "<h3>Median time · speedup · efficiency · throughput</h3>"
            f"<table class='sc-table'><thead><tr><th>Input</th>{head}</tr></thead><tbody>{''.join(rows)}</tbody>"
            f"</table><p class='note'>Run {e(bench.get('stamp', '?'))}: {e(bench.get('repeats', '?'))} repeat(s) "
            f"per cell (median shown), {e(bench.get('cores_per_worker', '?'))} cores per worker. Speedup = time on "
            "1 worker ÷ time on k workers; efficiency = speedup ÷ k. Small inputs scale poorly because Spark's "
            "fixed start-up cost (JVMs, scheduling) is a large share of a short job.</p>")


def _jobs(history: dict) -> str:
    apps = history.get("apps", [])
    if not apps:
        return f"<p>{e(history.get('error', 'No applications recorded.'))}</p>"
    peak = max((a.get("cores") or 0) for a in apps)
    rows = "".join(
        f"<tr class='{'degraded' if a.get('cores') is not None and a['cores'] < peak else ''}'><td><code>{e(a['id'])}</code></td>"
        f"<td>{e(a['name'])}</td><td>{e(a['state'])}</td><td>{e(a['cores'] if a.get('cores') is not None else '–')}</td><td>{e(a['duration_s'])} s</td>"
        f"<td>{e(datetime.fromtimestamp(a['started'] / 1000).strftime('%Y-%m-%d %H:%M:%S') if a.get('started') else '–')}</td></tr>"
        for a in apps)
    return ("<table><thead><tr><th>Application</th><th>Name</th><th>State</th><th>Cores</th><th>Duration</th>"
            f"<th>Started</th></tr></thead><tbody>{rows}</tbody></table>"
            f"<p class='note'>Highlighted rows ran with fewer than {peak} cores: those are the failure-demo runs "
            "that lost a worker mid-job and still finished.</p>")


def _setup(ctx: Context, spark_conf: str) -> str:
    keys = "".join(f"<tr><td>{e(r.kind)}</td><td>{e(r.owner)}</td><td><code>{e(r.fingerprint)}</code></td>"
                   f"<td>{e(', '.join(r.installed_on) or '-')}</td><td>{e(r.status)}</td></tr>"
                   for r in ctx.registry.records())
    return ("<h3>SSH key registry (public fingerprints only)</h3><table><thead><tr><th>Kind</th><th>Owner</th>"
            f"<th>Fingerprint</th><th>Installed on</th><th>Status</th></tr></thead><tbody>{keys}</tbody></table>"
            f"<h3>spark-defaults.conf on the master</h3><pre>{e(spark_conf)}</pre>")


# ------------------------------------------------------------------ assembly

def _master_conf(ctx: Context) -> str:
    with ctx.session(ctx.inventory.master) as s:
        return s.read_text(f"{ctx.inventory.cluster.spark_home}/conf/spark-defaults.conf")


def build_html(ctx: Context) -> str:
    generated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    st = _safe(lambda: status(ctx), lambda exc: {"master": {"status": f"UNREACHABLE ({exc})"}})
    nodes = _safe(lambda: collect_facts(ctx), lambda exc: [])
    history = _safe(lambda: job_history(ctx), lambda exc: {"apps": [], "error": str(exc)})
    index = results_index(ctx.paths)
    conf = _safe(lambda: _master_conf(ctx), lambda exc: f"(unavailable: {exc})")
    css = (TEMPLATES_DIR / "evidence.css").read_text(encoding="utf-8")
    sections = [
        ("summary", "1. Cluster status", _summary(st, nodes)),
        ("topology", "2. Topology", _topology(ctx)),
        ("nodes", "3. Physical machines", _nodes(nodes)),
        ("wordcount", "4. Word Count on the cluster", _wordcount((index["wordcount"] or [None])[0])),
        ("scaling", "5. Scaling benchmark", _scaling((index["bench"] or [None])[0])),
        ("recovery", "6. Failure recovery", _demo((index["demos"] or [None])[0])),
        ("history", "7. Applications run on the cluster", _jobs(history)),
        ("setup", "8. Setup proof", _setup(ctx, conf)),
    ]
    body = "".join(f"<section id='{sid}'><h2>{e(title)}</h2>{html}</section>" for sid, title, html in sections)
    return ("<!doctype html><html lang='en'><head><meta charset='utf-8'>"
            "<meta name='viewport' content='width=device-width,initial-scale=1'>"
            f"<title>Spark Cluster Evidence</title><style>{css}</style></head><body><main>"
            "<header><p class='kicker'>IBD group project · Team 2</p><h1>Apache Spark cluster on 4 physical "
            f"machines</h1><p class='meta'>Evidence generated live from the cluster at {e(generated)} by "
            "<code>clusterctl evidence</code>.</p></header>" + body + "</main></body></html>")


def generate_report(ctx: Context) -> Path:
    html = build_html(ctx)
    out = ctx.paths.results_dir / f"evidence-{datetime.now():%Y%m%d-%H%M%S}.html"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8")
    ctx.reporter.log("-", f"evidence report written: {out}", "ok")
    return out
