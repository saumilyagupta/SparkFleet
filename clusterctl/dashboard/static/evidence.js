// Evidence view: results first (cluster up, physical machines, Word Count correct,
// recovery from a node failure). Loaded after app.js and reuses its helpers
// (h, pill, td, api, state, followJob, banner, sysLog). DOM building only.
"use strict";

const SVG_NS = "http://www.w3.org/2000/svg";
const TOP_WORDS = 20;
const RESULT_JOBS = new Set(["run_wordcount", "demo_failure", "evidence"]);
const DEMO_INPUT = "corpus.txt";
const DEMO_PARTITIONS = 72;
const ev = { facts: null, factsError: "", status: null, statusError: "", latestRun: null };

// ------------------------------------------------------------------ helpers
const fmt = (n) => (n == null || Number.isNaN(Number(n)) ? "–" : Number(n).toLocaleString("en-US"));
const shortSpark = (raw) => ((/Spark\s+(\S+)/.exec(raw || "") || [])[1] || "");
const shortJava = (raw) => ((/version\s+"([^"]+)"/.exec(raw || "") || [])[1] || "");

function fileHref(kind, name) {
  return `/api/evidence/${kind}/${encodeURIComponent(name)}?token=${encodeURIComponent(state.token)}`;
}
function fileLink(text, kind, name) {
  return h("a", { class: "ev-link", href: fileHref(kind, name), target: "_blank", rel: "noopener noreferrer", text });
}
function svg(tag, attrs = {}, text) {
  const node = document.createElementNS(SVG_NS, tag);
  for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, String(v));
  if (text != null) node.textContent = text;
  return node;
}
function stat(label, value, kind = "") {
  return h("div", { class: `stat ${kind}` }, h("span", { class: "stat-v", text: value }), h("span", { class: "stat-l", text: label }));
}
function verdict(ok, yes, no) {
  if (ok == null) return h("span", { class: "verdict verdict-na", text: "– not checked against a reference" });
  return h("span", { class: `verdict ${ok ? "verdict-ok" : "verdict-bad"}`, text: ok ? `✓ ${yes}` : `✗ ${no}` });
}
function empty(text) { return h("p", { class: "ev-empty", text }); }
function nodeNameByIp(ip) {
  const all = [...(ev.facts || []), ...((state.inventory && state.inventory.hosts) || [])];
  const hit = all.find((x) => x.ip === ip);
  return hit ? hit.name : "";
}
function duration(s) {
  if (s == null) return "–";
  const sec = Number(s);
  return sec >= 60 ? `${Math.floor(sec / 60)}m ${Math.round(sec % 60)}s` : `${sec.toFixed(1)} s`;
}

// ------------------------------------------------------------------ 1. at a glance
function glanceTile(label, value, sub, kind = "") {
  const v = value instanceof Node ? value : h("span", { text: value });
  return h("div", { class: `tile ${kind}` }, h("span", { class: "tile-l", text: label }),
    h("span", { class: "tile-v" }, v), h("span", { class: "tile-s", text: sub }));
}

function renderGlance() {
  const m = (ev.status && ev.status.master) || {};
  const st = (m.status || (ev.statusError ? "UNREACHABLE" : "…")).toUpperCase();
  const masterKind = st === "ALIVE" ? "ok" : st === "…" ? "pending" : "down";
  const expected = state.inventory ? state.inventory.hosts.filter((x) => x.role === "worker").length : 0;
  const alive = m.alive_workers ? m.alive_workers.length : null;
  const facts = ev.facts || [];
  const reachable = facts.filter((n) => n.reachable);
  const physical = reachable.filter((n) => n.is_physical);
  const virts = [...new Set(reachable.map((n) => n.virt || "unknown"))].join(", ") || "–";
  const spark = shortSpark((reachable[0] || {}).spark) || "–";
  const java = shortJava((reachable[0] || {}).java) || "–";
  const allPhysical = facts.length > 0 && physical.length === facts.length;
  $("#ev-glance").replaceChildren(
    glanceTile("master", pill(st, masterKind), m.url || ev.statusError || "spark standalone master", masterKind),
    glanceTile("workers alive", alive == null ? "–" : `${alive}/${expected}`, "registered with the master",
      alive != null && alive === expected && expected > 0 ? "ok" : alive == null ? "" : "warn"),
    glanceTile("total cores", fmt(m.cores), "offered to Spark by workers"),
    glanceTile("memory", m.memory_mb != null ? `${(m.memory_mb / 1024).toFixed(1)} GB` : "–", "offered to Spark by workers"),
    glanceTile("physical machines", facts.length ? `${physical.length}/${facts.length}` : ev.factsError ? "?" : "…",
      facts.length ? `virt: ${virts}` : ev.factsError || "collecting facts over SSH", facts.length ? (allPhysical ? "ok" : "bad") : ""),
    glanceTile("spark version", spark, `Java ${java}`));
}

// ------------------------------------------------------------------ 2. physical nodes
function factRow(label, value) {
  return [h("dt", { text: label }), h("dd", { text: value || "–" })];
}

function nodeCard(n) {
  const head = h("div", { class: "node-head" }, h("strong", { class: "node-name", text: n.name }),
    pill(n.role || "?", n.role === "master" ? "pending" : "idle"));
  if (!n.reachable) {
    return h("article", { class: "node-card node-down" }, head, h("p", { class: "ip mono", text: n.ip }),
      pill("unreachable", "down"), h("p", { class: "muted small", text: n.error || "" }));
  }
  const bare = n.virt === "none";
  const virt = h("span", { class: `virt ${bare ? "virt-ok" : "virt-bad"}`, text: bare ? "bare metal" : `virtual: ${n.virt}` });
  const services = h("ul", { class: "node-svcs" }, ...(n.services || []).map((s) => h("li", {},
    pill(`${s.name.replace(/^spark-/, "")} ${s.state}`, s.state === "active" ? "ok" : "down"),
    h("span", { class: "svc-meta", text: `since ${s.since || "–"} · restarts ${s.restarts || "0"}` }))));
  return h("article", { class: "node-card" }, head,
    h("p", { class: "node-host mono" }, h("span", { text: n.hostname || "–" }), h("span", { class: "muted", text: ` · ${n.ip}` })),
    virt,
    h("dl", { class: "node-facts" },
      ...factRow("machine", n.product && n.vendor && n.product.startsWith(n.vendor) ? n.product
        : [n.vendor, n.product].filter(Boolean).join(" ")),
      ...factRow("cpu", `${n.cpu_model || "?"} × ${n.cpu_cores || "?"}`),
      ...factRow("ram", n.mem_gb != null ? `${n.mem_gb} GB` : ""),
      ...factRow("os", `${n.os || ""} · ${n.kernel || ""}`),
      ...factRow("booted", n.boot_time)),
    services);
}

function renderNodes() {
  const box = $("#ev-nodes");
  if (ev.factsError) { box.replaceChildren(empty(`Could not collect node facts: ${ev.factsError}`)); return; }
  if (!ev.facts) { box.replaceChildren(empty("Collecting hardware facts from every node over SSH…")); return; }
  box.replaceChildren(...ev.facts.map(nodeCard));
}

let factsBusy = false;
async function refreshFacts() {
  if (factsBusy) return;
  factsBusy = true;
  $("#ev-facts-refresh").disabled = true;
  ev.factsError = "";
  if (!ev.facts) renderNodes();
  try {
    ev.facts = (await api("GET", "/api/evidence/facts")).nodes;
    $("#ev-facts-at").textContent = `collected ${new Date().toLocaleTimeString([], { hour12: false })}`;
  } catch (e) {
    ev.factsError = e.message;
  } finally {
    factsBusy = false;
    $("#ev-facts-refresh").disabled = false;
    renderNodes();
    renderGlance();
    if (ev.latestRun) renderWordcount(ev.latestRun);   // executor IPs -> node names
  }
}

// ------------------------------------------------------------------ 3. word count
function barChart(top) {
  const rows = top.slice(0, TOP_WORDS);
  const rowH = 22, labelW = 64, valueW = 96, width = 640;
  const barMax = width - labelW - valueW;
  const max = Math.max(1, ...rows.map((r) => r.count));
  const chart = svg("svg", { viewBox: `0 0 ${width} ${rows.length * rowH + 4}`, class: "bars", role: "img",
    "aria-label": `Top ${rows.length} words by count` });
  rows.forEach((r, i) => {
    const y = i * rowH + 2;
    const w = Math.max(2, (r.count / max) * barMax);
    const g = svg("g", { class: "bar-row" });
    g.append(svg("title", {}, `${r.word}: ${fmt(r.count)}`),
      svg("text", { x: labelW - 8, y: y + rowH / 2, class: "bar-label", "text-anchor": "end", "dominant-baseline": "central" }, r.word),
      svg("rect", { x: labelW, y: y + 3, width: w.toFixed(1), height: rowH - 6, rx: 2, class: "bar" }),
      svg("text", { x: (labelW + w + 6).toFixed(1), y: y + rowH / 2, class: "bar-value", "dominant-baseline": "central" }, fmt(r.count)));
    chart.append(g);
  });
  return chart;
}

function executorTable(executors) {
  const byIp = new Map();
  for (const x of executors || []) byIp.set(x.ip, Math.max(byIp.get(x.ip) || 0, x.cores || 0));
  const body = h("tbody", {}, ...[...byIp].map(([ip, cores]) =>
    h("tr", {}, td(nodeNameByIp(ip) || "–"), td(ip), td(fmt(cores)))));
  return h("table", { class: "grid compact" },
    h("thead", {}, h("tr", {}, h("th", { text: "worker" }), h("th", { text: "ip" }), h("th", { text: "cores" }))), body);
}

function renderWordcount(run) {
  const box = $("#ev-wc");
  $("#ev-wc-badge").replaceChildren();
  if (!run) { box.replaceChildren(empty("No Word Count run saved yet. Press “Run Word Count now”.")); return; }
  const s = run.summary || {};
  $("#ev-wc-badge").append(verdict(run.matches_local_reference, "identical to single-machine count", "differs from single-machine count"));
  const links = h("p", { class: "ev-links" });
  if (run.result_file) links.append(fileLink(`open result ${run.result_file}`, "file", run.result_file));
  if (run.reference_file) links.append(fileLink("open single-machine reference", "file", run.reference_file));
  box.replaceChildren(h("div", { class: "ev-split" },
    h("div", { class: "ev-col" },
      h("div", { class: "stats" }, stat("total words", fmt(s.total_words)), stat("distinct words", fmt(s.distinct_words)),
        stat("partitions", fmt(s.partitions)), stat("elapsed", s.elapsed_s != null ? `${s.elapsed_s} s` : "–")),
      h("h3", { text: `executors (${(run.executors || []).length})` }), executorTable(run.executors),
      h("p", { class: "muted small mono", text: `${run.tag || ""} · ${s.app_id || ""} · input ${run.input || "?"} · ${run.started || ""}` }),
      links),
    h("div", { class: "ev-col" }, h("h3", { text: `top ${Math.min(TOP_WORDS, (run.top || []).length)} words` }),
      (run.top || []).length ? barChart(run.top) : empty("no top-word list in this run"))));
}

// ------------------------------------------------------------------ 4. failure recovery
function stepKind(event) {
  if (/identical to baseline: False|FAILED|differ/i.test(event)) return "bad";
  if (/stopped|kill|inject|no longer ALIVE|lost/i.test(event)) return "warn";
  if (/finished|identical|restarted|re-registered|recover/i.test(event)) return "ok";
  return "";
}

function coresChange(demo) {
  const base = (demo.timeline || []).map((x) => /(\d+) workers? \/ (\d+) cores/.exec(x.event)).find(Boolean);
  const victimIp = demo.victim && demo.victim.ip;
  const lost = (demo.executors || []).find((x) => x.ip === victimIp);
  if (!base || !lost) return null;
  const before = Number(base[2]);
  return `${before} → ${before - lost.cores}`;
}

function renderFailure(demo) {
  const box = $("#ev-fail");
  if (!demo) { box.replaceChildren(empty("No failure demo saved yet. Press “Run failure demo now”.")); return; }
  const who = demo.victim ? demo.victim.name : "a node";
  const what = demo.mode === "executor" ? `Executor on ${who} killed mid-job` : `Worker ${who} killed mid-job`;
  const ok = demo.identical === true;
  const headline = h("p", { class: `ev-headline ${ok ? "is-ok" : "is-bad"}` },
    h("span", { text: `${what} → job finished → result ` }),
    h("strong", { text: ok ? "identical ✓" : "DIFFERENT ✗" }));
  const tl = demo.timeline || [];
  const chips = h("div", { class: "stats" },
    stat("lost tasks", fmt(demo.lost_tasks), "warn"), stat("retried tasks", fmt(demo.retried_tasks), "ok"),
    stat("lost executors", fmt((demo.lost_executors || []).length), "warn"));
  const cores = coresChange(demo);
  if (cores) chips.append(stat("cores before → after", cores));
  chips.append(stat("job time", demo.summary && demo.summary.elapsed_s != null ? `${demo.summary.elapsed_s} s` : "–"),
    stat("demo total", tl.length ? `${tl[tl.length - 1].t} s` : "–"));
  const steps = h("ol", { class: "stepper" }, ...tl.map((x) =>
    h("li", { class: `step step-${stepKind(x.event) || "plain"}` },
      h("span", { class: "step-t mono", text: `+${Number(x.t).toFixed(1)}s` }), h("span", { class: "step-e", text: x.event }))));
  const logs = h("details", { class: "ev-logs" }, h("summary", { text: `key driver log lines (${(demo.key_log_lines || []).length})` }),
    h("pre", { class: "mono", text: (demo.key_log_lines || []).join("\n") }));
  const links = h("p", { class: "ev-links" });
  if (demo.report_file) links.append(fileLink("open demo report (.md)", "file", demo.report_file));
  if (demo.log_file) links.append(fileLink("open full driver log (.log)", "file", demo.log_file));
  if (demo.recovered && demo.recovered.file) links.append(fileLink("recovered result (.tsv)", "file", demo.recovered.file));
  if (demo.baseline && demo.baseline.file) links.append(fileLink("baseline result (.tsv)", "file", demo.baseline.file));
  box.replaceChildren(headline, h("div", { class: "ev-split" },
    h("div", { class: "ev-col" }, chips,
      h("p", { class: "muted small mono", text: `${demo.stamp || ""} · mode ${demo.mode || "?"} · ${fmt(demo.partitions)} partitions · victim ${who} ${demo.victim ? demo.victim.ip : ""}` }),
      links, logs),
    h("div", { class: "ev-col" }, h("h3", { text: "timeline" }), steps)));
}

// ------------------------------------------------------------------ 5 + 6. job history, reports
function renderReports(reports) {
  const list = $("#ev-reports");
  if (!reports.length) { list.replaceChildren(h("li", { class: "muted", text: "No report generated yet." })); return; }
  list.replaceChildren(...reports.map((r) => h("li", {}, fileLink(r.name, "report", r.name),
    h("span", { class: "muted small", text: ` ${r.modified || ""}` }))));
}

async function refreshResults() {
  try {
    const res = await api("GET", "/api/evidence/results");
    ev.latestRun = res.wordcount[0];
    renderWordcount(res.wordcount[0]);
    renderFailure(res.demos[0]);
    renderReports(res.reports || []);
  } catch (e) {
    $("#ev-wc").replaceChildren(empty(`Could not load results: ${e.message}`));
  }
}

async function refreshJobs() {
  const body = $("#ev-jobs tbody");
  try {
    const { apps } = await api("GET", "/api/evidence/jobs");
    if (!apps.length) { body.replaceChildren(h("tr", {}, h("td", { class: "empty", colspan: "6", text: "No applications since the master started." }))); return; }
    const maxCores = Math.max(...apps.map((a) => a.cores || 0));
    body.replaceChildren(...apps.map((a) => {
      const degraded = a.cores != null && a.cores < maxCores;  // null = History Server row, cores unknown
      const state_ = a.state === "FINISHED" ? pill("finished", "ok") : a.state === "RUNNING" ? pill("running", "running") : pill(a.state || "?", "down");
      return h("tr", { class: degraded ? "lost-worker" : "" }, td(a.id), td(a.name), td(state_),
        td(h("span", { text: fmt(a.cores) }), degraded ? h("span", { class: "tag-lost", text: "ran with a lost worker" }) : null),
        td(duration(a.duration_s)), td(a.started ? new Date(a.started).toLocaleString("en-GB", { hour12: false }) : "–"));
    }));
  } catch (e) {
    body.replaceChildren(h("tr", {}, h("td", { class: "empty", colspan: "6", text: `Job history unavailable: ${e.message}` })));
  }
}

// ------------------------------------------------------------------ actions + wiring
async function startEvidenceJob(name, params, question) {
  if (question && !confirm(question)) return;
  try {
    const { job_id } = await api("POST", `/api/actions/${name}`, params);
    sysLog(`▶ ${name}`);
    followJob(job_id, name);
  } catch (e) { banner(`${name}: ${e.message}`); }
}

function wireEvidence() {
  $("#ev-facts-refresh").addEventListener("click", refreshFacts);
  $("#ev-run-wc").addEventListener("click", () =>
    startEvidenceJob("run_wordcount", { input: DEMO_INPUT, partitions: DEMO_PARTITIONS }));
  $("#ev-run-demo").addEventListener("click", () =>
    startEvidenceJob("demo_failure", { input: DEMO_INPUT }, "Run the failure demo? It deliberately stops a worker mid-job, then restarts it."));
  $("#ev-generate").addEventListener("click", () => startEvidenceJob("evidence", {}));
  $("#ev-jobs-refresh").addEventListener("click", refreshJobs);
  document.addEventListener("status:update", (e) => {
    ev.status = e.detail.status;
    ev.statusError = e.detail.error || "";
    renderGlance();
  });
  document.addEventListener("job:start", (e) => { $("#ev-demo-hint").hidden = e.detail.name !== "demo_failure"; });
  document.addEventListener("job:end", (e) => {
    $("#ev-demo-hint").hidden = true;
    if (RESULT_JOBS.has(e.detail.name)) { refreshResults(); refreshJobs(); }
  });
}

document.addEventListener("DOMContentLoaded", () => {
  wireEvidence();
  renderGlance();
  if (!state.token) return;
  refreshResults();
  refreshJobs();
  refreshFacts();
});
