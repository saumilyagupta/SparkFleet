// clusterctl dashboard. Plain JS, no build step. Server data is only ever
// rendered through textContent / DOM nodes (never innerHTML).
"use strict";

const STATUS_REFRESH_MS = 10000;
const MAX_LOG_LINES = 2000;
const TOKEN_KEY = "clusterctl.token";
const LIMITABLE = new Set(["bootstrap", "provision", "up", "start", "stop", "restart"]);
const CONFIRM = {
  stop: "Stop Spark services on the selected hosts?",
  restart: "Restart Spark services on the selected hosts?",
  demo_failure: "Run the failure demo? It deliberately kills a worker or executor.",
};

const $ = (sel) => document.querySelector(sel);
const state = { token: "", inventory: null, status: null, selected: new Set(), job: null, source: null };

// ------------------------------------------------------------------ token + api
function initToken() {
  const url = new URL(window.location.href);
  const fromUrl = url.searchParams.get("token");
  if (fromUrl) {
    try { sessionStorage.setItem(TOKEN_KEY, fromUrl); } catch (_) { /* storage blocked */ }
    url.searchParams.delete("token");
    history.replaceState(null, "", url.pathname + url.search + url.hash);
  }
  let stored = "";
  try { stored = sessionStorage.getItem(TOKEN_KEY) || ""; } catch (_) { /* storage blocked */ }
  state.token = fromUrl || stored;
  if (!state.token) banner("No access token. Open the URL printed by 'clusterctl dashboard'.");
}

async function api(method, path, body) {
  const opts = { method, headers: { "X-Token": state.token } };
  if (body instanceof FormData) opts.body = body;
  else if (body !== undefined) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(path, opts);
  let data = {};
  try { data = await res.json(); } catch (_) { /* empty body */ }
  if (res.status === 401) banner("Access token rejected. Re-open the URL printed by 'clusterctl dashboard'.");
  if (!res.ok) throw new Error(data.error || `${res.status} ${res.statusText}`);
  return data;
}

function banner(text, ok = false) {
  const el = $("#banner");
  el.textContent = text;
  el.classList.toggle("ok", ok);
  el.hidden = !text;
  if (ok) setTimeout(() => { if (el.textContent === text) el.hidden = true; }, 4000);
}

// ------------------------------------------------------------------ dom helpers
function h(tag, props = {}, ...children) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(props)) {
    if (k === "class") node.className = v;
    else if (k === "text") node.textContent = v;
    else if (k.startsWith("on")) node.addEventListener(k.slice(2), v);
    else node.setAttribute(k, v);
  }
  for (const c of children) if (c != null) node.append(c instanceof Node ? c : String(c));
  return node;
}
const pill = (text, kind) => h("span", { class: `pill pill-${kind}`, text });
const td = (...c) => h("td", {}, ...c);

// ------------------------------------------------------------------ log pane
function logLine(ev) {
  const log = $("#log");
  const t = new Date((ev.t || Date.now() / 1000) * 1000).toLocaleTimeString([], { hour12: false });
  const level = ["ok", "warn", "error", "info", "sys"].includes(ev.level) ? ev.level : "info";
  log.append(h("li", { class: `lv-${level}` },
    h("span", { class: "t", text: t }),
    h("span", { class: "h", text: ev.host && ev.host !== "-" ? ev.host : "·" }),
    h("span", { class: "m", text: ev.msg })));
  while (log.childElementCount > MAX_LOG_LINES) log.firstElementChild.remove();
  if ($("#log-follow").checked) log.scrollTop = log.scrollHeight;
}
const sysLog = (msg, level = "sys") => logLine({ t: Date.now() / 1000, host: "ui", level, msg });

// ------------------------------------------------------------------ vault
async function refreshVault() {
  const v = await api("GET", "/api/vault");
  const p = v.unlocked ? pill("unlocked", "ok") : pill(v.exists ? "locked" : "no vault", "pending");
  p.id = "vault-state";
  $("#vault-state").replaceWith(p);
  $("#vault-create-row").hidden = v.exists;
  return v;
}

async function unlockVault(ev) {
  ev.preventDefault();
  const pass = $("#vault-pass");
  try {
    await api("POST", "/api/vault/unlock", { password: pass.value, create: $("#vault-create").checked });
    banner("Vault unlocked", true);
  } catch (e) { banner(`Vault: ${e.message}`); }
  pass.value = "";
  await refreshVault();
  await refreshInventory();
}

// ------------------------------------------------------------------ hosts
async function refreshInventory() {
  state.inventory = await api("GET", "/api/inventory");
  $("#jump-label").textContent = state.inventory.jump_host ? `via ${state.inventory.jump_host}` : "direct";
  const victim = $("#p-victim");
  const keep = victim.value;
  victim.replaceChildren(h("option", { value: "", text: "auto (busy worker)" }),
    ...state.inventory.hosts.filter((x) => x.role === "worker").map((x) => h("option", { value: x.name, text: x.name })));
  victim.value = keep;
  renderHosts();
  renderTunnelLinks();
}

function serviceCell(services) {
  const entries = Object.entries(services || {});
  if (!entries.length) return "–";
  return h("span", {}, ...entries.map(([name, st]) =>
    h("span", { class: "svc" }, pill(`${name.replace(/^spark-/, "")} ${st}`, st === "active" ? "ok" : "down"))));
}

function sparkVersion(raw) {
  const m = /Spark\s+(\S+)/.exec(raw || "");
  const node = h("span", { text: m ? m[1] : raw || "–" });
  if (raw) node.title = raw;
  return node;
}

function renderHosts() {
  const body = $("#hosts tbody");
  const hosts = state.inventory ? state.inventory.hosts : [];
  const live = new Map(((state.status && state.status.hosts) || []).map((x) => [x.name, x]));
  for (const name of [...state.selected]) if (!hosts.some((x) => x.name === name)) state.selected.delete(name);
  if (!hosts.length) {
    body.replaceChildren(h("tr", {}, h("td", { class: "empty", colspan: "11", text: "No hosts yet. Add one or import systems.json." })));
    return;
  }
  body.replaceChildren(...hosts.map((host) => {
    const s = live.get(host.name);
    const box = h("input", { type: "checkbox", "aria-label": `select ${host.name}` });
    box.checked = state.selected.has(host.name);
    const row = h("tr", { class: box.checked ? "is-selected" : "" });
    box.addEventListener("change", () => {
      if (box.checked) state.selected.add(host.name); else state.selected.delete(host.name);
      row.classList.toggle("is-selected", box.checked);
    });
    const reach = !s ? pill("?", "idle") : s.reachable ? pill("up", "ok") : pill("down", "down");
    if (s && s.error) reach.title = s.error;
    const worker = !s || host.role === "master" ? "–" : s.worker_alive ? pill("alive", "ok") : pill("dead", "down");
    const pw = host.has_password === false ? pill("no pw", "pending") : null;
    row.append(td(box), h("td", { class: "nowrap" }, h("strong", { text: host.name }), " ", pw), td(host.ip), td(host.user),
      td(host.role === "master" ? pill("master", "idle") : "worker"), td(reach),
      td(s ? s.java : "–"), td(s ? sparkVersion(s.spark) : "–"), td(s ? serviceCell(s.services) : "–"), td(worker),
      td(h("div", { class: "row-actions" },
        h("button", { class: "btn btn-sm btn-ghost", type: "button", text: "Set pw", onclick: () => setPassword(host) }),
        h("button", { class: "btn btn-sm btn-danger", type: "button", text: "Delete", onclick: () => deleteHost(host) }))));
    return row;
  }));
}

async function addHost(ev) {
  ev.preventDefault();
  const form = ev.target;
  const data = Object.fromEntries(new FormData(form));
  if (!data.password) delete data.password;
  try {
    const r = await api("POST", "/api/hosts", data);
    banner(`Added ${r.host.name}${r.password_stored ? " (password stored in vault)" : ""}`, true);
    form.reset();
    await refreshInventory();
  } catch (e) { banner(`Add host: ${e.message}`); }
  form.elements.password.value = "";
}

async function deleteHost(host) {
  if (!confirm(`Remove ${host.name} (${host.ip}) from the inventory? Its vault entry is kept.`)) return;
  try { await api("DELETE", `/api/hosts/${encodeURIComponent(host.name)}`); await refreshInventory(); }
  catch (e) { banner(`Delete: ${e.message}`); }
}

async function setPassword(host) {
  const pw = prompt(`New password for ${host.user}@${host.ip} (stored encrypted in the vault):`);
  if (!pw) return;
  try {
    await api("POST", `/api/hosts/${encodeURIComponent(host.name)}/password`, { password: pw });
    banner(`Password stored for ${host.name}`, true);
    await refreshInventory();
  } catch (e) { banner(`Set password: ${e.message}`); }
}

async function importSystems(ev) {
  ev.preventDefault();
  const form = ev.target;
  try {
    const r = await api("POST", "/api/import-systems", new FormData(form));
    banner(`Imported ${r.hosts.length} hosts, master ${r.master}, ${r.passwords_stored} passwords stored`, true);
    form.reset();
    await refreshInventory();
  } catch (e) { banner(`Import: ${e.message}`); }
}

// ------------------------------------------------------------------ status
let statusBusy = false;
async function refreshStatus() {
  if (statusBusy || !state.inventory || !state.inventory.hosts.length) return;
  statusBusy = true;
  $("#btn-refresh").disabled = true;
  try {
    state.status = await api("GET", "/api/status");
    renderMaster(state.status.master);
    renderHosts();
    $("#refresh-at").textContent = new Date().toLocaleTimeString([], { hour12: false });
  } catch (e) {
    renderMaster({ status: "UNKNOWN", error: e.message });
  } finally {
    statusBusy = false;
    $("#btn-refresh").disabled = false;
  }
}

function renderMaster(m) {
  const st = (m.status || "UNKNOWN").toUpperCase();
  const kind = st === "ALIVE" ? "ok" : st === "UNREACHABLE" || st === "UNKNOWN" ? "down" : "pending";
  const p = pill(st, kind);
  p.id = "m-status";
  if (m.error) p.title = m.error;
  $("#m-status").replaceWith(p);
  const expected = state.inventory ? state.inventory.hosts.filter((x) => x.role === "worker").length : 0;
  $("#m-workers").textContent = m.alive_workers ? `${m.alive_workers.length}/${expected}` : "–";
  $("#m-cores").textContent = m.cores ?? "–";
  $("#m-memory").textContent = m.memory_mb != null ? `${(m.memory_mb / 1024).toFixed(1)} GB` : "–";
}

// ------------------------------------------------------------------ jobs
function actionParams(name) {
  const num = (id) => Number($(id).value);
  const params = {};
  if (LIMITABLE.has(name) && state.selected.size) params.limit = [...state.selected];
  if (name === "push_data") params.path = $("#p-path").value.trim();
  if (name === "run_wordcount") Object.assign(params, { input: $("#p-input").value.trim(), partitions: num("#p-partitions"), top: num("#p-top") });
  if (name === "demo_failure") {
    Object.assign(params, { input: $("#p-input").value.trim(), mode: $("#p-mode").value, waves: num("#p-waves"), delay_ms: num("#p-delay") });
    if ($("#p-victim").value) params.victim = $("#p-victim").value;
  }
  return params;
}

async function runAction(name) {
  const scope = LIMITABLE.has(name) && state.selected.size ? ` on ${[...state.selected].join(", ")}` : "";
  if (CONFIRM[name] && !confirm(CONFIRM[name] + (scope ? `\n(${scope.trim()})` : "\n(all hosts)"))) return;
  try {
    const { job_id } = await api("POST", `/api/actions/${name}`, actionParams(name));
    sysLog(`▶ ${name}${scope}`);
    followJob(job_id, name);
  } catch (e) { banner(`${name}: ${e.message}`); }
}

function setJobState(text, kind) {
  const p = pill(text, kind);
  p.id = "job-state";
  $("#job-state").replaceWith(p);
  document.querySelectorAll("[data-action]").forEach((b) => { b.disabled = kind === "running"; });
}

function followJob(id, name) {
  if (state.source) state.source.close();
  state.job = id;
  setJobState(`${name} running`, "running");
  const src = new EventSource(`/api/jobs/${encodeURIComponent(id)}/stream?token=${encodeURIComponent(state.token)}`);
  state.source = src;
  src.addEventListener("log", (ev) => logLine(JSON.parse(ev.data)));
  src.addEventListener("end", (ev) => {
    const end = JSON.parse(ev.data);
    src.close();
    state.source = null;
    setJobState(`${name} ${end.state}`, end.state === "done" ? "ok" : "down");
    if (end.state === "failed") banner(`${name} failed: ${end.error}`);
    refreshAll();
  });
  src.onerror = () => { if (src.readyState === EventSource.CLOSED) setJobState(`${name} ?`, "pending"); };
}

async function resumeRunningJob() {
  const { jobs } = await api("GET", "/api/jobs");
  const running = jobs.find((j) => j.state === "running");
  if (running) followJob(running.id, running.name);
}

// ------------------------------------------------------------------ tunnels
function renderTunnelLinks(forwards) {
  const list = $("#tunnel-links");
  const items = forwards && forwards.length ? forwards : [
    { local_port: 8080, label: "master UI" }, { local_port: 4040, label: "running job" },
    ...((state.inventory && state.inventory.hosts) || []).filter((x) => x.role === "worker")
      .map((x, i) => ({ local_port: 8081 + i, label: `worker ${x.name}` })),
  ];
  list.replaceChildren(...items.map((f) => h("li", {},
    h("a", { href: `http://localhost:${Number(f.local_port)}`, target: "_blank", rel: "noopener noreferrer", text: `:${f.local_port} ${f.label}` }))));
}

async function refreshTunnel() {
  const t = await api("GET", "/api/tunnel");
  $("#tunnel-close").disabled = !t.open;
  if (t.open) renderTunnelLinks(t.forwards);
}

// ------------------------------------------------------------------ keys + audit
async function refreshKeys() {
  const { keys } = await api("GET", "/api/keys");
  const body = $("#keys tbody");
  if (!keys.length) { body.replaceChildren(h("tr", {}, h("td", { class: "empty", colspan: "6", text: "No keys registered yet." }))); return; }
  body.replaceChildren(...keys.map((k) => h("tr", {},
    td(pill(k.status, k.status === "active" ? "ok" : "down")), td(k.kind), td(k.owner),
    td(h("span", { class: "fp", text: k.fingerprint })), td((k.installed_on || []).join(", ") || "–"),
    td(k.kind === "control" || k.status !== "active" ? "" :
      h("button", { class: "btn btn-sm btn-danger", type: "button", text: "Revoke", onclick: () => revokeKey(k) })))));
}

async function revokeKey(k) {
  if (!confirm(`Revoke ${k.owner} key ${k.fingerprint}? It will be removed from: ${(k.installed_on || []).join(", ") || "no hosts"}.`)) return;
  try {
    const { job_id } = await api("POST", "/api/keys/revoke", { fingerprint: k.fingerprint });
    followJob(job_id, "revoke");
  } catch (e) { banner(`Revoke: ${e.message}`); }
}

async function refreshAudit() {
  const { entries } = await api("GET", "/api/audit?limit=200");
  $("#audit").replaceChildren(...entries.reverse().map((e) => {
    const { at, event, ...rest } = e;
    const kv = Object.entries(rest).map(([k, v]) => `${k}=${Array.isArray(v) ? v.join(",") : v}`).join("  ");
    return h("li", {}, h("span", { class: "at", text: at }), h("span", { class: "ev", text: event }), h("span", { class: "kv", text: kv }));
  }));
}

// ------------------------------------------------------------------ boot
async function settle(...tasks) {
  const results = await Promise.allSettled(tasks.map((t) => t()));
  const failed = results.find((r) => r.status === "rejected");
  if (failed) sysLog(failed.reason.message, "error");
}

async function refreshAll() {
  await settle(refreshVault, refreshInventory, refreshKeys, refreshAudit, refreshTunnel);
  refreshStatus();
}

function wire() {
  $("#vault-form").addEventListener("submit", unlockVault);
  $("#vault-lock").addEventListener("click", async () => {
    try { await api("POST", "/api/vault/lock"); await refreshVault(); await refreshInventory(); }
    catch (e) { banner(e.message); }
  });
  $("#host-form").addEventListener("submit", addHost);
  $("#import-form").addEventListener("submit", importSystems);
  $("#btn-refresh").addEventListener("click", refreshStatus);
  $("#log-clear").addEventListener("click", () => $("#log").replaceChildren());
  $("#tunnel-close").addEventListener("click", async () => {
    try { await api("POST", "/api/tunnel/close"); sysLog("tunnels closed"); await refreshTunnel(); renderTunnelLinks(); }
    catch (e) { banner(e.message); }
  });
  document.querySelectorAll("[data-action]").forEach((b) => b.addEventListener("click", () => runAction(b.dataset.action)));
  setInterval(() => { if (document.visibilityState === "visible") refreshStatus(); }, STATUS_REFRESH_MS);
  document.addEventListener("visibilitychange", () => { if (document.visibilityState === "visible") refreshStatus(); });
}

document.addEventListener("DOMContentLoaded", async () => {
  initToken();
  wire();
  if (!state.token) return;
  await refreshAll();
  settle(resumeRunningJob);
});
