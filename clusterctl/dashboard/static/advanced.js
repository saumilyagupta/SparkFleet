// Advanced view: live per-node load. Loaded after app.js + evidence.js and reuses
// their helpers (h, api, state, svg). DOM building only, never innerHTML.
"use strict";

const LOAD_POLL_MS = 3000;
const SPARK_CORES_PER_NODE = 24;
const W = 320, H = 90;

function sparkline(series, key, max, cls) {
  if (series.filter((p) => p[key] != null).length < 2) return null;
  const t0 = series[0].t, t1 = series[series.length - 1].t || t0 + 1;
  const x = (t) => ((t - t0) / Math.max(1, t1 - t0)) * W;
  const y = (v) => H - (Math.min(v, max) / max) * (H - 4) - 2;
  let pen = "M";  // lift the pen at gaps (node down / master unreachable)
  const d = series.map((p) => {
    if (p[key] == null) { pen = "M"; return ""; }
    const seg = `${pen}${x(p.t).toFixed(1)},${y(p[key]).toFixed(1)}`;
    pen = "L";
    return seg;
  }).join(" ");
  return svg("path", { d, class: cls, fill: "none" });
}

function loadCard(node) {
  const latest = node.latest || {};
  const down = node.status !== "up" && node.status !== "starting";
  const cpu = latest.cpu == null ? "–" : `${latest.cpu.toFixed(0)}%`;
  const mem = latest.mem == null ? "–" : `${latest.mem.toFixed(0)}%`;
  const chart = svg("svg", { viewBox: `0 0 ${W} ${H}`, class: "load-chart", role: "img",
    "aria-label": `${node.name} CPU and Spark cores over the last minutes` });
  chart.append(svg("line", { x1: 0, y1: H / 2, x2: W, y2: H / 2, class: "load-mid" }));
  const sparkPath = sparkline(node.series, "spark_cores", SPARK_CORES_PER_NODE, "load-spark");
  const cpuPath = sparkline(node.series, "cpu", 100, "load-cpu");
  if (sparkPath) chart.append(sparkPath);
  if (cpuPath) chart.append(cpuPath);
  return h("div", { class: `load-card${down ? " load-down" : ""}` },
    h("div", { class: "load-head" },
      h("b", { text: node.name }),
      h("span", { class: "muted mono small", text: `${node.role} · ${node.ip}` }),
      h("span", { class: `pill ${down ? "pill-down" : "pill-ok"}`, text: down ? "DOWN" : "UP" })),
    chart,
    h("div", { class: "load-stats mono small" },
      h("span", { text: `CPU ${cpu}` }), h("span", { text: `mem ${mem}` }),
      h("span", { text: `load ${latest.load ?? "–"}` }),
      h("span", { class: "load-spark-label", text: `Spark cores ${latest.spark_cores ?? 0}` })),
    down ? h("p", { class: "muted small", text: node.status }) : null);
}

async function refreshLoad() {
  const box = document.getElementById("adv-load");
  if (!box || document.hidden) return;
  try {
    const data = await api("GET", "/api/monitor");
    box.replaceChildren(...data.nodes.map(loadCard));
  } catch (err) {
    box.replaceChildren(h("p", { class: "ev-empty", text: `live load unavailable: ${err.message || err}` }));
  }
}

document.addEventListener("DOMContentLoaded", () => {
  refreshLoad();
  setInterval(refreshLoad, LOAD_POLL_MS);
});
