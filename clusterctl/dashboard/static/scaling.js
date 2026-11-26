// Scaling benchmark panel: speedup vs workers for the newest results/bench-*.json.
// Loaded after app.js + evidence.js and reuses their helpers (h, api, state, svg, verdict, empty).
// DOM building / textContent only, never innerHTML. Mirrors clusterctl/scaling.py.
"use strict";

const SC_POLL_MS = 60000;        // `clusterctl bench` runs from the CLI, so poll slowly as well
const SC_BARELY = 1.5;           // below this speedup an input "barely speeds up"
const SC_W = 640, SC_H = 300, SC_L = 52, SC_R = 20, SC_T = 18, SC_B = 46;

const scSizes = (b) => [...new Set((b.table || []).map((r) => Number(r.size_mb)))].sort((a, c) => a - c);
const scWorkers = (b) => (b.worker_counts && b.worker_counts.length ? b.worker_counts
  : [...new Set((b.table || []).map((r) => Number(r.workers)))].sort((a, c) => a - c)).map(Number);
const scRow = (b, size, k) => (b.table || []).find((r) => Number(r.size_mb) === size && Number(r.workers) === k);
const scSeries = (b, size) => (b.table || []).filter((r) => Number(r.size_mb) === size)
  .map((r) => [Number(r.workers), Number(r.speedup)]).sort((a, c) => a[0] - c[0]);
const scX = (v) => `${v.toFixed(1)}×`;

function scAllCorrect(b) {
  const vals = Object.values(b.correct_vs_reference || {});
  return vals.length ? vals.every(Boolean) : null;
}

function scInterpretation(b) {
  const sizes = scSizes(b);
  if (!sizes.length) return "No benchmark measurements recorded.";
  const largest = sizes[sizes.length - 1];
  const pts = scSeries(b, largest);
  const baseK = pts[0][0], topK = pts[pts.length - 1][0], sp = pts[pts.length - 1][1];
  const eff = topK ? Math.round((100 * sp) / topK) : 0;
  if (topK === baseK) return `Only ${topK} worker(s) measured for ${largest} MB, so there is no scaling to compare yet.`;
  const plural = topK !== 1 ? "workers" : "worker";
  let text = sp >= 1
    ? `Largest input (${largest} MB): ${topK} ${plural} are ${scX(sp)} faster than ${baseK} (${eff}% efficiency)`
    : `Largest input (${largest} MB): ${topK} ${plural} are slower than ${baseK} (${scX(sp)}, ${eff}% efficiency)`;
  const smallest = sizes[0];
  if (smallest === largest) return `${text}.`;
  const sPts = scSeries(b, smallest);
  const sSp = sPts.length ? sPts[sPts.length - 1][1] : 0;
  text += sSp < SC_BARELY
    ? `; ${smallest} MB barely speeds up (${scX(sSp)}) because fixed start-up cost dominates the short job`
    : `; ${smallest} MB reaches ${scX(sSp)}`;
  return `${text}.`;
}

function scChart(b) {
  const ks = scWorkers(b).length ? scWorkers(b) : [1];
  const k0 = Math.min(...ks), k1 = Math.max(...ks);
  const sizes = scSizes(b);
  const peak = Math.max(k1, 1, ...sizes.flatMap((s) => scSeries(b, s).map((p) => p[1])));
  const yMax = Math.max(1, Math.ceil(peak));
  const pw = SC_W - SC_L - SC_R, ph = SC_H - SC_T - SC_B;
  const x = (k) => (k1 === k0 ? SC_L + pw / 2 : SC_L + ((k - k0) / (k1 - k0)) * pw);
  const y = (v) => SC_T + (1 - Math.min(v, yMax) / yMax) * ph;
  const chart = svg("svg", { viewBox: `0 0 ${SC_W} ${SC_H}`, class: "sc-chart", role: "img",
    "aria-label": "Speedup versus number of workers, one line per input size" });
  const step = yMax <= 6 ? 1 : 2;
  for (let v = 0; v <= yMax; v += step) {
    chart.append(svg("line", { x1: SC_L, y1: y(v).toFixed(1), x2: SC_W - SC_R, y2: y(v).toFixed(1), class: "sc-grid" }),
      svg("text", { x: SC_L - 8, y: (y(v) + 4).toFixed(1), class: "sc-tick", "text-anchor": "end" }, `${v}×`));
  }
  for (const k of ks) {
    chart.append(svg("text", { x: x(k).toFixed(1), y: SC_H - SC_B + 18, class: "sc-tick", "text-anchor": "middle" }, k));
  }
  const midY = (SC_T + ph / 2).toFixed(1);
  chart.append(svg("text", { x: SC_L + pw / 2, y: SC_H - 6, class: "sc-axis", "text-anchor": "middle" }, "workers"),
    svg("text", { x: 14, y: midY, class: "sc-axis", "text-anchor": "middle", transform: `rotate(-90 14 ${midY})` },
      "speedup vs 1 worker"),
    svg("line", { x1: x(k0).toFixed(1), y1: y(k0).toFixed(1), x2: x(k1).toFixed(1), y2: y(k1).toFixed(1), class: "sc-ideal" }));
  sizes.forEach((size, i) => {
    const pts = scSeries(b, size);
    chart.append(svg("polyline", { points: pts.map(([k, v]) => `${x(k).toFixed(1)},${y(v).toFixed(1)}`).join(" "),
      class: `sc-line s${i % 6}` }));
    for (const [k, v] of pts) {
      const dot = svg("circle", { cx: x(k).toFixed(1), cy: y(v).toFixed(1), r: 3.5, class: `sc-dot s${i % 6}` });
      dot.append(svg("title", {}, `${size} MB, ${k} workers: ${v.toFixed(2)}×`));
      chart.append(dot);
    }
  });
  const legend = [...sizes.map((s, i) => [`${s} MB`, `sc-line s${i % 6}`]), ["ideal (linear)", "sc-ideal"]];
  legend.forEach(([label, cls], i) => {
    const ly = SC_T + 12 + i * 18;
    chart.append(svg("line", { x1: SC_L + 12, y1: ly, x2: SC_L + 34, y2: ly, class: cls }),
      svg("text", { x: SC_L + 40, y: ly + 4, class: "sc-legend" }, label));
  });
  return chart;
}

function scTable(b) {
  const ks = scWorkers(b);
  const rows = scSizes(b).map((size) => h("tr", {}, h("th", { scope: "row", text: `${size} MB` }),
    ...ks.map((k) => {
      const r = scRow(b, size, k);
      if (!r) return td("–");
      const sub = `${Number(r.speedup).toFixed(2)}× · ${Math.round(100 * r.efficiency)}% · ${Number(r.throughput_mb_s).toFixed(1)} MB/s`;
      return td(h("b", { text: `${Number(r.median_s).toFixed(1)} s` }), h("span", { class: "sc-cell-sub", text: sub }));
    })));
  return h("div", { class: "table-wrap" }, h("table", { class: "grid compact sc-table" },
    h("thead", {}, h("tr", {}, h("th", { scope: "col", text: "input" }),
      ...ks.map((k) => h("th", { scope: "col", text: `${k} worker${k === 1 ? "" : "s"}` })))),
    h("tbody", {}, ...rows)));
}

function scBadge(b) {
  const ok = scAllCorrect(b);
  if (ok == null) return h("span", { class: "verdict verdict-na", text: "– not checked" });
  return verdict(ok, "every run matched the single-machine count", "a run differed from the single-machine count");
}

function renderScaling(b) {
  const box = document.getElementById("sc-body");
  const badge = document.getElementById("sc-badge");
  if (!box || !badge) return;
  if (!b) {
    badge.replaceChildren();
    box.replaceChildren(h("p", { class: "muted", text: "No scaling benchmark yet. Run: clusterctl bench" }));
    return;
  }
  badge.replaceChildren(scBadge(b));
  box.replaceChildren(
    h("p", { class: "sc-sentence", text: scInterpretation(b) }),
    h("div", { class: "sc-split" },
      h("figure", { class: "sc-fig" }, scChart(b),
        h("figcaption", { class: "muted small", text: "Speedup vs workers · dashed = ideal linear scaling" })),
      h("div", { class: "sc-side" }, scTable(b),
        h("p", { class: "muted small mono",
          text: `run ${b.stamp || "?"} · ${b.repeats ?? "?"} repeat(s) per cell, median shown · ${b.cores_per_worker ?? "?"} cores per worker` }),
        h("p", { class: "muted small", text: "cell = median time · speedup · efficiency · throughput" }))));
}

async function refreshScaling() {
  if (!state.token || document.hidden) return;
  try {
    const res = await api("GET", "/api/evidence/results");
    renderScaling((res.bench || [])[0]);
  } catch (err) {
    const box = document.getElementById("sc-body");
    if (box) box.replaceChildren(h("p", { class: "ev-empty", text: `Could not load benchmark: ${err.message || err}` }));
  }
}

document.addEventListener("DOMContentLoaded", () => {
  refreshScaling();
  document.addEventListener("job:end", refreshScaling);
  setInterval(refreshScaling, SC_POLL_MS);
});
