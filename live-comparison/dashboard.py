#!/usr/bin/env python3
"""Render the live Mainnet or Testnet comparison page from dispatcher records."""
import argparse
from bisect import bisect_right
from collections import deque
from datetime import datetime, timezone
from html import escape
import json
from pathlib import Path
import re
import statistics
import sys
import tomllib

from latency_stats import summary
import report
from load_intervals import merge_intervals

TARGETS = ("zakura", "zebra-reference", "zebra-candidate")
LABELS = ("Zakura", "Zebra v7.0.0", "Optimized Zebra")
# The page shows Optimized Zebra first, then Zebra v7.0.0 and Zakura.
ORDER = (TARGETS[2], TARGETS[1], TARGETS[0])
HOST_STATES = {True: "busy", False: "quiet", None: "unknown"}
BINS = ("<20 kB", "20–300 kB", ">300 kB")
COLORS = ("#009E73", "#D55E00", "#0072B2")
DARK_COLORS = ("#54D6AD", "#FF9D64", "#66BDEB")
NETWORKS = ("Mainnet", "Testnet")
DEFAULT_CONFIG = Path(__file__).with_name("contestant.toml.example")


def size_bin(size):
    return BINS[0] if size < 20_000 else BINS[1] if size <= 300_000 else BINS[2]


def utc(at):
    return datetime.fromtimestamp(at / 1000, timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z") if at is not None else "No observations"


def short_build_label(version):
    """Keep the recorded build and feature labels, omitting source/checksum metadata."""
    label = re.sub(r"\b(?:source|sha256)\s+[0-9a-f]+\b[,;]?\s*", "", version, flags=re.I)
    return " ".join(re.sub(r"\s*\(", ", ", label).replace(")", "").split()).strip(" ,;")


def latency_summary(blocks, target):
    values = [b["first"][target]["latency_ms"] for b in blocks]
    stats = summary(values) if values else {"mean": None, "median": None, "mode": None}
    return {"n": len(values), **{f"{name}_ms": value for name, value in stats.items()}}


def glance_comparisons(blocks):
    """Ratio of paired means and signed same-block savings, clustered by dispatch time."""
    comparisons = {}
    for target in (TARGETS[1], TARGETS[0]):
        pairs = [(b["first"][target]["latency_ms"], b["first"][TARGETS[2]]["latency_ms"])
                 for b in blocks]
        candidate_total = sum(candidate for _, candidate in pairs)
        differences = [comparator - candidate for comparator, candidate in pairs]
        _, confidence = report.interval([(b["at"], difference) for b, difference in zip(blocks, differences)])
        comparisons[target] = {"ratio": sum(comparator for comparator, _ in pairs) / candidate_total
                               if candidate_total else None,
                               "n": len(pairs) if candidate_total else 0,
                               "saving_ms": statistics.fmean(differences) if differences else None,
                               "saving_ci95_ms": [confidence["lower_ms"], confidence["upper_ms"]]
                               if confidence["lower_ms"] is not None else None,
                               "saving_median_ms": statistics.median(differences) if differences else None,
                               "saving_n": len(pairs)}
    return comparisons


def rolling_means(points):
    """Trailing 15-block means; break and reset after gaps longer than 10 minutes."""
    window, trend = deque(maxlen=15), []
    for point in points:
        if window and point[2] - window[-1][2] > 10 * 60 * 1000:
            trend.append([point[2], None, None, None])
            window.clear()
        window.append(point)
        means = []
        for i in range(3):
            values = [p[i + 3] for p in window if p[i + 3] is not None]
            means.append(statistics.fmean(values) if values else None)
        trend.append([point[2], *means])
    return trend


def load_observations(path, config):
    """Stream JSONL; retain only timing/identity metadata, never raw block hex."""
    config = Path(config)
    with config.open("rb") as source:
        settings = json.load(source) if config.suffix == ".json" else tomllib.load(source)
    configured = {node["name"]: node for node in settings["contestants"]}
    if not all(target in configured for target in TARGETS):
        raise ValueError(f"config must include {', '.join(TARGETS)}")
    clients, attempts, sessions = {}, [], []
    with Path(path).open(encoding="utf-8") as source:
        for number, line in enumerate(source, 1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
                # One raw payload can exist during JSON decoding, but never survives this row.
                record.pop("raw_block_hex", None)
                if record.get("event") not in ("session", "submitblock_call"):
                    continue
                report.validate(record)
                if record["event"] == "submitblock_call" and record["observed_at_unix_ms"] < record["dispatch_unix_ms"]:
                    raise ValueError("submission response precedes dispatch")
                advertised = record.get("clients", {})
                if record["event"] == "session":
                    sessions.append({"synthetic": record["synthetic"]})
                else:
                    advertised = {record["target"]: {"name": record["client"], "version": record["version"]}}
                    if record["target"] in TARGETS:
                        attempts.append({key: record[key] for key in (
                            "hash", "height", "block_bytes", "target", "dispatch_unix_ms",
                            "observed_at_unix_ms", "latency_ms", "outcome")})
                for target, client in advertised.items():
                    if target not in TARGETS:
                        continue
                    identity = {key: client[key] for key in ("name", "version")}
                    if clients.setdefault(target, identity) != identity:
                        raise ValueError(f"client/version changed for {target}; use a separate records file")
            except (ValueError, TypeError, KeyError, AttributeError) as error:
                raise ValueError(f"{path}:{number}: {error}") from error
    if not sessions:
        raise ValueError("records require a session to distinguish real observations from synthetic data")
    for target in TARGETS:
        client = clients.setdefault(target, {"name": configured[target]["client"], "version": configured[target]["version"]})
        client["configured_version"] = configured[target]["version"]
    attempts.sort(key=lambda a: (a["dispatch_unix_ms"], a["observed_at_unix_ms"], a["target"], a["hash"]))
    return clients, attempts, sessions


def collect(path, config=None, load_intervals=None, network="Mainnet", covered_until=None):
    """Summarize first attempts on blocks that all three nodes accepted.

    Host load is known only where `load_intervals` covers: not at all without them, and only up to
    `covered_until` (the export time, in ms) when given. Blocks outside that are neither busy nor quiet.
    """
    if network not in NETWORKS:
        raise ValueError(f"network must be one of {', '.join(NETWORKS)}")
    if config is None:
        config = DEFAULT_CONFIG if network == "Mainnet" else DEFAULT_CONFIG.parent / "config" / "testnet" / "contestant.toml.example"
    clients, attempts, sessions = load_observations(path, config)
    blocks = {}
    for attempt in attempts:
        block = blocks.setdefault(attempt["hash"], {"hash": attempt["hash"], "height": attempt["height"],
                                                   "size": attempt["block_bytes"], "at": attempt["dispatch_unix_ms"], "first": {}})
        if (block["height"], block["size"]) != (attempt["height"], attempt["block_bytes"]):
            raise ValueError(f"inconsistent height/size for block {attempt['hash']}")
        block["first"].setdefault(attempt["target"], attempt)
    loads = merge_intervals(load_intervals or [], padding=30_000)
    load_starts = [start for start, _ in loads]

    def busy(at):
        """True or False while other recorded work ran on the host or not; None when unknown."""
        if load_intervals is None or (covered_until is not None and at > covered_until + 30_000):
            return None
        index = bisect_right(load_starts, at) - 1
        return index >= 0 and at <= loads[index][1]

    accepted, per_block = [], []
    for block in blocks.values():
        first = block["first"]
        if all(target in first and first[target]["outcome"] == "accepted" for target in TARGETS):
            accepted.append(block)
        # A rejected or failed first attempt has no acceptance time.
        per_block.append({"height": block["height"], "hash": block["hash"], "size_bytes": block["size"],
                          "busy": busy(block["at"]),
                          "latency_ms": {target: first[target]["latency_ms"]
                                         if target in first and first[target]["outcome"] == "accepted" else None
                                         for target in TARGETS}})
    start = attempts[0]["dispatch_unix_ms"] if attempts else None
    end = max((a["observed_at_unix_ms"] for a in attempts), default=None)
    by_size, size_comparisons = {}, {}
    for label in ("All sizes", *BINS):
        subset = [b for b in accepted if label == "All sizes" or size_bin(b["size"]) == label]
        nodes = {t: latency_summary(subset, t) for t in TARGETS}
        by_size[label] = {"n": len(subset),
                          **{name + "s": {t: node[name + "_ms"] for t, node in nodes.items()}
                             for name in ("mean", "median", "mode")}}
        size_comparisons[label] = glance_comparisons(subset)
    quiet_blocks = [b for b in accepted if busy(b["at"]) is False]
    quiet = {"nodes": {target: latency_summary(quiet_blocks, target) for target in TARGETS},
             "comparisons": glance_comparisons(quiet_blocks)}
    glance_end = max((b["at"] for b in blocks.values()), default=None)
    glance_start = glance_end - 24 * 60 * 60 * 1000 if glance_end is not None else None
    glance_blocks = [b for b in accepted if b["at"] >= glance_start]
    glance_nodes = {target: latency_summary(glance_blocks, target) for target in TARGETS}
    glance_points = [[b["height"], b["size"], b["at"],
                      *[b["first"][t]["latency_ms"] for t in TARGETS]] for b in glance_blocks]
    chart_start = glance_points[0][2] if glance_points else glance_end
    return {"generated_at": utc(datetime.now(timezone.utc).timestamp() * 1000),
            "network": network, "synthetic": any(session["synthetic"] for session in sessions), "clients": clients,
            "per_block": sorted(per_block, key=lambda b: (b["height"], b["hash"])),
            "mixed_load": by_size, "quiet": quiet,
            "points": [[b["height"], b["size"], b["at"], *[b["first"][t]["latency_ms"] for t in TARGETS]] for b in accepted],
            "load_intervals": [[max(a, start), min(b, end)] for a, b in loads if start is not None and a <= end and b >= start],
            "glance": {"window_start": utc(glance_start), "window_end": utc(glance_end),
                       "chart_start": utc(chart_start), "nodes": glance_nodes,
                       "comparisons": glance_comparisons(glance_blocks), "bins": size_comparisons,
                       "recent": list(reversed(per_block[-10:])),
                       "points": glance_points, "trend": rolling_means(glance_points)},
            "targets": TARGETS, "labels": LABELS, "colors": COLORS, "dark_colors": DARK_COLORS}


def ms(value):
    return "—" if value is None else f"{value:.1f}"


def ratio_text(value):
    if value is None:
        return "—"
    return f"{value:.2f}" if 1 <= value < 10 else f"{value:.3g}"


def comparison_text(ratio):
    if ratio is None:
        return "—"
    if ratio >= 1.05:
        return f"{ratio_text(ratio)}× faster"
    if ratio == 0:
        return "Slower (zero comparator latency)"
    if ratio <= 1 / 1.05:
        return f"{ratio_text(1 / ratio)}× slower"
    return "Within 5%"


def saving_text(value):
    if value is None:
        return "Difference unavailable"
    if abs(value) < .05:
        return "Difference < 0.05 ms per block"
    magnitude = abs(value)
    amount = f"{magnitude:.2f}" if magnitude < 1 else f"{magnitude:.1f}" if magnitude < 10 else f"{magnitude:.0f}"
    return f'{"Saves" if value > 0 else "Adds"} {amount} ms per block'


def ci_text(interval):
    return (f'bootstrap 95% CI [{interval[0]:+.3g}, {interval[1]:+.3g}] ms'
            if interval is not None else 'bootstrap 95% CI unavailable')


def render(data):
    network = data.get("network", "Mainnet")
    if network not in NETWORKS:
        raise ValueError(f"network must be one of {', '.join(NETWORKS)}")
    navigation = "".join(
        f'<a href="{page}"' + (' aria-current="page"' if name == network else '') + f'>{name}</a>'
        for name, page in (("Mainnet", "index.html"), ("Testnet", "testnet.html")))
    labels = dict(zip(TARGETS, LABELS))
    glance = data["glance"]
    colors, dark_colors = dict(zip(TARGETS, COLORS)), dict(zip(TARGETS, DARK_COLORS))
    size_rows = []
    for label, summary in data["mixed_load"].items():
        cells = "".join(
            f'<td>{ms(summary["means"][target])}<br><small>median {ms(summary["medians"][target])}; '
            f'half-sample mode {ms(summary["modes"][target])}</small></td>' for target in ORDER)
        ratio_cells = []
        for comparison in glance["bins"][label].values():
            ratio = comparison["ratio"]
            label_text = comparison_text(ratio)
            if label_text == "Within 5%":
                label_text = f"within 5% ({ratio_text(ratio)}×)"
            ratio_cells.append(f'<td><strong>{label_text}</strong><br><small>Mean: {escape(saving_text(comparison["saving_ms"]))}; '
                               f'{ci_text(comparison["saving_ci95_ms"])}; '
                               f'median: {escape(saving_text(comparison["saving_median_ms"]))}</small></td>')
        size_rows.append(f'<tr><th scope="row">{escape(label)}</th><td>{summary["n"]}</td>{cells}{"".join(ratio_cells)}</tr>')
    latest_block = glance["recent"][0] if glance["recent"] else None
    quiet = data["quiet"]
    tiles, headline_rows, recent_rows = [], [], []
    for target in ORDER:
        node, quiet_node = glance["nodes"][target], quiet["nodes"][target]
        latest_ms = latest_block["latency_ms"][target] if latest_block else None
        latest_label = f'Latest block {latest_block["height"]:,}' if latest_block else "Latest block"
        tiles.append(f'<article class="glance-tile" style="--node:{colors[target]};--node-dark:{dark_colors[target]}"><h3>{labels[target]}</h3>'
                     f'<p class="glance-metric">{ms(node["mean_ms"])} <small>ms</small></p>'
                     f'<p>Mean, last 24 h ({node["n"]} blocks)</p>'
                     f'<p>Median {ms(node["median_ms"])} ms; half-sample mode {ms(node["mode_ms"])} ms</p>'
                     f'<p>Quiet host, all records: <strong>{ms(quiet_node["mean_ms"])} ms</strong> ({quiet_node["n"]} blocks); '
                     f'median {ms(quiet_node["median_ms"])} ms; half-sample mode {ms(quiet_node["mode_ms"])} ms</p>'
                     f'<p>{latest_label}: <strong>{ms(latest_ms)} ms</strong></p></article>')
    for target, comparison in glance["comparisons"].items():
        quiet_comparison = quiet["comparisons"][target]
        headline_rows.append(f'<div><p class="glance-ratio">{comparison_text(comparison["ratio"])}</p><p>Optimized Zebra vs {labels[target]}</p>'
                             f'<p><strong>Mean: {escape(saving_text(comparison["saving_ms"]))}</strong> ({comparison["saving_n"]} paired blocks); '
                             f'{ci_text(comparison["saving_ci95_ms"])}; median: {escape(saving_text(comparison["saving_median_ms"]))}</p>'
                             f'<p class="note">Quiet host, all records: {comparison_text(quiet_comparison["ratio"])} '
                             f'({quiet_comparison["saving_n"]} paired blocks); mean: {escape(saving_text(quiet_comparison["saving_ms"]))}; '
                             f'{ci_text(quiet_comparison["saving_ci95_ms"])}; median: {escape(saving_text(quiet_comparison["saving_median_ms"]))}</p></div>')
    for row in glance["recent"]:
        cells = "".join(f'<td>{ms(row["latency_ms"][target])}</td>' for target in ORDER)
        recent_rows.append(f'<tr><th scope="row" title="{row["hash"]}">{row["height"]:,}</th>'
                           f'<td>{row["size_bytes"] / 1000:.3f}</td>{cells}<td>{HOST_STATES[row["busy"]]}</td></tr>')
    build_rows = "".join(
        f'<tr><th scope="row">{labels[target]}</th><td><code>{escape(short_build_label(data["clients"][target]["version"]))}</code></td>'
        f'<td>{"Pruned" if target == "zakura" else "Full chain"}</td></tr>' for target in ORDER)
    heads = "".join(f'<th scope="col">{labels[target]}</th>' for target in ORDER)
    payload = json.dumps(data, separators=(",", ":"), allow_nan=False).replace("&", "\\u0026").replace("<", "\\u003c").replace(">", "\\u003e")
    return TEMPLATE.replace("@@CONTENT@@", f'''
<header><div class="page-heading"><h1>Zebra perf stats</h1><nav class="network-tabs" aria-label="Network">{navigation}</nav></div>
<p class="note">{"SYNTHETIC DATA — not real measurements · " if data["synthetic"] else ""}Updated {data["generated_at"]}</p></header>
<section aria-labelledby="glance"><h2 id="glance">Block submission latency</h2>
<p>Each new {network} block is submitted to all three nodes concurrently via <code>submitblock</code>. Latency is the RPC round trip of each node's first attempt. Mean latency and the ratio of paired means (comparator ÷ Optimized Zebra) and paired mean savings are headlines (slow rounds matter); medians and half-sample modes show their effect. Savings are comparator minus Optimized Zebra. Bootstrap 95% intervals resample occupied five-minute dispatch clusters and are unavailable below {report.MIN_PAIRS} pairs or {report.MIN_CLUSTERS} clusters. Figures cover the last 24 hours unless marked otherwise. Quiet-host figures use only blocks dispatched outside recorded host-load intervals (±30 s).</p>
<div class="glance-headlines">{"".join(headline_rows)}</div>
<div class="glance-tiles">{"".join(tiles)}</div>
<h3>Latency, last 24 h</h3>
<p id="glance-chart-status" class="note" role="status">{"No blocks in the last 24 hours. " if not glance["points"] else ""}The chart requires JavaScript.</p>
<figure><div id="glance-chart" class="chart" role="img" aria-label="Submission latency over the last 24 hours"></div><figcaption>Points are individual blocks; lines are rolling means over 15 blocks. Shading marks recorded host load.</figcaption></figure>
<div class="table-wrap" tabindex="0" role="region" aria-label="Latency by block size"><table><caption>Mean, median and half-sample mode latency by block size, all records (ms)</caption><thead><tr><th scope="col">Size</th><th scope="col">Blocks</th>{heads}<th scope="col">Optimized vs Zebra v7.0.0</th><th scope="col">Optimized vs Zakura</th></tr></thead><tbody>{"".join(size_rows)}</tbody></table></div>
<figure><div id="size-chart" class="chart" role="img" aria-label="Submission latency by block size"></div><figcaption>Latency against serialized block size for every block accepted by all three nodes, all records.</figcaption></figure>
<div class="table-wrap" tabindex="0" role="region" aria-label="Latest blocks"><table><caption>Latest 10 blocks (ms)</caption><thead><tr><th scope="col">Height</th><th scope="col">kB</th>{heads}<th scope="col">Host</th></tr></thead><tbody>{"".join(recent_rows) or '<tr><td colspan="6">No blocks yet.</td></tr>'}</tbody></table></div></section>
<section aria-labelledby="how"><h2 id="how">Method</h2>
<p>A Zebra feeder follows {network} over P2P and dispatches each new block to the three nodes. Depending on dispatcher configuration, feeder mempool transactions may also be mirrored; this page does not separate that traffic. All four nodes share one host with other workloads; recorded host load is shaded and excluded from the quiet-host figures.</p>
<div class="table-wrap" tabindex="0" role="region" aria-label="Builds"><table><caption>Builds</caption><thead><tr><th scope="col">Node</th><th scope="col">Build</th><th scope="col">Storage</th></tr></thead><tbody>{build_rows}</tbody></table></div>
<button id="download-csv" type="button">Download per-block CSV</button></section>
''').replace("@@DATA@@", payload)


TEMPLATE = r'''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="darkreader-lock"><title>Zebra perf stats</title>
<style>
:root{color-scheme:light dark;--bg:#fff;--text:#20252b;--muted:#49525d;--border:#c4cbd3;--surface:#f3f5f7;--link:#005c94;--accent:#0072B2}*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:15px/1.45 system-ui,sans-serif}main{max-width:1160px;margin:auto;padding:28px 24px}h1{font-size:1.45rem;line-height:1.3;margin:0 0 12px}h2{font-size:1.15rem;margin:28px 0 10px;border-bottom:1px solid var(--border);padding-bottom:4px}h3{font-size:1rem;margin:20px 0 8px}p{margin:10px 0}a{color:var(--link)}code{font:0.9em ui-monospace,monospace;overflow-wrap:anywhere}.table-wrap{overflow:auto;margin:14px 0}table{border-collapse:collapse;width:100%;font-size:.84rem;font-variant-numeric:tabular-nums;text-align:left}caption{text-align:left;margin-bottom:8px;font-size:.88rem}td,th{border-bottom:1px solid var(--border);padding:6px 8px;vertical-align:top}thead th{border-block:1px solid var(--border);background:var(--surface)}tbody th{font-weight:500}figure{margin:14px 0;min-width:0}.chart{height:365px;width:100%;overflow:hidden}figcaption,.note{font-size:.86rem;color:var(--muted)}button{font:inherit;padding:5px 10px;border:1px solid var(--border);background:var(--surface);color:var(--text);cursor:pointer}:focus-visible{outline:2px solid var(--accent);outline-offset:3px}
.glance-tiles{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:16px}.glance-tile{border:1px solid var(--border);border-top:4px solid var(--node);padding:16px;min-width:0;background:var(--surface)}.glance-tile h3{margin:0}.glance-tile p{font-size:.86rem}.glance-tile .glance-metric{font-size:2.2rem;font-weight:650;margin:12px 0 0;font-variant-numeric:tabular-nums}.glance-metric small{font-size:1rem;font-weight:400}.glance-headlines{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px;margin:20px 0 12px}.glance-headlines>div{border-left:4px solid var(--accent);padding-left:16px}.glance-ratio{font-size:clamp(1.6rem,3vw,2.3rem);font-weight:650;line-height:1.2;margin:0}.glance-headlines p{margin:6px 0}
@media(prefers-color-scheme:dark){:root{--bg:#151a21;--text:#e9eef5;--muted:#c5ced9;--border:#4a5666;--surface:#202833;--link:#8ed3ff;--accent:#66BDEB}.glance-tile{border-top-color:var(--node-dark)}}
.page-heading{display:flex;align-items:center;justify-content:space-between;gap:16px;flex-wrap:wrap}.page-heading h1{margin:0}.network-tabs{display:inline-flex;gap:2px;padding:3px;border:1px solid var(--border);border-radius:8px;background:var(--surface)}.network-tabs a{padding:6px 14px;border-radius:5px;text-decoration:none;color:var(--muted)}.network-tabs a[aria-current=page]{background:var(--accent);color:var(--bg);font-weight:650}.network-tabs a:hover{outline:1px solid var(--accent)}
@media(max-width:700px){.glance-tiles,.glance-headlines{grid-template-columns:1fr}}
@media(max-width:700px){main{padding:18px 12px}.chart{height:330px}td,th{padding:5px 6px}}
</style></head><body><main>@@CONTENT@@</main>
<script id="dashboard-data" type="application/json">@@DATA@@</script>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<script>
const d=JSON.parse(document.getElementById('dashboard-data').textContent);
function perBlockCSV(data){
  const rows=[['height','hash','size_bytes','optimized_zebra_ms','zebra_v7_ms','zakura_ms','host_busy']];
  for(const block of data.per_block){
    rows.push([block.height,block.hash,block.size_bytes,block.latency_ms['zebra-candidate'],block.latency_ms['zebra-reference'],block.latency_ms.zakura,block.busy===null?'':(block.busy?1:0)]);
  }
  const quote=value=>'"'+String(value??'').replaceAll('"','""')+'"';
  return rows.map(row=>row.map(quote).join(',')).join('\r\n')+'\r\n';
}
document.getElementById('download-csv').addEventListener('click',()=>{
  const url=URL.createObjectURL(new Blob([perBlockCSV(d)],{type:'text/csv;charset=utf-8'}));
  const link=document.createElement('a');
  link.href=url; link.download='blocks.csv'; document.body.appendChild(link); link.click(); link.remove();
  setTimeout(()=>URL.revokeObjectURL(url),0);
});
if(window.Plotly){
function renderCharts(dark){
const theme=dark?{bg:'#151a21',text:'#e9eef5',muted:'#c5ced9',grid:'#344050',minor:'#222c38',hover:'#202833'}:{bg:'#fff',text:'#20252b',muted:'#49525d',grid:'#dce2e8',minor:'#eff2f5',hover:'#f3f5f7'};
const colors=dark?d.dark_colors:d.colors;
const names=['Zakura','Zebra v7.0.0','Optimized Zebra'], order=[2,1,0];
const config={responsive:true,displayModeBar:false,scrollZoom:false};
const layout={margin:{l:58,r:18,t:12,b:92},font:{family:'system-ui,sans-serif',size:12,color:theme.text},paper_bgcolor:theme.bg,plot_bgcolor:theme.bg,legend:{orientation:'h',x:0,y:-.24,font:{size:11,color:theme.text}},hoverlabel:{bgcolor:theme.hover,bordercolor:theme.grid,font:{color:theme.text}},hovermode:'closest',uirevision:'theme',xaxis:{gridcolor:theme.grid,zeroline:false},yaxis:{gridcolor:theme.grid,zeroline:false}};
const logAxis={type:'log',dtick:1,tickformat:'~g',minor:{dtick:'D1',showgrid:true,gridcolor:theme.minor}};
function series(i){return {name:names[i],marker:{color:colors[i],size:5,opacity:.7,symbol:i===0?'diamond':'circle'},line:{color:colors[i],width:2.75,dash:i===0?'dash':'solid'}}}
const loadShapes=d.load_intervals.map(([a,b])=>({type:'rect',xref:'x',yref:'paper',x0:new Date(a).toISOString(),x1:new Date(b).toISOString(),y0:0,y1:1,fillcolor:theme.text,opacity:.06,line:{width:0},layer:'below'}));
const glanceTraces=order.map(i=>{const target=d.targets[i], rows=d.glance.points.filter(p=>p[i+3]>0);return {...series(i),type:'scatter',mode:'markers',legendgroup:target,showlegend:false,marker:{...series(i).marker,opacity:.45},x:rows.map(p=>new Date(p[2]).toISOString()),y:rows.map(p=>p[i+3]),customdata:rows.map(p=>[p[0],p[1]/1000]),hovertemplate:'Height %{customdata[0]:,.0f}<br>Size %{customdata[1]:.3f} kB<br>%{y:.3f} ms<br>%{x}<extra>%{fullData.name}</extra>'}});
glanceTraces.push(...order.map(i=>({...series(i),type:'scatter',mode:'lines',legendgroup:d.targets[i],connectgaps:false,x:d.glance.trend.map(p=>new Date(p[0]).toISOString()),y:d.glance.trend.map(p=>p[i+1]>0?p[i+1]:null),hovertemplate:'Rolling mean (15 blocks)<br>%{y:.3f} ms<br>%{x}<extra>%{fullData.name}</extra>'})));
const sizeTraces=order.map(i=>{const rows=d.points.filter(p=>p[i+3]>0);return {...series(i),type:'scatter',mode:'markers',x:rows.map(p=>p[1]/1000),y:rows.map(p=>p[i+3]),customdata:rows.map(p=>p[0]),hovertemplate:'Height %{customdata:,.0f}<br>%{x:.1f} kB<br>%{y:.3f} ms<extra>%{fullData.name}</extra>'}});
Promise.all([Plotly.react('glance-chart',glanceTraces,{...layout,xaxis:{...layout.xaxis,type:'date',title:'Dispatch time (UTC)',range:d.glance.points.length?[d.glance.chart_start,d.glance.window_end]:undefined},yaxis:{...layout.yaxis,...logAxis,title:'Latency (ms)'},shapes:loadShapes},config),
  Plotly.react('size-chart',sizeTraces,{...layout,xaxis:{...layout.xaxis,title:'Block size (kB)'},yaxis:{...layout.yaxis,...logAxis,title:'Latency (ms)'}},config)]).then(()=>{document.getElementById('glance-chart-status').textContent=d.glance.points.length?'':'No blocks in the last 24 hours.'}).catch(()=>{document.getElementById('glance-chart-status').textContent='The charts could not be rendered; the tables contain the same figures.'});
}
const scheme=window.matchMedia('(prefers-color-scheme: dark)');
renderCharts(scheme.matches);
scheme.addEventListener('change',event=>renderCharts(event.matches));
}
</script></body></html>
'''


def build_site(records, output, config=None, load_path=None, network="Mainnet"):
    """Render dashboard.html; load_path accepts one path or a sequence of paths."""
    if load_path is None:
        default = Path(records).with_name("load-intervals.json")
        paths = [default] if default.exists() else []
    else:
        paths = [load_path] if isinstance(load_path, (str, Path)) else load_path
    loads, covered_until = None, None
    for path in paths:
        with Path(path).open(encoding="utf-8") as source:
            intervals = json.load(source)
        if not isinstance(intervals, list):
            raise ValueError("load interval file must contain a JSON array")
        loads = (loads or []) + intervals
        # A file says nothing about load after it was written.
        written = int(Path(path).stat().st_mtime * 1000)
        covered_until = written if covered_until is None else min(covered_until, written)
    data = collect(records, config, loads, network, covered_until)
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    # Publish only a complete page; preserve an earlier page if parsing/rendering fails.
    temporary = output / "dashboard.html.tmp"
    temporary.write_text(render(data), encoding="utf-8")
    temporary.replace(output / "dashboard.html")
    return data


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("records", type=Path)
    parser.add_argument("output", type=Path, nargs="?", default=Path("site"))
    parser.add_argument("--network", choices=NETWORKS, default="Mainnet")
    parser.add_argument("--config", type=Path,
                        help="contestant config; default: contestant.toml.example or config/testnet/contestant.toml.example")
    parser.add_argument("--load-intervals", type=Path, action="append",
                        help="JSON [start_ms,end_ms] pairs shaded on the chart; repeat to combine; default: load-intervals.json beside records")
    args = parser.parse_args()
    try:
        data = build_site(args.records, args.output, args.config, args.load_intervals, args.network)
    except (OSError, ValueError, KeyError) as error:
        print(f"dashboard: {error}", file=sys.stderr)
        return 1
    nodes = data["glance"]["nodes"]
    print(json.dumps({"page": str(args.output / "dashboard.html"), "blocks": nodes[TARGETS[2]]["n"],
                      "median_ms": {t: nodes[t]["median_ms"] for t in TARGETS}}, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
