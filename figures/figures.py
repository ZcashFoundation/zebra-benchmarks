#!/usr/bin/env python3
"""Render PNG and SVG figures from a complete V5 benchmark campaign.

Usage: uv run --no-project --with matplotlib python -B figures/figures.py
       --input CAMPAIGN --output OUTPUT

Inputs are raw campaign receipts, never derived reports. Both paths are required.
Fonts are bundled beside this script. No nodes are launched.
"""
import argparse
import json
import math
import statistics
from pathlib import Path

FONTS = Path(__file__).resolve().parent / "fonts"
# Zakura is an external reference, drawn dashed or hatched so it does not read as one of our builds.
REFERENCE = "zakura-main"
FILLINGS = ("transparent", "orchard", "mixed")
INK = "#2b2a28"
V5_ARMS = {
    "release": ("Zebra release", "#8a8580"),
    # Both final arms contain every PR, including the cryptography pins; they differ only in the
    # opt-in `pasta-asm` feature.
    "stack": ("Optimized Zebra", "#2d6a4a"),
    "crypto": ("Optimized Zebra + Pasta assembly", "#b5651d"),
    "zakura-main": ("Zakura (reference)", "#5b7290"),
}
V5_ZEBRA = {arm: V5_ARMS[arm] for arm in ("release", "stack", "crypto")}
# Raw inputs relative to the supplied campaign directory; no derived reports or fallbacks.
V5_SOURCES = {
    "sweep": "sweep-*/observations.jsonl",
    "relay-fresh": "relay-fresh/observations.jsonl",
    "relay-settled": "relay-settled/observations.jsonl",
    "empty-tip": "empty-tip-*/samples.jsonl",
    "peer-block": "peer-block/*/*/*/samples.jsonl",
    "tip-template": "tip-template/*/*/*/samples.jsonl",
    "tip-miner": "tip-miner/*/*/*/samples.jsonl",
    "relay-chain": "relay-chain/*/*/*/samples.jsonl",
    "inclusion": "inclusion/observations.jsonl",
    "rpc": "rpc/observations.jsonl",
    "startup": "startup/observations.jsonl",
    "replay": "replay-r*/observations.jsonl",
}
# Only this historical build has the published, documented inclusion timeouts.
V5_TIMEOUT_SOURCE = "c572f8faf88e58dcaefd80ec8e4ed05f2d8643b6"


def style():
    # Delay plotting imports so --help and raw-source checks need only Python's standard library.
    global plt
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib import font_manager

    for name in ("EBGaramond-Regular.otf", "EBGaramond-Italic.otf", "EBGaramond-SemiBold.otf"):
        font_manager.fontManager.addfont(str(FONTS / name))
    plt.rcParams.update({
        "font.family": "EB Garamond", "font.size": 12, "axes.edgecolor": INK, "axes.labelcolor": INK,
        "text.color": INK, "xtick.color": INK, "ytick.color": INK, "axes.spines.top": False,
        "axes.spines.right": False, "figure.facecolor": "white", "axes.facecolor": "white",
        "savefig.facecolor": "white", "legend.frameon": False,
    })


def save(fig, name, rect=(0, 0, 1, 1)):
    OUT.mkdir(parents=True, exist_ok=True)
    fig.tight_layout(rect=rect)
    for ext in ("svg", "png"):
        fig.savefig(OUT / f"{name}.{ext}", dpi=200, bbox_inches="tight")
    plt.close(fig)


def latency_by_size(endpoint, name, data, arms=V5_ARMS):
    data = [r for r in data if r["endpoint"] == endpoint and r["filling"] in FILLINGS]
    # The blog column is about 600 px wide, so these figures are drawn with larger text.
    plt.rcParams["font.size"] = 17
    fig, axes = plt.subplots(1, 3, figsize=(11, 4.4), sharey=True)
    for ax, filling in zip(axes, FILLINGS):
        points = sorted((r for r in data if r["filling"] == filling), key=lambda r: int(r["block_bytes"]))
        x = [int(r["block_bytes"]) / 1e6 for r in points]
        for arm, (label, color) in arms.items():
            y = [float(r[f"{arm}_latency_ms_median"]) for r in points]
            low = [float(r[f"{arm}_latency_ms_min"]) for r in points]
            high = [float(r[f"{arm}_latency_ms_max"]) for r in points]
            ax.plot(x, y, "o--" if arm == REFERENCE else "o-", color=color, label=label,
                    markersize=6, linewidth=2.2)
            ax.fill_between(x, low, high, color=color, alpha=0.15, linewidth=0)
        ax.set_yscale("log")
        ax.set_title(filling.capitalize(), fontsize=18)
        ax.set_xlabel("Block size (MB)")
    axes[0].set_ylabel("ms, log scale")
    fig.legend(*axes[0].get_legend_handles_labels(), loc="upper center", ncol=len(arms), fontsize=13)
    save(fig, name, rect=(0, 0, 1, 0.88))
    plt.rcParams["font.size"] = 12


def crypto_speedup(data):
    data = [r for r in data if r["endpoint"] == "cold-submitblock" and r["filling"] in FILLINGS]
    fig, ax = plt.subplots(figsize=(6.4, 3.6))
    for filling, color in zip(FILLINGS, ("#8a8580", "#b5651d", "#2d6a4a")):
        points = sorted((r for r in data if r["filling"] == filling), key=lambda r: int(r["block_bytes"]))
        x = [int(r["block_bytes"]) / 1e6 for r in points]
        y = [float(r["stack_latency_ms_median"]) / float(r["crypto_latency_ms_median"])
             for r in points]
        ax.plot(x, y, "o-", color=color, label=filling.capitalize(), markersize=4, linewidth=1.5)
    ax.axhline(1, color=INK, linewidth=0.8, linestyle="--")
    ax.set_xlabel("Block size (MB)")
    ax.set_ylabel("Speed-up from the Pasta assembly backend")
    ax.yaxis.set_major_formatter(lambda value, _: f"{value:g}×")
    ax.legend(fontsize=10)
    save(fig, "pasta-assembly-speedup")


def relay(fresh, settled, arms=V5_ARMS):
    groups = [("New\nconnection,\nempty block", (fresh, "empty-2000")),
              ("New\nconnection,\n1.9 MB transparent", (fresh, "transparent-1900000")),
              ("Established\nconnection,\nempty block", (settled, "empty-2000")),
              ("Established\nconnection,\n1.9 MB transparent", (settled, "transparent-1900000"))]
    bars(groups, "Submission to peer tip (ms, log scale)", "relay", log=True,
         arms=arms, values=lambda source, arm: source[0][source[1], arm])


def bars(groups, ylabel, name, values, log=False, arms=V5_ARMS):
    fig, ax = plt.subplots(figsize=(7.2, 3.6))
    width = 0.8 / len(arms)
    for i, (arm, (label, color)) in enumerate(arms.items()):
        heights = [values(row, arm) for _, row in groups]
        positions = [g + (i - (len(arms) - 1) / 2) * width for g in range(len(groups))]
        ax.bar(positions, heights, width, color=color, label=label,
               hatch="///" if arm == REFERENCE else None, edgecolor="white" if arm == REFERENCE else None)
        for x, h in zip(positions, heights):
            ax.annotate(f"{h:,.1f}" if h < 100 else f"{h:,.0f}", (x, h), ha="center", va="bottom",
                        fontsize=7, xytext=(0, 2), textcoords="offset points")
    ax.set_xticks(range(len(groups)), [g for g, _ in groups])
    if log:
        ax.set_yscale("log")
    ax.set_ylabel(ylabel)
    ax.legend(fontsize=8, ncol=2, loc="lower center", bbox_to_anchor=(0.5, 1.02))
    save(fig, name)


def tip_template(medians, arms=V5_ARMS):
    groups = [("Own block\n(submitblock)", "local"), ("Peer's block,\nestablished connection", "settled")]
    bars(groups, "Submission to first new-tip template (ms, log scale)",
         "empty-block-tip-template", log=True, arms=arms, values=lambda mode, arm: medians[mode, arm])


def rpc_serving(groups, arms=V5_ZEBRA):
    bars(groups, "Requests per second", "rpc-serving", arms=arms, values=lambda row, arm: row[arm])


def zebra_zakura_comparison(sweep, relays, arms=V5_ARMS):
    groups = [(label + "\nAnyone-can-spend transparent, 1.9 MB", sweep[endpoint, "transparent-1900000"])
              for label, endpoint in (("Burst to template", "admission-to-template"),
                                      ("Prepared submission", "prepared-submitblock"),
                                      ("Cold submission", "cold-submitblock"))]
    for link in ("fresh", "settled"):
        medians = relays[link]
        for fixture, filling in (("transparent-1900000", "Anyone-can-spend transparent, 1.9 MB"),
                                 ("empty-2000", "Empty block")):
            connection = "new" if link == "fresh" else "established"
            groups.append((f"Relay, {connection} connection\n{filling}",
                           {arm: medians[fixture, arm] for arm in arms}))
    groups.append(("Cold submission\nOrchard, 1.9 MB", sweep["cold-submitblock", "orchard-1900000"]))
    assert len(groups) == 8 and all(value > 0 for _, group in groups for value in group.values())

    fig, ax = plt.subplots(figsize=(9, 8.2))
    width = 0.8 / len(arms)
    for i, (arm, (label, color)) in enumerate(arms.items()):
        positions = [g + (i - (len(arms) - 1) / 2) * width for g in range(len(groups))]
        values = [group[arm] for _, group in groups]
        ax.barh(positions, values, width, color=color, label=label,
                hatch="///" if arm == REFERENCE else None,
                edgecolor="white" if arm == REFERENCE else None)
        for y, value in zip(positions, values):
            ax.annotate(f"{value:,.1f}" if value < 100 else f"{value:,.0f}", (value, y),
                        ha="left", va="center", fontsize=9, xytext=(4, 0), textcoords="offset points")
    ax.set_yticks(range(len(groups)), [label for label, _ in groups], fontsize=11)
    ax.invert_yaxis()
    ax.set_xscale("log")
    ax.set_xlim(1, 2_000_000)
    ax.set_xlabel("Median latency (ms, log scale; lower is better)")
    ax.set_title("Zebra and Zakura", pad=65)
    ax.legend(fontsize=10, ncol=2, loc="lower center", bbox_to_anchor=(0.5, 1.01))
    save(fig, "zebra-zakura-comparison")


def peer_block(samples, arms=V5_ARMS):
    endpoints = ("cold-submitblock-nominer", "warm-mempool-submitblock", "prepared-submitblock")
    fixtures = (("Signed transparent", "transparent-signed"),
                ("Orchard", "orchard"), ("Mixed", "mixed"))
    fig, axes = plt.subplots(1, 3, figsize=(11, 4.4), sharey=True)
    width = 0.8 / len(arms)
    for ax, (title, filling) in zip(axes, fixtures):
        for i, (arm, (label, color)) in enumerate(arms.items()):
            values = [samples[filling, endpoint, arm] for endpoint in endpoints]
            assert all(len(group) == 5 for group in values), (filling, arm)
            medians = [statistics.median(group) for group in values]
            positions = [j + (i - (len(arms) - 1) / 2) * width for j in range(len(endpoints))]
            ax.bar(positions, medians, width, label=label, color=color,
                   hatch="///" if arm == REFERENCE else None)
            for x, value in zip(positions, medians):
                ax.annotate(f"{value:.0f}", (x, value), ha="center", va="bottom",
                            fontsize=8, xytext=(0, 3), textcoords="offset points")
        ax.set_title(title, fontsize=17)
        ax.set_xticks(range(len(endpoints)), ("Cold", "Mempool\nverified", "Prepared"), fontsize=13)
        ax.set_ylim(bottom=0)
    axes[0].set_ylabel("Acceptance time (ms)", fontsize=15)
    fig.legend(*axes[0].get_legend_handles_labels(), loc="upper center", ncol=len(arms),
               fontsize=11)
    save(fig, "peer-block", rect=(0, 0, 1, 0.9))


def v5_failed_arms(arms):
    if arms["zakura-main"].get("source") == V5_TIMEOUT_SOURCE:
        return {"inclusion": "zakura-main"}
    return {}


def v5_campaign(root):
    manifest = json.loads((root / "campaign.json").read_text())
    if manifest.get("profile") == "extended":
        raise ValueError("Extended is supplemental data; render the CORE campaign root")
    if set(manifest["arms"]) != set(V5_ARMS):
        raise ValueError("v5 figures require release, stack, crypto and zakura-main")
    if manifest["snapshot_height"] != 2316084 or manifest["replay_heights"] != [1750000, 1752000]:
        raise ValueError("v5 Mainnet snapshot/window differs from the figure protocol")
    for piece in manifest["pieces"]:
        # Empty-template RPC and idle-admission controls are measured but have no blog figure.
        if piece == "empty-gbt" or piece.startswith("empty-admission-"):
            continue
        # v5_receipts confirms that only the documented arm failed.
        if piece in v5_failed_arms(manifest["arms"]):
            continue
        path = root / "completed" / f"{piece}.json"
        if not path.exists() or json.loads(path.read_text()).get("success") is not True:
            raise ValueError(f"Missing successful v5 piece: {path}")
    return manifest["arms"]


def v5_receipts(root, source, select=lambda row: True, arms=None, failed_arm=None):
    """Read raw records only; a missing source, failed sample or duplicate is never dropped.

    A `failed_arm` is excluded from the result, but only if its selected rows are distinct, timed-out
    runs of the campaign binary covering exactly the cells (scenario, repetition, ...) of every other
    arm, the source's completion receipt counts every row, and no other arm failed.
    """
    paths = sorted(root.glob(V5_SOURCES[source]))
    if not paths:
        raise ValueError(f"Missing v5 source: {root / V5_SOURCES[source]}")
    records, seen, cells = [], set(), {}
    identity = ("arm", "endpoint", "scenario", "mode", "fixture", "round",
                "repetition", "rep", "sample", "concurrency")

    def check(path, row):
        key = (str(path), *(row.get(field) for field in identity))
        if key in seen:
            raise ValueError(f"Duplicate v5 receipt: {path}: {key[1:]}")
        seen.add(key)
        cells.setdefault(row["arm"], set()).add((key[0], *key[2:]))
        if arms is not None:
            declared = arms.get(row["arm"])
            if declared is None or (row.get("binary_sha256", declared["sha256"]) != declared["sha256"]
                                    or row.get("binary", declared["path"]) != declared["path"]):
                raise ValueError(f"v5 receipt differs from campaign binary identity: {path}")

    for path in paths:
        count = 0
        with path.open() as stream:
            for line in stream:
                row = json.loads(line)
                count += 1
                failed = row.get("status", "ok") != "ok" or row.get("success") is False
                if failed_arm is not None and row.get("arm") == failed_arm:
                    if select(row):
                        check(path, row)
                        if row.get("success") is not False or row.get("stop_reason") != "timeout":
                            raise ValueError(f"Documented {failed_arm} run did not time out: {path}")
                    continue
                if failed_arm is not None and failed:
                    raise ValueError(f"v5 failure outside the documented arm {failed_arm}: {path}")
                if not select(row):
                    continue
                if failed:
                    raise ValueError(f"Failed v5 receipt: {path}")
                check(path, row)
                row["_source"] = str(path.relative_to(root))
                records.append(row)
        if not count:
            raise ValueError(f"Empty v5 source: {path}")
        completion = path.parent / "completion.json"
        if failed_arm is not None and not completion.exists():
            raise ValueError(f"Missing completion receipt beside a documented failure: {path}")
        if completion.exists():
            receipt = json.loads(completion.read_text())
            if (receipt["success"] is not True and failed_arm is None) or receipt["observations"] != count:
                raise ValueError(f"Incomplete v5 source: {path}")
    if not records:
        raise ValueError(f"No matching v5 measurements: {source}")
    if failed_arm is not None and any(cells.get(failed_arm) != arm_cells
                                      for arm, arm_cells in cells.items() if arm != failed_arm):
        raise ValueError(f"{failed_arm} must time out in exactly the other arms' cells: {source}")
    return records


def v5_groups(records, dimensions, value, *, arms=V5_ARMS, expected=None, repetitions=None):
    """Keep all repetitions; extrema are descriptive ranges, not confidence intervals."""
    groups = {}
    for row in records:
        key = tuple(row[field] for field in dimensions)
        arm = row["arm"]
        number = value(row)
        if arm not in arms or not math.isfinite(number):
            raise ValueError(f"Invalid v5 arm or measurement: {arm}, {key}, {number}")
        groups.setdefault(key, {}).setdefault(arm, []).append(number)
    if not groups or (expected is not None and set(groups) != set(expected)):
        raise ValueError(f"Incomplete v5 groups for {dimensions}")
    for key, by_arm in groups.items():
        if set(by_arm) != set(arms) or len({len(values) for values in by_arm.values()}) != 1:
            raise ValueError(f"Unmatched v5 arms: {key}")
        if repetitions is not None and any(len(values) != repetitions for values in by_arm.values()):
            raise ValueError(f"Incomplete v5 repetitions: {key}; expected {repetitions}")
    return groups


def v5_medians(groups):
    return {key: {arm: statistics.median(values) for arm, values in by_arm.items()}
            for key, by_arm in groups.items()}


def v5_latency(row):
    value = row["latency_ns"] / 1e6
    if value <= 0:
        raise ValueError(f"Nonpositive latency: {row['_source']}")
    return value


def v5_sweep(records, core=False):
    endpoints = ("admission-to-template", "prepared-submitblock", "cold-submitblock")
    sizes = (250000, 1900000) if core else (25000, 100000, 250000, 500000, 1000000, 1500000, 1900000)
    expected = {(endpoint, f"{filling}-{size}") for endpoint in endpoints
                for filling in (*FILLINGS, "transparent-signed")
                for size in ((250000, 1900000) if filling == "transparent-signed" else sizes)}
    groups = v5_groups(records, ("endpoint", "fixture"), v5_latency,
                       expected=expected, repetitions=5 if core else 9)
    metadata, result = {}, []
    for row in records:
        key = row["fixture"]
        shape = row["filling"], row["block_bytes"], row["candidate_sha256"], row["setup_chain_sha256"]
        if metadata.setdefault(key, shape) != shape:
            raise ValueError(f"Unmatched sealed v5 fixture: {key}")
    for (endpoint, fixture), by_arm in groups.items():
        filling, block_bytes, _, _ = metadata[fixture]
        row = dict(endpoint=endpoint, fixture=fixture, filling=filling, block_bytes=block_bytes)
        for arm, values in by_arm.items():
            row.update({f"{arm}_latency_ms_median": statistics.median(values),
                        f"{arm}_latency_ms_min": min(values), f"{arm}_latency_ms_max": max(values)})
        result.append(row)
    return result


def v5_empty_tip(records, core=False):
    modes, rounds, repetitions = (("local", "settled"), 1, 5) if core else (("local", "peer", "settled"), 6, 3)
    expected = {(mode, round_index) for mode in modes for round_index in range(rounds)}
    grouped = v5_medians(v5_groups(records, ("mode", "round"), v5_latency,
                                  expected=expected, repetitions=repetitions))
    return {(mode, arm): statistics.median(grouped[mode, r][arm] for r in range(rounds))
            for mode in modes for arm in V5_ARMS}


def v5_metric_panels(records, dimensions, labels, metrics, name, *, arms=V5_ARMS,
                     repetitions=5, title=None):
    """One median per repetition group; retain signed observation differences."""
    fig, axes = plt.subplots(1, len(metrics), figsize=(5 * len(metrics), 4), squeeze=False)
    width = 0.8 / len(arms)
    for ax, (ylabel, metric) in zip(axes[0], metrics):
        samples = v5_groups(records, dimensions, metric, arms=arms,
                            expected=labels, repetitions=repetitions)
        groups = v5_medians(samples)
        for i, (arm, (label, color)) in enumerate(arms.items()):
            positions = [j + (i - (len(arms) - 1) / 2) * width for j in range(len(labels))]
            ax.bar(positions, [groups[key][arm] for key in labels], width, color=color, label=label,
                   hatch="///" if arm == REFERENCE else None)
            for position, key in zip(positions, labels):
                ax.scatter([position] * len(samples[key][arm]), samples[key][arm],
                           s=10, color=INK, alpha=0.55, zorder=3)
        ax.set_xticks(range(len(labels)), labels.values(), rotation=25, ha="right", fontsize=9)
        ax.set_ylabel(ylabel)
        ax.axhline(0, color=INK, linewidth=0.5)
    # Title lines first, then the legend, then the axes; each title line takes 7% of the height.
    top = 0.99 - 0.07 * (title.count("\n") + 1 if title else 0)
    if title:
        fig.suptitle(title, y=0.99, va="top", fontsize=12)
    fig.legend(*axes[0, 0].get_legend_handles_labels(), loc="upper center", bbox_to_anchor=(0.5, top),
               ncol=len(arms), fontsize=10)
    fig.text(0.5, -0.02, "Bars: medians; dots: individual repetitions.", ha="center", fontsize=9)
    save(fig, name, rect=(0, 0, 1, top - 0.09))


def v5_template_window(row):
    """First fresh-tip work to full surviving-mempool work, not time on an old chain tip."""
    window = row["latency_ns"] - row["first_template_latency_ns"]
    if window < 0:
        raise ValueError(f"Full template precedes first template: {row['_source']}")
    return window / 1e6


def v5_mining(tips, miners, chains, core=False):
    fillings = ("transparent-signed", "orchard", "mixed")
    fixtures = ("empty-2000", *(f"{filling}-{size}" for size in (250000, 1900000) for filling in fillings))
    labels = {(fixture,): fixture.replace("transparent-signed", "Signed transparent").replace("-", "\n")
              for fixture in fixtures}
    metrics = (("Submission to first new-tip template (ms)", lambda row: row["first_template_latency_ns"] / 1e6),
               ("Submission to full template (ms)", v5_latency),
               ("First-to-full template window (ms)", v5_template_window))
    for link in (("settled",) if core else ("fresh", "settled")):
        for temperature in (("warm",) if core else ("cold", "warm")):
            selected = [row for row in tips if row["temperature"] == temperature
                        and row["settle_seconds"] == (0 if link == "fresh" else 20)]
            v5_metric_panels(selected, ("fixture",), labels, metrics, f"tip-template-{link}-{temperature}",
                             title=f"{link.capitalize()} legacy TCP link · {temperature} receiver mempool")
    miner_labels = {key: label for key, label in labels.items() if key[0] in
                    ("empty-2000", "mixed-250000", "mixed-1900000")}
    if core:
        # These are the exact same transparent-payout cells, not a second benchmark.
        miners = [*miners, *(row for row in tips if (row["fixture"],) in miner_labels)]
    for miner in (("transparent", "orchard") if core else ("transparent", "orchard", "sapling")):
        selected = [row for row in miners if row["miner"] == miner]
        title = f"{miner.capitalize()} payout · settled legacy TCP link · warm mempool"
        if miner == "transparent":
            v5_metric_panels(selected, ("fixture",), miner_labels, metrics, f"tip-miner-{miner}", title=title)
        else:
            # These payout modes are unavailable in Zakura with the campaign's NU5 fixtures.
            v5_metric_panels(selected, ("fixture",), miner_labels, metrics, f"tip-miner-{miner}",
                             arms=V5_ZEBRA, title=title + "\nZakura: n/a for these NU5 fixtures")
    chain_labels = {key: label for key, label in labels.items()
                    if key[0] == "empty-2000" or key[0].endswith("-1900000")}
    for temperature in (("cold",) if core else ("cold", "warm")):
        selected = [row for row in chains if row["temperature"] == temperature]
        v5_metric_panels(selected, ("fixture",), chain_labels,
                         (("Submission to B best-tip observation (ms)", lambda row: row["b_best_tip_latency_ns"] / 1e6),
                          ("Submission to C best-tip observation (ms)", lambda row: row["c_best_tip_latency_ns"] / 1e6),
                          ("C minus B observation time (ms; signed)", lambda row: row["b_to_c_observed_ns"] / 1e6)),
                         f"relay-chain-{temperature}",
                         title=f"A–B–C legacy TCP chain · {temperature} mempool · RPC timing, not per-hop CPU")


def render_v5(root, output):
    global OUT
    OUT = output
    arms = v5_campaign(root)
    failed_arms = v5_failed_arms(arms)
    core = json.loads((root / "campaign.json").read_text()).get("profile") == "core"
    # Load every source before rendering, so absent campaigns cannot silently reuse old figures.
    raw = {source: v5_receipts(root, source,
                             (lambda row: row.get("scenario") == "rpc-load") if source == "rpc" else
                             (lambda row: row.get("scenario") == "startup") if source == "startup" else
                             (lambda row: row.get("scenario") == "sync-full") if source == "replay" else
                             (lambda row: row.get("record") == "run") if source == "inclusion" else
                             (lambda row: True), arms=arms, failed_arm=failed_arms.get(source))
           for source in V5_SOURCES}
    sweep = v5_sweep(raw["sweep"], core)
    relays = {}
    for link in ("fresh", "settled"):
        fixtures = ("empty-2000", "transparent-1900000") if core else (
            "empty-2000", "transparent-1900000", "orchard-1900000", "mixed-1900000")
        expected = {(fixture,) for fixture in fixtures}
        medians = v5_medians(v5_groups(raw[f"relay-{link}"], ("fixture",), v5_latency,
                                      expected=expected, repetitions=5 if core else 6))
        relays[link] = {(fixture, arm): value for (fixture,), arms in medians.items()
                        for arm, value in arms.items()}
    empty = v5_empty_tip(raw["empty-tip"], core)
    expected_peers = {(fixture, endpoint) for fixture in
                      ("transparent-signed-1900000", "orchard-1900000", "mixed-1900000")
                      for endpoint in ("cold-submitblock-nominer", "warm-mempool-submitblock", "prepared-submitblock")}
    peers = v5_groups(raw["peer-block"], ("fixture", "endpoint"), lambda row: row["latency_ms"],
                      expected=expected_peers, repetitions=5)
    peer_samples = {(fixture.rsplit("-", 1)[0], endpoint, arm): values
                    for (fixture, endpoint), arms in peers.items() for arm, values in arms.items()}
    rpc = v5_medians(v5_groups(raw["rpc"], ("concurrency",), lambda row: row["successful_rps"],
                              arms=V5_ZEBRA, expected={(1,), (16,), (64,)}, repetitions=3))
    style()
    for endpoint, name in (("admission-to-template", "admission-to-template"),
                           ("cold-submitblock", "cold-submission"), ("prepared-submitblock", "prepared-submission")):
        latency_by_size(endpoint, name, sweep)
    crypto_speedup(sweep)
    relay(relays["fresh"], relays["settled"])
    tip_template(empty)
    rpc_serving([(f"Concurrency {c}", values) for (c,), values in sorted(rpc.items())], V5_ZEBRA)
    comparison = {(row["endpoint"], row["fixture"]):
                  {arm: row[f"{arm}_latency_ms_median"] for arm in V5_ARMS} for row in sweep}
    zebra_zakura_comparison(comparison, relays)
    peer_block(peer_samples, V5_ARMS)
    v5_mining(raw["tip-template"], raw["tip-miner"], raw["relay-chain"], core)
    v5_metric_panels(raw["rpc"], ("concurrency",), {(c,): f"Concurrency {c}" for c, in sorted(rpc)},
                     (("Cell CPU, including warmup (s)", lambda row: row["cpu_s"]),
                      ("Peak cell RSS, including warmup (MiB)", lambda row: row["peak_rss_kib"] / 1024)),
                     "rpc-resources", arms=V5_ZEBRA, repetitions=3)
    v5_metric_panels(raw["startup"], (), {(): "Warm-cache start"},
                     (("Launch to RPC-ready (ms)", v5_latency),
                      ("Idle RSS (MiB)", lambda row: row["idle_rss_kib"] / 1024)),
                     "startup", arms=V5_ZEBRA, repetitions=5 if core else 10)
    v5_metric_panels(raw["replay"], ("actual_start_height", "finalized_tip_height"),
                     {(1750000, 1752000): "Mainnet 1,750,000–1,752,000"},
                     (("Full-verification replay (blocks/s)", lambda row: row["blocks_per_s"]),
                      ("Whole-process CPU (s)", lambda row: row["cpu_s"]),
                      ("Process peak RSS (MiB)", lambda row: row["peak_rss_kib"] / 1024)),
                     "replay", arms=V5_ZEBRA, repetitions=3 if core else 2,
                     title="Isolated local replay · includes startup and flush · warm OS caches")
    v5_metric_panels(raw["inclusion"], ("corpus", "rate"), {("transparent", "100"): "Transparent · 100 tx/s"},
                     (("Measured load CPU, including quiet tail (s)", lambda row: row["cpu_s"]),
                      ("Peak sampled RSS (MiB)", lambda row: row["peak_rss_bytes"] / (1024 * 1024))),
                     "inclusion-resources", arms=V5_ZEBRA if failed_arms else V5_ARMS, repetitions=3,
                     title="Regtest admission with concurrent template probes · whole load, not per-proof cost"
                           + ("\nZakura v1.7.0 omitted: it hit the 600 s timeout in all three repetitions"
                              if failed_arms else ""))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", type=Path, required=True,
                        help="complete V5 campaign directory containing campaign.json and raw receipts")
    parser.add_argument("--output", type=Path, required=True,
                        help="directory for PNG and SVG figures (created if needed)")
    args = parser.parse_args(argv)
    render_v5(args.input, args.output)
    print("figures in", args.output)


if __name__ == "__main__":
    main()
