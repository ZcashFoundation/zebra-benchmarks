#!/usr/bin/env python3
"""Offline JSONL to deterministic five-minute and rolling-24h comparison reports."""
import argparse
from collections import Counter, defaultdict
import hashlib
import itertools
import json
import math
from pathlib import Path
import random
import shutil
import statistics

from latency_stats import summary
from workload import COMPONENTS, ParseError, THRESHOLDS, parse_block

WINDOW_MS = 300_000
DAY_MS = 86_400_000
MAX_SKEW_MS = 25
MIN_PAIRS = 20
MIN_CLUSTERS = 12
BOOTSTRAP_SAMPLES = 2000
OUTCOMES = ("accepted", "duplicate", "rejected", "error", "timeout")
EVENTS = ("submitblock_call", "submit_paths", "mempool_relay", "getblocktemplate", "resource_interval", "session")
MILESTONES = ("rpc_round_trip_ms", "submit_return_ms", "tip_ms", "template_ms", "peer_told_ms", "peer_has_ms")


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def finite(value):
    return type(value) in (int, float) and math.isfinite(value) and value >= 0


def record_time(record):
    return max((record[key] for key in ("at_unix_ms", "observed_at_unix_ms", "end_unix_ms")
                if finite(record.get(key))), default=None)


def validate(record):
    if not isinstance(record, dict):
        raise ValueError("record must be an object")
    event = record.get("event")
    if event not in EVENTS:
        return
    times = {"submitblock_call": ("queued_unix_ms", "dispatch_unix_ms", "observed_at_unix_ms"),
             "submit_paths": ("queued_unix_ms", "dispatch_unix_ms", "observed_at_unix_ms"),
             "mempool_relay": ("observed_at_unix_ms",),
             "getblocktemplate": ("at_unix_ms",), "resource_interval": ("start_unix_ms", "end_unix_ms"),
             "session": ("at_unix_ms",)}[event]
    for key in times:
        if type(record.get(key)) is not int or record[key] < 0:
            raise ValueError(f"{event}: {key} must be a nonnegative integer")
    if event == "session":
        if not isinstance(record.get("clients"), dict) or type(record.get("synthetic")) is not bool:
            raise ValueError("session requires clients object and synthetic boolean")
        for target, client in record["clients"].items():
            if not isinstance(target, str) or not isinstance(client, dict) or any(not isinstance(client.get(k), str) for k in ("name", "version")):
                raise ValueError("session client requires target, name and version strings")
        return
    if not isinstance(record.get("target"), str) or not record["target"]:
        raise ValueError(f"{event}: target must be a nonempty string")
    if event == "mempool_relay":
        txid = record.get("txid")
        if not isinstance(txid, str) or len(txid) != 64 or any(c not in "0123456789abcdef" for c in txid):
            raise ValueError("mempool_relay: txid must be lowercase 32-byte hex")
        if (record.get("outcome") not in OUTCOMES or "error" not in record
                or record["error"] is not None and not isinstance(record["error"], str)):
            raise ValueError("mempool_relay: invalid outcome or error")
        if record.get("dispatch_unix_ms") is not None:
            if type(record["dispatch_unix_ms"]) is not int or record["dispatch_unix_ms"] < 0:
                raise ValueError("mempool_relay: invalid dispatch_unix_ms")
            if record["dispatch_unix_ms"] > record["observed_at_unix_ms"]:
                raise ValueError("mempool_relay: response precedes dispatch")
        if "latency_ms" not in record or record["latency_ms"] is not None and not finite(record["latency_ms"]):
            raise ValueError("mempool_relay: invalid latency_ms")
        return
    if event == "submit_paths":
        if any(not isinstance(record.get(k), str) for k in ("client", "version")):
            raise ValueError("submit_paths requires client and version strings")
        block_hash = record.get("hash")
        if not isinstance(block_hash, str) or len(block_hash) != 64 or any(c not in "0123456789abcdef" for c in block_hash):
            raise ValueError("submit_paths: hash must be lowercase 32-byte hex")
        if any(type(record.get(k)) is not int or record[k] < 0 for k in ("height", "block_bytes")):
            raise ValueError("submit_paths: invalid height or block_bytes")
        if record["observed_at_unix_ms"] < record["dispatch_unix_ms"]:
            raise ValueError("submit_paths: response precedes dispatch")
        if record.get("outcome") not in OUTCOMES or type(record.get("catch_up")) is not bool:
            raise ValueError("submit_paths: invalid outcome or catch_up")
        if record.get("detail") is not None and not isinstance(record["detail"], str):
            raise ValueError("submit_paths: invalid detail")
        for key in MILESTONES:
            if key not in record or record[key] is not None and not finite(record[key]):
                raise ValueError(f"submit_paths: invalid {key}")
        return
    if event == "submitblock_call":
        if any(not isinstance(record.get(k), str) for k in ("client", "version")):
            raise ValueError("submitblock_call requires client and version strings")
        if not isinstance(record.get("hash"), str) or len(record["hash"]) != 64 or any(c not in "0123456789abcdef" for c in record["hash"]):
            raise ValueError("submitblock_call: hash must be lowercase 32-byte hex")
        if record.get("outcome") not in OUTCOMES or type(record.get("catch_up")) is not bool:
            raise ValueError("submitblock_call: invalid outcome or catch_up")
        for key in ("height", "block_bytes"):
            if type(record.get(key)) is not int or record[key] < 0:
                raise ValueError(f"submitblock_call: invalid {key}")
        if not finite(record.get("latency_ms")):
            raise ValueError("submitblock_call: latency_ms must be finite and nonnegative")
    elif event == "getblocktemplate":
        if record.get("status") not in ("success", "rpc_error", "timeout", "error"):
            raise ValueError("getblocktemplate: invalid status")
        if not finite(record.get("latency_ms")):
            raise ValueError("getblocktemplate: invalid latency_ms")
        if record["status"] == "success" and any(type(record.get(k)) is not int or record[k] < 0 for k in ("template_height", "transaction_count")):
            raise ValueError("successful getblocktemplate requires height and transaction_count")
    else:
        if type(record.get("valid")) is not bool or (record["valid"] and record["end_unix_ms"] <= record["start_unix_ms"]):
            raise ValueError("resource_interval: invalid interval or validity flag")
        if record["valid"] and (not finite(record.get("rss_bytes")) or not isinstance(record.get("deltas"), dict) or any(not finite(record["deltas"].get(k)) for k in ("cpu_seconds", "read_bytes", "write_bytes"))):
            raise ValueError("valid resource_interval requires RSS and nonnegative deltas")


def load_records(path):
    records = []
    with Path(path).open(encoding="utf-8") as source:
        for line_number, line in enumerate(source, 1):
            if not line.strip():
                continue
            try:
                record = json.loads(line, parse_constant=lambda value: (_ for _ in ()).throw(ValueError(f"invalid JSON number {value}")))
                validate(record)
            except (ValueError, TypeError) as error:
                raise ValueError(f"{path}:{line_number}: {error}") from error
            records.append(record)
    return records


def prepare(records):
    """Select first attempts globally before any window/category filtering."""
    blocks = {}
    identities = {}
    for record in records:
        validate(record)
        advertised = record.get("clients", {}) if record.get("event") == "session" else {}
        if record.get("event") in ("submitblock_call", "submit_paths"):
            advertised = {record["target"]: {"name": record["client"], "version": record["version"]}}
        for target, client in advertised.items():
            identity = (client["name"], client["version"])
            if identities.setdefault(target, identity) != identity:
                raise ValueError(f"incompatible client/version for {target}; use a new target name or separate log")
        if record.get("event") != "submitblock_call":
            continue
        block = blocks.setdefault(record["hash"], {"at": record["queued_unix_ms"], "attempts": [], "first": {}})
        block["at"] = min(block["at"], record["queued_unix_ms"])
        block["attempts"].append(record)
    for block_hash, block in blocks.items():
        block["attempts"].sort(key=lambda r: (r["dispatch_unix_ms"], r["observed_at_unix_ms"]))
        for attempt in block["attempts"]:
            block["first"].setdefault(attempt["target"], attempt)
        first = block["attempts"][0]
        try:
            feature = parse_block(first.get("raw_block_hex"))
            if feature["hash"] != block_hash:
                raise ParseError("record hash does not match serialized block header")
            for attempt in block["attempts"]:
                if attempt.get("raw_block_hex", "").lower() != first["raw_block_hex"].lower() or attempt["block_bytes"] != feature["size_bytes"] or attempt["height"] != first["height"]:
                    raise ParseError("inconsistent raw bytes, size or height across attempts")
        except (ParseError, AttributeError) as error:
            feature = dict.fromkeys(COMPONENTS, None)
            feature.update(hash=block_hash, schema_version=1, source="raw_block_hex",
                           component_scope="non_coinbase", size_bytes=first["block_bytes"],
                           tx_count=None, observed_parent_gap_ms=None, parse_status="error",
                           detail=str(error), labels=["workload:unknown"])
        block["feature"] = feature
    analysis = hashlib.sha256(b"".join(Path(__file__).with_name(name).read_bytes()
                                   for name in ("report.py", "workload.py", "latency_stats.py"))).hexdigest()
    # Windows scan these, not every record: diagnostics dominate long logs.
    sessions = [r for r in records if r.get("event") == "session"]
    telemetry = [r for r in records if r.get("event") in ("getblocktemplate", "resource_interval")]
    paths = {}
    for record in sorted((r for r in records if r.get("event") == "submit_paths"),
                         key=lambda r: (r["dispatch_unix_ms"], r["observed_at_unix_ms"])):
        paths.setdefault((record["hash"], record["target"]), record)
    relays = [r for r in records if r.get("event") == "mempool_relay"]
    return {"records": records, "blocks": blocks, "analysis_sha256": analysis,
            "sessions": sessions, "telemetry": telemetry, "paths": paths, "relays": relays}


def percentile(ordered, fraction):
    if not ordered:
        return None
    index = (len(ordered) - 1) * fraction
    lo, hi = math.floor(index), math.ceil(index)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (index - lo)


def latency(values):
    values = sorted(values)
    stats = summary(values) if values else {"mean": None, "median": None, "mode": None}
    return {"n": len(values), "mean_ms": stats["mean"], "median_ms": stats["median"],
            "mode_ms": stats["mode"],
            "p95_ms": percentile(values, .95) if len(values) >= 100 else None,
            "p99_ms": percentile(values, .99) if len(values) >= 100 else None}


def interval(pairs):
    """Bootstrap the paired mean, resampling occupied UTC five-minute clusters."""
    groups = defaultdict(list)
    for at, delta in sorted(pairs):
        groups[at // WINDOW_MS].append(delta)
    clusters = list(groups.values())
    result = {"clusters": len(clusters), "lower_ms": None, "upper_ms": None}
    if len(pairs) < MIN_PAIRS:
        return "insufficient_pairs", result
    if len(clusters) < MIN_CLUSTERS:
        return "insufficient_clusters", result
    rng = random.Random(0)
    samples = sorted(statistics.fmean(value for _ in clusters for value in rng.choice(clusters))
                     for _ in range(BOOTSTRAP_SAMPLES))
    result.update(lower_ms=percentile(samples, .025), upper_ms=percentile(samples, .975))
    return "exploratory", result


def exclusion(block, targets):
    reasons = set()
    for target in targets:
        attempt = block["first"].get(target)
        if attempt is None:
            reasons.add(f"{target}:missing")
        elif attempt["catch_up"]:
            reasons.add(f"{target}:catch_up")
        elif attempt["outcome"] != "accepted":
            reasons.add(f"{target}:{attempt['outcome']}")
    if block["feature"]["parse_status"] != "success":
        reasons.add("workload_error")
    if not reasons:
        dispatches = [block["first"][t]["dispatch_unix_ms"] for t in targets]
        if max(dispatches) - min(dispatches) > MAX_SKEW_MS:
            reasons.add("dispatch_skew")
    return reasons


def selection(blocks, targets):
    common = [b for b in blocks if targets and not exclusion(b, targets)]
    pairs = []
    for left, right in itertools.combinations(targets, 2):
        eligible, excluded = [], Counter()
        for block in blocks:
            reasons = exclusion(block, (left, right))
            if reasons:
                excluded.update(reasons)
            else:
                eligible.append(block)
        lhs = [b["first"][left]["latency_ms"] for b in eligible]
        rhs = [b["first"][right]["latency_ms"] for b in eligible]
        deltas = [(b["at"], r - l) for b, l, r in zip(eligible, lhs, rhs)]
        differences = [delta for _, delta in deltas]
        evidence, confidence = interval(deltas)
        pairs.append({"left": left, "right": right, "eligible_pairs": len(eligible),
                      "left_latency": latency(lhs), "right_latency": latency(rhs),
                      "paired_mean_right_minus_left_ms": statistics.fmean(differences) if differences else None,
                      "paired_median_right_minus_left_ms": statistics.median(differences) if differences else None,
                      "exclusions": dict(sorted(excluded.items())), "evidence": evidence, "interval": confidence})
    return {"blocks": len(blocks), "common_accepted_blocks": len(common), "pairs": pairs,
            "common_subset": {t: latency([b["first"][t]["latency_ms"] for b in common]) for t in targets}}


def summarize(model, start, end):
    blocks = sorted((b for b in model["blocks"].values() if start <= b["at"] < end),
                    key=lambda b: (b["at"], b["feature"]["hash"]))
    sessions = sorted((r for r in model["sessions"] if r["at_unix_ms"] < end), key=lambda r: r["at_unix_ms"])
    # Carry the most recent preceding session forward, not every historical roster.
    prior = [s for s in sessions if s["at_unix_ms"] < start]
    sessions = prior[-1:] + [s for s in sessions if s["at_unix_ms"] >= start]
    clients = {}
    for session in sessions:
        clients.update(session["clients"])
    attempts = sorted((a for b in blocks for a in b["attempts"]), key=lambda r: (r["dispatch_unix_ms"], r["target"], r["hash"]))
    for attempt in attempts:
        clients.setdefault(attempt["target"], {"name": attempt.get("client"), "version": attempt.get("version")})
    telemetry = [r for r in model["telemetry"] if start <= record_time(r) < end]
    paths = model["paths"]
    for block in blocks:
        for target, attempt in block["first"].items():
            path = paths.get((block["feature"]["hash"], target))
            if (path is None or path["dispatch_unix_ms"] != attempt["dispatch_unix_ms"]
                    or path["height"] != attempt["height"] or path["block_bytes"] != attempt["block_bytes"]
                    or path["outcome"] != attempt["outcome"] or path["catch_up"]):
                continue
            clients.setdefault(target, {"name": path["client"], "version": path["version"]})
    relays = [r for r in model["relays"] if start <= record_time(r) < end]
    for record in relays:
        clients.setdefault(record["target"], {"name": None, "version": None})
    for record in telemetry:
        clients.setdefault(record["target"], {"name": None, "version": None})
    targets = sorted(clients)
    coverage = {}
    for target in targets:
        observed = sum(target in b["first"] for b in blocks)
        own = [a for a in attempts if a["target"] == target]
        outcomes = Counter(a["outcome"] for a in own)
        coverage[target] = {"blocks_observed": observed, "missing_blocks": len(blocks) - observed,
                            "catch_up_attempts": sum(a["catch_up"] for a in own),
                            "all_attempt_outcomes": {key: outcomes[key] for key in OUTCOMES}}
    categories = defaultdict(list)
    for block in blocks:
        for label in block["feature"]["labels"]:
            categories[label].append(block)
    samples, resources = [], []
    for record in sorted(telemetry, key=lambda r: (record_time(r), r["target"])):
        stripped = {k: v for k, v in record.items() if k != "event"}
        if record["event"] == "resource_interval":
            # No prorating CPU/I/O or pretending a partial interval is a full sample.
            if record["start_unix_ms"] < start:
                stripped.update(valid=False, detail="interval crosses report start; not attributable to this window")
            resources.append(stripped)
        else:
            samples.append({"at_unix_ms": record["at_unix_ms"], "status": "unknown",
                            "getblocktemplate": {record["target"]: stripped}})
    def milestone(block, target, key):
        first = block["first"].get(target)
        path = paths.get((block["feature"]["hash"], target))
        if (first is None or path is None or first["dispatch_unix_ms"] != path["dispatch_unix_ms"]
                or first["height"] != path["height"] or first["block_bytes"] != path["block_bytes"]
                or first["outcome"] != path["outcome"] or path["catch_up"]):
            return None
        return path[key]

    milestones = {}
    for key in MILESTONES:
        contestants = {target: latency([value for block in blocks if not exclusion(block, (target,))
                                         if (value := milestone(block, target, key)) is not None])
                       for target in targets}
        comparisons = []
        for left, right in itertools.combinations(targets, 2):
            paired_samples = [(block["at"], r - l) for block in blocks if not exclusion(block, (left, right))
                              if (l := milestone(block, left, key)) is not None
                              if (r := milestone(block, right, key)) is not None]
            differences = [delta for _, delta in paired_samples]
            evidence, confidence = interval(paired_samples)
            comparisons.append({"left": left, "right": right, "eligible_pairs": len(paired_samples),
                                "paired_mean_right_minus_left_ms": statistics.fmean(differences) if differences else None,
                                "paired_median_right_minus_left_ms": statistics.median(differences) if differences else None,
                                "evidence": evidence, "interval": confidence})
        milestones[key] = {"contestants": contestants, "pairs": comparisons}
    relay_acceptance = {}
    for target in targets:
        own = [r for r in relays if r["target"] == target]
        accepted = sum(r["outcome"] == "accepted" for r in own)
        relay_acceptance[target] = {"attempts": len(own), "accepted": accepted,
                                    "acceptance_rate": accepted / len(own) if own else None}
    result = {"schema_version": 1, "taxonomy_version": "zcash-workloads-v1",
              "taxonomy_thresholds": THRESHOLDS, "mode": "live_fed",
              "synthetic": any(s["synthetic"] for s in sessions) if sessions else None,
              "start_unix_ms": start, "end_unix_ms": end, "blocks_observed": len(blocks),
              "endpoint": "submitblock_rpc_ms", "minimum_descriptive_pairs": MIN_PAIRS,
              "max_dispatch_skew_ms": MAX_SKEW_MS, "selection_tags": [],
              "bootstrap": {"cluster_ms": WINDOW_MS, "minimum_clusters": MIN_CLUSTERS,
                            "resamples": BOOTSTRAP_SAMPLES, "seed": 0, "confidence": .95},
              "clients": {t: clients[t] for t in targets}, "coverage": coverage,
              "selection": selection(blocks, targets), "submit_paths": {"milestones": milestones},
              "mempool_relay": relay_acceptance,
              "categories": {tag: selection(group, targets) for tag, group in sorted(categories.items())},
              "features": [b["feature"] for b in blocks],
              "observations": [{k: v for k, v in a.items() if k not in ("raw_block_hex", "event")} for a in attempts],
              "synchronization": {"counts": {"agreement": 0, "disagreement": 0, "unknown": len(samples)},
                                  "samples": samples, "resource_intervals": resources}}
    selected_paths = [paths[(b["feature"]["hash"], target)] for b in blocks for target in b["first"]
                      if (b["feature"]["hash"], target) in paths]
    result["inputs_sha256"] = {"records": digest(attempts + telemetry + sessions + selected_paths + relays)}
    result["analysis_sha256"] = model["analysis_sha256"]
    result["report_id"] = digest(result)
    return result


def make_report(records, start_ms, end_ms):
    return summarize(prepare(records), start_ms, end_ms)


def save_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_bytes(encoded(value) + b"\n")
    temporary.replace(path)


def build_site(records_path, output_path="site"):
    records = load_records(records_path)
    times = [record_time(r) for r in records if record_time(r) is not None]
    if not times:
        raise ValueError("log contains no timestamped records")
    model = prepare(records)
    output = Path(output_path)
    html = Path(__file__).with_name("index.html")
    if not html.is_file():
        raise ValueError(f"missing dashboard source: {html}")
    # ponytail: batch in-memory log; stream into SQLite when logs no longer fit RAM.
    buckets = {b["at"] // WINDOW_MS for b in model["blocks"].values()}
    buckets.update(record_time(r) // WINDOW_MS for r in records if r.get("event") in EVENTS and record_time(r) is not None)
    reports = []
    for bucket in sorted(buckets, reverse=True):
        report = summarize(model, bucket * WINDOW_MS, (bucket + 1) * WINDOW_MS)
        save_json(output / "reports" / report["report_id"] / "report.json", report)
        entry = {k: report[k] for k in ("report_id", "start_unix_ms", "end_unix_ms", "blocks_observed", "clients", "mode", "synthetic")}
        entry["common_accepted_blocks"] = report["selection"]["common_accepted_blocks"]
        reports.append(entry)
    # End is exclusive: include the latest observed millisecond, not wall-clock now.
    end = max(times) + 1
    rolling = summarize(model, max(0, end - DAY_MS), end)
    save_json(output / "rolling.json", rolling)
    shutil.copyfile(html, output / "index.html")
    index = {"schema_version": 1, "latest": reports[0]["report_id"] if reports else None,
             "reports": reports, "rolling": "rolling.json"}
    save_json(output / "index.json", index)
    return index


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("records", type=Path)
    parser.add_argument("--output", type=Path, default=Path("site"))
    args = parser.parse_args()
    try:
        build_site(args.records, args.output)
    except (OSError, ValueError) as error:
        parser.exit(1, f"report: {error}\n")


if __name__ == "__main__":
    main()
