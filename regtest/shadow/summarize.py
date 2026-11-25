#!/usr/bin/env python3
"""Summarize measured Mainnet shadow-block milestones and mirrored transactions."""

import argparse
import json
import math
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from latency_stats import paired, summary


MILESTONES = ("submit_return_ms", "tip_ms", "template_ms", "peer_told_ms", "peer_has_ms")


def summarize(rows_path, reference=None):
    rows_path = Path(rows_path)
    samples = {}
    result = {"nodes": {}, "paired": {}, "mirror": {}}
    with rows_path.open() as stream:
        for line in stream:
            row = json.loads(line)
            if row.get("measured") is not True:
                continue  # Ancestor warmups are not latency observations.
            node = row["node"]
            key = (row.get("round", 0), row["block_hash"])
            by_milestone = samples.setdefault(node, {name: {} for name in MILESTONES})
            for name in MILESTONES:
                value = row.get(name)
                if value is None:
                    continue
                if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
                    raise ValueError(f"invalid {name} for {node} {key}: {value!r}")
                if key in by_milestone[name]:
                    raise ValueError(f"duplicate {name} for {node} {key}")
                by_milestone[name][key] = value

    if reference is not None and reference not in samples:
        raise ValueError(f"reference node {reference!r} has no measured rows")

    for node in sorted(samples):
        result["nodes"][node] = {}
        print(f"{node} (milliseconds; mean is headline):")
        for name, observations in samples[node].items():
            if observations:
                stats = summary(observations.values())
                result["nodes"][node][name] = stats
                print(f"  {name}: mean {stats['mean']:.3f}, median {stats['median']:.3f}, "
                      f"half-sample mode {stats['mode']:.3f} (n={len(observations)})")
        if reference is not None and node != reference:
            print(f"  paired differences ({node} minus {reference}, milliseconds; negative is faster):")
            result["paired"][node] = {}
            for name, observations in samples[node].items():
                baseline = samples[reference][name]
                diffs = [observations[key] - baseline[key] for key in sorted(observations.keys() & baseline.keys())]
                if diffs:
                    stats = paired(diffs)
                    low, high = stats["mean_ci95"]
                    result["paired"][node][name] = stats
                    print(f"    {name}: mean {stats['mean']:+.3f} "
                          f"(bootstrap 95% CI [{low:+.3f}, {high:+.3f}]), "
                          f"median {stats['median']:+.3f} (pairs={len(diffs)})")
                else:
                    print(f"    {name}: no matched (round, block hash) observations")

    mirrors = {}
    with rows_path.with_name("mirror.jsonl").open() as stream:
        for line in stream:
            row = json.loads(line)
            if "node" not in row:
                continue  # Source fetch/poll errors have no per-node outcome.
            accepted = row["accepted"]
            if not isinstance(accepted, bool):
                raise ValueError(f"invalid mirror acceptance: {accepted!r}")
            counts = mirrors.setdefault(row["node"], [0, 0])
            counts[0] += accepted
            counts[1] += 1
    print("Mirror acceptance (accepted / attempted):")
    for node in sorted(samples.keys() | mirrors.keys()):
        accepted, attempted = mirrors.get(node, (0, 0))
        rate = f"{accepted / attempted:.1%}" if attempted else "n/a"
        result["mirror"][node] = dict(accepted=accepted, attempted=attempted,
                                       acceptance_rate=accepted / attempted if attempted else None)
        print(f"  {node}: {accepted}/{attempted} ({rate})")
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("rows", type=Path, help="rows.jsonl (mirror.jsonl must be alongside it)")
    parser.add_argument("--reference", metavar="LABEL", help="report paired node-minus-reference differences")
    args = parser.parse_args(argv)
    summarize(args.rows, args.reference)


if __name__ == "__main__":
    main()
