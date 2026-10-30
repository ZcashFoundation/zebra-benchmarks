#!/usr/bin/env python3
"""The sweep's empty-block admission-to-template sample, uninstrumented, on many fresh nodes.

Usage: empty-admission.py <output-dir> <reps> <label>:<arm>=<binary> [...]
Calls run_sweep.case exactly as the sweep does (timeout 600 s, 1 ms polls), one fresh node per
sample, interleaved by repetition. The first label is the baseline. Prints per-label medians and
the median difference with a bootstrap 95% interval. DELAY_MS (default 0) idles that long after
the replay barrier, before the timed call, to separate background work from request-path cost.
The shared case waits for startup Halo2 preload before replay and returns its receipt with each sample.
"""
import argparse
import os
import time
import json
import random
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "sweep"))
import run_sweep  # noqa: E402
from common import load_fixtures  # noqa: E402

from node_bench import BENCH_HELP, FIXTURES


def main():
    parser = argparse.ArgumentParser(description=__doc__, epilog=BENCH_HELP)
    parser.add_argument("output", type=Path)
    parser.add_argument("reps", type=int)
    parser.add_argument("arms", nargs="+", metavar="LABEL:ARM=BINARY")
    parser.add_argument("--fixtures", type=Path, default=FIXTURES)
    options = parser.parse_args()
    if options.reps < 1:
        parser.error("reps must be positive")
    output, reps = options.output.resolve(), options.reps
    arms = []
    for item in options.arms:
        label, rest = item.split(":", 1)
        arm, binary = rest.split("=", 1)
        arms.append((label, arm, Path(binary)))
    assert len(arms) >= 2, __doc__
    output.mkdir(parents=True)  # Never overwrite an earlier run.
    delay = float(os.environ.get("DELAY_MS", "0")) / 1000
    if delay:
        replay = run_sweep.Node.replay

        def replay_then_idle(node, chain):
            replay(node, chain)
            time.sleep(delay)

        run_sweep.Node.replay = replay_then_idle
    _, chain, fixtures = load_fixtures(options.fixtures)
    fixture = next(item for item in fixtures if item["id"] == "empty-2000")
    args = argparse.Namespace(timeout=600, poll_ms=1.0)
    latencies = {label: [] for label, _, _ in arms}
    rng = random.Random(20261019)
    with (output / "samples.jsonl").open("x") as rows:
        for rep in range(reps):
            # Shuffled per repetition: a fixed order confounds arm with position after the
            # previous node's shutdown.
            for position, (label, arm, binary) in enumerate(rng.sample(arms, len(arms))):
                result = run_sweep.case(arm, binary, output / f"{label}-{rep}", fixture, chain, args,
                                        "admission-to-template")
                latencies[label].append(result["latency_ns"] / 1e6)
                rows.write(json.dumps(dict(rep=rep, position=position, label=label, arm=arm, **result))
                           + "\n")
                rows.flush()
    base = arms[0][0]
    for label, _, _ in arms:
        a, b = latencies[base], latencies[label]
        boot = sorted(statistics.median(rng.choices(b, k=len(b))) - statistics.median(rng.choices(a, k=len(a)))
                      for _ in range(10_000))
        print(f"{label}: {statistics.median(b):.4f} ms; minus {base}: "
              f"{statistics.median(b) - statistics.median(a):+.4f} [{boot[250]:+.4f}, {boot[9750]:+.4f}] n={len(b)}")


if __name__ == "__main__":
    main()
