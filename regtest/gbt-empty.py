#!/usr/bin/env python3
"""Steady-state getblocktemplate latency with an empty mempool, the sweep's empty-block path.

Usage: gbt-empty.py <output-dir> <arm>=<binary> [<arm>=<binary> ...]
The first arm is the baseline. Each round starts one fresh node per arm (shuffled order), replays
the sweep's funding chain, makes WARMUP untimed calls and then CALLS timed calls. Prints per-arm
median of round medians and the baseline/arm ratio with a round-level bootstrap 95% interval.
Startup Halo2 preload finishes before replay; per-call receipts accompany the unchanged latency array.
"""
import argparse
import json
import random
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "sweep"))
from common import Node, load_fixtures  # noqa: E402

from node_bench import BENCH_HELP, FIXTURES
ROUNDS, WARMUP, CALLS = 6, 100, 1000


def main():
    parser = argparse.ArgumentParser(description=__doc__, epilog=BENCH_HELP)
    parser.add_argument("output", type=Path)
    parser.add_argument("arms", nargs="+", metavar="ARM=BINARY")
    parser.add_argument("--fixtures", type=Path, default=FIXTURES)
    options = parser.parse_args()
    output = options.output.resolve()
    arms = [item.split("=", 1) for item in options.arms]
    assert len(arms) >= 2 and all(len(arm) == 2 for arm in arms), __doc__
    output.mkdir(parents=True)  # Never overwrite an earlier run.
    _, chain, _ = load_fixtures(options.fixtures)
    medians = {name: [] for name, _ in arms}
    rng = random.Random(20261019)
    with (output / "rounds.jsonl").open("x") as rows:
        for round_index in range(ROUNDS):
            for name, binary in rng.sample(arms, len(arms)):
                with Node(binary, output / f"r{round_index}-{name}", name) as node:
                    node.replay(chain)
                    for _ in range(WARMUP):
                        node.rpc.call("getblocktemplate")
                    latencies, samples = [], []
                    for _ in range(CALLS):
                        started = time.perf_counter_ns()
                        template = node.rpc.call("getblocktemplate")
                        ended = time.perf_counter_ns()
                        latencies.append(ended - started)
                        samples.append(dict(start_ns=started, end_ns=ended))
                        assert not template["transactions"], "mempool must stay empty"
                    for sample in samples:
                        sample["preload"] = node.preload_receipt(sample["start_ns"])
                median = statistics.median(latencies) / 1e6
                medians[name].append(median)
                rows.write(json.dumps(dict(round=round_index, arm=name, binary=binary, median_ms=median,
                                           latencies_ns=latencies, samples=samples)) + "\n")
                rows.flush()
                print(f"round {round_index} {name}: {median:.4f} ms", flush=True)
    base = arms[0][0]
    for name, _ in arms:
        ratios = [b / a for b, a in zip(medians[base], medians[name])]
        boot = sorted(statistics.median(rng.choices(ratios, k=len(ratios))) for _ in range(10_000))
        print(f"{name}: {statistics.median(medians[name]):.4f} ms; {base}/{name} = "
              f"{statistics.median(ratios):.3f}× [{boot[250]:.3f}, {boot[9750]:.3f}]")


if __name__ == "__main__":
    main()
