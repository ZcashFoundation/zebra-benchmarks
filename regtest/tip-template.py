#!/usr/bin/env python3
"""New tip → first getblocktemplate built on it, for the sweep's empty block.

Usage: tip-template.py <output-dir> <arm>=<binary> [<arm>=<binary> ...]
The first arm is the baseline. Modes (env MODES, default all), each on fresh nodes per sample:
- local: the node accepts the block through submitblock (a pool's own block);
- peer: a sender submits it and the pool's node learns it over a fresh legacy P2P link;
- settled: as peer, after the relay harness's 20 s settle period on the link.
The timer starts at submitblock and stops when the observed node returns a template whose
previousblockhash is the new block; a concurrent loop polls getblocktemplate without sleeping.
Prints per-arm, per-mode median of round medians and baseline/arm with a round-level bootstrap.
Nodes finish supported startup Halo2 preloading before replay; samples include per-node preload receipts.
"""
import argparse
import json
import os
import random
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "sweep"))
import node_bench as h  # noqa: E402
from common import Node, load_fixtures  # noqa: E402

FIXTURES = h.FIXTURES
ROUNDS = int(os.environ.get("TIP_ROUNDS", "6"))
REPS = int(os.environ.get("TIP_REPS", "3"))
if ROUNDS < 1 or REPS < 1:
    raise ValueError("TIP_ROUNDS and TIP_REPS must be positive")
TIMEOUT, SETTLE_S = 120, 20
MODES = tuple(os.environ.get("MODES", "local,peer,settled").split(","))


def link(sender, peer):
    sender.rpc.call("addnode", [peer.p2p_addr, "add"])
    for node in (sender, peer):
        node.wait(lambda: node.rpc.call("getpeerinfo"), "legacy link", TIMEOUT)


def sample(observer, submitter, block):
    assert observer.rpc.call("getblocktemplate")["previousblockhash"] == block["parent"]
    # In local mode both roles are one node; never share its HTTP connection across threads.
    rpc = h.RPC(submitter.rpc_addr)
    started = threading.Event()
    stamp = {}

    def submit():
        stamp["start"] = time.perf_counter_ns()
        started.set()
        return rpc.call("submitblock", [block["raw"]])

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(submit)
        deadline, polls, errors = time.monotonic() + TIMEOUT, 0, 0
        while time.monotonic() < deadline:
            try:
                template = observer.rpc.call("getblocktemplate")
            except Exception:  # A node may refuse templates briefly while the tip changes.
                errors += 1
                continue
            polls += 1
            if template["previousblockhash"] == block["hash"]:
                end = time.perf_counter_ns()
                break
        else:
            raise TimeoutError("no template on the new tip")
        assert future.result(timeout=TIMEOUT) is None, "submitblock accepted"
    rpc.conn.close()
    preload = {'receiver': observer.preload_receipt(stamp["start"])}
    if submitter is not observer:
        preload['sender'] = submitter.preload_receipt(stamp["start"])
    return dict(start_ns=stamp["start"], end_ns=end, latency_ns=end - stamp["start"],
                polls=polls, errors=errors, preload=preload)


def main():
    parser = argparse.ArgumentParser(description=__doc__, epilog=h.BENCH_HELP)
    parser.add_argument("output", type=Path)
    parser.add_argument("arms", nargs="+", metavar="ARM=BINARY")
    parser.add_argument("--fixtures", type=Path, default=FIXTURES)
    options = parser.parse_args()
    if not MODES or set(MODES) - {"local", "peer", "settled"}:
        parser.error("MODES must be a comma-separated subset of local,peer,settled")
    output = options.output.resolve()
    arms = [item.split("=", 1) for item in options.arms]
    assert len(arms) >= 2 and all(len(arm) == 2 for arm in arms), __doc__
    output.mkdir(parents=True)  # Never overwrite an earlier run.
    _, chain, fixtures = load_fixtures(options.fixtures)
    block = next(item for item in fixtures if item["id"] == "empty-2000")
    rng = random.Random(20261019)
    medians = {(mode, name): [] for mode in MODES for name, _ in arms}
    with (output / "samples.jsonl").open("x") as rows:
        for round_index in range(ROUNDS):
            for mode, (name, binary) in rng.sample([(m, a) for m in MODES for a in arms], len(MODES) * len(arms)):
                latencies = []
                for rep in range(REPS):
                    directory = output / f"r{round_index}-{mode}-{name}-{rep}"
                    with Node(binary, directory / "pool", name) as pool:
                        pool.replay(chain)
                        if mode == "local":
                            result = sample(pool, pool, block)
                        else:
                            with Node(binary, directory / "sender", name) as sender:
                                sender.replay(chain)
                                link(sender, pool)
                                if mode == "settled":
                                    time.sleep(SETTLE_S)
                                    assert len(pool.rpc.call("getpeerinfo")) == 1, "link survived settling"
                                result = sample(pool, sender, block)
                    latencies.append(result["latency_ns"] / 1e6)
                    rows.write(json.dumps(dict(round=round_index, mode=mode, arm=name, rep=rep, **result)) + "\n")
                    rows.flush()
                medians[mode, name].append(statistics.median(latencies))
                print(f"round {round_index} {mode} {name}: {latencies}", flush=True)
    base = arms[0][0]
    for mode in MODES:
        for name, _ in arms:
            ratios = [b / a for b, a in zip(medians[mode, base], medians[mode, name])]
            boot = sorted(statistics.median(rng.choices(ratios, k=len(ratios))) for _ in range(10_000))
            print(f"{mode} {name}: {statistics.median(medians[mode, name]):.3f} ms; {base}/{name} = "
                  f"{statistics.median(ratios):.3f}× [{boot[250]:.3f}, {boot[9750]:.3f}]")


if __name__ == "__main__":
    main()
