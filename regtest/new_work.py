#!/usr/bin/env python3
"""Paired long-poll arrival versus back-to-back getbestblockhash RPC polling.

python3 -B regtest/new_work.py BINARY NEW_OUTPUT_DIRECTORY --samples 50

An isolated NU5 Regtest node generates one warmup block, then one block per
sample. Independent persistent HTTP connections observe the long-poll response
and the first best-hash response on that block. Negative
long_poll_after_tip_ms means the template arrived before that RPC observation;
it is not latency from the unobserved internal tip change. Generation is outside
the relative-arrival metric. The LP request is sent before a 300 ms registration
grace; this is not a server-side acknowledgement that it has parked.

The sealed BINARY.build.json receipt is required and its SHA256 checked before
launch. No fixtures, wallets, builds, affinity changes or external node needed.
Startup waits for supported Halo2 preloading. Run only on an otherwise idle
machine. A fresh output directory is required; run.json, samples.jsonl, node
logs and successful observations.json retain provenance. --self-check never
launches a node.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import http.client
import json
from pathlib import Path
import sys
import threading
import time

from front_common import file_hash, save_json, utc_now
import node_bench as h
from sweep.common import Node


class PendingConnection(http.client.HTTPConnection):
    """Signal after request transmission, immediately before waiting for a reply."""
    def __init__(self, address, timeout, sent):
        host, port = address.rsplit(":", 1)
        super().__init__(host, int(port), timeout=timeout)
        self.sent = sent

    def getresponse(self):
        self.sent.set()
        return super().getresponse()


def long_poll(address, initial, timeout, sent):
    rpc = h.RPC(address, timeout=timeout)
    rpc.conn = PendingConnection(address, timeout, sent)
    deadline = time.monotonic() + timeout
    latest = initial
    calls = 0
    try:
        while time.monotonic() < deadline:
            latest = rpc.call("getblocktemplate", [{"longpollid": latest["longpollid"]}])
            ended = time.perf_counter_ns()
            calls += 1
            if latest["previousblockhash"] != initial["previousblockhash"]:
                return dict(end_ns=ended, block=latest["previousblockhash"], calls=calls)
        raise TimeoutError("long poll did not return new-parent work")
    finally:
        rpc.conn.close()


def watch_tip(address, old_tip, timeout, ready):
    rpc = h.RPC(address, timeout=timeout)
    deadline = time.monotonic() + timeout
    calls = 0
    try:
        while time.monotonic() < deadline:
            started = time.perf_counter_ns()
            best = rpc.call("getbestblockhash")
            ended = time.perf_counter_ns()
            calls += 1
            if best != old_tip:
                return dict(start_ns=started, end_ns=ended, block=best, calls=calls)
            ready.set()
        raise TimeoutError("back-to-back best-hash observer did not see a new tip")
    finally:
        rpc.conn.close()


def arrival_row(block, polled, pushed):
    h.require(polled["block"] == pushed["block"] == block,
              "both independent observers must identify the generated block")
    return dict(block=block, tip_poll=polled, long_poll=pushed,
                long_poll_after_tip_ms=(pushed["end_ns"] - polled["end_ns"]) / 1e6,
                tip_poll_rtt_ms=(polled["end_ns"] - polled["start_ns"]) / 1e6)


def self_check():
    poll = dict(start_ns=100_000, end_ns=500_000, block="new", calls=2)
    push = dict(end_ns=300_000, block="new", calls=1)
    row = arrival_row("new", poll, push)
    assert row["long_poll_after_tip_ms"] == -0.2 and row["tip_poll_rtt_ms"] == 0.4
    try:
        arrival_row("other", poll, push)
    except RuntimeError:
        pass
    else:
        raise AssertionError("unmatched observations accepted")
    print("new-work paired-arrival self-check passed")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("binary", type=Path, nargs="?")
    parser.add_argument("output", type=Path, nargs="?")
    parser.add_argument("--build-receipt", type=Path)
    parser.add_argument("--samples", type=int, default=50)
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    if args.self_check:
        return self_check()
    if args.binary is None or args.output is None or args.samples < 1 or not 0 < args.timeout <= 120:
        parser.error("BINARY and OUTPUT required; samples positive; timeout in (0, 120]")
    binary = args.binary.expanduser().resolve(strict=True)
    receipt_path = args.build_receipt or Path(str(binary) + ".build.json")
    build = json.loads(receipt_path.read_text())
    h.require(build.get("source") and file_hash(binary) == build["sha256"], "binary must match its source/build receipt")
    output = args.output.expanduser().resolve()
    output.mkdir(parents=True, exist_ok=False)
    metadata = dict(schema=1, baseline="getbestblockhash", historical=False, build=build,
                    binary=str(binary), build_receipt=str(receipt_path.resolve()),
                    started_at=utc_now(), requested_samples=args.samples, registration_grace_seconds=0.3,
                    invocation=sys.argv, harness_sha256=file_hash(__file__),
                    helper_sha256={str(path): file_hash(path) for path in (Path(h.__file__),
                        Path(sys.modules[Node.__module__].__file__))})
    save_json(output / "run.json", metadata)
    rows = []
    try:
        with Node(str(binary), output / "node", timeout=args.timeout) as node:
            node.rpc.call("generate", [1])  # leave genesis, as in the original probe
            with ThreadPoolExecutor(max_workers=2) as executor, (output / "samples.jsonl").open("x") as stream:
                for index in range(args.samples):
                    initial = node.rpc.call("getblocktemplate")
                    h.require(bool(initial.get("longpollid")), "node must support template long polling")
                    sent, ready = threading.Event(), threading.Event()
                    push = executor.submit(long_poll, node.rpc_addr, initial, args.timeout, sent)
                    poll = executor.submit(watch_tip, node.rpc_addr, initial["previousblockhash"], args.timeout, ready)
                    h.require(sent.wait(args.timeout) and ready.wait(args.timeout), "both observers must be active before generation")
                    time.sleep(0.3)
                    generated_at = utc_now()
                    start = time.perf_counter_ns()
                    generated = node.rpc.call("generate", [1])
                    end = time.perf_counter_ns()
                    h.require(len(generated) == 1, "generate must return exactly one block")
                    row = arrival_row(generated[0], poll.result(timeout=args.timeout), push.result(timeout=args.timeout))
                    row.update(sample=index, generated_at=generated_at, generate_start_ns=start,
                               generate_end_ns=end, preload=node.preload_receipt(start))
                    stream.write(json.dumps(row, sort_keys=True) + "\n")
                    stream.flush()
                    rows.append(row)
        metadata.update(ended_at=utc_now(), success=True, samples=rows)
        save_json(output / "observations.json", metadata)
    except BaseException as error:
        metadata.update(ended_at=utc_now(), success=False, error=repr(error), completed_samples=len(rows))
        raise
    finally:
        save_json(output / "run.json", {key: value for key, value in metadata.items() if key != "samples"})
    print(f"Wrote {len(rows)} paired arrivals to {output / 'observations.json'}")


if __name__ == "__main__":
    main()
