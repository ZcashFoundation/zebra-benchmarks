#!/usr/bin/env python3
"""Closed-loop Mainnet RPC load, warm-cache startup, and idle RSS comparison.

Run from the benchmark worktree. --state must be a stopped, consistent Mainnet
state.cache_dir snapshot on the output filesystem; reflinks are mandatory.
--arm is LABEL=BINARY, checked against --provenance. --weights accepts comma-
separated VARIANT=WEIGHT overrides; all variants default to weight 1 (0 disables).
Request timestamps use perf_counter_ns. Throughput includes draining requests
started before the measurement deadline; errors remain in both counts and tails.
Each client has one persistent HTTP connection; buffered request journals are
merged after each cell, so the central observation writer is outside the timer.
Capability probes use the same corpus requests as load. UTXO queries request one
page of at most 1000 entries, starting at the sampled address's corpus height.
Load starts after startup Halo2 preload; startup latency still ends at RPC readiness.
Startup and load receipts record preload completion relative to their measured start.

--smoke uses concurrency 1,2, two repetitions, one restart, four corpus heights,
0.2 s warmup, 1 s load, and 1 s idle. --self-test starts no processes.

Verified against zebra source: zebra-rpc/src/methods.rs:228,253-257,307-336,
343-344,388-389,428-434,467-468,478-502,523-524,718-719; UTXO paging fields:
methods.rs:4013-4027; transparent addresses:
zebra-rpc/src/methods/types/transaction.rs:579-603. Network isolation uses empty
seed lists and a fresh isolated peer cache (zebra-network/src/config.rs:264-317),
with a loopback-only listener and getpeerinfo checks. A zero peer target is not
isolation: config.rs:870-884 resets it. RPC keys: zebra-rpc/src/config/rpc.rs:31,96.
"""

import argparse
from array import array
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import json
import math
from pathlib import Path
import random
import shutil
import signal
import threading
import time
import traceback

from node_bench import RPC, pct, require, summary, BENCH_HELP
from front_common import Campaign, MainnetNode, interleave, load_arms, reflink_copy


VARIANTS = {
    "getblock-0": "getblock",
    "getblock-1": "getblock",
    "getblockheader": "getblockheader",
    "getrawtransaction": "getrawtransaction",
    "getaddressbalance": "getaddressbalance",
    "getaddressutxos": "getaddressutxos",
    "getaddresstxids": "getaddresstxids",
    "getbestblockhash": "getbestblockhash",
    "getblockchaininfo": "getblockchaininfo",
    "z_gettreestate": "z_gettreestate",
    "getblocksubsidy": "getblocksubsidy",
}
ADDRESS_METHODS = {"getaddressbalance", "getaddressutxos", "getaddresstxids"}


def encode_request(method, params=None):
    return json.dumps(
        {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or []},
        separators=(",", ":"),
    ).encode("utf-8")


class RPCError(RuntimeError):
    def __init__(self, kind, detail):
        self.kind, self.detail = kind, detail
        super().__init__(f"{kind}: {detail}")


class LoadRPC(RPC):
    """Reuse RPC's connection, but reject HTTP errors as well as JSON-RPC errors."""

    def call(self, method, params=None):
        return self.encoded(encode_request(method, params))

    def encoded(self, body):
        try:
            self.conn.request("POST", "/", body, {"Content-Type": "application/json"})
            response = self.conn.getresponse()
            raw = response.read()
            if not 200 <= response.status < 300:
                raise RPCError("http", {"status": response.status, "body": raw.decode("utf-8", "replace")})
            result = json.loads(raw)
            if not isinstance(result, dict) or result.get("id") != 1:
                raise RPCError("protocol", "invalid JSON-RPC response or id")
        except Exception:
            self.conn.close()
            raise
        if result.get("error") is not None:
            raise RPCError("jsonrpc", result["error"])
        if "result" not in result:
            raise RPCError("protocol", "response has neither result nor error")
        return result["result"]


def error_info(error):
    if isinstance(error, RPCError):
        return {"kind": error.kind, "detail": error.detail}
    return {"kind": type(error).__name__, "detail": str(error)}


def missing(error):
    return (isinstance(error, RPCError) and error.kind == "jsonrpc"
            and isinstance(error.detail, dict) and error.detail.get("code") == -32601)


def sampled_heights(tip, count, seed):
    require(tip >= 1, "a synced non-genesis Mainnet state is required")
    return random.Random(seed).sample(range(1, tip + 1), min(tip, count))


def build_corpus(rpc, tip, count, seed, supported):
    supported = set(supported)
    heights = sampled_heights(tip, count, seed)
    corpus = dict(seed=seed, tip=tip, heights=heights, blocks=[], transactions=[], addresses=[])
    if "getblock" not in supported:
        return corpus
    rng = random.Random(seed)
    addresses = {}
    for height in heights:
        try:
            block = rpc.call("getblock", [str(height), 1])
        except RPCError as exc:
            if not missing(exc):
                raise
            return corpus
        txids = rng.sample(block["tx"], min(4, len(block["tx"])))
        corpus["blocks"].append(dict(height=height, hash=block["hash"], txids=txids))
        for txid in txids:
            corpus["transactions"].append(dict(txid=txid, height=height))
            if "getrawtransaction" not in supported:
                continue
            try:
                tx = rpc.call("getrawtransaction", [txid, 1])
            except RPCError as exc:
                if not missing(exc):
                    raise
                supported.discard("getrawtransaction")
                continue
            for output in tx["vout"]:
                for address in output["scriptPubKey"].get("addresses") or []:
                    addresses.setdefault(address, height)
    corpus["addresses"] = [dict(address=a, height=h) for a, h in sorted(addresses.items())]
    return corpus


def request_pool(corpus, variant, height_span):
    method = VARIANTS[variant]
    if variant.startswith("getblock-"):
        params = [[str(h), int(variant[-1])] for h in corpus["heights"]]
    elif method == "getblockheader":
        params = [[str(h), True] for h in corpus["heights"]]
    elif method == "getrawtransaction":
        params = [[t["txid"], 1] for t in corpus["transactions"]]
    elif method in ADDRESS_METHODS:
        params = []
        for item in corpus["addresses"]:
            value = {"addresses": [item["address"]]}
            if method == "getaddresstxids":
                start = max(1, item["height"] - height_span // 2)
                value.update(start=start, end=min(corpus["tip"], start + height_span - 1))
            elif method == "getaddressutxos":
                value.update(startHeight=item["height"], maxEntries=1000)
            params.append([value])
    elif method == "z_gettreestate":
        params = [[str(h)] for h in corpus["heights"]]
    elif method == "getblocksubsidy":
        params = [[h] for h in corpus["heights"]]
    else:
        params = [[]]
    return [encode_request(method, p) for p in params]


def parse_weights(text):
    weights = dict.fromkeys(VARIANTS, 1.0)
    if text:
        for assignment in text.split(","):
            key, value = assignment.split("=", 1)
            require(key in weights, "unknown method variant: " + key)
            number = float(value)
            require(math.isfinite(number) and number >= 0, "weights must be finite and nonnegative")
            weights[key] = number
    require(any(weights.values()) and math.isfinite(sum(weights.values())),
            "a positive finite total method weight is required")
    return weights


def latency_stats(values, errors, elapsed_s):
    return dict(
        requests=len(values), errors=errors, ok=len(values) - errors,
        throughput_rps=len(values) / elapsed_s,
        successful_rps=(len(values) - errors) / elapsed_s,
        p50_ns=pct(values, 50) if values else None,
        p90_ns=pct(values, 90) if values else None,
        p99_ns=pct(values, 99) if values else None,
    )


def probe_methods(rpc, campaign, identity, corpus, height_span):
    supported, visited = set(), set()
    for variant, method in VARIANTS.items():
        if method in visited:
            continue
        visited.add(method)
        pool = request_pool(corpus, variant, height_span)
        start = time.perf_counter_ns()
        row = dict(identity, scenario="rpc-capability", method=method, variant=variant,
                   start_ns=start, supported=None, status="ok")
        failure = None
        if not pool:
            row["exclusion"] = "no matching corpus samples from the first arm"
        else:
            row["params"] = json.loads(pool[0])["params"]
            try:
                rpc.encoded(pool[0])
            except Exception as exc:
                row["supported"] = False
                if missing(exc):
                    row["exclusion"] = error_info(exc)
                else:
                    row.update(status="error", error=error_info(exc))
                    failure = exc
            else:
                supported.add(method)
                row["supported"] = True
        end = time.perf_counter_ns()
        campaign.observe(dict(row, end_ns=end, latency_ns=end - start))
        if failure is not None:
            raise failure
    return supported


def check_isolation(rpc, tip=None):
    require(rpc.call("getpeerinfo") == [], "isolated node unexpectedly has peers")
    height = rpc.call("getblockcount")
    try:
        block_hash = rpc.call("getbestblockhash")
    except RPCError as exc:
        if not missing(exc):
            raise
        block_hash = None
    if tip is not None:
        require(height == tip[0] and (block_hash is None or tip[1] is None or block_hash == tip[1]),
                "all arms and repetitions must use the identical fixed chain tip")
    return height, block_hash or (tip[1] if tip else None)


@contextmanager
def running_node(arm, directory, state, args):
    edits = [("state", "cache_dir", json.dumps(str(state)))]
    node = MainnetNode(arm.path, directory, edits)
    rpc = None
    try:
        node.start()
        ready = node.wait_rpc_ready(args.startup_timeout_s)
        rpc = LoadRPC(node.rpc_addr, timeout=args.request_timeout_s)
        yield node, rpc, ready
    finally:
        if rpc is not None:
            rpc.conn.close()
        node.stop(timeout=args.shutdown_timeout_s)


def load_cell(node, corpus, variants, weights, concurrency, identity, directory, args, campaign):
    directory.mkdir(parents=True)
    pools = {v: request_pool(corpus, v, args.address_height_span) for v in variants}
    cumulative, total = [], 0
    for variant in variants:
        total += weights[variant]
        cumulative.append(total)
    origin = {}
    stop = threading.Event()

    def clock_start():
        origin["warmup_start_ns"] = time.perf_counter_ns()
        origin["start_ns"] = origin["warmup_start_ns"] + round(args.warmup_s * 1e9)
        origin["deadline_ns"] = origin["start_ns"] + round(args.duration_s * 1e9)

    barrier = threading.Barrier(concurrency + 1, action=clock_start)

    def worker(client):
        rng = random.Random(args.seed + identity["repetition"] * 1000003 + concurrency * 1009 + client)
        rpc = LoadRPC(node.rpc_addr, timeout=args.request_timeout_s)
        try:
            with (directory / f"client-{client}.jsonl").open("w", buffering=256 * 1024) as stream:
                barrier.wait(timeout=args.request_timeout_s)
                sequence = 0
                while not stop.is_set():
                    variant = rng.choices(variants, cum_weights=cumulative)[0]
                    body = rng.choice(pools[variant])
                    start = time.perf_counter_ns()
                    if start >= origin["deadline_ns"]:
                        break
                    error = None
                    try:
                        rpc.encoded(body)
                    except Exception as exc:
                        error = error_info(exc)
                    end = time.perf_counter_ns()
                    row = dict(client=client, sequence=sequence, method=VARIANTS[variant],
                               variant=variant, start_ns=start, end_ns=end, latency_ns=end - start,
                               phase="warmup" if start < origin["start_ns"] else "measured",
                               ok=error is None, status="ok" if error is None else "error", error=error)
                    stream.write(json.dumps(row, separators=(",", ":")) + "\n")
                    sequence += 1
        except BaseException:
            stop.set()
            barrier.abort()
            raise
        finally:
            rpc.conn.close()

    samples = []
    values = defaultdict(lambda: array("Q"))
    errors = defaultdict(int)
    warmup_requests = warmup_errors = 0
    measured_end = None
    worker_errors = []

    def sample_resource():
        stamp = time.perf_counter_ns()
        sample = node.sample()
        samples.append(sample)
        campaign.observe(dict(identity, scenario="rpc-resources", start_ns=stamp,
                              end_ns=time.perf_counter_ns(), **sample))
        require(node.proc.poll() is None, "node exited during RPC load")

    try:
        sample_resource()
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(worker, client) for client in range(concurrency)]
            try:
                barrier.wait(timeout=args.request_timeout_s)
                while not all(future.done() for future in futures):
                    sample_resource()
                    time.sleep(0.25)
            finally:
                stop.set()
                for future in futures:
                    try:
                        future.result()
                    except BaseException as exc:
                        worker_errors.append(repr(exc))
        sample_resource()
    finally:
        # Merge even interrupted cells: completed requests must not disappear.
        for client in range(concurrency):
            path = directory / f"client-{client}.jsonl"
            if not path.exists():
                continue
            with path.open() as stream:
                for line in stream:
                    row = json.loads(line)
                    campaign.observe(dict(identity, scenario="rpc-request", **row))
                    if row["phase"] == "warmup":
                        warmup_requests += 1
                        warmup_errors += not row["ok"]
                        continue
                    measured_end = max(measured_end or 0, row["end_ns"])
                    for key in ("overall", row["method"], "variant:" + row["variant"]):
                        values[key].append(row["latency_ns"])
                        errors[key] += not row["ok"]
            path.unlink()
    require(not worker_errors, "RPC client failures: " + repr(worker_errors))
    require(measured_end is not None, "no requests started in the measurement window")
    elapsed_s = (max(origin["deadline_ns"], measured_end) - origin["start_ns"]) / 1e9
    row = dict(identity, scenario="rpc-load", start_ns=origin["start_ns"],
               end_ns=max(origin["deadline_ns"], measured_end), elapsed_s=elapsed_s,
               duration_s=args.duration_s, warmup_s=args.warmup_s,
               warmup_requests=warmup_requests, warmup_errors=warmup_errors,
               peak_rss_kib=max(s["rss_kib"] for s in samples),
               hwm_kib=max(s["hwm_kib"] for s in samples),
               steady_rss_kib=samples[-1]["rss_kib"],
               cpu_s=samples[-1]["cpu_s"] - samples[0]["cpu_s"],
               cpu_scope="whole cell including warmup",
               status="error" if errors["overall"] or warmup_errors else "ok",
               methods={m: latency_stats(values[m], errors[m], elapsed_s)
                        for m in dict.fromkeys(VARIANTS[v] for v in variants)},
               variants={v: latency_stats(values["variant:" + v], errors["variant:" + v], elapsed_s)
                         for v in variants},
               **latency_stats(values["overall"], errors["overall"], elapsed_s))
    row.update(node.preload_receipt(origin["start_ns"]))
    campaign.observe(row)
    if row["errors"] or warmup_errors:
        campaign.fail(directory.name, dict(row, error="RPC request failures; see observations.jsonl"))
    return row


def print_summary(rows):
    groups = defaultdict(list)
    for row in rows:
        if row["scenario"] == "startup":
            groups[(row["arm"], "startup_ms")].append(row["latency_ns"] / 1e6)
            groups[(row["arm"], "idle_rss_kib")].append(row["idle_rss_kib"])
        elif row["scenario"] == "rpc-load":
            key = (row["arm"], "rpc", row["concurrency"])
            groups[key + ("overall_rps",)].append(row["throughput_rps"])
            for method, stats in row["methods"].items():
                groups[key + (method, "rps")].append(stats["throughput_rps"])
                if stats["p50_ns"] is not None:
                    groups[key + (method, "p50_ms")].append(stats["p50_ns"] / 1e6)
    for key, values in sorted(groups.items()):
        stats = summary(values)
        print(" ".join(map(str, key)),
              f"{stats['median']:.3f} [{stats['min']:.3f}, {stats['max']:.3f}] n={stats['n']}")


def self_test():
    assert [pct(list(range(1, 102)), p) for p in (50, 90, 99)] == [51, 91, 100]
    stats = latency_stats([10, 20, 30, 40], 1, 2)
    assert (stats["p50_ns"], stats["p90_ns"], stats["p99_ns"]) == (30, 40, 40)
    assert stats["throughput_rps"] == 2 and stats["successful_rps"] == 1.5
    assert latency_stats([], 0, 1)["p50_ns"] is None
    assert sampled_heights(100, 8, 42) == [82, 15, 4, 95, 36, 32, 29, 18]
    assert sampled_heights(100, 8, 42) == sampled_heights(100, 8, 42)
    assert len(set(sampled_heights(3, 8, 42))) == 3

    class CorpusRPC:
        def call(self, method, params):
            if method == "getblock":
                return dict(hash="block-" + params[0], tx=[params[0] + "-" + str(i) for i in range(8)])
            assert method == "getrawtransaction" and params[1] == 1
            return dict(vout=[dict(scriptPubKey=dict(addresses=["t1-" + params[0]]))])

    supported = set(VARIANTS.values())
    first = build_corpus(CorpusRPC(), 100, 8, 42, supported)
    assert first == build_corpus(CorpusRPC(), 100, 8, 42, supported)
    assert first != build_corpus(CorpusRPC(), 100, 8, 43, supported)
    assert len(first["transactions"]) == len(first["addresses"]) == 32
    corpus = dict(tip=100, heights=[50], transactions=[dict(txid="abc", height=50)],
                  addresses=[dict(address="t1example", height=50)])
    expected = {
        "getblock-0": ["50", 0], "getblock-1": ["50", 1],
        "getblockheader": ["50", True], "getrawtransaction": ["abc", 1],
        "getaddressbalance": [{"addresses": ["t1example"]}],
        "getaddressutxos": [{"addresses": ["t1example"], "startHeight": 50, "maxEntries": 1000}],
        "getaddresstxids": [{"addresses": ["t1example"], "start": 45, "end": 54}],
        "z_gettreestate": ["50"], "getblocksubsidy": [50],
        "getbestblockhash": [], "getblockchaininfo": [],
    }
    for variant, params in expected.items():
        assert json.loads(request_pool(corpus, variant, 10)[0]) == dict(
            jsonrpc="2.0", id=1, method=VARIANTS[variant], params=params)
    assert parse_weights("getblock-0=2,getblock-1=0")["getblock-0"] == 2
    assert missing(RPCError("jsonrpc", {"code": -32601}))
    assert not missing(RPCError("jsonrpc", {"code": -32602}))
    assert b'["50",true]' in request_pool(corpus, "getblockheader", 10)[0]
    assert b'["abc",1]' in request_pool(corpus, "getrawtransaction", 10)[0]

    from types import SimpleNamespace

    class ProbeRPC:
        def __init__(self, failure=None):
            self.failure = failure

        def encoded(self, body):
            request = json.loads(body)
            method = request["method"]
            variant = "getblock-0" if method == "getblock" else method
            assert request["params"] == expected[variant]
            if self.failure is not None and method == "getblock":
                raise self.failure

    rows = []
    assert probe_methods(ProbeRPC(), SimpleNamespace(observe=rows.append), {}, corpus, 10) == supported
    assert all(row["supported"] and row["status"] == "ok" and "error" not in row for row in rows)
    absent = RPCError("jsonrpc", {"code": -32601, "message": "Method not found"})
    assert probe_methods(ProbeRPC(absent), SimpleNamespace(observe=lambda row: None),
                         {}, corpus, 10) == supported - {"getblock"}
    invalid = RPCError("jsonrpc", {"code": -1, "message": "Invalid params", "data": "No more params"})
    rows = []
    try:
        probe_methods(ProbeRPC(invalid), SimpleNamespace(observe=rows.append), {}, corpus, 10)
    except RPCError as exc:
        assert exc is invalid and rows[-1]["status"] == "error"
    else:
        raise AssertionError("a real capability request error must fail")

    class Connection:
        def __init__(self):
            self.requests, self.closes = 0, 0
            self.responses = iter([
                (503, dict(id=1, result="not a success")),
                (200, dict(id=1, error=dict(code=-32601, message="missing"))),
                (200, dict(id=1, result="ok")),
            ])

        def request(self, *args):
            self.requests += 1

        def getresponse(self):
            status, body = next(self.responses)
            return SimpleNamespace(status=status, read=lambda: json.dumps(body).encode())

        def close(self):
            self.closes += 1

    rpc = LoadRPC.__new__(LoadRPC)
    rpc.conn = Connection()
    for kind in ("http", "jsonrpc"):
        try:
            rpc.call("getbestblockhash")
        except RPCError as error:
            assert error_info(error)["kind"] == kind
        else:
            raise AssertionError("error response accepted")
    assert rpc.call("getbestblockhash") == "ok"
    assert rpc.conn.requests == 3 and rpc.conn.closes == 1
    print("rpc_load self-test passed")


def arguments():
    parser = argparse.ArgumentParser(description=__doc__, epilog=BENCH_HELP, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--provenance", type=Path)
    parser.add_argument("--arm", action="append", default=[], metavar="LABEL=BINARY")
    parser.add_argument("--state", type=Path, help="consistent Mainnet state.cache_dir snapshot")
    parser.add_argument("--concurrency", default="1,16,64")
    parser.add_argument("--duration-s", type=float, default=60)
    parser.add_argument("--warmup-s", type=float, default=10)
    parser.add_argument("--restarts", type=int, default=5)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--weights", default="", help="comma-separated VARIANT=WEIGHT overrides")
    parser.add_argument("--seed", type=int, default=20261021)
    parser.add_argument("--corpus-size", type=int, default=128)
    parser.add_argument("--address-height-span", type=int, default=1000)
    parser.add_argument("--startup-timeout-s", type=float, default=600)
    parser.add_argument("--request-timeout-s", type=float, default=30)
    parser.add_argument("--shutdown-timeout-s", type=float, default=120)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        return args
    for key in ("provenance", "state", "output"):
        if getattr(args, key) is None:
            parser.error("--" + key + " is required")
    if not args.arm:
        parser.error("at least one --arm LABEL=BINARY is required")
    args.concurrency = [int(n) for n in args.concurrency.split(",")]
    args.idle_s = 60.0
    if args.smoke:
        args.concurrency = [1, 2]
        args.duration_s, args.warmup_s, args.idle_s = 1.0, 0.2, 1.0
        args.restarts, args.repetitions, args.corpus_size = 1, 2, 4
    require(args.concurrency and min(args.concurrency) >= 1 and
            len(set(args.concurrency)) == len(args.concurrency), "concurrency must contain distinct positive integers")
    for key in ("restarts", "repetitions", "corpus_size", "address_height_span"):
        require(getattr(args, key) > 0, key + " must be positive")
    for key in ("duration_s", "request_timeout_s", "startup_timeout_s", "shutdown_timeout_s"):
        require(math.isfinite(getattr(args, key)) and getattr(args, key) > 0, key + " must be finite and positive")
    require(math.isfinite(args.warmup_s) and args.warmup_s >= 0, "warmup must be finite and nonnegative")
    args.weights = parse_weights(args.weights)
    args.state, args.output = args.state.resolve(), args.output.resolve()
    require(args.state.is_dir(), "state snapshot is not a directory")
    require(args.state != args.output and args.state not in args.output.parents and
            args.output not in args.state.parents, "state and output directories must not contain each other")
    return args


def main():
    args = arguments()
    if args.self_test:
        self_test()
        return
    arms = load_arms(args.provenance, args.arm)
    by_label = {arm.label: arm for arm in arms}
    labels = list(by_label)
    require(len(labels) == len(arms), "duplicate arm labels")
    manifest_args = {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()}
    manifest_args.update(
        startup_order=[interleave(labels, n) for n in range(args.restarts)],
        load_order=[dict(repetition=r, arms=interleave(labels, r),
                         concurrency={a: interleave(args.concurrency, r + labels.index(a)) for a in labels})
                    for r in range(args.repetitions)],
        startup_cache_policy="one unmeasured discovery start per arm, then warm-cache interleaved restarts; no cache drops",
        request_record_policy="all warmup and measured requests; tails include errors; throughput includes drain",
        state_copy_policy="one mandatory reflink per arm; reused across starts; deleted at end",
    )
    campaign = Campaign(args.output, manifest_args, arms)
    states = {a: args.output / "nodes" / ("state-" + str(i)) for i, a in enumerate(labels)}
    rollups, supports, corpus = [], {}, None
    success = False
    context = dict(scenario="setup", repetition=0)

    def interrupted(signum, _frame):
        raise KeyboardInterrupt("signal " + str(signum))

    old_term = signal.signal(signal.SIGTERM, interrupted)
    try:
        for label in labels:
            context = dict(arm=label, scenario="reflink", repetition=0)
            reflink_copy(args.state, states[label])
        tip = None
        for index, label in enumerate(labels):
            context = dict(arm=label, scenario="discovery", repetition=0, arm_order=index)
            with running_node(by_label[label], args.output / "nodes" / f"discover-{index}",
                              states[label], args) as (node, rpc, ready):
                tip = check_isolation(rpc, tip)
                if corpus is None:
                    corpus = build_corpus(rpc, tip[0], args.corpus_size, args.seed, set(VARIANTS.values()))
                    corpus.update(tip_hash=tip[1], source_arm=label)
                    (args.output / "corpus.json").write_text(json.dumps(corpus, indent=2) + "\n")
                supports[label] = probe_methods(rpc, campaign, context, corpus, args.address_height_span)
                check_isolation(rpc, tip)
        exclusions = []
        variants = []
        for variant, method in VARIANTS.items():
            pool = request_pool(corpus, variant, args.address_height_span)
            unavailable = [a for a in labels if method not in supports[a]] if pool else []
            reason = None
            if not pool:
                reason = "no matching corpus samples from the first arm"
            elif unavailable:
                reason = "method missing from arm(s): " + ", ".join(unavailable)
            elif not args.weights[variant]:
                reason = "zero configured weight"
            if reason:
                exclusions.append(dict(variant=variant, method=method, reason=reason, missing_arms=unavailable))
            else:
                variants.append(variant)
        (args.output / "method-mix.json").write_text(json.dumps(dict(
            requested_weights=args.weights, active_weights={v: args.weights[v] for v in variants},
            exclusions=exclusions, supports={a: sorted(s) for a, s in supports.items()},
        ), indent=2) + "\n")
        require(variants, "no usable RPC methods remain")
        for repetition in range(args.restarts):
            for order, label in enumerate(interleave(labels, repetition)):
                context = dict(arm=label, scenario="startup", repetition=repetition, arm_order=order)
                directory = args.output / "nodes" / f"startup-{repetition}-{order}"
                with running_node(by_label[label], directory, states[label], args) as (node, rpc, ready):
                    idle_at = ready + round(args.idle_s * 1e9)
                    time.sleep(max(0, (idle_at - time.perf_counter_ns()) / 1e9))
                    sampled_at = time.perf_counter_ns()
                    sample = node.sample()
                    require(node.proc.poll() is None, "node exited before idle RSS sample")
                    check_isolation(rpc, tip)
                    row = dict(context, start_ns=node.exec_ns, end_ns=ready, latency_ns=ready - node.exec_ns,
                               idle_delay_s=args.idle_s, idle_sample_ns=sampled_at,
                               idle_rss_kib=sample["rss_kib"], **sample)
                    row.update(node.preload_receipt(node.exec_ns))
                    campaign.observe(row)
                    rollups.append(row)
        for repetition in range(args.repetitions):
            for order, label in enumerate(interleave(labels, repetition)):
                concurrency_order = interleave(args.concurrency, repetition + labels.index(label))
                context = dict(arm=label, scenario="rpc-node", repetition=repetition,
                               arm_order=order, concurrency_order=concurrency_order)
                directory = args.output / "nodes" / f"load-{repetition}-{order}"
                with running_node(by_label[label], directory, states[label], args) as (node, rpc, ready):
                    check_isolation(rpc, tip)
                    for cell_order, concurrency in enumerate(concurrency_order):
                        context = dict(context, scenario="rpc-load", concurrency=concurrency, cell_order=cell_order)
                        cell_dir = directory / f"cell-{repetition}-{order}-{concurrency}"
                        rollups.append(load_cell(node, corpus, variants, args.weights, concurrency,
                                                 context, cell_dir, args, campaign))
                        check_isolation(rpc, tip)
        success = not any(row.get("errors", 0) or row.get("warmup_errors", 0) for row in rollups)
    except BaseException:
        campaign.fail("campaign", dict(context, error=traceback.format_exc()))
        raise
    finally:
        for index, state in enumerate(states.values()):
            if state.exists():
                try:
                    shutil.rmtree(state)
                except Exception:
                    success = False
                    campaign.fail(f"cleanup-{index}", dict(error=traceback.format_exc(), path=str(state)))
        campaign.complete(success)
        signal.signal(signal.SIGTERM, old_term)
    print_summary(rollups)
    if not success:
        raise SystemExit("RPC campaign had failures; see failures/ and observations.jsonl")


if __name__ == "__main__":
    main()
