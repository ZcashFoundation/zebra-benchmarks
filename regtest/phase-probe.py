#!/usr/bin/env python3
"""Measure tip verification with the sweep's fresh-node, sealed-fixture cases.

Usage: phase-probe.py <binary> <arm-name> <fixture-id> <reps> <output-dir>
       phase-probe.py --self-test

Writes samples.jsonl and prints the same rows to stdout. Each sample retains
node logs/config and metrics-before.prom / metrics-after.prom under sample-NNNN.
Each node also retains measurement-window.json with inclusive Unix-microsecond
boundaries captured after setup/warmup and before the after-scrape, sealed input
identities, and measurement settings. This brackets the existing measurement,
not node startup; failed windows are marked and must not be summarized.
Preparation and both HTTP scrapes are outside latency_ms. delta_seconds contains
histogram/summary sum differences (seconds), not averages; delta_counts contains
observation-count differences. delta_counters contains plain cache counter
increments by verifier. Null means the series was absent in both scrapes,
not zero work. New series start at zero; disappearing/reset series are errors.
Rows contain binary, arm, fixture, endpoint, sample, latency_ms, delta_seconds,
delta_counts and delta_counters.

For unprepared tip verification, compare cold-submitblock-nominer with
warm-mempool-submitblock: neither configures a miner or requests any template or
proposal. Warm admission and exact-mempool polling precede the submission window.
Do not use --warmup-proposal for this comparison: fixtures can share transactions.
Cold still replays the funding prefix in the measured process. Binaries containing
the Halo2 preload completion log message must log it within 60 seconds after RPC
readiness, before replay, admission, warmup or measurement begins.
--restart-delay-seconds instead builds the prefix in a separate process and copies
its stopped state (including non-finalized backups) before starting the measured
node. The restart submission deliberately skips the preload wait and uses only
the requested delay after RPC readiness, measuring startup overlap. No prefix
blocks, transactions, templates or proposals are submitted in that process.

--finalize-prefix-dir reuses a deep state per arm: the selected large fixture is
the block to retire, not the measured body. An untimed empty filler after restore
initializes finalized-height/write metrics. The timed body is another empty block,
1000 heights above the large fixture (999 above it at the parent). Completion is
checked outside the timing window, including finalization after the RPC response.
Unlike the restart submission, finalization waits for preload before its filler.
For this case only, transparent mining is enabled. Independent post-response
observers record the first new-tip template and completed finalization, both
relative to submission start. These are observed readiness times, not internal
commit timestamps; the 1 ms polling and RPC/scrape overhead remain included.

Metric names below are Prometheus names: Zebra's dots become underscores;
histograms use _sum and _count, cache counters use the plain metric name.
Historical source references below are relative to zebra_v12_stack_fix:

* zebra_consensus_block_phase_duration_seconds{phase="block_checks"}
  zebra-consensus/src/block.rs:479-653 (records at 577 and 649): hash/known-block,
  height/PoW/time checks, proposal-key hashing/cache lookup, MiningCandidate, and
  on misses transaction hashes/Merkle, coinbase/subsidy/action-limit/decryption
  checks. Stops before transaction verification; proposals do not record it.
* Same family, phase="transaction_verification", block.rs:655-723 (record 719):
  build ordered outputs, dispatch all transaction checks, await/drain results,
  aggregate sigops/fees. Excludes later aggregate block-limit/fee checks.
  Prepared-cache hits skip this phase; proposals do not record it.
* Same family, phase="state_commit", block.rs:582-599 and 803-814 (595, 810):
  state-service readiness/request/response wait; on a prepared-cache hit also
  clone/rebind the cached contextual block and construct its commit request.
* state_block_writer_phase_duration_seconds{phase="queue"}
  zebra-state/src/service.rs:915 -> service/write.rs:406: writer-channel enqueue
  to dequeue, NOT earlier waiting for a parent in the state's queued-block map.
* Same family, phase="contextual_commit", service/write.rs:446-466 (462):
  commit_prevalidated_contextual (including replacing new_outputs) or full
  validate_and_commit_non_finalized; excludes proposals/restored backups.
* Same family, phase="update_latest_chain_channels", write.rs:512-525 (521):
  publish cloned non-finalized state and best tip, plus synchronous backup if
  configured; function body at write.rs:89-124.
* Same family, phase="finalization", write.rs:536-554 (550): finalize blocks
  beyond the reorg limit and commit them to finalized state; records even when
  no block needs finalizing. Runs AFTER the response send at write.rs:534.
* zebra_state_rocksdb_batch_commit_duration_seconds
  zebra-state/src/service/finalized_state/zebra_db/block.rs:587-592 (591): only
  self.db.write(batch), NOT batch preparation, contextual work, or all disk I/O.
* rpc_request_duration_seconds{method="submitblock"}
  zebra-rpc/src/server/rpc_metrics.rs:47-69 (68): middleware entry through the
  awaited service response; excludes client HTTP transport/response delivery.
* zebra_consensus_batch_duration_seconds{verifier}: halo2, groth16_sapling,
  ed25519, redjubjub, redpallas; aggregates success/failure and measures dispatched
  worker wait/execution, not exclusive CPU time (primitives/{verifier}.rs).
* zebra_consensus_script_duration_seconds{result="success"/"failure"}:
  script.rs on bench/script-timing; is_valid inside Rayon, excluding queue wait.
  Its count is the number of executed transparent inputs, not transactions.
* zebra_consensus_cache_{hit,miss,insert,evict}{verifier}: plain counters from
  primitives/cache.rs, split by groth16_sapling, halo2_pre_nu6_2, halo2_nu6_2,
  halo2_nu6_3_onward, transparent_scripts. Funding replay/admission are excluded from these deltas.

These are nested/overlapping wall times, not additive CPU-time partitions.
The immediate after-scrape can miss finalization that outlives the RPC response;
its delta is only what was observed, not a complete asynchronous-work total.
No sync.block.verify metric is selected: it measures peer downloads, not RPC.
Binaries must have the prometheus feature; unavailable metrics fail the scrape.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
import http.client
import json
import math
from pathlib import Path
import os
import re
import select
import signal
import socket
import subprocess
import sys
import threading
import time

from node_bench import BENCH_HELP, FIXTURES, next_port
METRICS = {
    "zebra_consensus_block_phase_duration_seconds": (
        "phase", ("block_checks", "transaction_verification", "state_commit")
    ),
    "state_block_writer_phase_duration_seconds": (
        "phase", ("queue", "contextual_commit", "update_latest_chain_channels", "finalization")
    ),
    "zebra_state_rocksdb_batch_commit_duration_seconds": (None, ("",)),
    "rpc_request_duration_seconds": ("method", ("submitblock",)),
    "zebra_consensus_batch_duration_seconds": (
        "verifier", ("halo2", "groth16_sapling", "ed25519", "redjubjub", "redpallas")
    ),
    "zebra_consensus_script_duration_seconds": ("result", ("success", "failure")),
}
COUNTERS = {f"zebra_consensus_cache_{event}": (
    "verifier", ("groth16_sapling", "halo2_pre_nu6_2", "halo2_nu6_2", "halo2_nu6_3_onward",
                 "transparent_scripts")
) for event in ("hit", "miss", "insert", "evict")}
SAMPLE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})?\s+(\S+)(?:\s+\S+)?$')
LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')
PERF_EVENTS = ("task-clock,context-switches,cpu-migrations,cycles:u,instructions:u,"
               "stalled-cycles-frontend:u,L1-icache-load-misses:u")


def series_key(name, label, value):
    return name + (f'{{{label}="{value}"}}' if label else "")


SERIES = [series_key(name, label, value)
          for name, (label, values) in METRICS.items() for value in values]
COUNTER_SERIES = [series_key(name, label, value)
                  for name, (label, values) in COUNTERS.items() for value in values]


def parse_metrics(text):
    """Select histogram pairs and plain counters, aggregating extra exporter labels."""
    samples = {}
    for line in text.splitlines():
        match = SAMPLE.fullmatch(line)
        if not match:
            continue
        raw_name, raw_labels, raw_value = match.groups()
        if raw_name in COUNTERS:
            name, statistic = raw_name, "counter"
            label, values = COUNTERS[name]
        else:
            name, _, statistic = raw_name.rpartition("_")
            if name not in METRICS or statistic not in ("sum", "count"):
                continue
            label, values = METRICS[name]
        labels = {key: json.loads('"' + value + '"')
                  for key, value in LABEL.findall(raw_labels or "")}
        value = labels.get(label, "")
        if value not in values:
            continue
        number = float(raw_value)
        if not math.isfinite(number) or number < 0:
            raise ValueError(f"invalid metric sample: {line}")
        if statistic in ("count", "counter"):
            if not number.is_integer():
                raise ValueError(f"non-integral metric count: {line}")
            number = int(number)
        key = series_key(name, label, value)
        sample = samples.setdefault(key, {})
        sample[statistic] = sample.get(statistic, 0) + number
    for key, sample in samples.items():
        if sample.keys() not in ({"sum", "count"}, {"counter"}):
            raise ValueError(f"incomplete metric sample: {key}")
    return samples


def metric_deltas(before, after):
    seconds, counts = {}, {}
    for key in SERIES:
        if key not in after:
            if key in before:
                raise ValueError(f"metric disappeared: {key}")
            seconds[key] = counts[key] = None
            continue
        old = before.get(key, {"sum": 0.0, "count": 0})
        seconds[key] = after[key]["sum"] - old["sum"]
        counts[key] = after[key]["count"] - old["count"]
        if seconds[key] < 0 or counts[key] < 0:
            raise ValueError(f"metric reset during submitblock: {key}")
    counters = {}
    for key in COUNTER_SERIES:
        if key not in after:
            if key in before:
                raise ValueError(f"metric disappeared: {key}")
            counters[key] = None
            continue
        counters[key] = after[key]["counter"] - before.get(key, {"counter": 0})["counter"]
        if counters[key] < 0:
            raise ValueError(f"metric reset during submitblock: {key}")
    return {"delta_seconds": seconds, "delta_counts": counts, "delta_counters": counters}


def scrape(node):
    connection = http.client.HTTPConnection("127.0.0.1", node.metrics_port, timeout=10)
    try:
        connection.request("GET", "/metrics")
        response = connection.getresponse()
        if response.status != 200:
            raise RuntimeError(f"metrics HTTP status {response.status}")
        return response.read().decode("utf-8")
    finally:
        connection.close()


def finalized_tip(node, height, block_hash, writes, timeout=600):
    """Wait for the pre-write height gauge AND the post-write RocksDB histogram."""
    deadline = time.monotonic() + timeout
    key = "zebra_state_rocksdb_batch_commit_duration_seconds"
    while time.monotonic() < deadline:
        text = scrape(node)
        heights = [float(match[3]) for line in text.splitlines()
                   if (match := SAMPLE.fullmatch(line))
                   and match[1] == "state_finalized_block_height"]
        count = parse_metrics(text).get(key, {}).get("count", 0)
        if len(heights) > 1 or any(not math.isfinite(h) or h > height for h in heights) or count > writes:
            raise RuntimeError("unexpected finalized height/write count")
        if heights == [height] and count == writes:
            actual_hash = node.rpc.call("getblockhash", [height])
            if actual_hash != block_hash:
                raise RuntimeError("finalized height has the wrong canonical block")
            return dict(height=height, hash=actual_hash, rocksdb_batch_commits=count,
                        observed_ns=time.perf_counter_ns())
        time.sleep(0.001)
    raise TimeoutError(f"finalization did not reach height {height}, writes {writes}")

def finalization_submit(node, candidate, submit, begin_window, finish_window):
    """Observe post-response mining readiness and durable writes on independent connections."""
    from common import h, require, save
    from tip_to_template import observe

    target = candidate["finalization"]["target"]
    before = finalized_tip(node, target["height"] - 1, target["parent"], 1)
    responded, ready, stopped = threading.Event(), threading.Event(), threading.Event()
    stamps = {}
    args = argparse.Namespace(timeout=600, poll_ms=1)
    rpc = h.RPC(node.rpc_addr, timeout=args.timeout)

    def completion():
        require(responded.wait(args.timeout) and not stopped.is_set(), "submission succeeded")
        return finalized_tip(node, target["height"], target["hash"],
                             before["rocksdb_batch_commits"] + 1, args.timeout)

    with ThreadPoolExecutor(max_workers=2) as executor:
        template_future = executor.submit(
            observe, rpc, "getblocktemplate", candidate, {}, responded, ready, stopped, stamps, args)
        finalized_future = executor.submit(completion)
        try:
            require(ready.wait(args.timeout), "template observer ready")
            state = begin_window(node)
            try:
                result = submit(node, candidate)
            except BaseException:
                finish_window(node, state, failed=True)
                raise
            require(result["reply"] is None, "measured finalization submission accepted")
            responded.set()
            result.update(finish_window(node, state))
            template = template_future.result(timeout=args.timeout)
            after = finalized_future.result(timeout=args.timeout)
            require(template["height"] == candidate["height"] + 1
                    and template["transactions"] == [], "usable empty template on the new tip")
            h.empty_regtest_block(template)
            save(node.dir / "first-template.json", template)
            first_ns = stamps["getblocktemplate"]["first_new"]["end_ns"]
            result.update(
                first_template_observed_ns=first_ns,
                first_template_latency_ns=first_ns - result["start_ns"],
                finalization_observed_ns=after["observed_ns"],
                finalization_latency_ns=after["observed_ns"] - result["start_ns"],
                finalization=dict(
                    candidate["finalization"], before=before, after=after,
                    check="state_finalized_block_height + rocksdb batch_commit count + getblockhash"),
            )
            return result
        finally:
            stopped.set()
            responded.set()



def self_test():
    from node_bench import preload_self_test
    preload_self_test()
    name = "zebra_consensus_block_phase_duration_seconds"
    text = (f'{name}_sum{{network="Regtest",phase="block_checks"}} 1.25\n'
            f'{name}_count{{phase="block_checks",network="Regtest"}} 2\n'
            'rpc_request_duration_seconds_sum{method="getblocktemplate"} 99\n')
    samples = parse_metrics(text)
    key = series_key(name, "phase", "block_checks")
    assert samples == {key: {"sum": 1.25, "count": 2}}
    assert metric_deltas({}, samples)["delta_seconds"][key] == 1.25
    assert metric_deltas(samples, samples)["delta_counts"][key] == 0
    assert metric_deltas({}, {})["delta_seconds"][key] is None
    try:
        metric_deltas(samples, {key: {"sum": 1.0, "count": 1}})
    except ValueError:
        pass
    else:
        raise AssertionError("counter resets must fail")
    batch = "zebra_consensus_batch_duration_seconds"
    script = "zebra_consensus_script_duration_seconds"
    text = "".join(f'{batch}_{stat}{{verifier="halo2",result="{result}"}} {value}\n'
                   for result in ("success", "failure") for stat, value in (("sum", 0.25), ("count", 1)))
    text += (f'{script}_sum{{result="success"}} 0.125\n'
             f'{script}_count{{result="success"}} 3\n')
    for name, (label, verifiers) in COUNTERS.items():
        for verifier in verifiers:
            text += f'{name}{{{label}="{verifier}",network="Regtest"}} 7\n'
    parsed = parse_metrics(text)
    deltas = metric_deltas({}, parsed)
    assert deltas["delta_seconds"][series_key(batch, "verifier", "halo2")] == 0.5
    assert deltas["delta_counts"][series_key(batch, "verifier", "halo2")] == 2
    assert deltas["delta_counts"][series_key(script, "result", "success")] == 3
    assert deltas["delta_seconds"][series_key(script, "result", "failure")] is None
    assert set(deltas["delta_counters"].values()) == {7}
    assert set(metric_deltas(parsed, parsed)["delta_counters"].values()) == {0}
    assert set(metric_deltas({}, {})["delta_counters"].values()) == {None}
    cache = series_key("zebra_consensus_cache_hit", "verifier", "groth16_sapling")
    for before, after in (({cache: {"counter": 2}}, {}),
                          ({cache: {"counter": 2}}, {cache: {"counter": 1}})):
        try:
            metric_deltas(before, after)
        except ValueError:
            pass
        else:
            raise AssertionError("cache resets/disappearing metrics must fail")
    for invalid in ("-1", "nan", "inf", "0.5"):
        try:
            parse_metrics(f'{cache} {invalid}\n')
        except ValueError:
            pass
        else:
            raise AssertionError("invalid plain counter accepted")
    from types import SimpleNamespace
    from unittest.mock import MagicMock, patch
    node = SimpleNamespace(rpc=MagicMock())
    node.rpc.call.return_value = "target"
    def finalized_metrics(height, count):
        return (f"state_finalized_block_height {height}\n"
                "zebra_state_rocksdb_batch_commit_duration_seconds_sum 0.1\n"
                f"zebra_state_rocksdb_batch_commit_duration_seconds_count {count}\n")
    with patch.dict(globals(), scrape=MagicMock(side_effect=[
            "", finalized_metrics(3, 1), finalized_metrics(3, 2)])), patch.object(time, "sleep") as pause:
        proof = finalized_tip(node, 3, "target", 2)
        assert proof["height"] == 3 and proof["rocksdb_batch_commits"] == 2
        assert pause.call_count == 2  # The pre-write height alone cannot complete the check.
        node.rpc.call.assert_called_once_with("getblockhash", [3])
    for text in (finalized_metrics(4, 2), finalized_metrics(3, 3)):
        with patch.dict(globals(), scrape=lambda _: text):
            try:
                finalized_tip(node, 3, "target", 2)
            except RuntimeError:
                pass
            else:
                raise AssertionError("wrong finalized height/write count accepted")
    with patch.dict(globals(), scrape=lambda _: finalized_metrics(3, 2)):
        try:
            finalized_tip(node, 3, "wrong-hash", 2)
        except RuntimeError:
            pass
        else:
            raise AssertionError("wrong finalized hash accepted")
    with patch.dict(globals(), scrape=lambda _: finalized_metrics(2, 1)), \
            patch.object(time, "monotonic", side_effect=[0, 0, 2]), patch.object(time, "sleep"):
        try:
            finalized_tip(node, 3, "target", 2, timeout=1)
        except TimeoutError:
            pass
        else:
            raise AssertionError("missing finalization accepted")
    print("phase-probe self-test passed")


def main():
    sys.path.insert(0, str(Path(__file__).resolve().parent / "sweep"))
    from common import PROBE_ENDPOINTS, load_fixtures
    parser = argparse.ArgumentParser(description=__doc__, epilog=BENCH_HELP, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("binary", type=Path)
    parser.add_argument("arm_name")
    parser.add_argument("fixture_id")
    parser.add_argument("reps", type=int)
    parser.add_argument("output_dir", type=Path)
    parser.add_argument("--endpoint", default="prepared-submitblock",
                        choices=["prepared-submitblock", "cold-submitblock", "admission-to-template",
                                 "finalize-submitblock", *PROBE_ENDPOINTS])
    parser.add_argument("--fixtures", type=Path, default=FIXTURES,
                        help="sealed fixture directory (use the signed build directory for transparent-signed)")
    parser.add_argument("--provenance", type=Path,
                        help="optional arm receipt JSON; verify this arm's executable before creating output")
    parser.add_argument("--perf", action="store_true",
                        help="per-thread perf stat counters and a cycles:u profile around the measured window")
    parser.add_argument("--perf-callgraph", choices=["dwarf", "fp"],
                        help="with --perf, record call stacks (fp needs a frame-pointer build)")
    parser.add_argument("--perf-frequency", type=int, default=19997,
                        help="perf record sampling frequency in Hz (lower it for multi-second windows)")
    parser.add_argument("--parallel-cpu-threads", type=int,
                        help="override the sweep config's parallel_cpu_threads (rayon pool size)")
    parser.add_argument("--warmup-proposal",
                        help="fixture id to proposal-validate (not commit) before the measured submit, so "
                             "one-time verifier caches are built outside the timer")
    parser.add_argument("--restart-delay-seconds", type=float,
                        help="restore a separately built prefix, then wait this many seconds after "
                             "RPC readiness (quick-ab: 5); restart submit skips preload wait to measure "
                             "startup overlap, but finalize still waits")
    parser.add_argument("--finalize-prefix-dir", type=Path,
                        help="shared deep-prefix directory for retiring this fixture with an empty "
                             "submission; requires --restart-delay-seconds and finalize-submitblock")
    args = parser.parse_args()
    if args.reps < 1:
        parser.error("reps must be positive")
    if args.warmup_proposal and (args.endpoint in PROBE_ENDPOINTS or args.finalize_prefix_dir):
        parser.error("no-miner/finalization endpoints forbid warm-up proposals")
    if args.restart_delay_seconds is not None:
        if (args.endpoint not in ("cold-submitblock-nominer", "finalize-submitblock")
                or not math.isfinite(args.restart_delay_seconds) or args.restart_delay_seconds < 0):
            parser.error("restart delay must be finite, nonnegative and use cold/finalization submit")
    if args.finalize_prefix_dir is not None:
        if args.restart_delay_seconds is None or args.endpoint != "finalize-submitblock":
            parser.error("finalization prefix requires restart delay and finalize-submitblock")
    elif args.endpoint == "finalize-submitblock":
        parser.error("finalize-submitblock requires --finalize-prefix-dir")
    binary = args.binary.expanduser().resolve(strict=True)
    declared = None
    if args.provenance is not None:
        from front_common import file_hash, load_arms
        arm = load_arms(args.provenance, [f"{args.arm_name}={binary}"])[0]
        declared = dict(receipt_sha256=file_hash(args.provenance), arm=arm.provenance)
    args.output_dir = args.output_dir.expanduser().resolve()
    import run_sweep

    manifest, chain, fixtures = load_fixtures(args.fixtures)
    fixture = next((item for item in fixtures if item["id"] == args.fixture_id), None)
    if fixture is None:
        parser.error("unknown fixture; choose from: " + ", ".join(item["id"] for item in fixtures))
    warmup = None
    if args.warmup_proposal:
        warmup = next((item for item in fixtures if item["id"] == args.warmup_proposal), None)
        if warmup is None or warmup["id"] == fixture["id"] or warmup["parent"] != fixture["parent"]:
            parser.error("warm-up must be a different fixture with the same parent")
    args.output_dir.mkdir(parents=True)  # Never overwrite an earlier run.
    original_init = run_sweep.Node.__init__
    original_submit = run_sweep.timed_submit

    def metrics_init(node, *a, **k):
        original_init(node, *a, **k)
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", next_port()))
            node.metrics_port = listener.getsockname()[1]
        # Node strips ZEBRA_* environment variables; configure metrics explicitly.
        config = node.config.read_text()
        if args.parallel_cpu_threads is not None:
            assert config.count("parallel_cpu_threads = 4") == 2, "sweep config changed"
            config = config.replace("parallel_cpu_threads = 4",
                                    f"parallel_cpu_threads = {args.parallel_cpu_threads}")
        node.config.write_text(config +
                               f'\n[metrics]\nendpoint_addr = "127.0.0.1:{node.metrics_port}"\n')

    def perf_tool(command, directory, name):
        """Start a perf tool attached but disabled; returns (process, control fd, ack fd)."""
        ctl, ack = directory / f"{name}.ctl", directory / f"{name}.ack"
        os.mkfifo(ctl)
        os.mkfifo(ack)
        process = subprocess.Popen(command + ["--delay", "-1", "--control", f"fifo:{ctl},{ack}"],
                                   stdout=subprocess.DEVNULL)
        # O_RDWR opens a FIFO without waiting for perf to open the other end.
        return process, os.open(ctl, os.O_RDWR), os.open(ack, os.O_RDWR)

    def perf_command(tools, command, deadline=30):
        for _, ctl, _ in tools:
            os.write(ctl, command)
        for process, _, ack in tools:
            # The ack FIFO is held open O_RDWR, so a dead perf never yields EOF: bound the wait.
            ready, _, _ = select.select([ack], [], [], deadline)
            if not ready:
                raise RuntimeError(f"{process.args[:2]} (exit {process.poll()}) didn't answer "
                                   f"{command!r} within {deadline} s")
            reply = os.read(ack, 16)
            if reply.rstrip(b"\0") != b"ack\n":  # perf writes "ack\n" plus a NUL terminator
                raise RuntimeError(f"{process.args[:2]} answered {reply!r} to {command!r}")

    def stop_tools(tools):
        """Stop every started tool and close its FIFOs; returns their exit codes."""
        codes = []
        for process, ctl, ack in tools:
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
            try:
                codes.append(process.wait(60))
            except subprocess.TimeoutExpired:
                process.kill()
                codes.append(process.wait())
            os.close(ctl)
            os.close(ack)
        return codes

    def start_window(node):
        """Scrapes metrics and, with --perf, attaches enabled perf tools; returns window state."""
        before = scrape(node)
        tools = []
        if args.perf:
            # Attach disabled, then enable: counting covers only the window. Threads the node already
            # has (tokio, rayon, blocking pool, state writer) are counted.
            pid = str(node.proc.pid)
            try:
                tools.append(perf_tool(["perf", "stat", "--per-thread", "-x", ",", "-e", PERF_EVENTS,
                                        "-p", pid, "-o", str(node.dir / "perf-stat.csv")],
                                       node.dir, "stat"))
                callgraph = {"dwarf": ["--call-graph", "dwarf,16384"], "fp": ["--call-graph", "fp"],
                             None: []}[args.perf_callgraph]
                tools.append(perf_tool(["perf", "record", "-q", "-F", str(args.perf_frequency), "-e", "cycles:u",
                                        *callgraph, "-p", pid, "-o", str(node.dir / "perf.data")],
                                       node.dir, "record"))
                perf_command(tools, b"enable\n")
            except BaseException:
                stop_tools(tools)
                raise
        return before, tools, time.time_ns() // 1000, time.perf_counter_ns()

    def end_window(node, state, failed=False):
        """Stops perf (always) and returns metric deltas for the window."""
        ended_unix_us = time.time_ns() // 1000
        before, tools, started_unix_us, started_ns = state
        if tools:
            try:
                if not failed:
                    perf_command(tools, b"disable\n")
            finally:
                codes = stop_tools(tools)
            for (process, _, _), code in zip(tools, codes):
                if code not in (0, -signal.SIGINT, 130):
                    raise RuntimeError(f"{process.args[:2]} exited with {code}")
        after = scrape(node)
        (node.dir / "metrics-before.prom").write_text(before)
        (node.dir / "metrics-after.prom").write_text(after)
        preload = node.preload_receipt(started_ns)
        (node.dir / "measurement-window.json").write_text(json.dumps(dict(
            schema=1, start_unix_us=started_unix_us, end_unix_us=ended_unix_us,
            failed=failed, endpoint=args.endpoint, fixture=args.fixture_id,
            provenance=declared,
            candidate_sha256=fixture["sha256"],
            setup_chain_sha256=manifest["files"]["chain.json"],
            warmup_sha256=warmup["sha256"] if warmup is not None else None,
            restart_delay_seconds=args.restart_delay_seconds,
            preload=preload,
            finalization=fixture.get("finalization"),
            parallel_cpu_threads=(args.parallel_cpu_threads
                                  if args.parallel_cpu_threads is not None else 4),
            perf=args.perf, perf_callgraph=args.perf_callgraph,
            perf_frequency=args.perf_frequency if args.perf else None,
        ), allow_nan=False) + "\n")
        return dict(metric_deltas(parse_metrics(before), parse_metrics(after)), preload=preload)

    def measured_submit(node, candidate):
        if warmup is not None:
            # Proposal validation runs full block verification without committing, so the measured
            # block below stays a cold, unproposed candidate on the same parent.
            reply = node.rpc.call("getblocktemplate", [{"mode": "proposal", "data": warmup["raw"]}])
            if reply is not None:
                raise RuntimeError(f"warm-up proposal rejected: {reply!r}")
        if candidate.get("finalization") is not None:
            return finalization_submit(node, candidate, original_submit, start_window, end_window)
        state = start_window(node)
        try:
            result = original_submit(node, candidate)
        except BaseException:
            end_window(node, state, failed=True)
            raise
        result.update(end_window(node, state))
        return result

    # admission-to-template times admit() through exact_template(); open the window around both.
    original_admit, original_template = run_sweep.admit, run_sweep.exact_template
    admission = {}

    def measured_admit(node, corpus):
        admission["state"] = start_window(node)
        return original_admit(node, corpus)

    def measured_template(node, *a, **k):
        try:
            result = original_template(node, *a, **k)
        except BaseException:
            end_window(node, admission.pop("state"), failed=True)
            raise
        admission["deltas"] = end_window(node, admission.pop("state"))
        return result

    run_sweep.Node.__init__ = metrics_init
    run_sweep.timed_submit = measured_submit
    if args.endpoint == "admission-to-template":
        run_sweep.admit, run_sweep.exact_template = measured_admit, measured_template
    try:
        prefix_state = prelude = None
        if args.finalize_prefix_dir is not None:
            fixture, prefix_state, prelude = run_sweep.finalization_prefix(
                args.arm_name, binary, args.finalize_prefix_dir, fixture, chain, 600)
        with (args.output_dir / "samples.jsonl").open("x") as output:
            for repetition in range(1, args.reps + 1):
                result = run_sweep.case(
                    args.arm_name, binary, args.output_dir / f"sample-{repetition:04d}",
                    fixture, chain, argparse.Namespace(timeout=600, poll_ms=1.0),
                    args.endpoint,
                    restart_delay_seconds=args.restart_delay_seconds,
                    prefix_state=prefix_state, prelude=prelude,
                )
                if args.endpoint == "admission-to-template":
                    result.update(admission.pop("deltas"))
                row = dict(binary=str(binary), arm=args.arm_name, fixture=args.fixture_id, endpoint=args.endpoint,
                           sample=repetition, latency_ms=result["latency_ns"] / 1e6,
                           delta_seconds=result["delta_seconds"], delta_counts=result["delta_counts"],
                           delta_counters=result["delta_counters"],
                           restart_delay_seconds=args.restart_delay_seconds, preload=result["preload"])
                if args.finalize_prefix_dir is not None:
                    row.update(
                        finalization=result["finalization"], candidate_sha256=fixture["sha256"],
                        start_ns=result["start_ns"], submit_response_ns=result["end_ns"],
                        first_template_observed_ns=result["first_template_observed_ns"],
                        finalization_observed_ns=result["finalization_observed_ns"],
                        first_template_latency_ms=result["first_template_latency_ns"] / 1e6,
                        finalization_latency_ms=result["finalization_latency_ns"] / 1e6,
                    )
                line = json.dumps(row, allow_nan=False)
                output.write(line + "\n")
                output.flush()
                print(line, flush=True)
    finally:
        run_sweep.Node.__init__ = original_init
        run_sweep.timed_submit = original_submit
        run_sweep.admit, run_sweep.exact_template = original_admit, original_template


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        self_test()
    else:
        main()
