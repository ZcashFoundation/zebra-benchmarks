#!/usr/bin/env python3
"""Replay an isolated Mainnet window; startup and resource costs are included.

Snapshots must be stopped state.cache_dir roots, not live databases. Each attempt
checks its copied tip with an isolated preflight node, then measures a fresh exec.
This warms filesystem caches; no caches are dropped. The first --arm supplies all
source nodes. Full mode requires source tip >= end + 1000: debug_stop_at_height
stops finalized commits, so blocks/s measures the finalized window, including the
full-verification tail. Source-to-source localhost connections are permitted;
empty seeds/fresh peer caches isolate the campaign from the public network.
The measured exec-to-exit window includes Halo2 preload without waiting for it;
each replay receipt records preload completion relative to exec.

--smoke caps the window at 10 blocks and repetitions at one; all arms and source
nodes still run. --self-test starts no processes. Linux and Python 3.11+ required.
"""

import argparse
import contextlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import tomllib
import traceback

from front_common import (
    Campaign, MainnetNode, file_hash, interleave, load_arms, print_summary,
    reflink_copy, set_key,
)
from node_bench import BENCH_HELP

FULL_FINALIZATION_TAIL = 1000
# Below Linux's default ephemeral range (32768-60999), so an outbound connection's local port can't
# take a source's listener port before it binds.
SOURCE_PORT_BASE = 31233
STOP_MESSAGE = "stopping at configured height, flushing database to disk"


def positive(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def cpu_list(value):
    if value != "all" and not re.fullmatch(r"\d+(?:-\d+)?(?:,\d+(?:-\d+)?)*", value):
        raise argparse.ArgumentTypeError("expected all or a taskset CPU list, e.g. 0-7,16")
    if value != "all":
        for part in value.split(","):
            ends = list(map(int, part.split("-")))
            if ends[0] > ends[-1]:
                raise argparse.ArgumentTypeError("CPU ranges must be ascending")
    return value


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, epilog=BENCH_HELP, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--provenance", type=Path)
    parser.add_argument("--arm", action="append", default=[], metavar="LABEL=PATH")
    parser.add_argument("--snapshot", type=Path, help="stopped state.cache_dir root at start height")
    parser.add_argument("--source-state", type=Path, help="stopped source state.cache_dir root")
    parser.add_argument("--start-height", type=int)
    parser.add_argument("--end-height", type=int)
    parser.add_argument("--mode", choices=("checkpoint", "full"))
    parser.add_argument("--sources", type=positive, default=4)
    parser.add_argument("--source-port-base", type=positive,
                        default=int(os.environ.get("BENCH_SOURCE_PORT_BASE", SOURCE_PORT_BASE)),
                        help="first source P2P listener; consecutive ports (default: BENCH_SOURCE_PORT_BASE or 31233)")
    parser.add_argument("--repetitions", type=positive, default=3)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--cpus", type=cpu_list, default="all",
                        help="measured node mask; all preserves current affinity (default)")
    parser.add_argument("--source-cpus", type=cpu_list, default="all",
                        help="source node mask; all preserves current affinity (default)")
    parser.add_argument("--startup-timeout", type=positive, default=600, metavar="SECONDS")
    parser.add_argument("--timeout", type=positive, default=86400, metavar="SECONDS")
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--node-set", action="append", default=[], metavar="SECTION.KEY=VALUE",
                        help="extra TOML setting for the measured node only (value is raw TOML)")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args(argv)
    if args.self_test:
        return args
    required = ("provenance", "snapshot", "source_state", "start_height", "end_height", "mode", "output")
    missing = ["--" + key.replace("_", "-") for key in required if getattr(args, key) is None]
    if missing or not args.arm:
        parser.error("required: " + ", ".join(missing + ([] if args.arm else ["--arm"])))
    if not 0 <= args.start_height < args.end_height <= 2**31 - 1 - FULL_FINALIZATION_TAIL:
        parser.error("heights must satisfy 0 <= start < end <= 2147482647")
    if args.sources > 1000:
        parser.error("--sources must be <= 1000 (reserved localhost port range)")
    if not 1 <= args.source_port_base <= 65536 - args.sources:
        parser.error("source listener range must stay within ports 1..65535")
    args.requested_end_height = args.end_height
    args.requested_repetitions = args.repetitions
    if args.smoke:
        args.end_height = min(args.end_height, args.start_height + 10)
        args.repetitions = 1
    return args


def check_cpus(value):
    available = os.sched_getaffinity(0)
    if value == "all":
        return sorted(available)
    selected = set()
    for part in value.split(","):
        ends = list(map(int, part.split("-")))
        if ends[-1] >= os.cpu_count():
            raise ValueError("CPU outside host range: " + part)
        selected.update(range(ends[0], ends[-1] + 1))
    if not selected <= available:
        raise ValueError("CPU list exceeds allowed process affinity: " + value)
    return sorted(selected)


def stop_height(log_path):
    found = []
    with Path(log_path).open(errors="replace") as stream:
        for raw in stream:
            line = re.sub(r"\x1b\[[0-9;]*m", "", raw)
            if STOP_MESSAGE not in line:
                continue
            match = re.search(r"\bheight\s*[=:]\s*Height\((\d+)\)", line)
            if not match:
                raise RuntimeError("stop message lacks finalized Height: " + line.strip())
            found.append(int(match[1]))
    if len(found) != 1:
        raise RuntimeError(f"expected one finalized stop message, found {found}")
    return found[0]


def audit_peers(peers):
    bad = [peer for peer in peers if not re.fullmatch(r"127\.0\.0\.1:\d+", peer.get("addr", ""))]
    if bad:
        raise RuntimeError("non-local peer in isolated campaign: " + json.dumps(bad))
    return peers


def source_audit(sources, row, key):
    row[key] = []
    for index, node in enumerate(sources):
        audit = dict(source=index, observed_ns=time.perf_counter_ns())
        row[key].append(audit)
        try:
            if node.proc.poll() is not None:
                raise RuntimeError(f"source {index} exited: {node.proc.returncode}")
            audit["peers"] = node.rpc("getpeerinfo")
            audit_peers(audit["peers"])
        except Exception as error:
            audit["error"] = repr(error)
    if any("error" in audit for audit in row[key]):
        raise RuntimeError("source peer audit failed: " + json.dumps(row[key]))


def base_edits(args, state):
    return [
        ("state", "cache_dir", json.dumps(str(state))),
        ("consensus", "checkpoint_sync", "true" if args.mode == "checkpoint" else "false"),
        ("network", "max_connections_per_ip", str(max(16, args.sources * 4))),
        ("network", "crawl_new_peer_interval", '"10d"'),
    ]


def measure(node, args, row, samples_path):
    done, cancel = threading.Event(), threading.Event()
    monitor_errors = []

    def wait_for_exit():
        deadline = time.monotonic() + args.timeout
        try:
            while not cancel.is_set():
                try:
                    node.wait_exit(min(0.1, max(0, deadline - time.monotonic())))
                    return
                except subprocess.TimeoutExpired:
                    if time.monotonic() >= deadline:
                        raise
        except BaseException as error:
            monitor_errors.append(error)
        finally:
            done.set()

    def sample_resources():
        try:
            with samples_path.open("w") as stream:
                while not done.is_set() and not cancel.is_set():
                    sampled = time.monotonic()
                    try:
                        sample = node.sample()
                    except (OSError, ValueError):
                        row["resource_sample_errors"] += 1
                    else:
                        row["peak_rss_kib"] = max(row["peak_rss_kib"], sample["hwm_kib"], sample["rss_kib"])
                        row["sampled_cpu_s"] = max(row["sampled_cpu_s"], sample["cpu_s"])
                        row["resource_samples"] += 1
                        stream.write(json.dumps(dict(observed_ns=time.perf_counter_ns(), **sample)) + "\n")
                        stream.flush()
                    done.wait(max(0, sampled + 1 - time.monotonic()))
        except BaseException as error:
            monitor_errors.append(error)
            done.set()

    # Whole-process replay includes startup preload; do not move peer startup or the timer.
    node.preload_wait = False
    node.start()
    row.update(start_ns=node.exec_ns, rpc_ready_s=None, rpc_max_best_tip_height=None,
               peak_rss_kib=0, sampled_cpu_s=0, resource_samples=0,
               resource_sample_errors=0, test_peers_last=None, rpc_errors=0)
    waiter = threading.Thread(target=wait_for_exit, name="sync-exit")
    sampler = threading.Thread(target=sample_resources, name="sync-resources")
    waiter.start()
    sampler.start()
    next_rpc = 0.0
    try:
        while not done.is_set():
            now = time.monotonic()
            if row["rpc_ready_s"] is None and time.perf_counter_ns() - node.exec_ns > args.startup_timeout * 1e9:
                raise TimeoutError("test RPC did not become ready before --startup-timeout")
            if now >= next_rpc:
                try:
                    height = node.rpc("getblockcount")
                    observed = time.perf_counter_ns()
                    if row["rpc_ready_s"] is None:
                        node.ready_ns = observed
                        row["rpc_ready_s"] = (observed - node.exec_ns) / 1e9
                        row["rpc_first_tip_height"] = height
                    row["rpc_max_best_tip_height"] = max(row["rpc_max_best_tip_height"] or 0, height)
                    peers = node.rpc("getpeerinfo")
                    row["test_peers_last"] = dict(peers=peers, observed_ns=time.perf_counter_ns())
                except Exception as error:
                    row["rpc_errors"] += 1
                    row["last_rpc_error"] = repr(error)
                else:
                    audit_peers(peers)
                next_rpc = now + (0.1 if row["rpc_ready_s"] is None else 1)
            done.wait(0.02)
        sampler.join()
        if monitor_errors:
            raise monitor_errors[0]
        row.update(end_ns=node.exit_ns, exit_status=node.proc.returncode,
                   latency_ns=node.exit_ns - node.exec_ns,
                   cpu_s=node.exit_resources["cpu_s"],
                   exit_resources=node.exit_resources)
        row["wall_s"] = row["latency_ns"] / 1e9
        row["peak_rss_kib"] = max(row["peak_rss_kib"], node.exit_resources["hwm_kib"])
        if node.exit_resources["proc_sample"]:
            row["peak_rss_kib"] = max(row["peak_rss_kib"], node.exit_resources["proc_sample"]["hwm_kib"])
    finally:
        cancel.set()
        done.set()
        waiter.join()
        sampler.join()
        row.update(node.preload_receipt(row["start_ns"]))


def attempt(arm, repetition, args, campaign, sources, row):
    directory = campaign.output / "nodes" / f"r{repetition:03d}-{arm.label}"
    directory.mkdir()
    state = directory / "state"
    row["node_dir"] = str(directory.relative_to(campaign.output))
    source_audit(sources, row, "sources_before")
    node = preflight = None
    try:
        reflink_copy(args.snapshot, state)
        edits = base_edits(args, state)
        preflight = MainnetNode(arm.path, directory / "preflight", edits, cpus=args.cpus)
        preflight.start()
        preflight.wait_rpc_ready(args.startup_timeout)
        row["actual_start_height"] = preflight.rpc("getblockcount")
        row["preflight_peers"] = preflight.rpc("getpeerinfo")
        audit_peers(row["preflight_peers"])
        if row["preflight_peers"]:
            raise RuntimeError("snapshot preflight must have no peers")
        if row["actual_start_height"] != args.start_height:
            raise RuntimeError(f"snapshot RPC tip {row['actual_start_height']} != --start-height {args.start_height}")
        preflight.stop(args.startup_timeout)
        peers = [f"127.0.0.1:{args.source_port_base + index}" for index in range(args.sources)]
        edits += [
            ("network", "initial_mainnet_peers", json.dumps(peers)),
            ("network", "peerset_initial_target_size", str(args.sources)),
            ("state", "debug_stop_at_height", str(args.end_height)),
        ]
        for setting in args.node_set:
            name, value = setting.split("=", 1)
            section, key = name.split(".", 1)
            edits.append((section, key, value))
        row["node_set"] = args.node_set
        node = MainnetNode(arm.path, directory / "replay", edits, cpus=args.cpus)
        measure(node, args, row, directory / "resources.jsonl")
        if row["exit_status"] != 0:
            raise RuntimeError(f"test node exited with status {row['exit_status']}")
        row["finalized_tip_height"] = stop_height(node.log_path)
        if row["finalized_tip_height"] != args.end_height:
            raise RuntimeError(f"finalized stop {row['finalized_tip_height']} != end {args.end_height}")
        if row["rpc_ready_s"] is None or row["test_peers_last"] is None:
            raise RuntimeError("test node exited without successful RPC readiness/peer audit")
        row["blocks"] = args.end_height - row["actual_start_height"]
        row["blocks_per_s"] = row["blocks"] / row["wall_s"]
        row["status"] = "ok"
    finally:
        try:
            source_audit(sources, row, "sources_after")
        finally:
            if preflight:
                preflight.stop(args.startup_timeout)
            if node:
                node.stop(args.startup_timeout)
            if state.exists():
                shutil.rmtree(state)


def run(args):
    for name in ("snapshot", "source_state"):
        path = getattr(args, name).expanduser().resolve(strict=True)
        if not path.is_dir():
            raise ValueError(name + " must be a state.cache_dir directory")
        setattr(args, name, path)
    output = args.output.expanduser().resolve()
    if any(path == output or path in output.parents for path in (args.snapshot, args.source_state)):
        raise ValueError("output must be outside input snapshots")
    cpus, source_cpus = check_cpus(args.cpus), check_cpus(args.source_cpus)
    arms = load_arms(args.provenance, args.arm)
    manifest_args = dict(vars(args), resolved_cpus=cpus, resolved_source_cpus=source_cpus,
                         provenance_sha256=file_hash(args.provenance),
                         source_arm=arms[0].label, resource_interval_s=1,
                         timing="Popen/taskset launch to observed exit; includes startup and flush",
                         throughput="finalized window blocks / whole-process wall seconds",
                         source_finalization_tail=FULL_FINALIZATION_TAIL if args.mode == "full" else 0,
                         cache_policy="warm: per-attempt isolated snapshot preflight; no cache drops",
                         peer_policy="localhost only; source interconnections allowed; test advertises 0.0.0.0:0",
                         schedule=[interleave([arm.label for arm in arms], repetition)
                                   for repetition in range(args.repetitions)])
    campaign = Campaign(output, manifest_args, arms)
    sources, source_states, rows = [], [], []
    success = False
    try:
        required_tip = args.end_height + (FULL_FINALIZATION_TAIL if args.mode == "full" else 0)
        source_tips = []
        for index in range(args.sources):
            directory = campaign.output / "nodes" / f"source-{index:02d}"
            directory.mkdir()
            state = directory / "state"
            source_states.append(state)
            reflink_copy(args.source_state, state)
            edits = base_edits(args, state) + [
                ("network", "listen_addr", json.dumps(f"127.0.0.1:{args.source_port_base + index}")),
                ("network", "peerset_initial_target_size", "1"),
            ]
            source = MainnetNode(arms[0].path, directory, edits, cpus=args.source_cpus)
            sources.append(source)
            source.start()
            source.wait_rpc_ready(args.startup_timeout)
            tip = source.rpc("getblockcount")
            source_tips.append(tip)
            if tip < required_tip:
                raise RuntimeError(f"source tip {tip} < required {required_tip} for {args.mode}")
            audit_peers(source.rpc("getpeerinfo"))
        campaign.manifest["source_tips"] = source_tips
        for repetition in range(args.repetitions):
            for arm_order, arm in enumerate(interleave(arms, repetition)):
                name = f"r{repetition:03d}-{arm.label}"
                row = dict(arm=arm.label, scenario="sync-" + args.mode, repetition=repetition,
                           arm_order=arm_order, requested_start_height=args.start_height,
                           requested_end_height=args.requested_end_height, end_height=args.end_height,
                           attempt_started_unix_ns=time.time_ns(), status="error")
                try:
                    attempt(arm, repetition, args, campaign, sources, row)
                except BaseException as error:
                    row.update(status="error", error=traceback.format_exc())
                    campaign.fail(name, row)
                    print(f"FAILED {name}: {error}", file=sys.stderr, flush=True)
                    if not isinstance(error, Exception):
                        raise
                finally:
                    row["attempt_finished_unix_ns"] = time.time_ns()
                    campaign.observe(row)
                    rows.append(row)
        success = len(rows) == args.repetitions * len(arms) and all(row["status"] == "ok" for row in rows)
    except BaseException:
        campaign.fail("campaign", dict(error=traceback.format_exc(), observed_ns=time.perf_counter_ns()))
        raise
    finally:
        retained_states = set()
        for index, source in enumerate(sources):
            try:
                source.stop(args.startup_timeout)
            except Exception:
                success = False
                retained_states.add(index)
                campaign.fail(f"source-{index:02d}-cleanup", dict(error=traceback.format_exc()))
        for index, state in enumerate(source_states):
            if index in retained_states:
                continue
            try:
                if state.exists():
                    shutil.rmtree(state)
            except Exception:
                success = False
                campaign.fail(f"source-{index:02d}-state-cleanup", dict(error=traceback.format_exc()))
        campaign.complete(success)
    for value in ("blocks_per_s", "wall_s", "cpu_s", "peak_rss_kib", "rpc_ready_s"):
        print_summary(rows, ("arm", "scenario"), value)
    return 0 if success else 1


def self_test():
    # Generated-config excerpt: zebrad/tests/common/configs/getblocktemplate-v1.0.1.toml:23-72.
    sample = '''[consensus]
checkpoint_sync = true
debug_skip_parameter_preload = false

[network]
cache_dir = true
crawl_new_peer_interval = "1m 1s"
initial_mainnet_peers = [
    "dnsseed.z.cash:8233",
    "dnsseed.str4d.xyz:8233",
]
initial_testnet_peers = [
    "dnsseed.testnet.z.cash:18233",
]
listen_addr = "0.0.0.0:8233"
max_connections_per_ip = 1
network = "Mainnet"
peerset_initial_target_size = 25

[rpc]
parallel_cpu_threads = 0
# enable_cookie_auth = true

[state]
cache_dir = "cache_dir"
ephemeral = false

[tracing]
use_color = true
'''
    for section, key, value in [
        ("consensus", "checkpoint_sync", "false"),
        ("network", "initial_mainnet_peers", '["127.0.0.1:38233", "127.0.0.1:38234"]'),
        ("network", "initial_testnet_peers", "[]"),
        ("network", "external_addr", '"0.0.0.0:0"'),
        ("rpc", "enable_cookie_auth", "false"),
        ("state", "debug_stop_at_height", "1760000"),
    ]:
        sample = set_key(sample, section, key, value)
    parsed = tomllib.loads(sample)
    assert not parsed["consensus"]["checkpoint_sync"]
    assert parsed["network"]["initial_mainnet_peers"] == ["127.0.0.1:38233", "127.0.0.1:38234"]
    assert parsed["network"]["initial_testnet_peers"] == [] and "dnsseed" not in sample
    assert parsed["network"]["external_addr"] == "0.0.0.0:0"
    assert not parsed["rpc"]["enable_cookie_auth"]
    assert parsed["state"]["debug_stop_at_height"] == 1760000
    assert [interleave("ABC", n) for n in range(4)] == [list(s) for s in ("ABC", "BCA", "CAB", "ABC")]
    assert interleave([], 2) == []
    argv = ["--provenance", "arms.json", "--arm", "main=/bin/main", "--arm", "optimized=/bin/optimized",
            "--snapshot", "snapshot", "--source-state", "source", "--start-height", "1750000",
            "--end-height", "1760000", "--mode", "full", "--output", "run", "--cpus", "16-31"]
    full = parse_args(argv)
    smoke = parse_args(argv + ["--smoke", "--repetitions", "5"])
    assert full.end_height == 1760000 and full.sources == 4 and full.repetitions == 3
    assert smoke.end_height == 1750010 and smoke.repetitions == 1 and smoke.requested_repetitions == 5
    assert smoke.requested_end_height == 1760000 and full.cpus == "16-31"
    with contextlib.redirect_stderr(io.StringIO()):
        for extra in (["--end-height", "1750000"], ["--sources", "0"], ["--cpus", "4-1"]):
            try:
                parse_args(argv + extra)
            except SystemExit as error:
                assert error.code != 0
            else:
                raise AssertionError("invalid argument accepted")
    audit_peers([{"addr": "127.0.0.1:38233"}])
    try:
        audit_peers([{"addr": "8.8.8.8:8233"}])
    except RuntimeError:
        pass
    else:
        raise AssertionError("external peer accepted")
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        log = root / "node.log"
        log.write_text(f"INFO zebra_state: {STOP_MESSAGE} height=Height(1760000) hash=abc\n")
        assert stop_height(log) == 1760000
        binary = root / "binary"
        binary.write_bytes(b"self-test, never executed")
        binary.chmod(0o700)
        provenance = root / "arms.json"
        provenance.write_text(json.dumps({"main": {"sha256": file_hash(binary), "path": str(binary)}}))
        arms = load_arms(provenance, ["main=" + str(binary)])
        campaign = Campaign(root / "campaign", {}, arms)
        campaign.observe(dict(arm="main", scenario="self-test", repetition=0, start_ns=1, end_ns=2, status="ok"))
        completion = campaign.complete(True)
        assert completion["success"] and completion["observations"] == 1
        assert completion["manifest_sha256"] == file_hash(root / "campaign" / "manifest.json")
        assert completion["observations_sha256"] == file_hash(root / "campaign" / "observations.jsonl")
        # Whole-process replay must retain startup and annotate only after its exit timestamp.
        from types import SimpleNamespace
        from unittest.mock import patch

        class FakeReplay:
            def start(self):
                assert self.preload_wait is False
                self.exec_ns = 100
                self.proc = SimpleNamespace(returncode=None)

            def wait_exit(self, timeout):
                self.exit_ns = 300
                self.proc.returncode = 0
                self.exit_resources = dict(cpu_s=0, hwm_kib=0, proc_sample=None)

            def preload_receipt(self, start_ns):
                assert self.exit_ns == row["end_ns"]
                return dict(binary_preloads=True, preload_wait=self.preload_wait,
                            preload_done_unix_ns=1000, preload_done_ns=200,
                            preload_done_relative_ns=200 - start_ns)

        row = {}
        with patch.object(threading, "Thread", side_effect=lambda target, name: SimpleNamespace(
                start=target, join=lambda: None)):
            measure(FakeReplay(), SimpleNamespace(timeout=1), row, root / "resources.jsonl")
        assert row["start_ns"] == 100 and row["end_ns"] == 300 and row["latency_ns"] == 200
        assert row["binary_preloads"] and row["preload_wait"] is False
        assert row["preload_done_relative_ns"] == 100
        binary.write_bytes(b"modified")
        try:
            load_arms(provenance, ["main=" + str(binary)])
        except ValueError:
            pass
        else:
            raise AssertionError("provenance mismatch accepted")
    print("sync_replay self-test: ok")


def main():
    args = parse_args()
    if args.self_test:
        self_test()
        return 0

    def interrupted(signum, _frame):
        raise KeyboardInterrupt(f"signal {signum}")

    signal.signal(signal.SIGTERM, interrupted)
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
