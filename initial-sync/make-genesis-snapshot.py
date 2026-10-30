#!/usr/bin/env python3
"""Build a stopped Mainnet genesis-only state for checkpoint sync.

A source node serves a reflink of a stopped synced state. A fresh node fetches
genesis from that source and stops at height 0. The output directory must be new;
no live source database may be used. Linux and a reflink-capable filesystem are
required. Output is <output>/snapshot-0; other output files are preparation logs.
"""

import argparse
import json
from pathlib import Path
import signal
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "regtest"))
from front_common import MainnetNode, file_hash, reflink_copy, save_json
from sync_replay import check_cpus, cpu_list, positive, stop_height


def verify_genesis(node, snapshot):
    """Do not label a failed or wrong-height node exit as a usable snapshot."""
    if node.proc.returncode != 0:
        raise RuntimeError(f"genesis node exited with status {node.proc.returncode}")
    if stop_height(node.log_path) != 0:
        raise RuntimeError("genesis node did not stop at finalized height 0")
    if not snapshot.is_dir():
        raise RuntimeError("genesis node did not create snapshot-0")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("source_state", type=Path, help="stopped state.cache_dir root")
    parser.add_argument("output", type=Path, help="new output directory")
    parser.add_argument("--cpus", type=cpu_list, default="all", help="default: current process affinity")
    parser.add_argument("--source-cpus", type=cpu_list, default="all", help="default: current process affinity")
    parser.add_argument("--source-port", type=positive, default=31300)
    parser.add_argument("--timeout", type=positive, default=600, help="seconds for source startup and genesis completion")
    args = parser.parse_args()
    if args.source_port > 65535:
        parser.error("--source-port must be in 1..65535")
    check_cpus(args.cpus)
    check_cpus(args.source_cpus)
    binary = args.binary.expanduser().resolve(strict=True)
    source_state = args.source_state.expanduser().resolve(strict=True)
    out = args.output.expanduser().resolve()
    if out == source_state or source_state in out.parents:
        parser.error("output must be outside the source state")
    out.mkdir(parents=True)
    (out / "source").mkdir()
    reflink_copy(source_state, out / "source" / "state")
    snapshot = out / "snapshot-0"
    peer = f"127.0.0.1:{args.source_port}"
    source = MainnetNode(binary, out / "source", [
        ("state", "cache_dir", json.dumps(str(out / "source" / "state"))),
        ("network", "listen_addr", json.dumps(peer)),
        ("network", "peerset_initial_target_size", "1"),
        ("network", "crawl_new_peer_interval", '"10d"'),
    ], cpus=args.source_cpus)
    genesis = MainnetNode(binary, out / "genesis", [
        ("state", "cache_dir", json.dumps(str(snapshot))),
        ("consensus", "checkpoint_sync", "true"),
        ("network", "initial_mainnet_peers", json.dumps([peer])),
        ("network", "peerset_initial_target_size", "1"),
        ("network", "crawl_new_peer_interval", '"10d"'),
        ("state", "debug_stop_at_height", "0"),
    ], cpus=args.cpus)

    def interrupted(signum, _frame):
        raise KeyboardInterrupt(f"signal {signum}")

    signal.signal(signal.SIGTERM, interrupted)
    try:
        source.start()
        source.wait_rpc_ready(args.timeout)
        source_tip = source.rpc("getblockcount")
        print("source tip", source_tip, flush=True)
        genesis.start()
        deadline = time.monotonic() + args.timeout
        while genesis.proc.poll() is None:
            if time.monotonic() > deadline:
                raise TimeoutError("genesis node did not stop at height 0")
            time.sleep(0.5)
        verify_genesis(genesis, snapshot)
        save_json(out / "genesis.json", dict(
            binary=str(binary), binary_sha256=file_hash(binary), source_tip=source_tip,
            snapshot=str(snapshot), finalized_height=0,
            cpus=check_cpus(args.cpus), source_cpus=check_cpus(args.source_cpus),
        ))
        print(snapshot)
    finally:
        try:
            genesis.stop(120)
        finally:
            source.stop(120)


if __name__ == "__main__":
    main()
