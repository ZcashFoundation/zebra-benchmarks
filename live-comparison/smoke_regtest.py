#!/usr/bin/env python3
"""Run sealed Regtest fixtures against the live dispatcher and isolated nodes."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "regtest"))
from sweep.common import Node, admit, exact_mempool, load_fixtures, require

import dispatch


DEFAULT_FIXTURES = Path("/home/m/zcash/zebra-benchmarks/fixtures/zakura-1.8.1/standard")
TIMEOUT = 600
MILESTONES = ("submit_return_ms", "tip_ms", "template_ms", "peer_told_ms", "peer_has_ms")


def arm_and_version(binary):
    version = subprocess.check_output([str(binary), "--version"], text=True, timeout=30).strip()
    program = version.split()[0].lower()
    if program.startswith("zakura"):
        return "zakura-main", version
    if program.startswith("zebra"):
        return "zebra-main", version
    raise ValueError(f"unrecognized node binary version: {binary}: {version}")


def config_for(feeder, contestants, versions):
    return {
        "network": "regtest",
        "synthetic": True,
        "mirror_mempool": True,
        "mempool_poll_seconds": .05,
        "poll_interval_seconds": .02,
        "rpc_timeout_seconds": TIMEOUT,
        "sample_interval_seconds": 1,
        "template_timeout_seconds": 30,
        "pause_threshold_seconds": 2,
        "feeder": {"url": f"http://{feeder.rpc_addr}/"},
        "contestants": [
            {"name": label, "client": "Zakura" if arm == "zakura-main" else "Zebra",
             "version": versions[label][1], "url": f"http://{node.rpc_addr}/",
             "p2p": node.p2p_addr, "pid": node.proc.pid}
            for label, node, arm in contestants
        ],
    }




def check_submission(row, fixture, labels):
    require(row["target"] in labels and row["hash"] == fixture["hash"]
            and row["height"] == fixture["height"], "unexpected candidate submission")
    require(row["outcome"] == "accepted" and row["catch_up"] is False,
            f"live candidate not accepted by {row['target']}: {row['outcome']}: {row.get('detail')}")
    require(row["raw_block_hex"].lower() == fixture["raw"].lower(),
            "submitted block differs from sealed candidate")


def check_paths(row, fixture, labels):
    require(row["target"] in labels and row["hash"] == fixture["hash"]
            and row["height"] == fixture["height"], "unexpected candidate milestones")
    require(row.get("outcome") == "accepted", f"candidate path rejected: {row.get('detail')}")
    for key in (*MILESTONES, "rpc_round_trip_ms"):
        value = row.get(key)
        require(type(value) in (int, float) and value >= 0,
                f"{row['target']} missing {key}: {value!r}")


def smoke(feeder_binary, binaries, fixture, chain, rounds, output):
    output.mkdir(parents=True, exist_ok=True)
    require(not any(output.iterdir()), "output directory must be empty")
    feeder_arm, _ = arm_and_version(feeder_binary)
    versions = {label: arm_and_version(binary) for label, binary in binaries.items()}
    expected_txids = {tx["txid"] for tx in fixture["corpus"]}
    labels = set(binaries)
    expected_relay = {(label, txid) for label in labels for txid in expected_txids}
    for number in range(1, rounds + 1):
        directory = output / f"round-{number:03d}"
        directory.mkdir()
        with ExitStack() as stack:
            feeder = stack.enter_context(Node(feeder_binary, directory / "feeder",
                                              arm=feeder_arm, timeout=TIMEOUT))
            contestants = [(label, stack.enter_context(Node(binary, directory / label,
                                                           arm=versions[label][0], timeout=TIMEOUT)),
                            versions[label][0]) for label, binary in binaries.items()]
            for node in (feeder, *(node for _, node, _ in contestants)):
                node.replay(chain)
                require(node.rpc.call("getpeerinfo") == [], "smoke nodes must have no P2P peers")
            config_path = directory / "dispatch.json"
            config_path.write_text(json.dumps(config_for(feeder, contestants, versions), indent=2) + "\n")
            config = dispatch.load_config(config_path)
            rows_path = directory / "records.jsonl"
            stop = threading.Event()
            with ThreadPoolExecutor(max_workers=1) as executor:
                dispatcher = dispatch.Dispatcher(config, rows_path, stop)
                future = executor.submit(dispatcher.run)
                try:
                    deadline = time.monotonic() + TIMEOUT
                    while dispatcher.latest is None:
                        if future.done():
                            future.result()
                            raise RuntimeError("dispatcher exited before observing the feeder")
                        if time.monotonic() >= deadline:
                            raise TimeoutError("dispatcher did not observe the sealed parent")
                        time.sleep(.05)
                    require(dispatcher.latest.hash == fixture["parent"], "dispatcher initial tip")
                    # Give contestant workers time to consume their initial, unmeasured tip.
                    time.sleep(.5)
                    admit(feeder, fixture["corpus"])
                    exact_mempool(feeder, fixture["corpus"], fixture["parent"], TIMEOUT)
                    seen_relay = set()
                    seen_submissions = set()
                    seen_paths = set()

                    def consume(row):
                        event = row.get("event")
                        if event == "diagnostic":
                            raise RuntimeError(f"dispatcher {row.get('target')}: {row.get('detail')}")
                        if event == "mempool_relay":
                            key = row.get("target"), row.get("txid")
                            require(key in expected_relay, f"unexpected mirrored transaction: {key}")
                            require(row.get("outcome") == "accepted",
                                    f"mirrored transaction failed: {key}: {row.get('error')}")
                            seen_relay.add(key)
                        elif event == "submitblock_call" and row.get("hash") == fixture["hash"]:
                            check_submission(row, fixture, labels)
                            require(row["target"] not in seen_submissions,
                                    "candidate submitted more than once to a contestant")
                            seen_submissions.add(row["target"])
                        elif event == "submit_paths" and row.get("hash") == fixture["hash"]:
                            check_paths(row, fixture, labels)
                            require(row["target"] not in seen_paths,
                                    "candidate milestones recorded more than once for a contestant")
                            seen_paths.add(row["target"])

                    # Keep one reader open across both phases, so no relay or path row is missed.
                    with rows_path.open(encoding="utf-8") as stream:
                        def until(done, timeout):
                            limit = time.monotonic() + timeout
                            while not done():
                                position = stream.tell()
                                line = stream.readline()
                                if line.endswith("\n"):
                                    consume(json.loads(line))
                                    continue
                                stream.seek(position)
                                if future.done():
                                    future.result()
                                    raise RuntimeError("dispatcher exited before all smoke observations")
                                if time.monotonic() >= limit:
                                    raise TimeoutError("missing smoke observations")
                                time.sleep(.05)

                        until(lambda: seen_relay == expected_relay, TIMEOUT)
                        for _, node, _ in contestants:
                            exact_mempool(node, fixture["corpus"], fixture["parent"], TIMEOUT)
                        require(feeder.rpc.call("submitblock", [fixture["raw"]]) is None,
                                "feeder rejected sealed candidate")
                        feeder.wait(lambda: feeder.rpc.call("getbestblockhash") == fixture["hash"],
                                    "candidate feeder tip", TIMEOUT)
                        until(lambda: seen_submissions == labels and seen_paths == labels, TIMEOUT)
                    for _, node, _ in contestants:
                        require(node.rpc.call("getbestblockhash") == fixture["hash"],
                                "contestant did not commit sealed candidate")
                        require(node.rpc.call("getblock", [fixture["hash"], 0]).lower()
                                == fixture["raw"].lower(), "contestant committed another block")
                        peers = node.rpc.call("getpeerinfo")
                        require(len(peers) == 1 and peers[0].get("inbound") is True,
                                "contestant must have only its inbound P2P observer")
                finally:
                    stop.set()
                future.result(timeout=TIMEOUT)
        with (output / "records.jsonl").open("a") as destination, rows_path.open() as source:
            shutil.copyfileobj(source, destination)
        print(f"round {number}/{rounds}: {fixture['id']} live milestones and "
              f"{len(expected_relay)} mirrored transactions accepted", flush=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--feeder", type=Path, required=True, metavar="BINARY")
    parser.add_argument("--contestant", action="append", required=True, metavar="LABEL=BINARY")
    parser.add_argument("--fixture-dir", type=Path, default=DEFAULT_FIXTURES)
    parser.add_argument("--candidate", required=True, metavar="ID")
    parser.add_argument("--rounds", type=int, default=1)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.rounds < 1:
        parser.error("--rounds must be positive")
    binaries = {}
    for entry in args.contestant:
        label, sep, binary = entry.partition("=")
        if not sep or not binary or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,95}", label):
            parser.error("--contestant must be LABEL=BINARY (safe alphanumeric label)")
        if label in binaries or label == "feeder":
            parser.error("--contestant labels must be unique and cannot be 'feeder'")
        binaries[label] = Path(binary).expanduser().resolve()
    feeder_binary = args.feeder.expanduser().resolve()
    for binary in (feeder_binary, *binaries.values()):
        if not binary.is_file() or not os.access(binary, os.X_OK):
            parser.error("not an executable binary: " + str(binary))
    _, chain, fixtures = load_fixtures(args.fixture_dir)
    selected = next((fixture for fixture in fixtures if fixture["id"] == args.candidate), None)
    if selected is None:
        parser.error("--candidate must be one of: " + ", ".join(f["id"] for f in fixtures))
    smoke(feeder_binary, binaries, selected, chain, args.rounds, args.output.expanduser().resolve())


if __name__ == "__main__":
    main()
