#!/usr/bin/env python3
"""Run isolated Regtest shadow-block rounds with sealed sweep fixtures."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from sweep.common import Node, admit, exact_mempool, load_fixtures, require
from node_bench import FIXTURES
from shadow.run import run


DEFAULT_FIXTURES = FIXTURES
TIMEOUT = 300


def arm_for(binary):
    program = subprocess.check_output([str(binary), "--version"], text=True, timeout=30).split()[0].lower()
    if program.startswith("zakura"):
        return "zakura-main"
    if program.startswith("zebra"):
        return "zebra-main"
    raise ValueError(f"unrecognized node binary version: {binary}: {program}")


def wait_for_ready(path, future):
    deadline = time.monotonic() + TIMEOUT
    while not path.exists():
        if future.done():
            future.result()
            raise RuntimeError("shadow observer exited before readiness")
        if time.monotonic() >= deadline:
            raise TimeoutError("shadow observer not ready: " + str(path))
        time.sleep(0.05)


def collect_rows(path, fixture_hash, labels, future):
    observed = set()
    deadline = time.monotonic() + 2 * TIMEOUT
    with path.open() as stream:
        while observed != labels:
            line = stream.readline()
            if line:
                row = json.loads(line)
                if "feed_error" in row:
                    raise RuntimeError("shadow feed: " + row["feed_error"])
                if row.get("measured") is True:
                    require(row["block_hash"] == fixture_hash, "unexpected measured block")
                    require(row["node"] in labels and row["node"] not in observed,
                            "missing, unknown or duplicate measured node")
                    require(row.get("submit_result") is None and not row.get("errors"),
                            f"measurement failed for {row['node']}: {row.get('errors')}")
                    observed.add(row["node"])
                continue
            if future.done():
                future.result()
                raise RuntimeError("shadow observer exited before all measured rows")
            if time.monotonic() >= deadline:
                raise TimeoutError("missing measured rows: " + ", ".join(sorted(labels - observed)))
            time.sleep(0.05)


def config_text(round_number, main, mirrors):
    lines = [f"round = {round_number}", "", "[main]",
             f"rpc = {json.dumps('http://' + main.rpc_addr + '/')}"]
    for label, node in mirrors:
        lines.extend(("", "[[node]]", f"label = {json.dumps(label)}",
                      f"rpc = {json.dumps('http://' + node.rpc_addr + '/')}",
                      f"p2p = {json.dumps(node.p2p_addr)}", 'network = "regtest"'))
    return "\n".join(lines) + "\n"


def smoke(main_binary, binaries, fixture, chain, rounds, output):
    output.mkdir(parents=True, exist_ok=True)
    require(not any(output.iterdir()), "output directory must be empty")
    labels = set(binaries)
    main_arm = arm_for(main_binary)
    arms = {label: arm_for(binary) for label, binary in binaries.items()}
    for number in range(1, rounds + 1):
        directory = output / f"round-{number:03d}"
        directory.mkdir()
        with ExitStack() as stack:
            main = stack.enter_context(Node(main_binary, directory / "main", arm=main_arm, timeout=TIMEOUT))
            mirrors = [(label, stack.enter_context(Node(binary, directory / label,
                                                       arm=arms[label], timeout=TIMEOUT)))
                       for label, binary in binaries.items()]
            for node in [main, *(node for _, node in mirrors)]:
                node.replay(chain)
            config = directory / "config.toml"
            config.write_text(config_text(number, main, mirrors))
            stop = threading.Event()
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(run, config, directory, stop)
                try:
                    wait_for_ready(directory / "ready", future)
                    admit(main, fixture["corpus"])
                    exact_mempool(main, fixture["corpus"], fixture["parent"], TIMEOUT)
                    for _, node in mirrors:
                        exact_mempool(node, fixture["corpus"], fixture["parent"], TIMEOUT)
                    require(main.rpc.call("submitblock", [fixture["raw"]]) is None,
                            "source candidate rejected")
                    main.wait(lambda: main.rpc.call("getbestblockhash") == fixture["hash"],
                              "candidate source tip", TIMEOUT)
                    collect_rows(directory / "rows.jsonl", fixture["hash"], labels, future)
                finally:
                    stop.set()
                future.result(timeout=TIMEOUT)
        for name in ("rows.jsonl", "mirror.jsonl"):
            with (output / name).open("a") as destination, (directory / name).open() as source:
                destination.write(source.read())
        print(f"round {number}/{rounds}: {fixture['id']} measured on {len(labels)} nodes", flush=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--main", type=Path, required=True, metavar="BINARY")
    parser.add_argument("--node", action="append", required=True, metavar="LABEL=BINARY",
                        help="isolated measured node")
    parser.add_argument("--fixture-dir", type=Path, default=DEFAULT_FIXTURES)
    parser.add_argument("--candidate", required=True, metavar="ID")
    parser.add_argument("--rounds", type=int, default=1)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.rounds < 1:
        parser.error("--rounds must be positive")
    binaries = {}
    for entry in args.node:
        label, sep, binary = entry.partition("=")
        if not sep or not binary or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,95}", label):
            parser.error("--node must be LABEL=BINARY (safe alphanumeric label)")
        if label in binaries or label == "main":
            parser.error("--node labels must be unique and cannot be 'main'")
        binaries[label] = Path(binary).expanduser().resolve()
    main_binary = args.main.expanduser().resolve()
    for binary in (main_binary, *binaries.values()):
        if not binary.is_file() or not os.access(binary, os.X_OK):
            parser.error("not an executable binary: " + str(binary))
    _, chain, fixtures = load_fixtures(args.fixture_dir)
    selected = next((fixture for fixture in fixtures if fixture["id"] == args.candidate), None)
    if selected is None:
        parser.error("--candidate must be one of: " + ", ".join(f["id"] for f in fixtures))
    smoke(main_binary, binaries, selected, chain, args.rounds, args.output.expanduser().resolve())


if __name__ == "__main__":
    main()
