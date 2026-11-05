#!/usr/bin/env python3
"""Randomized matched rounds; every repetition starts fresh Regtest processes/state."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import math
import os
from pathlib import Path
import platform
import random
import re
import secrets
import shutil
import statistics
import signal
import subprocess
import threading
import time
import traceback

from common import (
    ARMS,
    ENDPOINTS,
    PROBE_ENDPOINTS,
    MAIN_REVISIONS,
    GENERATE_ENDPOINT,
    GENERATE_BLOCKS,
    NODE_POLICY,
    Node,
    admit,
    exact_template,
    exact_mempool,
    file_hash,
    load_fixtures,
    require,
    save,
    sources,
    h,
    block_identity,
    no_other_pools,
    validate_timing,
)
from ablation import (
    BASELINE,
    GENERATE_ID,
    ORCHARD_MINER,
    DEFAULT_SUBSET,
    select_fixtures,
    declare_protocol,
    case_specs,
    generated_fixture,
    stage1_reference,
)


# Keep in sync with zebra-chain/src/parameters/constants.rs.
MAX_BLOCK_REORG_HEIGHT = 1000


def synchronous_backup(node):
    config = node.config.read_text()
    require(config.count("[state]\n") == 1, "one prefix state configuration")
    node.config.write_text(config.replace(
        "[state]\n", "[state]\ndebug_skip_non_finalized_state_backup_task = true\n", 1))


def finalization_prefix(arm, binary, directory, large, chain, timeout):
    """Build shared block bytes once, and one stopped prefix state per binary/arm."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    recipe_path = directory / "recipe.json"
    prefix_dir = directory / arm
    identity = dict(binary=str(binary), binary_sha256=file_hash(binary),
                    large_sha256=large["sha256"])
    identity_path = prefix_dir / "identity.json"
    if recipe_path.exists():
        recipe = json.loads(recipe_path.read_text())
        require(recipe["large_sha256"] == large["sha256"], "same finalization target")
        require(file_hash(directory / "chain.json") == recipe["chain_sha256"],
                "deep prefix seal")
        if not identity_path.exists():
            prefix = Node(binary, prefix_dir, arm, timeout, miner=None)
            synchronous_backup(prefix)
            with prefix:
                h.replay(prefix, json.loads((directory / "chain.json").read_text()))
            save(identity_path, identity)
    else:
        prefix = Node(binary, prefix_dir, arm, timeout, miner=h.P2SH_MINER)
        synchronous_backup(prefix)
        deep_chain = list(chain) + [large]
        with prefix:
            h.replay(prefix, deep_chain)
            parent, height = large["hash"], large["height"]
            # Save at H+998. One untimed filler per restored sample initializes finalized metrics.
            for _ in range(MAX_BLOCK_REORG_HEIGHT - 2):
                template, _ = exact_template(prefix, [], parent, timeout)
                require(template["height"] == height + 1, "consecutive filler height")
                raw = h.empty_regtest_block(template)
                block = dict(raw=raw, height=height + 1, **block_identity(raw))
                accepted(prefix, block, prefix.rpc.call("submitblock", [raw]))
                deep_chain.append(block)
                parent, height = block["hash"], block["height"]
            template, _ = exact_template(prefix, [], parent, timeout)
            raw = h.empty_regtest_block(template)
            prelude = dict(raw=raw, height=template["height"], **block_identity(raw))
        save(directory / "chain.json", deep_chain)
        # Construct, but never submit, the measured candidate on a disposable copy.
        producer = Node(binary, directory / "candidate-builder", arm, timeout, miner=h.P2SH_MINER)
        shutil.copytree(prefix_dir / "state", producer.dir / "state")
        with producer:
            accepted(producer, prelude, producer.rpc.call("submitblock", [prelude["raw"]]))
            template, _ = exact_template(producer, [], prelude["hash"], timeout)
            raw = h.empty_regtest_block(template)
            candidate = dict(raw=raw, height=template["height"], **block_identity(raw))
        require(prelude["height"] == large["height"] + MAX_BLOCK_REORG_HEIGHT - 1
                and candidate["height"] == large["height"] + MAX_BLOCK_REORG_HEIGHT,
                "measured submission, not its parent, crosses the target retention boundary")
        chain_sha256 = file_hash(directory / "chain.json")
        candidate["finalization"] = dict(
            target={key: large[key] for key in ("height", "hash", "parent", "sha256", "bytes")},
            max_block_reorg_height=MAX_BLOCK_REORG_HEIGHT, filler_blocks=MAX_BLOCK_REORG_HEIGHT - 1,
            parent_height=prelude["height"], candidate_height=candidate["height"],
            prefix_chain_sha256=chain_sha256,
        )
        recipe = dict(large_sha256=large["sha256"], chain_sha256=chain_sha256,
                      prelude=prelude, candidate=candidate)
        save(recipe_path, recipe)
        save(identity_path, identity)
    require(json.loads(identity_path.read_text()) == identity, "prefix belongs to this binary/arm")
    for block in (recipe["prelude"], recipe["candidate"]):
        require(all(block[key] == value for key, value in block_identity(block["raw"]).items()),
                "deep candidate identity")
    return recipe["candidate"], prefix_dir / "state", recipe["prelude"]


def accepted(node, fixture, reply):
    require(reply is None, "submitblock accepted: " + repr(reply))
    require(
        node.rpc.call("getbestblockhash") == fixture["hash"],
        "candidate became canonical tip",
    )
    require(
        node.rpc.call("getblock", [fixture["hash"], 0]).lower()
        == fixture["raw"].lower(),
        "exact candidate body committed",
    )


def timed_submit(node, fixture):
    started = time.perf_counter_ns()
    reply = node.rpc.call("submitblock", [fixture["raw"]])
    finished = time.perf_counter_ns()
    return dict(
        start_ns=started, end_ns=finished, latency_ns=finished - started, reply=reply,
        preload=node.preload_receipt(started),
    )


def relay(sender, peer, fixture, args):
    sender.rpc.call("addnode", [peer.p2p_addr, "add"])
    sender.wait(
        lambda: sender.rpc.call("getpeerinfo"), "legacy sender link", args.timeout
    )
    peer.wait(
        lambda: peer.rpc.call("getpeerinfo"), "legacy receiver link", args.timeout
    )
    require(
        len(sender.rpc.call("getpeerinfo")) == len(peer.rpc.call("getpeerinfo")) == 1,
        "isolated two-node legacy P2P topology",
    )
    for node in (sender, peer):
        require(
            node.rpc.call("getbestblockhash") == fixture["parent"],
            "relay starts at common parent",
        )
        require(
            node.rpc.call("getrawmempool") == [],
            "relay has no candidate transaction admission",
        )
    started = threading.Event()
    stamp = {}

    def submit():
        stamp["start_ns"] = time.perf_counter_ns()
        started.set()
        reply = sender.rpc.call("submitblock", [fixture["raw"]])
        return dict(reply=reply, end_ns=time.perf_counter_ns())

    # A concurrent observer can see peer adoption before the sender RPC returns.
    # Sequential submit-then-poll would censor precisely those observations.
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(submit)
        require(started.wait(args.timeout), "submit worker started")
        deadline = time.monotonic() + args.timeout
        last_old, polls = stamp["start_ns"], 0
        while time.monotonic() < deadline:
            query_start = time.perf_counter_ns()
            tip = peer.rpc.call("getbestblockhash")
            query_end = time.perf_counter_ns()
            polls += 1
            require(
                tip in (fixture["parent"], fixture["hash"]), "no unrelated peer tip"
            )
            if tip == fixture["hash"]:
                break
            last_old = query_start
            if future.done():
                require(future.result()["reply"] is None, "sender accepted relay block")
            time.sleep(args.poll_ms / 1000)
        else:
            raise TimeoutError("legacy P2P peer did not adopt candidate")
        submitted = future.result(timeout=args.timeout)
    accepted(sender, fixture, submitted["reply"])
    accepted(peer, fixture, None)
    return dict(
        start_ns=stamp["start_ns"],
        end_ns=query_end,
        latency_ns=query_end - stamp["start_ns"],
        polls=polls,
        last_old_query_start_ns=last_old,
        first_matching_query_start_ns=query_start,
        visibility_lower_ns=max(0, last_old - stamp["start_ns"]),
        sender_submit_end_ns=submitted["end_ns"],
        sender_submit_latency_ns=submitted["end_ns"] - stamp["start_ns"],
        poll_interval_ms=args.poll_ms,
        transport="legacy-tcp",
        preload={label: node.preload_receipt(stamp["start_ns"])
                 for label, node in (("sender", sender), ("peer", peer))},
    )


def generation_case(arm, binary, directory, chain, args):
    # The timing loop follows node_bench.py's generate scenario, with a parameterized
    # Orchard-only UA, no transaction admissions, and no standalone GBT requests.
    with Node(
        binary, directory / "node", arm, args.timeout, miner=args.orchard_miner
    ) as node:
        require(
            set(node.rpc.call("z_listunifiedreceivers", [args.orchard_miner]))
            == {"orchard"},
            "generation miner must contain only an Orchard receiver",
        )
        h.replay(
            node, chain
        )  # Node.replay would request a template; do not use it here.
        require(
            node.rpc.call("getrawmempool") == [],
            "generation starts without pending transactions",
        )
        samples = []
        for _ in range(GENERATE_BLOCKS):
            started = time.perf_counter_ns()
            hashes = node.rpc.call("generate", [1])
            ended = time.perf_counter_ns()
            require(len(hashes) == 1, "one generated block per call")
            samples.append(
                dict(
                    start_ns=started,
                    end_ns=ended,
                    latency_ns=ended - started,
                    hash=hashes[0],
                    preload=node.preload_receipt(started),
                )
            )
        require(
            node.rpc.call("getbestblockhash") == samples[-1]["hash"],
            "last generated block is canonical",
        )
        for sample in samples:
            raw = node.rpc.call("getblock", [sample["hash"], 0])
            block = node.rpc.call("getblock", [sample["hash"], 1])
            require(len(block["tx"]) == 1, "generation workload is coinbase-only")
            coinbase = node.rpc.call("getrawtransaction", [block["tx"][0], 1])
            no_other_pools(coinbase)
            # A one-output coinbase bundle has one action in both Zebra main and the stack.
            require(
                coinbase["version"] == 5
                and not coinbase["vout"]
                and coinbase.get("orchard", {}).get("actions"),
                "generated coinbase pays only Orchard",
            )
            identity = block_identity(raw)
            require(
                identity["hash"] == sample["hash"], "actual generated block identity"
            )
            sample.update(
                raw=raw, block_bytes=identity["bytes"], sha256=identity["sha256"]
            )
        return dict(
            latency_ns=statistics.median(sample["latency_ns"] for sample in samples),
            generate_samples=samples,
            statistic="median of 20 generate-1 calls",
            miner_address=args.orchard_miner,
            standalone_getblocktemplate_calls=0,
        )


def served_submit(node, fixture, args):
    """Mine the exact served work, without explicitly warming a proposal verdict."""
    started = time.perf_counter_ns()
    admit(node, fixture["corpus"])
    template, polls = exact_template(node, fixture["corpus"], fixture["parent"],
                                     args.timeout, args.poll_ms / 1000)
    served_ns = time.perf_counter_ns()
    raw = h.regtest_block(template, solve=True, timeout=args.timeout)
    candidate = dict(raw=raw, height=template["height"], **block_identity(raw))
    time.sleep(args.solve_delay_seconds)
    # Observe replacement without long-polling or switching away from our served work.
    latest = node.rpc.call("getblocktemplate")
    require(latest["previousblockhash"] == candidate["parent"], "served work parent is still active")
    fields = ("version", "previousblockhash", "defaultroots", "curtime", "bits")
    changed = (
        latest["longpollid"] != template["longpollid"]
        or any(latest[key] != template[key] for key in fields)
        or latest["coinbasetxn"]["data"] != template["coinbasetxn"]["data"]
        or [tx["data"] for tx in latest["transactions"]]
        != [tx["data"] for tx in template["transactions"]]
    )
    observation = timed_submit(node, candidate)
    accepted(node, candidate, observation.pop("reply"))
    observation.update(
        preparation_ns=observation["start_ns"] - started,
        preparation="full admission, exact served work, Regtest nonce search and solve delay; no explicit proposal; excluded from submit timer",
        template_ready_ns=served_ns, template_polls=polls,
        admitted_transactions=len(fixture["corpus"]),
        template_transactions=len(template["transactions"]),
        template_longpollid=template["longpollid"], pre_submit_longpollid=latest["longpollid"],
        template_rebuilt_before_submit=changed,
        template_rebuild_detection="longpoll id or serialized work changed at pre-submit poll; byte-identical internal rebuilds are not observable",
        solve_delay_seconds=args.solve_delay_seconds,
        solve_elapsed_ns=observation["start_ns"] - served_ns,
        pow_solution="regtest-zero-equihash-36",
        pow_nonce=int.from_bytes(bytes.fromhex(raw)[108:140], "little"),
        served_block_hash=candidate["hash"], served_block_sha256=candidate["sha256"],
        served_block_bytes=candidate["bytes"],
    )
    return observation


def case(arm, binary, directory, fixture, chain, args, endpoint, *, restart_delay_seconds=None,
         prefix_state=None, prelude=None):
    if restart_delay_seconds is not None:
        require(endpoint in ("cold-submitblock-nominer", "finalize-submitblock"),
                "restart requires a cold or finalization endpoint")
    if endpoint == GENERATE_ENDPOINT:
        return generation_case(arm, binary, directory, chain, args)
    if endpoint == "submit-to-peer-tip":
        with Node(binary, directory / "sender", arm, args.timeout) as sender, Node(
            binary, directory / "peer", arm, args.timeout
        ) as peer:
            sender.replay(chain)
            peer.replay(chain)
            return relay(sender, peer, fixture, args)
    if endpoint in PROBE_ENDPOINTS or endpoint == "finalize-submitblock":
        miner = h.P2SH_MINER if endpoint == "finalize-submitblock" else None
        node = Node(binary, directory / "node", arm, args.timeout, miner=miner)
        # Only the restart submission measures startup overlap; finalization still waits.
        node.preload_wait = not (
            restart_delay_seconds is not None and endpoint == "cold-submitblock-nominer")
        if restart_delay_seconds is not None:
            if prefix_state is None:
                prefix = Node(binary, directory / "prefix", arm, args.timeout, miner=None)
                # Flush every non-finalized block before stopping; a sleep can miss backups.
                synchronous_backup(prefix)
                with prefix:
                    h.replay(prefix, chain)
                prefix_state = prefix.dir / "state"
            # Never copy an open RocksDB or replay proofs in the measured process.
            shutil.copytree(prefix_state, node.dir / "state")
        with node as node:
            if restart_delay_seconds is None:
                # Node.replay requests a template; neither no-miner control may do so.
                h.replay(node, chain)
            else:
                if prelude is not None:
                    require(node.rpc.call("getbestblockhash") == prelude["parent"],
                            "deep snapshot restored the exact H+998 tip")
                    accepted(node, prelude, node.rpc.call("submitblock", [prelude["raw"]]))
                time.sleep(restart_delay_seconds)
                require(node.rpc.call("getbestblockhash") == fixture["parent"],
                        "restart must restore the exact candidate parent without replay")
                require(node.rpc.call("getpeerinfo") == [], "restarted node must have no peers")
            require(node.rpc.call("getrawmempool") == [], "fresh replay mempool must be empty")
            preparation = {}
            if endpoint == "warm-mempool-submitblock":
                started = time.perf_counter_ns()
                admit(node, fixture["corpus"])
                polls = exact_mempool(node, fixture["corpus"], fixture["parent"],
                                     args.timeout, args.poll_ms / 1000)
                preparation = dict(
                    preparation_ns=time.perf_counter_ns() - started,
                    preparation="full admission and exact mempool; no miner, templates or proposals; excluded from submit timer",
                    mempool_polls=polls, admitted_transactions=len(fixture["corpus"]),
                )
            observation = timed_submit(node, fixture)
            accepted(node, fixture, observation.pop("reply"))
            return dict(observation, **preparation)
    with Node(binary, directory / "node", arm, args.timeout) as node:
        node.replay(chain)
        if endpoint == "served-submitblock":
            return served_submit(node, fixture, args)
        preparation_ns = None
        if endpoint in ("admission-to-template", "prepared-submitblock"):
            started = time.perf_counter_ns()
            admit(node, fixture["corpus"])
            template, polls = exact_template(
                node,
                fixture["corpus"],
                fixture["parent"],
                args.timeout,
                args.poll_ms / 1000,
            )
            ended = time.perf_counter_ns()
            if endpoint == "admission-to-template":
                return dict(
                    start_ns=started,
                    end_ns=ended,
                    latency_ns=ended - started,
                    preload=node.preload_receipt(started),
                    template_polls=polls,
                    admitted_transactions=len(fixture["corpus"]),
                    template_transactions=len(template["transactions"]),
                )
            result = node.rpc.call(
                "getblocktemplate", [{"mode": "proposal", "data": fixture["raw"]}]
            )
            preparation_ns = time.perf_counter_ns() - started
            require(result is None, "identical candidate explicitly proposal-validated")
        observation = timed_submit(node, fixture)
        accepted(node, fixture, observation.pop("reply"))
        if preparation_ns is not None:
            observation.update(
                preparation_ns=preparation_ns,
                preparation="full admission, exact template and exact-body proposal check; excluded from submit timer",
            )
        return observation


def checkpoint_manifest(directory, desired):
    path = directory / "manifest.json"
    if path.exists():
        previous = json.loads(path.read_text())
        differences = [
            key
            for key in sorted(set(previous) | set(desired))
            if previous.get(key) != desired.get(key)
        ]
        require(
            not differences,
            "Cannot resume: manifest differs in " + ", ".join(differences),
        )
    else:
        require(
            not directory.exists() or not any(directory.iterdir()),
            "Cannot resume a nonempty output directory without manifest.json",
        )
        directory.mkdir(parents=True, exist_ok=True)
        save(path, desired)
    for name in ("observations", "failures"):
        (directory / name).mkdir(exist_ok=True)
    return file_hash(path)


def load_receipt(path, identity):
    if not path.exists():
        return None
    row = json.loads(path.read_text())
    require(
        all(row.get(key) == value for key, value in identity.items()),
        "Receipt identity differs: " + str(path),
    )
    require(
        row.get("status") in ("ok", "error"), "Invalid receipt status: " + str(path)
    )
    if row["status"] == "ok":
        validate_timing(row)
    else:
        require(bool(row.get("error")), "Failure receipt lacks traceback: " + str(path))
    return row


def needs_attempt(receipt, retry_failed):
    return receipt is None or (receipt["status"] == "error" and retry_failed)


def write_observations(directory, rows):
    path = directory / "observations.jsonl"
    temporary = directory / "observations.jsonl.tmp"
    with temporary.open("w") as stream:
        for row in rows:
            stream.write(json.dumps(row, sort_keys=True) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__, epilog=h.BENCH_HELP)
    parser.add_argument("--arm", action="append", required=True, metavar="NAME=PATH")
    parser.add_argument(
        "--provenance",
        type=Path,
        required=True,
        help="arm -> {source, sha256, build, ...}; hashes checked against actual executables",
    )
    parser.add_argument("--fixtures", type=Path, required=True)
    parser.add_argument(
        "--fixtures-subset",
        nargs="+",
        metavar="FIXTURE_ID",
        help="exact fixture ids; empty is an alias for the unique coinbase-only fixture",
    )
    parser.add_argument("--ablation-stage", type=int, choices=(1, 2))
    parser.add_argument(
        "--stage1", type=Path, help="completed stage-1 run for confirmatory stage 2"
    )
    parser.add_argument(
        "--confirm-seed",
        type=int,
        default=20261016,
        help="stage-2 seed predeclared in the stage-1 manifest",
    )
    parser.add_argument(
        "--orchard-miner",
        default=ORCHARD_MINER,
        help="Orchard-only Regtest UA for the 20-block generate primary",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument(
        "--seed",
        type=int,
        help="defaults to 20261015, or the predeclared seed in stage 2",
    )
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--poll-ms", type=float, default=1.0)
    parser.add_argument("--solve-delay-seconds", type=float, default=1.0,
                        help="served-submitblock delay after mining served work, before submission (default 1; 0 supported)")
    parser.add_argument("--endpoints", nargs="+", choices=ENDPOINTS,
                        help="comparison endpoints to run; ablations keep their declared matrix")
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="retry error receipts; prior failures remain immutable under failures/",
    )
    parser.add_argument(
        "--through-round",
        type=int,
        metavar="N",
        help="execution only, not part of the manifest: attempt rounds 1..N, then exit, "
        "succeeding only if all their endpoints have ok receipts",
    )
    args = parser.parse_args()
    if not math.isfinite(args.solve_delay_seconds) or args.solve_delay_seconds < 0:
        parser.error("--solve-delay-seconds must be finite and nonnegative")
    require(
        args.rounds >= 1
        and args.repetitions >= 1
        and args.timeout > 0
        and 0.1 <= args.poll_ms <= 100
        and (args.through_round is None or 1 <= args.through_round <= args.rounds),
        "positive repetitions/deadlines, bounded polling and a planned --through-round",
    )
    arms = {}
    for spec in args.arm:
        name, path = spec.split("=", 1)
        require(
            re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,95}", name) is not None
            and name not in arms,
            "distinct filesystem-safe arm names",
        )
        arms[name] = Path(path).resolve()
    require(
        len(arms) >= 2 and (not args.ablation_stage or BASELINE in arms),
        "at least two comparison arms; ablations require the optimized baseline",
    )
    arm_names = list(ARMS) if set(arms) == set(ARMS) else sorted(arms)
    provenance = json.loads(args.provenance.read_text())
    binaries = {}
    for name, path in arms.items():
        require(path.is_file() and os.access(path, os.X_OK), "executable: " + str(path))
        declared = provenance[name]
        require(
            isinstance(declared, dict)
            and declared.get("source")
            and declared.get("build"),
            "completed build provenance required: " + name,
        )
        digest = file_hash(path)
        require(digest == declared["sha256"], "binary/provenance checksum: " + name)
        if name in MAIN_REVISIONS:
            require(
                declared["source"] == MAIN_REVISIONS[name],
                "current-main source pin: " + name,
            )
        binaries[name] = dict(
            declared,
            path=str(path),
            sha256=digest,
            binary_preloads=h.binary_preloads(str(path.resolve())),
            observed_version=subprocess.check_output(
                [str(path), "--version"], text=True, timeout=30
            ).strip(),
        )
    fixture_manifest, chain, all_fixtures = load_fixtures(args.fixtures)
    protocol = reference = None
    if args.ablation_stage == 2:
        require(
            args.stage1 is not None and args.fixtures_subset is None,
            "stage 2 requires --stage1 and uses only predeclared primaries, not --fixtures-subset",
        )
        previous, reference = stage1_reference(args.stage1)
        require(
            args.output.resolve() != args.stage1.resolve(),
            "stage 2 requires a separate fresh run directory",
        )
        protocol = previous["ablation_protocol"]
        args.seed = protocol["stage2"]["seed"] if args.seed is None else args.seed
        require(
            args.seed == protocol["stage2"]["seed"] != previous["seed"]
            and args.rounds == protocol["stage2"]["rounds"]
            and args.repetitions == protocol["stage2"]["repetitions"],
            "stage-2 seed/rounds/repetitions must match the predeclared protocol",
        )
        require(
            binaries == previous["arms"]
            and sources() == previous["sources"]
            and NODE_POLICY == previous["node_policy"]
            and file_hash(args.fixtures / "manifest.json")
            == previous["fixture_manifest_sha256"],
            "both stages must use identical binaries, source hashes, node policy and sealed fixtures",
        )
        require(
            args.orchard_miner == protocol["generate"]["miner_address"],
            "same predeclared Orchard miner",
        )
        required_ids = {
            primary["fixture"] for primary in protocol["primaries"].values()
        } - {GENERATE_ID}
        fixtures = [
            fixture for fixture in all_fixtures if fixture["id"] in required_ids
        ]
        require(
            {fixture["id"] for fixture in fixtures} == required_ids,
            "all predeclared primary fixtures present",
        )
    else:
        require(args.stage1 is None, "--stage1 is only valid for ablation stage 2")
        args.seed = 20261015 if args.seed is None else args.seed
        subset = args.fixtures_subset
        if args.ablation_stage == 1 and subset is None:
            subset = DEFAULT_SUBSET
        fixtures = select_fixtures(all_fixtures, subset)
        if args.ablation_stage == 1:
            protocol = declare_protocol(
                arm_names,
                fixtures,
                args.seed,
                args.confirm_seed,
                args.rounds,
                args.repetitions,
                args.orchard_miner,
            )
    specs = case_specs(arm_names, fixtures, protocol, args.ablation_stage)
    if args.endpoints is not None:
        require(not args.ablation_stage, "--endpoints is only valid for comparisons")
        require(len(args.endpoints) == len(set(args.endpoints)), "distinct endpoints")
        for spec in specs:
            spec["endpoints"] = args.endpoints
    cells = [
        (spec["fixture"], endpoint, arm)
        for spec in specs
        for endpoint in spec["endpoints"]
        for arm in spec["arms"]
    ]
    require(len(cells) == len(set(cells)), "unique predeclared measurement cells")
    measurement_fixtures = [
        entry
        for entry in fixture_manifest["fixtures"]
        if entry["id"] in {fixture["id"] for fixture in fixtures}
    ]
    if any(spec["fixture"] == GENERATE_ID for spec in specs):
        descriptor = generated_fixture(fixture_manifest["files"]["chain.json"])
        measurement_fixtures.append(descriptor)
        fixtures.append(
            dict(
                descriptor,
                parent=chain[-1]["hash"],
                sha256=None,
                filling_stats={},
                corpus=[],
            )
        )
    rng, schedule = random.Random(args.seed), []
    for round_index in range(args.rounds):
        work = [(spec, rep) for spec in specs for rep in range(args.repetitions)]
        rng.shuffle(work)
        for spec, repetition in work:
            order = list(spec["arms"])
            rng.shuffle(order)
            schedule.append(
                dict(
                    round=round_index,
                    fixture=spec["fixture"],
                    repetition=repetition,
                    endpoints=spec["endpoints"],
                    arms=order,
                )
            )
    args.output = args.output.resolve()
    os.umask(0o077)
    desired_manifest = dict(
        schema=3,
        arms=binaries,
        rounds=args.rounds,
        repetitions=args.repetitions,
        seed=args.seed,
        campaign="ablation" if args.ablation_stage else "comparison",
        ablation_stage=args.ablation_stage,
        ablation_protocol=protocol,
        stage1_reference=reference,
        measurement_fixtures=measurement_fixtures,
        case_specs=specs,
        node_policy=NODE_POLICY,
        fixture_manifest_sha256=file_hash(args.fixtures / "manifest.json"),
        fixture_manifest=fixture_manifest,
        sources=sources(),
        schedule=schedule,
        endpoints=list(
            dict.fromkeys(endpoint for spec in specs for endpoint in spec["endpoints"])
        ),
        poll_interval_ms=args.poll_ms,
        timeout_seconds=args.timeout,
        **({"solve_delay_seconds": args.solve_delay_seconds}
           if any("served-submitblock" in spec["endpoints"] for spec in specs) else {}),
        host=dict(
            platform=platform.platform(),
            machine=platform.machine(),
            cpu_count=os.cpu_count(),
        ),
        clock=vars(time.get_clock_info("perf_counter")),
        notes=[
            "Admission/prepared/cold/relay use sealed fixture bytes. Served submission uses fixture admission but mines that node's exact served work; served_block_* identifies its actual bytes. Generation uses the same sealed prefix and Orchard-only miner but real randomized proof/block bytes.",
            "Every fixture/repetition/endpoint uses fresh processes and fresh state; prepared setup repeats admission without replacing its separate measurement.",
            "Cold means candidate transactions were never admitted or proposal-checked; funding replay still warms code/state. OS page caches are not flushed.",
            "Prepared is a protocol, not evidence that any implementation-specific production-PoW cache gate was reached on Regtest.",
            "Relay is cold sender submission to first receiver best-tip RPC receipt over isolated legacy TCP; native QUIC is not measured.",
            "Client timers include HTTP, JSON and exact-template matching; relay polling adds observation granularity, not internal commit timestamps.",
            "Empty is one real coinbase-only point. Per-endpoint failures are retained; the matrix continues and incomplete results are not analyzable.",
            "Resume requires the identical manifest. Successful receipts are never remeasured; --retry-failed explicitly retries errors while preserving their tracebacks.",
        ],
    )
    manifest_hash = checkpoint_manifest(args.output, desired_manifest)
    by_id = {fixture["id"]: fixture for fixture in fixtures}
    plan = []
    for item in schedule:
        fixture = by_id[item["fixture"]]
        for order, arm in enumerate(item["arms"]):
            identity = dict(
                round=item["round"],
                repetition=item["repetition"],
                arm=arm,
                arm_order=order,
                fixture=fixture["id"],
                filling=fixture["filling"],
                block_bytes=fixture["bytes"],
                transaction_count=fixture["transaction_count"],
                filling_stats=fixture["filling_stats"],
                candidate_sha256=fixture["sha256"],
                setup_chain_sha256=fixture_manifest["files"]["chain.json"],
                binary_sha256=binaries[arm]["sha256"],
                manifest_sha256=manifest_hash,
            )
            for endpoint in item["endpoints"]:
                stem = f"r{item['round']:02d}-{fixture['id']}-n{item['repetition']:02d}-{arm}-{endpoint}"
                plan.append((stem, dict(identity, endpoint=endpoint)))
    expected_names = {stem + ".json" for stem, _ in plan}
    require(
        {p.name for p in (args.output / "observations").glob("*.json")}
        <= expected_names,
        "Unknown receipt files in output directory",
    )
    receipts = {}
    for stem, identity in plan:
        row = load_receipt(args.output / "observations" / (stem + ".json"), identity)
        if row is not None:
            receipts[stem] = row
    completion_path = args.output / "completion.json"
    if completion_path.exists() and (
        len(receipts) != len(plan)
        or any(row["status"] != "ok" for row in receipts.values())
    ):
        completion_path.replace(
            args.output / ("completion.previous-" + secrets.token_hex(8) + ".json")
        )

    def interrupted(signum, _frame):
        raise KeyboardInterrupt("signal " + str(signum))

    signal.signal(signal.SIGTERM, interrupted)
    try:
        for stem, identity in plan:
            if args.through_round is not None and identity["round"] >= args.through_round:
                continue
            if not needs_attempt(receipts.get(stem), args.retry_failed):
                continue
            attempt = secrets.token_hex(16)
            started = time.time_ns()
            try:
                observation = case(
                    identity["arm"],
                    arms[identity["arm"]],
                    args.output / "nodes" / stem / attempt,
                    by_id[identity["fixture"]],
                    chain,
                    args,
                    identity["endpoint"],
                )
                require(observation["latency_ns"] > 0, "strictly positive observation")
                row = dict(
                    identity,
                    status="ok",
                    attempt=attempt,
                    attempt_started_unix_ns=started,
                    **observation,
                )
                validate_timing(row)
                save(args.output / "observations" / (stem + ".json"), row)
                receipts[stem] = row
            except BaseException as error:
                row = dict(
                    identity,
                    status="error",
                    attempt=attempt,
                    attempt_started_unix_ns=started,
                    attempt_finished_unix_ns=time.time_ns(),
                    error=traceback.format_exc(),
                )
                # Preserve every failure before replacing the canonical endpoint receipt.
                save(args.output / "failures" / (stem + "-" + attempt + ".json"), row)
                save(args.output / "observations" / (stem + ".json"), row)
                receipts[stem] = row
                print("FAILED", stem, repr(error), flush=True)
                if not isinstance(error, Exception):
                    raise
    finally:
        ordered = [receipts[stem] for stem, _ in plan if stem in receipts]
        write_observations(args.output, ordered)
        save(
            args.output / "status.json",
            dict(
                planned=len(plan),
                ok=sum(r["status"] == "ok" for r in ordered),
                failed=sum(r["status"] == "error" for r in ordered),
                missing=len(plan) - len(ordered),
            ),
        )
    if args.through_round is not None:
        selected = [stem for stem, identity in plan if identity["round"] < args.through_round]
        bad = sum(stem not in receipts or receipts[stem]["status"] != "ok" for stem in selected)
        if bad:
            raise SystemExit(
                f"Rounds 1-{args.through_round} incomplete: {bad} of {len(selected)} endpoints "
                "missing or failed; receipts preserved."
            )
        print(f"Rounds 1-{args.through_round} complete: {len(selected)} ok endpoints.", flush=True)
        return
    failures = sum(row["status"] == "error" for row in receipts.values())
    if len(receipts) != len(plan) or failures:
        raise SystemExit(
            f"Incomplete sweep: {failures} failed endpoints; receipts preserved. "
            "Repeat the identical command with --retry-failed to retry errors."
        )
    save(
        completion_path,
        dict(
            success=True,
            observations=len(receipts),
            observations_sha256=file_hash(args.output / "observations.jsonl"),
            manifest_sha256=manifest_hash,
            failure_receipts={
                str(path.relative_to(args.output)): file_hash(path)
                for path in sorted((args.output / "failures").glob("*.json"))
            },
        ),
    )


if __name__ == "__main__":
    main()
