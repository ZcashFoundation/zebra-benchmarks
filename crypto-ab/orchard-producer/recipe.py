#!/usr/bin/env python3
"""Build a separate sealed 330-action Orchard fixture using the pinned sweep harness."""

import argparse
import json
import os
import secrets
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent
SWEEP = ROOT.parent.parent / "regtest/sweep"
sys.path.insert(0, str(SWEEP))
import build_fixtures as base
from common import (
    LIMIT, NODE_POLICY, WALLET_IMAGE, Node, admit, block_identity, exact_template,
    file_hash, h, load_fixtures, no_other_pools, require, save,
)

COUNT = 330
TARGET = 1_980_000
FEE = 20_000


def sources():
    result = base.sources()
    for name in ("recipe.py", "Cargo.toml", "Cargo.lock", "src/main.rs"):
        path = ROOT / name
        require(path.is_file(), f"missing producer source: {path}")
        result["crypto-ab/orchard-producer/" + name] = file_hash(path)
    return result


def inputs(args):
    return dict(
        producer={"path": str(args.producer), "sha256": file_hash(args.producer)},
        rust_producer={"path": str(args.orchard_producer),
                       "sha256": file_hash(args.orchard_producer)},
        sizes=[TARGET],
        wallet_image=WALLET_IMAGE,
        node_policy=NODE_POLICY,
        setup_blocks=args.setup_blocks,
        sources=sources(),
    )


def funding(args, construction):
    seed_path = args.output / "seed.bin"
    addresses = subprocess.run(
        [str(args.orchard_producer), "addresses", "--seed-file", str(seed_path),
         "--count", str(COUNT)],
        check=True, capture_output=True, text=True,
    ).stdout.splitlines()
    require(len(addresses) == COUNT and len(set(addresses)) == COUNT,
            "producer must return 330 distinct Orchard-only unified addresses")
    wallet = base.Wallet(args.output / "wallet", args.timeout)
    try:
        wallet.start()
        miner = wallet.rpc("getnewaddress")
        require(miner.startswith("t"), "transparent wallet coinbase address")
        wallet.stop()
        wallet.start(miner)
        network = wallet.rpc("getnetworkinfo")
        wallet.mine(110)
        source = wallet.address()
        shield = wallet.completed(
            wallet.rpc("z_shieldcoinbase", [miner, source, None, 10])["opid"]
        )
        no_other_pools(wallet.rpc("getrawtransaction", [shield, 1]))
        wallet.mine(1)
        funding_txids = []
        for first in range(0, COUNT, 100):
            batch = addresses[first:first + 100]
            txid = wallet.send(
                source, [{"address": address, "amount": 0.02} for address in batch]
            )
            decoded = wallet.rpc("getrawtransaction", [txid, 1])
            require(len(decoded.get("orchard", {}).get("actions", [])) >= len(batch),
                    "each payout must be an Orchard action")
            funding_txids.append(txid)
            wallet.mine(1)
        require(len(funding_txids) == 4 and wallet.rpc("getrawmempool") == [],
                "four confirmed funding batches and empty mempool")
        wallet.mine(max(0, args.setup_blocks - wallet.rpc("getblockcount")))
        chain = []
        blocks = args.output / "blocks.jsonl"
        with blocks.open("x") as stream:
            for height in range(1, wallet.rpc("getblockcount") + 1):
                block_hash = wallet.rpc("getblockhash", [height])
                raw = wallet.rpc("getblock", [block_hash, 0])
                decoded = wallet.rpc("getblock", [block_hash, 2])
                require(decoded["height"] == height and decoded["hash"] == block_hash,
                        "ordered confirmed funding block")
                chain.append(dict(height=height, raw=raw, **block_identity(raw)))
                stream.write(json.dumps(decoded) + "\n")
        save(args.output / "chain.json", chain)
        subprocess.run(
            [str(args.orchard_producer), "spend", "--seed-file", str(seed_path),
             "--blocks", str(blocks), "--count", str(COUNT), "--fee", str(FEE),
             "--out", str(args.output / "corpus.json")],
            check=True,
        )
        blocks.unlink()
        corpus = json.loads((args.output / "corpus.json").read_text())
        require(len(corpus) == COUNT and all(
            t["kind"] == "orchard" and t["orchard_actions"] == 1
            and t["fee_zat"] == FEE and len(t["raw"]) == 2 * t["bytes"]
            for t in corpus
        ), "330 genuine unpadded Orchard spends with conventional fees")
        checkpoint = dict(
            schema=1,
            inputs=construction,
            files={name: file_hash(args.output / name)
                   for name in ("chain.json", "corpus.json")},
            transparent_miner=miner,
            funding={"shield_txid": shield, "payout_txids": funding_txids,
                     "payout_count": COUNT, "payout_zat": 2_000_000},
            wallet_network_info=network,
            wallet_commands=wallet.commands,
        )
        save(args.output / "checkpoint.json", checkpoint)
        save(args.output / "checkpoint.seal.json",
             {"sha256": file_hash(args.output / "checkpoint.json")})
        return checkpoint
    finally:
        wallet.stop()


def candidate(args, chain, corpus, checkpoint, construction):
    directory = args.output / "blocks"
    directory.mkdir(exist_ok=True)
    path = directory / "orchard1-1980000.json"
    seal_path = directory / "orchard1-1980000.seal.json"
    checkpoint_hash = file_hash(args.output / "checkpoint.json")
    indices = list(range(COUNT))
    stats = {
        "transparent": {"transactions": 0, "bytes": 0},
        "orchard": {"transactions": COUNT,
                    "bytes": sum(tx["bytes"] for tx in corpus)},
    }
    if seal_path.exists():
        require(json.loads(seal_path.read_text()) ==
                {"sha256": file_hash(path), "checkpoint_sha256": checkpoint_hash},
                "candidate and checkpoint seals")
        record = json.loads(path.read_text())
    else:
        require(not path.exists(), "unsealed candidate cannot be overwritten")
        attempt = args.output / "producers" / "orchard1-1980000" / secrets.token_hex(8)
        with Node(args.producer, attempt, timeout=args.timeout) as node:
            node.replay(chain)
            admit(node, corpus)
            template, _ = exact_template(node, corpus, chain[-1]["hash"], args.timeout)
            require(sum(tx["fee"] for tx in template["transactions"]) == COUNT * FEE,
                    "template contains all 330 conventional fees")
            block_hash = node.rpc.call("generate", [1])[0]
            raw = node.rpc.call("getblock", [block_hash, 0])
            mined_txids = node.rpc.call("getblock", [block_hash, 1])["tx"]
            require(len(mined_txids) == COUNT + 1 and
                    set(mined_txids[1:]) == {tx["txid"] for tx in corpus},
                    "mined block contains exactly the 330 Orchard transactions")
            coinbase = node.rpc.call("getrawtransaction", [mined_txids[0], 0])
        wallet = base.Wallet(args.output / "wallet", args.timeout, resume=True)
        try:
            wallet.start(checkpoint["transparent_miner"])
            decoded_coinbase = wallet.rpc("decoderawtransaction", [coinbase])
            no_other_pools(decoded_coinbase)
            require(decoded_coinbase["vout"] and
                    not decoded_coinbase.get("orchard", {}).get("actions"),
                    "candidate coinbase is transparent")
        finally:
            wallet.stop()
        record = dict(
            id="orchard1-1980000", filling="orchard1", target_bytes=TARGET,
            raw=raw, corpus_indices=indices,
            txids=[tx["txid"] for tx in corpus],
            transaction_count=COUNT + 1, height=len(chain) + 1,
            filling_stats=stats,
            construction={"sources": construction["sources"],
                          "node_policy": NODE_POLICY,
                          "rust_producer": construction["rust_producer"]},
            **block_identity(raw),
        )
        require(record["hash"] == block_hash, "generated block hash")
    identity = block_identity(record["raw"])
    require(all(record[k] == value for k, value in identity.items())
            and record["parent"] == chain[-1]["hash"]
            and record["bytes"] <= LIMIT
            and abs(record["bytes"] - TARGET) <= max(t["bytes"] for t in corpus) + 32
            and record["id"] == "orchard1-1980000"
            and record["filling"] == "orchard1"
            and record["target_bytes"] == TARGET
            and record["corpus_indices"] == indices
            and record["txids"] == [tx["txid"] for tx in corpus]
            and record["transaction_count"] == COUNT + 1
            and record["filling_stats"] == stats,
            "candidate matches the sealed funding and 2 MB limit")
    if not seal_path.exists():
        save(path, record)
        save(seal_path, {"sha256": file_hash(path),
                         "checkpoint_sha256": checkpoint_hash})
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--producer", type=Path, required=True,
                        help="NU5-capable Zebra executable with RPC generate")
    parser.add_argument("--orchard-producer", type=Path,
                        default=ROOT / "target/release/orchard-producer")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--setup-blocks", type=int, default=1100)
    parser.add_argument("--timeout", type=float, default=600)
    args = parser.parse_args()
    args.producer = args.producer.resolve()
    args.orchard_producer = args.orchard_producer.resolve()
    args.output = args.output.resolve()
    require(args.setup_blocks >= 1 and args.timeout > 0, "valid setup depth and timeout")
    require(all(path.is_file() and os.access(path, os.X_OK)
                for path in (args.producer, args.orchard_producer)),
            "both producer executables must exist")
    os.umask(0o077)
    if (args.output / "manifest.sha256").exists():
        manifest, _, fixtures = load_fixtures(args.output)
        require(manifest["producer"]["sha256"] == file_hash(args.producer)
                and len(fixtures) == 1
                and fixtures[0]["id"] == "orchard1-1980000",
                "already sealed manifest matches this binary and fixture")
        print("Already complete and sealed:", args.output)
        return
    if args.output.exists() and any(args.output.iterdir()):
        require((args.output / "checkpoint.json").is_file()
                and (args.output / "checkpoint.seal.json").is_file(),
                "incomplete funding has no checkpoint; use a new output directory")
    args.output.mkdir(parents=True, exist_ok=True)
    construction = inputs(args)
    checkpoint_path = args.output / "checkpoint.json"
    if checkpoint_path.exists():
        require(file_hash(checkpoint_path) ==
                json.loads((args.output / "checkpoint.seal.json").read_text())["sha256"],
                "funding checkpoint seal")
        checkpoint = json.loads(checkpoint_path.read_text())
        require(checkpoint["inputs"] == construction,
                "funding inputs changed; use a new output directory")
        for name, digest in checkpoint["files"].items():
            require(file_hash(args.output / name) == digest,
                    "sealed funding file: " + name)
    else:
        checkpoint = funding(args, construction)
    chain = json.loads((args.output / "chain.json").read_text())
    corpus = json.loads((args.output / "corpus.json").read_text())
    require(chain and len(chain) >= args.setup_blocks and len(corpus) == COUNT
            and len({tx["txid"] for tx in corpus}) == COUNT
            and sum(tx["bytes"] for tx in corpus) < LIMIT,
            "complete confirmed setup chain and 330 unique transactions under 2 MB")
    record = candidate(args, chain, corpus, checkpoint, construction)
    files = ["chain.json", "corpus.json", "checkpoint.json", "checkpoint.seal.json",
             "blocks/orchard1-1980000.json", "blocks/orchard1-1980000.seal.json"]
    manifest = dict(
        schema=1, complete=True,
        fixtures=[{key: record[key] for key in
                   ("id", "filling", "target_bytes", "bytes", "transaction_count")}
                  | {"file": "blocks/orchard1-1980000.json",
                     "construction": record["construction"]}],
        files={name: file_hash(args.output / name) for name in files},
        sources=construction["sources"],
        funding_sources=checkpoint["inputs"]["sources"],
        funding=checkpoint["funding"],
        node_policy=NODE_POLICY,
        setup_blocks=args.setup_blocks,
        setup_height=len(chain),
        producer=construction["producer"],
        rust_producer=construction["rust_producer"],
        wallet_image=WALLET_IMAGE,
        wallet_network_info=checkpoint["wallet_network_info"],
        wallet_commands=checkpoint["wallet_commands"],
        activation="NU5 at height 1; subsequent upgrades inactive",
        miner=h.P2SH_MINER,
        selection="all 330 funded one-action Orchard spends in chain order",
        reproducibility="Real proofs and wallet keys vary; all benchmark arms replay identical sealed bytes.",
        scope="Regtest real proofs; no production Equihash timing.",
    )
    save(args.output / "manifest.json", manifest)
    seal = args.output / "manifest.sha256"
    temporary = args.output / "manifest.sha256.tmp"
    temporary.write_text(file_hash(args.output / "manifest.json") + "\n")
    temporary.replace(seal)


if __name__ == "__main__":
    main()
