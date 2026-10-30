#!/usr/bin/env python3
"""Build once, then reuse sealed real transactions and identical candidate bytes."""

import argparse
import base64
import http.client
import json
import math
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import time
import traceback

from common import (
    LIMIT,
    WALLET_IMAGE,
    NODE_POLICY,
    Node,
    admit,
    block_identity,
    exact_template,
    file_hash,
    h,
    no_other_pools,
    require,
    load_fixtures,
    save,
    sha256,
    sources,
    transaction,
)

from protocol import fixture as protocol_fixture

SIZES = (2_000, 25_000, 100_000, 250_000, 500_000, 1_000_000, 1_500_000, 1_900_000)


class Wallet:
    """Pinned zcashd container with fresh local RPC credentials."""

    def __init__(self, directory, timeout, resume=False):
        self.directory, self.timeout = directory, timeout
        if resume:
            require(directory.is_dir(), "checkpoint wallet directory is missing")
        else:
            self.directory.mkdir()
        self.password = secrets.token_hex(20)
        (directory / "zcash.conf").write_text(
            "server=1\ni-am-aware-zcashd-will-be-replaced-by-zebrad-and-zallet-in-2025=1\n"
            "rpcuser=sweep\nrpcpassword=" + self.password + "\n"
        )
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", h.next_port()))
            self.port = sock.getsockname()[1]
        self.name = "zcash-size-sweep-" + secrets.token_hex(8)
        self.proc = self.log = None
        self.commands = []
        self.starts = 0

    def rpc(self, method, params=None):
        connection = http.client.HTTPConnection(
            "127.0.0.1", self.port, timeout=self.timeout
        )
        auth = base64.b64encode(("sweep:" + self.password).encode()).decode()
        try:
            connection.request(
                "POST",
                "/",
                json.dumps(
                    dict(jsonrpc="2.0", id=1, method=method, params=params or [])
                ),
                {"Authorization": "Basic " + auth, "Content-Type": "application/json"},
            )
            value = json.loads(connection.getresponse().read())
            require(not value.get("error"), "wallet RPC: " + str(value.get("error")))
            return value["result"]
        finally:
            connection.close()

    def wait(self, predicate, label):
        deadline, last = time.monotonic() + self.timeout, None
        while time.monotonic() < deadline:
            require(self.proc.poll() is None, "wallet exited: " + label)
            try:
                value = predicate()
                if value:
                    return value
            except (OSError, http.client.HTTPException) as error:
                last = str(error)
            time.sleep(0.05)
        raise TimeoutError(label + ": " + str(last))

    def start(self, miner=None):
        command = [
            "podman",
            "run",
            "--rm",
            "--name",
            self.name,
            "--network",
            "host",
            "-v",
            str(self.directory) + ":/wallet:rw",
            "-e",
            "RAYON_NUM_THREADS=2",
            "--entrypoint",
            "zcashd",
            WALLET_IMAGE,
            "-datadir=/wallet",
            "-regtest",
            "-daemon=0",
            "-printtoconsole",
            "-listen=0",
            "-dnsseed=0",
            "-discover=0",
            "-connect=0",
            "-maxconnections=0",
            "-rpcbind=127.0.0.1",
            "-rpcallowip=127.0.0.1",
            "-rpcport=" + str(self.port),
            "-maxtipage=3153600000",
            "-txindex=1",
            "-preferredtxversion=5",
            "-par=2",
            # The transparent coinbase key needs the deprecated getnewaddress RPC.
            "-allowdeprecated=getnewaddress",
            # Funding sends 100 Orchard outputs per transaction; the default limit is 50.
            "-orchardactionlimit=200",
        ]
        command += [
            "-nuparams=" + branch + ":1"
            for branch in (
                "5ba81b19",
                "76b809bb",
                "2bb40e60",
                "f5b9230b",
                "e9ff75a6",
                "c2d6d0b4",
            )
        ]
        if miner:
            command += ["-mineraddress=" + miner]
        self.commands.append(command)
        self.log = (
            self.directory.parent / f"wallet-{self.name[-16:]}-{self.starts}.log"
        ).open("xb")
        self.starts += 1
        self.proc = subprocess.Popen(command, stdout=self.log, stderr=subprocess.STDOUT)

        # Startup's warmup RPC error is expected; transaction-operation errors are not.
        def ready():
            try:
                return self.rpc("getblockchaininfo")
            except RuntimeError as error:
                if '"code": -28' in str(error) or "'code': -28" in str(error):
                    return None
                raise

        self.wait(ready, "wallet startup")

    def stop(self):
        if self.proc is not None and self.proc.poll() is None:
            try:
                self.rpc("stop")
                self.proc.wait(timeout=30)
            except (
                OSError,
                RuntimeError,
                http.client.HTTPException,
                subprocess.TimeoutExpired,
            ):
                subprocess.run(
                    ["podman", "stop", "--time", "10", self.name],
                    check=True,
                    timeout=30,
                )
                self.proc.wait(timeout=30)
        if self.log is not None:
            self.log.close()
        self.proc = self.log = None

    def mine(self, count):
        hashes = []
        while count:
            chunk = min(count, 10)
            hashes += self.rpc("generate", [chunk])
            count -= chunk
            self.wait(
                lambda: self.rpc("getblockchaininfo")["fullyNotified"],
                "wallet notifications",
            )
        return hashes

    def completed(self, operation):
        def done():
            rows = self.rpc("z_getoperationstatus", [[operation]])
            require(len(rows) == 1, "wallet operation exists")
            row = rows[0]
            require(row["status"] not in ("failed", "cancelled"), str(row))
            return row["result"]["txid"] if row["status"] == "success" else None

        return self.wait(done, "wallet proof " + operation)

    def address(self):
        account = self.rpc("z_getnewaccount")["account"]
        address = self.rpc("z_getaddressforaccount", [account, ["orchard"]])["address"]
        require(
            set(self.rpc("z_listunifiedreceivers", [address])) == {"orchard"},
            "only an Orchard receiver may be generated",
        )
        return address

    def send(self, source, recipients, fee=None):
        return self.completed(
            self.rpc("z_sendmany", [source, recipients, 1, fee, "FullPrivacy"])
        )


def transparent(source, values):
    raw, txid = h.v4_tx([source], values)
    parsed, end = protocol_fixture.parse_v4(raw)
    require(
        end == len(raw)
        and not parsed["sapling_spends"]
        and not parsed["sapling_outputs"]
        and not parsed["joinsplits"],
        "transparent-only serialized transaction",
    )
    return dict(
        txid=txid,
        raw=raw.hex(),
        kind="transparent",
        bytes=len(raw),
        sha256=sha256(raw),
        orchard_actions=0,
        fee_zat=source[2] - sum(values),
    )


def prefix_for_budget(indices, corpus, budget):
    total, selected = 0, []
    for index in indices:
        size = corpus[index]["bytes"]
        if abs(total + size - budget) > abs(total - budget):
            break
        selected.append(index)
        total += size
    require(selected, "target must fit at least one real transaction")
    return selected, total


def select(corpus, filling, target, empty_bytes):
    # Reserve the change from one-byte to three-byte CompactSize tx count.
    budget = target - empty_bytes - 2
    groups = {
        kind: [i for i, t in enumerate(corpus) if t["kind"] == kind]
        for kind in ("transparent", "transparent-signed", "orchard")
    }
    if filling == "mixed":
        orchard, used = prefix_for_budget(groups["orchard"], corpus, budget / 2)
        transparent_indices, _ = prefix_for_budget(
            groups["transparent"], corpus, budget - used
        )
        return transparent_indices + orchard
    return prefix_for_budget(groups[filling], corpus, budget)[0]


def build_inputs(args):
    inputs = dict(
        producer={"path": str(args.producer), "sha256": file_hash(args.producer)},
        sizes=list(args.sizes),
        wallet_image=WALLET_IMAGE,
        node_policy=NODE_POLICY,
        sources=sources(),
    )
    if args.transparent_signed:
        import cryptography
        import transparent_signed
        inputs["transparent_signed"] = dict(transparent_signed.LAYOUT,
                                             cryptography_version=cryptography.__version__)
    return inputs


def checkpoint_changes(previous, current, allow_source_change):
    require(all(previous[key] == current[key] for key in ("producer", "sizes", "wallet_image")),
            "sealed funding producer, size ladder or wallet image changed; cannot reuse corpus")
    require(previous.get("transparent_signed") == current.get("transparent_signed"),
            "signed filling/signing recipe changed; use a new output directory")
    changes = [key for key in sorted(set(previous) | set(current))
               if previous.get(key) != current.get(key)]
    require(not changes or allow_source_change,
            "fixture checkpoint construction inputs changed (" + ", ".join(changes)
            + "); use --resume-sealed-corpus to preserve verified funding/corpus and existing block seals")
    return changes


def fund(args):
    signer = None
    if args.transparent_signed:
        import transparent_signed as signer
    wallet = Wallet(args.output / "wallet", args.timeout)
    try:
        wallet.start()
        transparent_miner = wallet.rpc("getnewaddress")
        require(
            transparent_miner.startswith("t"), "transparent wallet coinbase address"
        )
        wallet.stop()
        wallet.start(transparent_miner)
        network_info = wallet.rpc("getnetworkinfo")
        wallet.mine(110)
        if not signer:
            funding, sink = wallet.address(), wallet.address()
            # https://zcash.github.io/rpc/z_shieldcoinbase.html (ZIP-317 fee selection).
            shield = wallet.completed(
                wallet.rpc("z_shieldcoinbase", [transparent_miner, funding, None, 10])[
                    "opid"
                ]
            )
            no_other_pools(wallet.rpc("getrawtransaction", [shield, 1]))
            wallet.mine(1)

        # zcashd only mines transparent P2PKH addresses. Move mature wallet coinbases
        # to OP_TRUE P2SH using signed transparent transactions, on this same chain.
        # Signed spends normally occupy 210/211 bytes; 200 leaves corpus headroom.
        spend_bytes = 200 if signer else transparent(
            ("00" * 32, 0, 1_000_000), [1_000_000 - h.FEE]
        )["bytes"]
        fanout_count = math.ceil((max(args.sizes) // spend_bytes + 1) / 200)
        hashes = wallet.mine(100 + fanout_count)
        funding_sources = []
        for block_hash in hashes[:fanout_count]:
            txid = wallet.rpc("getblock", [block_hash, 1])["tx"][0]
            coinbase = wallet.rpc("getrawtransaction", [txid, 1])
            no_other_pools(coinbase)
            payouts = [
                v
                for v in coinbase["vout"]
                if transparent_miner in v["scriptPubKey"].get("addresses", [])
            ]
            require(len(payouts) == 1, "one wallet-owned P2PKH coinbase payout")
            payout = payouts[0]
            value = payout["valueZat"] - h.FEE
            unsigned = wallet.rpc(
                "createrawtransaction",
                [
                    [{"txid": txid, "vout": payout["n"]}],
                    {h.P2SH_MINER: value / 1e8},
                    0,
                    0,
                ],
            )
            signed = wallet.rpc("signrawtransaction", [unsigned])
            require(signed["complete"], "wallet signed its mature P2PKH coinbase")
            decoded = wallet.rpc("decoderawtransaction", [signed["hex"]])
            no_other_pools(decoded)
            require(
                not decoded.get("orchard", {}).get("actions")
                and len(decoded["vin"]) == len(decoded["vout"]) == 1
                and decoded["vout"][0]["scriptPubKey"]["hex"] == h.SCRIPT.hex()
                and decoded["vout"][0]["valueZat"] == value,
                "fully transparent P2PKH-to-OP_TRUE funding transaction",
            )
            require(
                wallet.rpc("sendrawtransaction", [signed["hex"]]) == decoded["txid"],
                "wallet funding transaction admitted",
            )
            funding_sources.append((decoded["txid"], 0, value))
        wallet.mine(1)
        require(
            wallet.rpc("getrawmempool") == [], "all OP_TRUE funding outputs confirmed"
        )
        utxos = []
        for source in funding_sources:
            each = (source[2] - h.FANOUT_FEE) // 200
            require(each > h.FEE + 100_000, "dust-safe transparent funding")
            if signer:
                raw, txid = signer.fanout(source, [each] * 200)
                fanout = dict(raw=raw.hex(), txid=txid)
            else:
                fanout = transparent(source, [each] * 200)
            require(
                wallet.rpc("sendrawtransaction", [fanout["raw"]]) == fanout["txid"],
                "fanout admitted",
            )
            utxos.extend((fanout["txid"], i, each) for i in range(200))
        wallet.mine(1)
        require(wallet.rpc("getrawmempool") == [], "all fee-paying fanouts selected")

        accounts, probe = [], {"bytes": 0}
        if not signer:
            # The real Orchard sizing probe is mined into setup, never reused in a candidate.
            probe_source = wallet.address()
            wallet.send(funding, [{"address": probe_source, "amount": 0.02}])
            wallet.mine(1)
            probe_id = wallet.send(
                probe_source, [{"address": sink, "amount": 0.01}], h.FEE / 1e8
            )
            probe = transaction(wallet.rpc, probe_id, "orchard")
            wallet.mine(1)
            count = math.ceil(max(args.sizes) / probe["bytes"]) + 2
            accounts = [wallet.address() for _ in range(count)]
            for start in range(0, count, 100):
                wallet.send(
                    funding,
                    [
                        {"address": address, "amount": 0.02}
                        for address in accounts[start : start + 100]
                    ],
                )
                wallet.mine(1)
        require(
            wallet.rpc("getrawmempool") == [],
            "funding complete before corpus construction",
        )

        chain = []
        for height in range(1, wallet.rpc("getblockcount") + 1):
            block_hash = wallet.rpc("getblockhash", [height])
            raw = wallet.rpc("getblock", [block_hash, 0])
            block = wallet.rpc("getblock", [block_hash, 1])
            for index, txid in enumerate(block["tx"]):
                decoded = wallet.rpc("getrawtransaction", [txid, 1])
                no_other_pools(decoded)
                if index == 0:
                    require(
                        decoded["vout"]
                        and not decoded.get("orchard", {}).get("actions"),
                        "all setup coinbases must be transparent",
                    )
            chain.append(dict(height=height, raw=raw, **block_identity(raw)))
        save(args.output / "chain.json", chain)

        corpus = [signer.spend(u) if signer else transparent(u, [u[2] - h.FEE]) for u in utxos]
        for address in accounts:
            txid = wallet.send(
                address, [{"address": sink, "amount": 0.01}], h.FEE / 1e8
            )
            tx = transaction(wallet.rpc, txid, "orchard")
            require(
                tx["orchard_actions"] == 2,
                "one spend and two outputs give two Orchard actions",
            )
            tx["fee_zat"] = h.FEE
            corpus.append(tx)
        save(args.output / "corpus.json", corpus)
        checkpoint = dict(
            schema=1,
            inputs=build_inputs(args),
            files={
                name: file_hash(args.output / name)
                for name in ("chain.json", "corpus.json")
            },
            transparent_miner=transparent_miner,
            probe_bytes=probe["bytes"],
            wallet_network_info=network_info,
            wallet_commands=wallet.commands,
        )
        save(args.output / "checkpoint.json", checkpoint)
        save(
            args.output / "checkpoint.seal.json",
            {"sha256": file_hash(args.output / "checkpoint.json")},
        )
        return checkpoint
    finally:
        wallet.stop()


def build(args):
    checkpoint_path = args.output / "checkpoint.json"
    current_inputs = build_inputs(args)
    input_changes = []
    if checkpoint_path.exists():
        seal = json.loads((args.output / "checkpoint.seal.json").read_text())
        require(file_hash(checkpoint_path) == seal["sha256"], "funding checkpoint seal")
        checkpoint = json.loads(checkpoint_path.read_text())
        input_changes = checkpoint_changes(checkpoint["inputs"], current_inputs, args.resume_sealed_corpus)
        for name, digest in checkpoint["files"].items():
            require(
                file_hash(args.output / name) == digest,
                "funding checkpoint file: " + name,
            )
    else:
        checkpoint = fund(args)
    checkpoint_hash = file_hash(checkpoint_path)
    chain = json.loads((args.output / "chain.json").read_text())
    corpus = json.loads((args.output / "corpus.json").read_text())
    fixtures, files = [], [
        "chain.json",
        "corpus.json",
        "checkpoint.json",
        "checkpoint.seal.json",
    ]
    (args.output / "blocks").mkdir(exist_ok=True)
    wallet = Wallet(args.output / "wallet", args.timeout, resume=True)
    try:
        # No mining or transaction creation on resume: only decode candidate coinbases.
        wallet.start(checkpoint["transparent_miner"])

        def candidate(filling, target, indices):
            name = f"{filling}-{target}"
            relative = "blocks/" + name + ".json"
            path = args.output / relative
            seal_path = path.with_suffix(".seal.json")
            chosen = [corpus[i] for i in indices]
            stats = {
                kind: {
                    "transactions": sum(t["kind"] == kind for t in chosen),
                    "bytes": sum(t["bytes"] for t in chosen if t["kind"] == kind),
                }
                for kind in ("transparent", "orchard")
            }
            if args.transparent_signed:
                stats["transparent-signed"] = dict(
                    transactions=len(chosen), bytes=sum(t["bytes"] for t in chosen),
                    inputs=sum(t["transparent_inputs"] for t in chosen),
                    inputs_per_transaction=1,
                )
            if seal_path.exists():
                seal = json.loads(seal_path.read_text())
                require(
                    seal
                    == {
                        "sha256": file_hash(path),
                        "checkpoint_sha256": checkpoint_hash,
                    },
                    "sealed candidate/checkpoint identity: " + name,
                )
                record = json.loads(path.read_text())
            else:
                if path.exists():
                    # A crash between atomic candidate and seal writes cannot replace evidence.
                    path.replace(
                        path.with_name(path.name + ".unsealed-" + secrets.token_hex(8))
                    )
                attempt = args.output / "producers" / name / secrets.token_hex(8)
                with Node(args.producer, attempt, timeout=args.timeout) as node:
                    node.replay(chain)
                    admit(node, chosen)
                    template, _ = exact_template(
                        node, chosen, chain[-1]["hash"], args.timeout
                    )
                    require(
                        sum(t["fee"] for t in template["transactions"])
                        == sum(t["fee_zat"] for t in chosen),
                        "ZIP-317 selection includes every conventional-fee transaction",
                    )
                    block_hash = node.rpc.call("generate", [1])[0]
                    raw = node.rpc.call("getblock", [block_hash, 0])
                    txids = node.rpc.call("getblock", [block_hash, 1])["tx"]
                    require(
                        len(txids) == len(chosen) + 1
                        and set(txids[1:]) == {t["txid"] for t in chosen},
                        "generated candidate contains exactly the requested filling",
                    )
                    coinbase = node.rpc.call("getrawtransaction", [txids[0], 0])
                    decoded = wallet.rpc("decoderawtransaction", [coinbase])
                    no_other_pools(decoded)
                    require(
                        decoded["vout"]
                        and not decoded.get("orchard", {}).get("actions"),
                        "candidate coinbase is transparent",
                    )
                record = dict(
                    id=name,
                    filling=filling,
                    target_bytes=target,
                    raw=raw,
                    corpus_indices=indices,
                    txids=[t["txid"] for t in chosen],
                    transaction_count=len(chosen) + 1,
                    height=len(chain) + 1,
                    filling_stats=stats,
                    construction={"sources": current_inputs["sources"], "node_policy": NODE_POLICY},
                    **block_identity(raw),
                )
                require(record["hash"] == block_hash, "actual candidate hash")
            identity = block_identity(record["raw"])
            require(
                all(record[key] == value for key, value in identity.items())
                and record["parent"] == chain[-1]["hash"]
                and record["bytes"] <= LIMIT
                and record["id"] == name
                and record["filling"] == filling
                and record["target_bytes"] == target
                and record["corpus_indices"] == indices
                and record["txids"] == [t["txid"] for t in chosen]
                and record["transaction_count"] == len(chosen) + 1
                and record["filling_stats"] == stats,
                "candidate matches sealed funding inputs",
            )
            if chosen:
                require(
                    abs(record["bytes"] - target)
                    <= max(t["bytes"] for t in chosen) + 32,
                    "target missed by more than one transaction",
                )
            if filling == "mixed":
                require(
                    abs(stats["transparent"]["bytes"] - stats["orchard"]["bytes"])
                    <= 2 * checkpoint["probe_bytes"],
                    "mixed filling is approximately half bytes",
                )
            if not seal_path.exists():
                save(path, record)
                save(
                    seal_path,
                    {"sha256": file_hash(path), "checkpoint_sha256": checkpoint_hash},
                )
            files.extend((relative, str(seal_path.relative_to(args.output))))
            fixtures.append(
                {
                    key: record[key]
                    for key in (
                        "id",
                        "filling",
                        "target_bytes",
                        "bytes",
                        "transaction_count",
                    )
                }
                | {"file": relative, "construction": record.get("construction", {
                    "sources": checkpoint["inputs"]["sources"],
                    "node_policy": checkpoint["inputs"].get("node_policy", {"mempool_tx_cost_limit": 80_000_000}),
                    "scope": "original sealed candidate, reused byte-for-byte"})}
            )
            return record["bytes"]

        empty_bytes = candidate("empty", args.sizes[0], [])
        for target in args.sizes[1:]:
            for filling in (("transparent-signed",) if args.transparent_signed
                            else ("transparent", "orchard", "mixed")):
                candidate(filling, target, select(corpus, filling, target, empty_bytes))
        manifest = dict(
            schema=1,
            complete=True,
            fixtures=fixtures,
            files={name: file_hash(args.output / name) for name in files},
            sources=current_inputs["sources"],
            funding_sources=checkpoint["inputs"]["sources"],
            node_policy=NODE_POLICY,
            resume_input_changes=input_changes,
            producer=checkpoint["inputs"]["producer"],
            wallet_image=WALLET_IMAGE,
            wallet_network_info=checkpoint["wallet_network_info"],
            wallet_commands=checkpoint["wallet_commands"] + wallet.commands,
            activation="NU5 at height 1; subsequent upgrades inactive",
            miner=h.P2SH_MINER,
            selection="fixed input order; nearest transaction prefix by serialized bytes; conventional fees",
            reproducibility="Deterministic filling recipe; real wallet keys/proofs/timestamps are random. All arms replay the same sealed bytes.",
            scope="Regtest real proofs; no production Equihash timing. One actual coinbase-only empty point.",
            resume="Funding/corpus and all existing candidate seals remain unchanged; source/config changes explicitly authorized by --resume-sealed-corpus are recorded per construction.",
        )
        save(args.output / "manifest.json", manifest)
        seal = args.output / "manifest.sha256"
        temporary = seal.with_suffix(".sha256.tmp")
        temporary.write_text(file_hash(args.output / "manifest.json") + "\n")
        temporary.replace(seal)
    finally:
        wallet.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__, epilog=h.BENCH_HELP)
    parser.add_argument(
        "--producer",
        type=Path,
        required=True,
        help="NU5-capable Zebra binary with RPC generate",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sizes", type=int, nargs="+")
    parser.add_argument("--transparent-signed", action="store_true",
                        help="build only signed P2PKH fillings plus empty, in a NEW fixture directory; defaults to 2000 250000 1900000")
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--resume-sealed-corpus", action="store_true",
                        help="reuse sealed funding/blocks after source or producer-policy changes; immutable producer/image/size inputs must match")
    args = parser.parse_args()
    args.sizes = args.sizes or ([2_000, 250_000, 1_900_000] if args.transparent_signed else list(SIZES))
    if args.transparent_signed:
        import transparent_signed
        # Fail before creating a wallet if this Python/OpenSSL lacks deterministic secp256k1.
        transparent_signed.spend(("00" * 32, 0, 1_000_000))
    args.producer, args.output = args.producer.resolve(), args.output.resolve()
    require(
        args.producer.is_file() and os.access(args.producer, os.X_OK),
        "executable producer",
    )
    require(
        len(args.sizes) >= 2
        and args.sizes[0] == 2_000
        and args.sizes == sorted(set(args.sizes))
        and 25_000 <= args.sizes[1]
        and args.sizes[-1] <= 1_900_000
        and args.timeout > 0,
        "size ladder starts with 2000 then unique increasing 25000..1900000 targets",
    )
    os.umask(0o077)
    if (args.output / "manifest.sha256").exists():
        manifest, _, fixtures = load_fixtures(args.output)
        require(
            manifest["producer"]["sha256"] == file_hash(args.producer)
            and sorted({f["target_bytes"] for f in fixtures}) == args.sizes
            and any(f["filling"] == "transparent-signed" for f in fixtures) == args.transparent_signed,
            "completed fixture producer, size ladder or filling differs",
        )
        print("Already complete and sealed:", args.output)
        return
    if args.output.exists() and any(args.output.iterdir()):
        require(
            (args.output / "checkpoint.json").is_file()
            and (args.output / "checkpoint.seal.json").is_file(),
            f"Cannot resume {args.output}: no sealed funding/corpus checkpoint. "
            "Pre-checkpoint wallet work is preserved but cannot safely be reused; "
            "choose a new --output directory (for example, append -rebuild-1).",
        )
    args.output.mkdir(parents=True, exist_ok=True)
    try:
        build(args)
    except BaseException:
        save(
            args.output / f"failure-{time.time_ns()}.json",
            {"error": traceback.format_exc()},
        )
        raise


if __name__ == "__main__":
    main()
