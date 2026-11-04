#!/usr/bin/env python3
"""Shared immutable-input and Regtest helpers; no node starts at import time."""
import hashlib
import json
import os
from pathlib import Path
import statistics
import sys
import time

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT.parent))
import node_bench as h

ARMS = ("zebra-main", "zebra-optimized", "zakura-main")
ENDPOINTS = ("admission-to-template", "prepared-submitblock", "cold-submitblock", "submit-to-peer-tip")
# Direct-dispatch probes only: preserve the existing sweep/report matrix.
PROBE_ENDPOINTS = ("cold-submitblock-nominer", "warm-mempool-submitblock")
GENERATE_ENDPOINT = "generate-1-orchard-no-demand"
GENERATE_BLOCKS = 20
MAIN_REVISIONS = {
    "zebra-main": os.environ.get("ZEBRA_MAIN_REVISION", "e8db87eb69254e0d4eedd058eadd48bdfa09065a"),
    "zakura-main": os.environ.get("ZAKURA_MAIN_REVISION", "f5c20398c629c8179364b07f059da3c814af78ec"),
}
PARSER = ROOT.parent / "protocol" / "fixture.py"
WALLET_IMAGE = "docker.io/electriccoinco/zcashd@sha256:558f41b7b8b15e1b408247914ba9ed7b9d3c62f1061823bb811b8b24a693f349"
LIMIT = 2_000_000
MEMPOOL_TX_COST_LIMIT = 400_000_000
TEMPLATE_WAIT_SECONDS = 60
NODE_POLICY = {"mempool_tx_cost_limit": MEMPOOL_TX_COST_LIMIT,
               "exact_template_timeout_seconds": TEMPLATE_WAIT_SECONDS}
require = h.require


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def file_hash(path):
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def save(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w") as stream:
        stream.write(json.dumps(value, indent=2, sort_keys=True) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def sources():
    paths = [Path(h.__file__), Path(h.node_process.__file__), *sorted(ROOT.glob("*.py"))]
    result = {str(p.relative_to(ROOT.parent)): file_hash(p) for p in paths}
    result[str(PARSER.relative_to(ROOT.parent))] = file_hash(PARSER)
    return result


def block_identity(raw_hex):
    raw = bytes.fromhex(raw_hex)
    return dict(hash=h.dsha(raw[:h.header_len(raw_hex)])[::-1].hex(),
                parent=raw[4:36][::-1].hex(), bytes=len(raw), sha256=sha256(raw))


def no_other_pools(tx):
    # Check the decoded transaction, not the spelling of a wallet address.
    require(not tx.get("vShieldedSpend") and not tx.get("vShieldedOutput")
            and not tx.get("vjoinsplit"), "forbidden shielded bundle in fixture")


def transaction(wallet, txid, kind):
    tx = wallet("getrawtransaction", [txid, 1])
    no_other_pools(tx)
    actions = tx.get("orchard", {}).get("actions", [])
    if kind == "orchard":
        require(tx["version"] == 5 and actions and not tx["vin"] and not tx["vout"],
                "Orchard-only V5 transaction required")
    return dict(txid=txid, raw=tx["hex"], kind=kind, bytes=len(tx["hex"]) // 2,
                sha256=sha256(bytes.fromhex(tx["hex"])), orchard_actions=len(actions))


def validate_timing(row):
    require(row["latency_ns"] > 0, "strictly positive observation")
    if row["endpoint"] == GENERATE_ENDPOINT:
        samples = row["generate_samples"]
        require(len(samples) == GENERATE_BLOCKS, "exactly 20 measured generate calls per process")
        require(all(sample["latency_ns"] > 0
                    and sample["end_ns"] - sample["start_ns"] == sample["latency_ns"]
                    and sample["block_bytes"] > 0 for sample in samples),
                "actual per-call generation timing and serialized size")
        require(row["latency_ns"] == statistics.median(sample["latency_ns"] for sample in samples),
                "generation observation is the process median, not a synthetic dispatch interval")
    else:
        require(row["end_ns"] - row["start_ns"] == row["latency_ns"], "monotonic endpoint timing")


class Node(h.Node):
    def __init__(self, binary, directory, arm="zebra-main", timeout=600, miner=h.P2SH_MINER):
        environment = {k: v for k, v in h.env().items() if not k.startswith("NODE_")}
        environment.update(TOKIO_WORKER_THREADS="4", RAYON_NUM_THREADS="4")
        super().__init__(str(binary), directory, environment, miner=miner)
        # Select by arm, never by an arbitrarily named executable/symlink.
        text = h.config_for("zakurad" if arm == "zakura-main" else "zebrad",
                            json.dumps(str(self.dir / "state")), miner)
        text = text.replace("Canopy = 1\n", "Canopy = 1\nNU5 = 1\n")
        text = text.replace("[mempool]\n", f"[mempool]\ntx_cost_limit = {MEMPOOL_TX_COST_LIMIT}\n")
        if miner is None:
            # An unset address disables the stack's automatic template/proposal preparation.
            entry = 'miner_address = "None"\n'
            require(text.count(entry) == 1, "one rendered None miner address to omit")
            text = text.replace(entry, "", 1)
        self.config.write_text(text)
        self.timeout = timeout

    def __enter__(self):
        try:
            self.start()
            self.rpc.conn.timeout = self.timeout
            if self.rpc.conn.sock is not None:
                self.rpc.conn.sock.settimeout(self.timeout)
            return self
        except BaseException:
            self.stop()
            raise

    def __exit__(self, *_):
        try:
            self.rpc.conn.close()
        finally:
            self.stop()

    def replay(self, chain):
        h.replay(self, chain)
        require(self.rpc.call("getrawmempool") == [], "fresh replay mempool must be empty")
        self.wait(lambda: self.rpc.call("getblocktemplate")["previousblockhash"] == chain[-1]["hash"],
                  "active funding tip", self.timeout)


def template_diagnostic(node, expected, observed, corpus, calls, reason, mempool=None):
    mempool = set(node.rpc.call("getrawmempool")) if mempool is None else mempool
    evidence = []
    if node.log_path.exists():
        with node.log_path.open("rb") as stream:
            stream.seek(max(0, node.log_path.stat().st_size - 131072))
            evidence = [line for line in stream.read().decode(errors="replace").splitlines()
                        if any(word in line.lower() for word in ("evict", "queue was full", "rejected"))][-8:]
    return json.dumps(dict(
        reason=reason, template_calls=calls, chosen_count=len(expected),
        template_count=len(observed), mempool_count=len(mempool),
        missing_accepted_count=len(set(expected) - mempool),
        missing_accepted_txids=sorted(set(expected) - mempool)[:16],
        chosen_zip401_cost=sum(max(10_000, tx["bytes"]) for tx in corpus),
        configured_tx_cost_limit=MEMPOOL_TX_COST_LIMIT,
        mempool_info=node.rpc.call("getmempoolinfo"),
        eviction_status="RPC exposes no eviction counter; missing previously accepted IDs were evicted or removed",
        log_evidence=evidence), sort_keys=True)


def exact_template(node, corpus, parent, timeout, poll_seconds=0.001):
    expected = {t["txid"]: t["raw"].lower() for t in corpus}
    deadline = time.monotonic() + min(timeout, TEMPLATE_WAIT_SECONDS)
    calls, observed = 0, {}
    while time.monotonic() < deadline:
        template = node.rpc.call("getblocktemplate")
        calls += 1
        observed = {t["hash"]: t["data"].lower() for t in template["transactions"]}
        require(template["previousblockhash"] == parent, "template changed parent")
        require(set(observed) <= set(expected), "unrelated template transaction")
        if len(template["transactions"]) == len(expected) and observed == expected:
            require(template["sizelimit"] == LIMIT, "expected 2 MB consensus limit")
            return template, calls
        mempool = set(node.rpc.call("getrawmempool"))
        if set(expected) - mempool:
            raise RuntimeError(template_diagnostic(node, expected, observed, corpus, calls,
                               "accepted transactions disappeared before exact template selection", mempool))
        time.sleep(poll_seconds)
    raise TimeoutError(template_diagnostic(node, expected, observed, corpus, calls,
                                          "exact template deadline exceeded with no silent re-admission"))


def exact_mempool(node, corpus, parent, timeout, poll_seconds=0.001):
    """Wait for verified transaction residency without requesting templates or proposals."""
    expected = {tx["txid"] for tx in corpus}
    deadline = time.monotonic() + timeout
    polls = 0
    while time.monotonic() < deadline:
        observed = set(node.rpc.call("getrawmempool"))
        polls += 1
        require(node.rpc.call("getbestblockhash") == parent, "mempool changed parent")
        require(observed <= expected, "unrelated mempool transaction")
        if observed == expected:
            return polls
        time.sleep(poll_seconds)
    raise TimeoutError("exact mempool deadline exceeded")


def admit(node, corpus):
    cost = sum(max(10_000, tx["bytes"]) for tx in corpus)
    require(cost <= MEMPOOL_TX_COST_LIMIT,
            f"chosen ZIP-401 cost {cost} exceeds configured limit {MEMPOOL_TX_COST_LIMIT}")
    for tx in corpus:
        require(node.rpc.call("sendrawtransaction", [tx["raw"]]) == tx["txid"],
                "admission returned a different txid")


def load_fixtures(directory):
    directory = Path(directory)
    manifest_path = directory / "manifest.json"
    require(file_hash(manifest_path) == (directory / "manifest.sha256").read_text().strip(),
            "fixture manifest seal")
    manifest = json.loads(manifest_path.read_text())
    require(manifest["schema"] == 1 and manifest["complete"], "complete fixture schema")
    for relative, digest in manifest["files"].items():
        path = (directory / relative).resolve()
        require(path.is_relative_to(directory.resolve()) and file_hash(path) == digest,
                "fixture file seal: " + relative)
    chain = json.loads((directory / "chain.json").read_text())
    corpus = json.loads((directory / "corpus.json").read_text())
    require(chain and len({t["txid"] for t in corpus}) == len(corpus), "unique corpus and funding prefix")
    for tx in corpus:
        require(tx["kind"] in ("transparent", "transparent-signed", "orchard") and len(tx["raw"]) == 2 * tx["bytes"]
                and sha256(bytes.fromhex(tx["raw"])) == tx["sha256"], "sealed corpus identity")
    fixtures = []
    for entry in manifest["fixtures"]:
        fixture = json.loads((directory / entry["file"]).read_text())
        identity = block_identity(fixture["raw"])
        require(all(identity[k] == fixture[k] for k in identity), "candidate identity")
        require(fixture["parent"] == chain[-1]["hash"] and fixture["bytes"] <= LIMIT,
                "candidate funding parent and consensus size")
        selected = [corpus[i] for i in fixture["corpus_indices"]]
        require(fixture["txids"] == [t["txid"] for t in selected]
                and fixture["transaction_count"] == len(selected) + 1, "candidate corpus membership")
        fixture["corpus"] = selected
        fixtures.append(fixture)
    return manifest, chain, fixtures
