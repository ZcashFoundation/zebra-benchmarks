"""Sapling coinbase generation with a fresh signed fee-paying batch per block."""
import statistics
import time

from common import admit, block_identity, exact_mempool, h, require
from protocol import fixture as protocol_fixture
from transparent_signed import SCRIPT, spend

ENDPOINT = "generate-sapling-fee"


def refill(corpus):
    """Spend each just-mined P2PKH output; never re-admit already spent fixture inputs."""
    batch = []
    for tx in corpus:
        parsed, end = protocol_fixture.parse_v4(bytes.fromhex(tx["raw"]))
        require(end == tx["bytes"] and len(parsed["outputs"]) == 1,
                "exact one-output signed refill transaction")
        output = parsed["outputs"][0]
        require(output["script"] == SCRIPT.hex(), "refill output belongs to public fixture key")
        batch.append(spend((tx["txid"], 0, output["value"])))
    return batch


def run(node, fixture, chain, args):
    corpus = fixture["corpus"]
    require(corpus and all(tx["kind"] == "transparent-signed" and tx["fee_zat"] > 0
                           for tx in corpus), "generate-sapling-fee requires signed fee-paying fixtures")
    require(fixture["target_bytes"] in (250_000, 1_900_000), "250 kB or 1.9 MB signed fixture")
    # Unlike Node.replay, this never requests a standalone template. generate randomizes its
    # coinbase parameters and clears its clone's caches, bypassing the background updater.
    h.replay(node, chain)
    parent = fixture["parent"]
    require(node.rpc.call("getrawmempool") == [], "fee generation starts with an empty mempool")
    samples = []
    for index in range(args.generate_blocks):
        admit(node, corpus)
        polls = exact_mempool(node, corpus, parent, args.timeout, args.poll_ms / 1000)
        started = time.perf_counter_ns()
        hashes = node.rpc.call("generate", [1])
        ended = time.perf_counter_ns()
        require(len(hashes) == 1, "one generated block per fee-paying call")
        block = node.rpc.call("getblock", [hashes[0], 1])
        require(len(block["tx"]) == len(corpus) + 1
                and set(block["tx"][1:]) == {tx["txid"] for tx in corpus},
                "every generated block contains exactly the fee-paying batch")
        coinbase = node.rpc.call("getrawtransaction", [block["tx"][0], 1])
        require(coinbase.get("vShieldedOutput") and not coinbase.get("vShieldedSpend")
                and not coinbase.get("vout") and not coinbase.get("vjoinsplit")
                and not coinbase.get("orchard", {}).get("actions"), "coinbase pays only Sapling")
        identity = block_identity(node.rpc.call("getblock", [hashes[0], 0]))
        require(identity["hash"] == hashes[0] and identity["parent"] == parent,
                "fee-paying generated block extends the measured parent")
        require(node.rpc.call("getbestblockhash") == hashes[0], "generated block is canonical")
        require(node.rpc.call("getrawmempool") == [], "generated fee batch fully drained")
        samples.append(dict(start_ns=started, end_ns=ended, latency_ns=ended - started,
                            hash=hashes[0], parent=parent, sha256=identity["sha256"],
                            block_bytes=identity["bytes"], mempool_polls=polls,
                            transaction_count=len(corpus) + 1,
                            admitted_bytes=sum(tx["bytes"] for tx in corpus),
                            fees_zat=sum(tx["fee_zat"] for tx in corpus),
                            preload=node.preload_receipt(started)))
        parent = hashes[0]
        if index + 1 < args.generate_blocks:
            corpus = refill(corpus)
    return dict(latency_ns=statistics.median(sample["latency_ns"] for sample in samples),
                generate_samples=samples, generate_blocks=args.generate_blocks,
                statistic="median of individually timed generate-1 calls",
                miner_address=h.SAPLING_MINER, standalone_getblocktemplate_calls=0,
                refill="signed spends of the preceding block's fixture-key outputs; excluded from timer")
