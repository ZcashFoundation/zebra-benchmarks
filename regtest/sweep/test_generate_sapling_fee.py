"""Fee-generation timing, signed refills and rejection checks, without nodes or proofs."""
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import common
import generate_sapling_fee as fee
import run_sweep
import transparent_signed as signed


def transaction(source):
    raw = signed.serialize(source, [source[2] - common.h.FEE], b"test-signature")
    return dict(txid=common.h.dsha(raw)[::-1].hex(), raw=raw.hex(), bytes=len(raw),
                kind="transparent-signed", fee_zat=common.h.FEE)


class FakeNode:
    def __init__(self, *, missing=False, sapling=True):
        self.rpc = self
        self.parent = "parent"
        self.pool = {}
        self.blocks = {}
        self.calls = []
        self.missing = missing
        self.sapling = sapling

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def preload_receipt(self, started):
        return dict(started=started)

    def call(self, method, params=None):
        self.calls.append((method, params))
        if method == "getrawmempool":
            return list(self.pool)
        if method == "getbestblockhash":
            return self.parent
        if method == "sendrawtransaction":
            txid = common.h.dsha(bytes.fromhex(params[0]))[::-1].hex()
            self.pool[txid] = params[0]
            return txid
        if method == "generate":
            assert params == [1] and self.pool
            txids = list(self.pool)
            block_hash = f"{len(self.blocks) + 1:064x}"
            self.blocks[block_hash] = dict(hash=block_hash, parent=self.parent,
                                           tx=["coinbase", *([] if self.missing else txids)])
            self.parent = block_hash
            self.pool.clear()
            return [block_hash]
        if method == "getblock":
            return self.blocks[params[0]] if params[1] == 1 else params[0]
        if method == "getrawtransaction":
            return dict(vShieldedOutput=[{}] if self.sapling else [], vout=[])
        raise AssertionError("Unexpected RPC: " + method)


class GenerateFeeTests(unittest.TestCase):
    def fixture(self):
        return dict(corpus=[transaction(("01" * 32, 0, 1_000_000))],
                    target_bytes=250_000, parent="parent")

    def execute(self, node, fixture=None):
        args = SimpleNamespace(timeout=10, poll_ms=1, generate_blocks=3)
        with patch.object(fee.h, "replay"), patch.object(fee, "spend", side_effect=transaction) as signing, \
                patch.object(fee, "block_identity", side_effect=lambda raw: dict(
                    hash=raw, parent=node.blocks[raw]["parent"], bytes=250_000, sha256="sealed")), \
                patch.object(run_sweep, "Node", return_value=node) as factory:
            row = run_sweep.case("ours", "/binary", Path("/unused"), fixture or self.fixture(),
                                 [dict(hash="parent")], args, fee.ENDPOINT)
        factory.assert_called_once_with("/binary", Path("/unused/node"), "ours", 10,
                                        miner=common.h.SAPLING_MINER)
        return row, signing

    def test_each_timed_call_has_a_new_fee_paying_batch(self):
        node = FakeNode()
        row, signing = self.execute(node)
        self.assertEqual(signing.call_count, 2)
        sends = [params[0] for method, params in node.calls if method == "sendrawtransaction"]
        self.assertEqual(len(set(sends)), 3)
        first = self.fixture()["corpus"][0]
        signing.assert_any_call((first["txid"], 0, 1_000_000 - common.h.FEE))
        self.assertNotIn("getblocktemplate", [method for method, _ in node.calls])
        self.assertEqual(row["standalone_getblocktemplate_calls"], 0)
        self.assertEqual([s["fees_zat"] for s in row["generate_samples"]], [common.h.FEE] * 3)
        self.assertEqual([s["transaction_count"] for s in row["generate_samples"]], [2] * 3)
        common.validate_timing(dict(row, endpoint=fee.ENDPOINT))
        with self.assertRaises(RuntimeError):
            common.validate_timing(dict(row, endpoint=fee.ENDPOINT, generate_blocks=4))

    def test_rejects_non_fee_blocks_wrong_payout_and_unsigned_fixtures(self):
        for node, fixture, message in (
                (FakeNode(missing=True), None, "exactly the fee-paying batch"),
                (FakeNode(sapling=False), None, "only Sapling"),
                (FakeNode(), dict(self.fixture(), corpus=[dict(self.fixture()["corpus"][0], kind="transparent")]),
                 "signed fee-paying fixtures")):
            with self.subTest(message=message), self.assertRaisesRegex(RuntimeError, message):
                self.execute(node, fixture)

    def test_refill_rejects_outputs_not_owned_by_fixture_key(self):
        tx = self.fixture()["corpus"][0]
        with patch.object(fee, "SCRIPT", b"not-the-output-script"), self.assertRaisesRegex(RuntimeError, "fixture key"):
            fee.refill([tx])


if __name__ == "__main__":
    unittest.main()
