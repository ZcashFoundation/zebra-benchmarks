"""Pure benchmark accounting checks; no node or network required."""
import json
from pathlib import Path
import struct
import tempfile
import unittest
from unittest.mock import Mock

from shadow.observer import _announcements
from shadow.run import _ancestors, new_txids, row_for
from shadow.summarize import summarize


class ShadowAccounting(unittest.TestCase):
    def test_mempool_diff_forgets_departed_txids(self):
        seen = {"a", "b"}
        current = {"b", "c"}
        self.assertEqual(new_txids(seen, current), {"c"})
        seen.intersection_update(current)
        self.assertEqual(new_txids(seen, {"a", "b"}), {"a"})

    def test_ancestor_walk_stops_at_node_height(self):
        blocks = {
            "parent": dict(hash="parent", height=3, previousblockhash="middle"),
            "middle": dict(hash="middle", height=2, previousblockhash="tip"),
            "fork": dict(hash="fork", height=2, previousblockhash="genesis"),
        }
        rpc = Mock()
        rpc.call.side_effect = lambda method, args: blocks[args[0]]
        self.assertEqual([block["hash"] for block in _ancestors(rpc, "parent", "tip", 1)],
                         ["middle", "parent"])
        self.assertEqual(rpc.call.call_count, 2)
        rpc.call.reset_mock()
        with self.assertRaisesRegex(RuntimeError, "not an ancestor"):
            _ancestors(rpc, "fork", "tip", 2)
        self.assertEqual(rpc.call.call_count, 1)

    def test_row_uses_single_harness_clock(self):
        row = row_for(dict(round=2, hash="abcd", height=7, bytes=900, tx_count=4), "zebra",
                      dict(main_mempool_size=3, node_mempool_size=2, rpc_round_trip_ms=4),
                      1_000_000_000, 2_000_000_000,
                      dict(submit_return=1_012_000_000, tip=1_009_000_000,
                           template=None, peer_told=1_011_000_000, peer_has=1_013_000_000),
                      None, [])
        self.assertEqual(row["tip_ms"], 9)
        self.assertEqual(row["submit_return_ms"], 12)
        self.assertEqual(row["submit_wall_time_ns"], 2_000_000_000)
        self.assertIsNone(row["template_ms"])

    def test_summary_pairs_only_same_block_and_round(self):
        with tempfile.TemporaryDirectory() as temporary:
            rows = Path(temporary) / "rows.jsonl"
            entries = [dict(round=round_, block_hash=hash_, node=node, measured=True,
                            submit_return_ms=value, tip_ms=value, template_ms=value,
                            peer_told_ms=value, peer_has_ms=value)
                       for round_, hash_, ref, candidate in [(1, "a", 10, 12), (2, "b", 20, 22)]
                       for node, value in [("ref", ref), ("candidate", candidate)]]
            rows.write_text("\n".join(map(json.dumps, entries)) + "\n")
            (Path(temporary) / "mirror.jsonl").write_text(
                '{"node":"candidate","accepted":true}\n'
                '{"node":"candidate","accepted":false}\n')
            result = summarize(rows, reference="ref")
            self.assertEqual(result["nodes"]["candidate"]["tip_ms"]["mean"], 17)
            self.assertEqual(result["nodes"]["candidate"]["tip_ms"]["median"], 17)
            self.assertEqual(result["nodes"]["candidate"]["tip_ms"]["mode"], 17)
            self.assertEqual(result["paired"]["candidate"]["tip_ms"]["mean"], 2)
            self.assertEqual(result["paired"]["candidate"]["tip_ms"]["median"], 2)
            self.assertEqual(result["paired"]["candidate"]["tip_ms"]["mean_ci95"], [2, 2])
            self.assertEqual(result["mirror"]["candidate"]["acceptance_rate"], .5)

    def test_inventory_with_wtx_entries_yields_only_blocks(self):
        # Mirrored v5 transactions make peers gossip ZIP 239 MSG_WTX entries (txid + auth digest).
        block = bytes(range(32))
        payload = (bytes([3]) + struct.pack("<I", 5) + bytes(64) + struct.pack("<I", 2) + block
                   + struct.pack("<I", 1) + bytes(32))
        self.assertEqual(_announcements("inv", payload), [block[::-1].hex()])


if __name__ == "__main__":
    unittest.main()
