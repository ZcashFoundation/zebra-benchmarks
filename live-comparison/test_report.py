"""Offline tests: published Mainnet bytes plus explicitly synthetic wire layouts."""
import hashlib
import json
from pathlib import Path
import struct
import tempfile
import unittest

from latency_stats import half_sample_mode, paired
import report
from workload import COMPONENTS, GROUP_IDS, ParseError, THRESHOLDS, labels, parse_block

FIXTURES = Path(__file__).with_name("fixtures")


def compact(value):
    if value < 253:
        return bytes([value])
    return b"\xfd" + struct.pack("<H", value)


def orchard_bytes(count, flags=3):
    if not count:
        return b"\0"
    return compact(count) + bytes(820 * count) + bytes([flags]) + bytes(40) + b"\x03abc" + bytes(64 * count + 64)


def tx(version=1, coinbase=False, shielded=False):
    # Structurally complete, deliberately not cryptographically valid.
    result = struct.pack("<I", version | (0x80000000 if version >= 3 else 0))
    if version >= 3:
        result += struct.pack("<I", GROUP_IDS[version])
    if version >= 5:
        result += struct.pack("<III", 0x37A5165B if version == 6 else 0xC2D6D0B4, 0, 0)
    prevout = bytes(32) + b"\xff" * 4 if coinbase else b"\x01" * 32 + bytes(4)
    result += b"\x01" + prevout + b"\0" + b"\xff" * 4 + b"\x01" + bytes(8) + b"\0"
    if version <= 4:
        result += bytes(4 + (4 if version >= 3 else 0))
        if version == 4:
            result += bytes(8) + (b"\x01" + bytes(384) + b"\x01" + bytes(948) if shielded else bytes(2))
        if version >= 2:
            result += b"\x01" + bytes((1698 if version == 4 else 1802) + 96) if shielded else b"\0"
        if version == 4 and shielded:
            result += bytes(64)
    else:
        result += b"\x01" + bytes(96) + b"\x01" + bytes(756) + bytes(8 + 32 + 256 + 192 + 64) if shielded else bytes(2)
        result += orchard_bytes(1 if shielded else 0)
        if version == 6:
            result += orchard_bytes(2 if shielded else 0, flags=7)
    return result


def block_bytes(number=0, transactions=None):
    header = struct.pack("<I", 4) + bytes(132) + struct.pack("<I", number) + b"\0"
    transactions = [tx(coinbase=True)] if transactions is None else transactions
    return header + compact(len(transactions)) + b"".join(transactions)


def block_hash(raw, header_size=141):
    return hashlib.sha256(hashlib.sha256(raw[:header_size]).digest()).digest()[::-1].hex()


def attempt(number=0, target="a", at=1000, latency=10, **changes):
    raw = block_bytes(number)
    row = {"event": "submitblock_call", "hash": block_hash(raw), "height": number,
           "block_bytes": len(raw), "raw_block_hex": raw.hex(), "target": target,
           "client": "Zebra", "version": "test", "dispatch_unix_ms": at,
           "queued_unix_ms": at, "observed_at_unix_ms": at + 100,
           "latency_ms": latency, "outcome": "accepted", "detail": None, "catch_up": False}
    row.update(changes)
    return row


def session(targets=("a", "b"), at=0):
    return {"event": "session", "at_unix_ms": at, "mode": "live_fed", "synthetic": True,
            "clients": {t: {"name": "Zebra", "version": "test"} for t in targets}}


class WorkloadTests(unittest.TestCase):
    def test_real_mainnet_fixtures(self):
        sprout_raw = bytes.fromhex((FIXTURES / "block-main-0-000-396.txt").read_text().strip())
        sprout = parse_block(sprout_raw.hex())
        self.assertEqual(sprout["hash"], block_hash(sprout_raw, 1487))
        self.assertEqual(sprout["tx_count"], sprout_raw[1487])
        self.assertGreater(sprout["sprout_joinsplits"], 0)
        self.assertEqual(sprout["ironwood_actions"], 0)
        modern_raw = bytes.fromhex((FIXTURES / "block-main-1-687-107.txt").read_text().strip())
        modern = parse_block(modern_raw.hex())
        self.assertEqual(modern["hash"], block_hash(modern_raw, 1487))
        self.assertEqual(modern["tx_count"], modern_raw[1487])
        self.assertIn("5", modern["transaction_versions"])
        self.assertGreater(modern["sapling_outputs"], 0)
        self.assertGreater(modern["orchard_actions"], 0)
        self.assertEqual(modern["component_scope"], "non_coinbase")

    def test_all_six_transaction_layouts(self):
        for version in range(1, 7):
            with self.subTest(version=version):
                parsed = parse_block(block_bytes(transactions=[tx(coinbase=True), tx(version, shielded=True)]).hex())
                self.assertEqual(parsed["tx_count"], 2)
                self.assertEqual(parsed["transparent_inputs"], 1)
                self.assertEqual(parsed["transparent_outputs"], 1)
                self.assertEqual(parsed["sprout_joinsplits"], int(version in (2, 3, 4)))
                self.assertEqual(parsed["sapling_spends"], int(version >= 4))
                self.assertEqual(parsed["sapling_outputs"], int(version >= 4))
                self.assertEqual(parsed["orchard_actions"], int(version >= 5))
                self.assertEqual(parsed["ironwood_actions"], 2 if version == 6 else 0)
        coinbase_only = parse_block(block_bytes(transactions=[tx(6, coinbase=True, shielded=True)]).hex())
        self.assertTrue(all(coinbase_only[k] == 0 for k in COMPONENTS))
        self.assertIn("composition:empty", coinbase_only["labels"])

    def test_malformed_and_unsupported_are_explicit(self):
        raw = block_bytes(transactions=[tx(coinbase=True), tx(6, shielded=True)])
        bad = [b"", raw[:140], raw[:-1], raw + b"\0", raw[:141] + b"\xfd\x02\0" + raw[142:],
               block_bytes(transactions=[]), block_bytes(transactions=[tx()]),
               block_bytes(transactions=[tx(coinbase=True), tx(coinbase=True)]),
               block_bytes(transactions=[tx(coinbase=True), struct.pack("<I", 7)]),
               raw[:141] + b"\xff" + b"\xff" * 8]
        for data in bad:
            with self.subTest(length=len(data)), self.assertRaises(ParseError):
                parse_block(data.hex())
        for text in ("0", "gg", "00 00", None, "00" * 2_000_001):
            with self.assertRaises(ParseError):
                parse_block(text)
        wrong_group = tx(6, shielded=True)
        with self.assertRaisesRegex(ParseError, "version group"):
            parse_block(block_bytes(transactions=[tx(coinbase=True), wrong_group[:4] + bytes(4) + wrong_group[8:]]).hex())
        with self.assertRaisesRegex(ParseError, "consensus branch"):
            parse_block(block_bytes(transactions=[tx(coinbase=True), wrong_group[:8] + bytes(4) + wrong_group[12:]]).hex())

    def test_taxonomy_edges_are_frozen(self):
        base = dict.fromkeys(COMPONENTS, 0)
        base.update(tx_count=1, size_bytes=16384)
        self.assertIn("size:small", labels(base))
        for size, expected in ((16385, "medium"), (131072, "medium"), (131073, "large"), (1048577, "very_large")):
            self.assertIn("size:" + expected, labels(dict(base, size_bytes=size)))
        for count, expected in ((1, "coinbase_only"), (10, "few"), (11, "moderate"), (100, "moderate"), (101, "many")):
            self.assertIn("transactions:" + expected, labels(dict(base, tx_count=count)))
        self.assertIn("ironwood_actions:high", labels(dict(base, ironwood_actions=100)))
        self.assertTrue(THRESHOLDS["inferred"])


class ReportTests(unittest.TestCase):
    def test_pairwise_first_attempts_exclusions_and_common_subset(self):
        rows = [session(("a", "b", "c")), attempt(0, "a"), attempt(0, "b", latency=15),
                attempt(0, "c", outcome="rejected"),
                attempt(1, "a", catch_up=True), attempt(1, "b"),
                attempt(2, "a", outcome="timeout"), attempt(2, "b"),
                attempt(2, "a", at=2000),  # Acceptance cannot rescue first timeout.
                attempt(3, "a"), attempt(3, "b", at=1026),
                attempt(4, "a", outcome="duplicate"), attempt(4, "b"),
                attempt(5, "a"), attempt(5, "b", at=1025, latency=17)]
        rows.append(dict(rows[1]))  # Repeated log line must not create another pair.
        result = report.make_report(rows, 0, report.WINDOW_MS)
        pairs = {(p["left"], p["right"]): p for p in result["selection"]["pairs"]}
        self.assertEqual(len(pairs), 3)
        pair = pairs["a", "b"]
        self.assertEqual(pair["eligible_pairs"], 2)
        self.assertEqual(pair["paired_mean_right_minus_left_ms"], 6)
        self.assertEqual(pair["paired_median_right_minus_left_ms"], 6)
        self.assertEqual(pair["evidence"], "insufficient_pairs")
        self.assertEqual(pair["exclusions"], {"a:catch_up": 1, "a:timeout": 1, "a:duplicate": 1, "dispatch_skew": 1})
        self.assertEqual(result["selection"]["common_accepted_blocks"], 0)
        self.assertEqual(result["coverage"]["a"]["catch_up_attempts"], 1)
        self.assertEqual(result["coverage"]["c"]["missing_blocks"], 5)
        self.assertEqual(result["coverage"]["a"]["all_attempt_outcomes"]["accepted"], 6)

    def test_bootstrap_is_time_clustered_and_deterministic(self):
        pairs = [(cluster * report.WINDOW_MS + index, cluster)
                 for cluster in range(12) for index in range(2)]
        evidence, interval = report.interval(pairs)
        self.assertEqual(evidence, "exploratory")
        self.assertEqual(interval["clusters"], 12)
        self.assertEqual(report.interval(list(reversed(pairs))), (evidence, interval))
        self.assertLessEqual(interval["lower_ms"], 5.5)
        self.assertGreaterEqual(interval["upper_ms"], 5.5)
        self.assertEqual(report.interval(pairs[:19])[0], "insufficient_pairs")
        self.assertEqual(report.interval([(i, 7) for i in range(20)])[0], "insufficient_clusters")
        constant = [(i * report.WINDOW_MS, -3) for i in range(20)]
        self.assertEqual(report.interval(constant)[1], {"clusters": 20, "lower_ms": -3, "upper_ms": -3})
        skewed = [(cluster * report.WINDOW_MS + index, 100 if cluster == 0 else 1)
                  for cluster in range(12) for index in range(2)]
        self.assertGreater(report.interval(skewed)[1]["upper_ms"], 1)

    def test_feature_errors_do_not_invent_zero_work(self):
        rows = [session(), attempt(0, "a", raw_block_hex="garbage"), attempt(0, "b")]
        result = report.make_report(rows, 0, report.WINDOW_MS)
        feature = result["features"][0]
        self.assertEqual(feature["parse_status"], "error")
        self.assertTrue(all(feature[k] is None for k in COMPONENTS))
        self.assertEqual(result["selection"]["pairs"][0]["exclusions"], {"workload_error": 1})
        mismatch = [session(), attempt(0, "a", hash="f" * 64)]
        self.assertIn("hash does not match", report.make_report(mismatch, 0, report.WINDOW_MS)["features"][0]["detail"])

    def test_window_boundary_keeps_same_block_pair(self):
        rows = [session(), attempt(0, "a", at=299999),
                attempt(0, "b", at=300001, queued_unix_ms=299999)]
        first = report.make_report(rows, 0, 300000)
        self.assertEqual(first["selection"]["pairs"][0]["eligible_pairs"], 1)
        self.assertEqual(report.make_report(rows, 300000, 600000)["blocks_observed"], 0)

    def test_templates_resources_and_identity(self):
        rows = [session(), {"event": "getblocktemplate", "target": "a", "at_unix_ms": 300100,
                            "status": "rpc_error", "latency_ms": 3, "template_height": None,
                            "transaction_count": None, "detail": "not ready"},
                {"event": "resource_interval", "target": "a", "start_unix_ms": 299000,
                 "end_unix_ms": 301000, "rss_bytes": 100, "valid": True, "detail": None,
                 "deltas": {"cpu_seconds": 1, "read_bytes": 2, "write_bytes": 3}}]
        result = report.make_report(rows, 300000, 600000)
        sync = result["synchronization"]
        self.assertEqual(sync["counts"], {"agreement": 0, "disagreement": 0, "unknown": 1})
        self.assertFalse(sync["resource_intervals"][0]["valid"])
        self.assertEqual(sync["samples"][0]["getblocktemplate"]["a"]["status"], "rpc_error")
        changed = session(at=1000)
        changed["clients"]["a"]["version"] = "new"
        with self.assertRaisesRegex(ValueError, "incompatible client/version"):
            report.prepare([session(), changed])

    def test_invalid_resource_baseline_can_have_zero_duration(self):
        baseline = {"event": "resource_interval", "target": "a", "start_unix_ms": 1000,
                    "end_unix_ms": 1000, "rss_bytes": None, "valid": False,
                    "deltas": {}, "detail": "first sample has no baseline"}
        report.validate(baseline)
        report.validate(dict(baseline, end_unix_ms=999, detail="wall clock moved backwards"))
        with self.assertRaisesRegex(ValueError, "invalid interval"):
            report.validate(dict(baseline, valid=True))
        with self.assertRaisesRegex(ValueError, "invalid interval"):
            report.validate(dict(baseline, valid=True, end_unix_ms=999))

    def test_paired_mean_is_not_median_or_difference_of_medians(self):
        rows = [session()]
        for number, (left, right) in enumerate(((1, 2), (100, 3), (101, 102))):
            rows.extend((attempt(number, "a", latency=left), attempt(number, "b", latency=right)))
        pair = report.make_report(rows, 0, report.WINDOW_MS)["selection"]["pairs"][0]
        self.assertAlmostEqual(pair["paired_mean_right_minus_left_ms"], -95 / 3)
        self.assertEqual(pair["paired_median_right_minus_left_ms"], 1)
        self.assertEqual(pair["right_latency"]["median_ms"] - pair["left_latency"]["median_ms"], -97)
        self.assertAlmostEqual(pair["left_latency"]["mean_ms"], 202 / 3)
        self.assertEqual(report.latency([1, 1, 1, 50, 90])["mode_ms"], 1)
        self.assertEqual(half_sample_mode([1, 2, 2.1, 2.5, 9]), 2.05)
        self.assertEqual(paired([2, 2, 2], resamples=100),
                         {"mean": 2, "median": 2, "mean_ci95": [2, 2]})

    def test_submit_paths_and_mirror_relay_validation(self):
        first = attempt()
        path = {"event": "submit_paths", "hash": first["hash"], "height": 0,
                "block_bytes": first["block_bytes"], "target": "a",
                "client": "Zebra", "version": "test", "queued_unix_ms": 1000,
                "dispatch_unix_ms": 1000, "observed_at_unix_ms": 1100,
                "outcome": "accepted", "detail": None, "catch_up": False,
                **{key: None for key in report.MILESTONES}}
        relay = {"event": "mempool_relay", "txid": "a" * 64, "target": "a",
                 "observed_at_unix_ms": 1000, "outcome": "error",
                 "error": "source fetch failed", "latency_ms": None}
        report.validate(path)
        report.validate(relay)
        for bad in (dict(path, submit_return_ms=float("nan")),
                    dict(path, peer_has_ms=-1), dict(path, tip_ms="5"),
                    dict(path, hash="oops"), dict(path, catch_up=0),
                    dict(path, observed_at_unix_ms=999),
                    dict(relay, txid="bad"), dict(relay, latency_ms=-1),
                    dict(relay, dispatch_unix_ms=1001),
                    dict(relay, observed_at_unix_ms=True),
                    dict(relay, outcome="unexpected"),
                    {k: v for k, v in relay.items() if k != "error"},
                    {k: v for k, v in relay.items() if k != "latency_ms"}):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                report.validate(bad)
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "records.jsonl"
            source.write_text(json.dumps(path) + "\n" + json.dumps(dict(relay, txid="bad")) + "\n")
            with self.assertRaisesRegex(ValueError, r"records.jsonl:2:"):
                report.load_records(source)

    def test_submit_path_milestones_pair_by_block_and_mirror_acceptance(self):
        rows = [session()]
        for i in range(24):
            at = (i // 2) * report.WINDOW_MS + 1000
            for target, elapsed in (("a", 100 if i >= 22 else 5), ("b", 7)):
                submission = attempt(i, target, at=at)
                rows.append(submission)
                rows.append({"event": "submit_paths", "hash": submission["hash"],
                             "height": i, "block_bytes": submission["block_bytes"],
                             "target": target, "client": "Zebra", "version": "test",
                             "queued_unix_ms": at, "dispatch_unix_ms": at,
                             "observed_at_unix_ms": at + 100, "outcome": "accepted",
                             "detail": None, "catch_up": False,
                             "rpc_round_trip_ms": 10, "submit_return_ms": elapsed,
                             "tip_ms": None if target == "b" and i == 0 else elapsed + 1,
                             "template_ms": elapsed + 2 if target == "a" or i < 19 else None,
                             "peer_told_ms": None, "peer_has_ms": None})
        rows.extend(({"event": "mempool_relay", "txid": "a" * 64, "target": "a",
                      "dispatch_unix_ms": 1000, "observed_at_unix_ms": 1100,
                      "outcome": "accepted", "error": None, "latency_ms": 1},
                     {"event": "mempool_relay", "txid": "b" * 64, "target": "a",
                      "observed_at_unix_ms": 1200, "outcome": "error",
                      "error": "source fetch", "latency_ms": None},
                     {"event": "mempool_relay", "txid": "a" * 64, "target": "b",
                      "observed_at_unix_ms": 1200, "outcome": "rejected",
                      "error": "rejected", "latency_ms": 2}))
        result = report.make_report(rows, 0, 12 * report.WINDOW_MS)
        self.assertEqual(result["selection"]["pairs"][0]["eligible_pairs"], 24)
        milestone = result["submit_paths"]["milestones"]["submit_return_ms"]
        self.assertEqual(milestone["contestants"]["a"]["n"], 24)
        self.assertEqual(milestone["contestants"]["a"]["median_ms"], 5)
        self.assertEqual(milestone["contestants"]["a"]["mode_ms"], 5)
        self.assertAlmostEqual(milestone["contestants"]["a"]["mean_ms"], (22 * 5 + 2 * 100) / 24)
        pair = milestone["pairs"][0]
        self.assertEqual(pair["eligible_pairs"], 24)
        self.assertAlmostEqual(pair["paired_mean_right_minus_left_ms"], (22 * 2 - 2 * 93) / 24)
        self.assertEqual(pair["paired_median_right_minus_left_ms"], 2)
        self.assertEqual(pair["evidence"], "exploratory")
        self.assertIsNotNone(pair["interval"]["lower_ms"])
        self.assertIsNotNone(pair["interval"]["upper_ms"])
        self.assertEqual(result["submit_paths"]["milestones"]["tip_ms"]["pairs"][0]["eligible_pairs"], 23)
        short = result["submit_paths"]["milestones"]["template_ms"]["pairs"][0]
        self.assertEqual(short["eligible_pairs"], 19)
        self.assertEqual(short["evidence"], "insufficient_pairs")
        self.assertIsNone(short["interval"]["lower_ms"])
        self.assertEqual(result["submit_paths"]["milestones"]["peer_has_ms"]["contestants"]["a"]["n"], 0)
        self.assertEqual(result["mempool_relay"]["a"], {"attempts": 2, "accepted": 1, "acceptance_rate": .5})
        self.assertEqual(result["mempool_relay"]["b"], {"attempts": 1, "accepted": 0, "acceptance_rate": 0})
        without = report.make_report([row for row in rows if row["event"] not in ("submit_paths", "mempool_relay")],
                                     0, 12 * report.WINDOW_MS)
        self.assertEqual(result["selection"], without["selection"])
        self.assertNotEqual(result["report_id"], without["report_id"])
        mismatched = [dict(row, height=999) if row["event"] == "submit_paths"
                      and row["target"] == "b" and row["height"] == 0 else row for row in rows]
        different = report.make_report(mismatched, 0, 12 * report.WINDOW_MS)
        self.assertEqual(different["submit_paths"]["milestones"]["submit_return_ms"]["pairs"][0]["eligible_pairs"], 23)
        self.assertEqual(different["selection"], result["selection"])

    def test_site_and_rolling_are_relative_to_log(self):
        latest = report.DAY_MS + 1000
        rows = [session(), attempt(0, "a", at=100), attempt(0, "b", at=100),
                attempt(1, "a", at=latest), attempt(1, "b", at=latest, latency=20),
                {"event": "unrelated_diagnostic", "detail": "ignored"}]
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "records.jsonl"
            source.write_text("".join(json.dumps(r) + "\n" for r in rows))
            output = Path(directory) / "site"
            index = report.build_site(source, output)
            rolling = json.loads((output / "rolling.json").read_text())
            self.assertEqual(rolling["end_unix_ms"], latest + 101)
            self.assertEqual(rolling["start_unix_ms"], latest + 101 - report.DAY_MS)
            self.assertEqual(rolling["blocks_observed"], 1)
            self.assertEqual(rolling["selection"]["pairs"][0]["eligible_pairs"], 1)
            self.assertEqual(index["latest"], index["reports"][0]["report_id"])
            self.assertEqual(index["rolling"], "rolling.json")
            self.assertTrue((output / "index.html").is_file())
            html = (output / "index.html").read_text()
            self.assertIn("Mean right − left", html)
            self.assertIn("paired_median_right_minus_left_ms", html)
            self.assertIn("Half-sample mode", html)
            self.assertIn("Submit-path milestones since dispatch", html)
            self.assertIn("Mempool mirror RPC acceptance", html)
            for entry in index["reports"]:
                self.assertEqual(entry["start_unix_ms"] % report.WINDOW_MS, 0)
                self.assertTrue((output / "reports" / entry["report_id"] / "report.json").is_file())
            self.assertEqual(report.build_site(source, output), index)
            source.write_text('{"event":\n')
            with self.assertRaisesRegex(ValueError, r"records.jsonl:1:"):
                report.load_records(source)


if __name__ == "__main__":
    unittest.main()
