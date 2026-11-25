"""Offline assertions for the sealed live-Regtest smoke contract."""

from contextlib import redirect_stderr
from io import StringIO
from types import SimpleNamespace
import unittest

import smoke_regtest as smoke


class SmokeAssertions(unittest.TestCase):
    def test_config_is_regtest_isolated_with_mirroring_and_observers(self):
        feeder = SimpleNamespace(rpc_addr="127.0.0.1:10001")
        node = SimpleNamespace(rpc_addr="127.0.0.1:10002", p2p_addr="127.0.0.1:10003",
                               proc=SimpleNamespace(pid=42))
        config = smoke.config_for(feeder, [("reference", node, "zebra-main")],
                                  {"reference": ("zebra-main", "zebrad 7.0.0")})
        self.assertEqual(config["network"], "regtest")
        self.assertIs(config["synthetic"], True)
        self.assertIs(config["mirror_mempool"], True)
        self.assertIs(config["measure_submit_paths"], True)
        self.assertEqual(config["feeder"]["url"], "http://127.0.0.1:10001/")
        self.assertEqual(config["contestants"][0]["p2p"], node.p2p_addr)
        self.assertEqual(config["contestants"][0]["pid"], 42)

    def test_fixture_directory_is_required_before_starting_nodes(self):
        errors = StringIO()
        with redirect_stderr(errors), self.assertRaises(SystemExit) as exit:
            smoke.main(["--feeder", "/missing", "--contestant", "reference=/missing",
                        "--candidate", "mixed-1900000", "--output", "/missing"])
        self.assertEqual(exit.exception.code, 2)
        self.assertIn("--fixture-dir", errors.getvalue())

    def test_candidate_acceptance_and_all_five_milestones_are_required(self):
        fixture = {"hash": "ab", "height": 311, "raw": "0a"}
        submit = {"target": "reference", "hash": "ab", "height": 311,
                  "outcome": "accepted", "catch_up": False, "raw_block_hex": "0a"}
        paths = {"target": "reference", "hash": "ab", "height": 311,
                 "outcome": "accepted", "rpc_round_trip_ms": 1.0,
                 **{key: 1.0 for key in smoke.MILESTONES}}
        smoke.check_submission(submit, fixture, {"reference"})
        smoke.check_paths(paths, fixture, {"reference"})
        for key in smoke.MILESTONES:
            with self.subTest(key=key):
                with self.assertRaisesRegex(RuntimeError, "missing " + key):
                    smoke.check_paths(paths | {key: None}, fixture, {"reference"})
        with self.assertRaisesRegex(RuntimeError, "not accepted"):
            smoke.check_submission(submit | {"catch_up": True}, fixture, {"reference"})
        with self.assertRaisesRegex(RuntimeError, "not accepted"):
            smoke.check_submission(submit | {"outcome": "rejected"}, fixture, {"reference"})
        with self.assertRaisesRegex(RuntimeError, "path rejected"):
            smoke.check_paths(paths | {"outcome": "rejected"}, fixture, {"reference"})


if __name__ == "__main__":
    unittest.main()
