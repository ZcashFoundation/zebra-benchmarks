"""Synthetic network navigation and optional host-load shading checks."""
from contextlib import redirect_stderr, redirect_stdout
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import tomllib
import unittest
from unittest.mock import patch

import dashboard
import load_intervals
import test_dispatch


class TestnetDashboardTests(unittest.TestCase):
    BASE = test_dispatch.DashboardTests.BASE

    def records(self, count=1, synthetic=True):
        return test_dispatch.DashboardTests().records(count, synthetic)

    def write_rows(self, root, rows):
        path = root / "records.jsonl"
        path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
        return path

    def test_network_labels_navigation_defaults_and_json_override(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rows = self.records()
            rows.append({"event": "diagnostic", "detail": "/path/to/OMITTED_RUN"})
            path = self.write_rows(root, rows)
            for network, page, config in (
                    ("Mainnet", "index.html", dashboard.DEFAULT_CONFIG),
                    ("Testnet", "testnet.html", dashboard.DEFAULT_CONFIG.parent / "config" / "testnet" / "contestant.toml.example")):
                with self.subTest(network=network):
                    data = dashboard.build_site(path, root / network, network=network)
                    expected = tomllib.loads(config.read_text())
                    self.assertEqual(data["network"], network)
                    for node in expected["contestants"]:
                        self.assertEqual(data["clients"][node["name"]]["configured_version"], node["version"])
                    html = (root / network / "dashboard.html").read_text()
                    self.assertIn(f'href="{page}" aria-current="page"', html)
                    navigation = html.split('<nav class="network-tabs" aria-label="Network">')[1].split("</nav>")[0]
                    self.assertEqual(navigation.count("<a "), 2)
                    self.assertEqual(navigation.count('aria-current="page"'), 1)
                    self.assertNotIn("#glance", navigation)
                    for destination in ("index.html", "testnet.html"):
                        self.assertIn(f'href="{destination}"', html)
                    self.assertNotRegex(html, r'href="mainnet-[^"]*\.html"')
                    for label in dashboard.LABELS:
                        self.assertIn(label, html)
                    for omitted in ("OMITTED_RUN", "/path/to/", str(root), "raw_block_hex", "DO_NOT_PUBLISH"):
                        self.assertNotIn(omitted, html)
                    self.assertIn("perBlockCSV(data)", html)
            override = root / "contestant.json"
            settings = tomllib.loads(dashboard.DEFAULT_CONFIG.read_text())
            for node in settings["contestants"]:
                node["version"] = "synthetic captured candidate identity" if node["name"] == "zebra-candidate" else node["version"]
            override.write_text(json.dumps(settings))
            data = dashboard.build_site(path, root / "override", config=override, network="Testnet")
            self.assertEqual(data["clients"]["zebra-candidate"]["configured_version"], "synthetic captured candidate identity")
            self.assertEqual(data["clients"]["zebra-candidate"]["version"], rows[0]["clients"]["zebra-candidate"]["version"])

    def test_multiple_interval_files_merge_once_and_cli_repeats(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rows = self.records(5)
            offsets = (0, 29_999, 30_000, 150_000, 150_001)
            for row in rows[1:]:
                row["dispatch_unix_ms"] = self.BASE + offsets[row["height"]]
                row["observed_at_unix_ms"] = row["dispatch_unix_ms"] + 20
            path = self.write_rows(root, rows)
            files = [root / "main-load.json", root / "test-load.json"]
            for file, offset in zip(files, (60_000, 120_000)):
                file.write_text(json.dumps([[self.BASE + offset, self.BASE + offset]]))
            data = dashboard.build_site(path, root / "site", load_path=files)
            self.assertEqual([point[0] for point in data["glance"]["points"]], list(range(5)))
            self.assertEqual(data["load_intervals"], [[self.BASE + 30_000, self.BASE + 150_000]])
            self.assertEqual(data["mixed_load"]["All sizes"]["n"], 5)
            single = dashboard.build_site(path, root / "single", load_path=files[0])
            self.assertEqual(single["glance"]["points"], data["glance"]["points"])
            self.assertEqual(single["load_intervals"], [[self.BASE + 30_000, self.BASE + 90_000]])
            # Load after the oldest file was written is unknown, not quiet.
            for file, written in zip(files, (self.BASE + 200_000, self.BASE + 100_000)):
                os.utime(file, ns=(written * 1_000_000, written * 1_000_000))
            stale = dashboard.build_site(path, root / "stale", load_path=files)
            self.assertEqual([row["busy"] for row in stale["per_block"]], [False, False, True, None, None])
            argv = ["dashboard.py", str(path), str(root / "cli"), "--network", "Testnet",
                    "--config", str(dashboard.DEFAULT_CONFIG)]
            for file in files:
                argv.extend(("--load-intervals", str(file)))
            with patch.object(sys, "argv", argv), redirect_stdout(io.StringIO()) as output:
                self.assertEqual(dashboard.main(), 0)
            self.assertEqual(json.loads(output.getvalue())["blocks"], 5)
            html = (root / "cli/dashboard.html").read_text()
            self.assertIn('href="testnet.html" aria-current="page"', html)
            embedded = html.split('<script id="dashboard-data" type="application/json">')[1].split("</script>")[0]
            self.assertEqual(json.loads(embedded)["load_intervals"], data["load_intervals"])

    def test_optional_interval_input_and_atomic_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            exported = root / "load-intervals.json"
            argv = ["load_intervals.py", str(exported)]
            with patch.object(sys, "argv", argv), redirect_stdout(io.StringIO()):
                self.assertEqual(load_intervals.main(), 0)
            self.assertEqual(json.loads(exported.read_text()), [])
            source = root / "intervals.json"
            source.write_text(json.dumps([[4000, 6000], [1000, 2000], [2000, 5000], [8000, 8000]]))
            argv.extend(("--input", str(source)))
            with patch.object(sys, "argv", argv), redirect_stdout(io.StringIO()):
                self.assertEqual(load_intervals.main(), 0)
            self.assertEqual(json.loads(exported.read_text()), [[1000, 6000], [8000, 8000]])
            previous = exported.read_text()
            for content in ("{", '{"start": 0}', "[[2, 1]]", "[[true, 2]]", "[[-1, 0]]"):
                source.write_text(content)
                with self.subTest(content=content), patch.object(sys, "argv", argv), redirect_stderr(io.StringIO()):
                    self.assertEqual(load_intervals.main(), 1)
                self.assertEqual(exported.read_text(), previous)
            source.unlink()
            with patch.object(sys, "argv", argv), redirect_stderr(io.StringIO()):
                self.assertEqual(load_intervals.main(), 1)
            self.assertEqual(exported.read_text(), previous)

    def test_malformed_or_missing_load_files_preserve_page(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            current = self.write_rows(root, self.records())
            output = root / "site"
            dashboard.build_site(current, output)
            page = (output / "dashboard.html").read_text()
            valid, invalid = root / "valid.json", root / "invalid.json"
            valid.write_text("[]")
            for content in ("{", '{"start": 0}', "[[2, 1]]", "[[true, 2]]"):
                with self.subTest(content=content):
                    invalid.write_text(content)
                    with self.assertRaises(ValueError):
                        dashboard.build_site(current, output, load_path=[valid, invalid])
                    self.assertEqual((output / "dashboard.html").read_text(), page)
            with self.assertRaises(FileNotFoundError):
                dashboard.build_site(current, output, load_path=root / "missing.json")
            self.assertEqual((output / "dashboard.html").read_text(), page)

    def test_standalone_mainnet_and_network_api_allowlist(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            current = self.write_rows(root, self.records(synthetic=False))
            data = dashboard.collect(current)
            self.assertEqual(data["network"], "Mainnet")
            self.assertFalse(data["synthetic"])
            self.assertEqual(data["mixed_load"]["All sizes"]["n"], 1)
            self.assertEqual(data["load_intervals"], [])
            for network in ("Regtest", "testnet", "Mainnet<script>", None):
                with self.subTest(network=network):
                    with self.assertRaisesRegex(ValueError, "network must be"):
                        dashboard.collect(current, network=network)
                    with self.assertRaisesRegex(ValueError, "network must be"):
                        dashboard.build_site(current, root / "site", network=network)
                    with self.assertRaisesRegex(ValueError, "network must be"):
                        dashboard.render({"network": network})


if __name__ == "__main__":
    unittest.main()
