"""Loopback-only integration tests and a finite, explicitly synthetic dry demo."""

import argparse
import base64
from contextlib import ExitStack
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import signal
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import tomllib

import dispatch
import dashboard
import load_intervals


def make_chain(count):
    blocks = []
    previous = "0" * 64
    for height in range(count):
        header = (struct.pack("<I", 4) + bytes.fromhex(previous)[::-1] + bytes(64) +
                  struct.pack("<II", 1_700_000_000 + height * 75, 0x1F07FFFF) +
                  height.to_bytes(32, "little") + b"\xfd\x40\x05" + bytes(1344))
        coinbase = (struct.pack("<I", 1) + b"\x01" + bytes(32) + b"\xff" * 4 +
                    b"\x02\x01" + bytes([height % 256]) + b"\xff" * 4 + b"\x01" +
                    struct.pack("<Q", 1) + b"\x01\x51" + bytes(4))
        block_hash = hashlib.sha256(hashlib.sha256(header).digest()).digest()[::-1].hex()
        blocks.append({"hash": block_hash, "height": height, "previousblockhash": previous,
                       "raw": (header + b"\x01" + coinbase).hex()})
        previous = block_hash
    return blocks


class FakeNode:
    def __init__(self, chain, height=0, behavior=None, delay=0, auth=None):
        self.chain = chain
        self.height = height
        self.behavior = behavior or {}
        self.delay = delay
        self.auth = auth
        self.attempts = []
        self.calls = []
        self.lock = threading.Lock()
        node = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                if node.auth and self.headers.get("Authorization") != "Basic " + (
                        base64.b64encode(node.auth.encode()).decode()):
                    self.send_error(401)
                    return
                request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                result, error = None, None
                try:
                    result = node.call(request["method"], request["params"])
                except dispatch.RPCError as exc:
                    error = {"code": exc.code, "message": str(exc)}
                payload = json.dumps({"jsonrpc": "2.0", "id": request["id"],
                                      "result": result, "error": error}).encode()
                try:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                except (BrokenPipeError, ConnectionResetError):
                    pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       kwargs={"poll_interval": .01})
        self.thread.start()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def call(self, method, params):
        with self.lock:
            self.calls.append((method, time.monotonic()))
        if method == "getbestblockhash":
            return self.chain[self.height]["hash"]
        if method == "getblockhash":
            if params[0] > self.height:
                raise dispatch.RPCError({"code": -8, "message": "height out of range"})
            return self.chain[params[0]]["hash"]
        if method in ("getblockheader", "getblock"):
            block = next(block for block in self.chain if block["hash"] == params[0])
            return block["raw"] if method == "getblock" else {
                key: value for key, value in block.items() if key != "raw"}
        if method == "getblocktemplate":
            return {"height": self.height + 1, "transactions": []}
        if method == "submitblock":
            block = next(block for block in self.chain if block["raw"] == params[0])
            height = block["height"]
            with self.lock:
                self.attempts.append((height, time.monotonic()))
            behavior = self.behavior.get(height, "accept")
            time.sleep(.3 if behavior == "timeout" else self.delay)
            if behavior == "timeout":
                return "inconclusive"
            if behavior == "reject":
                return "rejected"
            if behavior == "rpc_error":
                raise dispatch.RPCError({"code": -32603, "message": "fake failure"})
            if behavior == "duplicate":
                return "duplicate"
            if height <= self.height:
                return "duplicate"
            if height != self.height + 1:
                return "bad-prevblk"
            self.height = height
            return None
        raise dispatch.RPCError({"code": -32601, "message": "unknown method"})


def config_for(feeder, nodes):
    return {"feeder": {"url": feeder.url}, "contestants": [
        {"name": f"node-{i}", "client": "fake", "version": "test", "url": node.url,
         "pid": os.getpid()} for i, node in enumerate(nodes)],
        "poll_interval_seconds": .02, "rpc_timeout_seconds": .1,
        "sample_interval_seconds": .05, "template_timeout_seconds": .1,
        "pause_threshold_seconds": 2, "synthetic": True}


def eventually(predicate, timeout=4):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(.01)
    raise AssertionError("condition not reached before deadline")


def read_records(path):
    if not Path(path).exists():
        return []
    # The dispatcher may still be writing the last record; parse only newline-terminated lines.
    text = Path(path).read_text()
    complete = text[:text.rfind("\n") + 1]
    return [json.loads(line) for line in complete.splitlines() if line.strip()]


class RunningDispatcher:
    def __init__(self, config, path):
        self.dispatcher = dispatch.Dispatcher(config, path)
        self.error = None

        def run():
            try:
                self.dispatcher.run()
            except Exception as error:
                self.error = error

        self.thread = threading.Thread(target=run)
        self.thread.start()
        eventually(lambda: self.dispatcher.latest is not None)
        # Workers need to confirm their initial tip before timing the next live block.
        time.sleep(.15)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.dispatcher.stop.set()
        self.thread.join(3)
        if self.thread.is_alive():
            raise AssertionError("dispatcher failed to stop")
        if self.error:
            raise self.error


class DispatchTests(unittest.TestCase):
    def test_outcomes(self):
        for result, expected in [(None, "accepted"), ("duplicate", "duplicate"),
                                 ("duplicate-inconclusive", "duplicate"),
                                 ("duplicate-invalid", "rejected"), ("rejected", "rejected"),
                                 ("inconclusive", "error"), ({}, "error")]:
            with self.subTest(result=result):
                self.assertEqual(dispatch.outcome(result)[0], expected)
        self.assertEqual(dispatch.outcome(error=TimeoutError())[0], "timeout")
        for code, label in [(-25, "rejected"), (-26, "rejected"), (-27, "duplicate"),
                            (-32603, "error")]:
            self.assertEqual(dispatch.outcome(error=dispatch.RPCError({"code": code}))[0], label)

    def test_concurrent_dispatch_and_samples(self):
        chain = make_chain(3)
        with ExitStack() as stack:
            directory = Path(stack.enter_context(tempfile.TemporaryDirectory()))
            feeder = stack.enter_context(FakeNode(chain))
            nodes = [stack.enter_context(FakeNode(chain, delay=.06)) for _ in range(3)]
            path = directory / "records.jsonl"
            with RunningDispatcher(config_for(feeder, nodes), path):
                feeder.height = 1
                eventually(lambda: all(node.height == 1 for node in nodes))
                eventually(lambda: any(row["event"] == "dispatch_skew"
                                       for row in read_records(path)))
            records = read_records(path)
            attempts = [row for row in records if row["event"] == "submitblock_call"]
            self.assertEqual(len(attempts), 3)
            self.assertTrue(all(not row["catch_up"] for row in attempts))
            self.assertTrue(all(row["outcome"] == "accepted" for row in attempts))
            starts = [node.attempts[0][1] for node in nodes]
            self.assertLess(max(starts) - min(starts), .025)
            skew = next(row for row in records if row["event"] == "dispatch_skew")
            self.assertLess(skew["dispatch_skew_ms"], 25)
            self.assertTrue(any(row["event"] == "getblocktemplate" and row["status"] == "success"
                                for row in records))
            self.assertTrue(any(row["event"] == "resource_interval" and row["valid"]
                                for row in records))

    def test_outcome_servers_and_slow_target_independence(self):
        chain = make_chain(4)
        with ExitStack() as stack:
            directory = Path(stack.enter_context(tempfile.TemporaryDirectory()))
            feeder = stack.enter_context(FakeNode(chain))
            behaviors = ["accept", "duplicate", "reject", "timeout", "rpc_error"]
            nodes = [stack.enter_context(FakeNode(chain, behavior={1: label}))
                     for label in behaviors]
            path = directory / "records.jsonl"
            with RunningDispatcher(config_for(feeder, nodes), path):
                feeder.height = 1
                eventually(lambda: nodes[0].height == 1)
                feeder.height = 2
                eventually(lambda: nodes[0].height == 2)
                eventually(lambda: any(row.get("outcome") == "timeout"
                                       for row in read_records(path)))
            first = {}
            for row in read_records(path):
                if row["event"] == "submitblock_call" and row["height"] == 1:
                    first.setdefault(row["target"], row["outcome"])
            self.assertEqual(list(first[name] for name in sorted(first)),
                             ["accepted", "duplicate", "rejected", "timeout", "error"])
            self.assertLess(nodes[0].attempts[1][1] - nodes[0].attempts[0][1], .1)

    def test_start_restart_catch_up_order_and_exclusion(self):
        import report
        chain = make_chain(8)
        with ExitStack() as stack:
            directory = Path(stack.enter_context(tempfile.TemporaryDirectory()))
            feeder = stack.enter_context(FakeNode(chain, height=3))
            nodes = [stack.enter_context(FakeNode(chain)) for _ in range(2)]
            config = config_for(feeder, nodes)
            path = directory / "records.jsonl"
            with RunningDispatcher(config, path):
                eventually(lambda: all(node.height == 3 for node in nodes))
                feeder.height = 4
                eventually(lambda: all(node.height == 4 for node in nodes))
            feeder.height = 6
            with RunningDispatcher(config, path):
                eventually(lambda: all(node.height == 6 for node in nodes))
                feeder.height = 7
                eventually(lambda: all(node.height == 7 for node in nodes))
            records = read_records(path)
            for node in nodes:
                self.assertEqual([height for height, _ in node.attempts], list(range(1, 8)))
            attempts = [row for row in records if row["event"] == "submitblock_call"]
            self.assertEqual({row["height"] for row in attempts if row["catch_up"]},
                             {1, 2, 3, 5, 6})
            report.build_site(path, directory / "site")
            rolling = json.loads((directory / "site" / "rolling.json").read_text())
            self.assertEqual(rolling["selection"]["pairs"][0]["eligible_pairs"], 2)

    def test_target_restart_and_wall_gap(self):
        chain = make_chain(5)
        with ExitStack() as stack:
            directory = Path(stack.enter_context(tempfile.TemporaryDirectory()))
            feeder = stack.enter_context(FakeNode(chain, height=2))
            node = stack.enter_context(FakeNode(chain, height=2))
            config = config_for(feeder, [node])
            config["pause_threshold_seconds"] = .03
            config["poll_interval_seconds"] = .05
            path = directory / "records.jsonl"
            with RunningDispatcher(config, path):
                node.height = 0
                eventually(lambda: node.height == 2)
                feeder.height = 3
                eventually(lambda: node.height == 3)
            attempts = [row for row in read_records(path) if row["event"] == "submitblock_call"]
            self.assertEqual([row["height"] for row in attempts], [1, 2, 3])
            self.assertTrue(all(row["catch_up"] for row in attempts))

    def test_cookie_rotation_and_config_validation(self):
        chain = make_chain(1)
        with tempfile.TemporaryDirectory() as directory, FakeNode(chain, auth="u:p") as node:
            root = Path(directory)
            cookie = root / "cookie"
            cookie.write_text("u:p\n")
            rpc = dispatch.RPC({"url": node.url, "cookie": str(cookie)})
            self.assertEqual(rpc.call("getbestblockhash"), chain[0]["hash"])
            node.auth = "new:secret"
            cookie.write_text(node.auth)
            self.assertEqual(rpc.call("getbestblockhash"), chain[0]["hash"])
            config = {"feeder": {"url": node.url}, "contestants": [
                {"name": "a", "client": "Zebra", "version": "test", "url": node.url}]}
            path = root / "config.json"
            path.write_text(json.dumps(config))
            with self.assertRaisesRegex(ValueError, "distinct RPC"):
                dispatch.load_config(path)
            config["contestants"][0]["url"] += "/other"
            path.write_text(json.dumps(config))
            self.assertEqual(dispatch.load_config(path)["poll_interval_seconds"], .02)
            path = root / "config.toml"
            path.write_text('[feeder]\nurl = "http://127.0.0.1:1"\n'
                            '[[contestants]]\nname = "a"\nclient = "Zebra"\nversion = "test"\n'
                            'url = "http://127.0.0.1:2"\ncookie = "cookie"\n')
            loaded = dispatch.load_config(path)
            self.assertEqual(loaded["contestants"][0]["cookie"], str(cookie))
            self.assertEqual(loaded["template_timeout_seconds"], 5)

    def test_example_configs_and_cookie_paths(self):
        root = Path(__file__).parent
        ports, state_dirs, cookie_dirs = set(), set(), set()
        for network_name in dashboard.NETWORKS:
            examples = root / "config" / network_name.lower()
            contestant = dashboard.DEFAULT_CONFIG if network_name == "Mainnet" else examples / "contestant.toml.example"
            config = dispatch.load_config(contestant)
            endpoints = [("feeder", config["feeder"]), *(
                (node["name"], node) for node in config["contestants"])]
            for name, endpoint in endpoints:
                with self.subTest(network=network_name, node=name):
                    node_config = tomllib.loads((examples / f"{name}.toml").read_text())
                    network, state, rpc = (node_config[key] for key in ("network", "state", "rpc"))
                    self.assertEqual(network["network"], network_name)
                    self.assertFalse(state["ephemeral"])
                    self.assertTrue(rpc["enable_cookie_auth"])
                    self.assertEqual(endpoint["url"], f'http://{rpc["listen_addr"]}')
                    self.assertEqual(Path(endpoint["cookie"]), Path(rpc["cookie_dir"]) / ".cookie")
                    self.assertNotIn("auth", endpoint)
                    self.assertNotIn("pid", endpoint)  # Operators supply current process IDs.
                    for address in (network["listen_addr"], rpc["listen_addr"]):
                        port = address.rsplit(":", 1)[1]
                        self.assertNotIn(port, ports)
                        self.assertLess(int(port), 32768)
                        ports.add(port)
                    self.assertNotIn(state["cache_dir"], state_dirs)
                    state_dirs.add(state["cache_dir"])
                    self.assertNotIn(rpc["cookie_dir"], cookie_dirs)
                    cookie_dirs.add(rpc["cookie_dir"])
                    if name == "feeder":
                        self.assertNotIn(f"initial_{network_name.lower()}_peers", network)
                        self.assertIsInstance(network["cache_dir"], str)
                    else:
                        self.assertEqual(network["initial_mainnet_peers"], [])
                        self.assertEqual(network["initial_testnet_peers"], [])
                        self.assertFalse(network["cache_dir"])
                        self.assertTrue(network["listen_addr"].startswith("127.0.0.1:"))
                    if name == "zakura":
                        self.assertEqual(network["p2p_stack"], "legacy")
                        self.assertEqual(state["storage_mode"]["pruned"]["tx_retention"], 10000)
                        bootstrap = tomllib.loads((examples / "zakura-bootstrap.toml").read_text())
                        for key in ("state", "rpc"):
                            self.assertEqual(bootstrap[key], node_config[key])
                        self.assertEqual(bootstrap["network"]["identity_dir"], network["identity_dir"])
                        self.assertNotIn(f"initial_{network_name.lower()}_peers", bootstrap["network"])

    def test_entry_point_help_without_nodes(self):
        root = Path(__file__).parent
        with tempfile.TemporaryDirectory() as directory:
            for script in ("dispatch.py", "report.py", "dashboard.py", "load_intervals.py"):
                with self.subTest(script=script):
                    result = subprocess.run([sys.executable, str(root / script), "--help"],
                                            cwd=directory, capture_output=True, text=True, timeout=5)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertIn("usage:", result.stdout)
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_sigterm_during_request_and_process_restart(self):
        chain = make_chain(4)
        with ExitStack() as stack:
            directory = Path(stack.enter_context(tempfile.TemporaryDirectory()))
            feeder = stack.enter_context(FakeNode(chain, height=1))
            node = stack.enter_context(FakeNode(chain, delay=.06))
            config = config_for(feeder, [node])
            config["rpc_timeout_seconds"] = 1
            config_path = directory / "config.json"
            config_path.write_text(json.dumps(config))
            output = directory / "records.jsonl"
            command = [sys.executable, str(Path(dispatch.__file__)), str(config_path),
                       "--output", str(output)]
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                eventually(lambda: bool(node.attempts))
                process.send_signal(signal.SIGTERM)
                stdout, stderr = process.communicate(timeout=3)
                self.assertEqual(process.returncode, 0, (stdout, stderr))
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate()
            self.assertEqual(node.height, 1)
            feeder.height = 3
            completed = subprocess.run(command + ["--duration", ".5"], capture_output=True,
                                       timeout=4)
            self.assertEqual(completed.returncode, 0, completed.stderr)
            attempts = [row for row in read_records(output) if row["event"] == "submitblock_call"]
            self.assertEqual([row["height"] for row in attempts], [1, 2, 3])
            self.assertTrue(all(row["catch_up"] for row in attempts))


class DashboardTests(unittest.TestCase):
    BASE = 1_700_000_000_000

    def records(self, count=1, synthetic=True):
        config = tomllib.loads(dashboard.DEFAULT_CONFIG.read_text())
        clients = {node["name"]: {"name": node["client"], "version": node["version"]}
                   for node in config["contestants"]}
        rows = [{"event": "session", "at_unix_ms": self.BASE, "clients": clients,
                 "synthetic": synthetic}]
        for number in range(count):
            at = self.BASE + number * 300_000
            for target, latency in zip(dashboard.TARGETS, (2, 12, 5)):
                rows.append({"event": "submitblock_call", "hash": f"{number:064x}",
                             "height": number, "block_bytes": 10_000,
                             "target": target, "client": clients[target]["name"],
                             "version": clients[target]["version"], "queued_unix_ms": at,
                             "dispatch_unix_ms": at, "observed_at_unix_ms": at + 20,
                             "latency_ms": latency, "outcome": "accepted", "catch_up": False,
                             "raw_block_hex": "DO_NOT_PUBLISH_RAW_BLOCK"})
        return rows

    def write_records(self, directory, rows):
        path = Path(directory) / "records.jsonl"
        path.write_text("".join(json.dumps(row) + "\n" for row in rows))
        return path

    def test_dashboard_size_bin_boundaries(self):
        self.assertEqual([dashboard.size_bin(size) for size in (0, 19_999, 20_000, 300_000, 300_001)],
                         ["<20 kB", "<20 kB", "20–300 kB", "20–300 kB", ">300 kB"])

    def test_dashboard_render_and_atomic_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rows = self.records(3)
            hostile = "</script><script>alert(1)</script>"
            rows[0]["clients"]["zakura"]["version"] = hostile
            rows[0]["hostname"] = "node.example.invalid"
            rows[0]["clients"]["zakura"]["cookie"] = "/path/to/rpc/.cookie"
            for row in rows[1:]:
                if row["target"] == "zakura":
                    row["version"] = hostile
                row["detail"] = "node.example.invalid /path/to/node OMITTED_RUN_ID"
            path = self.write_records(root, rows)
            output = root / "site"
            output.mkdir()
            (output / "index.html").write_text("existing report, untouched")
            data = dashboard.build_site(path, output)
            html = (output / "dashboard.html").read_text()
            self.assertIn("SYNTHETIC DATA", html)
            self.assertIn("Optimized Zebra", html)
            self.assertIn("Zebra v7.0.0", html)
            for old_label in ("Our stack", "our stack", "our Zebra stack", "Our Zebra stack", "Zebra reference"):
                self.assertNotIn(old_label, html)
            self.assertNotIn(hostile, html)
            self.assertNotIn("DO_NOT_PUBLISH", html)
            for omitted in ("node.example.invalid", "/path/to/", "OMITTED_RUN_ID", str(root),
                            "raw_block_hex", "config/", "report.py"):
                self.assertNotIn(omitted, html)
            self.assertIn("width=device-width", html)
            self.assertIn('id="glance-chart"', html)
            embedded = html.split('<script id="dashboard-data" type="application/json">')[1].split("</script>")[0]
            self.assertEqual(json.loads(embedded), json.loads(json.dumps(data)))
            self.assertEqual((output / "index.html").read_text(), "existing report, untouched")
            path.write_text(path.read_text() + '{"event":')
            with self.assertRaises(ValueError):
                dashboard.build_site(path, output)
            self.assertEqual((output / "dashboard.html").read_text(), html)

    def test_dashboard_load_interval_boundaries_and_mixed_table(self):
        rows = self.records(5)
        offsets = (0, 29_999, 30_000, 90_000, 90_001)
        for row in rows[1:]:
            row["dispatch_unix_ms"] = self.BASE + offsets[row["height"]]
            row["observed_at_unix_ms"] = row["dispatch_unix_ms"] + 20
        intervals = [[self.BASE + 60_000, self.BASE + 60_000]]
        with tempfile.TemporaryDirectory() as directory:
            data = dashboard.collect(self.write_records(directory, rows), load_intervals=intervals)
        self.assertEqual([point[0] for point in data["glance"]["points"]], list(range(5)))
        self.assertEqual(data["mixed_load"]["All sizes"]["n"], 5)
        self.assertEqual(data["load_intervals"], [[self.BASE + 30_000, self.BASE + 90_000]])
        # Quiet host excludes blocks whose first dispatch touches a padded interval, endpoints included.
        self.assertEqual([row["busy"] for row in data["per_block"]], [False, False, True, True, False])
        self.assertEqual(data["quiet"]["nodes"]["zebra-candidate"]["n"], 3)
        # Load is unknown, not quiet, for blocks dispatched more than the padding after the export.
        with tempfile.TemporaryDirectory() as directory:
            stale = dashboard.collect(self.write_records(directory, rows), load_intervals=intervals,
                                      covered_until=self.BASE + 30_000)
        self.assertEqual([row["busy"] for row in stale["per_block"]], [False, False, True, None, None])
        self.assertEqual(stale["quiet"]["nodes"]["zebra-candidate"]["n"], 2)
        with tempfile.TemporaryDirectory() as directory:
            unknown = dashboard.collect(self.write_records(directory, rows))
        self.assertEqual({row["busy"] for row in unknown["per_block"]}, {None})
        self.assertEqual(unknown["quiet"]["nodes"]["zebra-candidate"]["n"], 0)
        self.assertEqual(load_intervals.merge_intervals([[100, 200], [0, 50], [180, 300]], padding=30),
                         [[0, 330]])
        for invalid in ([[1, 0]], [[True, 2]], [[1.5, 2]], [[1]], {"start": 0}):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                load_intervals.merge_intervals(invalid)


    def test_dashboard_rejects_identity_or_block_metadata_changes(self):
        for field, value in (("version", "new-build"), ("block_bytes", 42), ("height", 42)):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                rows = self.records()
                rows[-1][field] = value
                with self.assertRaises(ValueError):
                    dashboard.collect(self.write_records(directory, rows))


def demo(output):
    import report
    output.mkdir(parents=True, exist_ok=True)
    path = output / "records.jsonl"
    if path.exists():
        raise ValueError(f"refusing to append a demo to existing {path}")
    chain = make_chain(26)
    with ExitStack() as stack:
        feeder = stack.enter_context(FakeNode(chain, height=2))
        nodes = [stack.enter_context(FakeNode(chain, delay=delay)) for delay in (.005, .015)]
        with RunningDispatcher(config_for(feeder, nodes), path):
            eventually(lambda: all(node.height == 2 for node in nodes))
            for height in range(3, 26):
                feeder.height = height
                eventually(lambda: all(node.height == height for node in nodes))
    report.build_site(path, output / "site")
    rolling = json.loads((output / "site" / "rolling.json").read_text())
    attempts = [row for row in read_records(path) if row["event"] == "submitblock_call"]
    print(json.dumps({"records": str(path), "site": str(output / "site"), "synthetic": True,
                      "attempts": len(attempts),
                      "catch_up_attempts": sum(row["catch_up"] for row in attempts),
                      "pairs": rolling["selection"]["pairs"]}, indent=2))


if __name__ == "__main__":
    if "--demo" in sys.argv:
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument("--demo", type=Path, required=True)
        demo(parser.parse_args().demo)
    else:
        unittest.main()
