#!/usr/bin/env python3
"""Offline CLI/dispatch and stopped-genesis checks; never launches a node."""

import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("genesis_snapshot", HERE / "make-genesis-snapshot.py")
genesis = importlib.util.module_from_spec(spec)
spec.loader.exec_module(genesis)


class InitialSyncTests(unittest.TestCase):
    def test_genesis_requires_successful_exact_stop(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            snapshot = root / "snapshot-0"
            snapshot.mkdir()
            log = root / "node.log"
            node = SimpleNamespace(proc=SimpleNamespace(returncode=0), log_path=log)
            log.write_text("stopping at configured height, flushing database to disk height=Height(0)\n")
            genesis.verify_genesis(node, snapshot)
            node.proc.returncode = 1
            with self.assertRaises(RuntimeError):
                genesis.verify_genesis(node, snapshot)
            node.proc.returncode = 0
            log.write_text("stopping at configured height, flushing database to disk height=Height(1)\n")
            with self.assertRaises(RuntimeError):
                genesis.verify_genesis(node, snapshot)
            log.write_text("stopping at configured height, flushing database to disk height=Height(0)\n")
            snapshot.rmdir()
            with self.assertRaises(RuntimeError):
                genesis.verify_genesis(node, snapshot)

    def test_entrypoint_help_does_not_require_inputs(self):
        scripts = ["run-sync-full.sh", "run-sync-optimized.sh", "precommit-phase.sh",
                   "commit-hashing-ab.sh", "commit-profile.sh", "prepare-mainnet-snapshots.sh"]
        with tempfile.TemporaryDirectory() as temp:
            for name in scripts + ["make-genesis-snapshot.py", "phase-summary.py"]:
                command = ["bash"] if name.endswith(".sh") else [sys.executable, "-B"]
                result = subprocess.run([*command, str(HERE / name), "--help"], cwd=temp,
                                        capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("usage:", result.stdout.lower())
            self.assertEqual(list(Path(temp).iterdir()), [])

    def test_wrapper_dispatch_and_first_arm_sources(self):
        # Intercept Python invocations: check wrapper arguments without executing the probes.
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            capture = root / "commands.jsonl"
            python = root / "python3"
            python.write_text(f"#!{sys.executable}\nimport json, os, sys\n"
                              "with open(os.environ['CAPTURE'], 'a') as output:\n"
                              "    output.write(json.dumps(sys.argv[1:]) + '\\n')\n")
            python.chmod(0o700)
            env = dict(os.environ, PATH=str(root) + os.pathsep + os.environ["PATH"],
                       CAPTURE=str(capture), REFERENCE_BIN="bin/reference with space",
                       OPTIMIZED_BIN="bin/optimized", BASE_BIN="bin/base", ONEHASH_BIN="bin/onehash",
                       SNAPSHOT="data/genesis", SOURCE_STATE="data/source", PROVENANCE="arms.json",
                       OUTPUT=str(root / "output"), CPUS="all", SOURCE_CPUS="all",
                       END_HEIGHT="3522460", TIMEOUT="259200", SOURCES="4", SOURCE_PORT_BASE="31233",
                       REPS="2", BENCH_FIXTURES="fixtures with space")

            def dispatch(name, *args):
                capture.write_text("")
                subprocess.run(["bash", str(HERE / name), *args], env=env, cwd=root,
                               check=True, capture_output=True, text=True, timeout=10)
                return [json.loads(line) for line in capture.read_text().splitlines()]

            def arms(command):
                return [command[i + 1] for i, value in enumerate(command) if value == "--arm"]

            full, = dispatch("run-sync-full.sh")
            optimized, = dispatch("run-sync-optimized.sh")
            self.assertEqual(arms(full), ["v7.0.0=bin/reference with space", "optimized-asm=bin/optimized"])
            self.assertEqual(arms(optimized), ["optimized-asm=bin/optimized"])
            self.assertEqual(full[full.index("--cpus") + 1], "all")
            self.assertEqual(full[full.index("--source-cpus") + 1], "all")
            fixtures = ["transparent-1900000", "mixed-1900000", "orchard-1900000"]
            precommit = dispatch("precommit-phase.sh")[:-1]  # final invocation is the summary
            self.assertEqual([command[-3] for command in precommit], fixtures * 2)
            hashing = dispatch("commit-hashing-ab.sh")[:-1]
            self.assertEqual([(command[-4], command[-3]) for command in hashing],
                             [(arm, fixture) for _ in range(2) for fixture in fixtures
                              for arm in ("base", "onehash")])
            for command in precommit + hashing:
                self.assertEqual(command[command.index("--fixtures") + 1], "fixtures with space")
                self.assertEqual(command[command.index("--provenance") + 1], "arms.json")
            profiles = dispatch("commit-profile.sh", "bin/profile with space", "profile")
            self.assertEqual(len(profiles), 2)
            for command in profiles:
                self.assertIn("--perf", command)
                self.assertEqual(command[command.index("--perf-callgraph") + 1], "fp")
                self.assertEqual(command[-5:-1], ["bin/profile with space", "profile", "transparent-1900000", "1"])

    def test_snapshot_destination_is_not_overwritten(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            snapshot = root / "snapshot"
            snapshot.mkdir()
            env = dict(os.environ, STOP_AT="1750000", SNAPSHOT_OUTPUT=str(snapshot))
            result = subprocess.run(["bash", str(HERE / "prepare-mainnet-snapshots.sh"),
                                     sys.executable, str(root / "run")], env=env,
                                    capture_output=True, text=True, timeout=10)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("already exists", result.stderr)
            self.assertFalse((root / "run").exists())


if __name__ == "__main__":
    unittest.main()
