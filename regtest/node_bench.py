#!/usr/bin/env python3
"""Regtest node benchmarks for the Zakura-port round.

Arms are zebrad binaries. Scenarios:
  gbci    getblockchaininfo latency on one node (concurrency 1 and 4)
  gossip  three-node line n1 -> n2 -> n3; blocks generated on n1 every --spacing seconds;
          latency = generate return on n1 -> block is n3's best tip (two relay hops; the
          second hop uses n2's committed-tip gossip path)
Each (scenario, arm) case runs in a fresh process set; arm order is shuffled per round.
"""

import hashlib
import struct
import argparse
import http.client
import json
import mmap
import os
import random
import re
import shutil
import statistics
import subprocess
import sys
import threading
import time
from contextlib import ExitStack
from datetime import datetime, timezone
from functools import cache
from pathlib import Path

from thorough import node_process

BENCH_ROOT = Path(__file__).resolve().parent.parent
FIXTURES = Path(os.environ.get("BENCH_FIXTURES", BENCH_ROOT / "fixtures/regtest")).expanduser().resolve()
BENCH_HELP = """Paths: BENCH_BINS=bins, BENCH_FIXTURES=fixtures/regtest,
BENCH_SIGNED_FIXTURES=fixtures/transparent-signed, BENCH_MAINNET=mainnet,
BENCH_OUTPUT=output (defaults relative to the repository root; overrides relative
to the invoking working directory). CLI paths override defaults. Nodes inherit
current CPU affinity; use taskset -c LIST for individual probes. V5_CPUS sets
campaign workload affinity; SOURCE_CPUS sets replay source affinity, each unset
by default. BENCH_PORT_BASE=0 selects ephemeral loopback RPC/P2P/wallet/phase
metrics ports; a nonzero base assigns consecutive ports within each driver.
Reserve that range for one driver at a time. Replay source P2P ports use
BENCH_SOURCE_PORT_BASE=31233 or --source-port-base. QUIET_DEVICE is required
by wait-quiet.sh (whole-device name under /sys/block, e.g. sda)."""
_port_offset = 0


def next_port():
    """Choose kernel allocation by default, or the next explicitly reserved port."""
    global _port_offset
    base = int(os.environ.get("BENCH_PORT_BASE", "0"))
    if base == 0:
        return 0
    port = base + _port_offset
    if not 1 <= base <= port <= 65535:
        raise ValueError("BENCH_PORT_BASE range must stay within 1..65535")
    _port_offset += 1
    return port

HALO2_PRELOAD_DONE = b"finished Halo2 verifying key preload"


@cache
def binary_preloads(binary):
    """Inspect each resolved executable path once, without copying the binary."""
    with Path(binary).open("rb") as executable:
        if os.fstat(executable.fileno()).st_size == 0:
            return False
        with mmap.mmap(executable.fileno(), 0, access=mmap.ACCESS_READ) as image:
            return image.find(HALO2_PRELOAD_DONE) != -1


def require(cond, msg):
    if not cond:
        raise RuntimeError(msg)


class RPC:
    def __init__(self, address, timeout=30):
        host, port = address.rsplit(":", 1)
        self.conn = http.client.HTTPConnection(host, int(port), timeout=timeout)
        self.n = 0

    def call(self, method, params=None):
        self.n += 1
        body = json.dumps(
            {"jsonrpc": "2.0", "id": self.n, "method": method, "params": params or []}
        )
        try:
            self.conn.request("POST", "/", body, {"Content-Type": "application/json"})
            resp = json.loads(self.conn.getresponse().read())
        except Exception:
            self.conn.close()
            raise
        if resp.get("error"):
            raise RuntimeError(json.dumps(resp["error"]))
        return resp["result"]


def resources(pid):
    status = Path("/proc/%d/status" % pid).read_text()
    stat = Path("/proc/%d/stat" % pid).read_text().rsplit(")", 1)[1].split()
    kb = dict(re.findall(r"^(VmRSS|VmHWM):\s+(\d+) kB$", status, re.M))
    return {
        "cpu_s": (int(stat[11]) + int(stat[12])) / os.sysconf("SC_CLK_TCK"),
        "rss": int(kb.get("VmRSS", 0)) * 1024,
        "hwm": int(kb.get("VmHWM", 0)) * 1024,
    }


CONFIG = """[network]
network = "Regtest"
listen_addr = "127.0.0.1:0"
initial_mainnet_peers = []
initial_testnet_peers = []
cache_dir = false
max_connections_per_ip = 16
crawl_new_peer_interval = "1s"

[network.testnet_parameters]
funding_streams = []

[network.testnet_parameters.activation_heights]
Overwinter = 1
Sapling = 1
Blossom = 1
Heartwood = 1
Canopy = 1

[state]
ephemeral = false
cache_dir = {state}

[sync]
parallel_cpu_threads = 4

[rpc]
listen_addr = "127.0.0.1:0"
enable_cookie_auth = false
parallel_cpu_threads = 4

[mempool]
debug_enable_at_height = 0

[mining]
miner_address = "{miner}"
"""

# Zakura's Regtest is configured under `network.network` and needs the legacy P2P stack.
ZAKURA_NETWORK = """[network]
listen_addr = "127.0.0.1:0"
initial_mainnet_peers = []
initial_testnet_peers = []
cache_dir = false
max_connections_per_ip = 16
crawl_new_peer_interval = "1s"
p2p_stack = "legacy"

[network.network]
regtest = true

[network.network.params]
funding_streams = []
max_block_time_start_height = 2

[network.network.params.activation_heights]
"""


def config_for(binary, state, miner):
    text = CONFIG.format(state=state, miner=miner)
    if "zakura" in Path(binary).name:
        zebra_network, rest = text.split("[state]", 1)
        heights = zebra_network.split(
            "[network.testnet_parameters.activation_heights]\n", 1
        )[1]
        text = ZAKURA_NETWORK + heights + "\n[state]" + rest
    text = text.replace('listen_addr = "127.0.0.1:0"',
                        f'listen_addr = "127.0.0.1:{next_port()}"', 1)
    rpc, separator, rest = text.partition("[rpc]")
    text = rpc + separator + rest.replace('listen_addr = "127.0.0.1:0"',
                                         f'listen_addr = "127.0.0.1:{next_port()}"', 1)
    return text


# Anyone-can-spend P2SH: redeem script OP_1, scriptSig pushes it.
HASH160 = bytes.fromhex("da1745e9b549bd0bfa1a569971c77eba30cd5a4b")
SCRIPT = b"\xa9\x14" + HASH160 + b"\x87"
FANOUT_FEE = 5000 * 200  # ZIP-317 conventional fee for 200 logical actions
FEE = 20000


def dsha(data):
    return hashlib.sha256(hashlib.sha256(data).digest()).digest()


def p2sh_address():
    payload = bytes.fromhex("1cba") + HASH160  # Testnet/Regtest P2SH prefix.
    raw = payload + dsha(payload)[:4]
    n = int.from_bytes(raw, "big")
    alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    out = ""
    while n:
        n, r = divmod(n, 58)
        out = alphabet[r] + out
    return out


P2SH_MINER = p2sh_address()
DEFAULT_MINER = "t27eWDgjFYJGVXmzrXeVjnb5J3uXDM9xH9v"


def compact(n):
    if n < 253:
        return bytes([n])
    if n <= 0xFFFF:
        return b"\xfd" + struct.pack("<H", n)
    return b"\xfe" + struct.pack("<I", n)


def v4_tx(sources, values):
    """Canopy V4 tx spending anyone-can-spend P2SH `sources` [(txid_hex, index, value)]."""
    require(sum(s[2] for s in sources) > sum(values), "fee")
    raw = struct.pack("<II", 0x80000004, 0x892F2085) + compact(len(sources))
    for txid, index, _ in sources:
        raw += (
            bytes.fromhex(txid)[::-1]
            + struct.pack("<I", index)
            + b"\x02\x01\x51\xff\xff\xff\xff"
        )
    raw += compact(len(values))
    for v in values:
        raw += struct.pack("<q", v) + compact(len(SCRIPT)) + SCRIPT
    raw += struct.pack("<IIq", 0, 0, 0) + b"\x00\x00\x00"
    return raw, dsha(raw)[::-1].hex()


class Node:
    preload_wait = True

    def init_preload(self):
        self.binary_preloads = binary_preloads(str(Path(self.binary).resolve()))
        self._preload_clock_offset_ns = time.time_ns() - time.perf_counter_ns()
        self._preload_done_unix_ns = None
        self._preload_timestamp_source = None

    def scan_preload(self):
        if self.binary_preloads and self._preload_done_unix_ns is None:
            with self.log_path.open("rb") as log:
                if os.fstat(log.fileno()).st_size == 0:
                    return None
                with mmap.mmap(log.fileno(), 0, access=mmap.ACCESS_READ) as text:
                    position = text.find(HALO2_PRELOAD_DONE)
                    if position < 0:
                        return None
                    line = text[text.rfind(b"\n", 0, position) + 1:position]
            stamp = re.search(rb"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d+))?Z", line)
            if stamp:
                seconds = int(datetime.fromisoformat(stamp[1].decode()).replace(
                    tzinfo=timezone.utc).timestamp())
                self._preload_done_unix_ns = seconds * 1_000_000_000 + int(
                    (stamp[2] or b"").ljust(9, b"0")[:9])
                self._preload_timestamp_source = "node-log"
            else:
                self._preload_done_unix_ns = time.time_ns()
                self._preload_timestamp_source = "log-observed"
        return self._preload_done_unix_ns

    def wait_preload(self, timeout=60):
        """Gate post-ready work, but never wait for binaries without the log marker."""
        if not self.binary_preloads or not self.preload_wait:
            reason = "binary has no preload" if not self.binary_preloads else "startup-overlap case"
            print(f"Halo2 preload: not waiting ({reason}): {self.binary}", file=sys.stderr, flush=True)
            return
        started = time.monotonic()
        print(f"Halo2 preload: waiting up to {timeout:g}s: {self.binary}", file=sys.stderr, flush=True)
        while True:
            require(self.proc.poll() is None, "node exited waiting for Halo2 verifying key preload")
            if self.scan_preload() is not None:
                print(f"Halo2 preload: done after {time.monotonic() - started:.3f}s: {self.binary}",
                      file=sys.stderr, flush=True)
                return
            if time.monotonic() - started >= timeout:
                raise TimeoutError(f"Halo2 verifying key preload did not finish within {timeout:g}s; "
                                   f"refusing to measure {self.binary}; log: {self.log_path}")
            time.sleep(0.02)

    def preload_receipt(self, start_ns):
        """Log completion relative to this window; positive values expose startup overlap."""
        unix_ns = self.scan_preload()
        done_ns = None if unix_ns is None else unix_ns - self._preload_clock_offset_ns
        return dict(binary_preloads=self.binary_preloads, preload_wait=self.preload_wait,
                    preload_done_unix_ns=unix_ns, preload_done_ns=done_ns,
                    preload_done_relative_ns=None if done_ns is None else done_ns - start_ns,
                    preload_timestamp_source=self._preload_timestamp_source)

    def __init__(self, binary, directory, env, miner=DEFAULT_MINER):
        self.binary, self.dir = binary, Path(directory)
        self.dir.mkdir(parents=True)
        self.config = self.dir / "config.toml"
        self.config.write_text(
            config_for(binary, json.dumps(str(self.dir / "state")), miner)
        )
        self.log_path = self.dir / "node.log"
        self.env = env
        self.rpc_addr = self.p2p_addr = None
        self.proc = None

    def _scan(self):
        text = re.sub(r"\x1b\[[0-9;]*m", "", self.log_path.read_text(errors="replace"))
        m = re.search(r"Opened RPC endpoint at (127\.0\.0\.1:\d+)", text)
        self.rpc_addr = m and m[1]
        m = re.search(r"Opened Zcash protocol endpoint at (127\.0\.0\.1:\d+)", text)
        self.p2p_addr = m and m[1]

    def wait(self, pred, label, timeout=120):
        end = time.monotonic() + timeout
        last = None
        while time.monotonic() < end:
            require(self.proc.poll() is None, "node exited waiting for " + label)
            try:
                r = pred()
                if r:
                    return r
            except Exception as e:  # transient while starting
                last = repr(e)
            time.sleep(0.02)
        raise TimeoutError("%s (%s)" % (label, last))

    def start(self):
        self.init_preload()
        self.log = self.log_path.open("wb")
        self.proc = node_process.spawn(
            [self.binary, "-c", str(self.config), "start"],
            stdout=self.log,
            stderr=subprocess.STDOUT,
            env=self.env,
            directory=self.dir,
        )
        try:
            self.wait(
                lambda: (self._scan(), self.rpc_addr and self.p2p_addr)[1], "endpoints"
            )
            self.rpc = RPC(self.rpc_addr)
            self.wait(
                lambda: self.rpc.call("getblock", [self.rpc.call("getbestblockhash"), 0]),
                "genesis",
            )
            self.wait_preload()
        except BaseException:
            self.stop()
            raise
        return self

    def stop(self):
        if getattr(self.proc, "_node_state", None) is not None:
            try:
                node_process.stop(self.proc)
            finally:
                self.log.close()
            return
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(20)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(10)
        self.log.close()


def preload_self_test():
    """No executables launched or real sleeps: detection, gate, timeout and overlap."""
    from tempfile import TemporaryDirectory
    from unittest.mock import MagicMock, patch
    from front_common import MainnetNode

    with TemporaryDirectory() as temporary:
        root = Path(temporary)
        binary, log = root / "binary", root / "node.log"
        binary.write_bytes(b"\x00" + HALO2_PRELOAD_DONE + b"\x00")
        old = root / "old"
        old.write_bytes(b"older Zebra or Zakura")
        assert binary_preloads(str(binary))
        binary.write_bytes(b"cache must avoid another scan")
        assert binary_preloads(str(binary))
        assert not binary_preloads(str(old))
        done = b"2026-10-24T00:00:00.123456789Z INFO zebrad::commands::start: " + HALO2_PRELOAD_DONE
        done_ns = 1792800000123456789
        node = Node.__new__(Node)
        node.binary, node.log_path, node.proc = binary, log, MagicMock()
        node.proc.poll.return_value = None
        node.init_preload()
        log.write_bytes(b"RPC ready\n")
        with patch.object(time, "sleep", side_effect=lambda _: log.write_bytes(done)) as sleep:
            node.wait_preload()
        assert sleep.call_count == 1
        node._preload_clock_offset_ns = 1_000_000
        receipt = node.preload_receipt(done_ns - 500_000)
        assert receipt["binary_preloads"] and receipt["preload_wait"]
        assert receipt["preload_done_unix_ns"] == done_ns
        assert receipt["preload_done_relative_ns"] == -500_000
        assert receipt["preload_timestamp_source"] == "node-log"

        node.init_preload()
        log.write_bytes(b"RPC ready, but no preload completion\n")
        with patch.object(time, "monotonic", side_effect=[0, 0, 60]), patch.object(time, "sleep"):
            try:
                node.wait_preload()
            except TimeoutError as error:
                assert "60s" in str(error) and "refusing to measure" in str(error)
            else:
                raise AssertionError("missing preload completion was measured")
        node.preload_wait = False
        with patch.object(node, "scan_preload", side_effect=AssertionError("restart must not wait")):
            node.wait_preload()
        assert node.preload_receipt(0)["preload_done_relative_ns"] is None
        log.write_bytes(done)
        node._preload_clock_offset_ns = 0
        assert node.preload_receipt(done_ns - 1)["preload_done_relative_ns"] == 1

        node.binary, node.preload_wait = old, True
        node.init_preload()
        log.unlink()
        with patch.object(time, "sleep", side_effect=AssertionError("old binary must not wait")):
            node.wait_preload()
        receipt = node.preload_receipt(0)
        assert not receipt["binary_preloads"] and receipt["preload_done_relative_ns"] is None

        # Mainnet readiness remains exec-to-RPC-ready, but work cannot begin before the gate.
        front = MainnetNode.__new__(MainnetNode)
        front.proc, front.rpc = node.proc, MagicMock()
        events = []
        front.rpc.side_effect = lambda *_: events.append("ready")
        front.wait_preload = lambda: events.append("preload")
        assert front.wait_rpc_ready(1) == front.ready_ns
        assert events == ["ready", "preload"]
        front.wait_preload = MagicMock(side_effect=TimeoutError("preload deadline"))
        try:
            front.wait_rpc_ready(1)
        except TimeoutError as error:
            assert str(error) == "preload deadline"
        else:
            raise AssertionError("preload timeout swallowed by RPC readiness retry")
    print("Halo2 preload self-test: ok (cached detection, ready gate, 60s timeout, restart overlap)")


def env():
    e = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("ZEBRA", "RUST_LOG", "MALLOC_", "RAYON", "TOKIO"))
    }
    e.update({"NO_COLOR": "1", "TERM": "dumb", "RAYON_NUM_THREADS": "4"})
    return e


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))]


def summary(xs):
    return {
        "n": len(xs),
        "median": statistics.median(xs),
        "p95": pct(xs, 95),
        "p99": pct(xs, 99),
        "mean": statistics.fmean(xs),
        "min": min(xs),
        "max": max(xs),
    }


def scenario_gbci(binary, out, args):
    node = Node(binary, out / "n1", env()).start()
    try:
        node.rpc.call("generate", [20])
        node.rpc.call("getblockchaininfo")
        result = {}
        for conc in (1, 4):
            lat = []
            lock = threading.Lock()
            per = args.gbci_calls // conc

            errors = []

            def worker():
                try:
                    rpc = RPC(node.rpc_addr)
                    mine = []
                    for _ in range(per):
                        t = time.perf_counter_ns()
                        rpc.call("getblockchaininfo")
                        mine.append((time.perf_counter_ns() - t) / 1e6)
                    with lock:
                        lat.extend(mine)
                except BaseException as e:
                    with lock:
                        errors.append(repr(e))

            t0 = time.perf_counter()
            threads = [threading.Thread(target=worker) for _ in range(conc)]
            [t.start() for t in threads]
            [t.join() for t in threads]
            wall = time.perf_counter() - t0
            require(not errors, "gbci worker failed: %s" % errors)
            require(len(lat) == per * conc, "incomplete gbci samples")
            result["conc%d" % conc] = dict(summary(lat), req_per_s=len(lat) / wall)
        result["resources"] = resources(node.proc.pid)
        return result
    finally:
        node.stop()


def scenario_gossip(binary, out, args):
    nodes = [Node(binary, out / ("n%d" % i), env()) for i in (1, 2, 3)]
    # Keep the line topology: all nodes share 127.0.0.1, so a per-IP limit of one connection on
    # the end nodes rejects any n1<->n3 link once each has its n2 link; n2 keeps a higher limit.
    for end in (nodes[0], nodes[2]):
        end.config.write_text(
            end.config.read_text().replace(
                "max_connections_per_ip = 16", "max_connections_per_ip = 1"
            )
        )
    try:
        for n in nodes:
            n.start()
        n1, n2, n3 = nodes
        n2.rpc.call("addnode", [n1.p2p_addr, "add"])
        n1.wait(lambda: n1.rpc.call("getpeerinfo"), "n1-n2 link")
        n3.rpc.call("addnode", [n2.p2p_addr, "add"])
        n3.wait(lambda: n3.rpc.call("getpeerinfo"), "n3-n2 link")

        def check_line():
            p1, p3 = n1.rpc.call("getpeerinfo"), n3.rpc.call("getpeerinfo")
            require(
                len(p1) == 1 and len(p3) == 1, "end nodes must each have one peer (n2)"
            )
            require(p3[0]["addr"] == n2.p2p_addr, "n3's only peer must be n2")

        check_line()
        # Warm up: a few blocks so every node is at the same tip and n2 considers itself synced.
        for _ in range(args.gossip_warmup):
            h = n1.rpc.call("generate", [1])[0]
            n3.wait(
                lambda: n3.rpc.call("getbestblockhash") == h, "warmup propagation", 120
            )
        # end_to_end: generate return on n1 -> first n3 response showing the block.
        # relay_hop: n2 commit -> n3 commit (n2's committed-block gossip, n3 download/commit).
        # A node's commit lies after the START of its last old-tip request (or the generate call)
        # and before the END of its first new-tip response, so each hop has valid bounds
        # [n3_lo - n2_hi, n3_hi - n2_lo]. Both bounds are reported per sample and summarized
        # separately; no sample is dropped.
        end_to_end, hop_lo, hop_hi = [], [], []
        for i in range(args.gossip_blocks):
            target = time.perf_counter() + args.spacing
            n2_lo = n3_lo = time.perf_counter_ns()
            h = n1.rpc.call("generate", [1])[0]
            t0 = time.perf_counter_ns()
            n2_hi = n3_hi = None
            deadline = time.monotonic() + 120
            while n3_hi is None:
                require(time.monotonic() < deadline, "propagation timeout")
                if n2_hi is None:
                    start = time.perf_counter_ns()
                    if n2.rpc.call("getbestblockhash") == h:
                        n2_hi = time.perf_counter_ns()
                    else:
                        n2_lo = start
                start = time.perf_counter_ns()
                if n3.rpc.call("getbestblockhash") == h:
                    n3_hi = time.perf_counter_ns()
                else:
                    n3_lo = start
                time.sleep(0.005)
            if n2_hi is None:
                # n3 observed the block first; n2 committed before n3's response at the latest.
                n2_hi = n3_hi
            end_to_end.append((n3_hi - t0) / 1e6)
            hop_lo.append(max(0, n3_lo - n2_hi) / 1e6)
            hop_hi.append((n3_hi - n2_lo) / 1e6)
            time.sleep(max(0.0, target - time.perf_counter()))
        check_line()
        require(len(end_to_end) == args.gossip_blocks, "incomplete gossip samples")
        return {
            "latency_ms": summary(end_to_end),
            "relay_hop_lower_ms": summary(hop_lo),
            "relay_hop_upper_ms": summary(hop_hi),
            "samples_ms": end_to_end,
            "relay_hop_lower_samples_ms": hop_lo,
            "relay_hop_upper_samples_ms": hop_hi,
        }
    finally:
        with ExitStack() as cleanup:
            for n in nodes:
                cleanup.callback(n.stop)


def coinbase_payout(node, block_hash):
    block = node.rpc.call("getblock", [block_hash, 1])
    tx = node.rpc.call("getrawtransaction", [block["tx"][0], 1])
    outs = [
        (tx["txid"], o["n"], o.get("valueZat", int(round(o["value"] * 1e8))))
        for o in tx["vout"]
        if o["scriptPubKey"]["hex"] == SCRIPT.hex()
    ]
    require(len(outs) == 1, "one P2SH payout")
    return outs[0]


def admit(node, txs):
    for raw, txid in txs:
        require(
            node.rpc.call("sendrawtransaction", [raw.hex()]) == txid, "admitted txid"
        )
    want = {t for _, t in txs}
    node.wait(
        lambda: want <= set(node.rpc.call("getrawmempool")), "mempool admission", 300
    )


def scenario_tchain(binary, out, args):
    """Transparent load: --tchain-blocks blocks of --tchain-txs 1-in/1-out P2SH spends over a
    pool of --tchain-utxos outputs, so the 100-block non-finalized window fills with
    transparent created/spent outputs. Times each `generate 1` (template + commit)."""
    node = Node(binary, out / "n1", env(), miner=P2SH_MINER).start()
    try:
        node.wait(lambda: node.rpc.call("getblockchaininfo"), "rpc")
        per = 200
        nf = (args.tchain_utxos + per - 1) // per
        # Coinbase at height h is spendable from height h + 100; the next block is 100 + nf + 1.
        hashes = node.rpc.call("generate", [100 + nf])
        fanouts, utxos = [], []
        for h in hashes[:nf]:
            src = coinbase_payout(node, h)
            value = (src[2] - FANOUT_FEE) // per
            raw, txid = v4_tx([src], [value] * per)
            fanouts.append((raw, txid))
            utxos.extend((txid, i, value) for i in range(per))
        admit(node, fanouts)
        node.rpc.call("generate", [1])
        utxos = utxos[: args.tchain_utxos]
        rng = random.Random(7)
        gen_ms, admit_ms = [], []
        for b in range(args.tchain_blocks):
            rng.shuffle(utxos)
            batch, utxos = utxos[: args.tchain_txs], utxos[args.tchain_txs :]
            txs = [v4_tx([u], [u[2] - FEE]) for u in batch]
            t = time.perf_counter_ns()
            admit(node, txs)
            admit_ms.append((time.perf_counter_ns() - t) / 1e6)
            t = time.perf_counter_ns()
            block_hash = node.rpc.call("generate", [1])[0]
            gen_ms.append((time.perf_counter_ns() - t) / 1e6)
            block_txids = node.rpc.call("getblock", [block_hash, 1])["tx"]
            require(
                set(block_txids[1:]) == {t for _, t in txs},
                "block holds exactly the batch",
            )
            utxos.extend((txid, 0, u[2] - FEE) for (raw, txid), u in zip(txs, batch))
        res = resources(node.proc.pid)
        return {
            "generate_ms": summary(gen_ms),
            "generate_last50_ms": summary(gen_ms[-50:]),
            "admit_ms": summary(admit_ms),
            "samples_generate_ms": gen_ms,
            "resources": res,
        }
    finally:
        node.stop()


SAPLING_MINER = "zregtestsapling1xl84ekz6stprmvrp39s77mf9t953nqjndwlcjtzfrr3cgjjez87639xm4u9pfuvylrhecx0c2j8"


def scenario_gbtsapling(binary, out, args):
    """Shielded-coinbase mining with fees: n1 (P2SH payout) funds a UTXO pool, n2 (Sapling
    payout) syncs it, then n1 stops. Each round admits --gbt-txs fee-paying transactions to n2
    and times `generate 1` (new-tip template with a proven Sapling coinbase + commit).
    """
    n1 = Node(binary, out / "n1", env(), miner=P2SH_MINER).start()
    n2 = None
    try:
        per = 200
        nf = (args.gbt_txs * args.gbt_blocks + per - 1) // per
        hashes = n1.rpc.call("generate", [100 + nf])
        fanouts, utxos = [], []
        for h in hashes[:nf]:
            src = coinbase_payout(n1, h)
            value = (src[2] - FANOUT_FEE) // per
            raw, txid = v4_tx([src], [value] * per)
            fanouts.append((raw, txid))
            utxos.extend((txid, i, value) for i in range(per))
        admit(n1, fanouts)
        tip = n1.rpc.call("generate", [1])[0]
        n2 = Node(binary, out / "n2", env(), miner=SAPLING_MINER).start()
        n2.rpc.call("addnode", [n1.p2p_addr, "add"])
        n2.wait(
            lambda: n2.rpc.call("getbestblockhash") == tip,
            "n2 synced funding chain",
            300,
        )
        n1.stop()
        n1 = None
        gen_ms = []
        for b in range(args.gbt_blocks):
            batch, utxos = utxos[: args.gbt_txs], utxos[args.gbt_txs :]
            txs = [v4_tx([u], [u[2] - FEE]) for u in batch]
            admit(n2, txs)
            t = time.perf_counter_ns()
            block_hash = n2.rpc.call("generate", [1])[0]
            gen_ms.append((time.perf_counter_ns() - t) / 1e6)
            block_txids = n2.rpc.call("getblock", [block_hash, 1])["tx"]
            require(
                set(block_txids[1:]) == {t for _, t in txs},
                "block holds exactly the batch",
            )
            coinbase = n2.rpc.call("getrawtransaction", [block_txids[0], 1])
            require(coinbase.get("vShieldedOutput"), "coinbase pays a Sapling output")
        return {
            "generate_ms": summary(gen_ms),
            "samples_generate_ms": gen_ms,
            "resources": resources(n2.proc.pid),
        }
    finally:
        with ExitStack() as cleanup:
            for n in (n1, n2):
                if n is not None:
                    cleanup.callback(n.stop)


def replay(node, chain):
    for entry in chain:
        require(node.rpc.call("submitblock", [entry["raw"]]) is None, "setup replay")
    require(node.rpc.call("getbestblockhash") == chain[-1]["hash"], "replayed parent")


def scenario_coldsubmit(binary, out, args):
    """`submitblock` latency for blocks whose transactions the node has never seen, like a
    competitor's block arriving cold. One fresh node per candidate; candidates are fixed bytes.
    """
    result = {}
    for label, (chain, raw) in sorted(args.cold_candidates.items()):
        node = Node(binary, out / label, env()).start()
        try:
            replay(node, chain)
            require(node.rpc.call("getrawmempool") == [], "cold node mempool empty")
            node.rpc.call("getinfo")
            t = time.perf_counter_ns()
            reply = node.rpc.call("submitblock", [raw])
            ms = (time.perf_counter_ns() - t) / 1e6
            require(reply is None, "%s accepted: %r" % (label, reply))
            block_hash = dsha(bytes.fromhex(raw)[: header_len(raw)])[::-1].hex()
            require(
                node.rpc.call("getbestblockhash") == block_hash,
                "candidate became the tip",
            )
            result[label] = ms
        finally:
            node.stop()
    return result


def header_len(raw_hex):
    raw = bytes.fromhex(raw_hex[: 2 * 160])
    # 140-byte fixed header, then a CompactSize-prefixed Equihash solution.
    lead = raw[140]
    size, width = (
        (lead, 1) if lead < 253 else (int.from_bytes(raw[141:143], "little"), 3)
    )
    return 140 + width + size


def empty_regtest_block(template):
    """Serialize an unmodified NU5+ coinbase-only template; Regtest skips PoW."""
    require(template["transactions"] == [], "empty template required")
    roots = template["defaultroots"]
    header = struct.pack("<I", template["version"])
    for digest in (template["previousblockhash"], roots["merkleroot"],
                   roots["blockcommitmentshash"]):
        raw = bytes.fromhex(digest)
        require(len(raw) == 32, "32-byte template hash")
        header += raw[::-1]
    header += struct.pack("<II", template["curtime"], int(template["bits"], 16))
    header += bytes(32) + compact(36) + bytes(36)
    return (header + compact(1) + bytes.fromhex(template["coinbasetxn"]["data"])).hex()


def load_cold_candidates(specs, producer_binary, out):
    """specs: label=fixture.json:candidate.json. Adds `coinbase` (empty block) and `transparent`
    (32 P2SH spends) candidates built on the first fixture's chain by `producer_binary`.
    """
    cands = {}
    for spec in specs:
        label, paths = spec.split("=", 1)
        fixture, candidate = paths.split(":")
        chain = json.loads(Path(fixture).read_text())["chain"]
        raw = json.loads(Path(candidate).read_text())["raw"]
        require(
            bytes.fromhex(raw)[4:36][::-1].hex() == chain[-1]["hash"], label + " parent"
        )
        cands[label] = (chain, raw)
    # Coinbase-only and transparent candidates get their own chain: the fixtures' coinbases are
    # already spent, so build fresh mature outputs.
    base = Node(
        producer_binary, out / "producer-chain", env(), miner=P2SH_MINER
    ).start()
    try:
        hashes = base.rpc.call("generate", [101])
        src = coinbase_payout(base, hashes[0])
        value = (src[2] - FANOUT_FEE) // 64
        fan_raw, fan_txid = v4_tx([src], [value] * 64)
        admit(base, [(fan_raw, fan_txid)])
        hashes += base.rpc.call("generate", [1])
        chain = [{"hash": h, "raw": base.rpc.call("getblock", [h, 0])} for h in hashes]
    finally:
        base.stop()
    for label, spends in (("coinbase", 0), ("transparent", 32)):
        node = Node(
            producer_binary, out / ("producer-" + label), env(), miner=P2SH_MINER
        ).start()
        try:
            replay(node, chain)
            txs = [v4_tx([(fan_txid, i, value)], [value - FEE]) for i in range(spends)]
            if txs:
                admit(node, txs)
            block_hash = node.rpc.call("generate", [1])[0]
            raw = node.rpc.call("getblock", [block_hash, 0])
            require(
                len(node.rpc.call("getblock", [block_hash, 1])["tx"]) == spends + 1,
                label,
            )
            cands[label] = (chain, raw)
        finally:
            node.stop()
    return cands


SCENARIOS = {
    "gbci": scenario_gbci,
    "gossip": scenario_gossip,
    "tchain": scenario_tchain,
    "gbtsapling": scenario_gbtsapling,
    "coldsubmit": scenario_coldsubmit,
}


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, epilog=BENCH_HELP, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--arm", action="append", required=True, help="name=path/to/zebrad")
    ap.add_argument(
        "--scenario", action="append", choices=sorted(SCENARIOS), required=True
    )
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--output", type=Path, required=True)
    ap.add_argument("--seed", type=int, default=20261012)
    ap.add_argument("--gbci-calls", type=int, default=2000)
    ap.add_argument("--gossip-blocks", type=int, default=30)
    ap.add_argument("--gossip-warmup", type=int, default=3)
    ap.add_argument("--spacing", type=float, default=2.0)
    ap.add_argument("--tchain-utxos", type=int, default=2000)
    ap.add_argument("--gbt-txs", type=int, default=32)
    ap.add_argument("--gbt-blocks", type=int, default=20)
    ap.add_argument("--tchain-blocks", type=int, default=130)
    ap.add_argument("--tchain-txs", type=int, default=64)
    ap.add_argument(
        "--cold", action="append", default=[], help="label=fixture.json:candidate.json"
    )
    args = ap.parse_args()
    arms = dict(a.split("=", 1) for a in args.arm)
    args.output.mkdir(parents=True, exist_ok=False)
    if "coldsubmit" in args.scenario:
        producer = next(p for p in arms.values() if "zakura" not in Path(p).name)
        args.cold_candidates = load_cold_candidates(args.cold, producer, args.output)
        (args.output / "cold-candidates.json").write_text(
            json.dumps(
                {
                    k: {"parent": c[0][-1]["hash"], "raw": c[1]}
                    for k, c in args.cold_candidates.items()
                }
            )
        )
    rng = random.Random(args.seed)
    results = []
    for rnd in range(args.rounds):
        for sc in args.scenario:
            order = list(arms)
            rng.shuffle(order)
            for arm in order:
                case = args.output / ("r%d-%s-%s" % (rnd, sc, arm))
                t = time.time()
                try:
                    r = SCENARIOS[sc](arms[arm], case, args)
                    status = "ok"
                except Exception as e:
                    r, status = {"error": repr(e)}, "error"
                rec = {
                    "round": rnd,
                    "scenario": sc,
                    "arm": arm,
                    "status": status,
                    "seconds": time.time() - t,
                    "result": r,
                }
                results.append(rec)
                print(
                    json.dumps(
                        {
                            k: rec[k]
                            for k in ("round", "scenario", "arm", "status", "seconds")
                        }
                    ),
                    flush=True,
                )
                (args.output / "results.json").write_text(json.dumps(results, indent=1))
                if status == "ok":
                    for state in case.glob("n*/state"):
                        shutil.rmtree(state, ignore_errors=True)
    failed = [r for r in results if r["status"] != "ok"]
    if failed:
        raise SystemExit("%d failed cases" % len(failed))


if __name__ == "__main__":
    main()
