#!/usr/bin/env python3
"""Feed the same Mainnet or Testnet blocks to isolated nodes; append JSONL observations."""

import argparse
import base64
from collections import defaultdict
from dataclasses import dataclass, field
import heapq
import http.client
import json
import math
import os
from pathlib import Path
import queue
import signal
import socket
import statistics
import sys
import threading
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request

from observer import Peer, TemplateWatch, header_hash
MAX_BLOCK_BYTES = 2_000_000
MAX_RESPONSE_BYTES = 8_000_000
MAX_MIRROR_TXIDS = 50_000
MAX_MIRROR_HEX = 256 * 1024 * 1024
MIRROR_QUEUE_SIZE = 10_000
# Bounds a stalled long-poll response; TemplateWatch re-arms after it expires. Longer than all but
# about 0.03% of 75-second block intervals.
LONGPOLL_TIMEOUT_SECONDS = 600


def now_ms():
    return time.time_ns() // 1_000_000


class RPCError(Exception):
    def __init__(self, error):
        self.code = error.get("code") if isinstance(error, dict) else None
        super().__init__(str(error))


class RPC:
    def __init__(self, config, timeout=10, response_limit=MAX_RESPONSE_BYTES):
        self.config = config
        self._longpoll = None
        self.timeout = timeout
        self.response_limit = response_limit

    def headers(self):
        headers = {"Content-Type": "application/json"}
        auth = self.config.get("auth")
        if self.config.get("cookie"):
            auth = Path(self.config["cookie"]).read_text().strip()
        if auth:
            headers["Authorization"] = "Basic " + base64.b64encode(auth.encode()).decode()
        return headers

    def call(self, method, params=None, timeout=None):
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method,
                           "params": params or []}, separators=(",", ":")).encode()
        return self.request(body, timeout)

    def request(self, body, timeout=None):
        request = urllib.request.Request(self.config["url"], data=body, headers=self.headers())
        try:
            response = urllib.request.urlopen(request, timeout=timeout or self.timeout)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            return self.parse(response, self.response_limit)

    @staticmethod
    def parse(response, limit=MAX_RESPONSE_BYTES):
        raw = response.read(limit + 1)
        if len(raw) > limit:
            raise ValueError("RPC response exceeds size limit")
        result = json.loads(raw)
        if not isinstance(result, dict) or result.get("id") != 1:
            raise ValueError("invalid JSON-RPC response envelope")
        if result.get("error") is not None:
            raise RPCError(result["error"])
        if response.status != 200 or "result" not in result:
            raise ValueError(f"invalid JSON-RPC response (HTTP {response.status})")
        return result["result"]

    def cancel_longpoll(self):
        connection = self._longpoll
        sock = connection.sock if connection is not None else None
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def longpoll(self, longpoll_id, on_sent, on_response):
        """Signal when the long poll is sent and when its response headers arrive."""
        url = urllib.parse.urlsplit(self.config["url"])
        connection = (http.client.HTTPSConnection if url.scheme == "https" else
                      http.client.HTTPConnection)(url.hostname, url.port, timeout=self.timeout)
        self._longpoll = connection
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "getblocktemplate",
                           "params": [{"longpollid": longpoll_id}]}).encode()
        try:
            # Connect within the RPC timeout, then wait for the response up to the long-poll bound.
            connection.connect()
            connection.sock.settimeout(LONGPOLL_TIMEOUT_SECONDS)
            connection.sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            connection.request("POST", url.path or "/", body, self.headers())
            on_sent()
            response = connection.getresponse()
            on_response()
            return self.parse(response, self.response_limit)
        finally:
            self._longpoll = None
            connection.close()


def outcome(result=None, error=None):
    if error is not None:
        reason = error.reason if isinstance(error, urllib.error.URLError) else error
        if isinstance(reason, (TimeoutError, socket.timeout)):
            return "timeout", str(error)
        if isinstance(error, RPCError):
            if error.code == -27:
                return "duplicate", str(error)
            if error.code in (-25, -26):
                return "rejected", str(error)
        return "error", str(error)
    if result is None:
        return "accepted", None
    if result in ("duplicate", "duplicate-inconclusive"):
        return "duplicate", result
    if result == "inconclusive":
        return "error", result
    if isinstance(result, str):
        return "rejected", result
    return "error", f"unexpected submitblock result: {result!r}"


def new_txids(previous, current):
    return set(current) - set(previous)

def parent_first(pool):
    """Topologically order verbose getrawmempool entries, parents before children."""
    if not isinstance(pool, dict) or len(pool) > MAX_MIRROR_TXIDS:
        raise ValueError("invalid or oversized feeder mempool")
    waiting = {}
    children = defaultdict(list)
    for txid, entry in pool.items():
        depends = entry.get("depends") if isinstance(entry, dict) else None
        if not isinstance(txid, str) or not isinstance(depends, list) or any(
                not isinstance(parent, str) for parent in depends):
            raise ValueError("invalid feeder mempool dependencies")
        parents = set(depends) & pool.keys()
        waiting[txid] = len(parents)
        for parent in parents:
            children[parent].append(txid)
    ready = [txid for txid, count in waiting.items() if not count]
    heapq.heapify(ready)
    ordered = []
    while ready:
        parent = heapq.heappop(ready)
        ordered.append(parent)
        for child in children[parent]:
            waiting[child] -= 1
            if not waiting[child]:
                heapq.heappush(ready, child)
    if len(ordered) != len(pool):
        raise ValueError("cyclic feeder mempool dependencies")
    return ordered


def submit_paths(block, node, dispatch, started, stamps, label, detail, rpc_round_trip_ms):
    return {"event": "submit_paths", "hash": block.hash, "height": block.height,
            "block_bytes": len(block.raw) // 2, "target": node["name"],
            "client": node["client"], "version": node["version"],
            "queued_unix_ms": block.queued, "dispatch_unix_ms": dispatch,
            "observed_at_unix_ms": now_ms(), "outcome": label, "detail": detail,
            "catch_up": False, "rpc_round_trip_ms": rpc_round_trip_ms,
            **{name + "_ms": (stamp - started) / 1_000_000 if stamp is not None and stamp >= started
               else None for name, stamp in stamps.items()}}


def rpc_round_trip(rpc):
    values = []
    for _ in range(10):
        started = time.perf_counter_ns()
        rpc.call("getblockcount")
        values.append((time.perf_counter_ns() - started) / 1_000_000)
    return statistics.median(values)

def load_config(path):
    path = Path(path)
    with path.open("rb") as source:
        config = json.load(source) if path.suffix == ".json" else tomllib.load(source)
    if not isinstance(config, dict) or not isinstance(config.get("feeder"), dict):
        raise ValueError("config requires a feeder object")
    for key, default in (("poll_interval_seconds", .02), ("rpc_timeout_seconds", 10),
                         ("sample_interval_seconds", 30), ("template_timeout_seconds", 5),
                         ("pause_threshold_seconds", 2)):
        value = config.setdefault(key, default)
        if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
            raise ValueError(f"{key} must be finite and positive")
    if type(config.setdefault("mirror_mempool", False)) is not bool:
        raise ValueError("mirror_mempool must be boolean")
    if type(config.setdefault("measure_submit_paths", False)) is not bool:
        raise ValueError("measure_submit_paths must be boolean")
    poll = config.setdefault("mempool_poll_seconds", .25)
    if type(poll) not in (int, float) or not math.isfinite(poll) or poll <= 0:
        raise ValueError("mempool_poll_seconds must be finite and positive")
    if config.setdefault("network", "mainnet") not in ("mainnet", "testnet", "regtest"):
        raise ValueError("network must be mainnet, testnet, or regtest")
    contestants = config.get("contestants", [])
    if not contestants or not isinstance(contestants, list):
        raise ValueError("at least one contestant is required")
    names = []
    for node in [config["feeder"], *contestants]:
        if not isinstance(node, dict) or not isinstance(node.get("url"), str):
            raise ValueError("each RPC endpoint requires a URL string")
        if any(key in node and not isinstance(node[key], str) for key in ("cookie", "auth")):
            raise ValueError("cookie and auth must be strings")
        url = urllib.parse.urlsplit(node["url"])
        if url.scheme not in ("http", "https") or not url.hostname or url.username or url.password:
            raise ValueError("RPC URLs must be http(s), without embedded credentials")
        if node.get("cookie") and node.get("auth"):
            raise ValueError("configure cookie or auth, not both")
        if node.get("auth") and ":" not in node["auth"]:
            raise ValueError("auth must be username:password")
        if node.get("cookie"):
            cookie = Path(node["cookie"]).expanduser()
            node["cookie"] = str(cookie if cookie.is_absolute() else path.parent / cookie)
    for node in contestants:
        if not all(isinstance(node.get(key), str) and node[key]
                   for key in ("name", "client", "version")):
            raise ValueError("contestants require nonempty name, client, and version")
        if "pid" in node and (type(node["pid"]) is not int or node["pid"] <= 0):
            raise ValueError("pid must be a positive local process id")
        if "p2p" in node:
            address = node["p2p"]
            if not isinstance(address, str) or ":" not in address:
                raise ValueError("p2p must be host:port")
            host, port = address.rsplit(":", 1)
            if not host.strip("[]") or not port.isdecimal() or not 0 < int(port) < 65536:
                raise ValueError("p2p must be host:port")
        names.append(node["name"])
    if len(set(names)) != len(names):
        raise ValueError("contestant names must be unique")
    urls = [node["url"].rstrip("/") for node in [config["feeder"], *contestants]]
    if len(set(urls)) != len(urls):
        raise ValueError("feeder and contestants must use distinct RPC endpoints")
    addresses = [node["p2p"] for node in contestants if "p2p" in node]
    if len(set(addresses)) != len(addresses):
        raise ValueError("contestant P2P addresses must be distinct")
    return config


class JSONL:
    def __init__(self, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.file = Path(path).open("a", encoding="utf-8", buffering=1)
        self.lock = threading.Lock()

    def record(self, record):
        line = json.dumps(record, separators=(",", ":"), allow_nan=False)
        with self.lock:
            self.file.write(line + "\n")
            self.file.flush()

    def close(self):
        self.file.close()


@dataclass
class Block:
    hash: str
    height: int
    previous: str
    raw: str
    queued: float
    catch_up: bool
    generation: int = 0
    starts: dict = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def __post_init__(self):
        self.body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "submitblock",
                                "params": [self.raw]}, separators=(",", ":")).encode()


def fetch_block(rpc, block_hash, catch_up, queued=None):
    header = rpc.call("getblockheader", [block_hash, True])
    raw = rpc.call("getblock", [block_hash, 0])
    if not isinstance(raw, str) or len(raw) > MAX_BLOCK_BYTES * 2 or len(raw) % 2:
        raise ValueError("invalid raw block length")
    data = bytes.fromhex(raw)
    if len(data) < 141 or len(data) * 2 != len(raw):
        raise ValueError("invalid raw block hex")
    return Block(block_hash, header["height"], header.get("previousblockhash", "0" * 64),
                 raw, now_ms() if queued is None else queued, catch_up)


def process_sample(pid):
    root = Path("/proc") / str(pid)
    # comm may contain spaces or parentheses; fields after its final ')' start at field 3.
    fields = (root / "stat").read_text().rsplit(")", 1)[1].split()
    io = dict(line.split(":", 1) for line in (root / "io").read_text().splitlines())
    return {"at_unix_ms": now_ms(), "start_ticks": int(fields[19]),
            "cpu_seconds": (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK"),
            "rss_bytes": int(fields[21]) * os.sysconf("SC_PAGE_SIZE"),
            "read_bytes": int(io["read_bytes"]), "write_bytes": int(io["write_bytes"])}


class Dispatcher:
    def __init__(self, config, output, stop=None):
        self.config = config
        self.sink = JSONL(output)
        self.stop = stop or threading.Event()
        self.changed = threading.Condition()
        self.latest = None
        self.failure = None
        self.threads = []
        self.round_trips = {}
        self.feeder = RPC(config["feeder"], config["rpc_timeout_seconds"])

    def diagnostic(self, target, error):
        self.sink.record({"event": "diagnostic", "target": target,
                          "at_unix_ms": now_ms(), "detail": str(error)})

    def spawn(self, function, node):
        def guarded():
            try:
                function(node)
            except Exception as error:
                self.failure = error
                self.stop.set()
                with self.changed:
                    self.changed.notify_all()
        thread = threading.Thread(target=guarded, name=f"{function.__name__}-{node['name']}")
        thread.start()
        self.threads.append(thread)

    def tip_watch(self, node, block_hash, start, finished, observed, errors):
        rpc = RPC(node, min(self.config["rpc_timeout_seconds"], 2))
        start.wait()
        deadline = None
        reported = False
        while not self.stop.is_set():
            if finished.is_set() and deadline is None:
                deadline = observed["returned_ns"] + int(
                    self.config["template_timeout_seconds"] * 1_000_000_000)
            if deadline is not None and time.perf_counter_ns() >= deadline:
                break
            try:
                if rpc.call("getbestblockhash") == block_hash:
                    observed["tip"] = time.perf_counter_ns()
                    return
            except (OSError, ValueError, RPCError) as error:
                if not reported:
                    errors.append(f"tip poll: {error}")
                    reported = True
            self.stop.wait(.001)
        if not self.stop.is_set() and not reported:
            errors.append("tip poll: timed out")

    def observe_submit(self, node, block, dispatch, started, returned, label, detail, round_trip,
                       peer, template, peer_ready, template_ready, watcher, observed, errors):
        timeout = self.config["template_timeout_seconds"]
        if peer is not None and label == "accepted":
            try:
                raw = bytes.fromhex(block.raw)
                _, end = header_hash(raw)
                peer.set_tip(block.hash, raw[:end])
            except (ValueError, OSError) as error:
                errors.append(f"P2P observer tip: {error}")
        deadline = returned + int(timeout * 1_000_000_000)
        watcher.join(timeout + min(timeout, self.config["rpc_timeout_seconds"]))
        if template_ready or peer_ready:
            while not self.stop.is_set() and time.perf_counter_ns() < deadline:
                seen = peer.observed.get(block.hash, {}) if peer_ready else {}
                template_done = not template_ready or block.hash in template.observed
                peer_done = not peer_ready or seen.get("peer_has_ns") is not None
                if template_done and peer_done:
                    break
                self.stop.wait(.005)
        seen = peer.observed.get(block.hash, {}) if peer_ready else {}
        stamps = {"submit_return": returned, "tip": observed.get("tip"),
                  "template": template.observed.get(block.hash) if template_ready else None,
                  "peer_told": seen.get("peer_told_ns"), "peer_has": seen.get("peer_has_ns")}
        self.sink.record(submit_paths(block, node, dispatch, started, stamps, label, detail, round_trip))
        for diagnostic in errors:
            self.diagnostic(node["name"], diagnostic)
        if peer is not None and peer.errors:
            self.diagnostic(node["name"], f"P2P observer: {peer.errors[-1]}")

    def submit(self, rpc, node, block, catch_up, peer=None, template=None):
        measured = not catch_up and self.config.get("measure_submit_paths", False)
        if measured:
            errors = []
            peer_ready = bool(peer is not None and peer.ready.is_set())
            template_ready = False
            if template is not None:
                try:
                    template.prepare(block.previous, 0)
                    template_ready = True
                except TimeoutError as error:
                    errors.append(f"template long poll: {error}")
            start = threading.Event()
            finished = threading.Event()
            observed = {}
            round_trip = self.round_trips.get(node["name"])
            watcher = threading.Thread(target=self.tip_watch,
                                       args=(node, block.hash, start, finished, observed, errors), daemon=True)
            watcher.start()
        dispatch = now_ms()
        started = time.perf_counter_ns()
        if measured:
            start.set()
        result = None
        error = None
        try:
            result = rpc.request(block.body)
        except (OSError, ValueError, RPCError) as exc:
            error = exc
        returned = time.perf_counter_ns()
        if measured:
            observed["returned_ns"] = returned
            finished.set()
        latency = (returned - started) / 1_000_000
        label, detail = outcome(result, error)
        self.sink.record({"event": "submitblock_call", "hash": block.hash,
                          "height": block.height, "block_bytes": len(block.raw) // 2,
                          "raw_block_hex": block.raw, "target": node["name"],
                          "client": node["client"], "version": node["version"],
                          "dispatch_unix_ms": dispatch, "queued_unix_ms": block.queued,
                          "dispatch_delay_ms": dispatch - block.queued,
                          "observed_at_unix_ms": now_ms(), "latency_ms": latency,
                          "outcome": label, "detail": detail, "catch_up": catch_up})
        if not catch_up:
            with block.lock:
                block.starts[node["name"]] = dispatch
                if len(block.starts) == len(self.config["contestants"]):
                    skew = max(block.starts.values()) - min(block.starts.values())
                    self.sink.record({"event": "dispatch_skew", "hash": block.hash,
                                      "height": block.height, "dispatch_skew_ms": skew,
                                      "at_unix_ms": now_ms()})
            if measured:
                monitor = threading.Thread(target=self.observe_submit,
                                           args=(node, block, dispatch, started, returned, label, detail,
                                                 round_trip, peer, template, peer_ready, template_ready,
                                                 watcher, observed, errors),
                                           daemon=True)
                monitor.start()
                self.threads.append(monitor)
        return label, detail

    def catch_up(self, rpc, node, tip, block):
        ancestor = rpc.call("getblockheader", [tip, True])
        height = ancestor["height"]
        while not self.stop.is_set():
            if height <= block.height and self.feeder.call("getblockhash", [height]) == tip:
                break
            if height == 0:
                raise ValueError("contestant and feeder have different genesis blocks")
            tip = ancestor["previousblockhash"]
            ancestor = rpc.call("getblockheader", [tip, True])
            height = ancestor["height"]
        for height in range(height + 1, block.height + 1):
            if self.stop.is_set():
                return False
            block_hash = self.feeder.call("getblockhash", [height])
            missing = block if block_hash == block.hash else fetch_block(self.feeder, block_hash, True)
            if missing.previous != tip or (height == block.height and block_hash != block.hash):
                raise ValueError("feeder reorganized during catch-up; resampling its tip")
            label, detail = self.submit(rpc, node, missing, True)
            if label not in ("accepted", "duplicate") or detail == "duplicate-inconclusive":
                return False
            tip = block_hash
        return not self.stop.is_set() and rpc.call("getbestblockhash") == block.hash

    def contestant(self, node):
        rpc = RPC(node, self.config["rpc_timeout_seconds"])
        peer = None
        template = None
        tip = None
        generation = -1
        synced = False
        try:
            while not self.stop.is_set():
                with self.changed:
                    self.changed.wait_for(lambda: self.stop.is_set() or
                                          (self.latest and self.latest.generation != generation), .5)
                    block = self.latest
                if self.stop.is_set():
                    break
                if block is None:
                    continue
                try:
                    live = (synced and tip == block.previous and not block.catch_up and
                            generation + 1 == block.generation)
                    generation = block.generation
                    if not live and self.config.get("measure_submit_paths", False):
                        if peer is None and "p2p" in node:
                            try:
                                tip_hash = rpc.call("getbestblockhash")
                                raw = bytes.fromhex(rpc.call("getblock", [tip_hash, 0]))
                                _, end = header_hash(raw)
                                peer = Peer(node["p2p"], self.config["network"], tip_hash, raw[:end])
                                peer.start()
                            except (OSError, ValueError, RPCError) as error:
                                self.diagnostic(node["name"], f"P2P observer: {error}")
                        if template is None:
                            template = TemplateWatch(
                                lambda: RPC(node, self.config["rpc_timeout_seconds"]),
                                lambda error: self.diagnostic(node["name"], f"template long poll: {error}"))
                            template.start()
                    if live:
                        label, detail = self.submit(rpc, node, block, False, peer, template)
                        synced = label == "accepted"
                        tip = block.hash if synced else None
                    else:
                        tip = rpc.call("getbestblockhash")
                        synced = tip == block.hash or self.catch_up(rpc, node, tip, block)
                        tip = block.hash if synced else None
                        if synced and peer is not None:
                            try:
                                raw = bytes.fromhex(block.raw)
                                _, end = header_hash(raw)
                                peer.set_tip(block.hash, raw[:end])
                            except (OSError, ValueError) as error:
                                self.diagnostic(node["name"], f"P2P observer tip: {error}")
                except (OSError, ValueError, RPCError, KeyError) as error:
                    synced, tip = False, None
                    self.diagnostic(node["name"], error)
                # Failed targets retry on the periodic wakeup, without blocking the feeder or peers.
        finally:
            if template is not None:
                template.stop()
            if peer is not None:
                peer.stop()

    def mirror(self, _):
        nodes = self.config["contestants"]
        channels = {node["name"]: queue.Queue(maxsize=MIRROR_QUEUE_SIZE) for node in nodes}
        status = {node["name"]: {"pending": set(), "accepted": set(), "failed": set()} for node in nodes}
        lock = threading.Lock()
        current = set()

        def sender(node):
            rpc = RPC(node, self.config["rpc_timeout_seconds"])
            channel = channels[node["name"]]
            while not self.stop.is_set():
                try:
                    txid, raw = channel.get(timeout=.1)
                except queue.Empty:
                    continue
                with lock:
                    active = txid in current
                if not active or self.stop.is_set():
                    with lock:
                        status[node["name"]]["pending"].discard(txid)
                    channel.task_done()
                    continue
                started = time.perf_counter_ns()
                dispatch = now_ms()
                try:
                    result = rpc.call("sendrawtransaction", [raw])
                    label, detail = ("accepted", None) if result == txid else (
                        "error", f"sendrawtransaction returned {result!r}, not {txid}")
                except RPCError as error:
                    label, detail = outcome(error=error)
                    if error.code == -27 or (error.code == -1 and
                                              "already exists in mempool" in str(error).lower()):
                        label = "accepted"
                except (OSError, ValueError) as error:
                    label, detail = outcome(error=error)
                with lock:
                    state = status[node["name"]]
                    state["pending"].discard(txid)
                    if txid in current:
                        state["accepted" if label == "accepted" else "failed"].add(txid)
                self.sink.record({"event": "mempool_relay", "txid": txid,
                                  "target": node["name"], "dispatch_unix_ms": dispatch,
                                  "observed_at_unix_ms": now_ms(), "outcome": label,
                                  "error": detail,
                                  "latency_ms": (time.perf_counter_ns() - started) / 1_000_000})
                channel.task_done()

        workers = [threading.Thread(target=sender, args=(node,), name=f"mirror-{node['name']}")
                   for node in nodes]
        for worker in workers:
            worker.start()
        source = RPC(self.config["feeder"], self.config["rpc_timeout_seconds"],
                     response_limit=32_000_000)
        raws = {}
        raw_size = 0
        previous_tip = None
        ordered = []
        try:
            while not self.stop.is_set():
                try:
                    pool = source.call("getrawmempool", [True])
                    tip = source.call("getbestblockhash")
                    if not isinstance(pool, dict) or len(pool) > MAX_MIRROR_TXIDS:
                        raise ValueError("invalid or oversized feeder mempool")
                    present = set(pool)
                    if present != current:
                        ordered = parent_first(pool)
                    with lock:
                        if previous_tip is not None and tip != previous_tip:
                            for state in status.values():
                                state["failed"].clear()
                        previous_tip = tip
                        current = present
                        for state in status.values():
                            for category in state.values():
                                category.intersection_update(present)
                    for txid in raws.keys() - present:
                        raw_size -= len(raws.pop(txid))
                    for txid in ordered:
                        if txid in raws or all(txid in state["failed"] for state in status.values()):
                            continue
                        try:
                            raw = source.call("getrawtransaction", [txid, 0])
                            if not isinstance(raw, str) or raw_size + len(raw) > MAX_MIRROR_HEX:
                                raise ValueError("mirror raw transaction cache exceeds size limit")
                            raws[txid] = raw
                            raw_size += len(raw)
                        except (OSError, ValueError, RPCError) as error:
                            with lock:
                                for node in nodes:
                                    label = node["name"]
                                    if txid in status[label]["failed"]:
                                        continue
                                    status[label]["failed"].add(txid)
                                    self.sink.record({"event": "mempool_relay", "txid": txid,
                                                      "target": label, "observed_at_unix_ms": now_ms(),
                                                      "outcome": "error", "error": f"source fetch: {error}",
                                                      "latency_ms": None})
                    with lock:
                        for node in nodes:
                            label = node["name"]
                            state = status[label]
                            for txid in ordered:
                                if (txid not in raws or any(txid in state[key]
                                                            for key in ("accepted", "pending", "failed"))):
                                    continue
                                try:
                                    channels[label].put_nowait((txid, raws[txid]))
                                    state["pending"].add(txid)
                                except queue.Full:
                                    break  # The rest stays eligible for the next poll, in order.
                except (OSError, ValueError, RPCError) as error:
                    self.diagnostic("feeder mempool", error)
                self.stop.wait(self.config["mempool_poll_seconds"])
        finally:
            for worker in workers:
                worker.join()

    def sampler(self, node):
        rpc = RPC(node, self.config["rpc_timeout_seconds"])
        previous = None
        interval = self.config["sample_interval_seconds"]
        while not self.stop.is_set():
            started = time.monotonic()
            if self.config.get("measure_submit_paths", False):
                try:
                    self.round_trips[node["name"]] = rpc_round_trip(rpc)
                except (OSError, ValueError, RPCError) as error:
                    self.round_trips.pop(node["name"], None)
                    self.diagnostic(node["name"], f"RPC round trip: {error}")
            if "pid" in node:
                current = None
                detail = None
                try:
                    current = process_sample(node["pid"])
                except (OSError, ValueError, KeyError, IndexError) as error:
                    detail = str(error)
                valid = bool(previous and current and
                             current["start_ticks"] == previous["start_ticks"])
                deltas = {key: current[key] - previous[key]
                          for key in ("cpu_seconds", "read_bytes", "write_bytes")} if valid else {}
                valid = valid and all(value >= 0 for value in deltas.values())
                end = current["at_unix_ms"] if current else now_ms()
                start = previous["at_unix_ms"] if previous else end
                if start >= end:
                    valid = False
                    detail = detail or "no positive wall-clock interval"
                self.sink.record({"event": "resource_interval", "target": node["name"],
                                  "start_unix_ms": start, "end_unix_ms": end,
                                  "rss_bytes": current["rss_bytes"] if current else None,
                                  "deltas": deltas if valid else {}, "valid": valid,
                                  "detail": detail or (None if valid else "no matching prior sample")})
                previous = current
            record = {"event": "getblocktemplate", "target": node["name"],
                      "at_unix_ms": now_ms(), "template_height": None,
                      "transaction_count": None, "detail": None}
            before = time.perf_counter_ns()
            try:
                template = rpc.call("getblocktemplate", [{}],
                                    self.config["template_timeout_seconds"])
                record.update(status="success", template_height=template["height"],
                              transaction_count=len(template["transactions"]))
            except (OSError, ValueError, RPCError, KeyError, TypeError) as error:
                label, detail = outcome(error=error)
                record.update(status="rpc_error" if isinstance(error, RPCError) else label,
                              detail=detail)
            record["latency_ms"] = (time.perf_counter_ns() - before) / 1_000_000
            self.sink.record(record)
            self.stop.wait(max(0, interval - (time.monotonic() - started)))

    def run(self, duration=None):
        start = time.monotonic()
        self.sink.record({"event": "session", "at_unix_ms": now_ms(), "mode": "live_fed",
                          "synthetic": self.config.get("synthetic", False),
                          "clients": {node["name"]: {"name": node["client"],
                                                   "version": node["version"]}
                                      for node in self.config["contestants"]}})
        last_poll = None
        failed = True
        try:
            if self.config.get("mirror_mempool"):
                self.spawn(self.mirror, {"name": "feeder"})
            for node in self.config["contestants"]:
                self.spawn(self.contestant, node)
                self.spawn(self.sampler, node)
            while not self.stop.is_set():
                before = time.monotonic()
                if duration is not None and before - start >= duration:
                    break
                try:
                    block_hash = self.feeder.call("getbestblockhash")
                    observed = now_ms()
                    paused = last_poll is None or observed - last_poll > (
                        self.config["pause_threshold_seconds"] * 1000)
                    if self.latest is None or block_hash != self.latest.hash:
                        block = fetch_block(self.feeder, block_hash, failed or paused, observed)
                        with self.changed:
                            block.generation = self.latest.generation + 1 if self.latest else 0
                            self.latest = block
                            self.changed.notify_all()
                    last_poll, failed = observed, False
                except (OSError, ValueError, RPCError, KeyError) as error:
                    failed = True
                    self.diagnostic("feeder", error)
                    self.stop.wait(1)
                self.stop.wait(max(0, self.config["poll_interval_seconds"] -
                                   (time.monotonic() - before)))
        finally:
            self.stop.set()
            with self.changed:
                self.changed.notify_all()
            for thread in self.threads:
                thread.join()
            self.sink.close()
        if self.failure:
            raise self.failure


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=Path)
    parser.add_argument("--output", default="records.jsonl", type=Path)
    parser.add_argument("--duration", type=float, help="stop after this many seconds")
    args = parser.parse_args()
    if args.duration is not None and (not math.isfinite(args.duration) or args.duration <= 0):
        parser.error("duration must be finite and positive")
    stop = threading.Event()
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda *_: stop.set())
    try:
        Dispatcher(load_config(args.config), args.output, stop).run(args.duration)
    except (OSError, ValueError, RPCError, KeyError) as error:
        print(f"dispatch: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
