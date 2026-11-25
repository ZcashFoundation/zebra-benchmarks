#!/usr/bin/env python3
"""Feed the same Mainnet or Testnet blocks to isolated nodes; append JSONL observations."""

import argparse
import base64
from dataclasses import dataclass, field
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


def now_ms():
    return time.time_ns() // 1_000_000


class RPCError(Exception):
    def __init__(self, error):
        self.code = error.get("code") if isinstance(error, dict) else None
        super().__init__(str(error))


class RPC:
    def __init__(self, config, timeout=10):
        self.config = config
        self._longpoll = None
        self.timeout = timeout

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
            return self.parse(response)

    @staticmethod
    def parse(response):
        raw = response.read(MAX_RESPONSE_BYTES + 1)
        if len(raw) > MAX_RESPONSE_BYTES:
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
        if connection is not None and connection.sock is not None:
            try:
                connection.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


    def longpoll(self, longpoll_id, on_sent):
        """Signal only after the dedicated RPC connection has sent its long poll."""
        url = urllib.parse.urlsplit(self.config["url"])
        connection = (http.client.HTTPSConnection if url.scheme == "https" else
                      http.client.HTTPConnection)(url.hostname, url.port, timeout=30)
        self._longpoll = connection
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "getblocktemplate",
                           "params": [{"longpollid": longpoll_id}]}).encode()
        try:
            connection.request("POST", url.path or "/", body, self.headers())
            on_sent()
            return self.parse(connection.getresponse())
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

    def tip_watch(self, node, block_hash, start, observed, errors):
        rpc = RPC(node, self.config["rpc_timeout_seconds"])
        start.wait()
        deadline = time.monotonic() + self.config["template_timeout_seconds"]
        while not self.stop.is_set() and time.monotonic() < deadline:
            try:
                if rpc.call("getbestblockhash") == block_hash:
                    observed["tip"] = time.perf_counter_ns()
                    return
            except (OSError, ValueError, RPCError) as error:
                errors.append(f"tip poll: {error}")
            self.stop.wait(.001)
        if not self.stop.is_set():
            errors.append("tip poll: timed out")

    def submit(self, rpc, node, block, catch_up, peer=None, template=None):
        timeout = self.config["template_timeout_seconds"]
        errors = []
        round_trip = None
        peer_ready = False
        template_ready = False
        if not catch_up:
            try:
                round_trip = rpc_round_trip(rpc)
            except (OSError, ValueError, RPCError) as error:
                errors.append(f"RPC round trip: {error}")
            if peer is not None:
                peer_ready = peer.ready.wait(timeout)
                if not peer_ready:
                    errors.append(f"P2P observer not ready: {peer.errors[-3:]}")
            if template is not None:
                if template.errors and not template.ready.is_set():
                    errors.append(f"template long poll: {template.errors[-1]}")
                else:
                    try:
                        template.prepare(block.previous, timeout)
                        template_ready = True
                    except TimeoutError as error:
                        errors.append(f"template long poll: {error}")
        start = threading.Event()
        observed = {}
        watcher = None
        if not catch_up:
            watcher = threading.Thread(target=self.tip_watch,
                                       args=(node, block.hash, start, observed, errors), daemon=True)
            watcher.start()
        dispatch = now_ms()
        started = time.perf_counter_ns()
        start.set()
        result = None
        error = None
        try:
            result = rpc.request(block.body)
        except (OSError, ValueError, RPCError) as exc:
            error = exc
        returned = time.perf_counter_ns()
        latency = (returned - started) / 1_000_000
        label, detail = outcome(result, error)
        if peer is not None and label == "accepted":
            raw = bytes.fromhex(block.raw)
            _, end = header_hash(raw)
            peer.set_tip(block.hash, raw[:end])
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
            watcher.join(timeout + .1)
            deadline = time.monotonic() + timeout
            if template_ready or peer_ready:
                while not self.stop.is_set() and time.monotonic() < deadline:
                    seen = peer.observed.get(block.hash, {}) if peer is not None else {}
                    template_done = not template_ready or block.hash in template.observed
                    peer_done = not peer_ready or seen.get("peer_has_ns") is not None
                    if template_done and peer_done:
                        break
                    self.stop.wait(.005)
            seen = peer.observed.get(block.hash, {}) if peer is not None else {}
            stamps = {"submit_return": returned, "tip": observed.get("tip"),
                      "template": template.observed.get(block.hash) if template else None,
                      "peer_told": seen.get("peer_told_ns"), "peer_has": seen.get("peer_has_ns")}
            self.sink.record(submit_paths(block, node, dispatch, started, stamps,
                                          label, detail, round_trip))
            for diagnostic in errors:
                self.diagnostic(node["name"], diagnostic)
            if peer is not None and peer.errors:
                self.diagnostic(node["name"], f"P2P observer: {peer.errors[-1]}")
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
        template = TemplateWatch(lambda: RPC(node, self.config["rpc_timeout_seconds"]),
                                 lambda error: self.diagnostic(node["name"], f"template long poll: {error}"))
        try:
            if "p2p" in node:
                tip_hash = rpc.call("getbestblockhash")
                raw = bytes.fromhex(rpc.call("getblock", [tip_hash, 0]))
                _, end = header_hash(raw)
                peer = Peer(node["p2p"], self.config["network"], tip_hash, raw[:end])
                peer.start()
            template.start()
            tip = None
            generation = -1
            synced = False
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
                    if live:
                        label, detail = self.submit(rpc, node, block, False, peer, template)
                        synced = label == "accepted"
                        tip = block.hash if synced else None
                    else:
                        tip = rpc.call("getbestblockhash")
                        synced = tip == block.hash or self.catch_up(rpc, node, tip, block)
                        tip = block.hash if synced else None
                        if synced and peer is not None:
                            raw = bytes.fromhex(block.raw)
                            _, end = header_hash(raw)
                            peer.set_tip(block.hash, raw[:end])
                except (OSError, ValueError, RPCError, KeyError) as error:
                    synced, tip = False, None
                    self.diagnostic(node["name"], error)
                # Failed targets retry on the periodic wakeup, without blocking the feeder or peers.
        finally:
            template.stop()
            if peer is not None:
                peer.stop()

    def mirror(self, _):
        nodes = self.config["contestants"]
        channels = {node["name"]: queue.Queue(maxsize=10_000) for node in nodes}
        status = {node["name"]: {"pending": set(), "accepted": set()} for node in nodes}
        lock = threading.Lock()
        current = set()

        def sender(node):
            rpc = RPC(node, self.config["rpc_timeout_seconds"])
            channel = channels[node["name"]]
            while not self.stop.is_set() or not channel.empty():
                try:
                    txid, raw = channel.get(timeout=.1)
                except queue.Empty:
                    continue
                with lock:
                    active = txid in current
                if not active:
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
                except (OSError, ValueError, RPCError) as error:
                    label, detail = outcome(error=error)
                with lock:
                    state = status[node["name"]]
                    state["pending"].discard(txid)
                    if label == "accepted" and txid in current:
                        state["accepted"].add(txid)
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
        raws = {}
        try:
            while not self.stop.is_set():
                try:
                    pool = set(self.feeder.call("getrawmempool"))
                    for txid in sorted(new_txids(raws, pool)):
                        try:
                            raws[txid] = self.feeder.call("getrawtransaction", [txid, 0])
                        except (OSError, ValueError, RPCError) as error:
                            for node in nodes:
                                self.sink.record({"event": "mempool_relay", "txid": txid,
                                                  "target": node["name"],
                                                  "observed_at_unix_ms": now_ms(),
                                                  "outcome": "error",
                                                  "error": f"source fetch: {error}",
                                                  "latency_ms": None})
                    for txid in raws.keys() - pool:
                        del raws[txid]
                    with lock:
                        current = pool
                        for node in nodes:
                            label = node["name"]
                            state = status[label]
                            state["accepted"].intersection_update(pool)
                            state["pending"].intersection_update(pool)
                            for txid, raw in raws.items():
                                if txid in state["accepted"] or txid in state["pending"]:
                                    continue
                                try:
                                    channels[label].put_nowait((txid, raw))
                                    state["pending"].add(txid)
                                except queue.Full:
                                    self.sink.record({"event": "mempool_relay", "txid": txid,
                                                      "target": label, "observed_at_unix_ms": now_ms(),
                                                      "outcome": "error", "error": "sender queue full",
                                                      "latency_ms": None})
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
