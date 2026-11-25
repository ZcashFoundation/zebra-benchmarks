"""Passive P2P announcements and an already-open getblocktemplate long poll."""

import hashlib
import os
import socket
import struct
import threading
import time

MAGIC = {"mainnet": bytes.fromhex("24e92764"),
         "testnet": bytes.fromhex("fa1af9bf"),
         "regtest": bytes.fromhex("aae83f5f")}
MAX_MESSAGE = 2 * 1024 * 1024


def _compact_size(data, offset=0):
    if offset >= len(data):
        raise ValueError("truncated CompactSize")
    first = data[offset]
    width = {253: 2, 254: 4, 255: 8}.get(first)
    if width is None:
        return first, offset + 1
    end = offset + width + 1
    if end > len(data):
        raise ValueError("truncated CompactSize")
    value = int.from_bytes(data[offset + 1:end], "little")
    if value < {2: 253, 4: 65536, 8: 4294967296}[width]:
        raise ValueError("noncanonical CompactSize")
    return value, end


def header_hash(data, offset=0):
    if len(data) - offset < 141:
        raise ValueError("truncated Zcash block header")
    size, end = _compact_size(data, offset + 140)
    end += size
    if end > len(data):
        raise ValueError("truncated Equihash solution")
    digest = hashlib.sha256(hashlib.sha256(data[offset:end]).digest()).digest()
    return digest[::-1].hex(), end


def _packet(magic, command, payload=b""):
    checksum = hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4]
    return magic + command.encode().ljust(12, b"\0") + struct.pack("<I", len(payload)) + checksum + payload


def _receive(sock, size, stop, deadline=None):
    data = bytearray()
    received_ns = None
    while len(data) < size:
        if stop.is_set():
            raise InterruptedError
        if deadline is not None and time.perf_counter() >= deadline:
            raise TimeoutError("P2P handshake timed out")
        try:
            chunk = sock.recv(size - len(data))
        except socket.timeout:
            continue
        received_ns = time.perf_counter_ns()
        if not chunk:
            raise ConnectionError("P2P peer closed connection")
        data.extend(chunk)
    return bytes(data), received_ns


def _messages(sock, magic, stop, ready):
    deadline = time.perf_counter() + 5
    while not stop.is_set():
        header, received_ns = _receive(sock, 24, stop, None if ready.is_set() else deadline)
        prefix, name, size, checksum = struct.unpack("<4s12sI4s", header)
        if prefix != magic or size > MAX_MESSAGE or not name.rstrip(b"\0").isalpha():
            raise ValueError("invalid P2P message header")
        if size:
            payload, received_ns = _receive(sock, size, stop, None if ready.is_set() else deadline)
        else:
            payload = b""
        if hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4] != checksum:
            raise ValueError("invalid P2P message checksum")
        yield name.rstrip(b"\0").decode("ascii"), payload, received_ns


def announcements(command, payload):
    if command == "inv":
        # ZIP 239 MSG_WTX (5) carries txid and auth digest, unlike one-hash inventory.
        count, offset = _compact_size(payload)
        if count > 50_000:
            raise ValueError("invalid block inventory")
        blocks = []
        for _ in range(count):
            if offset + 4 > len(payload):
                raise ValueError("invalid block inventory")
            kind = struct.unpack_from("<I", payload, offset)[0]
            end = offset + 4 + (64 if kind == 5 else 32)
            if end > len(payload):
                raise ValueError("invalid block inventory")
            if kind == 2:
                blocks.append(payload[offset + 4:end][::-1].hex())
            offset = end
        if offset != len(payload):
            raise ValueError("invalid block inventory")
        return blocks
    if command == "headers":
        count, offset = _compact_size(payload)
        if count > 2_000:
            raise ValueError("too many headers")
        hashes = []
        for _ in range(count):
            block_hash, offset = header_hash(payload, offset)
            tx_count, offset = _compact_size(payload, offset)
            if tx_count:
                raise ValueError("transactions in headers message")
            hashes.append(block_hash)
        if offset != len(payload):
            raise ValueError("trailing headers data")
        return hashes
    if command == "cmpctblock":
        return [header_hash(payload)[0]]
    return []


def _locator(payload):
    if len(payload) < 37:
        raise ValueError("short block locator")
    count, offset = _compact_size(payload, 4)
    if count > 101 or len(payload) != offset + count * 32 + 32:
        raise ValueError("invalid block locator")
    return {payload[i:i + 32][::-1].hex() for i in range(offset, offset + count * 32, 32)}


class Peer:
    """Observe gossip and answer locators as a passive same-tip peer."""

    def __init__(self, addr, network, tip_hash, tip_header):
        self.addr = addr
        self.magic = MAGIC[network]
        self.ready = threading.Event()
        self.observed = {}
        self.errors = []
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._thread = None
        self._socket = None
        self.set_tip(tip_hash, tip_header)

    def set_tip(self, tip_hash, tip_header):
        tip_header = bytes(tip_header)
        hash_from_header, end = header_hash(tip_header)
        if end != len(tip_header) or hash_from_header != tip_hash.lower():
            raise ValueError("tip hash does not match complete header")
        with self._lock:
            self._tip_hash = hash_from_header
            self._tip_header = tip_header
            self._parent_hash = tip_header[4:36][::-1].hex()

    def start(self):
        if self._thread is None:
            self._thread = threading.Thread(target=self._run, name="live-p2p", daemon=True)
            self._thread.start()

    def stop(self):
        self._stop.set()
        with self._lock:
            sock = self._socket
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        if self._thread is not None:
            self._thread.join(3)
        self.ready.clear()

    def _run(self):
        while not self._stop.is_set():
            try:
                host, port = self.addr.rsplit(":", 1)
                with socket.create_connection((host.strip("[]"), int(port)), timeout=2) as sock:
                    sock.settimeout(.5)
                    with self._lock:
                        self._socket = sock
                    self._connected(sock, int(port))
            except (OSError, ValueError, ConnectionError, TimeoutError, InterruptedError) as error:
                if not self._stop.is_set():
                    self.errors.append(str(error))
            finally:
                self.ready.clear()
                with self._lock:
                    self._socket = None
            self._stop.wait(1)

    def _connected(self, sock, port):
        incoming = _messages(sock, self.magic, self._stop, self.ready)
        first, version_data, _ = next(incoming)
        if first != "version" or len(version_data) < 4:
            raise ValueError("P2P peer did not send version")
        peer_version = struct.unpack_from("<I", version_data)[0]
        address = struct.pack("<Q16sH", 0, bytes(16), socket.htons(port))
        version = (struct.pack("<IQq", min(peer_version, 170190), 0, int(time.time()))
                   + address + struct.pack("<Q16sH", 0, bytes(16), 0)
                   + os.urandom(8) + b"\x0a/live-watch/" + struct.pack("<I", 0) + b"\0")
        sock.sendall(_packet(self.magic, "version", version))
        sock.sendall(_packet(self.magic, "verack"))
        requested = {}
        for command, payload, received_ns in incoming:
            if command == "verack":
                self.ready.set()
            elif command == "version":
                raise ValueError("duplicate P2P version")
            elif command == "ping" and len(payload) == 8:
                sock.sendall(_packet(self.magic, "pong", payload))
            elif not self.ready.is_set():
                continue
            elif command in ("getheaders", "getblocks"):
                known = _locator(payload)
                with self._lock:
                    tip_hash, parent_hash, tip_header = self._tip_hash, self._parent_hash, self._tip_header
                has_tip = tip_hash not in known and parent_hash in known
                if command == "getheaders":
                    reply = b"\x01" + tip_header + b"\0" if has_tip else b"\0"
                    sock.sendall(_packet(self.magic, "headers", reply))
                else:
                    reply = b"\x01" + struct.pack("<I", 2) + bytes.fromhex(tip_hash)[::-1] if has_tip else b"\0"
                    sock.sendall(_packet(self.magic, "inv", reply))
            elif command == "getdata":
                sock.sendall(_packet(self.magic, "notfound", payload))
            elif command == "notfound":
                for block_hash in announcements("inv", payload):
                    requested.pop(block_hash, None)
            elif command == "block":
                block_hash, offset = header_hash(payload)
                count, _ = _compact_size(payload, offset)
                if count and block_hash in requested:
                    requested.pop(block_hash)
                    self.observed[block_hash]["peer_has_ns"] = received_ns
            else:
                for block_hash in announcements(command, payload):
                    entry = self.observed.setdefault(block_hash, {"peer_told_ns": received_ns, "peer_has_ns": None})
                    if block_hash not in requested and entry["peer_has_ns"] is None:
                        requested[block_hash] = None
                        if len(requested) > 64:
                            requested.pop(next(iter(requested)))
                        sock.sendall(_packet(self.magic, "getdata", b"\x01" + struct.pack("<I", 2)
                                             + bytes.fromhex(block_hash)[::-1]))


class TemplateWatch:
    """Keep one outstanding GBT long poll on a dedicated RPC connection."""

    def __init__(self, rpc_factory, on_error=None):
        self._rpc_factory = rpc_factory
        self._on_error = on_error
        self.ready = threading.Event()
        self.observed = {}
        self.errors = []
        self._stop = threading.Event()
        self._changed = threading.Condition()
        self._thread = None
        self._rpc = None
        self._pending = None
        self._serial = 0

    def start(self):
        if self._thread is None:
            self._thread = threading.Thread(target=self._run, name="live-gbt", daemon=True)
            self._thread.start()

    def stop(self):
        self._stop.set()
        with self._changed:
            self.ready.clear()
            rpc = self._rpc
            self._changed.notify_all()
        if rpc is not None:
            rpc.cancel_longpoll()
        if self._thread is not None:
            self._thread.join(3)

    def prepare(self, parent_hash, timeout):
        deadline = time.perf_counter() + timeout
        with self._changed:
            while not self._stop.is_set():
                left = deadline - time.perf_counter()
                if left <= 0:
                    break
                if self._pending != parent_hash.lower() or not self.ready.is_set():
                    self._changed.wait(left)
                    continue
                serial = self._serial
                self._changed.wait(min(.1, left))
                if self._pending == parent_hash.lower() and self._serial == serial and self.ready.is_set():
                    return
        raise TimeoutError(f"no outstanding long poll on parent {parent_hash}: {self.errors[-3:]}")

    def _sent(self):
        with self._changed:
            self.ready.set()
            self._changed.notify_all()

    def _run(self):
        last_error = None
        while not self._stop.is_set():
            rpc = None
            try:
                rpc = self._rpc_factory()
                with self._changed:
                    self._rpc = rpc
                template = rpc.call("getblocktemplate")
                while not self._stop.is_set():
                    parent = template["previousblockhash"].lower()
                    longpoll_id = template["longpollid"]
                    with self._changed:
                        self._serial += 1
                        self._pending = parent
                        self._changed.notify_all()
                    try:
                        template = rpc.longpoll(longpoll_id, self._sent)
                        received_ns = time.perf_counter_ns()
                    finally:
                        with self._changed:
                            self._pending = None
                            self.ready.clear()
                            self._changed.notify_all()
                    new_parent = template["previousblockhash"].lower()
                    if new_parent != parent:
                        self.observed.setdefault(new_parent, received_ns)
            except Exception as error:
                if not self._stop.is_set():
                    message = str(error)
                    self.errors.append(message)
                    del self.errors[:-3]
                    if self._on_error is not None and message != last_error:
                        self._on_error(message)
                    last_error = message
            finally:
                with self._changed:
                    self._rpc = None
                    self._pending = None
                    self.ready.clear()
                    self._changed.notify_all()
            self._stop.wait(1)
