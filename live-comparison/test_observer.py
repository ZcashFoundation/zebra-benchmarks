"""Loopback protocol and timing checks for the passive observers."""

import hashlib
import queue
import socket
import struct
import threading
import time
import unittest

from observer import MAGIC, MAX_OBSERVED, Peer, TemplateWatch, _packet


def header(parent="0" * 64):
    raw = struct.pack("<I", 4) + bytes.fromhex(parent)[::-1] + bytes(104) + b"\0"
    block_hash = hashlib.sha256(hashlib.sha256(raw).digest()).digest()[::-1].hex()
    return block_hash, raw


def receive(sock, length):
    data = b""
    while len(data) < length:
        part = sock.recv(length - len(data))
        if not part:
            raise ConnectionError("peer disconnected")
        data += part
    return data


def message(sock):
    magic, name, size, checksum = struct.unpack("<4s12sI4s", receive(sock, 24))
    payload = receive(sock, size)
    assert hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4] == checksum
    return magic, name.rstrip(b"\0"), payload


def eventually(predicate, timeout=3):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(.005)
    raise AssertionError("observer did not reach expected state")


def handshake(sock, peer):
    magic = MAGIC["regtest"]
    sock.sendall(_packet(magic, "version", struct.pack("<I", 170190)))
    got_magic, command, version = message(sock)
    assert got_magic == magic and command == b"version"
    assert version[80:93] == b"\x0c/live-watch/"
    assert message(sock)[1:] == (b"verack", b"")
    sock.sendall(_packet(magic, "verack"))
    assert peer.ready.wait(1)


class PeerTests(unittest.TestCase):
    def test_handshake_replies_and_block_seen(self):
        parent, parent_header = header()
        block_hash, block_header = header(parent)
        peer = Peer("127.0.0.1:1", "regtest", parent, parent_header)
        server, client = socket.socketpair()
        client.settimeout(1)
        server.settimeout(.1)
        failures = []

        def connected():
            try:
                peer._connected(server, 1)
            except (ConnectionError, OSError, InterruptedError) as error:
                if not peer._stop.is_set():
                    failures.append(error)

        thread = threading.Thread(target=connected)
        thread.start()
        try:
            handshake(client, peer)
            peer.errors.append("stale disconnect")
            client.sendall(_packet(peer.magic, "getaddr"))
            self.assertEqual(message(client)[1:], (b"addr", b"\0"))
            client.sendall(_packet(peer.magic, "mempool"))
            self.assertEqual(message(client)[1:], (b"inv", b"\0"))
            locator = struct.pack("<I", 170190) + b"\x01" + bytes.fromhex("0" * 64) + bytes(32)
            client.sendall(_packet(peer.magic, "getheaders", locator))
            self.assertEqual(message(client)[1:], (b"headers", b"\x01" + parent_header + b"\0"))
            client.sendall(_packet(peer.magic, "getblocks", locator))
            self.assertEqual(message(client)[1:], (b"inv", b"\0"))
            for index in range(MAX_OBSERVED):
                peer.observed[f"{index:064x}"] = {"peer_told_ns": 0, "peer_has_ns": None}
            client.sendall(_packet(peer.magic, "inv", b"\x01" + struct.pack("<I", 2)
                                       + bytes.fromhex(block_hash)[::-1]))
            self.assertEqual(message(client)[1:], (b"getdata", b"\x01" + struct.pack("<I", 2)
                                                    + bytes.fromhex(block_hash)[::-1]))
            self.assertEqual(len(peer.observed), MAX_OBSERVED)
            self.assertNotIn(f"{0:064x}", peer.observed)
            client.sendall(_packet(peer.magic, "block", block_header + b"\x01"))
            eventually(lambda: peer.observed[block_hash]["peer_has_ns"] is not None)
            self.assertGreaterEqual(peer.observed[block_hash]["peer_has_ns"],
                                    peer.observed[block_hash]["peer_told_ns"])
        finally:
            peer._stop.set()
            client.close()
            server.close()
            thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(failures, [])

    def test_reconnect_clears_errors(self):
        block_hash, block_header = header()
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen(2)
            listener.settimeout(3)
            peer = Peer(f"127.0.0.1:{listener.getsockname()[1]}", "regtest", block_hash,
                        block_header)
            peer.start()
            try:
                with listener.accept()[0] as client:
                    client.settimeout(1)
                    handshake(client, peer)
                eventually(lambda: bool(peer.errors) and not peer.ready.is_set())
                with listener.accept()[0] as client:
                    client.settimeout(1)
                    handshake(client, peer)
                    self.assertEqual(peer.errors, [])
            finally:
                peer.stop()
        self.assertFalse(peer._thread.is_alive())


class FakeRPC:
    def __init__(self, parent):
        self.parent = parent
        self.responses = queue.Queue()
        self.sent = queue.Queue()
        self.received = threading.Event()
        self.response_ns = None
        self.parsed_ns = None

    def call(self, method):
        assert method == "getblocktemplate"
        return {"previousblockhash": self.parent, "longpollid": "first"}

    def longpoll(self, longpoll_id, on_sent, on_response):
        on_sent()
        self.sent.put(longpoll_id)
        template, release = self.responses.get(timeout=3)
        if template is None:
            raise InterruptedError
        on_response()
        self.response_ns = time.perf_counter_ns()
        self.received.set()
        assert release.wait(2), "fake response body was not released"
        self.parsed_ns = time.perf_counter_ns()
        return template

    def cancel_longpoll(self):
        self.responses.put((None, None))


class TemplateWatchTests(unittest.TestCase):
    def test_previous_hash_response_clock_and_rearm(self):
        old, _ = header()
        new, _ = header(old)
        rpc = FakeRPC(old)
        watch = TemplateWatch(lambda: rpc)
        watch.errors.append("prior failure")
        watch.start()
        try:
            self.assertEqual(rpc.sent.get(timeout=1), "first")
            watch.prepare(old, 0)
            self.assertEqual(watch.errors, [])
            same = threading.Event()
            same.set()
            rpc.responses.put(({"previousblockhash": old.upper(), "longpollid": "second"}, same))
            self.assertEqual(rpc.sent.get(timeout=1), "second")
            self.assertEqual(watch.observed, {})
            for index in range(MAX_OBSERVED):
                watch.observed[f"{index:064x}"] = index
            release = threading.Event()
            rpc.received.clear()
            rpc.responses.put(({"previousblockhash": new.upper(), "longpollid": "third"}, release))
            self.assertTrue(rpc.received.wait(1))
            self.assertNotIn(new, watch.observed)
            release.set()
            eventually(lambda: new in watch.observed)
            self.assertLessEqual(watch.observed[new], rpc.response_ns)
            self.assertLess(watch.observed[new], rpc.parsed_ns)
            self.assertEqual(len(watch.observed), MAX_OBSERVED)
            self.assertNotIn(f"{0:064x}", watch.observed)
        finally:
            watch.stop()
        self.assertFalse(watch._thread.is_alive())


if __name__ == "__main__":
    unittest.main()
