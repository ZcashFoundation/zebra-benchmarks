"""Mirror a source mempool and measure remote submitblock paths from one clock."""
import argparse
import base64
import http.client
import json
from pathlib import Path
import queue
import statistics
import threading
import time
import tomllib
from urllib.parse import urlsplit

from node_bench import header_len
from .observer import Peer, TemplateWatch


class RPC:
    def __init__(self, url, auth=None, cookie_file=None, timeout=120):
        parsed = urlsplit(url if "://" in url else "http://" + url)
        if parsed.scheme not in ("http", "https") or not parsed.hostname or not parsed.port:
            raise ValueError("RPC must be an http(s) URL with an explicit host and port")
        self.host, self.port, self.path = parsed.hostname, parsed.port, parsed.path or "/"
        self.connection_type = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
        self.auth, self.cookie_file, self.timeout = auth, cookie_file, timeout
        self.conn = None
        self.request_id = 0

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def encode(self, method, params=None):
        self.request_id += 1
        return json.dumps({"jsonrpc": "2.0", "id": self.request_id,
                           "method": method, "params": params or []}).encode()

    def call(self, method, params=None, on_sent=None, body=None):
        """`body` is a request from `encode`, so large submits can be built before a barrier."""
        if self.conn is None:
            self.conn = self.connection_type(self.host, self.port, timeout=self.timeout)
        if body is None:
            body = self.encode(method, params)
        auth = Path(self.cookie_file).read_text().strip() if self.cookie_file else self.auth
        headers = {"Content-Type": "application/json"}
        if auth:
            headers["Authorization"] = "Basic " + base64.b64encode(auth.encode()).decode()
        try:
            self.conn.request("POST", self.path, body, headers)
            if on_sent is not None:
                on_sent()
            response = self.conn.getresponse()
            payload = json.loads(response.read())
            if response.status != 200 or payload.get("error"):
                raise RuntimeError(f"{method}: HTTP {response.status}: {payload.get('error') or payload}")
            return payload["result"]
        except Exception:
            self.close()
            raise


def rpc_for(config, timeout=120):
    return RPC(config["rpc"], config.get("auth"), config.get("cookie_file"), timeout)


def new_txids(previous, current):
    """Reappearing txids count as new after an absence in the source pool."""
    return set(current) - set(previous)


def row_for(block, node, before, started_ns, started_wall_ns, stamps, result, errors):
    row = dict(round=block["round"], block_hash=block["hash"], height=block["height"],
               bytes=block["bytes"], tx_count=block["tx_count"], node=node,
               main_mempool_size=before["main_mempool_size"],
               node_mempool_size=before["node_mempool_size"], rpc_round_trip_ms=before["rpc_round_trip_ms"],
               submit_start_ns=started_ns, submit_wall_time_ns=started_wall_ns,
               submit_result=result, errors=errors, measured=True)
    for milestone, stamp in stamps.items():
        row[milestone + "_ms"] = (stamp - started_ns) / 1e6 if stamp is not None and stamp >= started_ns else None
    return row


def _write_line(file, value):
    file.write(json.dumps(value, allow_nan=False) + "\n")
    file.flush()


def _mirror(main, nodes, file, interval, stop):
    channels = {node["label"]: queue.Queue(maxsize=10000) for node in nodes}
    status = {node["label"]: {"pending": set(), "accepted": set()} for node in nodes}
    file_lock = threading.Lock()
    def sender(node):
        rpc = rpc_for(node)
        channel = channels[node["label"]]
        try:
            while not stop.is_set() or not channel.empty():
                try:
                    txid, raw = channel.get(timeout=0.1)
                except queue.Empty:
                    continue
                start = time.perf_counter_ns()
                error = None
                try:
                    accepted = rpc.call("sendrawtransaction", [raw]) == txid
                    if not accepted:
                        error = "sendrawtransaction returned a different txid"
                except Exception as exc:
                    accepted, error = False, str(exc)
                with file_lock:
                    status[node["label"]]["pending"].discard(txid)
                    if accepted:
                        status[node["label"]]["accepted"].add(txid)
                    _write_line(file, dict(node=node["label"], txid=txid, accepted=accepted,
                                           error=error, latency_ms=(time.perf_counter_ns() - start) / 1e6))
                channel.task_done()
        finally:
            rpc.close()

    workers = [threading.Thread(target=sender, args=(node,), daemon=True) for node in nodes]
    for worker in workers:
        worker.start()
    rpc = rpc_for(main)
    raws = {}
    try:
        while not stop.is_set():
            try:
                current = set(rpc.call("getrawmempool"))
                for txid in sorted(new_txids(raws, current)):
                    try:
                        raws[txid] = rpc.call("getrawtransaction", [txid, 0])
                    except Exception as exc:
                        with file_lock:
                            _write_line(file, dict(txid=txid, error=f"source fetch: {exc}", accepted=False))
                for txid in raws.keys() - current:
                    del raws[txid]
                with file_lock:
                    for node in nodes:
                        label = node["label"]
                        state = status[label]
                        state["accepted"].intersection_update(current)
                        state["pending"].intersection_update(current)
                        for txid, raw in raws.items():
                            if txid in state["accepted"] or txid in state["pending"]:
                                continue
                            try:
                                channels[label].put_nowait((txid, raw))
                                state["pending"].add(txid)
                            except queue.Full:
                                _write_line(file, dict(node=label, txid=txid,
                                                       error="sender queue full", accepted=False, latency_ms=None))
            except Exception as exc:
                with file_lock:
                    _write_line(file, dict(error=f"mirror poll: {exc}"))
            stop.wait(interval)
    finally:
        rpc.close()
        for worker in workers:
            worker.join(timeout=5)


def _tip_watch(config, target, start, timeout, observed, errors):
    rpc = rpc_for(config, timeout=min(timeout, 5))
    try:
        deadline = time.monotonic() + timeout
        start.wait()
        while time.monotonic() < deadline:
            try:
                matched = rpc.call("getbestblockhash") == target
                received_ns = time.perf_counter_ns()
                if matched:
                    observed["tip"] = received_ns
                    return
            except Exception as exc:
                errors.append("tip poll: " + str(exc))
            time.sleep(0.001)
        errors.append("tip poll: timed out")
    finally:
        rpc.close()


def _ancestors(main_rpc, parent, tip, tip_height):
    """Return missing blocks in chain order, or reject a fork at the node's height."""
    missing = []
    current = parent
    while current != tip:
        detail = main_rpc.call("getblock", [current, 1])
        if detail["height"] <= tip_height or not detail.get("previousblockhash"):
            raise RuntimeError(f"node tip {tip} is not an ancestor of {parent}")
        missing.append(detail)
        current = detail["previousblockhash"]
    return list(reversed(missing))


def _round_trip(rpc):
    values = []
    for _ in range(10):
        started = time.perf_counter_ns()
        rpc.call("getblockcount")
        values.append((time.perf_counter_ns() - started) / 1e6)
    return statistics.median(values)


def _measure(block, raw, nodes, main_rpc, file, timeout):
    parent = block["previousblockhash"]
    main_pool = main_rpc.call("getmempoolinfo")["size"]
    prepared = {}
    for state in nodes:
        node, rpc, peer, template = state["config"], state["rpc"], state["peer"], state["template"]
        errors = []
        try:
            tip = rpc.call("getbestblockhash")
            tip_height = rpc.call("getblock", [tip, 1])["height"]
            for ancestor in _ancestors(main_rpc, parent, tip, tip_height):
                ancestor_raw = main_rpc.call("getblock", [ancestor["hash"], 0])
                ancestor_start = time.perf_counter_ns()
                ancestor_wall = time.time_ns()
                answer = rpc.call("submitblock", [ancestor_raw])
                _write_line(file, dict(round=block["round"], block_hash=ancestor["hash"],
                                       node=node["label"], measured=False, submit_result=answer,
                                       submit_start_ns=ancestor_start, submit_wall_time_ns=ancestor_wall))
                if answer is not None:
                    raise RuntimeError(f"ancestor {ancestor['hash']} rejected: {answer}")
            raw_parent = main_rpc.call("getblock", [parent, 0])
            peer.set_tip(parent, bytes.fromhex(raw_parent[:header_len(raw_parent) * 2]))
            if not peer.ready.wait(timeout):
                raise TimeoutError("P2P handshake not ready")
            template.prepare(parent, timeout)
            before = dict(main_mempool_size=main_pool, node_mempool_size=rpc.call("getmempoolinfo")["size"],
                          rpc_round_trip_ms=_round_trip(rpc))
            prepared[node["label"]] = (state, before, errors)
        except Exception as exc:
            errors.append("pre-submit: " + str(exc))
            _write_line(file, dict(round=block["round"], block_hash=block["hash"],
                                   height=block["height"], bytes=block["bytes"], tx_count=block["tx_count"],
                                   node=node["label"], measured=True, main_mempool_size=main_pool,
                                   node_mempool_size=None, rpc_round_trip_ms=None,
                                   submit_start_ns=None, submit_wall_time_ns=None,
                                   submit_result=None, errors=errors, submit_return_ms=None,
                                   tip_ms=None, template_ms=None, peer_told_ms=None, peer_has_ms=None))
    barrier = threading.Barrier(len(prepared) + 1)
    results = {}

    def submit(label, state, before, errors):
        rpc = state["rpc"]
        # Encoding a 2 MB block under the GIL takes milliseconds; do it before the barrier so all
        # nodes are sent the block at the same moment.
        body = rpc.encode("submitblock", [raw])
        start = threading.Event()
        tip_observed = {}
        tip_thread = threading.Thread(target=_tip_watch,
                                      args=(state["config"], block["hash"], start, timeout,
                                            tip_observed, errors), daemon=True)
        tip_thread.start()
        barrier.wait()
        started_ns = time.perf_counter_ns()
        wall_ns = time.time_ns()
        start.set()
        answer = None
        stamps = {key: None for key in ("submit_return", "tip", "template", "peer_told", "peer_has")}
        try:
            answer = rpc.call("submitblock", body=body)
            if answer is not None:
                errors.append("submitblock rejected: " + str(answer))
        except Exception as exc:
            errors.append("submitblock: " + str(exc))
        stamps["submit_return"] = time.perf_counter_ns()
        tip_thread.join(timeout + 1)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            peer_seen = state["peer"].observed.get(block["hash"], {})
            # The observer adds `peer_has_ns: None` on announcement; wait for the block itself.
            if block["hash"] in state["template"].observed and peer_seen.get("peer_has_ns") is not None:
                break
            time.sleep(0.005)
        stamps["tip"] = tip_observed.get("tip")
        stamps["template"] = state["template"].observed.get(block["hash"])
        peer_seen = state["peer"].observed.get(block["hash"], {})
        stamps["peer_told"] = peer_seen.get("peer_told_ns")
        stamps["peer_has"] = peer_seen.get("peer_has_ns")
        errors.extend(state["peer"].errors)
        errors.extend(state["template"].errors)
        results[label] = row_for(block, label, before, started_ns, wall_ns, stamps, answer, errors)

    threads = [threading.Thread(target=submit, args=(label, *prepared[label]), daemon=True)
               for label in prepared]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join()
    for state in nodes:
        if state["config"]["label"] in results:
            _write_line(file, results[state["config"]["label"]])
    candidate_header = bytes.fromhex(raw[:header_len(raw) * 2])
    for state in nodes:
        row = results.get(state["config"]["label"])
        if row and row["submit_result"] is None and row["tip_ms"] is not None:
            state["peer"].set_tip(block["hash"], candidate_header)




def run(config_path, output_dir, stop_event=None):
    config = tomllib.loads(Path(config_path).read_text())
    nodes = config["node"]
    if not nodes or len({n["label"] for n in nodes}) != len(nodes):
        raise ValueError("need measured nodes with unique labels")
    if any(n["network"].lower() not in ("mainnet", "regtest") for n in nodes):
        raise ValueError("network must be mainnet or regtest")
    if any(n["rpc"] == config["main"]["rpc"] for n in nodes):
        raise ValueError("measured RPC address must differ from main RPC")
    if len({n["p2p"] for n in nodes}) != len(nodes):
        raise ValueError("measured P2P addresses must be distinct")
    stop = stop_event or threading.Event()
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    main = rpc_for(config["main"])
    states = []
    try:
        initial = main.call("getbestblockhash")
        for node in nodes:
            rpc = rpc_for(node)
            tip = rpc.call("getbestblockhash")
            header = bytes.fromhex(main.call("getblock", [tip, 0]))
            peer = Peer(node["p2p"], node["network"], tip, header[:header_len(header.hex())])
            template = TemplateWatch(lambda n=node: rpc_for(n))
            states.append(dict(config=node, rpc=rpc, peer=peer, template=template))
        for state in states:
            state["peer"].start()
            state["template"].start()
        with (output / "rows.jsonl").open("a") as rows, (output / "mirror.jsonl").open("a") as mirror:
            mirror_thread = threading.Thread(target=_mirror,
                                             args=(config["main"], nodes, mirror,
                                                   config.get("mempool_interval_ms", 250) / 1000, stop), daemon=True)
            mirror_thread.start()
            if not all(state["peer"].ready.wait(timeout=30) for state in states):
                raise TimeoutError("P2P observers did not handshake")
            for state in states:
                deadline = time.monotonic() + 30
                while True:
                    peers = state["rpc"].call("getpeerinfo")
                    if len(peers) == 1 and peers[0].get("inbound") is True:
                        break
                    if peers:
                        raise RuntimeError(f"{state['config']['label']} has a non-observer P2P connection")
                    if time.monotonic() >= deadline:
                        raise TimeoutError(f"{state['config']['label']} did not register the P2P observer")
                    time.sleep(0.02)
            (output / "ready").touch()
            last = initial
            feed_interval = config.get("block_interval_ms", 20) / 1000
            timeout = config.get("timeout_seconds", 120)
            try:
                while not stop.is_set():
                    try:
                        current = main.call("getbestblockhash")
                        if current != last:
                            info = main.call("getblock", [current, 1])
                            info["round"] = config.get("round", 0)
                            raw = main.call("getblock", [info["hash"], 0])
                            block = dict(info, bytes=len(raw) // 2, tx_count=len(info["tx"]))
                            _measure(block, raw, states, main, rows, timeout)
                            last = current
                    except Exception as exc:
                        _write_line(rows, dict(feed_error=str(exc), wall_time_ns=time.time_ns()))
                    stop.wait(feed_interval)
            finally:
                stop.set()
                mirror_thread.join(timeout=10)
    finally:
        (output / "ready").unlink(missing_ok=True)
        stop.set()
        for state in states:
            state["template"].stop()
            state["peer"].stop()
            state["rpc"].close()
        main.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=Path)
    parser.add_argument("--output", type=Path, default=Path("shadow-output"))
    args = parser.parse_args()
    run(args.config, args.output)


if __name__ == "__main__":
    main()
