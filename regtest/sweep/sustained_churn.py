"""Steady independent signed admissions with concurrent served-work/RPC observers."""
from concurrent.futures import ThreadPoolExecutor
import math
from pathlib import Path
import statistics
import threading
import time

from common import MEMPOOL_TX_COST_LIMIT, h, require
from protocol import fixture as protocol_fixture

POLL_SECONDS = 0.1
ADMISSION_WORKERS = 16


def distribution(values):
    """Median and nearest-rank tail percentiles; censored/empty stays explicit."""
    values = sorted(values)
    return dict(median=statistics.median(values) if values else None,
                **{key: (values[max(0, math.ceil(q * len(values)) - 1)] if values else None)
                   for key, q in (("p90", 0.9), ("p99", 0.99), ("max", 1))})


def select_corpus(fixture, rate, seconds):
    count = math.ceil(rate * seconds)
    corpus = fixture["corpus"][:count]
    require(len(corpus) == count, f"sustained-churn needs {count} independent signed transactions; fixture has {len(corpus)}")
    require(all(tx["kind"] == "transparent-signed" for tx in corpus), "sustained-churn requires signed transparent fixtures")
    require(sum(max(10_000, tx["bytes"]) for tx in corpus) <= MEMPOOL_TX_COST_LIMIT,
            "sustained-churn corpus fits configured mempool cost limit")
    # Everything must fit one served template: otherwise freshness would be censored by capacity.
    require(sum(tx["bytes"] for tx in corpus) + 10_000 < 2_000_000,
            "sustained-churn corpus must fit a single block, including coinbase overhead")
    inputs, txids = set(), set()
    for tx in corpus:
        decoded, end = protocol_fixture.parse_v4(bytes.fromhex(tx["raw"]))
        require(end == tx["bytes"] and tx["txid"] not in txids, "unique exact signed transactions")
        txids.add(tx["txid"])
        for inp in decoded["inputs"]:
            outpoint = (inp["txid"], inp["index"])
            require(outpoint not in inputs, "sustained-churn must not reuse inputs")
            inputs.add(outpoint)
    require(not any(txid in txids for txid, _ in inputs), "independent funded transactions, not unconfirmed chains")
    return corpus


def run(node, fixture, args, template_changed):
    corpus = select_corpus(fixture, args.churn_rate, args.churn_seconds)
    time.sleep(args.churn_settle_seconds)
    initial = node.rpc.call("getblocktemplate")
    require(initial["previousblockhash"] == fixture["parent"] and not initial["transactions"],
            "settled churn starts at funding tip with empty work")
    require(node.rpc.call("getrawmempool") == [], "settled churn starts with empty mempool")
    local = threading.local()
    clients, client_lock = [], threading.Lock()
    stop = threading.Event()
    first_seen, template_polls, cheap_polls = {}, [], []
    # Linux clear_refs=5 resets VmHWM so funding/replay cannot inflate the churn peak.
    Path(f"/proc/{node.proc.pid}/clear_refs").write_text("5\n")
    cpu_start = h.resources(node.proc.pid)
    started = time.perf_counter_ns()
    deadline = started + round(args.churn_seconds * 1e9)
    rebuilds = []

    def client():
        if not hasattr(local, "rpc"):
            local.rpc = h.RPC(node.rpc_addr, timeout=args.timeout)
            with client_lock:
                clients.append(local.rpc)
        return local.rpc

    def observe(method):
        previous, due = initial, started
        while not stop.is_set():
            if method == "getblockcount" and time.perf_counter_ns() >= deadline:
                return
            begin = time.perf_counter_ns()
            result = client().call(method)
            end = time.perf_counter_ns()
            sample = dict(start_ns=begin, end_ns=end, latency_ns=end - begin)
            if method == "getblocktemplate":
                require(result["previousblockhash"] == fixture["parent"], "churn template kept funding parent")
                for tx in result["transactions"]:
                    first_seen.setdefault(tx["hash"], end)
                if end <= deadline:
                    template_polls.append(sample)
                    if template_changed(previous, result, include_time=False):
                        rebuilds.append(end)
                    previous = result
            elif end <= deadline:
                cheap_polls.append(sample)
            due += round(POLL_SECONDS * 1e9)
            # Missed poll slots are not replayed as a burst.
            now = time.perf_counter_ns()
            if due < now:
                due = now + round(POLL_SECONDS * 1e9)
            stop.wait(max(0, (due - now) / 1e9))

    def send(tx, scheduled):
        begin = time.perf_counter_ns()
        row = dict(txid=tx["txid"], scheduled_ns=scheduled, start_ns=begin)
        try:
            require(begin < deadline, "admission worker missed churn window")
            reply = client().call("sendrawtransaction", [tx["raw"]])
            require(reply == tx["txid"], "admission returned expected txid")
            row["accepted"] = True
        except Exception as error:
            row.update(accepted=False, error=repr(error))
        row["end_ns"] = time.perf_counter_ns()
        return row

    futures, active, skipped = [], [], []
    try:
        with ThreadPoolExecutor(max_workers=ADMISSION_WORKERS) as admissions, \
                ThreadPoolExecutor(max_workers=2) as observers:
            watchers = [observers.submit(observe, method) for method in ("getblocktemplate", "getblockcount")]
            try:
                for index, tx in enumerate(corpus):
                    scheduled = started + round(index / args.churn_rate * 1e9)
                    time.sleep(max(0, (scheduled - time.perf_counter_ns()) / 1e9))
                    for watcher in watchers:
                        if watcher.done():
                            watcher.result()
                    now = time.perf_counter_ns()
                    # Bound queued work; overload is measured rather than an unbounded backlog.
                    active = [future for future in active if not future.done()]
                    if now >= deadline or len(active) >= 2 * ADMISSION_WORKERS:
                        skipped.append(dict(txid=tx["txid"], scheduled_ns=scheduled, error="admission scheduler capacity/deadline"))
                    else:
                        future = admissions.submit(send, tx, scheduled)
                        active.append(future)
                        futures.append(future)
                time.sleep(max(0, (deadline - time.perf_counter_ns()) / 1e9))
                ended = time.perf_counter_ns()
                cpu_end = h.resources(node.proc.pid)
                transactions = [future.result(timeout=args.timeout) for future in futures]
                accepted = [row for row in transactions if row["accepted"]]
                # Tail observations supply freshness for the final admissions, not churn cost.
                tail_deadline = time.monotonic() + min(60, args.timeout)
                while any(row["txid"] not in first_seen for row in accepted) and time.monotonic() < tail_deadline:
                    for watcher in watchers:
                        if watcher.done():
                            watcher.result()
                    time.sleep(POLL_SECONDS)
            finally:
                stop.set()
            for watcher in watchers:
                watcher.result(timeout=args.timeout)
    finally:
        stop.set()
        for rpc in clients:
            rpc.conn.close()
    for row in accepted:
        row["first_template_ns"] = first_seen.get(row["txid"])
        row["freshness_ns"] = (max(0, row["first_template_ns"] - row["end_ns"])
                               if row["first_template_ns"] is not None else None)
    in_window = [row for row in accepted if row["end_ns"] <= deadline]
    failures = [row for row in transactions if not row["accepted"]]
    wall_seconds = (ended - started) / 1e9
    require(template_polls and cheap_polls, "both concurrent churn RPC observers produced samples")
    return dict(
        start_ns=started, end_ns=ended, latency_ns=ended - started,
        churn_rate=args.churn_rate, churn_seconds=args.churn_seconds,
        settle_seconds=args.churn_settle_seconds, poll_interval_ms=100,
        scheduled_transactions=len(corpus), attempted_transactions=len(transactions),
        admitted_transactions=len(accepted), admitted_during_window=len(in_window),
        actual_admitted_rate=len(in_window) / wall_seconds,
        admission_failures=len(failures), generator_missed_admissions=len(skipped),
        admission_failure_details=failures, generator_missed_details=skipped,
        admission_start_lag_ns=distribution([row["start_ns"] - row["scheduled_ns"] for row in transactions]),
        transactions=transactions,
        freshness_ns=distribution([row["freshness_ns"] for row in accepted if row["freshness_ns"] is not None]),
        freshness_unobserved=sum(row["freshness_ns"] is None for row in accepted),
        template_rebuild_count=len(rebuilds), template_builds_per_second=len(rebuilds) / wall_seconds,
        template_rebuild_times_ns=rebuilds,
        gbt_latency_ns=distribution([row["latency_ns"] for row in template_polls]),
        cheap_rpc_latency_ns=distribution([row["latency_ns"] for row in cheap_polls]),
        gbt_polls=template_polls, cheap_rpc_polls=cheap_polls,
        cpu_seconds=cpu_end["cpu_s"] - cpu_start["cpu_s"],
        cpu_seconds_per_wall_second=(cpu_end["cpu_s"] - cpu_start["cpu_s"]) / wall_seconds,
        peak_rss_bytes=cpu_end["hwm"],
        preload=node.preload_receipt(started),
    )
