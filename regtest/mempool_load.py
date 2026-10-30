#!/usr/bin/env python3
"""Open-loop Regtest admission and concurrent template/resource measurements.

Each rate/arm/repetition replays the sealed funding chain into fresh state. Fixed
rates schedule arrivals independently of RPC completion; sender backlog is kept,
not dropped. Exact admission bounds span the last absent poll's request start to
the first present poll's response end (100 ms polls); RPC processing and transport
uncertainty are included. Ten-millisecond count polls measure aggregate progress,
never per-transaction latency. Throughput excludes the final five-second quiet
period. Template inclusion uses the first successful probe response containing
transactions[].hash, ordered by response end, not request start. Bounds span the
last absent probe completed before that request began to its response end, with
the lower bound at the absent request's start (or send start if later).
Admission-relative values use observed admission and can be negative when a
template response precedes the mempool poll. Probe spacing (default 500 ms) plus
RPC time limits resolution; failures/overlap widen bounds. Use --gbt-probe-ms 100
for a one-second throttle comparison, identically on every arm.
Smoke uses 16 transactions per selected kind, rates 10 and max, and one repetition;
funding replay is never truncated.

Run --self-test without binaries or fixtures. Campaigns require --provenance,
--arm LABEL=PATH (repeatable), --fixtures DIR, and a fresh --output DIR.
--no-miner omits the miner address and all template calls, including setup.
--gbt-probe-ms 0 disables only load probes, retaining the miner and setup template wait.
--node-env KEY=VALUE adds node-only environment overrides; Tokio/Rayon thread
counts stay fixed. Every variant retains the one-second RSS/HWM/CPU observations.
Startup Halo2 preload finishes before funding replay. Run receipts record preload
completion relative to the load start, outside the measured window.
"""

import argparse
from bisect import bisect_right
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import json
import math
from pathlib import Path
import queue
import re
import signal
import statistics
import threading
import time
import traceback

from relay.common import MEMPOOL_TX_COST_LIMIT, Node, h, load_fixtures, require
from front_common import Campaign, interleave, load_arms, summarize

NS = 1_000_000_000
STABLE_NS = 5 * NS
MEMPOOL_POLL_NS = 100_000_000
SIZE_POLL_NS = 10_000_000


def parse_rates(value):
    rates = value.split(",")
    if not rates or len(set(rates)) != len(rates):
        raise argparse.ArgumentTypeError("rates must be distinct positive integers or max")
    for rate in rates:
        if rate != "max" and (not rate.isdecimal() or not 0 < int(rate) <= NS):
            raise argparse.ArgumentTypeError("rates must be positive integers <= 1000000000 or max")
    return rates


def parse_gbt_probe_ms(value):
    interval = float(value)
    if not math.isfinite(interval) or (interval != 0 and interval < 0.001):
        raise argparse.ArgumentTypeError("gbt-probe-ms must be zero or at least 0.001")
    return interval


def parse_node_env(value):
    key, separator, setting = value.partition("=")
    if not separator or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key) or "\0" in setting:
        raise argparse.ArgumentTypeError("node environment must be KEY=VALUE without NUL bytes")
    if key in ("TOKIO_WORKER_THREADS", "RAYON_NUM_THREADS"):
        raise argparse.ArgumentTypeError("node environment cannot override " + key)
    return key, setting


def omit_miner_address(text):
    # The shared renderer formats None as a string rather than omitting the key.
    entry = 'miner_address = "None"\n'
    require(text.count(entry) == 1, "one rendered None miner address to omit")
    return text.replace(entry, "", 1)


def schedule(count, rate, start_ns):
    return [start_ns + (0 if rate == "max" else index * NS // int(rate))
            for index in range(count)]


def release(schedule_ns, put, wait, clock=time.perf_counter_ns):
    """wait returns true only on cancellation; put must not apply backpressure."""
    for index, due in enumerate(schedule_ns):
        if wait(max(0, due - clock()) / NS):
            return index
        put(index)
    return len(schedule_ns)


def quantiles(values, percentages):
    return {f"p{p}_ns": h.pct(values, p) if values else None for p in percentages}


def admission_bounds(send_start_ns, last_absent, first_present):
    lower = max(0, last_absent["start_ns"] - send_start_ns) if last_absent else 0
    upper = first_present["end_ns"] - send_start_ns
    return dict(latency_lower_ns=lower, latency_upper_ns=upper,
                last_absent_poll_start_ns=last_absent["start_ns"] if last_absent else None,
                last_absent_poll_end_ns=last_absent["end_ns"] if last_absent else None,
                first_present_poll_start_ns=first_present["start_ns"],
                first_present_poll_end_ns=first_present["end_ns"])


def observe_template(reply, row, first_seen):
    """Retain only each corpus txid's earliest completed positive probe."""
    if not (isinstance(reply, dict) and isinstance(reply.get("transactions"), list) and
            all(isinstance(tx, dict) and isinstance(tx.get("hash"), str)
                for tx in reply["transactions"])):
        row.update(status="error", error_message="invalid getblocktemplate response")
        return
    row["template_transactions"] = len(reply["transactions"])
    for tx in reply["transactions"]:
        txid = tx["hash"]
        if txid in first_seen:
            previous = first_seen[txid]
            if previous is None or row["end_ns"] < previous["end_ns"]:
                first_seen[txid] = row


def template_inclusions(admissions, probes, first_seen):
    """Bracket inclusion without retaining full templates or repeated txid lists."""
    successful = sorted((row for row in probes if row["status"] == "ok"),
                        key=lambda row: row["end_ns"])
    ends = [row["end_ns"] for row in successful]
    inclusions = []
    for admission in admissions:
        present = first_seen[admission["txid"]]
        if present is None:
            continue
        # All probes completed before this positive request began were absent.
        index = bisect_right(ends, present["start_ns"]) - 1
        absent = successful[index] if index >= 0 else None
        bounds = admission_bounds(admission["start_ns"], absent, present)
        observed_delay = admission["end_ns"] - admission["start_ns"]
        inclusions.append(dict(
            record="template_inclusion", txid=admission["txid"],
            start_ns=admission["start_ns"], end_ns=present["end_ns"],
            admission_observed_ns=admission["end_ns"],
            latency_ns=bounds["latency_upper_ns"],
            latency_lower_ns=bounds["latency_lower_ns"],
            latency_upper_ns=bounds["latency_upper_ns"],
            admission_latency_ns=bounds["latency_upper_ns"] - observed_delay,
            admission_latency_lower_ns=bounds["latency_lower_ns"] - observed_delay,
            admission_latency_upper_ns=bounds["latency_upper_ns"] - observed_delay,
            window_ns=bounds["latency_upper_ns"] - bounds["latency_lower_ns"],
            last_absent_probe_start_ns=bounds["last_absent_poll_start_ns"],
            last_absent_probe_end_ns=bounds["last_absent_poll_end_ns"],
            first_present_probe_start_ns=present["start_ns"],
            first_present_probe_end_ns=present["end_ns"]))
    return inclusions


def mempool_progress(size_samples, first_send_ns, accepted):
    """Count-only net growth; steady is the median of complete one-second windows."""
    result = {f"time_to_{p}pct_ns": None for p in (50, 90, 100)}
    result.update(peak_admission_rate_tx_s=None, steady_admission_rate_tx_s=None,
                  rate_window_ns=NS, steady_window_count=0)
    if first_send_ns is None or not accepted:
        return result
    points = [(first_send_ns, 0)]
    points.extend((row["end_ns"], row["mempool_size"]) for row in size_samples
                  if row["end_ns"] >= first_send_ns)
    for percentage in (50, 90, 100):
        target = (accepted * percentage + 99) // 100
        result[f"time_to_{percentage}pct_ns"] = next(
            (stamp - first_send_ns for stamp, size in points if size >= target), None)
    active_end = first_send_ns + result["time_to_100pct_ns"] if (
        result["time_to_100pct_ns"] is not None) else points[-1][0]
    left, peak = 0, 0
    for stamp, size in points:
        if stamp > active_end:
            break
        while left + 1 < len(points) and points[left + 1][0] <= stamp - NS:
            left += 1
        peak = max(peak, size - points[left][1])
    result["peak_admission_rate_tx_s"] = peak
    position, previous, rates = 0, 0, []
    for boundary in range(first_send_ns + NS, active_end + 1, NS):
        while position + 1 < len(points) and points[position + 1][0] <= boundary:
            position += 1
        size = points[position][1]
        rates.append(size - previous)
        previous = size
    result.update(steady_admission_rate_tx_s=statistics.median(rates) if rates else None,
                  steady_window_count=len(rates))
    return result


def aggregate(sends, admissions, inclusions, probes, samples, start_ns, end_ns, final_size, reason):
    attempted = [row for row in sends if row["start_ns"] is not None]
    statuses = Counter(row["status"] for row in sends)
    admitted_ids = {row["txid"] for row in admissions}
    active_end = max([start_ns, *(row["end_ns"] for row in attempted),
                      *(row["end_ns"] for row in admissions)])
    active_ns = active_end - start_ns
    rejection_messages = Counter(row["error_message"] for row in sends
                                 if row["status"] == "rejected")
    error_messages = Counter(row["error_message"] for row in sends
                             if row["status"] == "error")
    return dict(
        start_ns=start_ns, end_ns=end_ns, latency_ns=end_ns - start_ns,
        active_duration_ns=active_ns, stop_reason=reason,
        corpus_transactions=len(sends), submitted=len(attempted),
        accepted=len(admissions),
        accepted_tx_s=len(admissions) * NS / active_ns if active_ns else 0.0,
        rpc_success=statuses["ok"], rejected=statuses["rejected"],
        send_errors=statuses["error"], not_sent=statuses["not_sent"],
        admission_latency=quantiles([row["latency_ns"] for row in admissions], (50, 90, 99)),
        admission_latency_lower=quantiles([row["latency_lower_ns"] for row in admissions], (50, 90, 99)),
        admission_latency_upper=quantiles([row["latency_upper_ns"] for row in admissions], (50, 90, 99)),
        template_included=len(inclusions), template_never_seen=len(admissions) - len(inclusions),
        template_inclusion_latency=quantiles([row["latency_ns"] for row in inclusions], (50, 90, 99)),
        template_inclusion_latency_lower=quantiles(
            [row["latency_lower_ns"] for row in inclusions], (50, 90, 99)),
        template_inclusion_latency_upper=quantiles(
            [row["latency_upper_ns"] for row in inclusions], (50, 90, 99)),
        template_inclusion_after_admission=quantiles(
            [row["admission_latency_ns"] for row in inclusions], (50, 90, 99)),
        template_inclusion_after_admission_lower=quantiles(
            [row["admission_latency_lower_ns"] for row in inclusions], (50, 90, 99)),
        template_inclusion_after_admission_upper=quantiles(
            [row["admission_latency_upper_ns"] for row in inclusions], (50, 90, 99)),
        template_inclusion_window=quantiles([row["window_ns"] for row in inclusions], (50, 90, 99)),
        send_latency=quantiles([row["latency_ns"] for row in attempted], (50, 99)),
        schedule_lag=quantiles([row["schedule_lag_ns"] for row in attempted], (50, 99)),
        gbt_latency=quantiles([row["latency_ns"] for row in probes], (50, 99)),
        gbt_count=len(probes), gbt_errors=sum(row["status"] != "ok" for row in probes),
        peak_rss_bytes=max((row["rss"] for row in samples), default=None),
        process_hwm_bytes=max((row["hwm"] for row in samples), default=None),
        steady_rss_bytes=samples[-1]["rss"] if samples else None,
        cpu_s=samples[-1]["cpu_s"] - samples[0]["cpu_s"] if samples else None,
        final_mempool_size=final_size, rejections_by_message=dict(rejection_messages),
        errors_by_message=dict(error_messages),
        rpc_success_not_observed=sum(row["status"] == "ok" and row["txid"] not in admitted_ids
                                     for row in sends),
    )


def select_corpus(corpus, kind, smoke):
    groups = {name: [tx for tx in corpus if tx["kind"] == name]
              for name in (("transparent", "orchard") if kind == "mixed" else (kind,))}
    require(all(groups.values()), "fixture contains every selected transaction kind")
    if smoke:
        groups = {name: txs[:16] for name, txs in groups.items()}
    # Spread the smaller corpus through the larger one, preserving each input order.
    ordered = sorted((index / len(txs), name, tx)
                     for name, txs in groups.items() for index, tx in enumerate(txs))
    selected = [tx for _, _, tx in ordered]
    require(sum(max(10_000, tx["bytes"]) for tx in selected) <= MEMPOOL_TX_COST_LIMIT,
            "corpus ZIP-401 cost exceeds the relay campaign mempool limit")
    return selected


def timed_rpc(rpc, method, params, deadline):
    started = time.perf_counter_ns()
    timeout = max(0.001, min(rpc._load_timeout, (deadline - started) / NS))
    rpc.conn.timeout = timeout
    if rpc.conn.sock is not None:
        rpc.conn.sock.settimeout(timeout)
    reply = None
    row = dict(start_ns=started, status="ok")
    try:
        reply = rpc.call(method, params)
    except Exception as error:
        row.update(status="error", error_message=str(error), error_type=type(error).__name__)
        if isinstance(error, RuntimeError):
            try:
                detail = json.loads(str(error))
            except ValueError:
                detail = None
            if isinstance(detail, dict) and "code" in detail and "message" in detail:
                row.update(status="rpc_error", rpc_error=detail, error_message=detail["message"])
    row["end_ns"] = time.perf_counter_ns()
    row["latency_ns"] = row["end_ns"] - started
    return reply, row


def measure(node, corpus, rate, args, emit):
    start_ns = time.perf_counter_ns()
    deadline = start_ns + int(args.timeout * NS)
    arrivals = schedule(len(corpus), rate, start_ns)
    sends = [dict(txid=tx["txid"], kind=tx["kind"], bytes=tx["bytes"], index=index,
                  scheduled_ns=arrivals[index], start_ns=None, end_ns=None,
                  latency_ns=None, status="pending") for index, tx in enumerate(corpus)]
    by_txid = {row["txid"]: row for row in sends}
    first_template = dict.fromkeys(by_txid)
    tasks = queue.Queue(maxsize=len(corpus))
    stop, released = threading.Event(), threading.Event()
    lock = threading.Lock()
    counts = dict(finished=0, last_send_ns=start_ns, size_polls=0, size_poll_errors=0)
    admissions, probes, samples, size_samples, thread_errors = [], [], [], [], []
    seen, previous = set(), set()
    last_change = start_ns
    final_size, final_poll_ns, last_poll_ok = None, None, False
    poll_errors = 0
    last_successful_poll = None

    def rpc_client():
        rpc = h.RPC(node.rpc_addr, timeout=args.rpc_timeout)
        rpc._load_timeout = args.rpc_timeout
        return rpc

    def sender():
        rpc = rpc_client()
        try:
            while not stop.is_set():
                try:
                    index = tasks.get(timeout=0.05)
                except queue.Empty:
                    if released.is_set():
                        return
                    continue
                if stop.is_set():
                    return
                tx, row = corpus[index], sends[index]
                # Publish the dispatch timestamp before a poll can observe this txid.
                row["start_ns"] = time.perf_counter_ns()
                reply, timing = timed_rpc(rpc, "sendrawtransaction", [tx["raw"]], deadline)
                timing["latency_ns"] = timing["end_ns"] - row["start_ns"]
                timing.pop("start_ns")
                row.update(timing, reply=reply, schedule_lag_ns=row["start_ns"] - row["scheduled_ns"])
                if row["status"] == "rpc_error":
                    row["status"] = "rejected"
                elif row["status"] == "ok" and reply != tx["txid"]:
                    row.update(status="error", error_message="sendrawtransaction returned a different txid")
                emit(dict(row, record="send"))
                with lock:
                    counts["finished"] += 1
                    counts["last_send_ns"] = max(counts["last_send_ns"], row["end_ns"])
        finally:
            rpc.conn.close()

    def producer():
        try:
            release(arrivals, tasks.put_nowait, stop.wait)
        finally:
            released.set()

    def probe_once(due):
        rpc = rpc_client()
        try:
            reply, row = timed_rpc(rpc, "getblocktemplate", [], deadline)
            row.update(record="gbt", scheduled_ns=due)
            with lock:
                if row["status"] == "ok":
                    observe_template(reply, row, first_template)
                probes.append(row)
            emit(row)
        finally:
            rpc.conn.close()

    def probe():
        interval = int(args.gbt_probe_ms * 1_000_000)
        workers = max(1, math.ceil(args.rpc_timeout * NS / interval) + 1)
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="gbt") as executor:
            due = start_ns
            while due < deadline and not stop.wait(max(0, due - time.perf_counter_ns()) / NS):
                executor.submit(guarded, lambda due=due: probe_once(due))
                due += interval

    def poll_size():
        rpc = rpc_client()
        due, previous_size = start_ns, None
        try:
            while due < deadline and not stop.wait(max(0, due - time.perf_counter_ns()) / NS):
                reply, row = timed_rpc(rpc, "getmempoolinfo", [], deadline)
                counts["size_polls"] += 1
                row.update(record="mempool_size", scheduled_ns=due)
                size = reply.get("size") if isinstance(reply, dict) else None
                if row["status"] == "ok" and not (type(size) is int and 0 <= size <= len(corpus)):
                    row.update(status="error", error_message="invalid getmempoolinfo size")
                if row["status"] == "ok":
                    if size != previous_size:
                        row["mempool_size"] = size
                        size_samples.append(row)
                        emit(row)
                    previous_size = size
                else:
                    counts["size_poll_errors"] += 1
                    emit(row)
                due += max(1, (row["end_ns"] - due) // SIZE_POLL_NS + 1) * SIZE_POLL_NS
        finally:
            rpc.conn.close()

    def sample():
        due = start_ns
        while not stop.wait(max(0, due - time.perf_counter_ns()) / NS):
            started = time.perf_counter_ns()
            values = h.resources(node.proc.pid)
            row = dict(record="resources", start_ns=started, end_ns=time.perf_counter_ns(), **values)
            samples.append(row)
            emit(row)
            due += max(1, (row["end_ns"] - due) // NS + 1) * NS

    def guarded(function):
        try:
            function()
        except BaseException:
            with lock:
                thread_errors.append(traceback.format_exc())
            stop.set()

    workers = [threading.Thread(target=guarded, args=(function,), name=name)
               for name, function in [("arrivals", producer),
                                      *([] if args.no_miner or args.gbt_probe_ms == 0 else [("gbt", probe)]),
                                      ("resources", sample), ("mempool-size", poll_size),
                                      *[(f"sender-{i}", sender) for i in range(args.concurrency)]]]
    poll = rpc_client()
    reason = "timeout"
    try:
        for worker in workers:
            worker.start()
        due = start_ns
        while not stop.is_set():
            now = time.perf_counter_ns()
            if now >= deadline:
                break
            if node.proc.poll() is not None:
                reason = "node_exit"
                break
            if stop.wait(max(0, min(due, deadline) - now) / NS):
                break
            if time.perf_counter_ns() >= deadline:
                break
            reply, row = timed_rpc(poll, "getrawmempool", [], deadline)
            row.update(record="mempool_poll", scheduled_ns=due)
            if row["status"] == "ok" and not (isinstance(reply, list) and
                                                           all(isinstance(txid, str) for txid in reply)):
                row.update(status="error", error_message="invalid getrawmempool response")
            if row["status"] == "ok":
                current = set(reply)
                require(current <= by_txid.keys(), "unexpected transaction in isolated mempool")
                if current != previous or not last_poll_ok:
                    last_change = row["end_ns"]
                previous, final_size = current, len(current)
                final_poll_ns = row["end_ns"]
                row["mempool_size"] = final_size
                for txid in sorted(current - seen):
                    sent = by_txid[txid]
                    require(sent["start_ns"] is not None, "observed transaction was dispatched")
                    admission = dict(record="admission", txid=txid, kind=sent["kind"],
                                     start_ns=sent["start_ns"], end_ns=row["end_ns"],
                                     latency_ns=row["end_ns"] - sent["start_ns"],
                                     poll_start_ns=row["start_ns"],
                                     **admission_bounds(sent["start_ns"], last_successful_poll, row))
                    admissions.append(admission)
                    emit(admission)
                seen.update(current)
                last_poll_ok = True
                last_successful_poll = row
            else:
                poll_errors += 1
                last_poll_ok = False
            emit(row)
            with lock:
                finished, last_send = counts["finished"], counts["last_send_ns"]
            if (last_poll_ok and finished == len(corpus) and
                    row["end_ns"] - max(last_change, last_send) >= STABLE_NS):
                reason = "stable"
                break
            due += max(1, (row["end_ns"] - due) // MEMPOOL_POLL_NS + 1) * MEMPOOL_POLL_NS
    finally:
        stop.set()
        for worker in workers:
            if worker.ident is not None:
                worker.join()
        poll.conn.close()
        ended = time.perf_counter_ns()
        for row in sends:
            if row["status"] == "pending":
                if row["start_ns"] is None:
                    row.update(status="not_sent", end_ns=ended, error_message="load stopped before dispatch")
                else:
                    row.update(status="error", end_ns=ended, latency_ns=ended - row["start_ns"],
                               schedule_lag_ns=row["start_ns"] - row["scheduled_ns"],
                               error_message="sender interrupted during dispatch")
                emit(dict(row, record="send"))
    if thread_errors:
        reason = "worker_error"
    inclusions = template_inclusions(admissions, probes, first_template)
    for inclusion in inclusions:
        emit(inclusion)
    result = aggregate(sends, admissions, inclusions, probes, samples,
                       start_ns, ended, final_size, reason)
    first_send_ns = min((row["start_ns"] for row in sends if row["start_ns"] is not None), default=None)
    result["admission_progress"] = mempool_progress(size_samples, first_send_ns, len(admissions))
    result.update(record="run", poll_errors=poll_errors, worker_errors=thread_errors,
                  corpus_exhausted=counts["finished"] == len(corpus),
                  final_mempool_sample_ns=final_poll_ns,
                  mempool_poll_interval_ms=MEMPOOL_POLL_NS // 1_000_000, stable_seconds=5,
                  mempool_size_poll_interval_ms=SIZE_POLL_NS // 1_000_000,
                  size_polls=counts["size_polls"], size_poll_errors=counts["size_poll_errors"],
                  success=reason == "stable" and poll_errors == 0 and not result["gbt_errors"]
                          and not counts["size_poll_errors"] and not result["send_errors"]
                          and not result["not_sent"])
    result.update(node.preload_receipt(start_ns))
    return result


def self_test():
    origin = 123
    assert schedule(4, "10", origin) == [123, 100_000_123, 200_000_123, 300_000_123]
    assert schedule(4, "max", origin) == [origin] * 4
    assert schedule(4, "3", 0) == [0, 333_333_333, 666_666_666, NS]
    now, sent, waits = [0], [], []

    def wait(seconds):
        waits.append(seconds)
        now[0] += round(seconds * NS)
        return False

    def put(index):
        sent.append((index, now[0]))
        now[0] += 250_000_000

    assert release(schedule(4, "10", 0), put, wait, lambda: now[0]) == 4
    assert sent == [(0, 0), (1, 250_000_000), (2, 500_000_000), (3, 750_000_000)]
    assert waits == [0.0] * 4
    assert release([0, 1], put, lambda _: True, lambda: 0) == 0
    assert parse_rates("100,500,2000,max") == ["100", "500", "2000", "max"]
    for invalid in ("0", "-1", "nan", "1,1", "", "1.5", "1000000001"):
        try:
            parse_rates(invalid)
        except argparse.ArgumentTypeError:
            pass
        else:
            raise AssertionError(invalid)
    now[0], sent[:] = 0, []
    assert release(schedule(4, "10", 0), lambda i: sent.append((i, now[0])),
                   wait, lambda: now[0]) == 4
    assert sent == [(i, i * 100_000_000) for i in range(4)]
    assert release(schedule(3, "max", 0), lambda i: sent.append(i),
                   wait, lambda: now[0]) == 3
    assert sent[-3:] == [0, 1, 2]
    sends = [dict(txid=str(i), start_ns=i, end_ns=(i + 1) * NS, latency_ns=(i + 1) * 100,
                  schedule_lag_ns=i, status=status, error_message="reject" if status == "rejected" else "transport")
             for i, status in enumerate(("ok", "ok", "rejected", "error"))]
    sends.append(dict(txid="4", start_ns=None, end_ns=9 * NS, latency_ns=None, status="not_sent"))
    admissions = [dict(txid=str(i), start_ns=i, end_ns=2 * NS, latency_ns=value,
                       latency_lower_ns=value - 50, latency_upper_ns=value)
                  for i, value in enumerate((100, 300))]
    probes = [dict(status="ok", start_ns=2 * NS, end_ns=2 * NS + 10, latency_ns=10),
              dict(status="error", start_ns=3 * NS, end_ns=3 * NS + 30, latency_ns=30)]
    first_seen = dict.fromkeys(row["txid"] for row in sends)
    observe_template({"transactions": [{"hash": "0", "txid": "1"}]}, probes[0], first_seen)
    inclusions = template_inclusions(admissions, probes, first_seen)
    samples = [dict(rss=40, hwm=50, cpu_s=1), dict(rss=60, hwm=70, cpu_s=3)]
    result = aggregate(sends, admissions, inclusions, probes, samples, 0, 9 * NS, 2, "stable")
    assert result["accepted_tx_s"] == 0.5 and result["active_duration_ns"] == 4 * NS
    assert result["admission_latency"] == dict(p50_ns=100, p90_ns=300, p99_ns=300)
    assert result["admission_latency_lower"] == dict(p50_ns=50, p90_ns=250, p99_ns=250)
    assert result["admission_latency_upper"] == result["admission_latency"]
    assert result["send_latency"] == dict(p50_ns=300, p99_ns=400)
    assert result["gbt_count"] == 2 and result["gbt_errors"] == 1
    assert result["rejections_by_message"] == {"reject": 1}
    assert result["errors_by_message"] == {"transport": 1}
    assert result["not_sent"] == 1 and result["peak_rss_bytes"] == 60
    assert result["cpu_s"] == 2 and result["rpc_success_not_observed"] == 0
    assert result["template_included"] == 1 and result["template_never_seen"] == 1
    assert result["template_inclusion_latency"] == dict.fromkeys(
        ("p50_ns", "p90_ns", "p99_ns"), 2 * NS + 10)
    assert result["template_inclusion_latency_lower"] == dict.fromkeys(
        ("p50_ns", "p90_ns", "p99_ns"), 0)
    assert result["template_inclusion_latency_upper"] == result["template_inclusion_latency"]
    assert result["template_inclusion_after_admission"] == dict.fromkeys(
        ("p50_ns", "p90_ns", "p99_ns"), 10)
    assert result["template_inclusion_after_admission_lower"] == dict.fromkeys(
        ("p50_ns", "p90_ns", "p99_ns"), -2 * NS)
    assert (result["template_inclusion_after_admission_upper"] ==
            result["template_inclusion_after_admission"])
    assert result["template_inclusion_window"] == result["template_inclusion_latency"]
    assert quantiles([], (50, 99)) == dict(p50_ns=None, p99_ns=None)
    assert select_corpus([dict(kind="orchard", bytes=100), dict(kind="transparent", bytes=100)],
                         "mixed", False)[0]["kind"] == "orchard"
    bounds = admission_bounds(100, dict(start_ns=110, end_ns=130), dict(start_ns=150, end_ns=180))
    assert bounds["latency_lower_ns"] == 10 and bounds["latency_upper_ns"] == 80
    assert bounds["last_absent_poll_end_ns"] == 130 and bounds["first_present_poll_start_ns"] == 150
    bounds = admission_bounds(120, dict(start_ns=110, end_ns=130), dict(start_ns=115, end_ns=180))
    assert bounds["latency_lower_ns"] == 0 and bounds["latency_upper_ns"] == 60
    assert admission_bounds(100, None, dict(start_ns=90, end_ns=110))["latency_lower_ns"] == 0
    # A slow earlier request and out-of-order worker updates must not choose the wrong probe.
    inclusion_probes = [dict(start_ns=start, end_ns=end, status="ok")
                        for start, end in ((120, 240), (130, 145), (146, 149),
                                           (155, 190), (170, 185), (200, 230), (195, 200))]
    inclusion_probes[2]["status"] = "error"
    first_seen = dict.fromkeys(("a", "b", "c", "unadmitted"))
    hashes = (("a",), (), (), ("a", "b", "unadmitted"), (), ("a", "b"), ())
    for index in (0, 5, 4, 3, 2, 1, 6):
        row = inclusion_probes[index]
        if row["status"] == "ok":
            observe_template({"transactions": [{"hash": txid} for txid in hashes[index]]},
                             row, first_seen)
    observed = [dict(txid=txid, start_ns=start, end_ns=end)
                for txid, start, end in (("a", 100, 160), ("b", 110, 220), ("c", 120, 180))]
    included = template_inclusions(observed, inclusion_probes, first_seen)
    assert first_seen["a"] is inclusion_probes[3] and first_seen["b"] is inclusion_probes[3]
    assert [row["txid"] for row in included] == ["a", "b"]
    assert [row["latency_ns"] for row in included] == [90, 80]
    assert [row["latency_lower_ns"] for row in included] == [30, 20]
    assert [row["latency_upper_ns"] for row in included] == [90, 80]
    assert [row["admission_latency_ns"] for row in included] == [30, -30]
    assert [row["admission_latency_lower_ns"] for row in included] == [-30, -90]
    assert [row["admission_latency_upper_ns"] for row in included] == [30, -30]
    for row in included:
        assert row["first_present_probe_start_ns"] == 155
        assert row["first_present_probe_end_ns"] == 190
        assert row["last_absent_probe_start_ns"] == 130
        assert row["last_absent_probe_end_ns"] == 145
        assert row["window_ns"] == 60
    invalid_probe = dict(start_ns=125, end_ns=140, status="ok")
    observe_template({"transactions": [{"hash": "c"}, {"txid": "c"}]},
                     invalid_probe, first_seen)
    assert invalid_probe["status"] == "error" and first_seen["c"] is None
    progress = mempool_progress(
        [dict(end_ns=stamp, mempool_size=size) for stamp, size in
         ((NS // 2, 2), (NS, 5), (2 * NS, 9), (3 * NS, 10), (8 * NS, 10))], 0, 10)
    assert [progress[f"time_to_{p}pct_ns"] for p in (50, 90, 100)] == [NS, 2 * NS, 3 * NS]
    assert progress["peak_admission_rate_tx_s"] == 5
    assert progress["steady_admission_rate_tx_s"] == 4 and progress["steady_window_count"] == 3
    incomplete = mempool_progress([dict(end_ns=NS // 10, mempool_size=1)], 0, 2)
    assert incomplete["time_to_50pct_ns"] == NS // 10 and incomplete["time_to_100pct_ns"] is None
    assert incomplete["steady_admission_rate_tx_s"] is None
    assert mempool_progress([], None, 0)["peak_admission_rate_tx_s"] is None
    text = omit_miner_address(h.config_for("zebrad", '"state"', None))
    assert "[mining]\n" in text and "miner_address" not in text
    assert f'miner_address = "{h.P2SH_MINER}"' in h.config_for("zebrad", '"state"', h.P2SH_MINER)
    assert parse_node_env("MALLOC_ARENA_MAX=1") == ("MALLOC_ARENA_MAX", "1")
    assert parse_node_env("GLIBC_TUNABLES=glibc.malloc.arena_max=1") == (
        "GLIBC_TUNABLES", "glibc.malloc.arena_max=1")
    assert parse_node_env("EMPTY=") == ("EMPTY", "")
    for invalid in ("MISSING", "=1", "BAD KEY=1", "VALUE=bad\0value",
                    "TOKIO_WORKER_THREADS=1", "RAYON_NUM_THREADS=1"):
        try:
            parse_node_env(invalid)
        except argparse.ArgumentTypeError:
            pass
        else:
            raise AssertionError(invalid)
    assert parse_gbt_probe_ms("0") == 0 and parse_gbt_probe_ms("500") == 500
    assert parse_gbt_probe_ms("100") == 100
    for invalid in ("-1", "0.0001", "nan", "inf"):
        try:
            parse_gbt_probe_ms(invalid)
        except argparse.ArgumentTypeError:
            pass
        else:
            raise AssertionError(invalid)
    no_probes = aggregate(sends, admissions, [], [], samples, 0, 9 * NS, 2, "stable")
    assert no_probes["gbt_count"] == 0 and no_probes["gbt_errors"] == 0
    assert no_probes["gbt_latency"] == dict(p50_ns=None, p99_ns=None)
    assert no_probes["template_included"] == 0 and no_probes["template_never_seen"] == 2
    assert no_probes["template_inclusion_latency"] == dict.fromkeys(
        ("p50_ns", "p90_ns", "p99_ns"), None)
    assert no_probes["template_inclusion_after_admission"] == dict.fromkeys(
        ("p50_ns", "p90_ns", "p99_ns"), None)
    print("mempool_load self-test: passed")


def main():
    parser = argparse.ArgumentParser(description=__doc__, epilog=h.BENCH_HELP, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--provenance", type=Path)
    parser.add_argument("--arm", action="append", default=[], metavar="LABEL=PATH")
    parser.add_argument("--fixtures", type=Path)
    parser.add_argument("--corpus", choices=("transparent", "orchard", "mixed"), default="mixed")
    parser.add_argument("--rates", type=parse_rates, default=parse_rates("100,500,2000,max"))
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--gbt-probe-ms", type=parse_gbt_probe_ms, default=500,
                        help="load probe interval in ms (default 500; use 100 for a 1 s throttle); "
                             "zero disables load probes, not setup")
    parser.add_argument("--no-miner", action="store_true", help="omit miner address and all template calls")
    parser.add_argument("--node-env", action="append", type=parse_node_env, default=[], metavar="KEY=VALUE",
                        help="node-only environment override; repeatable, last value wins")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=600, help="load deadline in seconds, including drain")
    parser.add_argument("--rpc-timeout", type=float, default=30)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    args.node_env = dict(args.node_env)
    if not (args.provenance and args.arm and args.fixtures and args.output):
        parser.error("--provenance, --arm, --fixtures and --output are required")
    if (args.concurrency < 1 or args.repetitions < 1 or
            any(not math.isfinite(v) or v <= 0 for v in (args.timeout, args.rpc_timeout))):
        parser.error("positive concurrency/repetitions/timeouts required")
    requested_rates, requested_repetitions = args.rates, args.repetitions
    if args.smoke:
        args.rates, args.repetitions = ["10", "max"], 1
    arms = load_arms(args.provenance, args.arm)
    manifest, chain, fixtures = load_fixtures(args.fixtures)
    del fixtures
    corpus = select_corpus(json.loads((args.fixtures / "corpus.json").read_text()), args.corpus, args.smoke)
    metadata = {key: str(value) if isinstance(value, Path) else value for key, value in vars(args).items()}
    metadata.update(requested_rates=requested_rates, requested_repetitions=requested_repetitions,
                    mempool_poll_interval_ms=MEMPOOL_POLL_NS // 1_000_000,
                    mempool_size_poll_interval_ms=SIZE_POLL_NS // 1_000_000,
                    admission_bounds="last absent RPC request start to first present RPC response end, relative to send start; lower clamped at zero",
                    admission_progress="count-only net growth; percent targets use exact observed accepted count; no FIFO attribution",
                    template_inclusion="first successful response containing transactions[].hash, "
                    "ordered by response end; admitted txids only; unseen excluded from quantiles",
                    template_inclusion_bounds="last absent probe completed before first positive "
                    "request starts: absent request start (clamped to send start) through first "
                    "positive response end; no absent probe means send start",
                    template_inclusion_after_admission="relative to first getrawmempool response "
                    "end; signed, because a template can be observed before admission",
                    template_inclusion_resolution="configured gbt_probe_ms plus RPC uncertainty; "
                    "failures/overlap widen bounds; template_inclusion_window reports actual width",
                    admission_rate="peak rolling 1 s count increase; steady median complete 1 s windows from first send to 100% progress (or last count change if incomplete), excluding quiet tail",
                    fixture_files=manifest["files"], corpus_transactions=len(corpus),
                    corpus_by_kind=dict(Counter(tx["kind"] for tx in corpus)),
                    mempool_tx_cost_limit=MEMPOOL_TX_COST_LIMIT,
                    acceptance="first getrawmempool visibility, including RPC errors later observed",
                    throughput_window="load start to last send completion or first-seen admission; excludes quiet tail",
                    latency_percentile="nearest observed order statistic; all attempted RPC outcomes retained",
                    mixed_order="proportional interleaving, preserving input order within each kind",
                    setup="fresh relay.common.Node, complete sealed funding replay, NU5 at 1")
    campaign = Campaign(args.output, metadata, arms)
    results = []
    success = False
    by_label = {arm.label: arm for arm in arms}

    def interrupted(signum, _frame):
        raise KeyboardInterrupt("signal " + str(signum))

    signal.signal(signal.SIGTERM, interrupted)
    try:
        for repetition in range(args.repetitions):
            for rate in args.rates:
                for label in interleave(list(by_label), repetition):
                    arm = by_label[label]
                    scenario = f"mempool-{args.corpus}-{rate}"
                    stem = f"{scenario}-r{repetition:03d}-{label}"
                    identity = dict(arm=label, binary_sha256=arm.sha256, scenario=scenario,
                                    repetition=repetition, corpus=args.corpus, rate=rate,
                                    no_miner=args.no_miner, node_env=args.node_env,
                                    gbt_probe_ms=args.gbt_probe_ms)

                    def emit(row):
                        campaign.observe(dict(identity, **row))

                    started = time.perf_counter_ns()
                    try:
                        node = Node(arm.path, args.output.resolve() / "nodes" / stem,
                                    arm=label, timeout=args.timeout,
                                    miner=None if args.no_miner else h.P2SH_MINER)
                        node.env.update(args.node_env)
                        if args.no_miner:
                            node.config.write_text(omit_miner_address(node.config.read_text()))
                            require("miner_address" not in node.config.read_text(),
                                    "no-miner node config must omit miner_address")
                        with node:
                            if args.no_miner:
                                h.replay(node, chain)
                                require(node.rpc.call("getrawmempool") == [], "fresh replay mempool must be empty")
                            else:
                                node.replay(chain)
                            result = measure(node, corpus, rate, args, emit)
                        result.update(identity)
                        results.append(result)
                        campaign.observe(result)
                        if not result["success"]:
                            campaign.fail(stem, result)
                    except BaseException as error:
                        failure = dict(identity, record="run", start_ns=started,
                                       end_ns=time.perf_counter_ns(), success=False,
                                       error=repr(error), traceback=traceback.format_exc())
                        campaign.observe(failure)
                        campaign.fail(stem, failure)
                        results.append(failure)
                        if not isinstance(error, Exception):
                            raise
        expected = args.repetitions * len(args.rates) * len(arms)
        success = len(results) == expected and all(row["success"] for row in results)
    finally:
        campaign.complete(success)
    measured = []
    metrics = ["accepted_tx_s", "peak_rss_bytes", "steady_rss_bytes", "final_mempool_size",
               "gbt_count", "gbt_errors", "gbt_probe_ms", "template_included", "template_never_seen",
               "rejected", "send_errors", "not_sent", "cpu_s"]
    for row in results:
        if "accepted_tx_s" not in row:
            continue
        flat = dict(row)
        for name in ("admission_latency", "admission_latency_lower", "admission_latency_upper",
                     "template_inclusion_latency", "template_inclusion_latency_lower",
                     "template_inclusion_latency_upper", "template_inclusion_after_admission",
                     "template_inclusion_after_admission_lower",
                     "template_inclusion_after_admission_upper", "template_inclusion_window",
                     "admission_progress", "send_latency", "gbt_latency", "schedule_lag"):
            for percentile, value in row[name].items():
                metric = f"{name}_{percentile}"
                flat[metric] = value
                if metric not in metrics:
                    metrics.append(metric)
        measured.append(flat)
    for metric in metrics:
        for row in summarize(measured, ("arm", "corpus", "rate"), metric):
            label = " / ".join(str(row[key]) for key in ("arm", "corpus", "rate"))
            print(f"{label}: {metric} {row['median']:.6g} [{row['min']:.6g}, {row['max']:.6g}] n={row['n']}")
    if not success:
        raise SystemExit("Incomplete mempool campaign; see failures/ and completion.json")


if __name__ == "__main__":
    main()
