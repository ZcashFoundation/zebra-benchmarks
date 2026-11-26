#!/usr/bin/env python3
"""Bounded steady admission and concurrent observers, without nodes or sockets."""
import copy
import json
from pathlib import Path
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import run_sweep
import sustained_churn as churn
import transparent_signed as signed
from common import h


def fixture(count):
    corpus = []
    for index in range(count):
        raw = signed.serialize(('11' * 32, index, 1_000_000), [990_000], b'\x01\x51')
        corpus.append(dict(txid=h.dsha(raw)[::-1].hex(), raw=raw.hex(), bytes=len(raw),
                           kind='transparent-signed'))
    return dict(parent='22' * 32, corpus=corpus)


def work(selected=()):
    return dict(version=4, previousblockhash='22' * 32, defaultroots={}, bits='207fffff',
                curtime=1, longpollid='same-id', coinbasetxn=dict(data='aa'),
                transactions=[dict(hash=tx['txid'], data=tx['raw']) for tx in selected])


class SustainedChurnTests(unittest.TestCase):
    def test_corpus_capacity_unique_inputs_and_percentiles(self):
        source = fixture(6000)
        self.assertEqual(len(churn.select_corpus(source, 100, 60)), 6000)
        self.assertEqual(len(churn.select_corpus(source, 20, 60)), 1200)
        with self.assertRaisesRegex(RuntimeError, 'needs 6001'):
            churn.select_corpus(source, 6001, 1)
        duplicate = copy.deepcopy(source['corpus'][0])
        duplicate['txid'] = 'ff' * 32
        with self.assertRaisesRegex(RuntimeError, 'reuse inputs'):
            churn.select_corpus(dict(corpus=[source['corpus'][0], duplicate]), 2, 1)
        with self.assertRaisesRegex(RuntimeError, 'signed transparent'):
            churn.select_corpus(dict(corpus=[dict(source['corpus'][0], kind='transparent')]), 1, 1)
        self.assertEqual(churn.distribution([]), dict(median=None, p90=None, p99=None, max=None))
        self.assertEqual(churn.distribution(range(1, 101)), dict(median=50.5, p90=90, p99=99, max=100))
        self.assertNotIn('sustained-churn', run_sweep.case_specs(['a'], [dict(id='x', transaction_count=1)])[0]['endpoints'])

    def run_churn(self, *, rate=100, seconds=0.4, hidden=False, workers=16):
        source = fixture(round(rate * seconds))
        by_raw = {tx['raw']: tx for tx in source['corpus']}
        selected, calls, clients = [], [], []
        lock = threading.Lock()
        reject = source['corpus'][1]['txid']
        withheld = source['corpus'][-1]['txid'] if hidden else None

        def call(method, params=None):
            with lock:
                calls.append((method, params))
            if method == 'sendrawtransaction':
                tx = by_raw[params[0]]
                if tx['txid'] == reject:
                    raise RuntimeError('deliberate rejection')
                with lock:
                    selected.append(tx)
                time.sleep(0.03)  # A serial sender cannot sustain 100 tx/s.
                return tx['txid']
            if method == 'getblocktemplate':
                self.assertIsNone(params, 'no long polls or proposal requests')
                with lock:
                    template = work([tx for tx in selected if tx['txid'] != withheld])
                    template['curtime'] = len(calls)  # Clock-only changes are not builds.
                    return template
            if method == 'getblockcount':
                return 102
            if method == 'getrawmempool':
                return []
            raise AssertionError(method)

        def client(*args, **kwargs):
            rpc = SimpleNamespace(call=call, conn=SimpleNamespace(close=Mock()))
            clients.append(rpc)
            return rpc

        node = SimpleNamespace(rpc=SimpleNamespace(call=call), rpc_addr='unused:1',
                               proc=SimpleNamespace(pid=123), preload_receipt=lambda start: {})
        args = SimpleNamespace(churn_rate=rate, churn_seconds=seconds, churn_settle_seconds=0, timeout=0.2)
        with patch.object(churn.h, 'RPC', side_effect=client), \
                patch.object(churn.h, 'resources', side_effect=[dict(cpu_s=8, rss=10, hwm=20),
                                                              dict(cpu_s=9.2, rss=30, hwm=40)]), \
                patch.object(Path, 'write_text') as reset, \
                patch.object(churn, 'ADMISSION_WORKERS', workers):
            row = churn.run(node, source, args, run_sweep.template_changed)
        reset.assert_called_once_with('5\n')
        self.assertTrue(all(rpc.conn.close.call_count == 1 for rpc in clients))
        self.assertFalse(any(method == 'submitblock' for method, _ in calls))
        self.assertAlmostEqual(row['cpu_seconds'], 1.2)
        self.assertEqual(row['peak_rss_bytes'], 40)
        self.assertEqual(row['latency_ns'], row['end_ns'] - row['start_ns'])
        return row

    def test_concurrent_rate_observers_failures_and_freshness_tail(self):
        row = self.run_churn()
        self.assertGreater(row['actual_admitted_rate'], 50)
        self.assertEqual(row['admission_failures'], 1)
        self.assertEqual(row['generator_missed_admissions'], 0)
        self.assertEqual(row['freshness_unobserved'], 0)
        self.assertGreaterEqual(len(row['gbt_polls']), 2)
        self.assertGreaterEqual(len(row['cheap_rpc_polls']), 2)
        self.assertGreater(row['template_rebuild_count'], 0)
        self.assertLessEqual(row['template_rebuild_count'], len(row['gbt_polls']))
        self.assertGreaterEqual(row['freshness_ns']['median'], 0)
        self.assertTrue(all(sample['end_ns'] <= row['end_ns'] for sample in row['gbt_polls']))
        self.assertTrue(all(tx['freshness_ns'] is not None for tx in row['transactions'] if tx['accepted']))
        row = self.run_churn(rate=20, seconds=0.4, hidden=True)
        self.assertEqual(row['freshness_unobserved'], 1)

    def test_overload_bounds_queue_and_reports_generator_misses(self):
        row = self.run_churn(rate=1000, seconds=0.2, workers=1)
        self.assertGreater(row['generator_missed_admissions'], 0)
        self.assertEqual(row['attempted_transactions'] + row['generator_missed_admissions'], 200)

    def test_clock_only_roll_is_not_rebuild(self):
        template = work()
        self.assertFalse(run_sweep.template_changed(template, dict(template, curtime=2), include_time=False))
        self.assertTrue(run_sweep.template_changed(template, dict(template, longpollid='new-id'), include_time=False))

    def test_cli_defaults_and_invalid_values_are_node_free(self):
        argv = ['run_sweep.py', '--arm', 'a=/missing/a', '--arm', 'zakura-main=/missing/b',
                '--fixtures', '/missing', '--provenance', '/missing', '--output', '/missing',
                '--endpoints', 'sustained-churn']
        original_parse = run_sweep.argparse.ArgumentParser.parse_args
        parsed = []

        def capture(parser):
            options = original_parse(parser)
            parsed.append(options)
            return options

        with patch('sys.argv', argv), \
                patch.object(run_sweep.argparse.ArgumentParser, 'parse_args', capture), \
                patch.object(run_sweep, 'require', side_effect=StopIteration):
            with self.assertRaises(StopIteration):
                run_sweep.main()
        options = parsed[-1]
        self.assertEqual((options.churn_rate, options.churn_seconds, options.churn_settle_seconds), (20, 60, 20))
        for flag in ('--churn-rate', '--churn-seconds', '--churn-settle-seconds'):
            for value in ('-1', 'nan', 'inf'):
                with patch('sys.argv', [*argv, flag + '=' + value]), patch('sys.stderr'), self.assertRaises(SystemExit):
                    run_sweep.main()


if __name__ == '__main__':
    unittest.main()
