#!/usr/bin/env python3
"""Offline protocol, fixture and scheduling checks. Never starts a node or opens a socket."""
import importlib.util
import json
import os
from pathlib import Path
import shlex
import subprocess
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import tip_to_template as probe


def tx(txid, kind='transparent'):
    return dict(txid=txid, kind=kind, raw=txid.encode().hex(), bytes=len(txid), sha256=txid)


class TipTemplateTests(unittest.TestCase):
    def setUp(self):
        self.mined, self.extra = tx('mined'), tx('extra')
        self.fixture = dict(id='transparent-250000', hash='new', parent='old', raw='cc',
                            bytes=100, txids=['mined'], corpus=[self.mined])
        self.initial = dict(previousblockhash='old', transactions=[], longpollid='old-id')
        self.template = dict(previousblockhash='new',
                             transactions=[dict(hash='extra', data=self.extra['raw'])])
        self.args = SimpleNamespace(timeout=2, poll_ms=0.1)

    def test_first_template_membership(self):
        probe.check_template(self.template, self.fixture, [self.extra])
        for transactions in ([], [dict(hash='mined', data=self.mined['raw'])],
                             self.template['transactions'] * 2,
                             [dict(hash='extra', data='ff')]):
            with self.assertRaises(RuntimeError):
                probe.check_template(dict(self.template, transactions=transactions),
                                     self.fixture, [self.extra])

    def test_observation_modes_and_old_tip_wakeup(self):
        for longpoll in (True, False):
            initial = self.initial if longpoll else dict(previousblockhash='old')
            # Old tip, then coinbase-only work on the new tip, then the full template.
            replies = [dict(self.initial, longpollid='next-id'),
                       dict(previousblockhash='new', transactions=[], longpollid='empty-id'),
                       self.template]
            calls, stamps = [], {}
            conn = SimpleNamespace(sock=None, close=lambda: None)

            def call(method, params):
                calls.append((method, params))
                return replies.pop(0)

            started = threading.Event()
            started.set()
            value = probe.observe(SimpleNamespace(call=call, conn=conn), 'getblocktemplate',
                                  self.fixture, initial, started, threading.Event(),
                                  threading.Event(), stamps, self.args,
                                  lambda t: probe.is_full(t, self.fixture, [self.extra]))
            self.assertEqual(value, self.template)
            expected = ([[{'longpollid': 'old-id'}], [{'longpollid': 'next-id'}],
                         [{'longpollid': 'empty-id'}]] if longpoll else [None, None, None])
            self.assertEqual([params for _, params in calls], expected)
            receipt = stamps['getblocktemplate']
            self.assertEqual(receipt['calls'], 3)
            self.assertEqual(receipt['mode'], 'longpoll' if longpoll else 'poll')
            self.assertEqual(receipt['first_new_transactions'], 0)
            self.assertLessEqual(receipt['last_old']['end_ns'], receipt['first_new']['start_ns'])
            self.assertLessEqual(receipt['first_new']['end_ns'], receipt['full_new']['start_ns'])

    def test_mined_transaction_in_new_tip_work_fails(self):
        with self.assertRaises(RuntimeError):
            probe.is_full(dict(self.template, transactions=[dict(hash='mined', data='00')]),
                          self.fixture, [self.extra])

    def test_observation_errors_and_deadline(self):
        started = threading.Event()
        started.set()
        conn = SimpleNamespace(sock=None, close=lambda: None)
        for reply, error in [('unrelated', RuntimeError), ('old', TimeoutError)]:
            with self.assertRaises(error):
                probe.observe(SimpleNamespace(call=lambda *_: reply, conn=conn), 'getbestblockhash',
                              self.fixture, {}, started, threading.Event(), threading.Event(), {},
                              SimpleNamespace(timeout=0.005, poll_ms=0.1))

    def test_observers_do_not_wait_for_sender_reply(self):
        self.args.perf = True
        result = {}
        submitted = threading.Event()
        fixture, template = self.fixture, self.template

        def direct_call(method, params=None):
            if method == 'submitblock':
                submitted.set()
                self.assertIn('perf_enabled', result)
                deadline = time.monotonic() + 2
                while len(result['observations']) != 2 and time.monotonic() < deadline:
                    time.sleep(0.001)
                self.assertEqual(len(result['observations']), 2)
                return None
            return fixture['hash'] if method == 'getbestblockhash' else fixture['raw']

        def observer_rpc(*_args, **_kwargs):
            def call(method, params=None):
                self.assertTrue(submitted.wait(2))
                return template if method == 'getblocktemplate' else fixture['hash']
            return SimpleNamespace(call=call, conn=SimpleNamespace(sock=None, close=lambda: None))

        def preload_receipt(start_ns):
            self.assertEqual(start_ns, result['start_ns'])
            self.assertIn('end_ns', result)
            self.assertIn('receiver_best_tip_observed_ns', result)
            return dict(binary_preloads=True, preload_wait=True, preload_done_unix_ns=1,
                        preload_done_ns=start_ns - 10, preload_done_relative_ns=-10)

        node = SimpleNamespace(rpc=SimpleNamespace(call=direct_call), rpc_addr='unused',
                               proc=SimpleNamespace(pid=123), preload_receipt=preload_receipt)
        with tempfile.TemporaryDirectory() as directory, patch.object(probe.h, 'RPC', observer_rpc), \
                patch.object(probe, 'ReceiverPerf') as profiler:
            profiler.return_value.enable.side_effect = lambda: result.setdefault('perf_enabled', time.perf_counter_ns())
            profiler.return_value.disable.side_effect = lambda: result.setdefault('perf_disabled', time.perf_counter_ns())
            probe.measure(node, node, fixture, [self.extra], self.initial, self.args,
                          Path(directory), result)
            profiler.return_value.close.assert_called_once()
            profiler.return_value.report.assert_called_once()
            self.assertEqual(json.loads((Path(directory) / 'full-template.json').read_text()), template)
        self.assertLess(result['template_ready_ns'], result['sender_submit_end_ns'])
        self.assertLess(result['receiver_best_tip_observed_ns'], result['sender_submit_end_ns'])
        self.assertEqual(result['latency_ns'], result['end_ns'] - result['start_ns'])
        self.assertLess(result['perf_enabled'], result['start_ns'])
        self.assertLessEqual(result['end_ns'], result['perf_disabled'])
        self.assertLess(result['perf_disabled'], result['sender_submit_end_ns'])
        self.assertEqual(set(result['preload']), {'sender', 'receiver'})
        for receipt in result['preload'].values():
            self.assertTrue(receipt['binary_preloads'])
            self.assertTrue(receipt['preload_wait'])
            self.assertEqual(receipt['preload_done_relative_ns'], -10)

    def test_minimal_tip_preload_receipts(self):
        spec = importlib.util.spec_from_file_location('minimal_tip', probe.ROOT / 'tip-template.py')
        minimal = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(minimal)
        for local in (True, False):
            submitted = threading.Event()
            receipts = []
            calls = []

            def call(method, params=None):
                calls.append(method)
                if method == 'submitblock':
                    submitted.set()
                    return None
                if len(calls) == 1:
                    return dict(previousblockhash='old')
                self.assertTrue(submitted.wait(2))
                return dict(previousblockhash='new')

            def preload_receipt(start_ns):
                self.assertTrue(submitted.is_set())
                receipts.append(start_ns)
                return dict(binary_preloads=True, preload_wait=True, preload_done_unix_ns=1,
                            preload_done_ns=start_ns - 10, preload_done_relative_ns=-10)

            node = SimpleNamespace(rpc=SimpleNamespace(call=call), rpc_addr='unused',
                                   preload_receipt=preload_receipt)
            sender = node if local else SimpleNamespace(**vars(node))
            rpc = SimpleNamespace(call=call, conn=SimpleNamespace(close=lambda: None))
            with patch.object(minimal.h, 'RPC', return_value=rpc):
                result = minimal.sample(node, sender, self.fixture)
            self.assertEqual(set(result['preload']), {'receiver'} if local else {'sender', 'receiver'})
            self.assertEqual(receipts, [result['start_ns']] * (1 if local else 2))
            self.assertEqual(result['end_ns'] - result['start_ns'], result['latency_ns'])
            self.assertTrue(all(r['preload_done_relative_ns'] == -10 for r in result['preload'].values()))

    def test_empty_gbt_preserves_numeric_samples(self):
        spec = importlib.util.spec_from_file_location('empty_gbt', probe.ROOT / 'gbt-empty.py')
        minimal = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(minimal)
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(minimal, 'ROUNDS', 1), patch.object(minimal, 'WARMUP', 0), \
                patch.object(minimal, 'CALLS', 2), patch.object(minimal, 'Node') as nodes, \
                patch.object(minimal, 'load_fixtures', return_value=({}, [], [])), \
                patch.object(minimal.time, 'perf_counter_ns', side_effect=[10, 20, 30, 45, 50, 70, 80, 100]), \
                patch.object(minimal.sys, 'argv', ['gbt-empty.py', str(Path(directory) / 'out'), 'a=a', 'b=b']), \
                patch('builtins.print'):
            node = nodes.return_value.__enter__.return_value
            node.rpc.call.return_value = dict(transactions=[])

            def preload_receipt(start_ns):
                self.assertIn(node.rpc.call.call_count, (2, 4), 'read receipts after the timed call loop')
                return dict(binary_preloads=True, preload_wait=True, preload_done_unix_ns=1,
                            preload_done_ns=1, preload_done_relative_ns=1 - start_ns)

            node.preload_receipt.side_effect = preload_receipt
            minimal.main()
            rows = [json.loads(line) for line in (Path(directory) / 'out' / 'rounds.jsonl').read_text().splitlines()]
        self.assertEqual([row['latencies_ns'] for row in rows], [[10, 15], [20, 20]])
        for row in rows:
            self.assertEqual(len(row['samples']), len(row['latencies_ns']))
            for sample, latency in zip(row['samples'], row['latencies_ns']):
                self.assertEqual(sample['end_ns'] - sample['start_ns'], latency)
                self.assertEqual(sample['preload']['preload_done_relative_ns'], 1 - sample['start_ns'])

    def test_perf_control_and_reports(self):
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(probe.subprocess, 'Popen') as popen, \
                patch.object(probe.select, 'select', return_value=([1], [], [])), \
                patch.object(probe.os, 'read', return_value=b'ack\n\0'), \
                patch.object(probe.subprocess, 'run') as run:
            popen.return_value.poll.return_value = None
            popen.return_value.wait.return_value = 0
            receipt = {}
            profiler = probe.ReceiverPerf(123, Path(directory), receipt)
            command = popen.call_args.args[0]
            self.assertIn('cycles:u', command)
            self.assertIn('--user-callchains', command)
            self.assertIn('dwarf,16384', command)
            self.assertEqual(command[command.index('-p') + 1], '123')
            self.assertNotIn('-t', command)  # PID attachment includes every thread.
            profiler.enable()
            profiler.disable()
            disabled = receipt['perf']['disabled_ns']
            profiler.disable()
            self.assertEqual(disabled, receipt['perf']['disabled_ns'])
            profiler.close()
            popen.return_value.send_signal.assert_called_once_with(probe.signal.SIGINT)
            profiler.report()
            self.assertEqual(run.call_count, 3)
            self.assertTrue(all((Path(directory) / f'receiver-perf-{name}.txt').exists()
                                for name in ('self', 'children', 'threads')))

    def test_warm_and_cold_preparation(self):
        class FakeNode:
            def __init__(self, _binary, directory, _arm, _timeout, miner='configured'):
                self.role, self.miner, self.pool = directory.name, miner, []
                self.p2p_addr = 'unused'
                self.rpc = SimpleNamespace(call=self.call)

            def __enter__(self):
                return self

            def __exit__(self, *_):
                pass

            def replay(self, _chain):
                pass

            def wait(self, predicate, *_):
                return predicate()

            def call(self, method, _params=None):
                if method == 'getrawmempool':
                    return [item['txid'] for item in self.pool]
                if method == 'getbestblockhash':
                    return 'old'
                if method == 'getpeerinfo':
                    return [{}]
                if method == 'getblocktemplate':
                    assert self.role == 'receiver', 'sender must not request templates'
                    return dict(previousblockhash='old', sizelimit=probe.LIMIT,
                                transactions=[dict(hash=item['txid'], data=item['raw']) for item in self.pool])
                assert method == 'addnode'

        for temperature in ('cold', 'warm'):
            args = SimpleNamespace(**vars(self.args), temperature=temperature, settle_seconds=0)
            result = {}

            def measure(sender, receiver, _fixture, _extras, _initial, _args, _directory, receipt):
                self.assertIsNone(sender.miner)
                expected = [self.extra] + ([self.mined] if temperature == 'warm' else [])
                self.assertEqual(receiver.pool, expected)
                receipt['start_ns'] = time.perf_counter_ns()

            with tempfile.TemporaryDirectory() as directory, patch.object(probe, 'Node', FakeNode), \
                    patch.object(probe.h, 'replay'), patch.object(probe, 'measure', measure), \
                    patch.object(probe, 'admit', lambda node, corpus: node.pool.extend(corpus)):
                probe.case('unused', 'test', self.fixture, [], [self.extra], args, Path(directory), result)
            self.assertEqual(result['receiver_mempool_txids'],
                             ['extra', 'mined'] if temperature == 'warm' else ['extra'])

    def test_extra_selection_rejects_short_or_oversized_corpus(self):
        corpus = [self.mined, tx('one'), tx('two'), tx('three')]
        self.assertEqual([item['txid'] for item in probe.select_extras(self.fixture, corpus)], ['one', 'two'])
        with self.assertRaises(RuntimeError):
            probe.select_extras(self.fixture, corpus[:2])
        with self.assertRaises(RuntimeError):
            probe.select_extras(dict(self.fixture, bytes=probe.LIMIT), corpus)

    def test_real_sealed_fixture_selection(self):
        checked = []
        directories = [(probe.h.FIXTURES, ('empty', 'orchard', 'mixed')),
                       (Path(os.environ.get('BENCH_SIGNED_FIXTURES',
                             probe.h.BENCH_ROOT / 'fixtures/transparent-signed')).expanduser().resolve(),
                        ('transparent-signed',))]
        if any(not (directory / 'manifest.sha256').is_file() for directory, _ in directories):
            self.skipTest('generate BENCH_FIXTURES and BENCH_SIGNED_FIXTURES for sealed-corpus checks')
        for directory, kinds in directories:
            _, _, fixtures = probe.load_fixtures(directory)
            corpus = json.loads((directory / 'corpus.json').read_text())
            for fixture in fixtures:
                if fixture['filling'] not in kinds or fixture['target_bytes'] not in (2000, 250000, 1900000):
                    continue
                extras = probe.select_extras(fixture, corpus)
                self.assertFalse(set(fixture['txids']) & {item['txid'] for item in extras})
                self.assertEqual(len(extras), 4 if fixture['filling'] == 'mixed' else 2)
                checked.append(fixture['id'])
                print(f"sealed {fixture['id']}: {fixture['bytes']} bytes, "
                      f"{len(fixture['corpus'])} mined + {len(extras)} extra transactions")
        self.assertEqual(len(checked), 7)

    def test_local_schedule_dry_run(self):
        script = probe.ROOT / 'tip-to-template.sh'
        output = subprocess.check_output(['bash', str(script), '--dry-run',
                                          'a:binary-a', 'b:binary-b', 'c:binary-c'], text=True)
        lines = output.splitlines()
        self.assertEqual(shlex.split(lines[0]), ['bash', 'wait-quiet.sh'])
        commands = [shlex.split(line) for line in lines[1:-1]]
        self.assertEqual(len(commands), 5 * 7 * 2 * 2 * 3)
        self.assertEqual(len({command[-1] for command in commands}), len(commands))
        for rep in range(5):
            group = commands[rep * 84:(rep + 1) * 84]
            expected = 'abc'[rep % 3:] + 'abc'[:rep % 3]
            self.assertEqual(''.join(command[-3] for command in group[:3]), expected)
            self.assertEqual(len({command[-2] for command in group}), 7)
        for command in commands:
            directory = command[command.index('--fixtures') + 1]
            expected = (Path(os.environ.get('BENCH_SIGNED_FIXTURES',
                             probe.h.BENCH_ROOT / 'fixtures/transparent-signed')).expanduser().resolve()
                        if command[-2].startswith('transparent-signed-') else probe.h.FIXTURES)
            self.assertEqual(Path(directory), expected)
            settle = command[command.index('--settle-seconds') + 1]
            self.assertEqual(settle, '20' if '/settled-' in command[-1] else '0')
            self.assertIn('tip-to-template-DRY-RUN/', command[-1])
        print(f'dry run: quiet gate first, {len(commands)} unique interleaved sample commands; no nodes')


if __name__ == '__main__':
    unittest.main()
