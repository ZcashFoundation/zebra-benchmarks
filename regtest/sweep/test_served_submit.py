#!/usr/bin/env python3
"""Served work serialization and submission protocol; no nodes or sockets."""
import copy
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import run_sweep
from common import block_identity, h


def work():
    return dict(version=4, height=102, previousblockhash='11' * 32,
                defaultroots=dict(merkleroot='22' * 32, blockcommitmentshash='33' * 32),
                curtime=1700000000, bits='207fffff', target='7fffff' + '00' * 29,
                coinbasetxn=dict(data='aabb'),
                transactions=[dict(hash='b', data='bb'), dict(hash='a', data='aa')],
                longpollid='served-id', sizelimit=2_000_000)


class ServedSubmitTests(unittest.TestCase):
    def test_serializes_served_order_roots_and_solved_target(self):
        template = work()
        before = copy.deepcopy(template)
        raw = bytes.fromhex(h.regtest_block(template, solve=True))
        header_len = h.header_len(raw.hex())
        self.assertEqual(header_len, 177)
        self.assertEqual(raw[4:36], bytes.fromhex(template['previousblockhash'])[::-1])
        self.assertEqual(raw[36:68], bytes.fromhex(template['defaultroots']['merkleroot'])[::-1])
        self.assertEqual(raw[68:100], bytes.fromhex(template['defaultroots']['blockcommitmentshash'])[::-1])
        self.assertEqual(int.from_bytes(raw[100:104], 'little'), template['curtime'])
        self.assertEqual(int.from_bytes(raw[104:108], 'little'), int(template['bits'], 16))
        self.assertEqual(raw[140:177], b'\x24' + bytes(36))
        self.assertEqual(raw[177:], b'\x03\xaa\xbb\xbb\xaa')
        self.assertLessEqual(int.from_bytes(h.dsha(raw[:header_len]), 'little'), int(template['target'], 16))
        self.assertEqual(template, before)
        empty = dict(template, transactions=[])
        self.assertEqual(h.empty_regtest_block(empty), h.regtest_block(empty))

    def test_nonce_search_and_deadline(self):
        with patch.object(h, 'dsha', side_effect=[b'\xff' * 32, bytes(32)]):
            raw = bytes.fromhex(h.regtest_block(work(), solve=True))
        self.assertEqual(int.from_bytes(raw[108:140], 'little'), 1)
        with patch.object(h, 'dsha', return_value=b'\xff' * 32), \
                self.assertRaisesRegex(RuntimeError, 'nonce search deadline'):
            h.regtest_block(work(), solve=True, timeout=0)
        for bits, target in (('20800001', '01'), ('2100ffff', '01'), ('00000000', '00'),
                             ('207fffff', '01')):
            with self.assertRaises(RuntimeError):
                h.regtest_block(dict(work(), bits=bits, target=target), solve=True)

    def test_admits_waits_and_submits_served_not_fixture_without_proposal(self):
        for delay, changed in ((0, False), (1, True)):
            with self.subTest(delay=delay):
                template = work()
                latest = copy.deepcopy(template)
                if changed:
                    latest['longpollid'] = 'replacement-id'
                    latest['coinbasetxn']['data'] = 'cccc'
                corpus = [dict(txid='a', raw='aa', bytes=1), dict(txid='b', raw='bb', bytes=1)]
                fixture = dict(parent=template['previousblockhash'], raw='deadbeef', hash='fixture', corpus=corpus)
                calls, pending = [], []
                state = dict(polls=0, tip=fixture['parent'])

                def rpc(method, params=None):
                    calls.append((method, params))
                    if method == 'sendrawtransaction':
                        txid = {'aa': 'a', 'bb': 'b'}[params[0]]
                        pending.append(txid)
                        return txid
                    if method == 'getrawmempool':
                        return pending
                    if method == 'getblocktemplate':
                        self.assertIsNone(params, 'explicit proposals and long polls are forbidden')
                        state['polls'] += 1
                        if state['polls'] == 1:
                            return dict(template, transactions=[])
                        return template if state['polls'] == 2 else latest
                    if method == 'submitblock':
                        self.assertEqual(state['polls'], 3)
                        self.assertEqual(pending, ['a', 'b'])
                        self.assertEqual(params[0], h.regtest_block(template, solve=True))
                        self.assertNotEqual(params[0], fixture['raw'])
                        state.update(block=params[0], tip=block_identity(params[0])['hash'])
                        return None
                    if method == 'getbestblockhash':
                        return state['tip']
                    if method == 'getblock':
                        self.assertEqual(params, [state['tip'], 0])
                        return state['block']
                    raise AssertionError(method)

                node = SimpleNamespace(rpc=SimpleNamespace(call=rpc), replay=lambda chain: None,
                                       preload_receipt=lambda start: {})
                args = SimpleNamespace(timeout=1, poll_ms=0, solve_delay_seconds=delay)
                with patch.object(run_sweep, 'Node') as constructor, \
                        patch.object(run_sweep.time, 'sleep') as sleep:
                    constructor.return_value.__enter__.return_value = node
                    row = run_sweep.case('zebra-main', '/unused', Path('/unused'), fixture, [], args,
                                         'served-submitblock')
                self.assertEqual(sleep.call_args.args, (delay,))
                self.assertEqual(row['template_longpollid'], 'served-id')
                self.assertEqual(row['pre_submit_longpollid'], latest['longpollid'])
                self.assertEqual(row['template_rebuilt_before_submit'], changed)
                self.assertEqual(row['served_block_hash'], state['tip'])
                self.assertEqual(row['served_block_bytes'], len(state['block']) // 2)
                self.assertEqual(row['solve_delay_seconds'], delay)
                self.assertEqual(row['template_polls'], 2)
                self.assertGreater(row['latency_ns'], 0)
                self.assertEqual(row['latency_ns'], row['end_ns'] - row['start_ns'])
                self.assertLessEqual(row['template_ready_ns'], row['start_ns'])
                self.assertNotIn('reply', row)

    def test_rebuild_detects_changed_work_even_with_same_longpoll_id(self):
        template = work()
        latest = dict(template, curtime=template['curtime'] + 1)
        node = SimpleNamespace(rpc=SimpleNamespace(call=lambda *args: latest))
        fixture = dict(parent=template['previousblockhash'], corpus=[])
        args = SimpleNamespace(timeout=1, poll_ms=0, solve_delay_seconds=0)
        with patch.object(run_sweep, 'admit'), \
                patch.object(run_sweep, 'exact_template', return_value=(template, 1)), \
                patch.object(run_sweep.time, 'sleep'), \
                patch.object(run_sweep, 'accepted'), \
                patch.object(run_sweep, 'timed_submit', return_value=dict(start_ns=10**30, end_ns=10**30+1,
                                                                       latency_ns=1, reply=None)):
            self.assertTrue(run_sweep.served_submit(node, fixture, args)['template_rebuilt_before_submit'])

    def test_cli_delay_default_zero_and_invalid(self):
        arguments = ['run_sweep.py', '--arm', 'extra-ours=/missing/a', '--arm', 'zakura-main=/missing/b',
                     '--fixtures', '/missing/fixtures', '--output', '/missing/output',
                     '--provenance', '/missing/provenance',
                     '--endpoints', 'served-submitblock', '--dry-run']
        for flags, expected in (([], 1), (['--solve-delay-seconds', '0'], 0)):
            with patch('sys.argv', [*arguments, *flags]), patch('builtins.print') as output, \
                    patch.object(run_sweep, 'load_fixtures') as fixtures:
                run_sweep.main()
                self.assertEqual(json.loads(output.call_args.args[0])['solve_delay_seconds'], expected)
                fixtures.assert_not_called()
        for invalid in ('-1', 'nan', 'inf'):
            with patch('sys.argv', [*arguments, '--solve-delay-seconds=' + invalid]), \
                    patch('sys.stderr'), self.assertRaises(SystemExit):
                run_sweep.main()


if __name__ == '__main__':
    unittest.main()
