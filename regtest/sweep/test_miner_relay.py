#!/usr/bin/env python3
"""Offline miner payout, chain preparation, concurrency, topology and runner checks."""
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

import relay_chain as relay
import tip_to_template as tip


class MinerRelayTests(unittest.TestCase):
    def test_miner_config_and_coinbase_pool(self):
        for miner, address in tip.MINERS.items():
            with tempfile.TemporaryDirectory() as directory:
                node = tip.Node('unused', Path(directory) / miner, miner=address)
                self.assertIn(f'miner_address = "{address}"', node.config.read_text())
            prefix = bytes.fromhex('05000080') + bytes(16) + b'\x01' + bytes(32) + b'\xff' * 4
            prefix += tip.h.compact(253) + bytes(253) + bytes(4)  # Exercise CompactSize scripts.
            transparent = b'\x01' + bytes(8) + b'\x01\x51' if miner == 'transparent' else b'\0'
            sapling = b'\0\x01' + bytes(756 + 8 + 192 + 64) if miner == 'sapling' else b'\0\0'
            orchard = b'\x01' if miner == 'orchard' else b'\0'
            raw = prefix + transparent + sapling + orchard
            template = dict(coinbasetxn=dict(data=raw.hex()), transactions=[dict(fee=20000)])
            self.assertEqual(tip.check_coinbase(template, miner)[miner], 1)
            for other in set(tip.MINERS) - {miner}:
                with self.assertRaises(RuntimeError):
                    tip.check_coinbase(template, other)
            with self.assertRaises(RuntimeError):
                tip.check_coinbase(dict(template, transactions=[]), miner)
            for truncated in (raw[:3], raw[:-1]):
                with self.assertRaises(RuntimeError):
                    tip.check_coinbase(dict(template, coinbasetxn=dict(data=truncated.hex())), miner)

    def test_topology_rejects_shortcut_and_wrong_neighbor(self):
        peers = dict(A=[dict(addr='B', inbound=True)],
                     B=[dict(addr='A', inbound=False), dict(addr='C', inbound=False)],
                     C=[dict(addr='B', inbound=True)])
        nodes = {name: SimpleNamespace(p2p_addr=name, rpc=SimpleNamespace(call=lambda _, name=name: peers[name]))
                 for name in peers}
        relay.check_topology(nodes)
        peers['A'].append(dict(addr='C', inbound=False))
        with self.assertRaises(RuntimeError):
            relay.check_topology(nodes)
        peers['A'].pop()
        peers['B'][1]['addr'] = 'unrelated'
        with self.assertRaises(RuntimeError):
            relay.check_topology(nodes)

    def test_chain_observers_do_not_wait_for_submit_return(self):
        submitted = threading.Event()
        result = {}
        fixture = dict(hash='new', parent='old', raw='cc')

        def direct_call(method, params=None):
            if method == 'submitblock':
                submitted.set()
                deadline = time.monotonic() + 2
                while not all(result['observations'].values()) and time.monotonic() < deadline:
                    time.sleep(0.001)
                self.assertTrue(all(result['observations'].values()))
                return None
            return fixture['hash'] if method == 'getbestblockhash' else fixture['raw']

        def observer_rpc(*_args, **_kwargs):
            def call(method, params=None):
                self.assertTrue(submitted.wait(2))
                return fixture['hash']
            return SimpleNamespace(call=call, conn=SimpleNamespace(sock=None, close=lambda: None))

        def preload_receipt(start_ns):
            self.assertEqual(start_ns, result['start_ns'])
            self.assertIn('end_ns', result)
            self.assertIn('b_best_tip_observed_ns', result)
            return dict(binary_preloads=True, preload_wait=True, preload_done_unix_ns=1,
                        preload_done_ns=start_ns - 10, preload_done_relative_ns=-10)

        nodes = {name: SimpleNamespace(rpc_addr=name, rpc=SimpleNamespace(call=direct_call),
                                      preload_receipt=preload_receipt)
                 for name in ('A', 'B', 'C')}
        with patch.object(relay.h, 'RPC', observer_rpc):
            relay.measure(nodes, fixture, SimpleNamespace(timeout=2, poll_ms=0.1), result)
        for name in ('b', 'c'):
            self.assertLess(result[f'{name}_best_tip_observed_ns'], result['sender_submit_end_ns'])
        self.assertEqual(result['latency_ns'], result['c_best_tip_latency_ns'])
        self.assertEqual(result['b_to_c_observed_ns'],
                         result['c_best_tip_observed_ns'] - result['b_best_tip_observed_ns'])
        self.assertEqual(set(result['preload']), {'A', 'B', 'C'})
        for receipt in result['preload'].values():
            self.assertTrue(receipt['binary_preloads'])
            self.assertTrue(receipt['preload_wait'])
            self.assertEqual(receipt['preload_done_relative_ns'], -10)

    def test_chain_warm_cold_preparation(self):
        nodes = {}
        corpus = [dict(txid='tx', raw='ff', bytes=1)]
        fixture = dict(parent='old', corpus=corpus)

        class FakeNode:
            def __init__(self, _binary, directory, _arm, _timeout, miner):
                assert miner is None
                self.name, self.p2p_addr, self.pool, self.peers = directory.name, directory.name, [], []
                self.rpc = SimpleNamespace(call=self.call)
                nodes[self.name] = self
                directory.mkdir(exist_ok=True)
                self.config = directory / 'config.toml'
                self.config.write_text('max_connections_per_ip = 16\n')

            def __enter__(self):
                limit = 2 if self.name == 'B' else 1
                assert self.config.read_text() == f'max_connections_per_ip = {limit}\n'
                return self

            def __exit__(self, *_):
                pass

            def wait(self, predicate, *_):
                assert predicate()

            def call(self, method, params=None):
                if method == 'getrawmempool':
                    return [tx['txid'] for tx in self.pool]
                if method == 'getbestblockhash':
                    return 'old'
                if method == 'getpeerinfo':
                    return self.peers
                assert method == 'addnode', 'no templates or proposals permitted'
                self.peers.append(dict(addr=params[0], inbound=False))
                nodes[params[0]].peers.append(dict(addr=self.name, inbound=True))

        for temperature in ('cold', 'warm'):
            args = SimpleNamespace(timeout=2, poll_ms=0.1, temperature=temperature, settle_seconds=20)
            result = {}

            def measure(prepared, _fixture, _args, receipt):
                expected = corpus if temperature == 'warm' else []
                self.assertEqual(prepared['A'].pool, [])
                self.assertEqual(prepared['B'].pool, expected)
                self.assertEqual(prepared['C'].pool, expected)
                receipt['start_ns'] = time.perf_counter_ns()

            with tempfile.TemporaryDirectory() as directory, patch.object(relay, 'Node', FakeNode), \
                    patch.object(relay.h, 'replay'), patch.object(relay, 'measure', measure), \
                    patch.object(relay.time, 'sleep') as settle, \
                    patch.object(relay, 'admit', lambda node, txs: node.pool.extend(txs)):
                relay.case('unused', 'test', fixture, [], args, Path(directory), result)
                settle.assert_called_once_with(20)
            self.assertEqual(result['mempool_txids']['B'], ['tx'] if temperature == 'warm' else [])
            self.assertIn('peers_after_measurement', result)

    def test_interleaved_campaign_matrices(self):
        for script, fixtures, variants in (
            ('tip-to-template-miner.sh', {'empty-2000', 'mixed-250000', 'mixed-1900000'},
             {'transparent', 'orchard', 'sapling'}),
            ('relay-chain.sh', {'empty-2000', 'transparent-signed-1900000', 'orchard-1900000', 'mixed-1900000'},
             {'cold', 'warm'}),
        ):
            lines = subprocess.check_output(['bash', str(tip.ROOT / script), '--dry-run',
                                              'a:binary-a', 'b:binary-b'], text=True).splitlines()
            self.assertEqual(shlex.split(lines[0]), ['bash', 'wait-quiet.sh'])
            commands = [shlex.split(line) for line in lines[1:-1]]
            per_rep = len(fixtures) * len(variants) * 2
            self.assertEqual(len(commands), per_rep * 5)
            self.assertEqual(len({command[-1] for command in commands}), len(commands))
            option = '--miner' if script.startswith('tip-') else '--temperature'
            for rep in range(5):
                group = commands[rep * per_rep:(rep + 1) * per_rep]
                self.assertEqual([command[-3] for command in group[:2]], ['a', 'b'] if rep % 2 == 0 else ['b', 'a'])
                self.assertEqual({command[-2] for command in group}, fixtures)
                self.assertEqual({command[command.index(option) + 1] for command in group}, variants)
            for command in commands:
                self.assertEqual(command[command.index('--settle-seconds') + 1], '20')
                fixture_dir = command[command.index('--fixtures') + 1]
                expected = (Path(os.environ.get('BENCH_SIGNED_FIXTURES',
                                 tip.h.BENCH_ROOT / 'fixtures/transparent-signed')).expanduser().resolve()
                            if command[-2].startswith('transparent-signed-') else tip.h.FIXTURES)
                self.assertEqual(Path(fixture_dir), expected)
                if script.startswith('tip-'):
                    self.assertEqual(command[command.index('--temperature') + 1], 'warm')

    def test_zakura_nu5_skips_shielded_payouts(self):
        lines = subprocess.check_output(['bash', str(tip.ROOT / 'tip-to-template-miner.sh'),
                                          '--dry-run', 'release:binary-a', 'zakura-main:binary-b'],
                                        text=True).splitlines()
        commands = [shlex.split(line) for line in lines[1:-1]]
        zakura = [command for command in commands if command[-3] == 'zakura-main']
        self.assertEqual(len(zakura), 5 * 3)
        self.assertTrue(all(command[command.index('--miner') + 1] == 'transparent' for command in zakura))


if __name__ == '__main__':
    unittest.main()
