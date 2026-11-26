#!/usr/bin/env python3
"""Offline long-prefix generation, checkpoint isolation and replay receipts."""
import argparse
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import build_fixtures as builder
from common import h
import transparent_signed as signed


class LongChainTests(unittest.TestCase):
    def test_funding_padding_precedes_corpus(self):
        for minimum, expected in ((0, 213), (1100, 1100), (100, 213)):
            with self.subTest(minimum=minimum), tempfile.TemporaryDirectory() as directory:
                height = 0
                wallet = Mock(commands=[])
                value = 100_000_000 - h.FEE

                def mine(count):
                    nonlocal height
                    hashes = [str(n) for n in range(height + 1, height + count + 1)]
                    height += count
                    return hashes

                def rpc(method, params=None):
                    if method == 'getnewaddress':
                        return 't-test-miner'
                    if method == 'getnetworkinfo':
                        return {}
                    if method == 'getblockcount':
                        return height
                    if method == 'getblockhash':
                        return str(params[0])
                    if method == 'getblock':
                        return {'tx': ['coinbase-' + params[0]]} if params[1] else '00'
                    if method == 'getrawtransaction':
                        return dict(txid=params[0], vout=[dict(
                            n=0, valueZat=100_000_000,
                            scriptPubKey={'addresses': ['t-test-miner']})])
                    if method == 'createrawtransaction':
                        return 'unsigned'
                    if method == 'signrawtransaction':
                        return dict(complete=True, hex='funding')
                    if method == 'decoderawtransaction':
                        return dict(txid='funded', vin=[{}], vout=[dict(
                            valueZat=value, scriptPubKey={'hex': h.SCRIPT.hex()})])
                    if method == 'sendrawtransaction':
                        return 'funded' if params[0] == 'funding' else 'fanout'
                    if method == 'getrawmempool':
                        return []
                    raise AssertionError((method, params))

                def spend(source):
                    self.assertEqual(height, expected, 'padding must precede corpus signing')
                    return {'txid': str(source)}

                wallet.mine.side_effect = mine
                wallet.rpc.side_effect = rpc
                args = argparse.Namespace(transparent_signed=True, output=Path(directory),
                                          timeout=1, sizes=[2000, 25000], setup_blocks=minimum)
                with patch.object(builder, 'Wallet', return_value=wallet), \
                        patch.object(builder, 'build_inputs', return_value={}), \
                        patch.object(builder, 'block_identity', return_value={'hash': 'mock'}), \
                        patch.object(signed, 'fanout', return_value=(b'\0', 'fanout')), \
                        patch.object(signed, 'spend', side_effect=spend):
                    builder.fund(args)
                chain = json.loads((args.output / 'chain.json').read_text())
                self.assertEqual(len(chain), expected)
                self.assertEqual([entry['height'] for entry in chain], list(range(1, expected + 1)))
                self.assertEqual(wallet.mine.call_args.args, (max(0, minimum - 213),))
                self.assertEqual(len(json.loads((args.output / 'corpus.json').read_text())), 200)
                wallet.stop.assert_called()

    def test_setup_length_is_immutable_even_on_resume(self):
        original = dict(producer='same', sizes=[2000, 250000, 1900000],
                        wallet_image='same', sources={})
        self.assertEqual(builder.checkpoint_changes(original, original, False), [])
        long = dict(original, setup_blocks=1100)
        self.assertEqual(builder.checkpoint_changes(long, long, False), [])
        for previous, current in ((original, long), (long, original),
                                  (long, dict(long, setup_blocks=1200))):
            with self.subTest(previous=previous, current=current), self.assertRaisesRegex(
                    RuntimeError, 'setup chain length changed'):
                builder.checkpoint_changes(previous, current, True)

    def test_default_build_inputs_remain_compatible(self):
        args = argparse.Namespace(producer=Path('producer'), sizes=[2000, 250000, 1900000],
                                  transparent_signed=False, setup_blocks=0)
        with patch.object(builder, 'file_hash', return_value='hash'), \
                patch.object(builder, 'sources', return_value={}):
            self.assertNotIn('setup_blocks', builder.build_inputs(args))
            args.setup_blocks = 1100
            self.assertEqual(builder.build_inputs(args)['setup_blocks'], 1100)

    def test_negative_setup_height_rejected_before_nodes_start(self):
        with patch('sys.argv', ['build_fixtures.py', '--producer', 'unused', '--output', 'unused',
                                '--setup-blocks', '-1']), \
                patch.object(builder, 'Wallet') as wallet, patch('sys.stderr'):
            with self.assertRaises(SystemExit) as error:
                builder.main()
            self.assertEqual(error.exception.code, 2)
            wallet.assert_not_called()

    def test_signed_fixture_override_reaches_every_comparison_piece(self):
        import importlib.util
        import os
        path = Path(__file__).resolve().parents[1] / 'v5_campaign.py'
        spec = importlib.util.spec_from_file_location('long_chain_campaign', path)
        campaign = importlib.util.module_from_spec(spec)
        with patch.dict(os.environ, {'BENCH_SIGNED_FIXTURES': '/fixtures/signed-long'}):
            spec.loader.exec_module(campaign)
        arms = campaign.parse_arms(
            [f'{label}:unused-{label}' for label in campaign.COMPARISON_REVISIONS] +
            ['extra-ours:zebrad-ours', 'extra-other:zebrad-other'], 'comparison')
        pieces = campaign.campaign_plan(arms, Path('/output/long'), 'comparison')
        for name, (commands, _) in pieces.items():
            for command in commands:
                fixture_path = command[command.index('--fixtures') + 1]
                if name == 'smoke' and 'empty-2000' in command:
                    continue
                self.assertEqual(fixture_path, '/fixtures/signed-long', name)

    def test_replay_records_untimed_setup_duration(self):
        # Legacy replay inputs also omit per-block height metadata.
        chain = [dict(raw='raw-block', hash='tip')]
        with tempfile.TemporaryDirectory() as directory:
            node = SimpleNamespace(dir=Path(directory), rpc=Mock())
            node.rpc.call.side_effect = [None, 'tip']
            with patch.object(h.time, 'perf_counter_ns', side_effect=[100, 2100]):
                h.replay(node, chain)
            receipt = json.loads((node.dir / 'setup-replay.json').read_text())
            self.assertEqual(receipt['blocks'], 1)
            self.assertEqual(receipt['parent'], 'tip')
            self.assertEqual(receipt['elapsed_ns'], 2000)
            self.assertEqual(receipt['seconds'], 0.000002)
            self.assertEqual(node.rpc.call.call_count, 2, 'no extra RPC in setup timer')

    def test_failed_replay_has_no_success_receipt(self):
        for replies in (['rejected'], [None, 'wrong-tip']):
            with self.subTest(replies=replies), tempfile.TemporaryDirectory() as directory:
                node = SimpleNamespace(dir=Path(directory), rpc=Mock())
                node.rpc.call.side_effect = replies
                with self.assertRaises(RuntimeError):
                    h.replay(node, [dict(height=1, raw='raw', hash='tip')])
                self.assertFalse((node.dir / 'setup-replay.json').exists())


if __name__ == '__main__':
    unittest.main()
