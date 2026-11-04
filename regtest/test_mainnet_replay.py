#!/usr/bin/env python3
"""Offline replay contract checks; no node, state database, or build required."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import mainnet_replay as replay
from front_common import clean_env


class ReplayContracts(unittest.TestCase):
    def test_inherited_config_cannot_enable_checkpoints_or_peers(self):
        with patch.dict(replay.os.environ, {
                'ZAKURA_CONSENSUS__CHECKPOINT_SYNC': 'true',
                'ZAKURA_NETWORK__INITIAL_MAINNET_PEERS': 'example.com:8233',
                'ZEBRA_CONSENSUS__CHECKPOINT_SYNC': 'true',
                'RUST_LOG': 'error', 'PATH': '/preserved'}, clear=True):
            self.assertEqual(clean_env(), {'PATH': '/preserved', 'NO_COLOR': '1', 'TERM': 'dumb'})

    def arms(self, extra=False):
        sources = dict(replay.SOURCES)
        if extra:
            sources['extra'] = 'a' * 40
        environment = {'CARGO_PROFILE_RELEASE_' + key: value
                       for key, value in replay.PROFILE_SETTINGS.items()}
        environment['RUSTFLAGS'] = '-C codegen-units=1'
        return [SimpleNamespace(label=label, provenance=dict(
            source=source, build='cargo build --release' + (' --features pasta-asm' if label == 'crypto' else ''),
            compiler_settings=dict(rustc_verbose_version='rustc test', environment=dict(environment))))
            for label, source in sources.items()]

    def test_sources_features_and_compiler_parity(self):
        arms = self.arms()
        self.assertEqual(replay.validate_arms(arms), replay.SOURCES)
        extra = self.arms(extra=True)
        self.assertEqual(replay.validate_arms(extra, 'a' * 40)['extra'], 'a' * 40)
        for changed, revision, message in (
                (extra, None, 'exactly these arms'),
                (arms, 'short', 'full commit hash'),
                (extra, 'b' * 40, 'wrong source pin')):
            with self.assertRaisesRegex(RuntimeError, message):
                replay.validate_arms(changed, revision)
        changed = copy.deepcopy(arms)
        changed[0].provenance['build'] += ' --features pasta-asm'
        with self.assertRaisesRegex(RuntimeError, 'Pasta feature'):
            replay.validate_arms(changed)
        changed = copy.deepcopy(arms)
        changed[0].provenance['compiler_settings']['environment']['RUSTFLAGS'] = '-C opt-level=1'
        with self.assertRaisesRegex(RuntimeError, 'matching compiler_settings'):
            replay.validate_arms(changed)
        for arm in changed:
            arm.provenance['compiler_settings']['environment'].pop('CARGO_PROFILE_RELEASE_PANIC')
        with self.assertRaises(RuntimeError):
            replay.validate_arms(changed)
        for arm in arms:
            arm.provenance['compiler_settings']['environment']['RUSTFLAGS'] = '-C codegen-units=16'
        with self.assertRaisesRegex(RuntimeError, 'RUSTFLAGS'):
            replay.validate_arms(arms)

    def test_corpus_hashes_chain_and_window(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            parent = previous = '00' * 32
            for height in (101, 102):
                header = bytearray(140)
                header[4:36] = bytes.fromhex(previous)[::-1]
                header[36] = height
                # A synthetic header tests the wire/hash parser, not consensus validity.
                raw = (header + b'\x03abc\x00').hex()
                metadata = dict(height=height, **replay.block_check(raw))
                (root / f'{height}.hex').write_text(raw)
                (root / f'{height}.json').write_text(json.dumps(metadata))
                previous = metadata['hash']
            (root / 'corpus.json').write_text(json.dumps(dict(
                start_height=100, end_height=102, parent_hash=parent, tip_hash=previous)))
            _, blocks = replay.read_corpus(root, 100, 102)
            self.assertEqual([row['height'] for row in blocks], [101, 102])
            with self.assertRaisesRegex(RuntimeError, 'does not cover'):
                replay.read_corpus(root, 99, 102)
            (root / '102.hex').write_text((root / '101.hex').read_text())
            with self.assertRaisesRegex(RuntimeError, 'hash mismatch'):
                replay.read_corpus(root, 100, 102)

    def test_three_run_aggregation_and_tip_guard(self):
        results, samples = [], {}
        for label in replay.SOURCES:
            samples[label] = []
            for value in (1, 2, 9):
                results.append(dict(arm=label, submit_total_s=value, replay_wall_s=value,
                                    replay_cpu_s=value * 2, process_cpu_s=value * 3))
                samples[label].append([dict(height=101, hash='abc', submit_ms=value, accepted_ms=value + 1)])
        report, medians = replay.aggregate(results, samples)
        self.assertEqual(medians['stack'][0]['submit_ms'], 2)
        self.assertEqual(medians['stack'][0]['accepted_ms'], 3)
        self.assertEqual(report['stack']['submit_total_s_mean'], 4)
        self.assertEqual(report['stack']['replay_cpu_s_mean'], 8)
        samples['stack'][2][0]['height'] = 102
        with self.assertRaisesRegex(RuntimeError, 'different heights'):
            replay.aggregate(results, samples)

        class Node:
            responses = {'getpeerinfo': [], 'getrawmempool': [],
                         'getblockchaininfo': {'chain': 'main', 'blocks': 100},
                         'getbestblockhash': 'parent'}

            def rpc(self, method):
                return self.responses[method]

        node = Node()
        replay.isolated_tip(node, 100, 'parent')
        with self.assertRaisesRegex(RuntimeError, 'expected Mainnet tip'):
            replay.isolated_tip(node, 101, 'parent')
        node.responses['getpeerinfo'] = [{'addr': '127.0.0.1:8233'}]
        with self.assertRaisesRegex(RuntimeError, 'has peers'):
            replay.isolated_tip(node, 100, 'parent')

    def test_replay_requires_committed_tip_and_records_cpu(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / '3505701.hex').write_text('00')
            arm = self.arms()[0]
            arm.path = root / 'unused-binary'
            block = dict(height=replay.START + 1, hash='accepted', previous_hash='parent')
            args = SimpleNamespace(output=root, corpus=root, cpus='0',
                                   startup_timeout=1, rpc_timeout=1)
            node = MagicMock()
            tips = iter(['parent', 'accepted', 'accepted'])
            infos = iter([dict(chain='main', blocks=replay.START),
                          dict(chain='main', blocks=replay.START + 1)])

            def rpc(method, params=None):
                if method in ('getpeerinfo', 'getrawmempool'):
                    return []
                if method == 'getblockchaininfo':
                    return next(infos)
                if method == 'getbestblockhash':
                    return next(tips)
                self.assertEqual((method, params), ('submitblock', ['00']))
                return None

            node.rpc.side_effect = rpc
            node.preload_receipt.return_value = {}
            node.sample.side_effect = [dict(cpu_s=2), dict(cpu_s=7)]
            node.wait_exit.return_value = 0
            node.exit_resources = dict(cpu_s=9)
            node.exec_ns, node.exit_ns = 0, 10_000_000_000
            with patch.object(replay, 'MainnetNode', return_value=node) as constructor, \
                    patch.object(replay, 'reflink_copy', side_effect=lambda src, dst: dst.mkdir()):
                result, samples = replay.replay_run(args, arm, root / 'snapshot', [block], 0)
            self.assertIn(('consensus', 'checkpoint_sync', 'false'), constructor.call_args.kwargs['edits'])
            self.assertEqual((result['replay_cpu_s'], result['process_cpu_s']), (5, 9))
            self.assertEqual(result['status'], 'ok')
            self.assertGreaterEqual(samples[0]['accepted_ms'], samples[0]['submit_ms'])
            self.assertFalse((root / 'r1-stack/cache').exists())
            self.assertTrue((root / 'r1-stack/blocks.csv').is_file())
            node.stop.assert_called_once_with(timeout=300)

    def test_smoke_extra_arm_and_cpu_isolation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / 'output'
            arms = self.arms(extra=True)
            blocks = [dict(height=replay.START + 1, hash='abc')]
            argv = ['--provenance', str(root / 'unused.json'), '--corpus', str(root),
                    '--output', str(output), '--extra-revision', 'a' * 40, '--smoke']
            for arm in arms:
                argv.extend(['--snapshot', f'{arm.label}={root / arm.label}'])
                (root / arm.label).mkdir()

            def run(args, arm, snapshot, blocks, repetition):
                result = dict(arm=arm.label, submit_total_s=1, replay_wall_s=1,
                              replay_cpu_s=2, process_cpu_s=3)
                return result, [dict(blocks[0], submit_ms=1, accepted_ms=2)]

            with patch.object(replay, 'load_arms', return_value=arms), \
                    patch.object(replay, 'check_cpus', return_value=[8]), \
                    patch.object(replay.os, 'sched_setaffinity') as affinity, \
                    patch.object(replay, 'read_corpus', return_value=({}, blocks)), \
                    patch.object(replay, 'replay_run', side_effect=run) as replay_run, \
                    patch('builtins.print'):
                with self.assertRaisesRegex(RuntimeError, 'require --exclusive'):
                    replay.main(argv)
                self.assertFalse(output.exists())
                replay.main([*argv, '--exclusive'])
            affinity.assert_called_once_with(0, [8])
            self.assertEqual(replay_run.call_count, 5)
            manifest = json.loads((output / 'campaign.json').read_text())
            self.assertEqual(manifest['order'], [list(replay.SOURCES) + ['extra']])
            self.assertTrue(manifest['exclusive'])
            self.assertIn('extra', json.loads((output / 'summary.json').read_text()))


if __name__ == '__main__':
    unittest.main()
