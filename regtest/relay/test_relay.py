"""Direct-relay assertions from the retained B checks, without duplicate builders."""
import copy
import json
from pathlib import Path
import tempfile
import tomllib
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import run_sweep
from common import Node, MEMPOOL_TX_COST_LIMIT, file_hash, save, sources


class RelayTests(unittest.TestCase):
    def test_preload_receipts_follow_timing(self):
        fixture = dict(parent='old', hash='new', raw='00')
        metadata = dict(binary_preloads=True, preload_wait=True, preload_done_unix_ns=1,
                        preload_done_ns=5, preload_done_relative_ns=-5)
        node = SimpleNamespace(rpc=SimpleNamespace(call=lambda *_: None))
        with patch.object(run_sweep.time, 'perf_counter_ns', side_effect=[10, 40]) as clock:
            def preload_receipt(start_ns):
                self.assertEqual(start_ns, 10)
                self.assertEqual(clock.call_count, 2)
                return metadata
            node.preload_receipt = preload_receipt
            result = run_sweep.timed_submit(node, fixture)
        self.assertEqual(result['latency_ns'], 30)
        self.assertEqual(result['preload'], metadata)
        submitted, observed, receipts = [], [], {}

        def call(method, params=None):
            if method == 'submitblock':
                submitted.append(True)
            elif method == 'getpeerinfo':
                return [{}]
            elif method == 'getrawmempool':
                return []
            elif method == 'getbestblockhash':
                if submitted:
                    observed.append(True)
                    return 'new'
                return 'old'
            elif method == 'getblock':
                return '00'
            else:
                self.assertEqual(method, 'addnode')

        nodes = {}
        for name in ('sender', 'receiver'):
            def preload_receipt(start_ns, name=name):
                self.assertTrue(submitted and observed)
                receipts[name] = start_ns
                return dict(metadata, preload_done_ns=start_ns - 5)
            nodes[name] = SimpleNamespace(p2p_addr=name, rpc=SimpleNamespace(call=call),
                                          wait=lambda predicate, *_: predicate(),
                                          preload_receipt=preload_receipt)
        with patch.object(run_sweep.time, 'sleep'):
            result = run_sweep.relay(nodes['sender'], nodes['receiver'], fixture,
                                     SimpleNamespace(timeout=2, poll_ms=1, settle_seconds=0))
        self.assertEqual(receipts, dict(sender=result['start_ns'], receiver=result['start_ns']))
        self.assertEqual(set(result['preload']), {'sender', 'receiver'})
        self.assertTrue(all(item['preload_done_relative_ns'] == -5 for item in result['preload'].values()))
        self.assertEqual(result['latency_ns'], result['end_ns'] - result['start_ns'])

    def test_resume_and_config(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / 'run'
            manifest = dict(arms={'zebra-main': {'sha256': 'binary'}},
                            fixture_manifest_sha256='fixtures', sources={'driver': 'source'},
                            seed=20261015, schedule=[{'round': 0, 'arms': ['zebra-main']}])
            digest = run_sweep.checkpoint_manifest(output, manifest)
            self.assertEqual(run_sweep.checkpoint_manifest(output, manifest), digest)
            for key in manifest:
                changed = copy.deepcopy(manifest)
                changed[key] = 'changed'
                with self.assertRaises(RuntimeError):
                    run_sweep.checkpoint_manifest(output, changed)
            identity = dict(round=0, repetition=0, arm='zebra-main', fixture='empty-2000',
                            endpoint='submit-to-peer-tip', manifest_sha256=digest)
            path = output / 'observations' / 'case.json'
            self.assertIsNone(run_sweep.load_receipt(path, identity))
            failed = dict(identity, status='error', error='synthetic failure', attempt='first')
            historical = output / 'failures' / 'case-first.json'
            save(historical, failed)
            save(path, failed)
            self.assertEqual(run_sweep.load_receipt(path, identity), failed)
            self.assertFalse(run_sweep.needs_attempt(failed, False))
            self.assertTrue(run_sweep.needs_attempt(failed, True))
            historical_hash = file_hash(historical)
            successful = dict(identity, status='ok', start_ns=10, end_ns=40, latency_ns=30, attempt='second',
                              preload=dict(binary_preloads=True, preload_wait=True, preload_done_unix_ns=1,
                                           preload_done_ns=5, preload_done_relative_ns=-5))
            save(path, successful)
            self.assertEqual(run_sweep.load_receipt(path, identity), successful)
            self.assertFalse(run_sweep.needs_attempt(successful, True))
            self.assertEqual(file_hash(historical), historical_hash)
            with self.assertRaises(RuntimeError):
                run_sweep.load_receipt(path, dict(identity, arm='zakura-main'))
            save(path, dict(successful, latency_ns=0))
            with self.assertRaises(RuntimeError):
                run_sweep.load_receipt(path, identity)
            (output / 'observations.jsonl').write_text('{"partial":')
            run_sweep.write_observations(output, [successful])
            self.assertEqual([json.loads(line) for line in (output / 'observations.jsonl').read_text().splitlines()], [successful])
            node = Node('/unused-binary', root / 'zakura', 'zakura-main')
            self.assertIsNone(node.proc)
            config = tomllib.loads(node.config.read_text())
            self.assertEqual(config['mempool']['tx_cost_limit'], MEMPOOL_TX_COST_LIMIT)
            self.assertEqual(config['network']['p2p_stack'], 'legacy')
        self.assertIn('protocol/fixture.py', sources())


if __name__ == '__main__':
    unittest.main()
