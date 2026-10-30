#!/usr/bin/env python3
"""Offline unittest checks: synthetic inputs only, no node/cache/build prerequisites.

Optional PRUNING_COMPACTOR and SNAPSHOT_COMPACTOR executable paths enable tiny
RocksDB smoke/containment checks; they never access an existing node database.
"""
import base64
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

sys.dont_write_bytecode = True
import measure
import report

SCRIPT = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('snapshot_refresh', SCRIPT / 'snapshot-production/refresh.py')
refresh = importlib.util.module_from_spec(spec)
spec.loader.exec_module(refresh)


def synthetic_block(previous, marker):
    # Only the header/hash parser is exercised; this is not a valid consensus block.
    header = bytearray(140)
    header[4:36] = bytes.fromhex(previous)[::-1]
    header[36] = marker
    return (header + b'\x03abc\x00').hex()


class Smoke(unittest.TestCase):
    def test_journal_and_resumable_clone(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            journal = root / 'journal.jsonl'
            journal.write_bytes(b'{"size": 1}\n{"partial":')
            self.assertEqual(measure.rows(journal), [{'size': 1}])
            measure.append(journal, {'size': 2})
            self.assertEqual(measure.rows(journal), [{'size': 1}, {'size': 2}])
            source, target = root / 'source', root / 'target'
            source.mkdir()
            (source / 'sample').write_bytes(b'synthetic clone')
            # Test the clone journal without requiring a reflink-capable filesystem.
            with patch('front_common.reflink_copy', side_effect=shutil.copytree) as copy:
                measure.clone(source, target)
                measure.clone(source, target)
                copy.assert_called_once()
            self.assertEqual((target / 'sample').read_bytes(), b'synthetic clone')
            with self.assertRaisesRegex(RuntimeError, 'clone source changed'):
                measure.clone(root / 'other-source', target)

    def test_download_verifies_checksum_even_at_expected_size(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(measure, 'run') as run:
            root = Path(temp)
            data = b'synthetic archive'
            expected = hashlib.sha256(data).hexdigest()
            manifest, checksum = root / 'manifest.json', root / 'checksum'
            measure.save(manifest, {'archive': {'filename': 'snapshot', 'size_bytes': len(data),
                                               'sha256': expected, 'url': 'https://example.invalid/snapshot'}})
            checksum.write_text(expected + '  snapshot\n')
            args = measure.parser().parse_args(['download', '--output', temp, '--manifest', str(manifest),
                                                '--checksum', str(checksum)])
            (root / 'snapshot').write_bytes(data)
            with patch.object(measure, 'ROOT', root):
                receipt = {}
                measure.download(args, receipt)
                self.assertEqual(receipt['snapshot']['sha256'], expected)
                (root / 'snapshot').write_bytes(b'x' * len(data))
                with self.assertRaisesRegex(RuntimeError, 'SHA256 mismatch'):
                    measure.download(args, {})
            run.assert_not_called()

    def test_feeder_auth_chain_and_resume(self):
        first = synthetic_block('00' * 32, 1)
        first_hash = measure.block_check(first)['hash']
        second = synthetic_block(first_hash, 2)
        second_hash = measure.block_check(second)['hash']
        blocks = {101: (first_hash, first), 102: (second_hash, second)}
        requests = []

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                requests.append((request, self.headers.get('Authorization')))
                method, params = request['method'], request['params']
                if method == 'getblockchaininfo':
                    result = {'chain': 'main', 'blocks': 102}
                elif method == 'getblockhash':
                    result = blocks[params[0]][0]
                else:
                    result = next(raw for block_hash, raw in blocks.values() if block_hash == params[0])
                body = json.dumps({'result': result, 'error': None}).encode()
                self.send_response(200)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                cookie = root / 'cookie'
                cookie.write_text('synthetic:authentication')
                args = measure.parser().parse_args([
                    'fetch', '--output', temp, '--start-height', '100', '--blocks', '2',
                    '--feeder-url', f'http://127.0.0.1:{server.server_port}', '--feeder-cookie', str(cookie)])
                with patch.object(measure, 'ROOT', root):
                    receipt = {}
                    measure.fetch(args, receipt)
                    self.assertEqual(receipt['fetched'], 2)
                    self.assertEqual(receipt['parent_hash'], '00' * 32)
                    self.assertEqual(receipt['tip_hash'], second_hash)
                    count = len(requests)
                    measure.fetch(args, receipt)
                    self.assertEqual(receipt['reused'], 2)
                    self.assertEqual(len(requests), count)
                    args.start_height = None
                    with self.assertRaisesRegex(RuntimeError, 'stored finalized tip'):
                        measure.fetch(args, {})
                expected = 'Basic ' + base64.b64encode(b'synthetic:authentication').decode()
                self.assertTrue(all(auth == expected for _, auth in requests))
                self.assertNotIn('synthetic:authentication', json.dumps(receipt))
                self.assertEqual({request['method'] for request, _ in requests},
                                 {'getblockchaininfo', 'getblockhash', 'getblock'})
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
        with self.assertRaisesRegex(RuntimeError, 'hash mismatch'):
            measure.block_check(first, 'ff' * 32)
        with self.assertRaisesRegex(RuntimeError, 'embedded credentials'):
            measure.feeder_endpoint('http://user:secret@example.invalid')

    def test_paired_summary_and_synthetic_report(self):
        pairs = [dict(height=100 + i, status='ok', deleting=True,
                      timings=dict(archive=float(i + 1), pruned=float(i + 3))) for i in range(12)]
        summary = report.summarize_pairs(pairs, 'during')
        self.assertEqual(summary['n'], 12)
        self.assertEqual(summary['paired']['median_paired_delta_ms'], 2)
        self.assertEqual(summary['paired']['ci95_ms'], [2, 2])
        self.assertEqual(report.summarize_pairs([dict(pairs[0], deleting=False)], 'during')['n'], 0)
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            report.summarize_pairs([pairs[0], pairs[0]], 'after')
        with self.assertRaisesRegex(ValueError, 'failed samples'):
            report.summarize_pairs([dict(pairs[0], status='failed')], 'after')
        fake_size = dict(du_sb=1_000_000, allocated_referenced_bytes=1_010_000,
                         sst_bytes=900_000, btrfs_du='synthetic')
        pruning = dict(binary=dict(version='SYNTHETIC', sha256='0' * 64), before=fake_size,
                       after_deletion=fake_size, after_compaction=dict(fake_size, du_sb=600_000),
                       deletion=dict(requested_floor=90_001, witness_height=100_000, target_floor=90_001,
                                     final=dict(pruneheight=90_001), startup_to_floor_seconds=60,
                                     observed_deletion_seconds=50),
                       compaction=dict(method='SYNTHETIC forced', seconds=10, forced=True))
        receipts = [dict(phase=phase, status='ok', synthetic=True, started_at='2000-01-01T00:00:00Z',
                         wall_seconds=1, **fields) for phase, fields in [
            ('extract', dict(result=dict(extraction_seconds=20, size=fake_size))),
            ('prune', dict(result=pruning)),
            ('replay', dict(results={name: dict(summary=summary, actual_start_height=99)
                                    for name in ('during', 'after')})),
        ]]
        text = report.render(receipts)
        for expected in ('SYNTHETIC SMOKE INPUT', '40.00%', 'measured regression'):
            self.assertIn(expected, text)

    def test_snapshot_info_passes_explicit_root(self):
        with patch.object(refresh.subprocess, 'run') as run:
            run.return_value.stdout = '{"finalized_height": 100}'
            self.assertEqual(refresh.stored(Path('tool'), Path('root/state/db'), Path('root')),
                             {'finalized_height': 100})
            self.assertEqual(run.call_args.args[0],
                             ['tool', '--allowed-root', 'root', '--info', 'root/state/db'])

    def test_optional_compactors(self):
        binaries = [os.environ[name] for name in ('PRUNING_COMPACTOR', 'SNAPSHOT_COMPACTOR')
                    if os.environ.get(name)]
        if not binaries:
            self.skipTest('set PRUNING_COMPACTOR and/or SNAPSHOT_COMPACTOR for tiny RocksDB checks')
        for binary in binaries:
            with self.subTest(binary=binary), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                allowed, outside = root / 'allowed', root / 'outside'
                allowed.mkdir()
                outside.mkdir()
                link = allowed / 'link'
                link.symlink_to(outside, target_is_directory=True)
                for target in (outside / 'db', allowed, link, link / 'db', allowed / '..' / 'outside' / 'db'):
                    result = subprocess.run([binary, '--allowed-root', str(allowed), '--smoke', str(target)],
                                            capture_output=True, text=True, timeout=60)
                    self.assertNotEqual(result.returncode, 0, result.stdout)
                    self.assertIn('allowed-root', result.stderr)
                subprocess.run([binary, '--allowed-root', str(allowed), '--smoke', str(allowed / 'small-db')],
                               check=True, capture_output=True, text=True, timeout=600)


if __name__ == '__main__':
    unittest.main()
