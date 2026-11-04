"""Offline public-source closure checks; no nodes, builds, sockets or downloads."""
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import struct
import subprocess
import sys
import tempfile
import tomllib
import unittest
from unittest.mock import patch

from common import h, sources, PARSER
from protocol.fixture import parse_v4, take, varint
import transparent_signed as signed
import v5_campaign as campaign
import sync_replay


class PublicationTests(unittest.TestCase):
    def test_v4_parser_offsets_and_bounds(self):
        raw = (struct.pack('<II', 0x80000004, 0x892F2085) + b'\x01'
               + bytes(range(32)) + struct.pack('<I', 7) + b'\x02\x01\x51'
               + b'\xff' * 4 + b'\x01' + struct.pack('<q', 1234) + b'\x01\x51'
               + struct.pack('<IIq', 12, 34, -5) + b'\x00\x00\x00')
        tx, end = parse_v4(raw + b'extra')
        self.assertEqual(end, len(raw))
        self.assertEqual(tx['inputs'], [dict(txid=bytes(range(32))[::-1].hex(), index=7,
                                          script='0151', script_range=(46, 48), sequence='ffffffff')])
        self.assertEqual(tx['outputs'], [dict(value=1234, script='51')])
        self.assertEqual((tx['locktime'], tx['expiry'], tx['valuebalance']), (12, 34, -5))
        self.assertFalse(any(tx[k] for k in ('sapling_spends', 'sapling_outputs', 'joinsplits')))
        # All three shielded layout branches, including both trailing signatures.
        bundled = raw[:-3] + b'\x01' + bytes(384) + b'\x01' + bytes(948)
        bundled += b'\x01' + bytes(1698 + 32 + 64 + 64)
        parsed, end = parse_v4(bundled)
        self.assertEqual(end, len(bundled))
        self.assertEqual([parsed[k] for k in ('sapling_spends', 'sapling_outputs', 'joinsplits')], [1, 1, 1])
        for encoded in (raw, bundled):
            for size in range(len(encoded)):
                with self.assertRaises(ValueError):
                    parse_v4(encoded[:size])
        with self.assertRaises(ValueError):
            parse_v4(bytes(8))
        with self.assertRaises(ValueError):
            parse_v4(raw[:8] + b'\xff' + b'\xff' * 8)
        self.assertEqual(take(b'abc', 1, 2), (b'bc', 3))
        for pos, count in ((-1, 1), (0, -1), (2, 2), (4, 0)):
            with self.assertRaises(ValueError):
                take(b'abc', pos, count)
        for number in (0, 252, 253, 65535, 65536, 2**32 - 1, 2**32, 2**64 - 1):
            encoded = h.compact(number) if number < 2**32 else b'\xff' + struct.pack('<Q', number)
            self.assertEqual(varint(encoded, 0), (number, len(encoded)))
        for encoded in (b'', b'\xfd', b'\xfd\xfc\x00', b'\xfe\xff\xff\x00\x00', b'\xff' + bytes(8)):
            with self.assertRaises(ValueError):
                varint(encoded, 0)

    def test_unsigned_fanout_and_parser_provenance(self):
        raw, txid = signed.fanout(('01' * 32, 3, 1_000_000), [200_000, 300_000])
        parsed, end = parse_v4(raw)
        self.assertEqual(end, len(raw))
        self.assertEqual(txid, h.dsha(raw)[::-1].hex())
        self.assertEqual(parsed['inputs'][0]['script'], '0151')
        self.assertEqual([out['value'] for out in parsed['outputs']], [200_000, 300_000])
        self.assertEqual(sources()['protocol/fixture.py'], hashlib.sha256(PARSER.read_bytes()).hexdigest())

    def test_affinity_and_port_defaults(self):
        with patch.dict(os.environ, {'BENCH_PORT_BASE': '0'}, clear=False), patch.object(h, '_port_offset', 0):
            self.assertEqual([h.next_port(), h.next_port()], [0, 0])
        with patch.dict(os.environ, {'BENCH_PORT_BASE': '22000'}, clear=False), patch.object(h, '_port_offset', 0):
            config = tomllib.loads(h.config_for('zebrad', '"state"', h.P2SH_MINER))
            self.assertEqual(config['network']['listen_addr'], '127.0.0.1:22000')
            self.assertEqual(config['rpc']['listen_addr'], '127.0.0.1:22001')
        with patch.dict(os.environ, {'BENCH_PORT_BASE': '65535'}, clear=False), patch.object(h, '_port_offset', 0):
            self.assertEqual(h.next_port(), 65535)
            with self.assertRaises(ValueError):
                h.next_port()
        with patch.object(os, 'sched_getaffinity', return_value={3, 5}):
            self.assertEqual(sync_replay.check_cpus('all'), [3, 5])
        arms = {name: '/unused/' + name for name in campaign.LABELS}
        with patch.dict(os.environ, {'V5_CPUS': '', 'SOURCE_CPUS': ''}, clear=False):
            pieces = campaign.campaign_plan(arms, Path('/unused-output'), 'core')
            self.assertEqual(len(pieces), 46)
            command = pieces['replay-r3'][0][0]
            self.assertNotIn('taskset', command)
            self.assertEqual(command[command.index('--cpus') + 1], 'all')
        with patch.dict(os.environ, {'V5_CPUS': '3', 'SOURCE_CPUS': '5'}, clear=False):
            pieces = campaign.campaign_plan(arms, Path('/unused-output'), 'core')
            self.assertEqual(pieces['smoke'][0][0][:3], ['taskset', '-c', '3'])
            replay = pieces['replay-r1'][0][0]
            self.assertEqual(replay[replay.index('--source-cpus') + 1], '5')
            self.assertNotIn('taskset', replay)

    def test_source_port_range(self):
        argv = ['--provenance', 'arms.json', '--arm', 'a=unused', '--snapshot', 'snapshot',
                '--source-state', 'source', '--start-height', '0', '--end-height', '10',
                '--mode', 'checkpoint', '--output', 'output']
        args = sync_replay.parse_args(argv + ['--source-port-base', '21000'])
        self.assertEqual(args.source_port_base, 21000)
        self.assertEqual((args.cpus, args.source_cpus), ('all', 'all'))
        for invalid in ('0', '-1', '65534'):
            with patch.object(sys, 'stderr', io.StringIO()), self.assertRaises(SystemExit):
                sync_replay.parse_args(argv + ['--source-port-base', invalid])

    def test_phase_provenance_rejected_before_output(self):
        path = campaign.BENCH / 'phase-probe.py'
        spec = importlib.util.spec_from_file_location('publication_phase_probe', path)
        probe = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(probe)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / 'binary'
            binary.write_bytes(b'never executed')
            receipt = root / 'arms.json'
            receipt.write_text('{"test": {"sha256": "wrong"}}')
            output = root / 'output'
            argv = [str(path), str(binary), 'test', 'empty-2000', '1', str(output), '--provenance', str(receipt)]
            with patch.object(sys, 'argv', argv), self.assertRaises(ValueError):
                probe.main()
            self.assertFalse(output.exists())

    def test_build_receipt_compiler_settings(self):
        settings = []
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / 'binary'
            binary.write_bytes(b'never executed')
            for script in ('build-arm.sh', 'build-zakura.sh'):
                source = (campaign.BENCH / script).read_text().split("<<'PY'\n", 1)[1].rsplit('\nPY', 1)[0]
                with patch.object(sys, 'argv', ['receipt', str(binary), 'a' * 40, script]), \
                        patch.object(subprocess, 'check_output', return_value='rustc test\nhost: test\n') as compiler, \
                        patch.dict(os.environ, {'RUSTUP_TOOLCHAIN': '1.97.0', 'RUSTFLAGS': '-C opt-level=3'}, clear=True), \
                        patch('sys.stdout', io.StringIO()):
                    exec(compile(source, script, 'exec'), {})
                compiler.assert_called_once_with(['rustc', '-vV'], text=True)
                receipt = json.loads(Path(str(binary) + '.build.json').read_text())
                self.assertEqual(receipt['build'], script)
                settings.append(receipt['compiler_settings'])
            self.assertEqual(settings[0], settings[1])
            self.assertEqual(settings[0]['rustc_verbose_version'], 'rustc test\nhost: test')
            self.assertEqual(settings[0]['environment']['RUSTFLAGS'], '-C opt-level=3')
            self.assertIsNone(settings[0]['environment']['CFLAGS'])

    def test_shell_usage_never_starts_work(self):
        for script in ('build-arm.sh', 'build-zakura.sh', 'wait-quiet.sh', 'v5-campaign.sh',
                       'tip-to-template.sh', 'tip-to-template-miner.sh', 'relay-chain.sh'):
            result = subprocess.run(['bash', str(campaign.BENCH / script), '--help'],
                                    capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('usage:', result.stdout.lower())


if __name__ == '__main__':
    unittest.main()
