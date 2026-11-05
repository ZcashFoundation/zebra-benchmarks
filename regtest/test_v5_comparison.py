#!/usr/bin/env python3
"""Comparison scheduling, source pins and summaries; no binaries, sockets or nodes."""
from contextlib import redirect_stdout
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import v5_campaign as campaign


class ComparisonTests(unittest.TestCase):
    def setUp(self):
        self.specs = [f'{label}:unused-{label}' for label in campaign.COMPARISON_REVISIONS]
        self.arms = campaign.parse_arms(self.specs, 'comparison')

    def test_current_arms_and_interleaved_eighteen_run_rows(self):
        with patch.dict(os.environ, {}, clear=True):
            pieces = campaign.campaign_plan(self.arms, Path('/unused'), 'comparison')
        self.assertEqual(list(pieces), ['smoke', 'peer-work-fresh', 'peer-work-settled', 'transparent-burst', 'served-submit'])
        labels = list(self.arms)
        for link, settle in (('fresh', '0'), ('settled', '20')):
            commands = pieces['peer-work-' + link][0]
            self.assertEqual(len(commands), 18 * len(labels))
            self.assertEqual(len({command[-1] for command in commands}), len(commands))
            for repetition in range(18):
                group = commands[repetition * len(labels):(repetition + 1) * len(labels)]
                self.assertEqual([command[-3] for command in group],
                                 [arm for arm, _ in campaign.rotate(self.arms, repetition)])
            for command in commands:
                self.assertEqual(command[:3], ['taskset', '-c', '0-7,16-23'])
                self.assertEqual(command[-2], 'transparent-signed-1900000')
                self.assertEqual(command[command.index('--settle-seconds') + 1], settle)
                self.assertEqual(command[command.index('--temperature') + 1], 'warm')
        burst = pieces['transparent-burst'][0][0]
        self.assertEqual(burst[burst.index('--endpoints') + 1], 'admission-to-template')
        self.assertEqual(burst[burst.index('--fixtures-subset') + 1], 'transparent-signed-1900000')
        self.assertEqual(burst[burst.index('--rounds') + 1], '1')
        self.assertEqual(burst[burst.index('--repetitions') + 1], '3')
        served_commands = pieces['served-submit'][0]
        self.assertEqual(len(served_commands), 3)
        for served, (label, endpoint, delay) in zip(served_commands, (
                ('served', 'served-submitblock', '1'),
                ('rebuild-0ms', 'served-rebuild-submitblock', '0'),
                ('rebuild-50ms', 'served-rebuild-submitblock', '0.05'))):
            self.assertEqual(served[served.index('--endpoints') + 1:served.index('--solve-delay-seconds')],
                             [endpoint])
            self.assertEqual(served[served.index('--solve-delay-seconds') + 1], delay)
            if label == 'served':
                self.assertNotIn('--rebuild-observation-seconds', served)
            else:
                self.assertEqual(served[served.index('--rebuild-observation-seconds') + 1], '2')
            self.assertIn('sweep/run_sweep.py', served)
            for flag in ('--fixtures', '--fixtures-subset', '--rounds', '--repetitions',
                         '--provenance', '--seed', '--timeout'):
                self.assertEqual(served[served.index(flag) + 1], burst[burst.index(flag) + 1])
            self.assertEqual(served[served.index('--fixtures') + 1], str(campaign.SIGNED))
            self.assertEqual(served[served.index('--output') + 1], '/unused/served-submit/' + label)
            self.assertEqual([value for flag, value in zip(served, served[1:]) if flag == '--arm'],
                             [f'{arm}={binary}' for arm, binary in self.arms.items()])
        with patch.dict(os.environ, {'V5_CPUS': 'all'}):
            whole_host = campaign.campaign_plan(self.arms, Path('/unused'), 'comparison')
        self.assertTrue(all('taskset' not in command for commands, _ in whole_host.values() for command in commands))
        self.assertIn('extra-mempool',
                      campaign.parse_arms([*self.specs, 'extra-mempool:zebrad-mempool'], 'comparison'))
        for specs in (self.specs[:-1], self.specs + [self.specs[0]],
                      ['stack:../bad', *self.specs[1:]],
                      [*self.specs, 'extra:zebrad-extra'], [*self.specs, 'extra-Bad:zebrad-extra']):
            with self.assertRaises(ValueError):
                campaign.parse_arms(specs, 'comparison')

    def test_dry_run_is_node_and_binary_free(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'not-created'
            argv = ['v5_campaign.py', '--profile', 'comparison', '--dry-run', '--shared-host',
                    '--output', str(output), *self.specs]
            with patch('sys.argv', argv), redirect_stdout(io.StringIO()) as printed, \
                    patch.object(campaign.subprocess, 'run', side_effect=AssertionError('launched command')), \
                    patch.object(campaign, 'provenance', side_effect=AssertionError('inspected binaries')), \
                    patch.dict(os.environ, {'V5_CPUS': '0-7,16-23'}):
                campaign.main()
            self.assertIn('peer-work-fresh', printed.getvalue())
            self.assertNotIn('wait-quiet.sh', printed.getvalue())
            self.assertFalse(output.exists())

    def test_dry_run_selects_served_submit_without_inspecting_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'not-created'
            argv = ['v5_campaign.py', '--profile', 'comparison', '--piece', 'served-submit',
                    '--dry-run', '--output', str(output), *self.specs]
            with patch('sys.argv', argv), redirect_stdout(io.StringIO()) as printed, \
                    patch.object(campaign.subprocess, 'run', side_effect=AssertionError('launched command')), \
                    patch.object(campaign, 'provenance', side_effect=AssertionError('inspected binaries')), \
                    patch.dict(os.environ, {'V5_CPUS': '0-7,16-23'}):
                campaign.main()
            text = printed.getvalue()
            self.assertIn('# served-submit:', text)
            self.assertIn('--endpoints served-submitblock --solve-delay-seconds 1', text)
            self.assertIn('--endpoints served-rebuild-submitblock --solve-delay-seconds 0 '
                          '--rebuild-observation-seconds 2', text)
            self.assertIn('--endpoints served-rebuild-submitblock --solve-delay-seconds 0.05 '
                          '--rebuild-observation-seconds 2', text)
            for label in ('served', 'rebuild-0ms', 'rebuild-50ms'):
                self.assertIn('--output ' + str(output / 'served-submit' / label), text)
            self.assertNotIn('# submitblock-latency:', text)
            self.assertEqual(text.count('sweep/run_sweep.py'), 3)
            self.assertFalse(output.exists())

    def test_whole_host_requires_exclusive_acknowledgement(self):
        argv = ['v5_campaign.py', '--profile', 'comparison', '--steps', *self.specs]
        for cpus in ('', 'all'):
            with patch.dict(os.environ, {'V5_CPUS': cpus}), \
                    patch('sys.argv', [*argv, '--shared-host']), \
                    patch('sys.stderr', io.StringIO()), self.assertRaises(SystemExit):
                campaign.main()
            with patch.dict(os.environ, {'V5_CPUS': cpus}), \
                    patch('sys.argv', [*argv, '--exclusive']), \
                    redirect_stdout(io.StringIO()) as printed:
                campaign.main()
            commands = [line for line in printed.getvalue().splitlines() if not line.startswith('#')]
            self.assertEqual(len(commands), 5)
            self.assertTrue(all('--exclusive' in command and '--shared-host' not in command for command in commands))

    def test_comparison_rejects_wrong_source_even_for_stack(self):
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / 'binary'
            binary.write_bytes(b'not an executable image')
            binary.chmod(0o700)
            receipt = dict(path=str(binary), build='test only',
                           sha256=hashlib.sha256(binary.read_bytes()).hexdigest())
            for arm, source in campaign.COMPARISON_REVISIONS.items():
                path = Path(str(binary) + '.build.json')
                path.write_text(json.dumps(dict(receipt, source=source)))
                self.assertEqual(campaign.provenance({arm: str(binary)}, 'comparison')[arm]['source'], source)
                path.write_text(json.dumps(dict(receipt, source='0' * 40)))
                with self.assertRaisesRegex(ValueError, 'Wrong source revision'):
                    campaign.provenance({arm: str(binary)}, 'comparison')

    def test_summaries_select_first_work_and_reject_incomplete_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pieces = campaign.campaign_plan(self.arms, root, 'comparison')
            for name in ('peer-work-fresh', 'transparent-burst'):
                commands = pieces[name][0]
                peer = name.startswith('peer-work-')
                count, unit = (18, 'ms') if peer else (3, 's')
                scale = 1_000_000 if peer else 1_000_000_000
                rows = []
                for repetition in range(count):
                    for arm, _ in campaign.rotate(self.arms, repetition):
                        rows.append(dict(status='ok', arm=arm,
                                         endpoint={'peer-work-fresh': 'submit-to-peer-template',
                                                   'transparent-burst': 'admission-to-template'}[name],
                                         fixture='transparent-signed-1900000', block_bytes=1899900,
                                         transaction_count=5001,
                                         latency_ns=(1000 if peer else repetition + 1) * scale,
                                         first_template_latency_ns=(repetition + 1) * scale))
                if peer:
                    for command, row in zip(commands, rows):
                        path = Path(command[-1]) / 'sample.json'
                        path.parent.mkdir(parents=True)
                        path.write_text(json.dumps(row))
                else:
                    path = root / name / 'observations.jsonl'
                    path.parent.mkdir()
                    path.write_text(''.join(json.dumps(row) + '\n' for row in rows))
                result = campaign.comparison_result(name, commands, self.arms)
                self.assertEqual(result['unit'], unit)
                for arm in self.arms:
                    self.assertEqual(result['arms'][arm], dict(samples=count, median=(count + 1) / 2,
                                     min=1, max=count, block_bytes=[1899900], transaction_count=[5001]))
                if peer:
                    row['status'] = 'error'
                    path.write_text(json.dumps(row))
                else:
                    path.write_text(''.join(json.dumps(row) + '\n' for row in rows[:-1]))
                with self.assertRaisesRegex(ValueError, 'Incomplete or mismatched'):
                    campaign.comparison_result(name, commands, self.arms)

    def test_served_summaries_keep_endpoints_delays_and_sample_counts_separate(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            commands = campaign.campaign_plan(self.arms, root, 'comparison')['served-submit'][0]
            cells = (('served', 'served-submitblock', 1, 0),
                     ('rebuild-0ms', 'served-rebuild-submitblock', 0, 10),
                     ('rebuild-50ms', 'served-rebuild-submitblock', 0.05, 100))
            for label, endpoint, delay, offset in cells:
                path = root / 'served-submit' / label / 'observations.jsonl'
                path.parent.mkdir(parents=True)
                rows = [dict(status='ok', arm=arm, endpoint=endpoint,
                             solve_delay_seconds=delay, fixture='transparent-signed-1900000',
                             block_bytes=1899900, transaction_count=5001,
                             latency_ns=(offset + repetition + 1) * 1_000_000_000)
                        for arm in self.arms for repetition in range(3)]
                path.write_text(''.join(json.dumps(row) + '\n' for row in rows))
            result = campaign.comparison_result('served-submit', commands, self.arms)
            self.assertEqual(set(result['cells']), {cell[0] for cell in cells})
            for label, endpoint, delay, offset in cells:
                summary = result['cells'][label]
                self.assertEqual(summary['endpoint'], endpoint)
                self.assertEqual(summary['solve_delay_seconds'], delay)
                self.assertEqual(summary['metric'], 'latency_ns')
                self.assertEqual(summary['unit'], 's')
                self.assertIn('exact served template', summary['timing'])
                self.assertIn('no explicit proposal RPC', summary['timing'])
                for arm in self.arms:
                    self.assertEqual(summary['arms'][arm], dict(
                        samples=3, median=offset + 2, min=offset + 1, max=offset + 3,
                        block_bytes=[1899900], transaction_count=[5001]))
            # The last command must be read, and its 50ms rows cannot enter the 0ms cell.
            for bad_rows in (
                rows[:-1],
                [dict(rows[0], solve_delay_seconds=0), *rows[1:]],
                [dict(row, solve_delay_seconds=0) for row in rows],
                [{key: value for key, value in rows[0].items() if key != 'solve_delay_seconds'},
                 *rows[1:]],
                [dict(rows[0], arm=list(self.arms)[1]), *rows[1:]],
                [dict(rows[0], endpoint='served-submitblock'), *rows[1:]],
                [dict(rows[0], status='error'), *rows[1:]],
                [dict(rows[0], latency_ns=0), *rows[1:]],
            ):
                path.write_text(''.join(json.dumps(row) + '\n' for row in bad_rows))
                with self.assertRaises(ValueError):
                    campaign.comparison_result('served-submit', commands, self.arms)
            with self.assertRaises(ValueError):
                campaign.comparison_result('served-submit', commands[:1], self.arms)
            path.unlink()
            with self.assertRaises(FileNotFoundError):
                campaign.comparison_result('served-submit', commands, self.arms)


if __name__ == '__main__':
    unittest.main()
