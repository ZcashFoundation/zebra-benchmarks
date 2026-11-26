#!/usr/bin/env python3
"""Sustained-churn planning and per-process summary cells, without nodes."""
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import v5_campaign as campaign


class ChurnCampaignTests(unittest.TestCase):
    def setUp(self):
        self.specs = [f'{label}:unused-{label}' for label in campaign.COMPARISON_REVISIONS]
        self.specs.append('extra-ours:zebrad-ours')
        self.arms = campaign.parse_arms(self.specs, 'comparison')

    def test_two_rates_three_interleaved_repetitions_and_dry_run(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / 'not-created'
            commands = campaign.campaign_plan(self.arms, root, 'comparison')['sustained-churn'][0]
            self.assertEqual(len(commands), 2)
            for command, rate in zip(commands, (20, 100)):
                for flag, expected in (('--endpoints', 'sustained-churn'), ('--churn-rate', str(rate)),
                                       ('--churn-seconds', '60'), ('--churn-settle-seconds', '20'),
                                       ('--poll-ms', '100'), ('--rounds', '1'), ('--repetitions', '3')):
                    self.assertEqual(command[command.index(flag) + 1], expected)
                self.assertEqual([value for flag, value in zip(command, command[1:]) if flag == '--arm'],
                                 [f'{arm}={binary}' for arm, binary in self.arms.items()])
                self.assertEqual(command[command.index('--output') + 1], str(root / 'sustained-churn' / f'{rate}-tx-s'))
            argv = ['v5_campaign.py', '--profile', 'comparison', '--piece', 'sustained-churn',
                    '--dry-run', '--output', str(root), *self.specs]
            with patch('sys.argv', argv), redirect_stdout(io.StringIO()) as printed, \
                    patch.object(campaign.subprocess, 'run', side_effect=AssertionError('executed command')), \
                    patch.object(campaign, 'provenance', side_effect=AssertionError('inspected binaries')), \
                    patch.dict(os.environ, {'V5_CPUS': '0-7,16-23'}):
                campaign.main()
            self.assertEqual(printed.getvalue().count('sweep/run_sweep.py'), 2)
            self.assertFalse(root.exists())

    def test_summary_keeps_rate_cells_process_quantiles_and_failures_separate(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            commands = campaign.campaign_plan(self.arms, root, 'comparison')['sustained-churn'][0]
            paths = []
            for command, rate in zip(commands, (20, 100)):
                rows = []
                for repetition in range(3):
                    for arm in self.arms:
                        stats = dict(median=rate + repetition, p90=rate + 10 + repetition,
                                     p99=rate + 20 + repetition, max=rate + 30 + repetition)
                        rows.append(dict(status='ok', arm=arm, endpoint='sustained-churn',
                                         fixture='transparent-signed-1900000', round=0, repetition=repetition,
                                         churn_rate=rate, churn_seconds=60, poll_interval_ms=100,
                                         actual_admitted_rate=rate - repetition, admitted_during_window=rate * 60,
                                         admission_failures=repetition, generator_missed_admissions=0,
                                         freshness_unobserved=0, template_rebuild_count=rate,
                                         template_builds_per_second=rate / 60, cpu_seconds=12,
                                         cpu_seconds_per_wall_second=0.2, peak_rss_bytes=1000,
                                         freshness_ns=stats, gbt_latency_ns=stats, cheap_rpc_latency_ns=stats,
                                         admission_start_lag_ns=stats))
                path = Path(command[command.index('--output') + 1]) / 'observations.jsonl'
                path.parent.mkdir(parents=True)
                path.write_text(''.join(json.dumps(row) + '\n' for row in rows))
                paths.append(path)
            result = campaign.comparison_result('sustained-churn', commands, self.arms)
            self.assertEqual(set(result['cells']), {'20-tx-s', '100-tx-s'})
            for rate in (20, 100):
                arm = result['cells'][f'{rate}-tx-s']['arms']['extra-ours']
                self.assertEqual(arm['samples'], 3)
                self.assertEqual(arm['metrics']['freshness_ns_p90']['median'], rate + 11)
                self.assertEqual(arm['metrics']['actual_admitted_rate']['median'], rate - 1)
                self.assertEqual(arm['metrics']['admission_failures']['values'], [0, 1, 2])
            rows = [json.loads(line) for line in paths[-1].read_text().splitlines()]
            for altered in (rows[:-1], [dict(rows[0], repetition=2), *rows[1:]],
                            [dict(rows[0], churn_rate=20), *rows[1:]],
                            [dict(rows[0], status='error'), *rows[1:]]):
                paths[-1].write_text(''.join(json.dumps(row) + '\n' for row in altered))
                with self.assertRaises(ValueError):
                    campaign.comparison_result('sustained-churn', commands, self.arms)


if __name__ == '__main__':
    unittest.main()
