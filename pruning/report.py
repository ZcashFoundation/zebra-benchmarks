#!/usr/bin/env python3
"""Summarize timestamped measurement receipts; synthetic evidence stays labelled."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import random
import statistics
import sys

ROOT = Path(__file__).resolve().parent
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT.parent / 'regtest'))
from quick_ab import paired_stats, percentile, RESAMPLES, SEED


def summarize_pairs(observations, phase):
    selected = [row for row in observations if phase != 'during' or row.get('deleting')]
    if any(row['status'] != 'ok' for row in selected):
        raise ValueError('failed samples may not be silently discarded')
    if len({row['height'] for row in selected}) != len(selected):
        raise ValueError('duplicate same-block pairs')
    result = dict(n=len(selected), total_pairs=len(observations),
                  excluded_nonoverlap_pairs=len(observations) - len(selected),
                  bootstrap=dict(resamples=RESAMPLES, seed=SEED, confidence=.95, unit='same-block pair'),
                  archive=None, pruned=None, paired=None)
    if not selected:
        return result
    a = [row['timings']['archive'] for row in selected]
    b = [row['timings']['pruned'] for row in selected]
    result['paired'] = paired_stats(a, b)
    # Resample pairs together. Reuse the benchmark's percentile and paired estimator.
    rng = random.Random(SEED)
    boot = {arm: {'median': [], 'p90': []} for arm in ('archive', 'pruned')}
    if len(selected) > 1:
        for _ in range(RESAMPLES):
            indices = rng.choices(range(len(a)), k=len(a))
            for arm, data in (('archive', a), ('pruned', b)):
                sample = sorted(data[i] for i in indices)
                boot[arm]['median'].append(statistics.median(sample))
                boot[arm]['p90'].append(percentile(sample, .9))
    for arm, data in (('archive', a), ('pruned', b)):
        result[arm] = dict(median_ms=statistics.median(data), p90_ms=percentile(sorted(data), .9))
        for metric in ('median', 'p90'):
            samples = sorted(boot[arm][metric])
            result[arm][metric + '_ci95_ms'] = ([percentile(samples, .025), percentile(samples, .975)]
                                               if samples else None)
    return result


def number(value, digits=3):
    return 'unavailable' if value is None else f'{value:,.{digits}f}'


def interval(value):
    return 'unavailable' if value is None else f'[{number(value[0])}, {number(value[1])}]'


def render(receipts):
    latest = {}
    for row in sorted(receipts, key=lambda x: x.get('started_at', '')):
        if row['status'] == 'ok':
            latest[row['phase']] = row
    out = ['# Mainnet historical-body pruning measurement', '',
           'Retention: **10,000 finalized blocks**, plus genesis, non-finalized blocks and any restart witness.', '']
    if any(row.get('synthetic') for row in receipts):
        out += ['**SYNTHETIC SMOKE INPUT — these are not Mainnet performance results.**', '']
    out += ['## Phase receipts', '', '| Phase | Status | Started (UTC) | Wall seconds |',
            '|---|---|---|---:|']
    for row in receipts:
        out.append(f"| {row['phase']} | {row['status']} | {row.get('started_at', '')} | {number(row.get('wall_seconds'))} |")
    out += ['', '## Storage and deletion', '']
    extraction = latest.get('extract', {}).get('result')
    if extraction:
        out += [f"Extraction: {number(extraction.get('extraction_seconds'))} s; "
                f"state `du -sb`: {extraction['size']['du_sb']:,} bytes.", '']
    prune = latest.get('prune', {}).get('result')
    if prune:
        out += [f"Binary: `{prune['binary']['version']}`; SHA256 `{prune['binary']['sha256']}`.", '',
                '| Measurement | Apparent bytes (`du -sb`) | Allocated referenced bytes | SST bytes |',
                '|---|---:|---:|---:|']
        for key, label in [('before', 'Before deletion'), ('after_deletion', 'After deletion, stopped'),
                           ('after_compaction', 'After compaction, stopped')]:
            value = prune[key]
            out.append(f"| {label} | {value['du_sb']:,} | {value['allocated_referenced_bytes']:,} | {value['sst_bytes']:,} |")
        before, after = prune['before']['du_sb'], prune['after_compaction']['du_sb']
        delta = before - after
        out += ['', f"State-file reduction: **{delta:,} bytes ({100 * delta / before:.2f}%)**."]
        deletion = prune['deletion']
        out += [f"Requested floor {deletion['requested_floor']:,}; witness {deletion['witness_height']:,}; "
                f"effective target {deletion['target_floor']:,}; observed floor {deletion['final']['pruneheight']:,}.",
                f"Startup-to-floor: **{number(deletion['startup_to_floor_seconds'])} s**. "
                f"First-observed-progress-to-floor: {number(deletion['observed_deletion_seconds'])} s "
                '(polling lower bound, not exact worker CPU time).',
                f"Compaction: **{prune['compaction']['method']}**, {number(prune['compaction']['seconds'])} s.", '']
        if not prune['compaction']['forced']:
            out += ['**Compaction is non-final: idle time is not proof that all obsolete bytes were reclaimed.**', '']
        if prune['after_compaction'].get('btrfs_du'):
            out += ['Btrfs accounting after compaction:', '```text', prune['after_compaction']['btrfs_du'], '```', '']
    else:
        out += ['No successful pruning receipt yet.', '']
    out += ['`du -sb` measures file lengths, not uniquely freed physical space. Allocated bytes count',
            'referenced extents; reflink-shared archive extents remain allocated while the archive is kept.',
            'Btrfs total/exclusive/shared accounting and filesystem free bytes are retained in the receipts.',
            'Free-space changes can include unrelated machine activity; do not equate them to pruning savings.', '',
            '## Same-block submitblock latency', '']
    modes = {}
    for row in sorted(receipts, key=lambda x: x.get('started_at', '')):
        if row['phase'] == 'replay' and row['status'] == 'ok':
            modes.update(row['results'])
    for phase, replay in modes.items():
        summary = replay['summary']
        out += [f"### {phase.capitalize()} deletion", '',
                f"{summary['n']} paired blocks; {summary['excluded_nonoverlap_pairs']} non-overlap pairs excluded. "
                f"Actual starting tip: {replay['actual_start_height']:,}.", '',
                '| Node | Median ms (bootstrap 95% CI) | p90 ms (bootstrap 95% CI) |',
                '|---|---:|---:|']
        for arm in ('archive', 'pruned'):
            values = summary[arm]
            if values:
                out.append(f"| {arm} | {number(values['median_ms'])} {interval(values['median_ci95_ms'])} | "
                           f"{number(values['p90_ms'])} {interval(values['p90_ci95_ms'])} |")
        paired = summary['paired']
        if paired:
            ci = paired['ci95_ms']
            verdict = ('inconclusive' if ci is None or ci[0] <= 0 < ci[1] else
                       'measured regression' if ci[0] > 0 else 'no measured slowdown')
            out += ['', f"Median paired difference (pruned − archive): **{number(paired['median_paired_delta_ms'])} ms**, "
                    f"95% CI **{interval(ci)}** — **{verdict}**.", '']
    if not modes:
        out += ['No successful replay receipt yet.', '']
    out += ['Both arms use the same pruning binary; archive pruning is disabled. Per-height order alternates.',
            'Only submitblock RPC wall time is timed. Source reads, peer/tip audits and pruning polls are outside the timer.',
            'During-deletion summaries require a nonzero initial backlog throughout each pair and observed floor progress.',
            'These are cold-mempool replays, not cold-filesystem tests; the first ~1,000 blocks also grow non-finalized state.',
            'Bootstrap resamples matched blocks (10,000 draws, fixed seed), not independent machines or runs.',
            'Sequential block/cache effects can be correlated. A CI crossing zero does not prove non-inferiority;',
            'no acceptable slowdown margin has been predeclared. Repeat the experiment for stronger claims.', '']
    failures = [row for row in receipts if row['status'] != 'ok']
    if failures:
        out += ['## Failed attempts', '']
        for row in failures:
            out += [f"- {row['phase']} {row.get('started_at', '')}: "
                    f"`{row.get('error', 'see receipt').strip().splitlines()[-1]}`"]
    return '\n'.join(out) + '\n'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--receipts', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    # Only timestamped phase receipts are report inputs.
    receipts = [json.loads(path.read_text()) for path in sorted(args.receipts.glob('*-*.json'))
                if not path.name.endswith('-report.json')]
    text = render(receipts)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(text)
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ')
    receipt = dict(schema=1, phase='report', status='ok', started_at=datetime.now(timezone.utc).isoformat(),
                   input_count=len(receipts), output=str(args.output.resolve()))
    args.receipts.mkdir(parents=True, exist_ok=True)
    (args.receipts / f'{stamp}-report.json').write_text(json.dumps(receipt, indent=2) + '\n')
    print(args.output)


if __name__ == '__main__':
    main()
