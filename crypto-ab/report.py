#!/usr/bin/env python3
"""Print Markdown latency tables and paired bootstrap intervals for result directories.

Usage: python3 crypto-ab/report.py RUN_DIR [RUN_DIR ...]
Each directory must contain rows.jsonl from measure.sh. Mean client latency is the
headline; median and half-sample mode expose skew. Pairing uses common round IDs.
"""
import collections
import itertools
import json
from pathlib import Path
import random
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "regtest"))
import latency_stats  # noqa: E402


def load(path):
    by = collections.defaultdict(dict)
    arms, fixtures = [], []
    with path.open() as source:
        for line in source:
            row = json.loads(line)
            fixture, arm, round_id = row['fixture'], row['arm'], row['round']
            if arm not in arms:
                arms.append(arm)
            if fixture not in fixtures:
                fixtures.append(fixture)
            if round_id in by[fixture, arm]:
                raise ValueError(f'duplicate observation: {fixture} {arm} round {round_id}')
            by[fixture, arm][round_id] = row['latency_ms']
    return by, arms, fixtures


def table(by, arms, fixtures, stat):
    print('| Block | ' + ' | '.join(arms) + ' |')
    print('|---|' + '---:|' * len(arms))
    for fixture in fixtures:
        cells = [f'{latency_stats.summary(list(by[fixture, arm].values()))[stat]:.1f}'
                 if by[fixture, arm] else '—' for arm in arms]
        print(f'| {fixture} | ' + ' | '.join(cells) + ' |')
    print()


def main():
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)
    for argument in sys.argv[1:]:
        path = Path(argument)
        by, arms, fixtures = load(path / 'rows.jsonl' if path.is_dir() else path)
        rng = random.Random(0)
        print(f'## {path.name}\n')
        for stat, title in (('mean', 'mean'), ('median', 'median'), ('mode', 'half-sample mode')):
            print(f'**{title} client latency (ms):**\n')
            table(by, arms, fixtures, stat)
        print('**Paired differences (ms; subject − comparator; bootstrap 95% CI):**\n')
        for subject, comparator in itertools.combinations(arms, 2):
            parts = []
            for fixture in fixtures:
                rounds = sorted(by[fixture, subject].keys() & by[fixture, comparator].keys())
                if not rounds:
                    continue
                result = latency_stats.paired(
                    [by[fixture, subject][i] - by[fixture, comparator][i] for i in rounds], rng)
                low, high = result['mean_ci95']
                parts.append(f'{fixture} (n={len(rounds)}) {result["mean"]:+.1f} '
                             f'[{low:+.1f}, {high:+.1f}] (median {result["median"]:+.1f})')
            if parts:
                print(f'- {subject} − {comparator}: ' + '; '.join(parts))
        print()


if __name__ == '__main__':
    main()
