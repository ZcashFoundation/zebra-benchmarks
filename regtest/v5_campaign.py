#!/usr/bin/env python3
"""Portable campaign planner; --dry-run, --steps and --eta never start nodes or inspect binaries.

Each --piece is independently runnable. Raw harness receipts remain under --output;
completed/<piece>.json records only fully successful pieces. Existing pieces are never overwritten.
Short post-ready workloads wait for supported Halo2 key preload (60s deadline).
Startup RPC and sync replay retain their startup-inclusive metrics; their receipts
record preload completion without removing that real startup cost.
--shared-host skips the host-wide quiet gate; CPU placement remains explicit.
--profile comparison adds 18 fresh/settled peer-to-first-work samples and three
1.9 MB signed-transparent admission-to-template bursts per arm. Peer receivers
are warm; first work may be coinbase-only. These controlled Regtest rows do not
claim to reproduce the announcement's unspecified transaction/mempool setup.
Comparison defaults to CPUs 0-7,16-23; V5_CPUS=all (or empty) requires --exclusive
after other host workloads have stopped. --shared-host never grants whole-host use.
"""
import argparse
import hashlib
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import re
import shlex
import statistics
import subprocess
import time

from node_bench import binary_preloads, BENCH_HELP, BENCH_ROOT, FIXTURES
BENCH = Path(__file__).resolve().parent
ROOT = BENCH_ROOT
BIN = Path(os.environ.get('BENCH_BINS', ROOT / 'bins')).expanduser().resolve()
RUNS = Path(os.environ.get('BENCH_OUTPUT', ROOT / 'output')).expanduser().resolve()
SIGNED = Path(os.environ.get('BENCH_SIGNED_FIXTURES', ROOT / 'fixtures/transparent-signed')).expanduser().resolve()
SOAK = Path(os.environ.get('BENCH_MAINNET', ROOT / 'mainnet')).expanduser().resolve()
RELEASES = {'release': 'e8db87eb69254e0d4eedd058eadd48bdfa09065a',
            'zakura-main': 'f5c20398c629c8179364b07f059da3c814af78ec'}
LABELS = ('release', 'stack', 'crypto', 'zakura-main')
COMPARISON_REVISIONS = {
    'stack': '5527c563929fbe3b9e82d099b4d4cfc19b092e20',
    'crypto': '5527c563929fbe3b9e82d099b4d4cfc19b092e20',
    'zakura-main': RELEASES['zakura-main'],
}
SIZES = (25000, 100000, 250000, 500000, 1000000, 1500000, 1900000)
ENDPOINTS = {'admission': 'admission-to-template', 'prepared': 'prepared-submitblock',
             'cold': 'cold-submitblock'}


def parse_arms(specs, profile='core'):
    arms = {}
    for spec in specs:
        # Comparison runs may add `extra-NAME` Zebra arms; their sources come from build receipts.
        labels = ('stack|crypto|zakura-main|extra-[a-z0-9-]+' if profile == 'comparison'
                  else 'release|stack|crypto|zakura-main')
        if not re.fullmatch(rf'({labels}):(?:[A-Za-z0-9_.-]+|<binary>)', spec):
            raise ValueError('Expected label:binary-name for this profile: ' + spec)
        label, binary = spec.split(':')
        if label in arms:
            raise ValueError('Duplicate arm: ' + label)
        arms[label] = str(BIN / binary)
    required = set(COMPARISON_REVISIONS) if profile == 'comparison' else {'release', 'stack', 'crypto'}
    if not required <= arms.keys():
        raise ValueError('Required arms: ' + ', '.join(sorted(required)))
    return arms


def arm_flags(arms):
    return [arg for label, binary in arms.items() for arg in ('--arm', f'{label}={binary}')]


def rotate(arms, rep):
    items = list(arms.items())
    shift = rep % len(items)
    return items[shift:] + items[:shift]


def plan(arms, output, profile='core'):
    """Return ordered pieces: commands + estimated minutes for the selected comparison."""
    pieces = {}
    provenance = str(output / 'arms.json')
    zebra = {a: b for a, b in arms.items() if a != 'zakura-main'}

    def add(name, commands, minutes):
        assert name not in pieces
        pieces[name] = (commands, minutes)

    def py(script, *args):
        command = ['python3', '-B', script, *map(str, args)]
        cpus = os.environ.get('V5_CPUS', '0-7,16-23' if profile == 'comparison' else None)
        return ['taskset', '-c', cpus, *command] if cpus and cpus != 'all' and script != 'sync_replay.py' else command

    add('smoke', [py('phase-probe.py', '--fixtures', fixtures, '--endpoint', endpoint,
                     binary, arm, fixture, 1, output / 'smoke' / fixture / endpoint / arm)
                  for fixtures, fixture in ((FIXTURES, 'empty-2000'), (SIGNED, 'transparent-signed-250000'))
                  for endpoint in ('cold-submitblock-nominer', 'warm-mempool-submitblock', 'prepared-submitblock')
                  for arm, binary in arms.items()], 15)

    if profile == 'comparison':
        for link, settle in (('fresh', 0), ('settled', 20)):
            name = f'peer-work-{link}'
            commands = [py('sweep/tip_to_template.py', '--fixtures', SIGNED,
                           '--temperature', 'warm', '--settle-seconds', settle,
                           '--poll-ms', 1, '--timeout', 1200,
                           binary, arm, 'transparent-signed-1900000',
                           output / name / f'{arm}-{rep + 1}')
                        for rep in range(18) for arm, binary in rotate(arms, rep)]
            add(name, commands, 90)
        name = 'transparent-burst'
        add(name, [py('sweep/run_sweep.py', *arm_flags(arms), '--provenance', provenance,
                      '--fixtures', SIGNED, '--fixtures-subset', 'transparent-signed-1900000',
                      '--endpoints', 'admission-to-template', '--rounds', 1, '--repetitions', 3,
                      '--seed', 20261104, '--poll-ms', 1, '--timeout', 1200,
                      '--output', output / name)], 35)
        return pieces

    # Nine samples per cell, retaining the v3 three-round / three-repetition design.
    # Split each endpoint and round: Zakura's 1.9 MB transparent preparation took ~647 s.
    for filling in ('transparent', 'orchard', 'mixed', 'transparent-signed'):
        sizes = (250000, 1900000) if filling == 'transparent-signed' else SIZES
        fixtures = SIGNED if filling == 'transparent-signed' else FIXTURES
        for round_index in range(1, 4):
            for short, endpoint in ENDPOINTS.items():
                name = f'sweep-{filling}-{short}-r{round_index}'
                minutes = 90 if filling == 'transparent' and short != 'cold' else 50
                if short == 'cold' or filling == 'orchard':
                    minutes = 15
                add(name, [py('sweep/run_sweep.py', *arm_flags(arms), '--provenance', provenance,
                              '--fixtures', fixtures, '--fixtures-subset',
                              *(f'{filling}-{size}' for size in sizes), '--endpoints', endpoint,
                              '--rounds', 1, '--repetitions', 3, '--seed', 20261023 + round_index,
                              '--timeout', 1200, '--output', output / name)], minutes)
    for link, settle in (('fresh', 0), ('settled', 20)):
        name = f'relay-{link}'
        add(name, [py('relay/run_sweep.py', *arm_flags(arms), '--provenance', provenance,
                      '--fixtures', FIXTURES, '--fixtures-subset', 'empty-2000',
                      'transparent-1900000', 'orchard-1900000', 'mixed-1900000',
                      '--settle-seconds', settle, '--rounds', 2, '--repetitions', 3,
                      '--output', output / name)], 12 if not settle else 45)
    for mode in ('local', 'peer', 'settled'):
        name = 'empty-tip-' + mode
        add(name, [['env', 'MODES=' + mode, *py('tip-template.py', '--fixtures', FIXTURES,
                    output / name, *(f'{a}={b}' for a, b in arms.items()))]], 40 if mode == 'settled' else 12)
    add('empty-gbt', [py('gbt-empty.py', '--fixtures', FIXTURES, output / 'empty-gbt',
                        *(f'{a}={b}' for a, b in arms.items()))], 8)
    for delay in (0, 1000):
        name = f'empty-admission-{delay}'
        add(name, [['env', f'DELAY_MS={delay}', *py('empty-admission.py', '--fixtures', FIXTURES,
                    output / name, 24, *(f'{a}:{a}={b}' for a, b in arms.items()))]], 10)
    peer_fixtures = ('transparent-signed-1900000', 'orchard-1900000', 'mixed-1900000')
    for fixture in peer_fixtures:
        fixtures = SIGNED if fixture.startswith('transparent-signed') else FIXTURES
        for endpoint in ('cold-submitblock-nominer', 'warm-mempool-submitblock', 'prepared-submitblock'):
            commands = [py('phase-probe.py', '--fixtures', fixtures, '--endpoint', endpoint,
                           binary, arm, fixture, 1,
                           output / 'peer-block' / endpoint / fixture / f'{arm}-{rep + 1}')
                        for rep in range(5) for arm, binary in rotate(arms, rep)]
            add(f'peer-block-{fixture}-{endpoint}', commands, 65 if 'cold' not in endpoint else 8)
    tip_fixtures = ('empty-2000', 'transparent-signed-250000', 'orchard-250000', 'mixed-250000',
                    *peer_fixtures)
    for fixture in tip_fixtures:
        fixtures = SIGNED if fixture.startswith('transparent-signed') else FIXTURES
        for link, settle in (('fresh', 0), ('settled', 20)):
            for temperature in ('cold', 'warm'):
                name = f'tip-template-{fixture}-{link}-{temperature}'
                commands = [py('sweep/tip_to_template.py', '--fixtures', fixtures,
                               '--temperature', temperature, '--settle-seconds', settle, '--poll-ms', 1,
                               binary, arm, fixture,
                               output / 'tip-template' / f'{link}-{temperature}' / fixture / f'{arm}-{rep + 1}')
                            for rep in range(5) for arm, binary in rotate(arms, rep)]
                add(name, commands, 70 if temperature == 'warm' and '1900000' in fixture else 18)
    for fixture in ('empty-2000', 'mixed-250000', 'mixed-1900000'):
        for miner in ('transparent', 'orchard', 'sapling'):
            name = f'tip-miner-{fixture}-{miner}'
            # These NU5 fixtures cannot use Zakura's shielded coinbase payouts.
            miner_arms = arms if miner == 'transparent' else {
                a: b for a, b in arms.items() if a != 'zakura-main'}
            commands = [py('sweep/tip_to_template.py', '--fixtures', FIXTURES,
                           '--temperature', 'warm', '--settle-seconds', 20, '--poll-ms', 1,
                           '--miner', miner, binary, arm, fixture,
                           output / 'tip-miner' / miner / fixture / f'{arm}-{rep + 1}')
                        for rep in range(5) for arm, binary in rotate(miner_arms, rep)]
            add(name, commands, 70 if fixture.endswith('1900000') else 18)
    for fixture in ('empty-2000', *peer_fixtures):
        fixtures = SIGNED if fixture.startswith('transparent-signed') else FIXTURES
        for temperature in ('cold', 'warm'):
            name = f'relay-chain-{fixture}-{temperature}'
            commands = [py('sweep/relay_chain.py', '--fixtures', fixtures,
                           '--temperature', temperature, '--settle-seconds', 20, '--poll-ms', 1,
                           binary, arm, fixture,
                           output / 'relay-chain' / temperature / fixture / f'{arm}-{rep + 1}')
                        for rep in range(5) for arm, binary in rotate(arms, rep)]
            add(name, commands, 100 if temperature == 'warm' and fixture != 'empty-2000' else 20)
    add('inclusion', [py('mempool_load.py', *arm_flags(arms), '--provenance', provenance,
                         '--fixtures', FIXTURES, '--corpus', 'transparent', '--rates', 100,
                         '--gbt-probe-ms', 100, '--repetitions', 3, '--output', output / 'inclusion')], 65)
    # Zakura cannot open Zebra's v29 Mainnet state: never silently feed it this snapshot.
    mainnet = [*arm_flags(zebra), '--provenance', provenance, '--state', str(SOAK / 'snapshot-2316084')]
    add('startup', [py('rpc_load.py', *mainnet, '--restarts', 10, '--repetitions', 1,
                      '--concurrency', 1, '--duration-s', 5, '--warmup-s', 1,
                      '--output', output / 'startup')], 45)
    add('rpc', [py('rpc_load.py', *mainnet, '--smoke', '--output', output / 'rpc-smoke'),
                py('rpc_load.py', *mainnet, '--output', output / 'rpc')], 80)
    # CPU partitions are explicit: without a mask, preserve the caller's affinity.
    for rep in range(2):
        name = f'replay-r{rep + 1}'
        add(name, [py('sync_replay.py', *arm_flags(dict(rotate(zebra, rep))),
                      '--provenance', provenance, '--snapshot', SOAK / 'snapshot-1750000',
                      '--source-state', SOAK / 'snapshot-2316084', '--start-height', 1750000,
                      '--end-height', 1752000, '--mode', 'full',
                      '--cpus', os.environ.get('V5_CPUS') or 'all',
                      '--source-cpus', os.environ.get('SOURCE_CPUS') or 'all',
                      '--repetitions', 1, '--output', output / name)], 75)
    return pieces


def campaign_plan(arms, output, profile):
    """Core covers each figure; extended adds disjoint sizes/modes, not repeated core cells."""
    full = plan(arms, output, profile)
    if profile == 'comparison':
        return full
    core = {}

    def take(name, minutes=None):
        commands, original_minutes = full[name]
        core[name] = ([command.copy() for command in commands],
                      original_minutes if minutes is None else minutes)
        return core[name][0]

    take('smoke')
    for name in full:
        if name.startswith('peer-block-'):
            minutes = 5 if 'cold-submitblock-nominer' in name else 8 if 'orchard-' in name else 35
            take(name, minutes)
    for name in full:
        if name.startswith('tip-template-') and name.endswith('-settled-warm'):
            minutes = 12 if 'orchard-1900000' in name else 40 if '1900000' in name else 10
            if '250000' in name and 'orchard-' not in name:
                minutes = 15
            take(name, minutes)
    for name in full:
        if name.startswith('tip-miner-') and name.endswith('-orchard'):
            take(name, 40 if '1900000' in name else 15 if '250000' in name else 10)
    for name in full:
        if name.startswith('relay-chain-') and name.endswith('-cold'):
            take(name, 10)

    def sweep(short):
        for filling in ('transparent', 'orchard', 'mixed', 'transparent-signed'):
            minutes = 5 if short == 'cold' else {
                'transparent': 70, 'orchard': 8, 'mixed': 38, 'transparent-signed': 38}[filling]
            command = take(f'sweep-{filling}-{short}-r1', minutes)[0]
            start, end = command.index('--fixtures-subset') + 1, command.index('--endpoints')
            command[start:end] = [f'{filling}-{size}' for size in (250000, 1900000)]
            command[command.index('--repetitions') + 1] = '5'

    sweep('prepared')
    sweep('cold')
    for link, minutes in (('fresh', 8), ('settled', 20)):
        command = take('relay-' + link, minutes)[0]
        start, end = command.index('--fixtures-subset') + 1, command.index('--settle-seconds')
        command[start:end] = ['empty-2000', 'transparent-1900000']
        command[command.index('--rounds') + 1] = '1'
        command[command.index('--repetitions') + 1] = '5'
    for mode, minutes in (('local', 4), ('settled', 10)):
        command = take('empty-tip-' + mode, minutes)[0]
        command[1:1] = ['TIP_ROUNDS=1', 'TIP_REPS=5']
    sweep('admission')
    take('inclusion')
    take('rpc', 70)
    command = take('startup', 23)[0]
    command[command.index('--restarts') + 1] = '5'
    for rep in range(3):
        name = f'replay-r{rep + 1}'
        command = full['replay-r1'][0][0].copy()
        start, end = command.index('--arm'), command.index('--provenance')
        zebra = {a: b for a, b in arms.items() if a != 'zakura-main'}
        command[start:end] = arm_flags(dict(rotate(zebra, rep)))
        command[command.index('--output') + 1] = str(output / name)
        core[name] = ([command], 45)
    if profile == 'core':
        return core

    extended = {name: entry for name, entry in full.items()
                if name not in core and not name.startswith('sweep-')
                and not (name.startswith('tip-miner-') and name.endswith('-transparent'))}
    # The transparent payout cells are identical to settled/warm tip-template cells in core.
    extended = {'smoke': full['smoke'], **extended}
    for name, (commands, minutes) in full.items():
        if name.startswith('sweep-') and not name.startswith('sweep-transparent-signed-'):
            command = commands[0].copy()
            start, end = command.index('--fixtures-subset') + 1, command.index('--endpoints')
            command[start:end] = [fixture for fixture in command[start:end]
                                  if not fixture.endswith(('-250000', '-1900000'))]
            extended[name] = ([command], minutes)
    for link, minutes in (('fresh', 8), ('settled', 25)):
        command = full['relay-' + link][0][0].copy()
        start, end = command.index('--fixtures-subset') + 1, command.index('--settle-seconds')
        command[start:end] = ['orchard-1900000', 'mixed-1900000']
        extended['relay-' + link] = ([command], minutes)
    return extended


def provenance(arms, profile='core'):
    result = {}
    for label, binary in arms.items():
        receipt = json.loads(Path(binary + '.build.json').read_text())
        if not os.access(binary, os.X_OK):
            raise ValueError('Missing executable: ' + binary)
        with open(binary, 'rb') as stream:
            digest = hashlib.file_digest(stream, 'sha256').hexdigest()
        if receipt.get('sha256') != digest or receipt.get('path') != binary:
            raise ValueError('Build receipt does not match binary: ' + binary)
        if not re.fullmatch('[0-9a-f]{40}', receipt.get('source', '')) or not receipt.get('build'):
            raise ValueError('Incomplete build receipt: ' + binary)
        revisions = COMPARISON_REVISIONS if profile == 'comparison' else RELEASES
        if label in revisions and receipt['source'] != revisions[label]:
            raise ValueError('Wrong source revision: ' + label)
        result[label] = dict(receipt, binary_preloads=binary_preloads(str(Path(binary).resolve())))
    return result


def comparison_result(name, commands, arms):
    """Summarize only complete raw rows; never substitute full-work for first-work latency."""
    peer = name.startswith('peer-work-')
    if peer:
        rows = [json.loads((Path(command[-1]) / 'sample.json').read_text())
                for command in commands]
    else:
        directory = Path(commands[0][commands[0].index('--output') + 1])
        rows = [json.loads(line) for line in (directory / 'observations.jsonl').read_text().splitlines()]
    count = 18 if peer else 3
    metric = 'first_template_latency_ns' if peer else 'latency_ns'
    endpoint = 'submit-to-peer-template' if peer else 'admission-to-template'
    if len(rows) != count * len(arms) or any(
            row['status'] != 'ok' or row['arm'] not in arms
            or row['endpoint'] != endpoint or row['fixture'] != 'transparent-signed-1900000'
            for row in rows):
        raise ValueError('Incomplete or mismatched comparison rows: ' + name)
    result = dict(piece=name, metric=metric, unit='ms' if peer else 's',
                  fixture='transparent-signed-1900000', arms={},
                  timing=('sender submitblock request to first receiver new-parent template response'
                          if peer else 'start of serial transaction admission to exact full template, including matching'))
    for arm in arms:
        selected = [row for row in rows if row['arm'] == arm]
        if len(selected) != count or any(row[metric] <= 0 for row in selected):
            raise ValueError('Missing or invalid arm samples: ' + arm)
        scale = 1e6 if peer else 1e9
        values = [row[metric] / scale for row in selected]
        result['arms'][arm] = dict(samples=count, median=statistics.median(values),
                                  min=min(values), max=max(values),
                                  block_bytes=sorted({row['block_bytes'] for row in selected}),
                                  transaction_count=sorted({row['transaction_count'] for row in selected}))
    return result


def save_once(path, value):
    text = json.dumps(value, indent=2, sort_keys=True) + '\n'
    if path.exists():
        if path.read_text() != text:
            raise ValueError('Campaign identity changed: ' + str(path))
    else:
        with path.open('x') as stream:
            stream.write(text)


def queue_durations(path, output):
    """Successful piece wall times, including quiet gates; queue clocks have minute precision."""
    starts, completed, profiles = {}, {}, set()
    previous, day = 0, 0
    for line in path.read_text().splitlines():
        match = re.fullmatch(r'(\d\d):(\d\d) run (\S+) (.*)', line)
        if not match:
            continue
        hour, minute, run, message = match.groups()
        clock = int(hour) * 60 + int(minute)
        if clock < previous:
            day += 24 * 60
        previous = clock
        clock += day
        if message.startswith('started:'):
            argv = shlex.split(message.removeprefix('started:'))
            if not any(arg.endswith('v5-campaign.sh') for arg in argv):
                continue
            if '--output' not in argv or '--piece' not in argv:
                continue
            if Path(argv[argv.index('--output') + 1]).resolve() != output.resolve():
                continue
            profile = argv[argv.index('--profile') + 1] if '--profile' in argv else 'core'
            profiles.add(profile)
            starts[run] = (argv[argv.index('--piece') + 1], clock)
        elif message.endswith(' exit=0') and run in starts:
            name, started = starts.pop(run)
            completed[name] = (clock - started) * 60
    if len(profiles) > 1:
        raise ValueError('Queue log mixes campaign profiles for this output directory')
    return next(iter(profiles), None), completed


def eta(output, queue_log, profile, now=None):
    """Read receipts first, supplement with queue successes; never probe or alter running work."""
    queue_profile, completed = queue_durations(queue_log, output) if queue_log else (None, {})
    manifest = output / 'campaign.json'
    campaign = json.loads(manifest.read_text()) if manifest.is_file() else {}
    profile = campaign.get('profile', queue_profile or profile)
    if queue_profile and queue_profile != profile:
        raise ValueError('Queue profile differs from campaign receipt')
    arms = {label: '<binary>' for label in (COMPARISON_REVISIONS if profile == 'comparison' else LABELS)}
    pieces = campaign_plan(arms, output, profile)
    names = campaign.get('pieces', list(pieces))
    estimates = {name: pieces[name][1] * 60 for name in names}
    notes = []
    for name in names:
        receipt = output / 'completed' / f'{name}.json'
        if not receipt.is_file():
            continue
        try:
            value = json.loads(receipt.read_text())
        except json.JSONDecodeError:
            notes.append(f'Ignoring incomplete receipt: {receipt}')
            continue
        if value.get('success') is True:
            elapsed = (value['ended_unix_ns'] - value['started_unix_ns']) / 1e9
            if elapsed < 0:
                raise ValueError('Negative piece duration: ' + str(receipt))
            completed[name] = elapsed
    completed = {name: seconds for name, seconds in completed.items() if name in estimates}
    rows = [f'Pieces: {len(completed)}/{len(names)} ({profile})']
    if completed and sum(completed.values()) > 0:
        ratio = sum(completed.values()) / sum(estimates[name] for name in completed)
        remaining = sum(seconds for name, seconds in estimates.items() if name not in completed) * ratio
        finish = (now or datetime.now().astimezone()) + timedelta(seconds=remaining)
        rows += [f'Observed/estimated duration: {ratio:.3f}x',
                 f'Projected finish: {finish.isoformat(timespec="minutes")} '
                 f'({remaining / 3600:.2f} h remaining)']
    else:
        rows += ['Observed/estimated duration: unavailable (no positive completed duration)',
                 'Projected finish: unavailable']
    if not output.is_dir():
        rows.append(f'Output directory missing: {output}; '
                    + ('using queue log' if queue_log else 'pass --queue-log for local evidence'))
    if queue_log:
        rows.append(f'Queue log: {queue_log} (minute precision; receipts take precedence)')
    rows.append('Projection starts now; unfinished pieces are counted in full, excluding future queue waits.')
    return '\n'.join([*rows, *notes])


def self_test():
    specs = [f'{label}:test-{label}' for label in LABELS]
    arms = parse_arms(specs)
    pieces = plan(arms, Path('/not-created'))
    assert len([name for name in pieces if name.startswith('sweep-')]) == 36
    assert all(0 < minutes < 120 for _, minutes in pieces.values())
    for name in ('rpc', 'startup', 'replay-r1', 'replay-r2'):
        assert all('zakura-main=' not in ' '.join(cmd) for cmd in pieces[name][0])
    for name, (commands, _) in pieces.items():
        if name.startswith(('peer-block-', 'tip-template-', 'tip-miner-', 'relay-chain-')):
            labels = LABELS
            if name.startswith('tip-miner-') and not name.endswith('-transparent'):
                labels = LABELS[:3]
                assert all('zakura-main=' not in ' '.join(map(str, cmd)) for cmd in commands)
            assert len(commands) == 5 * len(labels)
            used = [next(a for a in labels if a in cmd) for cmd in commands]
            assert used[:len(labels)] == list(labels)
            assert used[len(labels):2 * len(labels)] == list(labels[1:] + labels[:1])
            assert len({cmd[-1] for cmd in commands}) == len(commands)
    assert len(pieces) == 104, len(pieces)
    core = campaign_plan(arms, Path('/not-created'), 'core')
    extended = campaign_plan(arms, Path('/not-created-extended'), 'extended')
    assert len(core) == 46 and len(extended) == 62
    assert sum(minutes for _, minutes in core.values()) <= 20 * 60
    assert list(core).index('inclusion') > list(core).index('empty-tip-settled')
    assert all(name.endswith('-settled-warm') for name in core if name.startswith('tip-template-'))
    assert all(name.endswith('-cold') for name in core if name.startswith('relay-chain-'))
    sweep_cells = []
    for profile in (core, extended):
        cells = set()
        for name, (commands, _) in profile.items():
            if not name.startswith('sweep-'):
                continue
            command = commands[0]
            fixtures = command[command.index('--fixtures-subset') + 1:command.index('--endpoints')]
            endpoint = command[command.index('--endpoints') + 1]
            cells.update((fixture, endpoint) for fixture in fixtures)
        sweep_cells.append(cells)
    assert len(sweep_cells[0]) == 24 and len(sweep_cells[1]) == 45
    assert sweep_cells[0].isdisjoint(sweep_cells[1])
    for invalid in (specs + [specs[0]], ['release:../bad', *specs[1:]], specs[1:]):
        try:
            parse_arms(invalid)
        except ValueError:
            pass
        else:
            raise AssertionError('invalid arms accepted')
    import tempfile
    import io
    from contextlib import redirect_stdout
    from unittest.mock import patch
    with tempfile.TemporaryDirectory() as directory:
        binary = Path(directory) / 'test-binary'
        binary.write_bytes(b'provenance test, never executed')
        binary.chmod(0o700)
        receipt = dict(source='a' * 40, path=str(binary), build='self-test',
                       sha256=hashlib.sha256(binary.read_bytes()).hexdigest())
        save_once(Path(str(binary) + '.build.json'), receipt)
        assert provenance({'stack': str(binary)}) == {'stack': dict(receipt, binary_preloads=False)}
        for operation in (
            lambda: provenance({'release': str(binary)}),
            lambda: save_once(Path(str(binary) + '.build.json'), dict(receipt, source='b' * 40)),
        ):
            try:
                operation()
            except ValueError:
                pass
            else:
                raise AssertionError('changed provenance accepted')
        binary.write_bytes(b'changed')
        try:
            provenance({'stack': str(binary)})
        except ValueError:
            pass
        else:
            raise AssertionError('changed binary accepted')
        output = Path(directory) / 'campaign'
        log = Path(directory) / 'queue.log'
        log.write_text(
            f'23:55 run one started: bash v5-campaign.sh --piece smoke --output {output}\n'
            '00:25 run one in /repo exit=0\n'
            f'00:26 run two started: bash v5-campaign.sh --piece rpc --output {output}\n'
            '00:27 run two in /repo exit=1\n'
            f'00:28 run other started: bash v5-campaign.sh --piece rpc --output {output}-other\n'
            '00:29 run other in /repo exit=0\n')
        assert queue_durations(log, output) == ('core', {'smoke': 1800})
        now = datetime.fromisoformat('2026-10-26T00:00:00+00:00')
        report = eta(output, log, 'core', now)
        assert 'Pieces: 1/46' in report and 'duration: 2.000x' in report, report
        assert not output.exists()
        assert 'Projected finish: unavailable' in eta(output, None, 'core', now)
        (output / 'completed').mkdir(parents=True)
        save_once(output / 'completed/smoke.json',
                  dict(success=True, started_unix_ns=0, ended_unix_ns=900_000_000_000))
        report = eta(output, log, 'core', now)
        remaining = sum(minutes for name, (_, minutes) in core.items() if name != 'smoke')
        expected = (now + timedelta(minutes=remaining)).isoformat(timespec='minutes')
        assert 'duration: 1.000x' in report and f'Projected finish: {expected}' in report, report
        (output / 'completed/rpc.json').write_text('{')
        assert 'Ignoring incomplete receipt:' in eta(output, log, 'core', now)
        for shared_host in (False, True):
            run_output = Path(directory) / f'shared-{shared_host}'
            argv = ['v5_campaign.py', '--piece', 'smoke', '--output', str(run_output), *specs]
            if shared_host:
                argv.append('--shared-host')
            with patch('sys.argv', argv), patch(__name__ + '.provenance', return_value={}), \
                    patch.object(subprocess, 'run') as run, redirect_stdout(io.StringIO()):
                main()
            calls = [call.args[0] for call in run.call_args_list]
            quiet = ['bash', 'wait-quiet.sh']
            assert (quiet in calls) == (not shared_host)
            if not shared_host:
                assert calls[0] == quiet
            manifest = json.loads((run_output / 'campaign.json').read_text())
            assert manifest.get('shared_host', False) == shared_host
            assert ('shared_host' in manifest) == shared_host
            with patch('sys.argv', [*argv, '--steps']), redirect_stdout(io.StringIO()) as printed:
                main()
            steps = [shlex.split(line) for line in printed.getvalue().splitlines() if not line.startswith('#')]
            assert steps and all(('--shared-host' in step) == shared_host for step in steps)
            other_mode = [arg for arg in argv if arg != '--shared-host']
            if not shared_host:
                other_mode.append('--shared-host')
            with patch('sys.argv', other_mode), patch(__name__ + '.provenance', return_value={}), \
                    patch.object(subprocess, 'run') as run, redirect_stdout(io.StringIO()):
                try:
                    main()
                except ValueError as error:
                    assert 'Campaign identity changed' in str(error)
                else:
                    raise AssertionError('mixed quiet/shared host campaign accepted')
                assert not run.called
    print('v5 campaign self-test: ok')


def main():
    parser = argparse.ArgumentParser(description=__doc__, epilog=BENCH_HELP)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--dry-run', action='store_true')
    mode.add_argument('--steps', action='store_true')
    mode.add_argument('--list', action='store_true')
    mode.add_argument('--self-test', action='store_true')
    mode.add_argument('--eta', type=Path, metavar='OUTPUT_DIR', help='read-only progress and finish estimate')
    parser.add_argument('--queue-log', type=Path, help='supplement --eta with a local queue log')
    parser.add_argument('--piece', help='one name from --list; default runs every piece')
    parser.add_argument('--profile', choices=('core', 'extended', 'comparison'), default='core')
    parser.add_argument('--output', type=Path, help='default: BENCH_OUTPUT/v5[-PROFILE]')
    host = parser.add_mutually_exclusive_group()
    host.add_argument('--shared-host', action='store_true',
                      help='skip the host-wide quiet gate for an explicitly CPU-partitioned shared host')
    host.add_argument('--exclusive', action='store_true',
                      help='acknowledge other host workloads are stopped; permit whole-host comparison CPUs')
    parser.add_argument('arms', nargs='*')
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    if args.queue_log and args.eta is None:
        parser.error('--queue-log requires --eta')
    if args.eta is not None:
        try:
            print(eta(args.eta, args.queue_log, args.profile))
        except (OSError, ValueError, KeyError) as error:
            parser.error(str(error))
        return
    if args.profile == 'comparison' and not args.exclusive and os.environ.get('V5_CPUS') in ('', 'all'):
        parser.error('Whole-host comparison CPUs require --exclusive after stopping other host workloads')
    args.output = (args.output or RUNS / ('v5' if args.profile == 'core' else 'v5-' + args.profile)).resolve()
    try:
        arms = parse_arms(args.arms, args.profile)
        pieces = campaign_plan(arms, args.output, args.profile)
        if args.piece:
            pieces = {args.piece: pieces[args.piece]}
    except (ValueError, KeyError) as error:
        parser.error(str(error))
    quiet_gate = [] if args.shared_host else [['bash', 'wait-quiet.sh']]
    gate_note = ' (shared host; quiet gate skipped)' if args.shared_host else ' + quiet gate'
    if args.steps or args.list:
        total = sum(minutes for _, minutes in pieces.values())
        print(f'# {args.profile.upper()}: {len(pieces)} pieces; approximately {total} min '
              f'({total // 60} h {total % 60} min), excluding quiet gates and builds')
        for name, (_, minutes) in pieces.items():
            if args.list:
                print(f'{name}\t~{minutes} min{gate_note}')
            else:
                print(f'# {name}: approximately {minutes} min{gate_note}')
                print(shlex.join(['bash', str(BENCH / 'v5-campaign.sh'), '--profile', args.profile, '--piece', name,
                      '--output', str(args.output), *(['--shared-host'] if args.shared_host else []),
                      *(['--exclusive'] if args.exclusive else []), *args.arms]))
        return
    environment = dict(os.environ, ZAKURA_MAIN_REVISION=RELEASES['zakura-main'])
    if not args.dry_run:
        declared = provenance(arms, args.profile)
        args.output.mkdir(parents=True, exist_ok=True)
        save_once(args.output / 'arms.json', declared)
        save_once(args.output / 'campaign.json', dict(
            arms=declared, profile=args.profile, pieces=list(campaign_plan(arms, args.output, args.profile)),
            **(dict(cpus=os.environ.get('V5_CPUS', '0-7,16-23') or 'all') if args.profile == 'comparison' else
               dict(snapshot_height=2316084, replay_heights=[1750000, 1752000])),
            **({'exclusive': True} if args.exclusive else {}),
            **({'shared_host': True} if args.shared_host else {})))
        (args.output / 'completed').mkdir(exist_ok=True)
    for name, (commands, minutes) in pieces.items():
        print(f'# {name}: approximately {minutes} min{gate_note}', flush=True)
        if not args.dry_run and (args.output / 'completed' / f'{name}.json').exists():
            raise ValueError('Piece already complete; do not mix reruns: ' + name)
        if not args.dry_run and name != 'smoke' and not (args.output / 'completed/smoke.json').is_file():
            raise ValueError('Run --piece smoke successfully before measurements')
        started = time.time_ns()
        for command in [*quiet_gate, *commands]:
            print(shlex.join(command), flush=True)
            if not args.dry_run:
                subprocess.run(command, cwd=BENCH, env=environment, check=True)
        if not args.dry_run:
            if args.profile == 'comparison' and name != 'smoke':
                summary = comparison_result(name, commands, arms)
                save_once(args.output / name / 'summary.json', summary)
                print(json.dumps(summary, sort_keys=True), flush=True)
            save_once(args.output / 'completed' / f'{name}.json', dict(
                success=True, started_unix_ns=started, ended_unix_ns=time.time_ns(), commands=commands))


if __name__ == '__main__':
    main()
