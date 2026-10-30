#!/usr/bin/env python3
"""Short paired A/B comparison on an otherwise idle host.

Each repetition measures each case once per binary on fresh nodes. Odd repetitions
run A,B; even repetitions B,A (ABBA across two repetitions, not four samples per
repetition). Five repetitions give five matched pairs per case. A single quiet
gate precedes the entire run. --dry-run and --self-test never start nodes or wait.
Fresh nodes wait for Halo2 key preload when the binary supports it (60s deadline),
before replay or timed work. Receipts retain completion relative to the window.

Default cases measure block submission. --cases template instead measures peer
delivery to first/full mining templates: empty and Orchard 250 kB/1.9 MB, transparent
payout, warm receiver mempool, 20-second settled links. Both latencies start at
sender submission, including sender verification and relay, not an internal peer
arrival timestamp. First work is usually coinbase-only; its transaction count is
retained so a first observation that is already full is never mislabeled.

--cases restart-orchard-1900000 restores a separately built prefix state, waits
five seconds after RPC readiness, then submits the first Orchard candidate in
that process. It deliberately skips the preload wait to measure startup overlap;
unlike cold-orchard-1900000, funding replay cannot warm its key.

--cases finalize-transparent-signed-1900000 submits an empty block that retires
the sealed 1.9 MB transparent block. With a 1000-block reorg window, a target at H
is retired by H+1000, NOT H+1001: its measured parent is H+999. Deep block bytes
are built once per comparison, stopped prefix states once per arm; each sample
copies its own arm's state and replays only one untimed filler. Finalized height,
canonical hash and completed RocksDB writes must advance exactly once onto H.
That completion check is outside the timer, even for response-before-finalize arms.
The measured node uses the template cases' transparent payout. Independent observers
start at the response: first usable new-tip template and completed finalization are
reported from the same submission timestamp, alongside primary RPC latency.
Finalization waits for preload before its untimed filler, unlike restart-*.

The latency statistic is median(B_i - A_i), not median(B) - median(A). Its 95%
percentile bootstrap resamples matched repetitions (10,000 draws, fixed seed).
Negative means B is faster. Intervals containing zero mean no detectable change,
not equivalence; one repetition is smoke-only and has no interval. Phase values
are probe delta_seconds converted to milliseconds, including zero observations;
missing series stay unavailable, never zero. Phases can overlap: do not add them.
"""
import argparse
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import random
import re
import shlex
import statistics
import subprocess
import time

from v5_campaign import BENCH, BIN, FIXTURES, RUNS, SIGNED, provenance
from node_bench import BENCH_HELP

# Endpoint, sealed fixture, estimated seconds including setup, optional restart delay.
# The restart delay is fixed here, not a quick-ab command-line tuning knob.
# Planning estimates for stack-vs-stack experiments, not measured runtime guarantees.
CASES = {
    'cold-orchard-1900000': ('cold-submitblock-nominer', 'orchard-1900000', 10, None),
    'cold-transparent-signed-1900000': ('cold-submitblock-nominer', 'transparent-signed-1900000', 6, None),
    'warm-orchard-1900000': ('warm-mempool-submitblock', 'orchard-1900000', 12, None),
    'warm-transparent-signed-250000': ('warm-mempool-submitblock', 'transparent-signed-250000', 12, None),
    'prepared-transparent-signed-1900000': ('prepared-submitblock', 'transparent-signed-1900000', 62, None),
}
DEFAULT_CASES = tuple(CASES)
# Optional submission cases; defaults above retain their original setup.
CASES.update({
    'cold-orchard-250000': ('cold-submitblock-nominer', 'orchard-250000', 6, None),
    'warm-transparent-signed-1900000': ('warm-mempool-submitblock', 'transparent-signed-1900000', 40, None),
    'restart-orchard-1900000': ('cold-submitblock-nominer', 'orchard-1900000', 20, 5),
    'finalize-transparent-signed-1900000': (
        'finalize-submitblock', 'transparent-signed-1900000', 120, 5),
})
TEMPLATE_CASES = {
    'template-empty-2000': ('submit-to-peer-template', 'empty-2000', 30, None),
    'template-orchard-250000': ('submit-to-peer-template', 'orchard-250000', 32, None),
    'template-orchard-1900000': ('submit-to-peer-template', 'orchard-1900000', 40, None),
}
CASES.update(TEMPLATE_CASES)
FINALIZE_CASE = 'finalize-transparent-signed-1900000'
PHASES = ('block_checks', 'transaction_verification', 'state_commit')
SEED, RESAMPLES = 20261023, 10_000


def parse_arms(specs):
    if len(specs) != 2:
        raise ValueError('exactly two label:binary-name arms are required')
    arms = {}
    for spec in specs:
        if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*:[A-Za-z0-9][A-Za-z0-9_.-]*', spec):
            raise ValueError('expected label:binary-name (no paths): ' + spec)
        label, binary = spec.split(':')
        if label in arms:
            raise ValueError('arm labels must differ')
        arms[label] = str(BIN / binary)
    return arms


def parse_cases(value):
    cases = list(TEMPLATE_CASES) if value == 'template' else value.split(',')
    if not cases or len(cases) != len(set(cases)) or any(case not in CASES for case in cases):
        raise ValueError('--cases must be template or a nonempty, duplicate-free subset of: '
                         + ','.join(CASES))
    return cases


def plan(arms, cases, reps, output):
    samples = []
    for rep in range(1, reps + 1):
        order = list(arms) if rep % 2 else list(reversed(arms))
        for case in cases:
            endpoint, fixture, _, restart_delay = CASES[case]
            fixtures = SIGNED if fixture.startswith('transparent-signed-') else FIXTURES
            for position, label in enumerate(order):
                directory = output / case / f'{rep:02d}-{label}'
                if case in TEMPLATE_CASES:
                    command = ['python3', '-B', str(BENCH / 'sweep/tip_to_template.py'),
                               '--fixtures', str(fixtures), '--temperature', 'warm',
                               '--settle-seconds', '20', '--poll-ms', '1', '--miner', 'transparent',
                               arms[label], label, fixture, str(directory)]
                else:
                    command = ['python3', '-B', str(BENCH / 'phase-probe.py'),
                               '--fixtures', str(fixtures), '--endpoint', endpoint,
                               arms[label], label, fixture, '1', str(directory)]
                    if restart_delay is not None:
                        command += ['--restart-delay-seconds', str(restart_delay)]
                    if case == FINALIZE_CASE:
                        command += ['--finalize-prefix-dir', str(output / case / 'prefix')]
                samples.append(dict(case=case, repetition=rep, position=position,
                                    arm=label, command=command, directory=str(directory)))
    return samples


def percentile(ordered, fraction):
    index = (len(ordered) - 1) * fraction
    low = math.floor(index)
    high = math.ceil(index)
    return ordered[low] + (ordered[high] - ordered[low]) * (index - low)


def paired_stats(a, b):
    if not a or len(a) != len(b):
        raise ValueError('nonempty matched samples are required')
    for value in a + b:
        if value is not None and (isinstance(value, bool) or not isinstance(value, (int, float))
                                  or not math.isfinite(value) or value < 0):
            raise ValueError('timings must be finite, nonnegative numbers or absent')
    result = dict(median_a_ms=None if None in a else statistics.median(a),
                  median_b_ms=None if None in b else statistics.median(b),
                  median_paired_delta_ms=None, ci95_ms=None,
                  available_a=sum(x is not None for x in a),
                  available_b=sum(x is not None for x in b),
                  paired_differences_ms=None)
    # Do not discard incomplete pairs or substitute zero for an absent phase series.
    if None in a or None in b:
        return result
    differences = [right - left for left, right in zip(a, b)]
    result.update(median_paired_delta_ms=statistics.median(differences),
                  paired_differences_ms=differences)
    if len(differences) > 1:
        rng = random.Random(SEED)
        boot = sorted(statistics.median(rng.choices(differences, k=len(differences)))
                      for _ in range(RESAMPLES))
        result['ci95_ms'] = [percentile(boot, 0.025), percentile(boot, 0.975)]
    return result


def verdict(stats):
    interval = stats['ci95_ms']
    if interval is not None and interval[1] < 0:
        return 'faster'
    if interval is not None and interval[0] > 0:
        return 'slower'
    return 'no detectable change'


def summarize(rows, arms, cases, reps):
    indexed = {}
    expected = {(case, rep, label) for case in cases
                for rep in range(1, reps + 1) for label in arms}
    for row in rows:
        key = (row['case'], row['repetition'], row['arm'])
        if key not in expected or key in indexed:
            raise ValueError('unexpected or duplicate sample: ' + str(key))
        indexed[key] = row
    if indexed.keys() != expected:
        raise ValueError('incomplete comparison; no samples may be silently dropped')
    result = {}
    for case in cases:
        paired = [[indexed[case, rep, label] for rep in range(1, reps + 1)] for label in arms]
        latency = paired_stats(*[[row['latency_ms'] for row in arm] for arm in paired])
        if latency['paired_differences_ms'] is None:
            raise ValueError('latency must be present in every sample')
        if case in TEMPLATE_CASES:
            first = paired_stats(*[[row['first_template_latency_ms'] for row in arm] for arm in paired])
            if first['paired_differences_ms'] is None:
                raise ValueError('first-template latency must be present in every sample')
            result[case] = dict(
                endpoint=CASES[case][0], fixture=CASES[case][1], pairs=reps,
                first_template_latency=first, full_template_latency=latency,
                first_template_verdict=verdict(first), verdict=verdict(latency),
                first_template_transactions={
                    label: [row['first_template_transactions'] for row in arm]
                    for label, arm in zip(arms, paired)})
            continue
        phases = {}
        for phase in PHASES:
            key = f'zebra_consensus_block_phase_duration_seconds{{phase="{phase}"}}'
            values = [[row['delta_seconds'].get(key) for row in arm] for arm in paired]
            phases[phase] = paired_stats(*[[None if x is None else x * 1000 for x in arm]
                                         for arm in values])
        result[case] = dict(endpoint=CASES[case][0], fixture=CASES[case][1], pairs=reps,
                            latency=latency, phases=phases, verdict=verdict(latency))
        if case == FINALIZE_CASE:
            for name in ("first_template", "finalization"):
                stats = paired_stats(*[[row[name + "_latency_ms"] for row in arm] for arm in paired])
                if stats["paired_differences_ms"] is None:
                    raise ValueError("finalization endpoints must be present in every sample")
                result[case][name + "_latency"] = stats
                result[case][name + "_verdict"] = verdict(stats)
    return result


def describe(stats):
    def number(value):
        return 'unavailable' if value is None else f'{value:.3f}'
    interval = stats['ci95_ms']
    ci = 'unavailable' if interval is None else f'[{interval[0]:.3f}, {interval[1]:.3f}]'
    return (f'A={number(stats["median_a_ms"])} B={number(stats["median_b_ms"])} '
            f'paired B-A={number(stats["median_paired_delta_ms"])} 95% CI={ci} ms')


def read_sample(sample, arms):
    directory = Path(sample['directory'])
    rows = [json.loads(line) for line in (directory / 'samples.jsonl').read_text().splitlines()]
    if len(rows) != 1:
        raise ValueError('expected exactly one probe row: ' + str(directory))
    row = rows[0]
    endpoint, fixture, _, restart_delay = CASES[sample['case']]
    if (row['arm'] != sample['arm'] or row['endpoint'] != endpoint or row['fixture'] != fixture
            or row['binary'] != str(Path(arms[sample['arm']]).resolve())):
        raise ValueError('probe identity mismatch: ' + str(directory))
    if sample['case'] in TEMPLATE_CASES:
        if (row['status'] != 'ok' or row['temperature'] != 'warm' or row['miner'] != 'transparent'
                or row['settle_seconds'] != 20 or row['poll_interval_ms'] != 1
                or row['fixture_directory'] != str(FIXTURES.resolve())):
            raise ValueError('failed or mismatched template measurement: ' + str(directory))
        for key in ('latency_ns', 'first_template_latency_ns'):
            value = row[key]
            if (isinstance(value, bool) or not isinstance(value, (int, float))
                    or not math.isfinite(value) or value < 0):
                raise ValueError('template timings must be finite nonnegative numbers')
        count = row['first_template_transactions']
        if (type(count) is not int or count < 0
                or row['first_template_latency_ns'] > row['latency_ns']):
            raise ValueError('invalid first-template observation: ' + str(directory))
        row = dict(row, latency_ms=row['latency_ns'] / 1_000_000,
                   first_template_latency_ms=row['first_template_latency_ns'] / 1_000_000)
    else:
        if row['sample'] != 1:
            raise ValueError('probe sample mismatch: ' + str(directory))
        window = json.loads((directory / 'sample-0001/node/measurement-window.json').read_text())
        if window['failed'] is not False:
            raise ValueError('failed measurement window: ' + str(directory))
        if (row.get('restart_delay_seconds') != restart_delay
                or window.get('restart_delay_seconds') != restart_delay):
            raise ValueError('probe restart setup mismatch: ' + str(directory))
        if row.get('preload') != window.get('preload'):
            raise ValueError('sample/window preload evidence mismatch: ' + str(directory))
        if sample['case'] == FINALIZE_CASE:
            proof = row.get('finalization', {})
            target, before, after = (proof.get(key, {}) for key in ('target', 'before', 'after'))
            expected = {key: value for key, value in proof.items()
                        if key not in ('before', 'after', 'check')}
            if (window.get('finalization') != expected or proof.get('max_block_reorg_height') != 1000
                    or proof.get('filler_blocks') != 999 or not target
                    or proof.get('parent_height') != target['height'] + 999
                    or proof.get('candidate_height') != target['height'] + 1000
                    or before.get('height') != target['height'] - 1
                    or before.get('hash') != target['parent']
                    or after.get('height') != target['height'] or after.get('hash') != target['hash']
                    or before.get('rocksdb_batch_commits') != 1
                    or after.get('rocksdb_batch_commits') != 2
                    or row.get('candidate_sha256') != window.get('candidate_sha256')):
                raise ValueError('missing or incorrect finalization proof: ' + str(directory))
            endpoints = (
                ("submit_response_ns", "latency_ms"),
                ("first_template_observed_ns", "first_template_latency_ms"),
                ("finalization_observed_ns", "finalization_latency_ms"),
            )
            if type(row.get("start_ns")) is not int or not row.get("candidate_sha256"):
                raise ValueError("missing finalization measurement identity")
            for stamp, duration in endpoints:
                if (type(row.get(stamp)) is not int or row[stamp] < row["submit_response_ns"]
                        or row.get(duration) != (row[stamp] - row["start_ns"]) / 1e6):
                    raise ValueError("invalid finalization endpoint timing")
            if after.get("observed_ns") != row["finalization_observed_ns"]:
                raise ValueError("finalization timestamp differs from its proof")
    preloads = row.get('preload', {})
    if sample['case'] not in TEMPLATE_CASES:
        preloads = {'node': preloads}
    if not preloads:
        raise ValueError('missing preload evidence: ' + str(directory))
    for preload in preloads.values():
        if (type(preload.get('binary_preloads')) is not bool
                or preload.get('preload_wait') is not (not sample['case'].startswith('restart-'))):
            raise ValueError('missing or incorrect preload policy: ' + str(directory))
        relative = preload.get('preload_done_relative_ns')
        if preload['binary_preloads'] and preload['preload_wait']:
            if type(relative) is not int or relative > 0:
                raise ValueError('preload did not finish before measurement: ' + str(directory))
    return dict(row, case=sample['case'], repetition=sample['repetition'],
                position=sample['position'], directory=str(directory))


def finalization_self_test():
    """Exercise real prefix/copy/observer code with an in-memory 1000-block writer."""
    import importlib.util
    import sys
    from tempfile import TemporaryDirectory
    from types import SimpleNamespace
    from unittest.mock import MagicMock, patch

    sys.path.insert(0, str(BENCH / 'sweep'))
    import run_sweep
    spec = importlib.util.spec_from_file_location('phase_probe', BENCH / 'phase-probe.py')
    probe = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(probe)
    assert FINALIZE_CASE not in DEFAULT_CASES and parse_cases(FINALIZE_CASE) == [FINALIZE_CASE]
    blocks, nodes, created = {}, {}, []

    def template(parent, height):
        return dict(previousblockhash=parent, height=height, version=4, curtime=1, sizelimit=2_000_000,
                    bits='207fffff', transactions=[], coinbasetxn=dict(data=height.to_bytes(4, 'little').hex()),
                    defaultroots=dict(merkleroot=bytes(range(32)).hex(),
                                      blockcommitmentshash=bytes(range(32, 64)).hex()))

    def block(parent, height):
        raw = run_sweep.h.empty_regtest_block(template(parent, height))
        value = dict(height=height, raw=raw, **run_sweep.block_identity(raw))
        blocks[value['hash']] = value
        return value

    genesis = dict(height=0, hash='00' * 32)
    first = block(genesis['hash'], 1)
    second = block(first['hash'], 2)
    large = block(second['hash'], 3)
    header = bytes.fromhex(large['raw'])
    assert header[36:68] == bytes(range(32))[::-1]
    assert header[68:100] == bytes(range(32, 64))[::-1]
    assert header[140:178] == b'\x24' + bytes(36) + b'\x01'

    def fake_node(binary, directory, arm, timeout, *, miner):
        directory.mkdir(parents=True)
        created.append(directory)
        node = MagicMock(dir=directory, config=directory / 'config.toml', rpc_addr=str(directory))
        node.preload_receipt.return_value = dict(
            binary_preloads=True, preload_wait=True, preload_done_relative_ns=-1)
        node.config.write_text('[state]\nephemeral = false\n')
        nodes[str(directory)] = node
        node.retained, node.finalized, node.writes = [], genesis, 0
        node.template_calls = 0
        def ready():
            if directory.name == 'node':
                assert node.preload_wait is True  # Finalization is not the restart exception.
            if (directory / 'state/snapshot.json').exists():
                snapshot = json.loads((directory / 'state/snapshot.json').read_text())
                node.retained, node.finalized = snapshot['retained'], snapshot['finalized']
                assert len(node.retained) == 1000
                assert node.retained[0]['height'] == 2
                assert node.retained[-1]['height'] == 1001
            return node
        def stop(*_):
            (directory / 'state').mkdir(exist_ok=True)
            (directory / 'state/snapshot.json').write_text(json.dumps(
                dict(retained=node.retained, finalized=node.finalized)))
        def rpc(method, params=None):
            tip = node.retained[-1] if node.retained else node.finalized
            if method == 'submitblock':
                raw = params[0]
                value = dict(raw=raw, height=int.from_bytes(bytes.fromhex(raw)[-4:], 'little'),
                             **run_sweep.block_identity(raw))
                assert value['parent'] == tip['hash'] and value['height'] == tip['height'] + 1
                blocks[value['hash']] = value
                node.retained.append(value)
                if len(node.retained) > 1000:
                    node.finalized = node.retained.pop(0)
                    node.writes += 1
                return None
            if method == 'getbestblockhash':
                return tip['hash']
            if method == 'getblocktemplate':
                assert miner == run_sweep.h.P2SH_MINER
                node.template_calls += 1
                return template(tip['hash'], tip['height'] + 1)
            if method == 'getblock':
                return blocks[params[0]]['raw']
            if method == 'getblockhash':
                return next(b['hash'] for b in blocks.values() if b['height'] == params[0])
            assert method in ('getrawmempool', 'getpeerinfo')
            return []
        node.__enter__.side_effect, node.__exit__.side_effect = ready, stop
        node.rpc.call.side_effect = rpc
        return node

    def scrape(node):
        return (f'state_finalized_block_height {node.finalized["height"]}\n'
                'zebra_state_rocksdb_batch_commit_duration_seconds_sum 0.1\n'
                f'zebra_state_rocksdb_batch_commit_duration_seconds_count {node.writes}\n')

    with TemporaryDirectory() as temporary:
        root = Path(temporary)
        arms = {label: str(root / label) for label in ('base', 'candidate')}
        for label, binary in arms.items():
            Path(binary).write_text(label)
        samples = plan(arms, [FINALIZE_CASE], 2, root / 'run')
        assert [sample['arm'] for sample in samples] == ['base', 'candidate', 'candidate', 'base']
        cache = root / 'run' / FINALIZE_CASE / 'prefix'
        rows = []
        original_submit = run_sweep.timed_submit
        def measured(node, candidate):
            return probe.finalization_submit(
                node, candidate, original_submit, lambda _: None, lambda *_: {})
        with patch.object(run_sweep, 'Node', fake_node), patch.object(run_sweep.time, 'sleep'), \
                patch.object(probe, 'scrape', scrape), patch.object(run_sweep, 'timed_submit', measured), \
                patch.object(run_sweep.h, 'RPC', lambda address, **_: nodes[address].rpc):
            for sample in samples:
                assert sample['command'][-2:] == ['--finalize-prefix-dir', str(cache)]
                candidate, state, prelude = run_sweep.finalization_prefix(
                    sample['arm'], arms[sample['arm']], cache, large, [first, second], 1)
                assert candidate['height'] == 1003 and prelude['height'] == 1002
                directory = Path(sample['directory'])
                result = run_sweep.case(
                    sample['arm'], arms[sample['arm']], directory / 'sample-0001',
                    candidate, [], SimpleNamespace(timeout=1, poll_ms=1), 'finalize-submitblock',
                    restart_delay_seconds=5, prefix_state=state, prelude=prelude)
                assert result['finalization']['before']['height'] == 2
                assert result['finalization']['after']['height'] == 3
                assert result['first_template_observed_ns'] >= result['end_ns']
                assert result['finalization_observed_ns'] >= result['end_ns']
                assert nodes[str(directory / 'sample-0001/node')].template_calls == 1
                receipt = dict(arm=sample['arm'], binary=arms[sample['arm']], sample=1,
                               fixture=CASES[FINALIZE_CASE][1], endpoint='finalize-submitblock',
                               restart_delay_seconds=5, delta_seconds={}, finalization=result['finalization'],
                               preload=result['preload'],
                               candidate_sha256=candidate['sha256'], start_ns=result['start_ns'],
                               submit_response_ns=result['end_ns'],
                               first_template_observed_ns=result['first_template_observed_ns'],
                               finalization_observed_ns=result['finalization_observed_ns'],
                               latency_ms=result['latency_ns'] / 1e6,
                               first_template_latency_ms=result['first_template_latency_ns'] / 1e6,
                               finalization_latency_ms=result['finalization_latency_ns'] / 1e6)
                (directory / 'samples.jsonl').write_text(json.dumps(receipt) + '\n')
                (directory / 'sample-0001/node/measurement-window.json').write_text(json.dumps(
                    dict(failed=False, restart_delay_seconds=5, finalization=candidate['finalization'],
                         candidate_sha256=candidate['sha256'], preload=result['preload'])))
                rows.append(read_sample(sample, arms))
            assert len(created) == 7  # Two arm prefixes, one candidate builder, four samples.
        summary = summarize(rows, arms, [FINALIZE_CASE], 2)[FINALIZE_CASE]
        assert all(summary[name]['paired_differences_ms'] is not None for name in (
            'latency', 'first_template_latency', 'finalization_latency'))
        for change in (dict(finalization={}), dict(first_template_latency_ms=None),
                       dict(finalization_observed_ns=0), dict(candidate_sha256='wrong')):
            (directory / 'samples.jsonl').write_text(json.dumps(dict(receipt, **change)) + '\n')
            try:
                read_sample(sample, arms)
            except ValueError:
                pass
            else:
                raise AssertionError('incorrect finalization evidence accepted')


def self_test():
    from tempfile import TemporaryDirectory
    from types import SimpleNamespace
    from unittest.mock import MagicMock, patch
    import sys

    sys.path.insert(0, str(BENCH / 'sweep'))
    import run_sweep
    finalization_self_test()
    run_sweep.h.preload_self_test()

    a = [10, 20, 30, 40, 50]
    slower = paired_stats(a, [x + 2 for x in a])
    assert slower['median_a_ms'] == 30 and slower['median_b_ms'] == 32
    assert slower['median_paired_delta_ms'] == 2 and slower['ci95_ms'] == [2, 2]
    assert verdict(slower) == 'slower'
    assert verdict(paired_stats(a, [x - 2 for x in a])) == 'faster'
    assert verdict(paired_stats(a, a)) == 'no detectable change'
    paired = paired_stats([0, 100, 101], [50, 51, 102])
    assert paired['median_paired_delta_ms'] == 1  # Difference of medians would be -49.
    assert paired['ci95_ms'] == [-49, 50] and verdict(paired) == 'no detectable change'
    assert percentile([0, 10], 0.025) == 0.25 and percentile([0, 10], 0.975) == 9.75
    assert paired_stats([1], [2])['ci95_ms'] is None
    assert verdict(paired_stats([1], [2])) == 'no detectable change'
    assert paired_stats([None, None], [0, 0])['median_a_ms'] is None
    assert paired_stats([1, None], [0, 0])['median_paired_delta_ms'] is None
    arms = parse_arms(['base:zebrad-base', 'candidate:zebrad-candidate'])
    case = next(iter(CASES))
    samples = plan(arms, [case], 5, Path('/not-created'))
    assert [s['arm'] for s in samples] == ['base', 'candidate', 'candidate', 'base'] * 2 + ['base', 'candidate']
    assert len({s['directory'] for s in samples}) == 10
    key = 'zebra_consensus_block_phase_duration_seconds{phase="block_checks"}'
    rows = [dict(s, latency_ms=10 + (s['arm'] == 'candidate'),
                 delta_seconds={key: 0.002 if s['arm'] == 'candidate' else 0.001}) for s in samples]
    summary = summarize(list(reversed(rows)), arms, [case], 5)[case]
    assert summary['verdict'] == 'slower' and summary['phases']['block_checks']['median_paired_delta_ms'] == 1
    assert summary['phases']['state_commit']['median_paired_delta_ms'] is None
    restart = 'restart-orchard-1900000'
    assert parse_cases(restart) == [restart] and restart not in DEFAULT_CASES
    assert all('--restart-delay-seconds' not in s['command'] for s in plan(
        arms, DEFAULT_CASES, 1, Path('/not-created')))
    with TemporaryDirectory() as directory:
        restarted = plan(arms, [restart], 5, Path(directory))
        assert [s['arm'] for s in restarted] == [s['arm'] for s in samples]
        assert len({s['directory'] for s in restarted}) == 10
        for sample in restarted:
            assert sample['command'] == [
                'python3', '-B', str(BENCH / 'phase-probe.py'), '--fixtures', str(FIXTURES),
                '--endpoint', 'cold-submitblock-nominer', arms[sample['arm']], sample['arm'],
                'orchard-1900000', '1', sample['directory'], '--restart-delay-seconds', '5']
        sample = restarted[0]
        path = Path(sample['directory'])
        (path / 'sample-0001/node').mkdir(parents=True)
        receipt = dict(arm=sample['arm'], binary=arms[sample['arm']], sample=1,
                       endpoint='cold-submitblock-nominer', fixture='orchard-1900000',
                       latency_ms=1100, delta_seconds={}, restart_delay_seconds=5,
                       preload=dict(binary_preloads=True, preload_wait=False, preload_done_relative_ns=None))
        (path / 'samples.jsonl').write_text(json.dumps(receipt) + '\n')
        window = path / 'sample-0001/node/measurement-window.json'
        window.write_text(json.dumps(dict(failed=False, restart_delay_seconds=5, preload=receipt['preload'])))
        assert read_sample(sample, arms)['latency_ms'] == 1100
        for delay in (None, 0):
            window.write_text(json.dumps(dict(failed=False, restart_delay_seconds=delay)))
            try:
                read_sample(sample, arms)
            except ValueError:
                pass
            else:
                raise AssertionError('missing or incorrect restart delay accepted')

        # Exercise the real endpoint and filesystem copy, but never launch nodes or sleep.
        events = []
        def fake_node(binary, directory, arm, timeout, *, miner):
            assert miner is None
            directory.mkdir(parents=True)
            node = MagicMock(dir=directory, config=directory / 'config.toml', tip='genesis')
            node.preload_receipt.return_value = dict(
                binary_preloads=True, preload_wait=False, preload_done_relative_ns=None)
            node.config.write_text('[state]\nephemeral = false\n')
            name = directory.name
            def ready():
                if name == 'prefix':
                    assert 'debug_skip_non_finalized_state_backup_task = true' in node.config.read_text()
                else:
                    assert node.preload_wait is False
                    assert events[-1] == ('prefix', 'stop')
                    assert 'debug_skip_non_finalized_state_backup_task' not in node.config.read_text()
                    node.tip = (directory / 'state/tip').read_text()
                events.append((name, 'ready'))
                return node
            def stop(*_):
                if name == 'prefix':
                    (directory / 'state').mkdir()
                    (directory / 'state/tip').write_text(node.tip)
                events.append((name, 'stop'))
            def rpc(method, params=None):
                events.append((name, method))
                if method == 'submitblock':
                    assert params == (['00'] if name == 'prefix' else ['cc'])
                    node.tip = 'parent' if name == 'prefix' else 'candidate'
                    return None
                if method == 'getbestblockhash':
                    return node.tip
                if method in ('getpeerinfo', 'getrawmempool'):
                    return []
                assert method == 'getblock' and params == ['candidate', 0]
                return 'cc'
            node.__enter__.side_effect, node.__exit__.side_effect = ready, stop
            node.rpc.call.side_effect = rpc
            return node
        with patch.object(run_sweep, 'Node', fake_node), patch.object(
                run_sweep.time, 'sleep', lambda delay: events.append(('sleep', delay))):
            result = run_sweep.case(
                'base', '/unused', Path(directory) / 'flow',
                dict(parent='parent', hash='candidate', raw='cc'), [dict(hash='parent', raw='00')],
                SimpleNamespace(timeout=1, poll_ms=1), 'cold-submitblock-nominer',
                restart_delay_seconds=5)
        assert result['latency_ns'] > 0
        assert events == [
            ('prefix', 'ready'), ('prefix', 'submitblock'), ('prefix', 'getbestblockhash'),
            ('prefix', 'stop'), ('node', 'ready'), ('sleep', 5), ('node', 'getbestblockhash'),
            ('node', 'getpeerinfo'), ('node', 'getrawmempool'), ('node', 'submitblock'),
            ('node', 'getbestblockhash'), ('node', 'getblock'), ('node', 'stop')]
    template_cases = parse_cases('template')
    assert [CASES[name][1] for name in template_cases] == ['empty-2000', 'orchard-250000', 'orchard-1900000']
    assert 3 + 10 * sum(CASES[name][2] for name in template_cases) / 60 == 20
    template_rows = []
    with TemporaryDirectory() as directory:
        templates = plan(arms, template_cases, 5, Path(directory))
        assert len(templates) == len({s['directory'] for s in templates}) == 30
        for name in template_cases:
            assert [s['arm'] for s in templates if s['case'] == name] == [s['arm'] for s in samples]
        for sample in templates:
            command = sample['command']
            assert command[:3] == ['python3', '-B', str(BENCH / 'sweep/tip_to_template.py')]
            assert command[3:-4] == ['--fixtures', str(FIXTURES), '--temperature', 'warm',
                                    '--settle-seconds', '20', '--poll-ms', '1', '--miner', 'transparent']
            assert command[-4:] == [arms[sample['arm']], sample['arm'],
                                    CASES[sample['case']][1], sample['directory']]
            candidate = sample['arm'] == 'candidate'
            receipt = dict(arm=sample['arm'], binary=arms[sample['arm']],
                           endpoint='submit-to-peer-template', fixture=CASES[sample['case']][1],
                           status='ok', temperature='warm', miner='transparent',
                           settle_seconds=20, poll_interval_ms=1, fixture_directory=str(FIXTURES),
                           preload={name: dict(binary_preloads=True, preload_wait=True,
                                               preload_done_relative_ns=-1)
                                    for name in ('sender', 'receiver')},
                           first_template_transactions=0,
                           first_template_latency_ns=(sample['repetition'] * 10 + candidate) * 1_000_000,
                           latency_ns=(sample['repetition'] * 20 + 2 * candidate) * 1_000_000)
            path = Path(sample['directory'])
            path.mkdir(parents=True)
            path = path / 'samples.jsonl'
            path.write_text(json.dumps(receipt) + '\n')
            template_rows.append(read_sample(sample, arms))
        for change in (dict(status='error'), dict(miner='orchard'), dict(temperature='cold'),
                       dict(settle_seconds=0), dict(fixture='mixed-1900000'),
                       dict(first_template_latency_ns=-1), dict(latency_ns=float('nan')),
                       dict(first_template_latency_ns=True), dict(first_template_transactions=-1),
                       dict(first_template_latency_ns=receipt['latency_ns'] + 1)):
            path.write_text(json.dumps(dict(receipt, **change)) + '\n')
            try:
                read_sample(sample, arms)
            except ValueError:
                pass
            else:
                raise AssertionError('invalid template receipt accepted: ' + str(change))
        # A first response can already be full; retain its count rather than call it coinbase-only.
        path.write_text(json.dumps(dict(receipt, first_template_transactions=2,
                                       first_template_latency_ns=receipt['latency_ns'])) + '\n')
        observed = read_sample(sample, arms)
        assert observed['first_template_transactions'] == 2
        assert observed['first_template_latency_ms'] == observed['latency_ms']
    template_summary = summarize(list(reversed(template_rows)), arms, template_cases, 5)
    for result in template_summary.values():
        first, full = result['first_template_latency'], result['full_template_latency']
        assert first['median_paired_delta_ms'] == 1 and first['ci95_ms'] == [1, 1]
        assert full['median_paired_delta_ms'] == 2 and full['ci95_ms'] == [2, 2]
        assert result['first_template_verdict'] == result['verdict'] == 'slower'
        assert result['first_template_transactions'] == {label: [0] * 5 for label in arms}
    for operation in (lambda: summarize(rows[:-1], arms, [case], 5),
                      lambda: summarize(rows + rows[:1], arms, [case], 5),
                      lambda: summarize(template_rows[:-1], arms, template_cases, 5),
                      lambda: summarize(template_rows + template_rows[:1], arms, template_cases, 5),
                      lambda: summarize([dict(row, first_template_latency_ms=None) for row in template_rows],
                                        arms, template_cases, 5),
                      lambda: parse_cases('template,template'),
                      lambda: paired_stats([float('nan')], [1]),
                      lambda: paired_stats([-1], [1]),
                      lambda: parse_arms(['base:../binary', 'candidate:binary']),
                      lambda: parse_arms(['base:binary', 'base:other'])):
        try:
            operation()
        except ValueError:
            pass
        else:
            raise AssertionError('invalid comparison accepted')
    print('quick-ab self-test: ok (paired medians, bootstrap, verdicts, missing phases, ABBA, '
          'completeness, preload detection/gate/timeout/receipts, restart exception/state-copy ordering, '
          'first/full template summaries, deep prefix reuse/restore/finalization/template endpoints)')


def main():
    parser = argparse.ArgumentParser(description=__doc__, epilog=BENCH_HELP, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--dry-run', action='store_true', help='print commands/estimate; do not create output or inspect binaries')
    mode.add_argument('--self-test', action='store_true', help='check summary math without binaries, fixtures or nodes')
    parser.add_argument('--reps', type=int, default=5, help='matched repetitions per case (default: 5)')
    parser.add_argument('--cases', default=','.join(DEFAULT_CASES),
                        help='template (empty/Orchard, warm/settled, transparent payout), or a '
                             'comma-separated subset of: ' + ','.join(CASES)
                             + '; default submission cases: ' + ','.join(DEFAULT_CASES))
    parser.add_argument('arms', nargs='*', metavar='label:binary-name')
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    try:
        arms = parse_arms(args.arms)
        cases = parse_cases(args.cases)
        if args.reps < 1:
            raise ValueError('--reps must be positive')
    except ValueError as error:
        parser.error(str(error))
    a, b = arms
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    output = RUNS / f'quick-ab-{stamp}-{a}-vs-{b}'
    samples = plan(arms, cases, args.reps, output)
    work_minutes = 2 * args.reps * sum(CASES[case][2] for case in cases) / 60
    print(f'# Output: {output}', flush=True)
    print(f'# A={a}, B={b}; {args.reps} paired reps/case; {len(samples)} samples; '
          'odd reps A,B; even reps B,A (ABBA)', flush=True)
    print(f'# Estimate: ~{work_minutes + 3:.1f} min including one 3-min quiet gate '
          f'(~{work_minutes:.1f} min samples; planning range '
          f'{work_minutes * 0.75 + 3:.1f}-{work_minutes * 1.25 + 3:.1f} min). '
          'Builds/additional busy-host waiting excluded; quiet gate can take up to 120 min.', flush=True)
    quiet = ['bash', str(BENCH / 'wait-quiet.sh')]
    if args.dry_run:
        for command in [quiet, *(sample['command'] for sample in samples)]:
            print(shlex.join(command))
        return
    declared = provenance(arms)
    output.mkdir(parents=True)  # Never mix earlier measurements or silently overwrite a run.
    manifest = dict(arms=declared, a=a, b=b, reps=args.reps, cases=cases, samples=samples,
                    estimated_minutes=work_minutes + 3,
                    bootstrap=dict(method='paired percentile median B-A', seed=SEED, resamples=RESAMPLES))
    (output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print(shlex.join(quiet), flush=True)
    subprocess.run(quiet, cwd=BENCH, check=True)
    rows = []
    with (output / 'samples.jsonl').open('x') as stream:
        for sample in samples:
            print(shlex.join(sample['command']), flush=True)
            started = time.monotonic()
            subprocess.run(sample['command'], cwd=BENCH, check=True)
            row = read_sample(sample, arms)
            row['elapsed_s'] = time.monotonic() - started
            stream.write(json.dumps(row, allow_nan=False) + '\n')
            stream.flush()
            rows.append(row)
    results = summarize(rows, arms, cases, args.reps)
    summary = dict(a=a, b=b, reps=args.reps, cases=results, bootstrap=manifest['bootstrap'],
                   note='Missing phase series are unavailable, not zero; phases are not additive. '
                        'One pair has no interval. No detectable change does not establish equivalence. '
                        'Template latencies start at sender submitblock, including verification/relay; '
                        'first work is coinbase-only only when first_template_transactions is zero.')
    (output / 'summary.json').write_text(json.dumps(summary, indent=2, allow_nan=False) + '\n')
    for case, result in results.items():
        if case in TEMPLATE_CASES:
            print(f'{case}:')
            for name in ('first', 'full'):
                decision = result['first_template_verdict'] if name == 'first' else result['verdict']
                print(f'  {name}_template: {b} {decision}; {describe(result[name + "_template_latency"])}')
            counts = result['first_template_transactions']
            print('  first-template coinbase-only observations: '
                  f'A={counts[a].count(0)}/{args.reps} B={counts[b].count(0)}/{args.reps}')
        else:
            print(f'{case}: {b} {result["verdict"]}; {describe(result["latency"])}')
            for phase, stats in result['phases'].items():
                print(f'  {phase}: {describe(stats)}')
            if case == FINALIZE_CASE:
                for name in ("first_template", "finalization"):
                    print(f'  {name}: {b} {result[name + "_verdict"]}; '
                          f'{describe(result[name + "_latency"])}')
    print(f'Summary: {output / "summary.json"}')


if __name__ == '__main__':
    main()
