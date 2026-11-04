#!/usr/bin/env python3
"""Replay the Zakura comparison window through submitblock, without peers or mining.

Fetch once with pruning/fetch-blocks.sh; supply implementation-specific stopped
state.cache_dir roots at height 3,505,700. Every run uses a fresh reflink copy.
Linux, Python 3.11+, and a reflink-capable filesystem are required. No cache drops.
"""
import argparse
import csv
import json
import os
from pathlib import Path
import re
import shutil
import signal
import statistics
import sys
import time

from front_common import MainnetNode, interleave, load_arms, reflink_copy, save_json, utc_now
from node_bench import summary
from sync_replay import check_cpus, cpu_list

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'pruning'))
from measure import block_check, load, require

START, END = 3_505_700, 3_538_560
SOURCES = {
    'stack': '5527c563929fbe3b9e82d099b4d4cfc19b092e20',
    'crypto': '5527c563929fbe3b9e82d099b4d4cfc19b092e20',
    'andale': '7ae1a827471235e8d14204e2aaa77dc9b61ca25c',
    'zakura-main': 'f5c20398c629c8179364b07f059da3c814af78ec',
}
PROFILE_SETTINGS = dict(
    OPT_LEVEL='3', LTO='thin', CODEGEN_UNITS='1', DEBUG='0', STRIP='none',
    PANIC='unwind', INCREMENTAL='false', DEBUG_ASSERTIONS='false', OVERFLOW_CHECKS='false')


def validate_arms(arms, extra_revision=None):
    sources = dict(SOURCES)
    if extra_revision is not None:
        require(re.fullmatch(r'[0-9a-f]{40}', extra_revision) is not None,
                '--extra-revision must be a full commit hash')
        sources['extra'] = extra_revision
    require({arm.label for arm in arms} == sources.keys(),
            'provide exactly these arms: ' + ', '.join(sources))
    for arm in arms:
        require(arm.provenance.get('source') == sources[arm.label], 'wrong source pin: ' + arm.label)
        recipe = arm.provenance.get('build', '')
        require(bool(recipe) and (arm.label == 'extra' or
                ('pasta-asm' in recipe) == (arm.label == 'crypto')),
                'incorrect Pasta feature build receipt: ' + arm.label)
    compiler = arms[0].provenance.get('compiler_settings')
    require(bool(compiler) and all(arm.provenance.get('compiler_settings') == compiler for arm in arms),
            'arms need matching compiler_settings receipts; rebuild with the runbook environment')
    environment = compiler.get('environment', {})
    require(all(environment.get('CARGO_PROFILE_RELEASE_' + key) == value
                for key, value in PROFILE_SETTINGS.items()),
            'set the explicit common release profile from the runbook')
    # Zakura overrides dependency codegen units in Cargo.toml; rustflags override
    # those package-specific settings too, unlike the top-level release profile.
    require(environment.get('CARGO_ENCODED_RUSTFLAGS') is None and
            re.search(r'(?:^|\s)-C\s?codegen-units=1(?:\s|$)', environment.get('RUSTFLAGS') or ''),
            'set RUSTFLAGS="-C codegen-units=1"; unset CARGO_ENCODED_RUSTFLAGS')
    return sources


def read_corpus(directory, start=START, end=END):
    manifest = load(directory / 'corpus.json')
    require(manifest['start_height'] == start and manifest['end_height'] >= end,
            'corpus does not cover the requested window')
    previous = manifest['parent_hash']
    blocks = []
    for height in range(start + 1, end + 1):
        metadata = load(directory / f'{height}.json')
        checked = block_check((directory / f'{height}.hex').read_text().strip(), metadata['hash'])
        require(metadata['height'] == height and
                all(metadata[key] == value for key, value in checked.items()),
                f'corrupt block metadata: {height}')
        require(checked['previous_hash'] == previous, f'broken block chain: {height}')
        blocks.append(metadata)
        previous = metadata['hash']
    if end == manifest['end_height']:
        require(previous == manifest['tip_hash'], 'corpus tip differs from manifest')
    return manifest, blocks


def isolated_tip(node, height, block_hash):
    require(node.rpc('getpeerinfo') == [], 'measured node has peers')
    require(node.rpc('getrawmempool') == [], 'measured node has a nonempty mempool')
    info = node.rpc('getblockchaininfo')
    require(info['chain'] == 'main' and info['blocks'] == height and
            node.rpc('getbestblockhash') == block_hash,
            f'expected Mainnet tip {height} / {block_hash}; got {info}')
    return info


def replay_run(args, arm, snapshot, blocks, repetition):
    directory = args.output / f'r{repetition + 1}-{arm.label}'
    directory.mkdir()
    cache = directory / 'cache'
    reflink_copy(snapshot, cache)
    edits = [
        ('state', 'cache_dir', json.dumps(str(cache))),
        ('state', 'should_backup_non_finalized_state', 'false'),
        ('network', 'cache_dir', 'false'),
        ('consensus', 'checkpoint_sync', 'false'),
        ('tracing', 'filter', '"info"'),
    ]
    if arm.label == 'zakura-main':
        edits.append(('network', 'p2p_stack', '"legacy"'))
    node = MainnetNode(arm.path, directory / 'node', edits=edits, cpus=args.cpus)
    result = dict(arm=arm.label, repetition=repetition + 1, started_at=utc_now(),
                  binary=arm.provenance, snapshot=str(snapshot), status='failed')
    samples = []
    try:
        node.start()
        node.wait_rpc_ready(args.startup_timeout)
        node._client.conn.timeout = args.rpc_timeout
        if node._client.conn.sock:
            node._client.conn.sock.settimeout(args.rpc_timeout)
        result['initial'] = isolated_tip(node, START, blocks[0]['previous_hash'])
        result['preload'] = node.preload_receipt(time.perf_counter_ns())
        with (directory / 'blocks.csv').open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=['height', 'hash', 'submit_ms', 'accepted_ms'])
            writer.writeheader()
            cpu_before = node.sample()['cpu_s']
            started = time.perf_counter_ns()
            for metadata in blocks:
                raw = (args.corpus / f"{metadata['height']}.hex").read_text().strip()
                before = time.perf_counter_ns()
                outcome = node.rpc('submitblock', [raw])
                submitted = time.perf_counter_ns()
                require(outcome is None, f"submitblock rejected {metadata['height']}: {outcome}")
                require(node.rpc('getbestblockhash') == metadata['hash'],
                        f"accepted tip differs at {metadata['height']}")
                accepted = time.perf_counter_ns()
                sample = dict(height=metadata['height'], hash=metadata['hash'],
                              submit_ms=(submitted - before) / 1e6,
                              accepted_ms=(accepted - before) / 1e6)
                writer.writerow(sample)
                samples.append(sample)
            result['replay_wall_s'] = (time.perf_counter_ns() - started) / 1e9
            result['replay_cpu_s'] = node.sample()['cpu_s'] - cpu_before
        result['final'] = isolated_tip(node, blocks[-1]['height'], blocks[-1]['hash'])
        result['submit_total_s'] = sum(row['submit_ms'] for row in samples) / 1000
        result['submit_ms'] = summary([row['submit_ms'] for row in samples])
        result['accepted_ms'] = summary([row['accepted_ms'] for row in samples])
        # wait4 collects all node-thread CPU, including startup and clean shutdown.
        node.proc.send_signal(signal.SIGINT)
        code = node.wait_exit(300)
        require(code in (0, -signal.SIGINT), f'node shutdown failed: {code}')
        result['process_cpu_s'] = node.exit_resources['cpu_s']
        result['process_wall_s'] = (node.exit_ns - node.exec_ns) / 1e9
        result['status'] = 'ok'
    finally:
        node.stop(timeout=300)
        save_json(directory / 'run.json', result)
    # Only successful, stopped per-run clones are removed. Failure retains diagnostics/state.
    shutil.rmtree(cache)
    return result, samples


def aggregate(results, samples):
    """Per-block medians, but averages of complete run totals (announcement convention)."""
    report, medians = {}, {}
    for label in samples:
        runs = [row for row in results if row['arm'] == label]
        series = samples[label]
        require(len(runs) == len(series) and len(series) in (1, 3), 'missing replay repetitions')
        heights = [row['height'] for row in series[0]]
        require(all([(row['height'], row['hash']) for row in run] ==
                    [(row['height'], row['hash']) for row in series[0]] for run in series),
                'repetitions cover different heights')
        medians[label] = [dict(height=height, hash=series[0][index]['hash'],
                               **{key: statistics.median(run[index][key] for run in series)
                                  for key in ('submit_ms', 'accepted_ms')})
                          for index, height in enumerate(heights)]
        report[label] = dict(
            repetitions=len(runs),
            per_block_median_ms=summary([row['submit_ms'] for row in medians[label]]),
            per_block_median_accepted_ms=summary([row['accepted_ms'] for row in medians[label]]),
            **{key + '_mean': statistics.fmean(row[key] for row in runs)
               for key in ('submit_total_s', 'replay_wall_s', 'replay_cpu_s', 'process_cpu_s')})
    return report, medians


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--provenance', type=Path, required=True)
    parser.add_argument('--arm', action='append', default=[], metavar='LABEL=BINARY')
    parser.add_argument('--extra-revision', help='full source hash for an optional fifth Zebra arm named extra')
    parser.add_argument('--snapshot', action='append', default=[], metavar='LABEL=CACHE_ROOT')
    parser.add_argument('--corpus', type=Path, required=True, help='pruning fetch output/blocks')
    parser.add_argument('--output', type=Path, required=True, help='new directory; never overwritten')
    parser.add_argument('--cpus', type=cpu_list, default='0-7,16-23')
    parser.add_argument('--exclusive', action='store_true',
                        help='allow CPUs outside the shared-host partition; stop the dashboard first')
    parser.add_argument('--startup-timeout', type=int, default=7200)
    parser.add_argument('--rpc-timeout', type=int, default=120)
    parser.add_argument('--smoke', action='store_true', help='10 blocks, one run/arm; not a comparison')
    args = parser.parse_args(argv)
    arms = load_arms(args.provenance, args.arm)
    sources = validate_arms(arms, args.extra_revision)
    snapshots = {}
    for spec in args.snapshot:
        label, separator, path = spec.partition('=')
        require(separator and label in sources and label not in snapshots, 'invalid/duplicate snapshot: ' + spec)
        snapshots[label] = Path(path).expanduser().resolve(strict=True)
    require(snapshots.keys() == sources.keys(), 'provide one stopped snapshot per arm')
    cpus = check_cpus(args.cpus)
    require(args.exclusive or set(cpus) <= set(range(8)) | set(range(16, 24)),
            'CPUs outside the shared-host partition require --exclusive')
    os.sched_setaffinity(0, cpus)
    args.corpus = args.corpus.resolve(strict=True)
    args.output = args.output.resolve()
    for snapshot in snapshots.values():
        require(snapshot.is_dir() and snapshot != args.output and snapshot not in args.output.parents,
                'output must be outside every source snapshot')
    manifest, blocks = read_corpus(args.corpus, end=START + 10 if args.smoke else END)
    repetitions = 1 if args.smoke else 3
    order = [interleave(list(sources), repetition) for repetition in range(repetitions)]
    args.output.mkdir(parents=True, exist_ok=False)
    save_json(args.output / 'campaign.json', dict(
        started_at=utc_now(), cpus=cpus, exclusive=args.exclusive,
        host=os.uname().nodename, smoke=args.smoke,
        start_height=START, end_height=blocks[-1]['height'], corpus=manifest,
        order=order, arms={arm.label: arm.provenance for arm in arms},
        snapshots={key: str(value) for key, value in snapshots.items()},
        checkpoint_sync=False, tracing_filter='info', filesystem_cache='not dropped',
        timing='submitblock RPC response; accepted_ms additionally includes getbestblockhash',
        replay_cpu='node process utime+stime delta over replay loop, excluding startup/shutdown',
        process_cpu='wait4 node user+system CPU, including startup/shutdown'))
    results, samples = [], {label: [] for label in sources}
    by_label = {arm.label: arm for arm in arms}
    for repetition, labels in enumerate(order):
        for label in labels:
            result, rows = replay_run(args, by_label[label], snapshots[label], blocks, repetition)
            results.append(result)
            samples[label].append(rows)
    report, medians = aggregate(results, samples)
    save_json(args.output / 'per-block-medians.json', medians)
    save_json(args.output / 'summary.json', report)
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
