#!/usr/bin/env python3
"""Sender submitblock -> first receiver template on that block, with surviving mempool work.

Every sample uses two fresh Regtest nodes and the sealed sweep funding chain.
The receiver admits two unused corpus transactions per candidate filling (two
transparent transactions for the empty control), plus the candidate only in warm
mode. Preparation includes an exact old-tip template, never an explicit proposal.
The sender has no miner configured and learns the block only through submitblock.
Timing and environment options are described by --help.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import io
import json
import os
from pathlib import Path
import select
import signal
import subprocess
import threading
import time
import traceback

from common import (LIMIT, NODE_POLICY, Node, admit, exact_mempool, exact_template,
                    file_hash, h, load_fixtures, require, save)
from ablation import ORCHARD_MINER

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = h.FIXTURES
MINERS = dict(transparent=h.P2SH_MINER, orchard=ORCHARD_MINER, sapling=h.SAPLING_MINER)
MINER_SOURCES = dict(
    transparent='deterministic P2SH(OP_1), node_bench.p2sh_address',
    orchard='fixed deterministic Zebra Regtest Orchard-only address vector; sweep/ablation.py',
    sapling='fixed deterministic Zebra Regtest Sapling address vector; node_bench.SAPLING_MINER')
PERF_LOG_FILTER = (
    'info,zebrad::components::mempool=trace,'
    'zebra_rpc::methods::types::get_block_template=debug,'
    'zebrad::components::sync=debug,zebrad::components::inbound=debug,'
    'zebra_state::service::chain_tip=trace,zebra_state::service::write=trace'
)


class ReceiverPerf:
    """Attach all receiver threads, enabling only for the submission-to-full window."""

    def __init__(self, pid, directory, result):
        self.directory, self.result = directory, result
        self.process = None
        self.enabled = False
        self.control, self.ack = os.pipe(), os.pipe()
        self.log = (directory / 'receiver-perf.log').open('w')
        try:
            command = ['perf', 'record', '-q', '-e', 'cycles:u', '-F', '999',
                       '--call-graph', 'dwarf,16384', '--user-callchains', '--clockid', 'mono',
                       '-p', str(pid), '-o', str(directory / 'receiver-perf.data'),
                       '--delay', '-1', '--control', f'fd:{self.control[0]},{self.ack[1]}']
            self.process = subprocess.Popen(command, pass_fds=(self.control[0], self.ack[1]),
                                            stdout=subprocess.DEVNULL, stderr=self.log)
            result['perf'] = dict(command=command, pid=pid, clock='CLOCK_MONOTONIC')
        except BaseException:
            self.close()
            raise

    def command(self, command):
        os.write(self.control[1], command + b'\n')
        ready, _, _ = select.select([self.ack[0]], [], [], 30)
        require(ready, f'perf did not acknowledge {command!r}; see receiver-perf.log')
        require(os.read(self.ack[0], 16).rstrip(b'\0') == b'ack\n', 'perf control acknowledgement')

    def enable(self):
        self.command(b'enable')
        self.enabled = True
        self.result['perf']['enabled_ns'] = time.perf_counter_ns()

    def disable(self):
        if self.enabled:
            self.command(b'disable')
            self.enabled = False
            self.result['perf']['disabled_ns'] = time.perf_counter_ns()

    def close(self):
        try:
            if self.process is not None:
                if self.process.poll() is None:
                    self.process.send_signal(signal.SIGINT)
                try:
                    code = self.process.wait(timeout=60)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=10)
                    raise
                require(code in (0, -signal.SIGINT, 130), 'perf recording succeeded')
        finally:
            for fd in (*self.control, *self.ack):
                os.close(fd)
            self.log.close()

    def report(self):
        for name, options in (
            ('self', ['--no-children', '--sort', 'symbol']),
            ('children', ['--children', '--sort', 'symbol']),
            ('threads', ['--no-children', '--sort', 'pid,comm,symbol']),
        ):
            with (self.directory / f'receiver-perf-{name}.txt').open('w') as output:
                subprocess.run(['perf', 'report', '--stdio', '-n', '--percent-limit', '0.1',
                                '-i', str(self.directory / 'receiver-perf.data'), *options],
                               stdout=output, stderr=subprocess.STDOUT, check=True, timeout=120)


def check_coinbase(template, miner):
    """Read V5 coinbase bundle counts outside timing; Zebra has no decoderawtransaction RPC."""
    data = io.BytesIO(bytes.fromhex(template['coinbasetxn']['data']))

    def take(count):
        value = data.read(count)
        require(len(value) == count, 'complete V5 coinbase encoding')
        return value

    def compact():
        value = take(1)[0]
        return value if value < 253 else int.from_bytes(take({253: 2, 254: 4, 255: 8}[value]), 'little')

    require(take(4) == bytes.fromhex('05000080'), 'NU5 benchmark requires a V5 coinbase')
    take(16)  # Version group, branch ID, lock time, expiry height.
    require(compact() == 1, 'one coinbase input')
    require(take(36) == bytes(32) + b'\xff' * 4, 'coinbase null outpoint')
    take(compact())
    take(4)  # Input sequence.
    transparent = compact()
    for _ in range(transparent):
        take(8)
        take(compact())
    require(compact() == 0, 'coinbase cannot have Sapling spends')
    sapling = compact()
    if sapling:
        # V5 output prefixes (756), value balance (8), Groth16 proofs (192), binding sig (64).
        take(sapling * (756 + 192) + 8 + 64)
    pools = dict(transparent=transparent, sapling=sapling, orchard=compact())
    require(pools[miner] > 0 and all(not count for pool, count in pools.items() if pool != miner),
            f'coinbase must pay only the requested {miner} pool (funding streams disabled)')
    require(sum(tx['fee'] for tx in template['transactions']) > 0, 'survivors pay coinbase fees')
    return pools


def select_extras(fixture, corpus):
    """Use independently funded leftovers from the same sealed corpus, not another chain."""
    excluded = set(fixture['txids'])
    kinds = sorted({tx['kind'] for tx in fixture['corpus']} or {'transparent'})
    extras = []
    for kind in kinds:
        chosen = [tx for tx in corpus if tx['kind'] == kind and tx['txid'] not in excluded][:2]
        require(len(chosen) == 2, f'two unused {kind} corpus transactions required')
        extras.extend(chosen)
    require(len({tx['txid'] for tx in extras}) == len(extras), 'unique extra transactions')
    require(fixture['bytes'] + sum(tx['bytes'] for tx in extras) + 4 <= LIMIT,
            'candidate plus extras must fit an old-tip template')
    return extras


def check_template(template, fixture, extras):
    require(template['previousblockhash'] == fixture['hash'], 'template uses the new tip')
    observed = {tx['hash']: tx['data'].lower() for tx in template['transactions']}
    expected = {tx['txid']: tx['raw'].lower() for tx in extras}
    require(not set(observed).intersection(fixture['txids']),
            'new-tip templates must exclude every mined transaction')
    require(len(template['transactions']) == len(expected) and observed == expected,
            'full new-tip template must contain exactly the surviving extra transactions')


def is_full(template, fixture, extras):
    """Zebra publishes coinbase-only work on a new tip before the mempool template: wait for the
    template carrying the surviving extras; a mined transaction in any new-tip template fails."""
    observed = {tx['hash'] for tx in template['transactions']}
    require(not observed.intersection(fixture['txids']),
            'new-tip templates must exclude every mined transaction')
    return observed == {tx['txid'] for tx in extras}


def observe(rpc, method, fixture, initial, started, ready, stopped, stamps, args, full=None,
            on_full=None):
    """Each observer owns its HTTP connection; neither waits for submitblock to return. With `full`,
    new-tip responses are recorded until one satisfies it."""
    longpoll = method == 'getblocktemplate' and bool(initial.get('longpollid'))
    longpollid = initial.get('longpollid')
    observation = dict(mode='longpoll' if longpoll else 'poll',
                       poll_interval_ms=None if longpoll else args.poll_ms,
                       calls=0, last_old=None, first_new=None, full_new=None,
                       first_new_transactions=None)
    ready.set()
    require(started.wait(args.timeout), 'submission timer started')
    deadline = time.monotonic() + args.timeout
    try:
        while not stopped.is_set() and time.monotonic() < deadline:
            remaining = max(0.001, deadline - time.monotonic())
            rpc.conn.timeout = remaining
            if rpc.conn.sock is not None:
                rpc.conn.sock.settimeout(remaining)
            params = [{'longpollid': longpollid}] if longpoll else None
            query_start = time.perf_counter_ns()
            value = rpc.call(method, params)
            query_end = time.perf_counter_ns()
            tip = value['previousblockhash'] if method == 'getblocktemplate' else value
            observation['calls'] += 1
            query = dict(start_ns=query_start, end_ns=query_end)
            require(tip in (fixture['parent'], fixture['hash']), 'no unrelated receiver tip')
            if tip == fixture['hash']:
                if observation['first_new'] is None:
                    observation['first_new'] = query
                    if method == 'getblocktemplate':
                        observation['first_new_transactions'] = len(value['transactions'])
                if full is None or full(value):
                    observation['full_new'] = query
                    if on_full is not None:
                        on_full()
                    return value
            else:
                observation['last_old'] = query
            # Next query: an old-tip or a not-yet-full new-tip response.
            if longpoll:
                longpollid = value.get('longpollid')
                require(bool(longpollid), 'longpoll support must persist across template responses')
            else:
                stopped.wait(args.poll_ms / 1000)
        raise TimeoutError(f'no new-tip {method} before deadline or submission failure')
    finally:
        stamps[method] = observation
        rpc.conn.close()


def measure(sender, receiver, fixture, extras, initial, args, directory, result):
    started, stopped = threading.Event(), threading.Event()
    stamps = result['observations'] = {}
    profiler = None
    with ExitStack() as cleanup, ThreadPoolExecutor(max_workers=2) as executor:
        futures, ready_events = {}, []
        for method in ('getbestblockhash', 'getblocktemplate'):
            ready = threading.Event()
            ready_events.append(ready)
            rpc = h.RPC(receiver.rpc_addr, timeout=args.timeout)
            full = (lambda t: is_full(t, fixture, extras)) if method == 'getblocktemplate' else None
            futures[method] = executor.submit(observe, rpc, method, fixture, initial,
                                              started, ready, stopped, stamps, args, full,
                                              (lambda: profiler.disable()) if full and getattr(args, 'perf', False) else None)
        try:
            require(all(event.wait(args.timeout) for event in ready_events), 'observers ready')
            if getattr(args, 'perf', False):
                profiler = ReceiverPerf(receiver.proc.pid, directory, result)
                cleanup.callback(profiler.close)
                cleanup.callback(profiler.disable)
                profiler.enable()
            result['start_unix_ns'] = time.time_ns()
            result['start_ns'] = time.perf_counter_ns()
            started.set()
            reply = sender.rpc.call('submitblock', [fixture['raw']])
            result['sender_submit_end_ns'] = time.perf_counter_ns()
            require(reply is None, f'sender accepted candidate: {reply!r}')
            # The first full new-tip template; the first new-tip one (often coinbase-only) is timed too.
            template = futures['getblocktemplate'].result(timeout=args.timeout)
            save(directory / 'full-template.json', template)
            gbt = stamps['getblocktemplate']
            result['end_ns'] = gbt['full_new']['end_ns']
            result['latency_ns'] = result['end_ns'] - result['start_ns']
            result['template_ready_ns'] = result['end_ns']
            result['first_template_latency_ns'] = gbt['first_new']['end_ns'] - result['start_ns']
            result['first_template_transactions'] = gbt['first_new_transactions']
            check_template(template, fixture, extras)
            result['membership_checked_ns'] = time.perf_counter_ns()
            futures['getbestblockhash'].result(timeout=args.timeout)
            result['receiver_best_tip_observed_ns'] = stamps['getbestblockhash']['first_new']['end_ns']
            for node in (sender, receiver):
                require(node.rpc.call('getbestblockhash') == fixture['hash'], 'candidate is canonical')
                require(node.rpc.call('getblock', [fixture['hash'], 0]).lower() == fixture['raw'].lower(),
                        'exact sealed block committed')
            result['sender_submit_latency_ns'] = result['sender_submit_end_ns'] - result['start_ns']
            result['receiver_best_tip_latency_ns'] = result['receiver_best_tip_observed_ns'] - result['start_ns']
            if hasattr(args, 'miner'):
                result['full_coinbase_outputs'] = check_coinbase(template, args.miner)
            result['preload'] = {
                name: node.preload_receipt(result['start_ns'])
                for name, node in (('sender', sender), ('receiver', receiver))
            }
        finally:
            stopped.set()
            started.set()
    if profiler:
        profiler.report()


def case(binary, arm, fixture, chain, extras, args, directory, result):
    miner = getattr(args, 'miner', 'transparent')
    receiver_node = Node(binary, directory / 'receiver', arm, args.timeout, miner=MINERS[miner])
    if getattr(args, 'perf', False):
        with receiver_node.config.open('a') as config:
            config.write(f'\n[tracing]\nfilter = "{PERF_LOG_FILTER}"\n')
        result['receiver_log_filter'] = PERF_LOG_FILTER
    with Node(binary, directory / 'sender', arm, args.timeout, miner=None) as sender, receiver_node as receiver:
        h.replay(sender, chain)
        receiver.replay(chain)
        resident = extras + (fixture['corpus'] if args.temperature == 'warm' else [])
        admit(receiver, resident)
        exact_mempool(receiver, resident, fixture['parent'], args.timeout, args.poll_ms / 1000)
        exact_template(receiver, resident, fixture['parent'], args.timeout, args.poll_ms / 1000)
        sender.rpc.call('addnode', [receiver.p2p_addr, 'add'])
        for node in (sender, receiver):
            node.wait(lambda node=node: node.rpc.call('getpeerinfo'), 'legacy peer link', args.timeout)
        linked_ns = time.perf_counter_ns()
        time.sleep(args.settle_seconds)
        for node in (sender, receiver):
            require(len(node.rpc.call('getpeerinfo')) == 1, 'isolated two-node legacy topology')
            require(node.rpc.call('getbestblockhash') == fixture['parent'], 'common sealed parent')
        # Link startup may gossip transactions. Confirm the receiver temperature after settling.
        require(set(receiver.rpc.call('getrawmempool')) == {tx['txid'] for tx in resident},
                'receiver has exactly the requested warm/cold corpus plus extras at submission')
        initial, _ = exact_template(receiver, resident, fixture['parent'], args.timeout,
                                    args.poll_ms / 1000)
        save(directory / 'initial-template.json', initial)
        if hasattr(args, 'miner'):
            result['initial_coinbase_outputs'] = check_coinbase(initial, miner)
        sender_pool = sender.rpc.call('getrawmempool')
        require(set(sender_pool) <= {tx['txid'] for tx in resident}, 'no unrelated sender transaction')
        result.update(sender_mempool_txids=sender_pool, receiver_mempool_txids=[tx['txid'] for tx in resident],
                      link_established_ns=linked_ns,
                      preparation_end_ns=time.perf_counter_ns())
        measure(sender, receiver, fixture, extras, initial, args, directory, result)
        result['link_age_at_submit_ns'] = result['start_ns'] - linked_ns


def main():
    parser = argparse.ArgumentParser(description=__doc__, epilog=h.BENCH_HELP)
    parser.add_argument('binary', type=Path)
    parser.add_argument('arm')
    parser.add_argument('fixture')
    parser.add_argument('output', type=Path)
    parser.add_argument('--fixtures', type=Path, default=FIXTURES)
    parser.add_argument('--temperature', choices=('cold', 'warm'), required=True)
    parser.add_argument('--miner', choices=tuple(MINERS), default='transparent',
                        help='receiver payout pool, using deterministic Regtest benchmark addresses')
    parser.add_argument('--settle-seconds', type=float, default=20)
    parser.add_argument('--poll-ms', type=float, default=1)
    parser.add_argument('--timeout', type=float, default=600)
    parser.add_argument('--perf', action='store_true',
                        help='profile all receiver threads with user-space DWARF stacks; enable phase logs')
    args = parser.parse_args()
    require(args.timeout > 0 and args.settle_seconds >= 0 and 0.1 <= args.poll_ms <= 100,
            'positive timeout, nonnegative settling and bounded polling')
    binary = args.binary.expanduser().resolve(strict=True)
    manifest, chain, fixtures = load_fixtures(args.fixtures)
    fixture = next((item for item in fixtures if item['id'] == args.fixture), None)
    require(fixture is not None, 'fixture must exist in sealed manifest')
    corpus = json.loads((args.fixtures / 'corpus.json').read_text())
    extras = select_extras(fixture, corpus)
    args.output.mkdir(parents=True)  # Never overwrite an earlier sample.
    paths = [Path(__file__), ROOT / 'tip-to-template.sh', ROOT / 'tip-to-template-miner.sh',
             Path(h.__file__), Path(h.node_process.__file__), Path(__file__).with_name('common.py'),
             Path(__file__).with_name('ablation.py')]
    result = dict(schema=1, endpoint='submit-to-peer-template', arm=args.arm,
                  binary=str(binary), binary_sha256=file_hash(binary), fixture=fixture['id'],
                  filling=fixture['filling'], target_bytes=fixture['target_bytes'],
                  block_bytes=fixture['bytes'], transaction_count=fixture['transaction_count'],
                  candidate_hash=fixture['hash'], candidate_sha256=fixture['sha256'],
                  parent=fixture['parent'], fixture_directory=str(args.fixtures.resolve()),
                  fixture_manifest_sha256=file_hash(args.fixtures / 'manifest.json'),
                  setup_chain_sha256=manifest['files']['chain.json'], node_policy=NODE_POLICY,
                  temperature=args.temperature, settle_seconds=args.settle_seconds,
                  poll_interval_ms=args.poll_ms, transport='legacy-tcp-loopback',
                  perf_enabled=args.perf,
                  miner=args.miner, miner_address=MINERS[args.miner],
                  miner_address_source=MINER_SOURCES[args.miner],
                  sources={str(path.relative_to(ROOT)): file_hash(path) for path in paths},
                  extras=[{key: tx[key] for key in ('txid', 'sha256', 'kind', 'bytes')} for tx in extras])
    save(args.output / 'protocol.json', result)
    try:
        case(binary, args.arm, fixture, chain, extras, args, args.output, result)
        result['status'] = 'ok'
    except BaseException:
        result.update(status='error', traceback=traceback.format_exc())
        raise
    finally:
        save(args.output / 'sample.json', result)
        with (args.output / 'samples.jsonl').open('x') as output:
            output.write(json.dumps(result, allow_nan=False) + '\n')
        print(json.dumps(result, allow_nan=False), flush=True)


if __name__ == '__main__':
    main()
