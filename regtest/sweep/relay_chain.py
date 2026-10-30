#!/usr/bin/env python3
"""Submit a sealed block on A; concurrently observe the best tips of B and C in A--B--C.

Every sample uses fresh no-miner Regtest nodes, the sealed funding chain, and
20-second settled links. B initiates both links so its outbound peer addresses
identify A and C unambiguously. No templates or proposals are requested.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import json
from pathlib import Path
import threading
import time
import traceback

from common import NODE_POLICY, Node, admit, exact_mempool, file_hash, h, load_fixtures, require, save
from tip_to_template import FIXTURES, ROOT, observe


def check_topology(nodes):
    peers = {name: node.rpc.call('getpeerinfo') for name, node in nodes.items()}
    require(len(peers['A']) == len(peers['C']) == 1 and len(peers['B']) == 2,
            'isolated A--B--C topology: peer counts must be 1, 2, 1')
    require(all(peer['inbound'] for name in ('A', 'C') for peer in peers[name]),
            'A and C only accept the links initiated by B')
    require(all(not peer['inbound'] for peer in peers['B']) and
            {peer['addr'] for peer in peers['B']} == {nodes['A'].p2p_addr, nodes['C'].p2p_addr},
            'B connects only to A and C; no direct A--C link')
    return peers


def measure(nodes, fixture, args, result):
    started, stopped = threading.Event(), threading.Event()
    observations = result['observations'] = {name: {} for name in ('B', 'C')}
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures, ready_events = {}, []
        for name in ('B', 'C'):
            ready = threading.Event()
            ready_events.append(ready)
            rpc = h.RPC(nodes[name].rpc_addr, timeout=args.timeout)
            futures[name] = executor.submit(observe, rpc, 'getbestblockhash', fixture, {},
                                            started, ready, stopped, observations[name], args)
        try:
            require(all(event.wait(args.timeout) for event in ready_events), 'B and C observers ready')
            result['start_unix_ns'] = time.time_ns()
            result['start_ns'] = time.perf_counter_ns()
            started.set()
            reply = nodes['A'].rpc.call('submitblock', [fixture['raw']])
            result['sender_submit_end_ns'] = time.perf_counter_ns()
            require(reply is None, f'A accepted the sealed candidate: {reply!r}')
            for name, future in futures.items():
                require(future.result(timeout=args.timeout) == fixture['hash'], 'candidate observed')
                observed = observations[name]['getbestblockhash']['first_new']['end_ns']
                result[f'{name.lower()}_best_tip_observed_ns'] = observed
                result[f'{name.lower()}_best_tip_latency_ns'] = observed - result['start_ns']
            result['end_ns'] = result['c_best_tip_observed_ns']
            result['latency_ns'] = result['end_ns'] - result['start_ns']
            result['sender_submit_latency_ns'] = result['sender_submit_end_ns'] - result['start_ns']
            # Signed RPC-observation difference, not pure verification CPU or packet arrival time.
            result['b_to_c_observed_ns'] = result['c_best_tip_observed_ns'] - result['b_best_tip_observed_ns']
            for node in nodes.values():
                require(node.rpc.call('getbestblockhash') == fixture['hash'], 'candidate is canonical')
                require(node.rpc.call('getblock', [fixture['hash'], 0]).lower() == fixture['raw'].lower(),
                        'exact sealed block committed')
            result['preload'] = {
                name: node.preload_receipt(result['start_ns']) for name, node in nodes.items()
            }
        finally:
            stopped.set()
            started.set()


def case(binary, arm, fixture, chain, args, directory, result):
    with ExitStack() as cleanup:
        nodes = {}
        for name, limit in (('A', 1), ('B', 2), ('C', 1)):
            node = Node(binary, directory / name, arm, args.timeout, miner=None)
            # All peers share loopback: occupied per-IP slots also prevent a discovered A--C shortcut.
            config = node.config.read_text()
            require(config.count('max_connections_per_ip = 16') == 1, 'known loopback node policy')
            node.config.write_text(config.replace('max_connections_per_ip = 16',
                                                   f'max_connections_per_ip = {limit}', 1))
            nodes[name] = cleanup.enter_context(node)
        for node in nodes.values():
            h.replay(node, chain)
            require(node.rpc.call('getrawmempool') == [], 'fresh replay mempool is empty')
        resident = fixture['corpus'] if args.temperature == 'warm' else []
        for name in ('B', 'C'):
            admit(nodes[name], resident)
            exact_mempool(nodes[name], resident, fixture['parent'], args.timeout, args.poll_ms / 1000)
        for name in ('A', 'C'):
            nodes['B'].rpc.call('addnode', [nodes[name].p2p_addr, 'add'])
        for name, count in (('A', 1), ('B', 2), ('C', 1)):
            node = nodes[name]
            node.wait(lambda node=node, count=count: len(node.rpc.call('getpeerinfo')) == count,
                      'chain links', args.timeout)
        result['peers_before_settle'] = check_topology(nodes)
        linked_ns = time.perf_counter_ns()
        time.sleep(args.settle_seconds)
        result['peers_before_submit'] = check_topology(nodes)
        expected = {tx['txid'] for tx in resident}
        pools = {name: node.rpc.call('getrawmempool') for name, node in nodes.items()}
        for name in ('B', 'C'):
            require(set(pools[name]) == expected, f'{name} exact {args.temperature} mempool after settling')
        require(set(pools['A']) <= expected, 'A only learns requested resident transactions by gossip')
        for node in nodes.values():
            require(node.rpc.call('getbestblockhash') == fixture['parent'], 'common sealed parent')
        result.update(mempool_txids=pools, link_established_ns=linked_ns,
                      preparation_end_ns=time.perf_counter_ns())
        measure(nodes, fixture, args, result)
        result['link_age_at_submit_ns'] = result['start_ns'] - linked_ns
        result['peers_after_measurement'] = check_topology(nodes)


def main():
    parser = argparse.ArgumentParser(description=__doc__, epilog=h.BENCH_HELP)
    parser.add_argument('binary', type=Path)
    parser.add_argument('arm')
    parser.add_argument('fixture')
    parser.add_argument('output', type=Path)
    parser.add_argument('--fixtures', type=Path, default=FIXTURES)
    parser.add_argument('--temperature', choices=('cold', 'warm'), required=True)
    parser.add_argument('--settle-seconds', type=float, default=20)
    parser.add_argument('--poll-ms', type=float, default=1)
    parser.add_argument('--timeout', type=float, default=600)
    args = parser.parse_args()
    require(args.timeout > 0 and args.settle_seconds >= 20 and 0.1 <= args.poll_ms <= 100,
            'positive timeout, at least 20 seconds settling, bounded polling')
    binary = args.binary.expanduser().resolve(strict=True)
    manifest, chain, fixtures = load_fixtures(args.fixtures)
    fixture = next((item for item in fixtures if item['id'] == args.fixture), None)
    require(fixture is not None, 'fixture must exist in sealed manifest')
    args.output.mkdir(parents=True)
    paths = [Path(__file__), ROOT / 'relay-chain.sh', Path(h.__file__), Path(h.node_process.__file__),
             Path(__file__).with_name('common.py'), Path(__file__).with_name('tip_to_template.py')]
    result = dict(schema=1, endpoint='submit-to-chain-tip', arm=args.arm,
                  binary=str(binary), binary_sha256=file_hash(binary), fixture=fixture['id'],
                  filling=fixture['filling'], target_bytes=fixture['target_bytes'],
                  block_bytes=fixture['bytes'], transaction_count=fixture['transaction_count'],
                  candidate_hash=fixture['hash'], candidate_sha256=fixture['sha256'], parent=fixture['parent'],
                  fixture_directory=str(args.fixtures.resolve()),
                  fixture_manifest_sha256=file_hash(args.fixtures / 'manifest.json'),
                  setup_chain_sha256=manifest['files']['chain.json'], node_policy=NODE_POLICY,
                  temperature=args.temperature, settle_seconds=args.settle_seconds,
                  poll_interval_ms=args.poll_ms, transport='legacy-tcp-loopback',
                  topology='A--B--C; B initiates both connections', miner_address=None,
                  sources={str(path.relative_to(ROOT)): file_hash(path) for path in paths})
    save(args.output / 'protocol.json', result)
    try:
        case(binary, args.arm, fixture, chain, args, args.output, result)
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
