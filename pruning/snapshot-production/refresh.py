#!/usr/bin/env python3
"""Later snapshot-production workflow, not the historical pruning measurement.

Sync an explicitly selected v30 cache with 10,000-block retention, stop cleanly,
compact, archive, and restore-check the stored finalized tip and retained floor.
"""
import argparse
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import urllib.request

SCRIPT = Path(__file__).resolve().parent
sys.dont_write_bytecode = True
sys.path.insert(0, str(SCRIPT.parents[1] / 'regtest'))
RETENTION = 10_000


def rpc(port, method, params=()):
    body = json.dumps(dict(jsonrpc='2.0', id=1, method=method, params=list(params))).encode()
    request = urllib.request.Request(f'http://127.0.0.1:{port}', body,
                                     {'content-type': 'application/json'})
    with urllib.request.urlopen(request, timeout=30) as response:
        reply = json.load(response)
    if reply.get('error'):
        raise RuntimeError(reply['error'])
    return reply['result']


def finalized_height(port):
    with urllib.request.urlopen(f'http://127.0.0.1:{port}/metrics', timeout=30) as response:
        for line in response.read().decode().splitlines():
            if line.startswith('state_finalized_block_height '):
                return int(float(line.split()[1]))
    return None


def stored(compactor, db, allowed_root):
    """Read finalized tip/floor from a stopped database using the later compactor."""
    result = subprocess.run([str(compactor), '--allowed-root', str(allowed_root), '--info', str(db)],
                            capture_output=True, text=True, check=True, timeout=600)
    return json.loads(result.stdout)


def files(db):
    return {str(p.relative_to(db)): p.stat().st_size for p in db.rglob('*') if p.is_file()}


def log(*args):
    print(datetime.datetime.now(datetime.UTC).strftime('%H:%M:%S'), *args, flush=True)


def stop(node):
    if node.poll() is None:
        node.send_signal(signal.SIGINT)
        try:
            node.wait(timeout=600)
        except subprocess.TimeoutExpired:
            node.kill()
            node.wait(timeout=30)
            raise
    if node.returncode != 0:
        raise RuntimeError(f'zebrad did not stop cleanly: {node.returncode}; not compacting')


def refresh(args):
    from front_common import clean_env, generate_config, set_key

    root, output = args.cache_dir, args.output
    db_relative = Path('state/v30') / args.network
    db = root / db_relative
    config = output / 'zebrad.toml'
    text = generate_config(args.binary, config)
    for section, key, value in [
        ('network', 'network', json.dumps(args.network.capitalize())),
        ('network', 'listen_addr', json.dumps(f'0.0.0.0:{args.p2p_port}')),
        ('state', 'cache_dir', json.dumps(str(root))),
        ('state', 'prune_blocks_older_than', str(RETENTION)),
        ('rpc', 'listen_addr', json.dumps(f'127.0.0.1:{args.rpc_port}')),
        ('rpc', 'enable_cookie_auth', 'false'),
        ('metrics', 'endpoint_addr', json.dumps(f'127.0.0.1:{args.metrics_port}')),
        ('tracing', 'filter', '"info"'),
    ]:
        text = set_key(text, section, key, value)
    config.write_text(text)
    command = [str(args.binary), '-c', str(config), 'start']
    if args.cpus:
        command = ['taskset', '-c', args.cpus, *command]
    with (output / 'zebrad.log').open('w') as node_log:
        node = subprocess.Popen(command, stdout=node_log, stderr=subprocess.STDOUT, env=clean_env())
        try:
            deadline, settled = time.monotonic() + args.timeout, 0
            while time.monotonic() < deadline:
                time.sleep(args.poll_seconds)
                if node.poll() is not None:
                    raise RuntimeError(f'zebrad exited early: {node.returncode}; see {output / "zebrad.log"}')
                try:
                    info = rpc(args.rpc_port, 'getblockchaininfo')
                    finalized = finalized_height(args.metrics_port)
                except (OSError, ValueError, RuntimeError) as error:
                    log('not ready:', error)
                    continue
                blocks, estimated, floor = info['blocks'], info.get('estimatedheight', 0), info.get('pruneheight')
                log(f'best {blocks} estimated {estimated} finalized {finalized} pruneheight {floor}')
                synced = estimated and blocks >= estimated - 2
                pruned = finalized and floor and floor >= max(1, finalized + 1 - RETENTION)
                settled = settled + 1 if synced and pruned else 0
                if settled >= 3:
                    break
            else:
                raise RuntimeError('timed out before reaching the tip with pruning caught up')
        finally:
            stop(node)
    before = stored(args.compactor, db, root)
    log('zebrad stopped:', before)
    if before['retained_floor'] < max(1, before['finalized_height'] + 1 - RETENTION):
        raise RuntimeError('pruning did not reach the retention floor')
    subprocess.run([str(args.compactor), '--allowed-root', str(root), str(db)],
                   check=True, timeout=args.timeout)
    if stored(args.compactor, db, root) != before:
        raise RuntimeError('compaction changed the stored tip or floor')

    date = datetime.datetime.now(datetime.UTC)
    name = f'zebrad-state-{args.network}-v30-pruned-{date:%Y%m%dT%H%M%SZ}.tar.zst'
    archive = output / name
    if archive.exists():
        raise RuntimeError(f'refusing to overwrite {archive}')
    subprocess.run(['tar', '-I', f'zstd -q -T{args.compression_threads} -10', '-C', str(root),
                    '-cf', str(archive), str(db_relative)], check=True, timeout=args.timeout)
    with tempfile.TemporaryDirectory(prefix='restore-', dir=output) as scratch:
        verify = Path(scratch)
        subprocess.run(['tar', '-I', f'zstd -q -T{args.compression_threads}', '-C', str(verify),
                        '-xf', str(archive)], check=True, timeout=args.timeout)
        restored = verify / db_relative
        if files(restored) != files(db) or stored(args.compactor, restored, verify) != before:
            raise RuntimeError('restored archive differs from the database')
    with archive.open('rb') as stream:
        sha256 = hashlib.file_digest(stream, 'sha256').hexdigest()
    with args.binary.open('rb') as stream:
        binary_hash = hashlib.file_digest(stream, 'sha256').hexdigest()
    (output / f'{name}.sha256').write_text(f'{sha256}  {name}\n')
    manifest = {
        'network': args.network,
        'chain_tip': {'height': before['finalized_height'], 'block_hash': before['finalized_hash'],
                      'note': 'Stored finalized tip; the node syncs remaining blocks on start.'},
        'pruning': {'prune_blocks_older_than': RETENTION, 'retained_floor': before['retained_floor'],
                    'note': 'Bodies below the floor are deleted; genesis and validation state remain.'},
        'database_format_version': (db / 'version').read_text().strip(),
        'database_format_major_version': 30,
        'producing_zebrad': {'commit': args.commit, 'sha256': binary_hash},
        'archive': {'filename': name, 'size_bytes': archive.stat().st_size, 'sha256': sha256,
                    'compression': 'zstd', 'extraction_path': str(db_relative)},
        'compatibility': {'statement': 'Requires database format 30 support and retention of at least 10000 blocks.'},
        'produced_at': date.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'signing': {'status': 'unsigned'},
    }
    (output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    log('done:', archive, archive.stat().st_size, sha256, 'restore verified')


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--binary', type=Path, required=True, help='pruning-enabled zebrad with format 30 support')
    p.add_argument('--commit', required=True, help='full source commit of the supplied zebrad')
    p.add_argument('--cache-dir', type=Path, required=True, help='dedicated local Zebra cache to update')
    p.add_argument('--output', type=Path, required=True, help='logs, archive, checksum and manifest directory')
    p.add_argument('--compactor', type=Path, default=SCRIPT / 'compact/target/release/prune-compact')
    p.add_argument('--network', choices=['mainnet', 'testnet'], default='mainnet')
    p.add_argument('--cpus', help='taskset CPU list for node; default: inherit process affinity')
    p.add_argument('--rpc-port', type=int, help='loopback RPC port (mainnet 18999, testnet 18998)')
    p.add_argument('--metrics-port', type=int, help='loopback metrics port (mainnet 19999, testnet 19998)')
    p.add_argument('--p2p-port', type=int, help='P2P port (mainnet 28233, testnet 28234)')
    p.add_argument('--compression-threads', type=int, default=len(os.sched_getaffinity(0)))
    p.add_argument('--timeout', type=int, default=43200, help='sync/compaction/archive timeout in seconds')
    p.add_argument('--poll-seconds', type=float, default=30)
    return p


def main(argv=None):
    p = parser()
    args = p.parse_args(argv)
    if len(args.commit) != 40 or any(c not in '0123456789abcdefABCDEF' for c in args.commit):
        p.error('--commit must be a full 40-digit hexadecimal source commit')
    if min(args.timeout, args.poll_seconds, args.compression_threads) <= 0:
        p.error('timeouts, poll interval and compression threads must be positive')
    defaults = (18999, 19999, 28233) if args.network == 'mainnet' else (18998, 19998, 28234)
    for key, default in zip(('rpc_port', 'metrics_port', 'p2p_port'), defaults):
        value = getattr(args, key)
        setattr(args, key, default if value is None else value)
    ports = (args.rpc_port, args.metrics_port, args.p2p_port)
    if any(not 1 <= port <= 65535 for port in ports) or len(set(ports)) != 3:
        p.error('ports must be distinct and between 1 and 65535')
    args.binary = args.binary.resolve(strict=True)
    args.compactor = args.compactor.resolve(strict=True)
    args.cache_dir = args.cache_dir.resolve()
    args.output = args.output.resolve()
    if args.output.is_relative_to(args.cache_dir / 'state'):
        p.error('--output must be outside the database state directory')
    args.cache_dir.mkdir(parents=True, exist_ok=True)
    args.output.mkdir(parents=True, exist_ok=True)
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f'signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    with (args.cache_dir / '.refresh.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        refresh(args)


if __name__ == '__main__':
    main()
