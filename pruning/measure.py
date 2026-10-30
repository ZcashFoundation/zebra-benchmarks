#!/usr/bin/env python3
"""Resumable public Mainnet snapshot download, pruning and paired replay phases."""
import argparse
import base64
import contextlib
from datetime import datetime, timezone
import fcntl
import hashlib
import http.client
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import signal
import subprocess
import sys
import tarfile
import time
import traceback
from urllib.parse import urlsplit

SCRIPT = Path(__file__).resolve().parent
ROOT = SCRIPT
BENCH = SCRIPT.parent / 'regtest'
sys.path.insert(0, str(BENCH))
sys.dont_write_bytecode = True
RETENTION = 10_000


def utc():
    return datetime.now(timezone.utc).isoformat()


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + '.tmp')
    with temp.open('w') as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())
    temp.replace(path)


def load(path):
    return json.loads(path.read_text())


def run(command, **kwargs):
    return subprocess.run(list(map(str, command)), check=True, **kwargs)


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def free_space(minimum):
    free = shutil.disk_usage(ROOT).free
    require(free >= minimum, f'need {minimum} free bytes, have {free}')
    return free


def db_path(cache):
    paths = [p.parent for p in cache.glob('state/v*/mainnet/CURRENT')]
    require(len(paths) == 1, f'expected exactly one Mainnet database in {cache}: {paths}')
    return paths[0]


def size(cache):
    db = db_path(cache)
    apparent = int(run(['du', '-sb', db], capture_output=True, text=True).stdout.split()[0])
    allocated = int(run(['du', '-s', '-B1', db], capture_output=True, text=True).stdout.split()[0])
    ssts = list(db.glob('*.sst'))
    physical = subprocess.run(['btrfs', 'filesystem', 'du', '-s', '--raw', str(db)],
                              capture_output=True, text=True)
    return dict(db=str(db), du_sb=apparent, allocated_referenced_bytes=allocated,
                sst_count=len(ssts), sst_bytes=sum(p.stat().st_size for p in ssts),
                filesystem_free_bytes=shutil.disk_usage(ROOT).free,
                btrfs_du=physical.stdout.strip(), btrfs_du_error=physical.stderr.strip(),
                at=utc())


def append(path, row):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a') as stream:
        stream.write(json.dumps(row, sort_keys=True) + '\n')
        stream.flush()
        os.fsync(stream.fileno())


def rows(path):
    if not path.exists():
        return []
    result = []
    with path.open('r+b') as stream:
        while line := stream.readline():
            end = stream.tell()
            try:
                result.append(json.loads(line))
            except json.JSONDecodeError:
                require(not stream.read(1), f'corrupt journal: {path}')
                stream.truncate(end - len(line))
    return result


def clone(source, target):
    """Interrupted copies are resumed; only the finished directory can be opened."""
    from front_common import reflink_copy
    marker = target.with_name(target.name + '.clone.json')
    if marker.exists():
        require(load(marker)['source'] == str(source), 'clone source changed')
        if not target.exists():
            target.with_name(target.name + '.copying').rename(target)
        require(target.is_dir(), 'completed clone is missing')
        return
    require(not target.exists(), f'unmarked destination exists: {target}')
    staging = target.with_name(target.name + '.copying')
    if staging.exists():
        # The only removable tree is this script's incomplete reflink destination.
        shutil.rmtree(staging)
    started = time.monotonic()
    reflink_copy(source, staging)
    save(marker, dict(source=str(source), copied_at=utc(), seconds=time.monotonic() - started))
    staging.rename(target)


def snapshot_input(args):
    manifest = load(args.manifest)
    archive = manifest['archive']
    require(Path(archive['filename']).name == archive['filename'], 'archive filename must be a basename')
    require(archive['sha256'] == args.checksum.read_text().split()[0],
            'manifest and checksum file disagree')
    return manifest, ROOT / archive['filename']


def download(args, receipt):
    manifest, target = snapshot_input(args)
    archive = manifest['archive']
    if not target.exists() or target.stat().st_size != archive['size_bytes']:
        run(['curl', '--fail', '--location', '--continue-at', '-', '--retry', '5',
             '--retry-all-errors', '--output', target, archive['url']])
    require(target.stat().st_size == archive['size_bytes'], 'snapshot download is incomplete')
    require(digest(target) == archive['sha256'], 'snapshot SHA256 mismatch')
    signature = dict(size=target.stat().st_size, mtime_ns=target.stat().st_mtime_ns,
                     sha256=archive['sha256'])
    save(ROOT / 'snapshot.verified.json', signature)
    receipt['snapshot'] = signature


def extract(args, receipt):
    manifest, source = snapshot_input(args)
    prefix = PurePosixPath(manifest['archive']['extraction_path'])
    marker = ROOT / 'extract.complete.json'
    receipt['free_before_bytes'] = free_space(args.minimum_free_gib * 1024**3)
    require(source.stat().st_size == manifest['archive']['size_bytes'], 'snapshot download is incomplete')
    expected = manifest['archive']['sha256']
    require(not prefix.is_absolute() and '..' not in prefix.parts, 'unsafe extraction prefix')
    signature = dict(size=source.stat().st_size, mtime_ns=source.stat().st_mtime_ns, sha256=expected)
    verified = ROOT / 'snapshot.verified.json'
    if not verified.exists() or load(verified) != signature:
        started = time.monotonic()
        require(digest(source) == expected, 'snapshot SHA256 mismatch; extraction refused')
        receipt['verification_seconds'] = time.monotonic() - started
        save(verified, signature)
    receipt['snapshot'] = signature
    if marker.exists():
        require(load(marker)['snapshot'] == signature, 'completed extraction is from another snapshot')
        receipt.update(reused=True, result=load(marker))
        return
    staging, target = ROOT / 'archive.extracting', ROOT / 'archive'
    ready = ROOT / 'extract.ready.json'
    if ready.exists():
        require(load(ready)['snapshot'] == signature, 'ready extraction has a different snapshot')
        if not target.exists():
            staging.rename(target)
        ready.replace(marker)
        receipt.update(reused=True, result=load(marker))
        return
    require(not target.exists(), 'unmarked archive/ exists; preserve it and investigate')
    staging.mkdir(exist_ok=True)
    journal = ROOT / 'extracted-files.jsonl'
    completed = {row['name']: row['size'] for row in rows(journal)}
    started = time.monotonic()
    process = subprocess.Popen(['zstd', '-dc', '--', str(source)], stdout=subprocess.PIPE)
    try:
        with tarfile.open(fileobj=process.stdout, mode='r|') as archive:
            for member in archive:
                name = PurePosixPath(member.name)
                require(not name.is_absolute() and '..' not in name.parts,
                        f'unsafe archive path: {name}')
                require(name == prefix or prefix in name.parents or name in prefix.parents,
                        f'unexpected archive layout: {name}')
                path = staging.joinpath(*name.parts)
                if member.isdir():
                    path.mkdir(parents=True, exist_ok=True)
                    continue
                require(member.isfile(), f'unsupported archive member type: {name}')
                path.parent.mkdir(parents=True, exist_ok=True)
                if completed.get(str(name)) == member.size and path.exists() and path.stat().st_size == member.size:
                    continue
                temp = path.with_name(path.name + '.extracting')
                with archive.extractfile(member) as source_file, temp.open('wb') as output:
                    shutil.copyfileobj(source_file, output, 4 * 1024**2)
                    output.flush()
                    os.fsync(output.fileno())
                require(temp.stat().st_size == member.size, 'truncated tar member')
                temp.replace(path)
                append(journal, dict(name=str(name), size=member.size))
        require(process.wait() == 0, 'zstd extraction failed')
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait()
    measured = size(staging)
    measured['db'] = str(target / db_path(staging).relative_to(staging))
    result = dict(snapshot=signature, extraction_seconds=time.monotonic() - started, size=measured)
    save(ready, result)
    staging.rename(target)
    ready.replace(marker)
    receipt['result'] = result


def block_check(raw, expected=None):
    wire = bytes.fromhex(raw)
    require(len(wire) > 143, 'truncated Zcash block')
    tag = wire[140]
    width = {253: 2, 254: 4, 255: 8}.get(tag, 0)
    length = int.from_bytes(wire[141:141 + width], 'little') if width else tag
    end = 141 + width + length
    require(end < len(wire), 'truncated Equihash header')
    block_hash = hashlib.sha256(hashlib.sha256(wire[:end]).digest()).digest()[::-1].hex()
    if expected is not None:
        require(block_hash == expected, 'raw block hash mismatch')
    return dict(hash=block_hash, previous_hash=wire[4:36][::-1].hex(),
                sha256=hashlib.sha256(wire).hexdigest(), bytes=len(wire))


def feeder_endpoint(value):
    url = urlsplit(value)
    require(url.scheme in ('http', 'https') and url.hostname and
            not (url.username or url.password or url.query or url.fragment),
            'feeder URL must be HTTP(S), without embedded credentials, query or fragment')
    return url


def feeder_call(args, method, params=()):
    """Read a replicator's HTTP(S) feeder; never record its authentication material."""
    url = feeder_endpoint(args.feeder_url)
    headers = {'Content-Type': 'application/json'}
    auth = None
    if args.feeder_cookie:
        auth = args.feeder_cookie.read_text().strip()
    elif args.feeder_auth_env:
        auth = os.environ.get(args.feeder_auth_env)
        require(auth is not None, 'feeder authentication environment variable is unset')
    if auth is not None:
        require(':' in auth and '\n' not in auth, 'feeder authentication must be user:password')
        headers['Authorization'] = 'Basic ' + base64.b64encode(auth.encode()).decode()
    connection = (http.client.HTTPSConnection if url.scheme == 'https' else http.client.HTTPConnection)(
        url.hostname, url.port, timeout=args.rpc_timeout)
    try:
        connection.request('POST', url.path or '/', json.dumps(
            dict(jsonrpc='2.0', id=1, method=method, params=list(params))), headers)
        response = connection.getresponse()
        require(response.status == 200, f'feeder HTTP status {response.status}')
        reply = json.loads(response.read())
        require(not reply.get('error'), f"feeder RPC failed: {reply.get('error')}")
        return reply['result']
    finally:
        connection.close()


def fetch(args, receipt):
    measured = ROOT / 'prune.deleted.json'
    start = args.start_height
    if start is None:
        require(measured.exists(), 'run prune first or supply the stopped database --start-height; '
                'the snapshot manifest records a best tip, not its stored finalized tip')
        start = load(measured)['deletion']['initial']['tip']
    corpus = ROOT / 'blocks'
    corpus.mkdir(exist_ok=True)
    missing = []
    for height in range(start + 1, start + args.blocks + 1):
        hexfile, meta = corpus / f'{height}.hex', corpus / f'{height}.json'
        if hexfile.exists() and meta.exists():
            require(block_check(hexfile.read_text().strip(), load(meta)['hash']) ==
                    {key: load(meta)[key] for key in ('hash', 'previous_hash', 'sha256', 'bytes')},
                    f'cached block changed: {height}')
        else:
            missing.append(height)
    if missing:
        info = feeder_call(args, 'getblockchaininfo')
        require(info['chain'] == 'main', 'feeder is not Mainnet')
        require(info['blocks'] >= start + args.blocks, 'feeder is below the requested end height')
        for height in missing:
            block_hash = feeder_call(args, 'getblockhash', [height])
            raw = feeder_call(args, 'getblock', [block_hash, 0])
            require(feeder_call(args, 'getblockhash', [height]) == block_hash, 'feeder reorg during fetch')
            meta = dict(height=height, **block_check(raw, block_hash))
            temp = corpus / f'{height}.hex.tmp'
            temp.write_text(raw + '\n')
            temp.replace(corpus / f'{height}.hex')
            save(corpus / f'{height}.json', meta)
    chain = [load(corpus / f'{h}.json') for h in range(start + 1, start + args.blocks + 1)]
    for a, b in zip(chain, chain[1:]):
        require(b['previous_hash'] == a['hash'], 'fetched corpus is not one chain')
    receipt.update(start_height=start, end_height=start + args.blocks, blocks=args.blocks,
                   fetched=len(missing), reused=args.blocks - len(missing),
                   parent_hash=chain[0]['previous_hash'], tip_hash=chain[-1]['hash'],
                   raw_bytes=sum(row['bytes'] for row in chain), feeder_url=args.feeder_url)
    save(corpus / 'corpus.json', {key: receipt[key] for key in
         ('start_height', 'end_height', 'blocks', 'parent_hash', 'tip_hash', 'raw_bytes')})


@contextlib.contextmanager
def node(args, cache, directory, pruning):
    from front_common import MainnetNode
    edits = [('state', 'cache_dir', json.dumps(str(cache))),
             ('network', 'cache_dir', 'false'),
             ('state', 'should_backup_non_finalized_state', 'false'),
             ('tracing', 'filter', '"info,zebra_state=debug"')]
    if pruning:
        edits.append(('state', 'prune_blocks_older_than', str(RETENTION)))
    instance = MainnetNode(args.binary, directory, edits=edits, cpus=args.cpus)
    try:
        instance.start()
        instance.wait_rpc_ready(args.startup_timeout)
        instance._client.conn.timeout = args.rpc_timeout
        if instance._client.conn.sock:
            instance._client.conn.sock.settimeout(args.rpc_timeout)
        require(instance.rpc('getpeerinfo') == [], 'isolated node has peers')
        yield instance
    finally:
        code = instance.stop(timeout=300)
        require(code in (0, -signal.SIGINT, -signal.SIGTERM, None), f'node did not stop cleanly: {code}')


def status(instance):
    require(instance.proc.poll() is None, 'node exited unexpectedly')
    require(instance.rpc('getpeerinfo') == [], 'isolated node has peers')
    info = instance.rpc('getblockchaininfo')
    require(info['chain'] == 'main', 'not Mainnet')
    return dict(at=utc(), monotonic=time.monotonic(), tip=info['blocks'],
                hash=info['bestblockhash'], pruned=info.get('pruned', False),
                pruneheight=info.get('pruneheight', 0))


def witness(instance, finalized_tip):
    # This matches service/check.rs::legacy_chain: first V5/V6 from finalized tip.
    # The node already checks branch-ID consistency. No older pruned body is needed.
    for height in range(finalized_tip, max(finalized_tip - 100_000, 1_687_103), -1):
        block = instance.rpc('getblock', [str(height), 2])
        if any(tx['version'] in (5, 6) for tx in block['tx']):
            return height
    raise RuntimeError('no restart witness found within the documented 100,000-block bound')


def wait_floor(instance, target, args, journal):
    deadline = time.monotonic() + args.pruning_timeout
    first_change = None
    previous = None
    while True:
        sample = status(instance)
        append(journal, sample)
        if previous is not None and sample['pruneheight'] > previous:
            first_change = first_change or sample['monotonic']
        previous = sample['pruneheight']
        if sample['pruned'] and sample['pruneheight'] >= target:
            return dict(final=sample, first_observed_progress_monotonic=first_change,
                        target_floor=target)
        require(time.monotonic() < deadline, f'pruning deadline reached; target={target}, sample={sample}')
        time.sleep(args.poll_seconds)


def binary_info(args):
    args.binary = args.binary.resolve(strict=True)
    require(os.access(args.binary, os.X_OK), 'pruning binary must be executable')
    return dict(path=str(args.binary), sha256=digest(args.binary),
                version=run([args.binary, '--version'], capture_output=True, text=True).stdout.strip())


def prune(args, receipt):
    require((ROOT / 'extract.complete.json').exists(), 'run extract.sh first')
    receipt['binary'] = binary_info(args)
    receipt['free_before_bytes'] = free_space(args.minimum_free_gib * 1024**3)
    complete = ROOT / 'prune.complete.json'
    if complete.exists():
        result = load(complete)
        require(result['binary']['sha256'] == receipt['binary']['sha256'], 'pruning binary changed')
        receipt.update(reused=True, result=result)
        return
    cache = ROOT / 'pruned'
    clone(ROOT / 'archive', cache)
    checkpoint = ROOT / 'prune.deleted.json'
    if checkpoint.exists():
        deleted = load(checkpoint)
        require(deleted['binary']['sha256'] == receipt['binary']['sha256'], 'cannot resume with changed binary')
    else:
        before = size(cache)
        started = time.monotonic()
        with node(args, cache, receipt['run_dir'] / 'delete-node', True) as instance:
            initial = status(instance)
            # The official tar contains finalized state only (no non-finalized backup).
            finalized_tip = initial['tip']
            witness_height = witness(instance, finalized_tip)
            requested = max(1, finalized_tip + 1 - RETENTION)
            target = min(requested, witness_height)
            result = wait_floor(instance, target, args, ROOT / 'prune-progress.jsonl')
            result.update(requested_floor=requested, witness_height=witness_height,
                          initial=initial, startup_to_floor_seconds=time.monotonic() - started)
            first = result['first_observed_progress_monotonic']
            result['observed_deletion_seconds'] = None if first is None else result['final']['monotonic'] - first
            result['measurement_note'] = 'startup-to-floor includes migrations/checks; progress duration is a polling lower bound'
        deleted = dict(binary=receipt['binary'], before=before, deletion=result, after_deletion=size(cache))
        save(checkpoint, deleted)
    receipt['deletion_checkpoint'] = deleted
    compact_started = time.monotonic()
    compact_log = receipt['run_dir'] / 'compaction.jsonl'
    method, error = 'forced-all-column-families', None
    try:
        require(args.compactor.is_file(), 'compaction utility is not built')
        with compact_log.open('w') as output:
            run([args.compactor, '--allowed-root', ROOT, db_path(cache)],
                stdout=output, stderr=subprocess.STDOUT, timeout=args.pruning_timeout)
    except (RuntimeError, subprocess.SubprocessError) as failure:
        error = repr(failure)
        require(args.allow_idle_fallback, f'forced compaction failed ({error}); inspect {compact_log}; '
                'explicit --allow-idle-fallback permits a non-final observation')
        method = 'restart-and-idle-nonfinal'
        with node(args, cache, receipt['run_dir'] / 'idle-node', True) as instance:
            deadline = time.monotonic() + args.idle_seconds
            while time.monotonic() < deadline:
                append(ROOT / 'prune-idle-progress.jsonl', status(instance))
                time.sleep(min(args.poll_seconds, max(0, deadline - time.monotonic())))
    result = dict(**deleted, compaction=dict(method=method, forced=error is None, error=error,
                  seconds=time.monotonic() - compact_started, log=str(compact_log)),
                  after_compaction=size(cache))
    save(complete, result)
    receipt['result'] = result


def replay(args, receipt):
    require((ROOT / 'extract.complete.json').exists(), 'run extract.sh first')
    require((ROOT / 'prune.complete.json').exists(), 'run prune.sh first (provides the after-deletion source)')
    receipt['binary'] = binary_info(args)
    require(load(ROOT / 'prune.complete.json')['binary']['sha256'] == receipt['binary']['sha256'],
            'replay must use the exact pruning build used by prune.sh')
    receipt['free_before_bytes'] = free_space(args.minimum_free_gib * 1024**3)
    corpus = load(ROOT / 'blocks/corpus.json')
    end = corpus['start_height'] + args.blocks
    require(end <= corpus['end_height'], 'fetch the requested block count before replay')
    from report import summarize_pairs
    summaries = {}
    for phase in ('during', 'after') if args.mode == 'both' else (args.mode,):
        completion = ROOT / f'replay-{phase}.complete.json'
        if completion.exists():
            old = load(completion)
            require(old['binary']['sha256'] == receipt['binary']['sha256'] and
                    old['requested_blocks'] == args.blocks and old['end_height'] == end,
                    'completed replay arguments differ; preserve receipts and use a new measurement directory')
            summaries[phase] = old
            continue
        # A failed/interrupted timing run starts a fresh attempt; duplicate/restarted
        # submissions cannot be treated as valid latency samples. Completed modes resume.
        attempt = receipt['run_dir'] / phase
        attempt.mkdir()
        archive_cache, pruned_cache = attempt / 'archive-cache', attempt / 'pruned-cache'
        clone(ROOT / 'archive', archive_cache)
        clone(ROOT / ('archive' if phase == 'during' else 'pruned'), pruned_cache)
        observation_path = attempt / 'observations.jsonl'
        observations = []
        try:
            with node(args, archive_cache, attempt / 'archive-node', False) as archive_node:
                # Archive startup/preload completes first, leaving maximum deletion overlap.
                initial_archive = status(archive_node)
                with node(args, pruned_cache, attempt / 'pruned-node', True) as pruned_node:
                    initial_pruned = status(pruned_node)
                    require(initial_pruned['tip'] == initial_archive['tip'] and
                            initial_pruned['hash'] == initial_archive['hash'], 'replay sources have different tips')
                    tip = initial_archive['tip']
                    require(tip >= corpus['start_height'],
                            f'actual finalized tip is below the corpus; rerun fetch-blocks.sh --start-height {tip} --blocks {args.blocks}')
                    require(tip < end, 'fetch more blocks: actual snapshot tip is beyond the requested corpus')
                    first = load(ROOT / 'blocks' / f'{tip + 1}.json')
                    require(first['previous_hash'] == initial_archive['hash'], 'snapshot and fetched chain disagree')
                    witness_height = witness(pruned_node, tip)
                    target = min(max(1, tip + 1 - RETENTION), witness_height)
                    if phase == 'during':
                        # A nonzero backlog is required; never label already-finished deletion as overlap.
                        sample = status(pruned_node)
                        require(sample['pruneheight'] < target, 'deletion completed before replay readiness; during phase unavailable')
                        if sample['pruneheight'] == 0:
                            deadline = time.monotonic() + args.pruning_timeout
                            while sample['pruneheight'] == 0:
                                append(attempt / 'prune-progress.jsonl', sample)
                                require(time.monotonic() < deadline, 'pruning did not start')
                                time.sleep(min(args.poll_seconds, .1))
                                sample = status(pruned_node)
                        require(sample['pruneheight'] < target, 'deletion finished before the first submission')
                    else:
                        wait_floor(pruned_node, target, args, attempt / 'prune-progress.jsonl')
                    for index, height in enumerate(range(tip + 1, end + 1)):
                        raw = (ROOT / 'blocks' / f'{height}.hex').read_text().strip()
                        metadata = load(ROOT / 'blocks' / f'{height}.json')
                        block_check(raw, metadata['hash'])
                        before = status(pruned_node)
                        pair = dict(height=height, hash=metadata['hash'], phase=phase,
                                    before=before, started_at=utc(), timings={}, outcomes={})
                        order = [('archive', archive_node), ('pruned', pruned_node)]
                        if index % 2:
                            order.reverse()
                        pair['order'] = [name for name, _ in order]
                        try:
                            for name, instance in order:
                                require(instance.rpc('getpeerinfo') == [], 'isolated node has peers')
                                started_ns = time.perf_counter_ns()
                                outcome = instance.rpc('submitblock', [raw])
                                elapsed_ns = time.perf_counter_ns() - started_ns
                                pair['timings'][name] = elapsed_ns / 1e6
                                pair['outcomes'][name] = outcome
                                require(outcome is None, f'{name} submitblock rejected: {outcome}')
                                require(instance.rpc('getbestblockhash') == metadata['hash'], 'accepted tip differs')
                            pair['after'] = status(pruned_node)
                            # Initial backlog only; regular future finalization pruning is not
                            # confused with deleting millions of historical blocks.
                            pair['deleting'] = (before['pruneheight'] > 0 and before['pruneheight'] < target and
                                                pair['after']['pruneheight'] < target)
                            pair['floor_advanced'] = pair['after']['pruneheight'] > before['pruneheight']
                            pair['status'] = 'ok'
                        except BaseException:
                            pair['status'] = 'failed'
                            pair['error'] = traceback.format_exc()
                            raise
                        finally:
                            append(observation_path, pair)
                        observations.append(pair)
                    final = status(pruned_node)
                    result = dict(binary=receipt['binary'], phase=phase, requested_blocks=args.blocks,
                                  actual_start_height=tip, end_height=end, actual_pairs=len(observations),
                                  initial_target=target, witness_height=witness_height,
                                  final=final, observations=str(observation_path),
                                  summary=summarize_pairs(observations, phase))
                    if phase == 'during':
                        require(result['summary']['n'] > 1 and any(r['floor_advanced'] for r in observations if r['deleting']),
                                'insufficient demonstrated deletion overlap; retain raw observations, do not claim no regression')
            save(completion, result)
            summaries[phase] = result
        finally:
            # Preserve failed attempts for diagnosis. Never remove a possibly live
            # database if shutdown itself failed.
            if phase in summaries:
                for cache in (archive_cache, pruned_cache):
                    shutil.rmtree(cache)
    receipt['results'] = summaries


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('phase', choices=['download', 'extract', 'fetch', 'prune', 'replay'])
    p.add_argument('--output', type=Path, required=True, help='local measurement/data directory')
    p.add_argument('--manifest', type=Path, default=SCRIPT / 'inputs/archive-snapshot.json')
    p.add_argument('--checksum', type=Path, default=SCRIPT / 'inputs/archive-snapshot.sha256')
    p.add_argument('--dry-run', action='store_true')
    p.add_argument('--blocks', type=int, default=1000)
    p.add_argument('--start-height', type=int)
    p.add_argument('--feeder-url', help='read-only Mainnet feeder HTTP(S) JSON-RPC endpoint')
    auth = p.add_mutually_exclusive_group()
    auth.add_argument('--feeder-cookie', type=Path, help='runtime user:password cookie file')
    auth.add_argument('--feeder-auth-env', help='environment variable containing user:password')
    p.add_argument('--binary', type=Path, help='pruning-enabled zebrad; required for prune/replay')
    p.add_argument('--compactor', type=Path, default=SCRIPT / 'compact/target/release/prune-compact')
    p.add_argument('--cpus', help='taskset CPU list for nodes; default: inherit process affinity')
    p.add_argument('--minimum-free-gib', type=int, default=None)
    p.add_argument('--startup-timeout', type=int, default=7200)
    p.add_argument('--pruning-timeout', type=int, default=86400)
    p.add_argument('--rpc-timeout', type=int, default=120)
    p.add_argument('--poll-seconds', type=float, default=1)
    p.add_argument('--idle-seconds', type=int, default=3600)
    p.add_argument('--allow-idle-fallback', action='store_true')
    p.add_argument('--mode', choices=['both', 'during', 'after'], default='both')
    return p


def main(argv=None):
    global ROOT
    p = parser()
    args = p.parse_args(argv)
    if args.phase in ('prune', 'replay') and args.binary is None:
        p.error('--binary is required for prune/replay')
    if args.phase == 'fetch' and not args.feeder_url:
        p.error('--feeder-url is required for fetch')
    if args.feeder_url:
        feeder_endpoint(args.feeder_url)
    ROOT = args.output.resolve()
    args.compactor = args.compactor.resolve()
    require(args.blocks > 0 and args.poll_seconds > 0, 'blocks and poll interval must be positive')
    require(args.start_height is None or args.start_height >= 0, 'start height must be nonnegative')
    require(min(args.startup_timeout, args.pruning_timeout, args.rpc_timeout, args.idle_seconds) > 0,
            'timeouts must be positive')
    if args.minimum_free_gib is None:
        args.minimum_free_gib = 1200 if args.phase == 'extract' else 500
    require(args.minimum_free_gib >= 0, 'minimum free space must be nonnegative')
    plan = dict(phase=args.phase, root=str(ROOT), args={key: str(value) if isinstance(value, Path) else value
                                                    for key, value in vars(args).items()},
                snapshot_layout='archive/state/v29/mainnet; nodes migrate copies to state/v30/mainnet',
                deletion_target='min(finalized_tip + 1 - 10000, restart_witness_height)',
                isolation='loopback port 0, empty seeds, peer cache disabled, zero-peer audits')
    if args.dry_run:
        print(json.dumps(plan, indent=2))
        return 0
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ')
    run_dir = ROOT / 'runs' / f'{stamp}-{args.phase}'
    run_dir.mkdir(parents=True)
    receipt = dict(schema=1, **plan, started_at=utc(), run_dir=run_dir, status='running')
    path = ROOT / 'receipts' / f'{stamp}-{args.phase}.json'
    started = time.monotonic()
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f'signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    try:
        with (ROOT / '.phase.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            globals()[args.phase](args, receipt)
        receipt['status'] = 'ok'
        return 0
    except BaseException:
        receipt.update(status='failed', error=traceback.format_exc())
        raise
    finally:
        receipt['finished_at'] = utc()
        receipt['wall_seconds'] = time.monotonic() - started
        receipt['run_dir'] = str(run_dir)
        save(path, receipt)
        print(f'receipt: {path}', file=sys.stderr)


if __name__ == '__main__':
    sys.exit(main())
