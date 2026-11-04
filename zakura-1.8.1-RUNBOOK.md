# Zakura v1.8.1 comparison runbook

This is a new campaign, not a relabeling of the historical v1.7.0 results in
[README.md](README.md). It covers controlled Regtest mining-path comparisons and
32,860 Mainnet blocks, heights **3,505,701–3,538,560**, starting from a stopped
state at **3,505,700**. No timings from this campaign have been measured here.

The [Zakura 1.8.0 announcement][announcement] reports that Mainnet window on an
8-vCPU Intel Xeon host: three alternating runs, no peers, empty mempools, full
verification and commit. Its 461 s / 1,141 s totals are published observations,
not expected results for these revisions, this harness, or this machine. Its
“Coming next” section is not a description of the tagged 1.8.0 release. This
runbook pins 1.8.1 and records its actual build identity.

## 1. Inputs, machine placement, and build parity

Run these Bash commands on the build/measurement host from the harness root.
Use a reflink-capable filesystem for states and output. Requirements are in the
[README](README.md#requirements-and-environment). Zebra still needs libclang for
its RocksDB bindings; pinned Zakura does not. x86-64 must support BMI2 and ADX for
the assembly arms. Keep the same native compiler, linker, libraries, Rust
compiler, and environment throughout the builds.

```bash
set -euo pipefail
export HARNESS="$PWD"
export WORK="$HARNESS/work/zakura-1.8.1"
export BENCH_BINS="$HARNESS/bins/zakura-1.8.1"
export BENCH_OUTPUT="$HARNESS/out/zakura-1.8.1"
export BENCH_MAINNET="$HARNESS/snapshots/zakura-1.8.1"
export BENCH_FIXTURES="$HARNESS/fixtures/zakura-1.8.1/standard"
export BENCH_SIGNED_FIXTURES="$HARNESS/fixtures/zakura-1.8.1/signed"
export V5_CPUS=0-7,16-23
export SOURCE_CPUS=8-15,24-31
export QUIET_DEVICE=nvme0n1
export ZEBRA_REPO="$WORK/zebra-source"
export ZAKURA_REPO="$WORK/zakura-source"
export ARCHIVE="$HARNESS/snapshots/source-state-clean"
mkdir -p "$WORK" "$BENCH_BINS" "$BENCH_OUTPUT" "$BENCH_MAINNET"

export RUSTUP_TOOLCHAIN=1.97.0
export CARGO_BUILD_JOBS=8
export CARGO_TARGET_DIR=target
export CARGO_PROFILE_RELEASE_OPT_LEVEL=3
export CARGO_PROFILE_RELEASE_LTO=thin
export CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1
export CARGO_PROFILE_RELEASE_DEBUG=0
export CARGO_PROFILE_RELEASE_STRIP=none
export CARGO_PROFILE_RELEASE_PANIC=unwind
export CARGO_PROFILE_RELEASE_INCREMENTAL=false
export CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=false
export CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS=false
export RUSTFLAGS='-C codegen-units=1'
unset CARGO_ENCODED_RUSTFLAGS RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER
unset ZEBRA_EXTRA_FEATURES
export CC=gcc CXX=g++ AR=ar
export CFLAGS='' CXXFLAGS='' CPPFLAGS='' LDFLAGS=''
```

Choose the real block device and inspect CPU topology before using these masks
on another machine. Keep the default measured mask `0-7,16-23` while live nodes
are running on the other CCD. For whole-host measurements, stop the dashboard
**and its live nodes**, obtain an exclusive host reservation, and keep other
builds, syncs, and I/O jobs stopped. `--exclusive` is an operator assertion, not
a service manager or a host lock. Never run a whole-host and a default-mask
campaign as if they were repetitions of the same experiment.

Why explicit profile settings: [Zebra's manifest][zebra-manifest] has
`panic="abort"` and no release codegen-unit override; [Zakura's][zakura-manifest]
requires unwinding, sets four release codegen units and one for dependencies.
Merely using the same `cargo build --release` or matching unset environment
variables does not equalize those profiles. The common `RUSTFLAGS` override
also covers package-specific codegen-unit settings. Do not add `target-cpu=native`
to just one arm. Avoid personal Cargo configuration/target-specific flags that
change compiler or linker settings without appearing in the receipts.

| Label | Immutable source | Binary | Feature difference |
|---|---|---|---|
| `stack` | `5527c563929fbe3b9e82d099b4d4cfc19b092e20` | `zebrad-stack` | Zebra release features + `internal-miner` |
| `crypto` | same as `stack` | `zebrad-crypto` | same source + `pasta-asm` |
| `andale` | `7ae1a827471235e8d14204e2aaa77dc9b61ca25c` | `zebrad-andale` | pinned Zebra next, not an arbitrary moving branch |
| `zakura-main` | `f5c20398c629c8179364b07f059da3c814af78ec` | `zakurad-1.8.1` | Zakura `default-release-binaries` |
| `extra` (optional replay only) | explicit full 40-character `EXTRA_REVISION` | `zebrad-extra` | same Zebra recipe as `stack`, without `pasta-asm` |

The names identify this campaign, not equivalence to another publisher's build.
Zakura's default release features include `x86_64-asm`, `indexer-rpc`, Prometheus,
and an INFO compile-time log ceiling; do not silently add `portable`.

## 2. Build and seal binaries

Use fresh clean clones; do not change existing working checkouts. The source
repositories above must contain the pinned objects. Public clone URLs are
`https://github.com/ZcashFoundation/zebra.git` and
`https://github.com/zakura-core/zakura.git`; local clones also preserve access to
pinned commits available only in the supplied source repositories.

```bash
git clone --no-hardlinks "$ZEBRA_REPO" "$WORK/zebra-stack"
git -C "$WORK/zebra-stack" checkout --detach 5527c563929fbe3b9e82d099b4d4cfc19b092e20
git clone --no-hardlinks "$ZEBRA_REPO" "$WORK/zebra-andale"
git -C "$WORK/zebra-andale" checkout --detach 7ae1a827471235e8d14204e2aaa77dc9b61ca25c
git clone --no-hardlinks "$ZAKURA_REPO" "$WORK/zakura"
git -C "$WORK/zakura" checkout --detach f5c20398c629c8179364b07f059da3c814af78ec
(cd "$WORK/zebra-stack" && bash "$HARNESS/regtest/build-arm.sh" zebrad-stack)
(cd "$WORK/zebra-stack" && ZEBRA_EXTRA_FEATURES=pasta-asm \
  bash "$HARNESS/regtest/build-arm.sh" zebrad-crypto)
(cd "$WORK/zebra-andale" && bash "$HARNESS/regtest/build-arm.sh" zebrad-andale)
(cd "$WORK/zakura" && bash "$HARNESS/regtest/build-zakura.sh" zakurad-1.8.1)

# Optional: set EXTRA_REVISION to the complete immutable Zebra commit first.
if [[ -n ${EXTRA_REVISION:-} ]]; then
  [[ $EXTRA_REVISION =~ ^[0-9a-f]{40}$ ]]
  export EXTRA_REVISION
  git clone --no-hardlinks "$ZEBRA_REPO" "$WORK/zebra-extra"
  git -C "$WORK/zebra-extra" checkout --detach "$EXTRA_REVISION"
  (cd "$WORK/zebra-extra" && bash "$HARNESS/regtest/build-arm.sh" zebrad-extra)
fi
```

The wrappers execute these recipes under the environment above and write
`BINARY.build.json` receipts containing source, path, SHA256, build command,
compiler version and environment:

```bash
# stack, andale, optional extra
cargo build --locked --release -p zebrad --features default-release-binaries,internal-miner
# crypto
cargo build --locked --release -p zebrad --features default-release-binaries,internal-miner,pasta-asm
# zakura-main
cargo build --locked --release -p zakura --bin zakurad --features default-release-binaries
```

Compose replay provenance with the existing receipt validator. It checks the
executable against its build receipt; the replay driver additionally checks the
pinned sources, feature recipes, and explicit compiler-setting parity. Embedded
version strings are evidence, not a replacement for source and binary hashes.

```bash
export PROVENANCE="$BENCH_OUTPUT/replay-arms.json"
python3 -B - <<'PY'
import json, os, subprocess, sys
from pathlib import Path
sys.path.insert(0, str(Path(os.environ['HARNESS']) / 'regtest'))
from v5_campaign import provenance
bins = Path(os.environ['BENCH_BINS'])
names = {'stack': 'zebrad-stack', 'crypto': 'zebrad-crypto',
         'andale': 'zebrad-andale', 'zakura-main': 'zakurad-1.8.1'}
if os.environ.get('EXTRA_REVISION'):
    names['extra'] = 'zebrad-extra'
receipts = provenance({label: str(bins / name) for label, name in names.items()})
for label, receipt in receipts.items():
    receipt['version'] = subprocess.check_output(
        [receipt['path'], '--version'], text=True).strip()
with Path(os.environ['PROVENANCE']).open('x') as stream:
    json.dump(receipts, stream, indent=2)
    stream.write('\n')
PY
```

Keep the binaries, adjacent receipts, provenance, build logs, `rustc -vV`, and
native compiler/linker versions. Rebuilding an executable requires new campaign
output and provenance; historical SHA256 values in the README are not expected
hashes for these builds.

## 3. Prepare exact stopped Mainnet states

### Zebra: copy through height 3,505,700

The supplied `$ARCHIVE` is a **cache root** containing `state/v29/mainnet`, not a
path directly to RocksDB. It must be a stopped, complete archive whose stored
finalized tip is at least 3,505,700. Do not use a snapshot's advertised best tip
as proof of its stored finalized tip. Keep the original archive unchanged.

`copy-state` takes **`--max-source-height`**, not `--max-height`. It reads the
source, commits blocks into the target, and compares their bytes/hashes; it is
not a reflink or full cryptographic re-verification of the prefix. The
[stack implementation][copy-stack] supports read-only access to compatible v29
state and produces its own current format. [Pinned next][copy-next] has the same
CLI. Prepare each distinct Zebra revision using its own executable rather than
assuming that a newer stack database can be opened by next.

```bash
export PREP="$BENCH_OUTPUT/state-preparation"
mkdir -p "$PREP"
cat >"$PREP/source.toml" <<EOF
[network]
network = "Mainnet"
[state]
cache_dir = "$ARCHIVE"
delete_old_database = false
should_backup_non_finalized_state = false
[tracing]
filter = "info"
use_color = false
EOF

copy_prefix() {
  local label=$1 binary=$2 target="$BENCH_MAINNET/$1-3505700"
  [[ ! -e $target ]]
  cat >"$PREP/$label-target.toml" <<EOF
[state]
cache_dir = "$target"
delete_old_database = false
should_backup_non_finalized_state = false
EOF
  taskset -c "$V5_CPUS" "$binary" -c "$PREP/source.toml" copy-state \
    --max-source-height 3505700 --target-config-path "$PREP/$label-target.toml" \
    >"$PREP/$label-copy.log" 2>&1
}
copy_prefix stack "$BENCH_BINS/zebrad-stack"
copy_prefix andale "$BENCH_BINS/zebrad-andale"
# crypto uses exactly stack's state format; create a separate stopped cache root.
[[ ! -e $BENCH_MAINNET/crypto-3505700 ]]
cp -a --reflink=always "$BENCH_MAINNET/stack-3505700" "$BENCH_MAINNET/crypto-3505700"
if [[ -n ${EXTRA_REVISION:-} ]]; then
  copy_prefix extra "$BENCH_BINS/zebrad-extra"
fi
```

Check each copy log's final target height is **3,505,700**. `copy-state` clamps to
the available source height, so a zero exit status alone is insufficient. An
arbitrary extra revision must support this command, the source format and this
Mainnet era; if not, prepare an independently synced state with that binary.
The smoke run below checks each starting height and hash against the corpus
before any measured submission. Do not turn a wrong-height state into a valid
input by editing metadata.

### Zakura: sync its own archive

**Do not give Zakura the Zebra archive, rename version directories, or treat
matching `v29`/`v30` names as compatibility.** The persisted `BlockInfo` layouts
already differ: [Zebra][zebra-codec] places the block size at bytes 48–51 and NSM
at 52–59; [Zakura][zakura-codec] places NSM at 48–55 and the size at 56–59.
Zakura's [upgrade registry][zakura-upgrades] reuses *its own* v29 for v30; this is
not a Zebra import migration. Its current [format version][zakura-format] is
30.0.0, unchanged between the examined v1.7.0 and v1.8.1 pins. No cross-project
state import has been exercised here. The supported preparation in this
runbook is an independent Zakura sync.

Reuse the existing snapshot helper, with explicit legacy P2P and archive mode.
Disable VCT fast sync during preparation so the stopped prefix has ordinary
per-height commitment-tree state even though it ends below the release's last
checkpoint. Preparation uses checkpoints; **only the measured suffix** is
claimed as fully verified. Do not inherit unrelated `ZAKURA_*`/`ZEBRA_*` config
overrides into this command.

```bash
ZAKURA_NETWORK__P2P_STACK=legacy \
ZAKURA_STATE__STORAGE_MODE=archive \
ZAKURA_STATE__DELETE_OLD_DATABASE=false \
ZAKURA_STATE__SHOULD_BACKUP_NON_FINALIZED_STATE=false \
ZAKURA_CONSENSUS__CHECKPOINT_SYNC=true \
ZAKURA_CONSENSUS__VCT_FAST_SYNC=false \
ZAKURA_TRACING__FILTER=info \
CPUS="$V5_CPUS" STOP_AT=3505700 \
SNAPSHOT_OUTPUT="$BENCH_MAINNET/zakura-main-3505700" \
RPC_PORT=28242 P2P_PORT=28243 STARTUP_TIMEOUT=7200 \
bash initial-sync/prepare-mainnet-snapshots.sh \
  "$BENCH_BINS/zakurad-1.8.1" "$PREP/zakura-sync"
```

The helper requires a successful configured-height stop and the matching stop
log before making its reflink snapshot. Keep its config, logs, samples, and
binary hash. Never launch two processes against one cache. Do not substitute a
current pruned bootstrap: it is neither this height nor this archive layout.

## 4. Fetch the block corpus once

Use an archive RPC with all bodies through 3,538,560, not the height-limited
start states. The existing local v29 archive may end below that height; it can
still supply the prefix but then cannot supply the complete corpus. Use a
separately synced archive RPC or continue syncing a **copy** of that archive.
The helper needs `getblockchaininfo`, `getblockhash`, and `getblock HASH 0`.

```bash
export FEEDER_URL=http://127.0.0.1:8232/
export FEEDER_COOKIE=/path/to/archive-rpc/.cookie
bash pruning/fetch-blocks.sh --output "$BENCH_OUTPUT/corpus" \
  --start-height 3505700 --blocks 32860 \
  --feeder-url "$FEEDER_URL" --feeder-cookie "$FEEDER_COOKIE"
export CORPUS="$BENCH_OUTPUT/corpus/blocks"
```

Set the endpoint and cookie path to the actual archive. For an intentionally
unauthenticated loopback RPC, omit `--feeder-cookie`; for credentials held in an
environment variable use `--feeder-auth-env VARIABLE_NAME` instead. Never embed
credentials in the URL or publish cookies.

This writes 32,860 raw **hex** block files, one metadata JSON per height, and
`corpus.json` containing parent/tip hashes and byte count. Re-running the fetch
reuses checked files and only downloads missing ones. It checks header hashes,
raw-byte SHA256, and continuity; this does not replace consensus verification.
All arms and repetitions must use this one corpus. Stop its feeder or keep it
fully outside the measured host before timing.

## 5. Mainnet smoke and full replay

```bash
replay_args=(
  --provenance "$PROVENANCE"
  --arm "stack=$BENCH_BINS/zebrad-stack"
  --arm "crypto=$BENCH_BINS/zebrad-crypto"
  --arm "andale=$BENCH_BINS/zebrad-andale"
  --arm "zakura-main=$BENCH_BINS/zakurad-1.8.1"
  --snapshot "stack=$BENCH_MAINNET/stack-3505700"
  --snapshot "crypto=$BENCH_MAINNET/crypto-3505700"
  --snapshot "andale=$BENCH_MAINNET/andale-3505700"
  --snapshot "zakura-main=$BENCH_MAINNET/zakura-main-3505700"
  --corpus "$CORPUS" --cpus "$V5_CPUS"
)
# Each extra arm needs `--extra LABEL=REVISION`, a binary and a snapshot; repeat for more arms,
# e.g. one per performance change to attribute each one's effect.
if [[ -n ${EXTRA_REVISION:-} ]]; then
  replay_args+=(--extra "extra=$EXTRA_REVISION"
    --arm "extra=$BENCH_BINS/zebrad-extra"
    --snapshot "extra=$BENCH_MAINNET/extra-3505700")
fi
python3 -B regtest/mainnet_replay.py "${replay_args[@]}" \
  --smoke --output "$BENCH_OUTPUT/replay-smoke"
python3 -B regtest/mainnet_replay.py "${replay_args[@]}" \
  --output "$BENCH_OUTPUT/replay-full"
```

Smoke is ten blocks and one run per arm, not a comparison. Full replay is three
runs per arm in interleaved order. Every run uses a fresh mandatory reflink of
its implementation-specific stopped snapshot, with no peers, empty mempool,
mining disabled, `checkpoint_sync=false`, and `tracing.filter="info"`. Successful
stopped run clones are removed; failures retain diagnostics/state. Output
locations must be new, outside the input snapshots. No filesystem caches are
dropped: a fresh process is not a cold-disk experiment.
The shared node launcher removes inherited `ZEBRA_*` and `ZAKURA_*` overrides
before generating/starting measured configs, so an ambient environment cannot
silently restore checkpoint sync or seed peers.

After stopping the dashboard and its live nodes and reserving the entire host,
the explicit whole-host alternative on the documented 32-thread machine is:

```bash
python3 -B regtest/mainnet_replay.py "${replay_args[@]}" \
  --exclusive --cpus 0-31 --output "$BENCH_OUTPUT/replay-exclusive"
```

The later `--cpus` value overrides the default-mask array entry. Choose the real
whole-host CPU mask on other machines. Preserve affinity and exclusivity in the
campaign record; do not mix this output with default-mask repetitions.

### What “full verification and commit” means here

Both [Zebra's router][zebra-router] and [Zakura's router][zakura-router] route
above their selected checkpoint boundary to the semantic block verifier and
then contextual state validation/commit. `checkpoint_sync=false` selects the
first checkpoint at/after the mandatory checkpoint height; it does **not**
remove mandatory historical checkpoint verification or retroactively verify a
snapshot. This recent replay window is above that boundary in both nodes.

[Zebra `submit_block`][zebra-rpc] awaits the verifier result even if it emits an
early validated-block notification. [Zakura `submit_block` and
`run_mined_block_lifecycle`][zakura-rpc] likewise await verification/commit,
even if optimistic inventory is sent earlier. Neither path turns a normal
submission into a proposal-only check. Empty mempools and no template/proposal
warmup exclude prior candidate verification in this workload. A JSON `null`
result can still represent a side-chain commit; the harness therefore also
requires the exact submitted hash to become the best tip after every block.

“Commit” is the implementation's state-service commit, not an `fsync` or proof
that the newest block is finalized on disk. The normal non-finalized tail can
remain in memory; this replay does not add a finalization tail. Do not describe
all 32,860 blocks as individually durable RocksDB commits.

Read `campaign.json`, per-run `run.json` / `blocks.csv`, `summary.json`, and
`per-block-medians.json` together. `submit_ms` measures request-to-RPC-response;
`accepted_ms` additionally includes the best-tip query. Replay wall time also
includes corpus reads and tip checks. Replay CPU is node user+system CPU over
the replay loop; whole-process CPU also includes startup/shutdown. Per-block
statistics use medians across three runs; complete-run totals use means. Do
not compare an RPC-only total with the announcement's whole-run total without
identifying the difference. Preserve failures instead of dropping slow or
unsuccessful observations.

## 6. Regtest comparison rows

The announcement does not publish all fixture, mempool, signature, concurrency,
or timer details. These are **controlled comparisons of the named operations**,
not an exact replication of its undisclosed protocol. Generate fixtures once
and use identical sealed bytes for every arm. The standard producer recipe and
rootless Podman requirements are described in the README.

```bash
python3 -B regtest/sweep/build_fixtures.py \
  --producer "$BENCH_BINS/zebrad-stack" --sizes 2000 250000 1900000 \
  --output "$BENCH_FIXTURES"
python3 -B regtest/sweep/build_fixtures.py \
  --producer "$BENCH_BINS/zebrad-stack" --transparent-signed \
  --sizes 2000 250000 1900000 --output "$BENCH_SIGNED_FIXTURES"

comparison() {
  bash "$HARNESS/regtest/v5-campaign.sh" --profile comparison --shared-host \
    --output "$BENCH_OUTPUT/regtest-comparison" "$@" \
    stack:zebrad-stack crypto:zebrad-crypto andale:zebrad-andale \
    zakura-main:zakurad-1.8.1
}
comparison --dry-run
comparison --list
comparison --piece smoke
comparison --piece peer-work-fresh
comparison --piece peer-work-settled
comparison --piece transparent-burst
```

Alternatively, invoke `comparison` once in a fresh output directory to run the
whole profile, including smoke. Do not invoke the whole profile after the
piece-by-piece sequence above. The driver creates its own `arms.json` from the
same binary receipts and refuses completed-piece overwrites. `--shared-host`
skips the host-wide quiet gate; it does not reserve CPUs or prove the absence
of interference. After stopping the dashboard and other live workloads and
reserving the whole host, use this separate exclusive campaign:

```bash
V5_CPUS=all bash regtest/v5-campaign.sh --profile comparison --exclusive \
  --output "$BENCH_OUTPUT/regtest-exclusive" \
  stack:zebrad-stack crypto:zebrad-crypto andale:zebrad-andale \
  zakura-main:zakurad-1.8.1
```

`all` inherits the caller's affinity: launch from an unrestricted shell.
`--exclusive` retains the quiet gate and is mutually exclusive with
`--shared-host`. Never reuse campaign output across modes, masks, or binaries.
The Regtest validator checks source/binary receipts; compiler/feature parity
still depends on using the common build protocol above.

| Piece | Controlled workload and timing |
|---|---|
| `peer-work-fresh` | Signed-transparent ~1.9 MB block; receiver preloads the candidate transactions plus two surviving transactions; 18 samples per arm with rotated order. From before sender `submitblock` to receiver's first template on the new parent; 0-second connection-settling delay. |
| `peer-work-settled` | Same workload and timer, but 20-second connection-settling delay. |
| `transparent-burst` | Signed-transparent ~1.9 MB corpus, serial `sendrawtransaction` with no rate limiter, then the exact full template; three repetitions per arm. |

The peer timer includes sender validation and relay but excludes solving/mining
proof of work. “Fresh” means no added sleep after the handshake, not a zero-age
connection: setup RPC/JSON work remains and observed link age is recorded.
The primary first-template result may be coinbase-only; the full-surviving-work
result must separately pass transaction-membership checks. Both peers use the
same arm. The sender has no miner but may learn transactions through gossip.
Regtest uses legacy TCP, transparent payout, and fixtures with upgrades through
NU5. The [Zakura coinbase builder][zakura-coinbase] rejects Sapling payout and
pays an Orchard receiver through Ironwood only from NU6.3; neither supplies a
shielded-payout cell under these fixtures. The optional `extra` arm applies to
Mainnet replay, not this fixed four-arm Regtest profile.

Each measured piece writes `summary.json` with per-arm sample counts,
median/min/max, actual block bytes and transaction counts, selected metric, and
timing definition. Peer raw samples live in
`peer-work-{fresh,settled}/ARM-N/sample.json`, retaining first/full-work times,
link age and mempool identities. Burst samples are in
`transparent-burst/observations.jsonl`, with its sealed manifest and failure
receipts. The fixture set needs `empty-2000`, `transparent-signed-250000` for
smoke, and `transparent-signed-1900000` plus at least two unused signed
transactions for the peer cases. Preserve incomplete/error receipts; they are
not successful latency samples.

## 7. Capacity planning and interpretation

The following are **unmeasured planning estimates**, not benchmark results or
completion promises. Allow roughly 1–4 hours for clean builds, 6–48 hours per
Zebra prefix reconstruction, and 1–3 days for an independent archive sync;
network/SSD speed and peer availability can change these substantially. Allow
hours, rather than the announcement's exact minutes, for a complete replay
campaign including fresh-state startup; the Regtest comparison can also take
hours because each sample rebuilds its process/state setup.

Reserve about **2 TiB free beyond the existing source archive** for build trees,
separate archive prefixes, temporary compactions and active clones. This is a
conservative unmeasured allowance, not a measured requirement. Reflinks save
initial space but diverging writes and compactions consume real extents. A
single successful replay clone is active at a time; failed clones remain.
An optional extra revision needs another build and compatible prefix.

The 32,860 raw bodies have a protocol upper bound near 65.72 GB; hex encoding
alone can use about 131.44 GB (122.4 GiB), plus metadata and filesystem overhead.
The actual corpus size is `corpus.json.raw_bytes`, not this upper bound. Record
`df -h` and `du -sh` before expensive preparation. Do not delete shared archives,
other campaigns or unrelated caches to make space.

Report exact source hashes, binary hashes, compiler settings, storage mode,
start/end hashes, CPU mask, run order, failures, and whether host exclusivity
was obtained. Loopback Regtest does not measure Internet propagation, orphan
rates, or mining revenue. The replay excludes P2P and normal mempool reuse, so
it cannot establish those properties either.

## 8. Source compatibility: v1.7.0 → v1.8.1

The comparison is between `c572f8faf88e58dcaefd80ec8e4ed05f2d8643b6` and
`f5c20398c629c8179364b07f059da3c814af78ec`, not whatever is checked out locally.
Sources below are pinned permalinks. The [tagged changelog][zakura-changelog]
contains the complete release entries; the harness-relevant differences are:

| Surface | Finding and harness consequence |
|---|---|
| TOML schema and renamed keys | Existing harness keys for state cache, RPC listener/authentication, metrics endpoint, checkpoint sync and nested Regtest parameters remain. The generated config diff adds only `network.advert_window_min=32`, `advert_window_max=64`, `advert_window_small_size_bytes=102400`, and `advert_window_full_size_bytes=1572864` in 1.8.1. No harness-used TOML key was renamed in this interval. See [defaults][zakura-defaults] and [network config][zakura-network-config]. |
| Existing cross-project names | Zakura uses `zakurad`, package `zakura`, config `zakura.toml`, and `ZAKURA_SECTION__KEY`. Deprecated `ZEBRA_` environment/config-name fallback already existed in 1.7.0; it is not a new 1.8 migration. Regtest remains `[network.network]` / `[network.network.params]`, unlike Zebra's schema. Zakura storage uses `storage_mode` / `storage_mode.pruned.tx_retention`, not Zebra's `prune_blocks_older_than`. See [CLI][zakura-cli], [defaults][zakura-defaults], and [state config][zakura-state-config]. |
| CLI / JSON-RPC | `generate -o FILE`, `-c FILE start`, and the harness's `getblocktemplate`, `submitblock`, `sendrawtransaction`, `getblockchaininfo`, `getbestblockhash`, `getrawmempool`, `getpeerinfo`, `getblockhash`, `getblock` names remain. `generate` CLI writes config; `generate` RPC creates Regtest blocks. Zakura does not have Zebra's `copy-state` command. No harness RPC method was renamed. `getpeerinfo` adds `startingheight`; GBT's Mainnet stale-tip rejection now uses a fixed six-hour age and `-10`. `check_synced_to_tip` returns early for test networks, including Regtest: cached Regtest fixtures need no timestamp rebasing for this guard. Mainnet replay uses `submitblock`, not GBT, so this guard does not reject its historical blocks either. Pruned `z_gettreestate` can serve below the prune height. See [RPC][zakura-rpc], [GBT sync guard][zakura-gbt], and [changelog][zakura-changelog]. |
| Tracing | `tracing.filter` now accepts only `level`, bare `target`, and `target=level`. Span/field directives are warned about and ignored; a filter with no usable directives falls back to ERROR. INFO no longer has per-transaction commit spans, and `log`-crate records are not forwarded. Keep `info` enabled for runtime endpoint discovery; use `zakurad`, `zakura_consensus`, `zakura_state`, etc. for Zakura targets, not Zebra targets. Release INFO ceilings cannot be overridden at runtime. See [filter parser][zakura-tracing]. |
| Metrics names / format | The in-tree exporter replaces `metrics-exporter-prometheus`; `[metrics].endpoint_addr`, `zakura_build_info`, sanitized underscore names and histogram-as-summary `_sum`/`_count` remain. Quantiles now include every sample in the last minute within 0.1%; cumulative totals are distinct from that recent window. `/health` returns `OK`. Do not infer phase-metric availability merely from the exporter: `commit-metrics` remains a separate feature. See [exporter][zakura-metrics], [renderer][zakura-metric-text], and [features][zakura-features]. |
| Additional metrics | New raw names: `sync.block.applying.buffered_bytes`, `sync.block.sequencer_input.queued_bytes`, `sync.block.submission.in_flight_bytes`, `sync.block.work.reserved_above_floor_bytes`, `sync.block.work.reserved_bytes`. `sync.block.budget.reserved_bytes` and `sync.block.reorder.buffered_bytes` now update with their owners instead of reactor publication. Prometheus replaces dots with underscores; these are additions/timing changes, not renames of existing harness metrics. See [changelog][zakura-changelog]. |
| P2P stack and trace keys | Explicit `p2p_stack="legacy"` still selects TCP. `"zakura"` and `"dual"` select experimental native P2Pv2 paths; `"default"` means legacy on Mainnet and dual elsewhere. Do not replace legacy Regtest with the default. Native trace `closed.neutral` reasons `ordered_read_error` / `request_read_error` split into `_rejected`, `_timeout`, `_transport`; ordered oversize/rate limiting use `ordered_oversize` / `ordered_rate_limited`. Those are trace-reason changes, not TOML or RPC renames. See [defaults][zakura-defaults] and [changelog][zakura-changelog]. |
| Relay and admission workload | Legacy announcements/downloads no longer queue behind unrelated `getblocks`, earlier broadcasts, or empty-mempool inventory timeouts; idle peer sets refresh/retry, missing parents are fetched after PoW checks, and local admission loses its extra 50 ms delay. 1.8.1 selects an immediately usable advertising peer and size-dependent fanout. These directly affect fresh/settled peer-work and burst rows; old 1.7 timings cannot be reused. See [changelog][zakura-changelog]. |
| Validation and startup | 1.8 adds Common 5.0.0 crypto, transaction-memory/layout reductions, less repeated hashing/parsing/logging, concurrent parent reads and CPU-worker proof preparation. 1.8.1 starts script checks sooner, avoids shielded-only/pre-v5 hash work, reuses parent UTXOs, reduces address-output copying and preloads Halo2 keys at startup. The harness's existing preload gate must complete before short timings; do not compare a cold key build against an already-preloaded arm. Pruned checkpoint-sync nodes also skip non-finalized transparent index maintenance; this runbook uses archives. See [changelog][zakura-changelog] and [router][zakura-router]. |
| Native sync and sidecar | Native sync bounds decoded submission bodies to its 512 MiB window, fixes reset/reorg/discovery/checkpoint-boundary stalls and handshake/frame limits. Recent Mainnet downloads have a minimum-work policy, with the historical exception limited to claimed height ≤3,535,411 while the committed tip is below it. Custom-testnet version compatibility is fixed. Managed sidecar pins zcashd v1.3.1. These do not run in isolated RPC replay; do not claim replay evaluates them. See [changelog][zakura-changelog]. |
| Builds, assets, support | RocksDB 0.24.4 uses committed bindings and omits unused bundled features, without changing Zakura's on-disk format. Default x86-64 builds enable Pasta assembly with a startup BMI2/ADX check; `portable` overrides it. `indexer-rpc` becomes optional but stays default. Assets change from `0.3524060.0` to `0.3538611.0`. Embedded version depends on build metadata; record `--version`. The release supports Mainnet through height 3,643,309, above this window. See [manifest][zakura-manifest], [features][zakura-features], [build metadata][zakura-build-metadata], and [changelog][zakura-changelog]. |

[announcement]: https://zakura.com/announcements/zakura-1-8-0/
[zebra-manifest]: https://github.com/ZcashFoundation/zebra/blob/5527c563929fbe3b9e82d099b4d4cfc19b092e20/Cargo.toml
[zakura-manifest]: https://github.com/zakura-core/zakura/blob/f5c20398c629c8179364b07f059da3c814af78ec/Cargo.toml
[copy-stack]: https://github.com/ZcashFoundation/zebra/blob/5527c563929fbe3b9e82d099b4d4cfc19b092e20/zebrad/src/commands/copy_state.rs
[copy-next]: https://github.com/ZcashFoundation/zebra/blob/7ae1a827471235e8d14204e2aaa77dc9b61ca25c/zebrad/src/commands/copy_state.rs
[zebra-codec]: https://github.com/ZcashFoundation/zebra/blob/5527c563929fbe3b9e82d099b4d4cfc19b092e20/zebra-state/src/service/finalized_state/disk_format/chain.rs
[zakura-codec]: https://github.com/zakura-core/zakura/blob/f5c20398c629c8179364b07f059da3c814af78ec/crates/zakura-state/src/service/finalized_state/disk_format/chain.rs
[zakura-upgrades]: https://github.com/zakura-core/zakura/blob/f5c20398c629c8179364b07f059da3c814af78ec/crates/zakura-state/src/service/finalized_state/disk_format/upgrade.rs
[zakura-format]: https://github.com/zakura-core/zakura/blob/f5c20398c629c8179364b07f059da3c814af78ec/crates/zakura-state/src/constants.rs
[zebra-router]: https://github.com/ZcashFoundation/zebra/blob/5527c563929fbe3b9e82d099b4d4cfc19b092e20/zebra-consensus/src/router.rs
[zakura-router]: https://github.com/zakura-core/zakura/blob/f5c20398c629c8179364b07f059da3c814af78ec/crates/zakura-consensus/src/router.rs
[zebra-rpc]: https://github.com/ZcashFoundation/zebra/blob/5527c563929fbe3b9e82d099b4d4cfc19b092e20/zebra-rpc/src/methods.rs
[zakura-rpc]: https://github.com/zakura-core/zakura/blob/f5c20398c629c8179364b07f059da3c814af78ec/crates/zakura-rpc/src/methods.rs
[zakura-changelog]: https://github.com/zakura-core/zakura/blob/f5c20398c629c8179364b07f059da3c814af78ec/CHANGELOG.md#181---2026-11-03
[zakura-defaults]: https://github.com/zakura-core/zakura/blob/f5c20398c629c8179364b07f059da3c814af78ec/crates/zakurad/src/commands/default_config.toml
[zakura-network-config]: https://github.com/zakura-core/zakura/blob/f5c20398c629c8179364b07f059da3c814af78ec/crates/zakura-network/src/config.rs
[zakura-cli]: https://github.com/zakura-core/zakura/blob/f5c20398c629c8179364b07f059da3c814af78ec/crates/zakurad/src/commands.rs
[zakura-state-config]: https://github.com/zakura-core/zakura/blob/f5c20398c629c8179364b07f059da3c814af78ec/crates/zakura-state/src/config.rs
[zakura-tracing]: https://github.com/zakura-core/zakura/blob/f5c20398c629c8179364b07f059da3c814af78ec/crates/zakurad/src/components/tracing/component.rs
[zakura-metrics]: https://github.com/zakura-core/zakura/blob/f5c20398c629c8179364b07f059da3c814af78ec/crates/zakurad/src/components/metrics.rs
[zakura-metric-text]: https://github.com/zakura-core/zakura/blob/f5c20398c629c8179364b07f059da3c814af78ec/crates/zakurad/src/components/metrics/prometheus/text.rs
[zakura-features]: https://github.com/zakura-core/zakura/blob/f5c20398c629c8179364b07f059da3c814af78ec/crates/zakurad/Cargo.toml
[zakura-build-metadata]: https://github.com/zakura-core/zakura/blob/f5c20398c629c8179364b07f059da3c814af78ec/crates/zakurad/build/metadata.rs
[zakura-coinbase]: https://github.com/zakura-core/zakura/blob/f5c20398c629c8179364b07f059da3c814af78ec/crates/zakura-rpc/src/methods/types/transaction.rs
[zakura-gbt]: https://github.com/zakura-core/zakura/blob/f5c20398c629c8179364b07f059da3c814af78ec/crates/zakura-rpc/src/methods/types/get_block_template.rs
