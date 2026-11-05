# Zebra benchmarks

Source-only harness for the published Zebra mining and performance measurements. The code reproduces workloads and analysis, not the original machine, randomized fixture bytes or live arrivals. No binaries, node databases, wallet data or historical run directories are included.

For the Zakura v1.8.1 rerun and new Mainnet replay, follow the [v1.8.1 runbook](zakura-1.8.1-RUNBOOK.md). Historical Zakura results below remain v1.7.0 measurements, not new v1.8.1 measurements.

The measurements are distinct: Regtest mining paths; RPC and full-verification replay against Mainnet snapshots; checkpoint sync from local peers; live-network block submission with optional mempool mirroring; and Mainnet storage pruning. Regtest loopback results do not measure Internet propagation, orphan rates or fee revenue.

| Directory | Purpose |
|---|---|
| `regtest/` | Final V5 campaign, fixture generation, node runtime, RPC and replay |
| `initial-sync/` | Mainnet snapshot preparation, checkpoint sync and phase attribution |
| `pruning/` | Download, extract, prune, compact and paired replay |
| `pruning/snapshot-production/` | Separate, later snapshot-refresh workflow |
| `live-comparison/` | Concurrent live block dispatcher and current plain dashboard |
| `figures/` | V5 receipt validation and figures; bundled EB Garamond fonts |

The harness is licensed **MIT OR Apache-2.0**. Font files retain SIL OFL 1.1; public block test vectors retain their included MIT notice.

## Requirements and environment

Use Linux and Python 3.11 or newer. The recorded final sync ran Python 3.14. The harness uses `/proc`, process affinity, `taskset`, loopback ports and POSIX signals. Snapshot workflows require a reflink-capable filesystem and deliberately use `cp --reflink=always`, with no full-copy fallback. Install GNU coreutils, Bash, curl, zstd and tar. Fixture generation needs rootless Podman with host networking; signed fixture generation additionally needs `cryptography >=43` with deterministic secp256k1 ECDSA. Plotting needs matplotlib and numpy. No JavaScript build is required for the dashboard; its interactive plot loads Plotly from a CDN.

Node builds need Rust **1.97.0** and a C/C++ toolchain, plus each pinned node's native prerequisites. Zebra's RocksDB bindings still require libclang/bindgen; Zakura v1.8.1 uses committed bindings and no longer needs libclang. On x86-64, the assembly arms require BMI2 and ADX. The two pruning compactors need RocksDB 0.24.0 / librocksdb-sys 0.17.3+10.4.2 with default features disabled and `bindgen-runtime,lz4`.

The principal measurements used an AMD Ryzen 9 9950X3D: 16 physical cores, 32 logical CPUs. Measured work ran on CPUs `0-7,16-23`, sharing 96 MB L3; replay sources ran on `8-15,24-31`, on the other CCD. The pruning storage measurement ran on a separate machine. Neither is a minimum requirement.

Run measurements on an otherwise idle host. The quiet gate requires three consecutive one-minute intervals with disk busy below 1%, writes below 20 MiB/minute and one-minute load below 2; it stops waiting after two hours. Choose the actual block device, not a mount path. Never infer CPU topology from CPU numbering on another machine.

All commands below start at the repository root in Bash. Choose locations on a filesystem with sufficient free space:

```bash
export HARNESS="$PWD"
export WORK="$PWD/work"
export BENCH_BINS="$PWD/bins"
export BENCH_FIXTURES="$PWD/fixtures/standard"
export BENCH_SIGNED_FIXTURES="$PWD/fixtures/signed"
export BENCH_MAINNET="$PWD/snapshots"
export BENCH_OUTPUT="$PWD/out"
# Published host placement; replace these masks on another machine.
export V5_CPUS=0-7,16-23
export SOURCE_CPUS=8-15,24-31
export QUIET_DEVICE=nvme0n1
mkdir -p "$WORK" "$BENCH_BINS" "$BENCH_OUTPUT" "$BENCH_MAINNET"
```

Without these overrides, Regtest path defaults are relative to the repository root:

| Setting | Default |
|---|---|
| `BENCH_BINS` | `bins/` |
| `BENCH_FIXTURES` | `fixtures/regtest/` |
| `BENCH_SIGNED_FIXTURES` | `fixtures/transparent-signed/` |
| `BENCH_MAINNET` | `mainnet/` |
| `BENCH_OUTPUT` | `output/` |
| `V5_CPUS`, `SOURCE_CPUS` | Unset: inherit process affinity |
| `BENCH_PORT_BASE` | `0`: ephemeral loopback RPC/P2P/wallet/phase-metrics ports |
| `BENCH_SOURCE_PORT_BASE` | `31233`: consecutive replay-source P2P ports |
| `QUIET_DEVICE` | No default: explicitly select a whole block device |

Relative environment overrides resolve from the invoking directory; CLI paths override defaults. For individual probes, use `taskset -c LIST`. A nonzero port base reserves consecutive ports for one driver; do not share that range with another concurrent run. The campaign driver remains unpinned so replay sources can use the separately selected CPUs.

Keep generated outputs outside version control. Generated receipts can contain local paths, system inventories and RPC configuration; review them before publishing. Use dedicated test-only wallets, keep RPC on loopback or a trusted isolated network, and never fund the signed fixture builder's deliberately public key on Mainnet.

## Historical V5 build layout

The Zebra pins below belong to the historical V5 campaign; the runnable Zakura recipe has been updated to v1.8.1. Running this combination creates new results, not a reproduction of the recorded v1.7.0 measurements. For the current three-arm stack/crypto/Zakura comparison, see the [v1.8.1 runbook](zakura-1.8.1-RUNBOOK.md): its Zebra stack is `1c60fe3f00` from the public `bench/zakura-1.8.1-stack` branch, with an explicit common compiler environment.

Use separate clean checkouts and each commit's own lockfile. The builder wrappers write a SHA256/source/build receipt beside each executable, including `compiler_settings` with `rustc -vV` and the selected build environment (unset variables are JSON `null`); the campaign refuses missing or mismatched binary receipts. Matching unset variables alone does not establish compiler-profile parity across projects.

```bash
git clone https://github.com/ZcashFoundation/zebra.git "$WORK/zebra-reference"
git -C "$WORK/zebra-reference" checkout --detach e8db87eb69254e0d4eedd058eadd48bdfa09065a
git clone https://github.com/ZcashFoundation/zebra.git "$WORK/zebra-optimized"
git -C "$WORK/zebra-optimized" checkout --detach 89390f741931b5238a8ef94a86810d89b3320f1b
git clone https://github.com/zakura-core/zakura.git "$WORK/zakura-reference"
git -C "$WORK/zakura-reference" checkout --detach f5c20398c629c8179364b07f059da3c814af78ec
(cd "$WORK/zebra-reference" && bash "$HARNESS/regtest/build-arm.sh" zebrad-reference)
(cd "$WORK/zebra-optimized" && bash "$HARNESS/regtest/build-arm.sh" zebrad-optimized)
(cd "$WORK/zebra-optimized" && ZEBRA_EXTRA_FEATURES=pasta-asm bash "$HARNESS/regtest/build-arm.sh" zebrad-assembly)
(cd "$WORK/zakura-reference" && bash "$HARNESS/regtest/build-zakura.sh" zakurad-reference)
```

The Zebra recipe is `RUSTUP_TOOLCHAIN=1.97.0 cargo build --locked --release -p zebrad --features default-release-binaries,internal-miner`, adding `pasta-asm` only for the assembly arm. The Zakura recipe is `RUSTUP_TOOLCHAIN=1.97.0 cargo build --locked --release -p zakura --bin zakurad --features default-release-binaries`.

Zakura v1.8.1's `default-release-binaries` now includes `x86_64-asm` and `indexer-rpc` as well as Prometheus and the release INFO log ceiling. Its default x86-64 build requires BMI2 and ADX; do not silently substitute a `portable` build in the same campaign.

| Campaign label | Source commit | Meaning |
|---|---|---|
| `release` | `e8db87eb69254e0d4eedd058eadd48bdfa09065a` | Zebra v7.0.0 |
| `stack` | `89390f741931b5238a8ef94a86810d89b3320f1b` | Optimized Zebra, including upstream cryptography forks |
| `crypto` | Same optimized commit | Same source plus the opt-in Pasta assembly backend |
| `zakura-main` | `f5c20398c629c8179364b07f059da3c814af78ec` | Zakura v1.8.1 reference for new runs |

The historical optimized arms contain these public pins; their comparison isolates assembly, not forks versus upstream. These are not the dependency revisions of the new `1c60fe3f00` stack:

| Dependency | Public repository | Revision |
|---|---|---|
| RedDSA | [ZcashFoundation/reddsa](https://github.com/ZcashFoundation/reddsa) | `da27c7d7daf9d4b5d13347f57a4a6fd0c8b69c91` |
| Equihash | [zcash/librustzcash](https://github.com/zcash/librustzcash) | `ea8ff760cb5b21ac5d244dfb1d2f1b4e277c22ea` |
| Encoding, primitives, transparent | [upbqdn/librustzcash](https://github.com/upbqdn/librustzcash) | `dd3a49af26f2a2f0704b8cd1a6311accbac2984c` |
| Pasta curves | [upbqdn/pasta_curves](https://github.com/upbqdn/pasta_curves) | `0ba0cf2cdc00f259ffe6bef73d8fdc3b51507c0c` |
| Halo 2 proofs, gadgets, Poseidon | [upbqdn/halo2](https://github.com/upbqdn/halo2) | `b27151aaf703f2f6de76e64e0d9b88288a1ba09d` |
| Sinsemilla | [upbqdn/sinsemilla](https://github.com/upbqdn/sinsemilla) | `f4b18727394db9478bb40da3e8347824d7c152c1` |

Use the exact Zakura commit, not an arbitrary local tag. The v1.8.1 manifest pins `zakura-assets = 0.3538611.0`. Its embedded version is derived from build-time Git tags/metadata, falling back to the package version; record the actual `--version` output alongside the source/SHA256 receipt rather than inferring it from the release label.

Zakura Regtest retains `[network.network]` / `[network.network.params]` and requires explicit `p2p_stack = "legacy"` for these comparisons; its default is dual-stack outside Mainnet. The fixtures activate upgrades only through NU5. Zakura's coinbase builder rejects Sapling payouts and pays Orchard receivers through Ironwood only from NU6.3, so neither capability supplies a shielded-payout cell under these fixtures.

Do not open a Zebra v29 cache with Zakura: their persisted `BlockInfo` layouts differ despite overlapping format-version numbers. Prepare an independent Zakura state. The harness-used CLI/RPC names and TOML keys remain supported from v1.7.0 to v1.8.1, but that is not a claim of complete cross-project configuration compatibility.

Tracing accepts levels, bare targets and `target=level`; v1.8.1 warns and ignores span/field directives. Keep `info` enabled for startup endpoint discovery. The in-tree Prometheus exporter preserves `[metrics].endpoint_addr`, `zakura_build_info`, underscore-normalized names and cumulative summary `_count`/`_sum`; its recent-window quantiles have changed. The [source compatibility table](zakura-1.8.1-RUNBOOK.md#8-source-compatibility-v170--v181) documents these changes, added keys/metrics, P2P behavior, and pinned source evidence.

Historical results below remain v1.7.0 measurements at `c572f8faf88e58dcaefd80ec8e4ed05f2d8643b6`, with recorded embedded version `zakurad 1.0.3-rc0+758.gc572f8faf` and `zakura-assets = 0.3524060.0`. They are not v1.8.1 results, and their executable hashes are not the expected hashes for the new build.

The recorded reference, assembly and Zakura executable SHA256 values were respectively `8bf211fb912775a769dc383e37b9114b8380c01fa56f6e8839f1483eacca32a7`, `f43267d0e7d9d084b63eb7af008091d13d42fd14b4b02a21b8691da0849f001f` and `9ed3b1cee8b958e7fadaddf604a7644aa3b6456b30fe240ea2c675e4eff8372a`. The default optimized arm's final hash was not recovered. Matching source and recipe does not promise bit-identical binaries: native library/linker versions and inherited build flags were not fully recorded.

## Generate Regtest fixtures

The pinned generator image is zcashd 6.12.5:

```text
docker.io/electriccoinco/zcashd@sha256:558f41b7b8b15e1b408247914ba9ed7b9d3c62f1061823bb811b8b24a693f349
```

Generate the core sizes and the independently signed transparent corpus:

```bash
python3 -B regtest/sweep/build_fixtures.py \
  --producer "$BENCH_BINS/zebrad-reference" \
  --sizes 2000 250000 1900000 --output "$BENCH_FIXTURES"
python3 -B regtest/sweep/build_fixtures.py \
  --producer "$BENCH_BINS/zebrad-reference" --transparent-signed \
  --sizes 2000 250000 1900000 --output "$BENCH_SIGNED_FIXTURES"
```

The builder matures coinbases, funds independent transparent outputs and Orchard accounts, generates real Orchard proofs, and selects transaction prefixes near each byte budget. Unsigned transparent transactions spend OP_TRUE P2SH outputs; signed transactions use V4 ZIP-243 and the NU5 branch ID. Mixed blocks divide transaction bytes approximately equally between transparent and Orchard. The empty candidate is not padded: the retained example was 1,628 bytes. Manifest and SHA256 seals cover the generated chain, corpus and candidates.

The recipe is deterministic for fixed inputs, but wallet keys, proofs, timestamps and outpoints vary between fresh generations. Deterministic signatures do not make the entire corpus byte-identical. Use `--resume-sealed-corpus` only when intentionally reusing already sealed inputs; it is unnecessary for a fresh directory. For the optional extended size sweep, generate `2000 25000 100000 250000 500000 1000000 1500000 1900000` instead.

## Run the final campaign

Define this shell function once; the result tables below use literal calls to it:

```bash
C() {
  bash "$HARNESS/regtest/v5-campaign.sh" --profile core \
    --output "$BENCH_OUTPUT/v5" "$@" \
    release:zebrad-reference stack:zebrad-optimized \
    crypto:zebrad-assembly zakura-main:zakurad-reference
}
C --dry-run
C --piece smoke
# Run each remaining piece once, or use C without --piece in a fresh output.
C --list
```

A complete run includes Mainnet snapshot workloads, so prepare those inputs first. The runner refuses to overwrite a completed piece and requires a successful smoke piece before timing. For a complete fresh campaign, use `C` directly, without first running smoke separately. For a resumed campaign, invoke only unfinished names from `C --list`; do not merge reruns into the same cells.

Core Regtest cells have five samples at 250 kB and 1.9 MB; RPC, inclusion and replay have three repetitions, startup five. Short post-ready tests wait for supported key preload. Startup and sync deliberately include startup costs. “Cold” means no candidate mempool/proposal verdict, not cold filesystem caches. Direct relay's fresh-connection panel is the historical pre-NODELAY experiment.

### Served-template submission comparison

The `comparison` profile keeps `submitblock-latency` unchanged: its cold and
prepared cells time fixture-candidate submission, with an explicit proposal check
only in the prepared cell. The separate `served-submit` piece runs three commands
against `$BENCH_SIGNED_FIXTURES/transparent-signed-1900000`. Each uses the same
declared arms/provenance, one round, three repetitions, seed `20261104` and
1,200-second timeout as `submitblock-latency`:

| Output under `served-submit/` | Endpoint | Solve delay | Rebuild observation |
| --- | --- | --- | --- |
| `served` | `served-submitblock` | 1 s | None |
| `rebuild-0ms` | `served-rebuild-submitblock` | 0 s | 2 s |
| `rebuild-50ms` | `served-rebuild-submitblock` | 0.05 s | 2 s |

The piece's `summary.json` reads all three command outputs and reports separate
`cells` with endpoint and `solve_delay_seconds` metadata. Each cell requires three
successful samples per arm with matching delay metadata; the two rebuild delays
are never pooled.

```bash
bash "$HARNESS/regtest/v5-campaign.sh" --profile comparison \
  --piece served-submit --dry-run --output "$BENCH_OUTPUT/v5-comparison" \
  stack:zebrad-optimized crypto:zebrad-assembly zakura-main:zakurad-reference
```

The original endpoint admits the full corpus and waits for an exact template.
That initial serving mostly warms all arms; it does **not** by itself isolate
the serving-half cache benefit. The rebuilt case targets that benefit: admit
`corpus[:-1]`, seed exact template **A**, then admit the last transaction. Poll
ordinary `getblocktemplate` and take the **first RPC-visible replacement**, based
on changed longpoll ID or work (including roots, coinbase and transaction order),
ignoring a `curtime`-only roll. It does not require the replacement to contain
the final transaction or wait for another exact full-corpus match. If no switch
is observed within two seconds, keep A. The observation window is bounded by
`min(timeout, rebuild_observation_seconds)`.
The rebuild endpoint requires a nonempty corpus; default matrices omit it for
coinbase-only fixtures.

Both endpoints mine a block from the selected node's **served template bytes**,
including its coinbase and transaction ordering, rather than reusing the fixture
candidate. Neither issues an explicit proposal RPC. For direct sweep invocations,
`--rebuild-observation-seconds` defaults to `2` and accepts finite positive values;
`--solve-delay-seconds` defaults to `1` and accepts finite nonnegative values,
including `0` and `0.05`. Admission, template matching/observation, delay and mining
are outside the timed `submitblock` request/reply.

Receipts retain the served block's hash, SHA256 and byte size,
`solve_delay_seconds`, `template_ready_ns`, `template_longpollid`,
`pre_submit_longpollid` and `template_rebuilt_before_submit`. Rebuild receipts also
label `served_template_id` as `"A"` or `"rebuilt"` and record
`final_admission_start_ns`, `final_admission_end_ns`,
`template_switch_latency_ns` (from final admission completion; null if no switch)
and `observation_elapsed_ns`. `template_ready_ns` marks selection of the template
to mine. These monotonic-clock observations are separate from submission latency.

`template_rebuilt_before_submit` compares the **selected** template with a final
non-longpoll `getblocktemplate` immediately before submission: changed longpoll
IDs or work fields (version, parent, roots, time, bits, coinbase or ordered
transaction bytes) count as an observed further rebuild. This flag is distinct
from the initial A-to-rebuilt selection; byte-identical internal rebuilds are not
observable. This is a controlled Regtest workload, not a measurement of
real-network solve-time distributions.

Mining keeps the template's commitments and changes only the 32-byte nonce. It
uses a CompactSize-prefixed 36-byte zero Equihash solution and searches SHA256d
header hashes against the target encoded by the served `bits`. Both Zebra
Regtest and Zakura v1.8.1 Regtest disable Equihash and hash-to-target verification;
the nonce search still meets the served target, but this is not a production
Equihash solver. A zero-length solution is not accepted.

### Sustained mempool churn comparison

The separate `comparison --piece sustained-churn` runs 20 and 100 tx/s as
independent summary cells, each for 60 seconds after 20 seconds of settling,
with one randomized/interleaved round and three fresh-process repetitions per
arm. Supply the required `stack:zebrad-stack`, `crypto:zebrad-crypto` and
`zakura-main:zakurad-1.8.1` arms (and any declared `extra-NAME` arms).
The signed 1.9 MB fixture contains more than the stress cell's 6,000 transactions.
Each run checks corpus size, unique inputs, absence of transaction dependencies,
mempool cost and single-template capacity before admission; it never recycles inputs.
Direct endpoint flags are `--churn-rate` (20), `--churn-seconds` (60) and
`--churn-settle-seconds` (20). Churn polling is fixed at 100 ms.

Admissions use 16 independent persistent RPC clients, paced against absolute
deadlines rather than RPC completion. The queue is bounded to 32 outstanding
requests; skipped slots, RPC failures, request-start lag and the actual
acknowledged admissions per wall-second expose generator/server saturation.
Responses finishing after the window are recorded but excluded from that rate.
There are no measured `submitblock` or explicit proposal calls.

Receipts and `sustained-churn/summary.json` retain:

- Freshness median/p90/max: admission RPC response to first ordinary served GBT
  response containing each txid (zero if inclusion preceded the acknowledgement).
  Polling continues for up to `min(timeout, 60)` seconds after admissions finish;
  this tail is excluded from cost metrics. Missing inclusions remain explicitly
  censored via `freshness_unobserved`; percentiles describe observed inclusions.
- Rebuild count/builds per second: observed longpoll-ID or work transitions
  during the timed window, ignoring `curtime`-only changes. The initial template
  is not a rebuild. This is a poll-resolution lower bound, not an internal build
  counter: intermediate or byte-identical builds are unobservable.
- GBT and concurrent `getblockcount` response latency median/p99 for polls
  completed during churn, including HTTP/JSON costs. Raw timings retain sample
  counts and expose missed polling slots; observers never replay missed slots.
- Node CPU-seconds (`utime + stime` from `/proc/PID/stat`) divided by measured
  window wall-seconds. Peak RSS is Linux `VmHWM` at the window endpoint, reset
  with `/proc/PID/clear_refs=5` immediately before the window so replay is excluded.
- Admission failures and generator misses, separately, including details.

Summary cells report median/min/max of three **process-level statistics**, not
pooled transaction/RPC percentiles; raw per-transaction observations remain in
the sweep receipts. These cost metrics include the real node work induced by
the two observers. No benchmark is launched by the planner's `--dry-run`.

### Headline results → commands

Values below identify the published measurements, not expected assertions on another machine. Three-way values are optimized Zebra / Zebra v7.0.0 / Zakura.

| Published result | Timed boundary | Reproduction command |
|---|---|---|
| Transparent 1.9 MB burst: 4.1 s / 8.5 min / 9.9 min | First transaction send to exact template | `C --piece sweep-transparent-admission-r1` |
| Peer empty block: 1.99 ms / 12.1 ms / 2.6 s | Submission at sender to first receiver template, established peer | `C --piece empty-tip-settled` |
| Prepared transparent submission: 25.0 / 264 / 99.2 ms | Exact proposal already checked; `submitblock` call to reply | `C --piece sweep-transparent-prepared-r1` |
| Cold Orchard submission: 73.2 / 434 / 102 ms | Unseen 1.9 MB candidate; full submission verification | `C --piece sweep-orchard-cold-r1` |
| Empty block relay: 2.64 / 12.7 / 1,114 ms | Submission at sender to established receiver best tip | `C --piece relay-settled` |
| Live Mainnet acceptance: 5.8 / 116.9 / 6.3 ms | Concurrent cold submission, matched live blocks | Not reproducible here; see exclusions below |
| Single-client RPC: 2,401 / 1,137 requests/s | Ten wallet/indexer methods, Mainnet state | `C --piece rpc` |
| Full replay: 6.68 / 2.56 blocks/s | Heights 1,750,000–1,752,000, full verification | `for r in 1 2 3; do C --piece replay-r$r; done` |
| Initial sync: 6,143 / 13,165 s | Genesis-only state to checkpoint 3,522,460; four local sources | `OUTPUT="$BENCH_OUTPUT/checkpoint-full" bash initial-sync/run-sync-full.sh`; then `OUTPUT="$BENCH_OUTPUT/checkpoint-optimized" bash initial-sync/run-sync-optimized.sh`, with inputs below |
| Pruning: 262.6 → 13.4 GiB | Apparent database bytes after deletion and forced compaction | `bash pruning/download.sh --output "$DATA"`; `bash pruning/extract.sh --output "$DATA"`; `bash pruning/prune.sh --output "$DATA" --binary "$ZEBRAD" --compactor "$COMPACTOR"` |
| Paired pruning: +0.067 ms during deletion, −0.006 ms after compaction | 1,000 same-build/same-block pairs, alternating order | After fetching blocks: `bash pruning/replay.sh --output "$DATA" --blocks 1000 --mode both --binary "$ZEBRAD" --compactor "$COMPACTOR"`; exact historical build incomplete |

For signed transparent, Orchard and mixed admission/prepared/cold tables, substitute `transparent-signed`, `orchard` or `mixed` in `C --piece sweep-FILLING-ENDPOINT-r1`; the endpoint token is `admission`, `prepared` or `cold`. Own-block empty-template latency uses `C --piece empty-tip-local`. First/full mixed templates and their difference use `C --piece tip-template-mixed-1900000-settled-warm`. The Orchard-payout table uses `C --piece tip-miner-mixed-1900000-orchard`. Two-hop results use `C --piece relay-chain-empty-2000-cold` and `C --piece relay-chain-mixed-1900000-cold`. Resource/startup tables use `C --piece inclusion`, `C --piece startup`, `C --piece rpc` and the three replay pieces. Inclusion sends 18,400 transparent transactions at 100 tx/s while probing templates every 100 ms; Zakura's 600-second timeout is retained as a failure, not a completed latency sample.

### Published figures → inputs

The renderer requires a complete campaign and matching success receipts, including resource categories not plotted in every image. It never substitutes legacy results. Generate all V5 images with the figure command below after the complete campaign.

| Published image | Producing pieces |
|---|---|
| `admission-to-template.png` | Four `sweep-FILLING-admission-r1` pieces |
| `prepared-submission.png` | Four `sweep-FILLING-prepared-r1` pieces |
| `cold-submission.png` | Four `sweep-FILLING-cold-r1` pieces |
| `empty-block-tip-template.png` | `empty-tip-local`, `empty-tip-settled` |
| `relay.png` | `relay-fresh`, `relay-settled` |
| `peer-block.png` | Three fixtures (`transparent-signed-1900000`, `orchard-1900000`, `mixed-1900000`) × three endpoints (`cold-submitblock-nominer`, `warm-mempool-submitblock`, `prepared-submitblock`), named `peer-block-FIXTURE-ENDPOINT` |
| `zebra-zakura-comparison.png` | Sweep pieces and both direct-relay pieces |
| `rpc-serving.png` | `rpc` |
| `live-mainnet.png` | Not regenerable here: historical controlled dashboard and records absent |
| `new-work-delay.png` | Not regenerable here: ZMQ run provenance/input absent; polling panel alone is a mathematical half-interval model |

Render the complete campaign:

```bash
uv run --no-project --with numpy --with matplotlib python -B figures/figures.py \
  --input "$BENCH_OUTPUT/v5" --output "$BENCH_OUTPUT/figures"
```

The output includes PNG and SVG versions. The V5-only renderer has no historical proof-phase input and no implicit fallback dataset.

## Mainnet snapshot prerequisites

RPC/startup require a stopped compatible archive cache at `snapshot-2316084`. Full replay additionally requires `snapshot-1750000`. These are cache roots containing the state layout, not paths directly to RocksDB. They cannot be generated from Regtest fixtures. Independently sync an archive node with a configured stop height and preserve the stopped cache with a reflink. Do not substitute a later state for RPC: UTXOs, indexes and database size differ.

A replay source must contain at least the measured end height plus 1,000 blocks for the non-finalized tail through finalization. Initial sync needs an archive source beyond 3,523,460 and a separately generated genesis-only state. The initial-sync preflight warms the filesystem; it is not a cold-machine test. The first arm supplies the four source nodes: the successful reference run used reference sources, while the successful optimized-only retry used optimized sources. Source executables were not held constant across those published runs.

No original Mainnet state, checksum inventory or sealed randomized fixture corpus is included. Independently reconstructed states reproduce the workload, not bit-identical historical inputs. Do not let incompatible binaries open the same cache; migrations and pruning can make older versions unable to read it.

### Prepare snapshots and run checkpoint sync

Successive height stops reuse one archive sync directory and create separate stopped snapshots. Choose a compatible binary and do not interrupt the final clean stop:

```bash
for height in 1750000 2316084 3523460; do
  STOP_AT=$height SNAPSHOT_OUTPUT="$BENCH_MAINNET/snapshot-$height" \
    CPUS="$SOURCE_CPUS" bash initial-sync/prepare-mainnet-snapshots.sh \
    "$BENCH_BINS/zebrad-reference" "$BENCH_OUTPUT/mainnet-sync"
done
python3 -B initial-sync/make-genesis-snapshot.py \
  "$BENCH_BINS/zebrad-reference" "$BENCH_MAINNET/snapshot-3523460" \
  "$BENCH_OUTPUT/genesis" --cpus "$V5_CPUS" --source-cpus "$SOURCE_CPUS"
python3 - <<'PY'
import json, os
from pathlib import Path
bins = Path(os.environ["BENCH_BINS"])
arms = {label: json.loads((bins / (binary + ".build.json")).read_text())
        for label, binary in (("v7.0.0", "zebrad-reference"),
                              ("optimized-asm", "zebrad-assembly"))}
(Path(os.environ["BENCH_OUTPUT"]) / "sync-arms.json").write_text(json.dumps(arms, indent=2) + "\n")
PY
export REFERENCE_BIN="$BENCH_BINS/zebrad-reference"
export OPTIMIZED_BIN="$BENCH_BINS/zebrad-assembly"
export PROVENANCE="$BENCH_OUTPUT/sync-arms.json"
export SNAPSHOT="$BENCH_OUTPUT/genesis/snapshot-0"
export SOURCE_STATE="$BENCH_MAINNET/snapshot-3523460"
export CPUS="$V5_CPUS"
OUTPUT="$BENCH_OUTPUT/checkpoint-full" bash initial-sync/run-sync-full.sh
OUTPUT="$BENCH_OUTPUT/checkpoint-optimized" bash initial-sync/run-sync-optimized.sh
unset PROVENANCE
```

Use the reference observation from the full run and the optimized observation from the optimized-only run for the published source-arm arrangement. The full wrapper also attempts its optimized arm; that observation is a distinct experiment with reference sources, not the published optimized retry.

Both wrappers default to checkpoint height 3,522,460, one repetition, four source nodes, 259,200 s timeout and source ports starting at 31233. Without explicit masks, their `CPUS` and `SOURCE_CPUS` values are `all`, meaning inherited process affinity. The shared runtime is byte-identical to the sync campaign's dependencies; its only original replay-file difference was the source-port base, now configurable with the sync campaign's 31233 default.

Snapshot preparation defaults to RPC port 28232, P2P port 28233 and 60 s sampling. Without `STOP_AT`, it follows Mainnet until interruption or a `STOP` file in its run directory. The genesis helper defaults to source port 31300 and 600 s timeout. All helpers reject existing output snapshots instead of replacing them.

### Supplemental phase attribution

The precommit wrapper uses eight repetitions of transparent, mixed and Orchard 1.9 MB candidates:

```bash
OPTIMIZED_BIN="$BENCH_BINS/zebrad-optimized" OUTPUT="$BENCH_OUTPUT/precommit-phase" \
  bash initial-sync/precommit-phase.sh
python3 -B initial-sync/phase-summary.py "$BENCH_OUTPUT"/precommit-phase/*/samples.jsonl
```

For a new commit-hashing comparison, explicitly supply `BASE_BIN` and `ONEHASH_BIN` to `initial-sync/commit-hashing-ab.sh`; it runs ten repetitions. For Linux perf profiling, invoke `initial-sync/commit-profile.sh BINARY NAME`; it runs three repetitions and needs a binary built with frame pointers, metrics and perf access. These wrappers accept `BENCH_FIXTURES`, `OUTPUT`, `CPUS`, `REPS` and optional matching-label `PROVENANCE`. Their default outputs are timestamped directories under `output/initial-sync/`. The retained historical attribution does not establish the missing comparison binaries' build identities.

## Pruning

The storage input is the public archive at `https://snapshots.zfnd.org/mainnet/20261026/zebrad-state-mainnet-v29-20261026.tar.zst`: 274,298,923,615 compressed bytes, SHA256 `5387d71e3aadcb7e98c2725c896e4e471ec151aeaeddbafee7388d1f14cc0763`. The sanitized manifest is unsigned. Its hash checks integrity, not publisher authenticity or independently validated history.

The actual stored finalized tip was 3,529,825, not the manifest's advertised best tip 3,530,825. A 10,000-block retention window gave floor 3,519,826. The storage receipt records 281,955,704,522 apparent bytes before and 14,422,291,846 after forced compaction, 9,435.15 s to the floor and 358.78 s compaction. Apparent bytes are file lengths, not uniquely freed physical space: reflink-shared archive extents remain allocated.

For a new method reproduction, build the earlier public pruning revision with an explicitly recorded recipe. This choice does not recover the historical binary's unrecorded build flags:

```bash
git clone https://github.com/ZcashFoundation/zebra.git "$WORK/zebra-pruning"
git -C "$WORK/zebra-pruning" checkout --detach 4d04ee163fdc51f3e689ca9572aaa7e0bfa3ad49
(cd "$WORK/zebra-pruning" && bash "$HARNESS/regtest/build-arm.sh" zebrad-pruning)
cargo +1.97.0 build --locked --release \
  --manifest-path pruning/compact/Cargo.toml --target-dir pruning/compact/target
```

Use disposable stopped state copies. Pruning is irreversible, and compaction writes extensively. The free-space guards require 1,200 GiB before extraction and 500 GiB before pruning/replay; `--minimum-free-gib` changes the guard, not the actual storage need.

```bash
DATA="$BENCH_OUTPUT/pruning"
ZEBRAD="$BENCH_BINS/zebrad-pruning"
COMPACTOR="$HARNESS/pruning/compact/target/release/prune-compact"
bash pruning/download.sh --output "$DATA"
bash pruning/extract.sh --output "$DATA"
bash pruning/prune.sh --output "$DATA" --binary "$ZEBRAD" --compactor "$COMPACTOR" --cpus "$V5_CPUS"
bash pruning/fetch-blocks.sh --output "$DATA" --blocks 1000 \
  --feeder-url http://127.0.0.1:18232 --feeder-cookie /path/to/runtime.cookie
bash pruning/replay.sh --output "$DATA" --blocks 1000 --mode both \
  --binary "$ZEBRAD" --compactor "$COMPACTOR" --cpus "$V5_CPUS"
python3 -B pruning/report.py --receipts "$DATA/receipts" --output "$DATA/summary.md"
```

Replace the feeder endpoint and authentication file with your own archive feeder. An optional `--feeder-auth-env VARIABLE` reads authentication from the named environment variable instead of a file; omit both auth options only for an intentionally unauthenticated local endpoint. No remote shell or deployment layout is needed. Fetch checks public raw-block hashes and chain continuity, using the recorded actual stored tip; for an independent fetch, explicitly supply `--start-height`.

The paired replay submits the same 1,000 blocks to archive and pruned copies of the same build, alternating order. The during-deletion sample covers the first minute, not all deletion work. The report uses matched-block bootstrap intervals, not independent-machine uncertainty. A confidence interval crossing zero does not establish non-inferiority. Default timeouts are 7,200 s for startup, 86,400 s for pruning, 120 s for RPC and 1 s polling. Omitted CPU selection inherits affinity.

Both compactors require an explicit `--allowed-root` and canonical containment of the database below it. Callers pass their disposable working root; do not widen it to a parent containing valuable databases. The historical measurement compactor and the later snapshot compactor are deliberately separate.

### Later snapshot production

This is a separate later workflow: sync a dedicated cache, stop cleanly, compact, archive, restore into scratch space, then compare stored tip/floor and file inventory. It did not produce the original 262.6 → 13.4 GiB experiment.

```bash
cargo +1.97.0 build --locked --release \
  --manifest-path pruning/snapshot-production/compact/Cargo.toml \
  --target-dir pruning/snapshot-production/compact/target
python3 -B pruning/snapshot-production/refresh.py \
  --binary "$BENCH_BINS/zebrad-pruning" \
  --commit 4d04ee163fdc51f3e689ca9572aaa7e0bfa3ad49 \
  --cache-dir "$BENCH_MAINNET/snapshot-production" \
  --output "$BENCH_OUTPUT/snapshot-production" \
  --compactor "$HARNESS/pruning/snapshot-production/compact/target/release/prune-compact" \
  --network mainnet
```

Use a compatible format-30 pruning binary and record its actual full source commit. Defaults are Mainnet, 43,200 s timeout, 30 s polling and compression threads equal to the available affinity CPUs. RPC/metrics/P2P ports are configurable; Mainnet defaults are 18999/19999/28233, Testnet 18998/19998/28234. Use distinct caches and ports for simultaneous workflows.

## Live comparison

The included dashboard is the current plain view, not the historical controlled-statistics implementation. A new live run cannot recreate historical arrivals or the published 286-block selection and confidence intervals. Use **one dedicated machine per contestant, another for the Mainnet feeder, and a separate dispatcher host**, all on a trusted private LAN. Give measured machines identical CPU, memory, storage, OS tuning, limits and initial block hash; record each binary commit and SHA-256. Do not co-locate contestants: CPU/disk contention and loopback versus LAN latency bias comparisons. The dispatcher timestamps each contestant's RPC dispatch and receipt on its **own monotonic clock**, so no cross-host wall-clock subtraction is needed. Nevertheless, host and network contention, feeder arrival and RPC latency still affect the result; keep clocks synchronized for logs and record offsets.

Each node needs an independent, already-synced, **stopped** compatible state copy. Stop the snapshot source cleanly before copying and check all initial `getbestblockhash` values match (height alone is insufficient). The historical live comparison used archive Zebra states and a pruned Zakura state. A public Zakura bootstrap catalog is available at `https://zakura.valargroup.dev/mainnet-pruned/snapshots.json`; independently syncing is the trust-minimizing alternative. Never run the bootstrap and isolated Zakura configurations against the same database concurrently. The dispatcher defaults to **block-only mode** (`measure_submit_paths = false`, `mirror_mempool = false`): it submits blocks without template/tip/P2P measurement watchers or mempool mirroring. Contestants must have no peers and empty mempools; only the feeder connects to Mainnet. Transaction mirroring is a separate `mirror_mempool` switch, independent of submit-path measurement. When observing peer milestones, each contestant connects to exactly one inbound passive P2P observer from the dispatcher; an empty contestant mempool is *not* a mirror-mode prerequisite.

Copy the examples **onto their respective hosts** and replace every `/path/to` path, `REPLACE_WITH_MAINNET_TRANSPARENT_ADDRESS` miner address and example LAN IP with your own values. The miner address is required when measuring `getblocktemplate`, but this harness does not mine. The Mainnet contestant configs bind P2P and cookie-authenticated RPC on `0.0.0.0`: firewall **both** ports to the dispatcher host only and block contestants' outbound P2P. Allow the feeder normal Mainnet P2P, but firewall its RPC port to the dispatcher only. HTTP cookie auth does not encrypt traffic: use an isolated trusted LAN or SSH tunnels for RPC; copy fresh cookies to the dispatcher over a secure channel after every restart, with mode 0600, and never publish them. Replace `0.0.0.0` with each host's private interface address when possible. The Zakura bootstrap config is for syncing, not for the isolated comparison.

Use the following host placement (IP addresses are **examples**, not defaults):

| Host | Mainnet config | Example address | Role |
|---|---|---|---|
| `feeder` | `feeder.toml` | `10.0.0.10` | Connected Mainnet source, not a contestant |
| `zebra-reference` | `zebra-reference.toml` | `10.0.0.11` | Measured reference |
| `zebra-candidate` | `zebra-candidate.toml` | `10.0.0.12` | Measured candidate |
| `zakura` | `zakura.toml` | `10.0.0.13` | Measured Zakura |
| `dispatcher` | `contestant.toml.example` | separate host | Source polling, mirroring, submission and timing |

On **each node host**, from a checkout of this repository, set `CONFIG` to that host's table entry and `NODE_BINARY` to its compatible **absolute** executable path. Edit the copied config's state, cookie, network binding and miner-address values before starting; each node needs its *own* state and log:

```bash
E=/srv/live-comparison
mkdir -p "$E"
cp "live-comparison/config/mainnet/$CONFIG" "$E/node.toml"
# Edit "$E/node.toml"; start only after its private Mainnet snapshot is ready.
"$NODE_BINARY" -c "$E/node.toml" start >"$E/node.log" 2>&1 &
printf '%s\n' "$!" >"$E/node.pid"
```

On the **dispatcher host**, copy each fresh cookie securely after its node starts. The following `scp` loop assumes SSH aliases named as in the table; adjust remote cookie paths to match the four edited node configs. The dispatcher config's four cookie paths must match these local files:

```bash
E="$BENCH_OUTPUT/live"
install -m 700 -d "$E/cookies"
for role in feeder zebra-reference zebra-candidate zakura; do
  scp "$role:/path/to/live-comparison/mainnet/$role/rpc/.cookie" "$E/cookies/$role.cookie"
done
chmod 600 "$E"/cookies/*.cookie
cp live-comparison/contestant.toml.example "$E/contestant.toml"
chmod 600 "$E/contestant.toml"
# Edit its LAN URLs, cookie paths, P2P addresses and exact binary versions.
# Confirm matching initial best-block hashes and RPC readiness; if measuring
# peer milestones, verify each contestant has only its inbound observer peer.
python3 -B live-comparison/dispatch.py "$E/contestant.toml" \
  --output "$E/records.jsonl" --duration 3600
python3 -B live-comparison/load_intervals.py "$E/load-intervals.json"
python3 -B live-comparison/report.py "$E/records.jsonl" --output "$E/site/reports-mainnet"
python3 -B live-comparison/dashboard.py "$E/records.jsonl" "$E/site" \
  --network Mainnet --config "$E/contestant.toml" --load-intervals "$E/load-intervals.json"
cp "$E/site/dashboard.html" "$E/site/index.html"
python3 -m http.server 8000 --bind 127.0.0.1 --directory "$E/site"
```

Stop each node on **its own host** with `kill -TERM "$(< /srv/live-comparison/node.pid)"` and wait for a clean shutdown before copying its state. Never reuse a live database. Do not configure `pid` in the dispatcher TOML for remote processes; `/proc` sampling is local-only.

The interval helper writes `[]` without input. Supply `--input intervals.json` to validate and merge closed Unix-millisecond pairs such as `[[1700000000000,1700000030000]]` for periods when other work ran on the host. The dashboard shades them, marks each latest block busy or quiet, and reports "quiet host" medians and speed-ups from blocks outside them (with 30 s padding). An interval file covers only blocks dispatched before it was written; later blocks, or all blocks when no file is given, show as unknown and never count as quiet. This is simpler than the historical controlled exclusion filter.

The example explicitly enables `mirror_mempool = true` and `measure_submit_paths = true`; neither is the default. Mirroring polls the feeder mempool every `mempool_poll_seconds` (0.25 s in the example) and relays new transactions over RPC independently of the submit-path switch. Submit-path measurement starts best-tip and template watchers and, when `p2p` is configured, one passive P2P observer per contestant to record block announcements/bodies alongside `submitblock`. A measured block's latencies use the dispatcher's **single monotonic clock**, relative to that contestant's own submission start. A dedicated sampler thread collects ten `getblockcount` round trips per contestant; its median is an RPC/network baseline, not a pre-submission delay or a precise per-block correction. Submission does not wait for observer or template readiness. Inspect missing observations, rejected submissions, feeder polling errors, catch-up blocks and mirror admission failures rather than treating them as fast measurements. For the original block-only comparison, set both switches to `false`, remove `p2p` entries and keep contestant mempools empty and peers absent.

Mainnet `getblocktemplate` may be gated by node sync-status heuristics even when a disconnected node has the correct tip. The observer is passive; its empty `getblocks` reply is not an immediate sync signal. Zebra's `obtain_tips` records length zero after a timeout with a connected peer, which can mark it close to tip. Verify template readiness, matching tips and the sole inbound peer on **each actual Mainnet build**; a local Regtest smoke check cannot establish that this works on Mainnet. No new Mainnet latency claim follows from this harness until a real run produces complete, matched records.

The remaining defaults match the retained experiment: block polling every 0.02 s, 10 s RPC timeout, resource samples every 30 s, 5 s template timeout and 2 s pause threshold. The dispatcher appends records and runs indefinitely unless a duration is given. Serve the generated dashboard at `/` and detailed reports at `/reports-mainnet/`; explicitly rerun generation to refresh them. For Testnet, use its separate configuration examples, databases and records, set `--network Testnet`, and publish its dashboard as `testnet.html`.

To exercise the opt-in measurement paths locally with the sealed Regtest fixtures generated above, run this from the repository root with an empty output directory and compatible binaries. The smoke harness **starts isolated Regtest nodes**; offline checks below do not run any nodes:

```bash
python3 -B live-comparison/smoke_regtest.py \
  --feeder "$BENCH_BINS/zebrad-reference" \
  --contestant zebra-reference="$BENCH_BINS/zebrad-reference" \
  --contestant zebra-candidate="$BENCH_BINS/zebrad-optimized" \
  --contestant zakura="$BENCH_BINS/zakurad-reference" \
  --fixture-dir "$BENCH_FIXTURES" --candidate mixed-1900000 \
  --rounds 1 --output "$BENCH_OUTPUT/live-smoke"
```

## Offline checks

The retained source checks do not require node databases or running nodes:

```bash
python3 -B regtest/v5_campaign.py --self-test
for directory in regtest regtest/sweep regtest/relay initial-sync pruning live-comparison figures; do
  (cd "$directory" && python3 -B -m unittest)
done
python3 -B -m doctest initial-sync/phase-summary.py
```

Optional signing, browser-JavaScript and compiled-compactor checks need their respective dependencies. The public block fixtures under `live-comparison/fixtures/` are small test vectors, not a published benchmark dataset. Fixture generation, live node operation, Mainnet sync, pruning and real figure rendering require the external inputs described above; offline tests do not establish their measured performance.

## Not reproducible from this repository

- **Pool single-event timings (1.31 ms / 803 ms):** excluded pool/miner integration and final image/build context are not published here.
- **ZMQ notification timings and `new-work-delay.png`:** exact original executable, feature/build receipt and notification observations were not recovered; the separate timing harness is excluded.
- **TCP_NODELAY A/B (12 samples per condition):** exact patched variants/build receipts are incomplete and that separate campaign is excluded. The retained V5 fresh-connection relay must not be presented as this A/B.
- **Historical live-Mainnet figure, 286-block controlled subset and grouped confidence intervals:** the included current plain dashboard does not implement the retired controlled-statistics renderer, and historical arrivals/records are not included.
- **Bit-identical binaries:** the default optimized arm's final receipt and complete native-library/linker/build-flag inventories are missing. Source pins and recorded recipes reproduce the build intent only.
- **Exact original fixtures and Mainnet databases:** randomized wallet generation and unpublished original state inventories prevent byte-for-byte reconstruction; fresh generation and independent sync reproduce the methods.
- **Exact historical pruning executable:** the earlier pruning-stack source is public at [`4d04ee163fdc51f3e689ca9572aaa7e0bfa3ad49`](https://github.com/ZcashFoundation/zebra/commit/4d04ee163fdc51f3e689ca9572aaa7e0bfa3ad49), but its build features, toolchain and native linkage were not recorded. The paired replay receipt identifies SHA256 `6a38f43c6f0a0d17cfdceed413e3651b4a9fdca31e2fb2330cba96cd1f4efb95` and version `zebrad 6.2.2+1209.g4d04ee1`; the separate storage receipt identifies `e3cb171b4d9130d8d8bd477d2457edf2685058f28cafc6b18bf5134f19ffbd6f` and `zebrad 7.0.0+210.g4d04ee1`. Do not conflate them.
- **Exact commit-hashing attribution builds:** phase wrappers are included, but the comparison build receipts were not recovered; choose and record both binaries before a new experiment.
- **Throwaway initialization/hash microtimings:** no retained receipts; microbenchmark trees are intentionally excluded.

The separately included snapshot-production workflow is later operational tooling, not the producer of the original storage comparison.

AI coding agents wrote this harness.
