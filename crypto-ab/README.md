# Paired Orchard crypto A/B

An **arm** is one named, pinned `zebrad` or `zakurad` executable. All arms replay the
same sealed Regtest funding prefix and submit the same block bytes to a fresh node per
observation. The runner reverses arm order every other round (20 rounds per fixture).
This measures cold `submitblock` latency, not proof generation or network propagation.
Run timing on an otherwise idle Linux machine; no runner or CPU topology is assumed.

From this repository's root, with Rust, Python 3.11+, Bash, GNU coreutils
and the nodes' native build dependencies installed:

```sh
export HARNESS="$PWD"
export RUN_ROOT="$PWD/output/crypto-ab"
export ZEBRA_SRC="${ZEBRA_SRC:-$PWD/../zebra}"
export ZAKURA_SRC="${ZAKURA_SRC:-$PWD/../zakura}"
export ZAKURA_MAIN_REVISION="${ZAKURA_MAIN_REVISION:-$(git -C "$ZAKURA_SRC" rev-parse HEAD)}"
(cd "$ZEBRA_SRC" && BENCH_BINS="$RUN_ROOT/bins" \
  bash "$HARNESS/regtest/build-arm.sh" zebra)
(cd "$ZAKURA_SRC" && BENCH_BINS="$RUN_ROOT/bins" \
  bash "$HARNESS/regtest/build-zakura.sh" zakura)
```

Use clean committed checkouts. The Zebra builder accepts any commit; the Zakura
builder requires `ZAKURA_MAIN_REVISION` to match the chosen checkout (for Zakura v1.9.0-rc2,
`bc75a7b8f4962d40744130d545b4915a8ce0977b`). Both build with `cargo --locked --release`,
the checkout's release profile and `RUSTUP_TOOLCHAIN` (default 1.97.0). Zakura
uses `default-release-binaries`; Zebra uses
`default-release-binaries,internal-miner`. Every binary has a `.build.json`
receipt recording its SHA-256, source commit, command and compiler settings.
To build variants, use distinct arm names and set `RUSTUP_TOOLCHAIN`, `RUSTFLAGS`
or `CARGO_PROFILE_RELEASE_*`, for example:

```sh
(cd "$ZEBRA_SRC" && BENCH_BINS="$RUN_ROOT/bins" RUSTUP_TOOLCHAIN=1.97.0 \
  RUSTFLAGS='-C target-cpu=native' bash "$HARNESS/regtest/build-arm.sh" zebra-native)
```

`RUN_ROOT` defaults to `output/crypto-ab/` relative to this repository.
`BENCH_BINS` selects the builders' output directory and must be `RUN_ROOT/bins`.
The runner requires the receipts. Set `LD_LIBRARY_PATH` if Zakura requires
additional shared libraries.

To produce the 330-action Regtest fixture, follow the
[producer README](orchard-producer/README.md). Its 330 one-action bundles match
the Orchard per-block cap specified by [ZIP 218](https://zips.z.cash/zip-0218)
for NU7; this fixture activates NU5 at height 1 and uses the
`orchard = 0.16.0` circuit, **not** NU7 consensus activation. The serialized
block is approximately 1,984,600 bytes, below the 2 MB limit. Proof generation
and chain replay may take hours.

The ordinary sweep fixtures are produced by `regtest/sweep/`; `BENCH_FIXTURES`
defaults to `fixtures/zakura-1.8.1/standard`. `EXTRA_FIXTURES` selects another
sealed set, and `BENCH_SIGNED_FIXTURES` selects signed transparent blocks.
`FIXTURE_LIST` specifies IDs to measure. With a sealed Orchard fixture:

```sh
EXTRA_FIXTURES="$PWD/fixtures/orchard1" \
  FIXTURE_LIST='orchard1-1980000' ARMS='zebra zakura' THREADS=4 \
  bash crypto-ab/measure.sh
```

Without `FIXTURE_LIST`, measurement uses `orchard-250000 orchard-1900000
mixed-1900000` from the standard sealed set; optionally include
`transparent-signed-1900000` with `BENCH_SIGNED_FIXTURES`. `ARMS` defaults to
`zebra zakura`; `THREADS` defaults to the CPUs the probe may use (`V5_CPUS` if set). Processes inherit CPU
affinity; optionally set `V5_CPUS` or use `taskset` after checking your topology.
`MEASURE_ROOT` overrides the fresh output directory. Each run writes `rows.jsonl`,
`summary.txt`, logs, receipts and checksums under `RUN_ROOT/measure-*`; captured
runs contain machine-specific paths, so review them before publishing.

`summary.txt` gives client latency in ms: **mean** is the headline, with
**median** and **half-sample mode** alongside it. Each unique pair gives the
**subject − comparator** mean difference and a percentile-bootstrap **95%
confidence interval**; negative means the subject is faster. To print Markdown
tables from one or more runs, use `python3 crypto-ab/report.py RUN_DIR [RUN_DIR ...]`.
