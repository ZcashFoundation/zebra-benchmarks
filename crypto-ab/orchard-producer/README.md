# One-action Orchard cold-submit fixture

From the benchmarks repository root, with Rust, rootless Podman (host networking), the
pinned zcashd image used by `regtest/sweep/build_fixtures.py`, and a NU5-capable Zebra
binary:

```sh
export ZEBRAD="${ZEBRAD:-$PWD/../zebra/target/release/zebrad}"
cargo build --locked --release --manifest-path crypto-ab/orchard-producer/Cargo.toml
python3 crypto-ab/orchard-producer/recipe.py \
  --producer "$ZEBRAD" --output fixtures/orchard1 \
  --setup-blocks 1100 --timeout 600
```

The 330 one-action Orchard transactions match the [ZIP 218](https://zips.z.cash/zip-0218)
NU7 per-block Orchard-action cap. This **Regtest** fixture instead activates NU5 at
height 1 and uses the producer's `orchard = 0.16.0` circuit; it does not test NU7
consensus activation. The legal block is near 1,984,600 bytes (under 2,000,000).
Proof generation and chain replay can take hours. The recipe records the producer's
lockfile hash and seals its fixtures. Keep the output separate
from other fixture sets. Never publish `seed.bin` or `wallet/`: only entries under
`manifest.json.files` are sealed benchmark inputs. See the parent README for paired
cold-submit measurements using `EXTRA_FIXTURES=fixtures/orchard1`.
