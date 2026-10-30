#!/bin/bash
# Run from the clean pinned public Zakura checkout.
set -euo pipefail
if [[ $# != 1 || ${1:-} == --help || ${1:-} == -h ]]; then
    cat <<'HELP'
Usage: bash build-zakura.sh BINARY_NAME
Run inside a clean Zakura checkout at c572f8faf88e58dcaefd80ec8e4ed05f2d8643b6.
Builds locked Rust 1.97.0 release package zakura, binary zakurad, with features
default-release-binaries. BENCH_BINS selects output directory (default:
repository-root/bins). Writes BINARY_NAME and BINARY_NAME.build.json.
Public source: https://github.com/zakura-core/zakura
HELP
    [[ ${1:-} == --help || ${1:-} == -h ]] && exit 0
    exit 2
fi
pin=c572f8faf88e58dcaefd80ec8e4ed05f2d8643b6
name=$1
[[ $name =~ ^[a-zA-Z0-9][a-zA-Z0-9._-]*$ ]] || { echo 'Invalid binary basename' >&2; exit 2; }
bins=${BENCH_BINS:-$(dirname "$(realpath "$0")")/../bins}
mkdir -p -- "$bins"
bins=$(realpath "$bins")
cd "$(git rev-parse --show-toplevel)"
source=$(git rev-parse HEAD)
[[ $source == "$pin" ]] || { echo "HEAD must be $pin, not $source" >&2; exit 2; }
[[ -z $(git status --porcelain --untracked-files=no) ]] || { echo 'Dirty tracked worktree' >&2; exit 2; }
recipe='RUSTUP_TOOLCHAIN=1.97.0 cargo build --locked --release -p zakura --bin zakurad --features default-release-binaries'
export RUSTUP_TOOLCHAIN=1.97.0
cargo build --locked --release -p zakura --bin zakurad --features default-release-binaries
target=$(cargo metadata --locked --format-version 1 --no-deps |
    python3 -c 'import json, sys; print(json.load(sys.stdin)["target_directory"])')
[[ $(git rev-parse HEAD) == "$source" && -z $(git status --porcelain --untracked-files=no) ]] || {
    echo 'Tracked source changed during build; refusing receipt' >&2; exit 2;
}
bin=$bins/$name
install -m 755 -- "$target/release/zakurad" "$bin"
python3 - "$bin" "$source" "$recipe" <<'PY'
import hashlib, json, sys
from pathlib import Path
binary, source, recipe = sys.argv[1:]
with open(binary, 'rb') as stream:
    digest = hashlib.file_digest(stream, 'sha256').hexdigest()
receipt = Path(binary + '.build.json')
temporary = receipt.with_suffix(receipt.suffix + '.tmp')
temporary.write_text(json.dumps(dict(source=source, path=binary, sha256=digest,
                                    build=recipe), indent=2) + '\n')
temporary.replace(receipt)
print('BUILT', source, binary, digest)
PY
