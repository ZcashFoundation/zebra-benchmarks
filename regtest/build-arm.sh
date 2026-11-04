#!/bin/bash
# Run from a clean public Zebra checkout at the desired commit.
set -euo pipefail
if [[ $# != 1 || ${1:-} == --help || ${1:-} == -h ]]; then
    cat <<'HELP'
Usage: bash build-arm.sh BINARY_NAME
Run inside a clean Zebra checkout. Builds locked Rust 1.97.0 release zebrad with
features default-release-binaries,internal-miner. ZEBRA_EXTRA_FEATURES adds a
comma-separated feature list (e.g. pasta-asm). BENCH_BINS selects output directory
(default: repository-root/bins). Writes BINARY_NAME and BINARY_NAME.build.json.
Reference commit: e8db87eb69254e0d4eedd058eadd48bdfa09065a
Optimized commit: 5527c563929fbe3b9e82d099b4d4cfc19b092e20
Public source: https://github.com/ZcashFoundation/zebra
HELP
    [[ ${1:-} == --help || ${1:-} == -h ]] && exit 0
    exit 2
fi
name=$1
[[ $name =~ ^[a-zA-Z0-9][a-zA-Z0-9._-]*$ ]] || { echo 'Invalid binary basename' >&2; exit 2; }
bins=${BENCH_BINS:-$(dirname "$(realpath "$0")")/../bins}
mkdir -p -- "$bins"
bins=$(realpath "$bins")
cd "$(git rev-parse --show-toplevel)"
source=$(git rev-parse HEAD)
[[ -z $(git status --porcelain --untracked-files=no) ]] || { echo 'Dirty tracked worktree' >&2; exit 2; }
features=default-release-binaries,internal-miner${ZEBRA_EXTRA_FEATURES:+,$ZEBRA_EXTRA_FEATURES}
recipe="RUSTUP_TOOLCHAIN=1.97.0 cargo build --locked --release -p zebrad --features $features"
export RUSTUP_TOOLCHAIN=1.97.0
cargo build --locked --release -p zebrad --features "$features"
target=$(cargo metadata --locked --format-version 1 --no-deps |
    python3 -c 'import json, sys; print(json.load(sys.stdin)["target_directory"])')
[[ $(git rev-parse HEAD) == "$source" && -z $(git status --porcelain --untracked-files=no) ]] || {
    echo 'Tracked source changed during build; refusing receipt' >&2; exit 2;
}
bin=$bins/$name
install -m 755 -- "$target/release/zebrad" "$bin"
python3 - "$bin" "$source" "$recipe" <<'PY'
import hashlib, json, os, subprocess, sys
from pathlib import Path
binary, source, recipe = sys.argv[1:]
with open(binary, 'rb') as stream:
    digest = hashlib.file_digest(stream, 'sha256').hexdigest()
compiler_settings = dict(
    rustc_verbose_version=subprocess.check_output(['rustc', '-vV'], text=True).strip(),
    environment={key: os.environ.get(key) for key in (
        'RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS', 'RUSTC_WRAPPER', 'RUSTC_WORKSPACE_WRAPPER',
        'CARGO_PROFILE_RELEASE_OPT_LEVEL', 'CARGO_PROFILE_RELEASE_LTO',
        'CARGO_PROFILE_RELEASE_CODEGEN_UNITS', 'CARGO_PROFILE_RELEASE_DEBUG',
        'CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS', 'CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS',
        'CARGO_PROFILE_RELEASE_STRIP', 'CARGO_PROFILE_RELEASE_PANIC',
        'CARGO_PROFILE_RELEASE_INCREMENTAL', 'CC', 'CXX', 'AR',
        'CFLAGS', 'CXXFLAGS', 'CPPFLAGS', 'LDFLAGS',
    )})
receipt = Path(binary + '.build.json')
temporary = receipt.with_suffix(receipt.suffix + '.tmp')
temporary.write_text(json.dumps(dict(source=source, path=binary, sha256=digest,
                                    build=recipe, compiler_settings=compiler_settings), indent=2) + '\n')
temporary.replace(receipt)
print('BUILT', source, binary, digest)
PY
