# Mainnet shadow-block measurements

This harness copies blocks and mempool transactions **from an existing Mainnet node** into independently running Zebra/Zakura nodes. It never mines a public block. Run the controller on a separate machine, with one dedicated machine per measured node and another for the Mainnet source. Use the same hardware, OS settings, node resource limits and initial chain tip for every measured arm; record binary revisions and snapshot tip. Results are **unconfirmed until a real Mainnet run** has produced matching observations.

## Prepare each measured machine

1. Install the intended `zebrad` or `zakurad` binary and record its commit and SHA-256. Provide enough CPU, RAM and SSD for a fully synced Mainnet node. Do not co-locate arms, share a database, or let other workloads contend for disk/CPU. Keep system clocks synchronized (NTP/PTP) and record their offset; the harness uses its own monotonic clock for each latency, but RPC/SSH/network delays and the source's block arrival remain uncontrolled.
2. Start with a **stopped**, fully synced snapshot of that binary's `state.cache_dir` root. Stop the source node cleanly before copying; never copy a live RocksDB directory. On each measured machine, copy its compatible snapshot to a private state directory (for example `cp -a /srv/snapshots/mainnet-state /srv/shadow/state`). A Zakura snapshot must be compatible with the exact Zakura build; if Zebra's snapshot cannot be opened by it, sync Zakura independently and stop it at the *same block hash* instead. Check `getblockchaininfo`/`getbestblockhash` on all nodes before the run. A height match without a hash match is insufficient.
3. Put one config alongside each binary. Measured nodes have **no outbound peers** and accept inbound P2P only from the controller (firewall allowlist for its IP); do not connect them to the source or each other. Disable cached peers and seeders. Check `getpeerinfo` shows exactly the harness's single inbound observer. Permit LAN RPC only from the controller's IP, with cookie authentication.

Zebra `zebra.toml`:

```toml
[network]
network = "Mainnet"
listen_addr = "0.0.0.0:8233"
initial_mainnet_peers = []
cache_dir = false

[state]
cache_dir = "/srv/shadow/state"

[rpc]
listen_addr = "10.0.0.11:8232" # replace with this machine's private LAN address
cookie_dir = "/srv/shadow/cookies"
enable_cookie_auth = true

[mempool]
debug_enable_at_height = 0

[mining]
miner_address = "t1T92bmyoPM7PTSWUnaWnLGcxkF6Jp1AwMY"
```

Zakura v1.8.1 `zakura.toml`:

```toml
[network]
network = "Mainnet"
p2p_stack = "legacy"
listen_addr = "0.0.0.0:8233"
initial_mainnet_peers = []
cache_dir = false

[state]
cache_dir = "/srv/shadow/state"

[rpc]
listen_addr = "10.0.0.12:8232" # replace with this machine's private LAN address
cookie_dir = "/srv/shadow/cookies"
enable_cookie_auth = true

[mempool]
debug_enable_at_height = 0

[mining]
miner_address = "t1T92bmyoPM7PTSWUnaWnLGcxkF6Jp1AwMY"
```

`[mempool].debug_enable_at_height = 0` is supported by Zebra and Zakura (`crates/zakurad/src/components/mempool/config.rs`): with a non-genesis snapshot tip, it bypasses sync-status-based mempool disabling. Pin Zakura's legacy P2P stack for the observer. Replace the sample transparent payout address with your miner address. The harness requests templates and submits source blocks; it does not mine.

Use `./shadow/launch.sh <ssh-host> <absolute-binary-path> <absolute-config-path> start|stop|logs` for each remote measured process. Example:

```sh
./shadow/launch.sh zebra-host /srv/shadow/zebrad /srv/shadow/zebra.toml start
./shadow/launch.sh zakura-host /srv/shadow/zakurad /srv/shadow/zakura.toml start
./shadow/launch.sh zebra-host /srv/shadow/zebrad /srv/shadow/zebra.toml logs
# At the end: replace logs with stop for each host.
```

The launcher stores PID and appended logs as `<config>.pid` and `<config>.log` on that host. Start the Mainnet source independently with normal network connectivity and a matching tip. The source is not a `[[node]]` and must never connect to measured nodes over P2P.

## Controller

Allow RPC from the controller only over a private LAN or SSH tunnels: cookie authentication over plain HTTP reveals credentials to anyone able to inspect that LAN. For direct LAN RPC, use each machine's private address from the config. Copy each fresh `.cookie` securely through SSH after every restart to a controller file with mode 0600; never commit these credentials. Use SSH tunnels instead if the LAN is not trusted. Example `campaign.toml` (replace IPs, paths and labels):

```toml
mempool_interval_ms = 250
block_interval_ms = 20
timeout_seconds = 120

[main]
rpc = "http://10.0.0.10:8232/"
cookie_file = "/path/to/shadow-cookies/main.cookie"

[[node]]
label = "zebra-main"
rpc = "http://10.0.0.11:8232/"
cookie_file = "/path/to/shadow-cookies/zebra.cookie"
p2p = "10.0.0.11:8233"
network = "mainnet"

[[node]]
label = "zakura-main"
rpc = "http://10.0.0.12:8232/"
cookie_file = "/path/to/shadow-cookies/zakura.cookie"
p2p = "10.0.0.12:8233"
network = "mainnet"

```

Put optional timing settings at the **top level before `[main]`**, rather than below `[[node]]` (TOML table scope). From `regtest/`:

```sh
python3 -m shadow.run campaign.toml --output shadow-results/run-001
# Stop with Ctrl-C after enough real blocks; use a fresh output directory per run.
python3 shadow/summarize.py shadow-results/run-001/rows.jsonl --reference zebra-main
```

The controller writes `rows.jsonl` (one row per measured node/block plus `measured=false` ancestor catch-up rows) and sibling `mirror.jsonl` (per-node `sendrawtransaction` attempts). `submit_return_ms` is RPC return after submit, `tip_ms` is the controller's observed `getbestblockhash`, `template_ms` is the GBT long-poll's new parent, `peer_told_ms` is the passive P2P observer's first block advert, and `peer_has_ms` is the requested block body received by that observer. All intervals start at **that node's** `submitblock` dispatch; `null` means not observed, not zero latency. The summary prints per-node milestone means (headline), medians and half-sample modes, then paired node-minus-reference mean differences with bootstrap 95% CI and median differences, matched strictly by `(round, block_hash)`; negative differences mean the node was faster. Mirror rates count accepted/attempted **per node**; source polling/fetch errors are not per-node attempts. Inspect rows' `errors`, missing milestones, source `feed_error` and snapshot/binary provenance before drawing conclusions; failed cases and missing pairs must not be interpreted as fast observations.

## Regtest host smoke check

From `regtest/`, with the sealed fixture directory and the binaries built by the
[v1.8.1 runbook](../../zakura-1.8.1-RUNBOOK.md):

```sh
python3 -m shadow.smoke \
  --main "${BENCH_BINS:-../bins/zakura-1.8.1}/zebrad-stack" \
  --node stack="${BENCH_BINS:-../bins/zakura-1.8.1}/zebrad-stack" \
  --node crypto="${BENCH_BINS:-../bins/zakura-1.8.1}/zebrad-crypto" \
  --node zakura="${BENCH_BINS:-../bins/zakura-1.8.1}/zakurad-1.8.1" \
  --fixture-dir "${BENCH_FIXTURES:-../fixtures/zakura-1.8.1/standard}" \
  --candidate mixed-1900000 --rounds 3 --output shadow-results/smoke
```

Use a fresh output directory. Each round starts new isolated nodes, replays the sealed funding chain, admits candidate transactions only to the source, waits for the harness mirror and observers, then submits the candidate to the source. This local Regtest check cannot establish Mainnet sync status, remote network latency, or resource isolation; those need an actual host run.

## Mainnet sync-status caveat

Zebra's `zebrad/src/components/sync/status.rs` returns false on Mainnet when no recent sync lengths exist; once a length is recorded, it considers the node close to tip if the moving average is under 20. In `zebrad/src/components/sync.rs::obtain_tips`, the syncer sends `FindBlocks` (wire `getblocks`) using a locator headed by the current tip, filters known hashes, then records `download_set.len()` via `push_obtain_tips_length` **even if the result is zero**. The observer answers `getblocks` with an empty inventory when the locator contains its current tip, and `getheaders` with an empty header list for that same-tip case; it answers `ping` with `pong`. Thus a successful same-tip obtain-tips cycle should record sync length 0 and allow GBT, without advertising any candidate before submission. This is a source-code conclusion, **not confirmed without a real Mainnet run**: it still depends on Zebra selecting the inbound observer, completing requests, and maintaining that status, and on Zakura's own sync checks. Inspect actual `getblocktemplate`, peer status, tips and row errors before relying on Mainnet figures.
