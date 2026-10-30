#!/usr/bin/env python3
"""Opt-in Linux node recording; no opt-in means an ordinary subprocess.Popen.

Driver interface (shell quoting is parsed, never executed by a shell)::

    NODE_WRAPPER='samply record --save-only --no-open -o {profile} --rate 4000 --'
    NODE_METRICS_ARM=zebra-stack
    NODE_METRICS_BINARY=/absolute/sealed/zebra-stack/binary
    NODE_METRICS_BINARIES='{"zebra-stack":"/sealed/stack","zakura-head":"/sealed/head"}'
    NODE_PHASE_DELTAS=1  # only selected explicit-prepared/proposal-prepared nodes
    NODE_PHASE_MODE=metrics-only  # or profiled, when a recorder is enabled
    NODE_WORKLOAD=cold-sapling16
    NODE_PROFILE_MODES=explicit-prepared,proposal-prepared  # optional directory names
    NODE_PROFILE_RATE=1000  # optional override of the wrapper's --rate (1..100000 Hz)
    NODE_PROFILE_ROOT=/absolute/campaign/artifacts
    NODE_PROFILE_MAX_BYTES=16106127360  # optional paired ROOT/MAX_BYTES budget

The wrapper may additionally contain --presymbolicate or --unstable-presymbolicate.
{profile} MUST be the output argument, not a fixed shared filename. Recording covers
whole node lifetimes, including setup and shutdown, NOT just timed request windows.
Only exact opted-in binaries receive a localhost :0 metrics listener. The JSON
allowlist supports zebra-stack and zakura-head; the legacy ARM/BINARY pair still
supports zebra-stack. Actual bound sockets are discovered from each node's /proc
files, then identified by zebrad_build_info or zakura_build_info respectively.
Metrics are cumulative and still scraped before stopping the node.

Explicit diagnostic fallback: omit NODE_WRAPPER and set NODE_PROFILE_FALLBACK=gdb
in a NEW output directory. GDB captures all-thread bt12 stacks.
GDB launches the exact node command and
node-only SIGINTs request stacks every 1.5 seconds. These intrusive, low-resolution
diagnostics are NOT CPU-time samples. gdb-stacks.txt replaces profile.json.gz;
metadata marks fallback=true. No failed run is automatically retried.

With ROOT/MAX_BYTES, each lifetime gets a UUID artifact directory. All files below
ROOT count, including sidecars, transient outputs, metrics and hard-linked shared
recorder/node logs. Original log paths still work; databases remain outside ROOT.
A locked ledger reserves at most three simultaneous captures, each with four
equal file allowances plus 65 MiB for metrics/metadata. After the exact node has
execed, prlimit applies RLIMIT_FSIZE only to the recorder (not the running node).
A 100 ms watchdog checks aggregate usage before launch, through shutdown/profile
serialization, and after recorder exit; any breach latches campaign failure.
Four file allowances cover gzip, symbols, the shared log and one transient output.
Normal recorder outputs fit reservations. Extra files, node writes to shared logs,
and pre-discovery recorder writes can overshoot between scans: there is NO fixed
aggregate-byte guarantee. Each post-discovery recorder file is kernel-limited;
unrestricted writes have a 100 ms detection interval, not a byte bound. Use a
filesystem quota for a hard global cap. Root and node logs must share a filesystem.
No artifacts are removed, including failed/truncated captures. A budget failure
stops the node and recorder immediately; ordinary shutdown never kills a recorder.
Use a new ROOT after failure; errors[] in metadata makes summaries fail explicitly.

Hooks: spawn(command, stdout, stderr, env, directory, arm=None), then stop(process).
spawn updates the command's -c/--config file before launch when metrics are enabled.
A harness can alternatively call prepare_config before writing that file.
Always call stop, including after spontaneous node exit, to drain the recorder.
Prepared-submit hooks: token = begin_operation(process, "submitblock") BEFORE the
client timer; call end_operation(process, token, elapsed_ns, success=True) just
after recording the RPC return timestamp. It returns a JSON-safe receipt including
receipt_path (or None when disabled). Before/after raw operation-*.prom snapshots
and operation-*.phase-delta.json live beside lifetime metadata, never under the
phase-metrics*.prom names used for cumulative summaries. Only _count/_sum change;
quantiles are never subtracted. Missing-before complete new series use zero with
an explicit warning; absent-after series are unavailable, not zero. Main/160 keep
client-total-only receipts. Scrape intervals include work outside the RPC timer,
including asynchronous work after return; clocks/interval overhead make this
visible. Profiled and metrics-only receipts must never be pooled. Source-pinned
Zakura d875ccf92 mined metrics are not gated by the unrelated commit-metrics feature.
Exporter 0.17.2 normalizes dots to underscores, renders summaries by default, and
appends _count/_sum:
https://docs.rs/crate/metrics-exporter-prometheus/0.17.2/source/src/formatting.rs
https://docs.rs/crate/metrics-exporter-prometheus/0.17.2/source/src/recorder.rs
Opted-in harnesses install SIGTERM/SIGINT and atexit cleanup for their nodes.
Signals arriving during launch are deferred until the exact node is registered.
The coordinator must allow 2000 seconds of cleanup for up to three profiled nodes;
SIGKILL cannot be intercepted. Recorder sessions stay isolated from group signals.

The proxy implements the Popen subset used by both harnesses: args, pid, returncode,
poll, send_signal, terminate, kill, wait. pid is ALWAYS the node, never samply.
Samply owns waitpid, so a reaped node's exact exit status may be unavailable. In that
case poll/wait return 255 (exited, status unknown), and metadata node_returncode is
null; recorder_returncode is separate. Signals use pidfds, not process groups.
wait(timeout) bounds NODE exit only; recorder flush has a separate 600-second limit.
A flush failure raises RuntimeError, never TimeoutExpired (which callers interpret
as permission to kill the node). Only budget failure forcibly stops a recorder.

Stock samply currently requests kernel samples even for child processes. At
perf_event_paranoid=2 use a user-only samply build, not a sysctl change; see
https://github.com/mstange/samply/blob/f5a8bf100cd54b771897f400b438c415f7d0f205/samply/src/linux/perf_group.rs#L134-L145
Shutdown waits for the child and drains its events before writing the profile:
https://github.com/mstange/samply/blob/f5a8bf100cd54b771897f400b438c415f7d0f205/samply/src/linux/profiler.rs#L169-L242
Metrics key and feature gate:
https://github.com/ZcashFoundation/zebra/blob/main/zebrad/src/components/metrics.rs

Run this file with --self-check for the small, pure-helper check; no node is started.
"""

import atexit
from contextlib import contextmanager
import fcntl
import gzip
import http.client
import json
import math
import os
from pathlib import Path
import re
import resource
import select
import shlex
import shutil
import signal
import subprocess
import sys
import threading
import time
import tomllib
import uuid

ARMS = ("zebra-main", "zebra-stack", "zakura-head", "zakura-160")
FLUSH_TIMEOUT = 600
BUDGET_INTERVAL = 0.1
BUDGET_RECORDERS = 3
BUDGET_FILE_SLOTS = 4
BUDGET_AUX_BYTES = 65 * 1024 * 1024
BUDGET_LEDGER_BYTES = 1024 * 1024
PREPARED_MODES = ("explicit-prepared", "proposal-prepared")
PHASE_METRICS = {
    "zebra-stack": {
        "zebra_consensus_block_phase_duration_seconds": (
            "block_checks", "transaction_verification", "state_commit"),
        "state_block_writer_phase_duration_seconds": (
            "queue", "contextual_commit", "update_latest_chain_channels", "finalization"),
    },
    # d875ccf92: write.rs (queue, total, initial checks, header transitions) and
    # non_finalized_state.rs (remaining contextual/parallel task durations).
    "zakura-head": {
        "state_block_writer_queue_mined_duration_seconds": (),
        **{"state_contextual_mined_" + phase + "_duration_seconds": () for phase in (
            "total", "initial_checks", "snapshot_clone", "header_transition_prepare",
            "header_transition_commit", "parent_chain", "chain_new", "transparent_spend",
            "shielded_anchors", "sprout_anchor_fetch", "block_construction", "parallel_update",
            "parallel_task_block_commitment", "parallel_task_sprout_anchor_check",
            "parallel_task_chain_clone", "parallel_task_chain_push")},
    },
}
_PROM_LABEL = re.compile(r'\s*([A-Za-z_][A-Za-z_0-9]*)\s*=\s*("(?:[^"\\]|\\.)*")\s*(?:,|$)')
_PROM_SAMPLE = re.compile(r'^([A-Za-z_:][A-Za-z_0-9:]*)(?:\{(.*)\})?\s+(\S+)(?:\s+.*)?$')
_active_processes = set()
_cleanup_installed = False
_launching = False
_pending_signal = None


def _cleanup_active(reason):
    for process in tuple(_active_processes):
        state = getattr(process, "_node_state", None)
        if state:
            state["metadata"]["shutdown_reason"] = reason
        try:
            stop(process)
        except Exception as error:
            print(f"node {process.pid} cleanup failed: {error}", file=sys.stderr, flush=True)


def _termination_signal(signum, _frame):
    global _pending_signal
    _pending_signal = signum
    if _launching:
        return
    # Do not recursively enter cleanup if the coordinator sends another signal.
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    _cleanup_active(f"harness-signal:{signum}")
    # A benchmark's exception handler must not swallow the termination request.
    os._exit(128 + signum)


def _install_cleanup():
    global _cleanup_installed
    if _cleanup_installed:
        return
    if threading.current_thread() is not threading.main_thread():
        raise RuntimeError("instrumented node spawn must run on the harness main thread")
    signal.signal(signal.SIGTERM, _termination_signal)
    signal.signal(signal.SIGINT, _termination_signal)
    atexit.register(_cleanup_active, "harness-exit")
    _cleanup_installed = True


def _workload(directory, env):
    workload = env.get("NODE_WORKLOAD", directory.parent.name)
    if workload == "node-coldsubmit":
        workload += ":" + directory.name
    if (any(part.startswith("producer") for part in directory.parts)
            or (workload == "node-gbtsapling" and directory.name == "n1")):
        workload = "setup:" + workload
    return workload


def _selected(directory, env):
    modes = env.get("NODE_PROFILE_MODES", "")
    return not modes or Path(directory).name in {x.strip() for x in modes.split(",")}


def _arm(binary, arm=None):
    if arm is not None:
        return arm
    # Check nearest parent first: an old zebra-stack filename can also be main.
    return next((p.name for p in Path(binary).parents if p.name in ARMS), None)


def _metrics_arm(binary, env, arm=None):
    allowed = json.loads(env.get("NODE_METRICS_BINARIES", "{}"))
    if not isinstance(allowed, dict) or any(
            key not in PHASE_METRICS or not isinstance(value, str) or not value
            for key, value in allowed.items()):
        raise ValueError("NODE_METRICS_BINARIES must map zebra-stack/zakura-head to exact paths")
    requested = env.get("NODE_METRICS_ARM")
    if requested:
        if requested != "zebra-stack" or not env.get("NODE_METRICS_BINARY"):
            raise ValueError("metrics require NODE_METRICS_ARM=zebra-stack and NODE_METRICS_BINARY")
        legacy = env["NODE_METRICS_BINARY"]
        if "zebra-stack" in allowed and Path(allowed["zebra-stack"]).resolve() != Path(legacy).resolve():
            raise ValueError("legacy metrics binary conflicts with NODE_METRICS_BINARIES")
        allowed["zebra-stack"] = legacy
    paths = {key: Path(value).resolve() for key, value in allowed.items()}
    if len(set(paths.values())) != len(paths):
        raise ValueError("metrics arms require distinct exact binaries")
    matched = next((key for key, path in paths.items() if Path(binary).resolve() == path), None)
    if matched is not None and arm is not None and arm != matched:
        raise ValueError("metrics binary belongs to a different arm: " + arm)
    return matched


def _metrics_enabled(binary, env, arm=None):
    return _metrics_arm(binary, env, arm) is not None


def _phase_enabled(directory, env):
    return (env.get("NODE_PHASE_DELTAS") == "1"
            and Path(directory).name in PREPARED_MODES and _selected(directory, env))


def prepare_config(text, binary, env, arm=None, directory=None):
    """Append the metrics table only for the exact opted-in executable/mode."""
    if directory is not None and not _selected(directory, env):
        return text
    if not _metrics_enabled(binary, env, arm):
        return text
    metrics = tomllib.loads(text).get("metrics")
    if metrics is not None:
        if metrics.get("endpoint_addr") != "127.0.0.1:0":
            raise ValueError("opt-in profiling requires metrics.endpoint_addr = '127.0.0.1:0'")
        return text
    return text + '\n[metrics]\nendpoint_addr = "127.0.0.1:0"\n'


def _wrapper_args(template, profile, rate_override=None):
    args = shlex.split(template)
    if len(args) < 3 or Path(args[0]).name != "samply" or args[1] != "record" or args[-1] != "--":
        raise ValueError("NODE_WRAPPER must be samply record ... --")
    options = args[2:-1]
    flags = set()
    output = rate = None
    index = 0
    while index < len(options):
        option = options[index]
        if option in ("--save-only", "--no-open", "--presymbolicate", "--unstable-presymbolicate"):
            if option in flags:
                raise ValueError("duplicate wrapper flag: " + option)
            flags.add(option)
            index += 1
        elif option in ("-o", "--output", "--rate") and index + 1 < len(options):
            value = options[index + 1]
            if option == "--rate" and rate is None:
                rate = value
            elif option in ("-o", "--output") and output is None:
                output = value
            else:
                raise ValueError("duplicate wrapper option: " + option)
            index += 2
        else:
            raise ValueError("unsupported NODE_WRAPPER option: " + option)
    if not {"--save-only", "--no-open"} <= flags or output != "{profile}" or rate is None:
        raise ValueError("wrapper requires --save-only --no-open -o {profile} --rate HZ")
    rate = _profile_rate(rate_override if rate_override is not None else rate)
    args[args.index("--rate") + 1] = str(rate)
    return [str(profile) if item == "{profile}" else item for item in args]


def _profile_rate(value):
    if not re.fullmatch(r"[0-9]+", str(value)) or not 1 <= int(value) <= 100000:
        raise ValueError("profile rate must be an integer from 1 to 100000 Hz")
    return int(value)


def _budget_share(maximum, used, reservations, active_count):
    """Reserve space for all outstanding captures, not just files already saved."""
    slots = BUDGET_RECORDERS - active_count
    available = maximum - used - reservations - BUDGET_LEDGER_BYTES
    allocation = available // slots if slots > 0 else 0
    file_limit = (allocation - BUDGET_AUX_BYTES) // BUDGET_FILE_SLOTS
    if file_limit < 1024 * 1024:
        raise RuntimeError("profile budget exhausted or three recorders already active")
    return allocation, file_limit


def _disk_files(root):
    """Count logical and allocated bytes, including unknown/temporary outputs."""
    files = {}
    for directory, dirs, names in os.walk(root, followlinks=False):
        if any((Path(directory) / name).is_symlink() for name in dirs):
            raise RuntimeError("profile artifact root must not contain directory symlinks")
        for name in names:
            path = Path(directory) / name
            try:
                stat = path.lstat()
            except FileNotFoundError:  # A recorder may rename its temporary output.
                continue
            if path.is_symlink():
                raise RuntimeError("profile artifact root must not contain file symlinks")
            files[path] = max(stat.st_size, stat.st_blocks * 512)
    return files


class ProfileBudget:
    """Cross-harness reservations plus a watchdog; no filesystem-quota claim."""

    def __init__(self, root, maximum, state):
        self.root, self.maximum, self.state = root, maximum, state
        self.key = state["metadata"]["process_lifetime_id"]
        self.recorder = self.process = None
        state["budget"] = self
        self.done = threading.Event()
        self.thread = None
        self.reserved = False
        self.details = state["metadata"]["profile_budget"] = {
            "root": str(root), "max_bytes": maximum, "status": "pending",
            "watch_interval_s": BUDGET_INTERVAL, "max_concurrent_recorders": BUDGET_RECORDERS,
            "reserved_file_slots": BUDGET_FILE_SLOTS, "strict_aggregate_quota": False,
            "scope": "all artifact-root files including hard-linked node/recorder logs; "
                     "excludes node databases",
            "overshoot": "100ms detection interval, not fixed byte bound: node shared-log "
                         "writes, pre-discovery writes, unexpected extra files",
        }
        with self.ledger() as ledger:
            if ledger.get("failure"):
                raise RuntimeError("profile campaign already failed: " + ledger["failure"])
            files = _disk_files(root)
            outstanding = sum(max(0, entry["bytes"] - sum(
                size for path, size in files.items() if path.is_relative_to(root / key)
            )) for key, entry in ledger["active"].items())
            allocation, limit = _budget_share(maximum, sum(files.values()), outstanding,
                                              len(ledger["active"]))
            hard = resource.getrlimit(resource.RLIMIT_FSIZE)[1]
            if hard != resource.RLIM_INFINITY:
                limit = min(limit, hard)
            if limit < 1024 * 1024:
                raise RuntimeError("inherited hard RLIMIT_FSIZE is too small for profiling")
            ledger["active"][self.key] = {"bytes": allocation, "owner_pid": os.getpid()}
            self.details.update(status="reserved", reserved_bytes=allocation,
                                file_limit_bytes=limit, root_bytes_before=sum(files.values()))
            self.reserved = True

    @contextmanager
    def ledger(self):
        with (self.root / ".profile-budget.lock").open("a+b") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            path = self.root / ".profile-budget"
            if path.exists():
                ledger = json.loads(path.read_text())
                if ledger["max_bytes"] != self.maximum:
                    raise RuntimeError("NODE_PROFILE_MAX_BYTES changed within an existing root")
            else:
                ledger = {"max_bytes": self.maximum, "active": {}, "failure": None}
            try:
                yield ledger
            except BaseException:
                raise
            else:
                temporary = self.root / ".profile-budget.tmp"
                temporary.write_text(json.dumps(ledger, indent=2) + "\n")
                temporary.replace(path)

    def fail(self, reason):
        message = self.details.get("failure") or "profile budget/capture failure: " + str(reason)
        if self.details["status"] != "failed":
            self.state["errors"].append(message)
        self.details.update(status="failed", failure=message)
        try:
            with self.ledger() as ledger:
                ledger["failure"] = ledger["failure"] or message
        except (OSError, ValueError, KeyError) as error:
            # Full/corrupt storage must not prevent emergency writer shutdown.
            self.state["errors"].append("could not persist profile failure latch: " + str(error))

    def check(self):
        with self.ledger() as ledger:
            if ledger.get("failure"):
                raise RuntimeError(ledger["failure"])
            files = _disk_files(self.root)
            total = sum(files.values())
            own = {path: size for path, size in files.items()
                   if path.is_relative_to(self.state["directory"])}
            self.details["root_bytes_observed"] = total
            self.details["capture_bytes_observed"] = sum(own.values())
            self.details["root_peak_bytes_observed"] = max(
                total, self.details.get("root_peak_bytes_observed", 0))
            if shutil.disk_usage(self.root).free < BUDGET_AUX_BYTES:
                raise RuntimeError("less than 65 MiB free for profile shutdown/metadata")
            if total >= self.maximum - BUDGET_LEDGER_BYTES:
                raise RuntimeError("aggregate profile artifact budget exhausted")
            if sum(own.values()) >= self.details["reserved_bytes"]:
                raise RuntimeError("capture exhausted its reserved aggregate allowance")
            # Operation snapshots/receipts share the bounded auxiliary reserve,
            # not the recorder's four output slots. All still count toward ROOT.
            auxiliary = {path for path in own
                         if path.name in ("profile-metadata.json", "phase-metrics.json",
                                          "phase-metrics.prom", "gdb-commands.txt")
                         or re.fullmatch(r"operation-[0-9a-f-]+(?:-(?:before|after)\.prom|\.phase-delta\.json)", path.name)}
            if sum(own[path] for path in auxiliary) > BUDGET_AUX_BYTES:
                raise RuntimeError("metrics/metadata exhausted the 65 MiB auxiliary allowance")
            recorder_files = {path: size for path, size in own.items() if path not in auxiliary}
            if len(recorder_files) > BUDGET_FILE_SLOTS:
                raise RuntimeError("recorder exceeded the four-file reservation contract")
            for path, size in recorder_files.items():
                if size >= self.details["file_limit_bytes"]:
                    raise RuntimeError("recorder reached RLIMIT_FSIZE: " + str(path))

    def start(self, recorder):
        self.recorder = recorder
        self.thread = threading.Thread(target=self.watch, daemon=True)
        self.thread.start()

    def abort_writers(self):
        """Emergency only: never called by ordinary before_stop/recorder flushing."""
        if self.recorder is None:
            if self.process is not None:
                self.process.kill()
            return
        # Freeze the writer before asking its exact node to stop. A budget breach
        # cannot afford normal shutdown's potentially large final serialization.
        try:
            self.recorder.send_signal(signal.SIGSTOP)
        except ProcessLookupError:
            pass
        if self.process is not None:
            try:
                signal.pidfd_send_signal(self.process._pidfd, signal.SIGKILL)
            except (ProcessLookupError, OSError):
                pass
        # Also stop owned symbol helpers or a not-yet-execed child. Never signal
        # a PID without pinning and rechecking its observed process identity.
        for pid in _descendants(self.recorder.pid):
            try:
                identity = _stat_fields(Path(f"/proc/{pid}/stat").read_text())[1]
                fd = os.pidfd_open(pid)
                try:
                    if _stat_fields(Path(f"/proc/{pid}/stat").read_text())[1] == identity:
                        signal.pidfd_send_signal(fd, signal.SIGKILL)
                finally:
                    os.close(fd)
            except (FileNotFoundError, ProcessLookupError):
                pass
        self.recorder.kill()

    def watch(self):
        while not self.done.wait(BUDGET_INTERVAL):
            try:
                self.check()
            except Exception as error:
                try:
                    self.fail(error)
                finally:
                    self.abort_writers()
                return

    def finish(self):
        # Keep monitoring until wait() has reaped the recorder, INCLUDING gzip
        # serialization, symbol sidecars and shutdown debug output.
        self.done.set()
        if self.thread is not None:
            self.thread.join()
        try:
            self.check()
        except Exception as error:
            self.fail(error)
        if self.state["errors"]:
            self.fail("; ".join(self.state["errors"]))
        if self.reserved:
            with self.ledger() as ledger:
                ledger["active"].pop(self.key, None)
            self.reserved = False
        if self.details["status"] != "failed":
            self.details["status"] = "complete"


def _link_profile_logs(stdout, stderr, state):
    """Count shared logs without copying or changing either process's descriptors."""
    streams = [(stdout, 1)]
    if stderr != subprocess.STDOUT:
        streams.append((stderr, 2))
    linked = set()
    paths = []
    for stream, default_fd in streams:
        if stream == subprocess.DEVNULL:
            continue
        if stream == subprocess.PIPE:
            raise ValueError("budgeted profiling requires regular-file logs, not PIPE")
        fd = default_fd if stream is None else (stream if isinstance(stream, int) else stream.fileno())
        info = os.fstat(fd)
        source = Path(os.readlink(f"/proc/self/fd/{fd}"))
        if not source.is_absolute() or not source.is_file():
            raise ValueError("budgeted profiling requires named regular-file node logs")
        identity = (info.st_dev, info.st_ino)
        if identity in linked:
            continue
        log = state["directory"] / ("recorder-and-node.log" if not paths else "recorder-and-node-stderr.log")
        os.link(source, log)
        if not os.path.samestat(info, log.stat()):
            raise RuntimeError("node log changed while creating its profiling hard link")
        linked.add(identity)
        paths.append(str(log))
    state["metadata"]["shared_log_paths"] = paths
    state["metadata"]["recorder_log_path"] = paths[0] if paths else None


def _stat_fields(text):
    # comm can contain spaces and ')'; field 22 is starttime, field 52 exit_code.
    fields = text.rsplit(")", 1)[1].split()
    return fields[0], int(fields[19]), int(fields[49])


def _descendants(pid):
    pending = [pid]
    seen = {pid}
    while pending:
        parent = pending.pop()
        try:
            tasks = list(Path(f"/proc/{parent}/task").iterdir())
        except FileNotFoundError:
            continue
        for task in tasks:
            try:
                children = (task / "children").read_text().split()
            except FileNotFoundError:
                continue
            for child in map(int, children):
                if child not in seen:
                    seen.add(child)
                    pending.append(child)
                    yield child


def _find_node(recorder, binary, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for pid in _descendants(recorder.pid):
            try:
                if not os.path.samefile(f"/proc/{pid}/exe", binary):
                    continue
                identity = _stat_fields(Path(f"/proc/{pid}/stat").read_text())[1]
                fd = os.pidfd_open(pid)
                try:
                    if (not os.path.samefile(f"/proc/{pid}/exe", binary)
                            or _stat_fields(Path(f"/proc/{pid}/stat").read_text())[1] != identity):
                        os.close(fd)
                        continue
                except BaseException:
                    os.close(fd)
                    raise
                return pid, fd, identity
            except (FileNotFoundError, ProcessLookupError):
                continue
        code = recorder.poll()
        if code is not None:
            raise RuntimeError(f"recorder exited {code} before the exact node executable was observed")
        time.sleep(0.01)
    raise RuntimeError(f"node discovery timed out; recorder pid {recorder.pid} was not signalled")


def _tcp_ports(text, inodes):
    ports = []
    for line in text.splitlines()[1:]:
        fields = line.split()
        if len(fields) > 9 and fields[3] == "0A" and fields[9] in inodes:
            address, port = fields[1].split(":")
            if address == "0100007F":
                ports.append(int(port, 16))
    return sorted(set(ports))


def _metrics_ports(pid):
    inodes = set()
    for fd in Path(f"/proc/{pid}/fd").iterdir():
        try:
            target = os.readlink(fd)
        except FileNotFoundError:
            continue
        match = re.fullmatch(r"socket:\[(\d+)\]", target)
        if match:
            inodes.add(match[1])
    return _tcp_ports(Path(f"/proc/{pid}/net/tcp").read_text(), inodes)


def _gdb_commands(path):
    if "\n" in str(path) or "\r" in str(path):
        raise ValueError("gdb output path cannot contain a newline")
    return "\n".join([
        "set pagination off",
        "set print thread-events off",
        "set confirm off",
        "set logging file " + str(path),
        "set logging overwrite off",
        "set logging redirect off",
        "set logging enabled on",
        "handle SIGINT stop noprint nopass",
        "handle SIGTERM nostop noprint pass",
        "handle SIGPIPE SIGUSR1 SIGUSR2 nostop noprint pass",
        "set $node_profile_snapshot = 0",
        "run",
        "while $_isvoid($_exitcode) && $_isvoid($_exitsignal)",
        "set $node_profile_snapshot = $node_profile_snapshot + 1",
        r'printf "=== GDB SNAPSHOT %d ===\n", $node_profile_snapshot',
        "thread apply all bt 12",
        "continue",
        "end",
        "quit",
        "",
    ])


def _sample_gdb(process):
    while not process._sampler_stop.wait(1.5):
        if process.poll() is not None:
            return
        try:
            signal.pidfd_send_signal(process._pidfd, signal.SIGINT)
            process._node_state["metadata"]["diagnostic_interrupts_sent"] += 1
        except ProcessLookupError:
            return
        except OSError as error:
            process._node_state["errors"].append("gdb interrupt: " + str(error))
            return


def _stop_sampling(process):
    event = getattr(process, "_sampler_stop", None)
    if event is not None:
        event.set()
        process._sampler_thread.join()


def _scrape_metrics(process, observation):
    """Read the identified node-owned endpoint; no sleeps or delayed rescrapes."""
    state = process._node_state
    observation.update(started_unix_ns=time.time_ns(), started_monotonic_ns=time.monotonic_ns())
    try:
        if process.poll() is not None:
            raise RuntimeError("node exited before metrics scrape")
        expected = "zakura_build_info" if state["metadata"]["arm"] == "zakura-head" else "zebrad_build_info"
        owned_ports = _metrics_ports(process.pid)
        port = state.get("metrics_port")
        if port is not None and port not in owned_ports:
            raise RuntimeError("previously identified metrics endpoint is no longer node-owned")
        errors = []
        for port in ([port] if port is not None else owned_ports):
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
            try:
                conn.request("GET", "/metrics", headers={"Connection": "close"})
                response = conn.getresponse()
                body = response.read(64 * 1024 * 1024 + 1)
                if len(body) > 64 * 1024 * 1024:
                    raise RuntimeError("metrics response exceeds 64 MiB")
                if response.status == 200 and re.search(
                        rb"(?m)^" + expected.encode() + rb"(?:_total)?[ {]", body):
                    state["metrics_port"] = port
                    observation["endpoint"] = f"127.0.0.1:{port}"
                    state["metadata"]["metrics_endpoint"] = observation["endpoint"]
                    return body
                errors.append(f"{port}: HTTP {response.status}, no {expected} metric")
            except (OSError, http.client.HTTPException) as error:
                errors.append(f"{port}: {error}")
            finally:
                conn.close()
        raise RuntimeError("no node-owned Prometheus endpoint: " + "; ".join(errors))
    finally:
        observation.update(finished_unix_ns=time.time_ns(), finished_monotonic_ns=time.monotonic_ns())
        observation["elapsed_ns"] = observation["finished_monotonic_ns"] - observation["started_monotonic_ns"]


def _phase_values(text, arm):
    """Parse only source-verified duration _count/_sum pairs, never quantiles."""
    families = PHASE_METRICS.get(arm, {})
    values = {}
    for line in text.splitlines():
        match = _PROM_SAMPLE.fullmatch(line)
        if match is None:
            if any(line.startswith(name + suffix) for name in families for suffix in ("_count", "_sum")):
                raise ValueError("malformed phase sample: " + line)
            continue
        name, raw_labels, raw_value = match.groups()
        metric, _, component = name.rpartition("_")
        if metric not in families or component not in ("count", "sum"):
            continue
        labels, position = {}, 0
        raw_labels = raw_labels or ""
        while position < len(raw_labels):
            label = _PROM_LABEL.match(raw_labels, position)
            if label is None or label[1] in labels:
                raise ValueError("invalid/duplicate phase labels: " + raw_labels)
            labels[label[1]] = json.loads(label[2])
            position = label.end()
        if "quantile" in labels or "le" in labels:
            raise ValueError("phase count/sum unexpectedly has quantile/bucket label")
        if families[metric] and labels.get("phase") not in families[metric]:
            raise ValueError("unknown/missing source-verified phase: " + str(labels))
        value = float(raw_value)
        if not math.isfinite(value) or value < 0 or (component == "count" and not value.is_integer()):
            raise ValueError("invalid phase count/sum: " + line)
        pair = values.setdefault((metric, tuple(sorted(labels.items()))), {})
        if component in pair:
            raise ValueError("duplicate phase count/sum: " + line)
        pair[component] = int(value) if component == "count" else value
    for (metric, labels), pair in values.items():
        if pair.keys() != {"count", "sum"}:
            raise ValueError(f"incomplete count/sum pair: {metric} {labels}")
        if pair["count"] == 0 and pair["sum"] != 0:
            raise ValueError(f"nonzero phase sum with zero count: {metric} {labels}")
    return values


def _phase_deltas(before, after, arm):
    series, errors, warnings, unavailable = [], [], [], []
    for metric, phases in PHASE_METRICS.get(arm, {}).items():
        for phase in phases or (None,):
            labels = {"phase": phase} if phase is not None else {}
            if not any(key[0] == metric and labels.items() <= dict(key[1]).items() for key in after):
                unavailable.append({"metric": metric, "labels": labels})
    for key in before.keys() - after.keys():
        errors.append(f"series disappeared after operation: {key}")
    for key, end in sorted(after.items()):
        metric, labels = key
        start = before.get(key, {"count": 0, "sum": 0})
        count, total = end["count"] - start["count"], end["sum"] - start["sum"]
        if count < 0 or total < 0:
            errors.append(f"counter reset/nonmonotonic phase series: {key}")
            continue
        if count == 0 and total != 0:
            errors.append(f"sum changed without count: {key}")
            continue
        first = key not in before
        if first:
            warnings.append(f"new series absent before scrape; assuming lazy initial zero: {key}")
        if count != 1:
            warnings.append(f"expected one observation for submit, observed {count}: {key}")
        series.append({"metric": metric, "labels": dict(labels), "count_delta": count,
                       "sum_delta_seconds": total, "mean_seconds": total / count if count else None,
                       "baseline_assumed_zero": first})
    if unavailable:
        warnings.append("some source-verified phases unavailable after operation; "
                        "conditional paths and prepared consensus bypass are not zero durations")
    return series, errors, warnings, unavailable


def _operation_scrape(process, receipt, which):
    observation = receipt[which] = {"raw_file": None}
    try:
        body = _scrape_metrics(process, observation)
        path = process._node_state["directory"] / f"operation-{receipt['operation_id']}-{which}.prom"
        with path.open("xb") as output:
            output.write(body)
        observation["raw_file"] = path.name
        budget = process._node_state.get("budget")
        if budget is not None:
            budget.check()
        return _phase_values(body.decode("utf-8"), receipt["arm"])
    except Exception as error:
        receipt["errors"].append(f"{which} operation scrape: {error}")
        return None


def begin_operation(process, label):
    """Take the baseline outside the client timer; disabled hooks return None."""
    state = getattr(process, "_node_state", None)
    if not state or not state.get("phase_deltas"):
        return None
    metadata = state["metadata"]
    receipt = {key: metadata[key] for key in (
        "arm", "workload", "process_lifetime_id", "measurement_mode", "metrics_availability")}
    receipt.update(schema_version=1, operation_id=str(uuid.uuid4()), label=label,
                   success=None, client_elapsed_ns=None, before=None, after=None,
                   series=[], unavailable_metrics=[], errors=[], warnings=[])
    receipt["warnings"].append(
        "scrapes bracket, not isolate, the RPC: background/post-return work may contribute; "
        "nested and parallel phases must not be summed as wall time")
    before = _operation_scrape(process, receipt, "before") if state["metrics"] else None
    receipt["begin_hook_finished_monotonic_ns"] = time.monotonic_ns()
    return {"receipt": receipt, "before_values": before, "state": state, "ended": False}


def end_operation(process, token, client_elapsed_ns, success=True):
    """Scrape immediately after the caller timestamps RPC return; persist receipt."""
    ended_ns = time.monotonic_ns()
    if token is None:
        return None
    state = getattr(process, "_node_state", None)
    if state is not token["state"] or token["ended"]:
        raise ValueError("operation token belongs to another process or was already ended")
    receipt = token["receipt"]
    after = _operation_scrape(process, receipt, "after") if state["metrics"] else None
    token["ended"] = True
    receipt.update(success=bool(success), end_hook_started_monotonic_ns=ended_ns)
    if type(client_elapsed_ns) is not int or client_elapsed_ns < 0:
        receipt["errors"].append("client_elapsed_ns must be a nonnegative integer")
    else:
        receipt["client_elapsed_ns"] = client_elapsed_ns
    if token["before_values"] is not None and after is not None:
        series, errors, warnings, unavailable = _phase_deltas(
            token["before_values"], after, receipt["arm"])
        receipt.update(series=series, unavailable_metrics=unavailable)
        receipt["errors"].extend(errors)
        receipt["warnings"].extend(warnings)
    if state["metrics"] and not receipt["series"]:
        receipt["metrics_availability"] = "unavailable"
    if receipt["before"] is not None and receipt["after"] is not None:
        span = receipt["after"]["finished_monotonic_ns"] - receipt["before"]["started_monotonic_ns"]
        receipt["observation_interval_ns"] = span
        receipt["interval_overhead_ns"] = span - client_elapsed_ns if receipt["client_elapsed_ns"] is not None else None
        if receipt["interval_overhead_ns"] is not None and receipt["interval_overhead_ns"] < 0:
            receipt["errors"].append("client timer exceeds bracketing observation interval")
    path = state["directory"] / f"operation-{receipt['operation_id']}.phase-delta.json"
    receipt["receipt_path"] = str(path)
    with path.open("x") as output:
        output.write(json.dumps(receipt, indent=2, allow_nan=False) + "\n")
    state["metadata"].setdefault("operation_receipts", []).append(path.name)
    _save_metadata(state)
    if state.get("budget") is not None:
        state["budget"].check()
    return receipt


def before_stop(process):
    """Keep the separate whole-lifetime scrape, even with per-operation deltas."""
    _stop_sampling(process)
    state = getattr(process, "_node_state", None)
    if not state or not state["metrics"] or state.get("scrape_attempted"):
        return
    state["scrape_attempted"] = True
    try:
        observation = {}
        body = _scrape_metrics(process, observation)
        with (state["directory"] / "phase-metrics.prom").open("xb") as output:
            output.write(body)
        state["metadata"]["metrics_scraped_unix_ns"] = observation["finished_unix_ns"]
    except Exception as error:
        state["errors"].append("metrics scrape: " + str(error))


def _save_metadata(state):
    state["metadata"]["errors"] = list(state["errors"])
    path = state["directory"] / ("profile-metadata.json" if state["metadata"]["sampler"] else "phase-metrics.json")
    metadata = dict(state["metadata"])
    if "profile_budget" in metadata:
        metadata["profile_budget"] = dict(metadata["profile_budget"])
    path.write_text(json.dumps(metadata, indent=2) + "\n")


class ProfiledProcess:
    def __init__(self, recorder, command, state, pid, pidfd, identity):
        self.recorder = recorder
        self.args = command
        self.pid = pid
        self.returncode = None
        self._pidfd = pidfd
        self._identity = identity
        self._node_state = state
        self._finished = False
        self._poller = select.poll()
        self._poller.register(pidfd, select.POLLIN)

    def poll(self):
        if self.returncode is not None:
            return self.returncode
        try:
            status, identity, code = _stat_fields(Path(f"/proc/{self.pid}/stat").read_text())
        except FileNotFoundError:
            status, identity, code = "X", None, None
        if identity == self._identity and status in ("Z", "X"):
            self.returncode = os.waitstatus_to_exitcode(code)
            self._node_state["metadata"]["node_returncode"] = self.returncode
        elif identity != self._identity or self._poller.poll(0):
            self.returncode = 255  # Explicit unknown status, NOT inferred success.
        return self.returncode

    def send_signal(self, sig):
        before_stop(self)
        if self.poll() is None:
            try:
                signal.pidfd_send_signal(self._pidfd, sig)
                self._node_state["metadata"].setdefault("requested_node_signals", []).append(int(sig))
            except ProcessLookupError:
                pass

    def terminate(self):
        self.send_signal(signal.SIGTERM)

    def kill(self):
        self.send_signal(signal.SIGKILL)

    def wait(self, timeout=None):
        deadline = None if timeout is None else time.monotonic() + timeout
        while self.poll() is None:
            if deadline is not None and time.monotonic() >= deadline:
                raise subprocess.TimeoutExpired(self.args, timeout)
            self._poller.poll(20)
        _stop_sampling(self)
        state = self._node_state
        if not self._finished:
            try:
                code = self.recorder.wait(timeout=FLUSH_TIMEOUT)
            except subprocess.TimeoutExpired as error:
                state["errors"].append(f"recorder flush exceeded {FLUSH_TIMEOUT}s; recorder pid {self.recorder.pid} left unsignalled")
                _save_metadata(state)
                raise RuntimeError(state["errors"][-1]) from error
            state["metadata"]["recorder_returncode"] = code
            if code != 0:
                state["errors"].append(f"recorder exited with status {code}; see "
                                       + (state["metadata"].get("recorder_log_path") or "node.log"))
            try:
                if state["profile"] is not None:
                    # Full JSON/symbol checks belong to summarization.
                    with gzip.open(state["profile"], "rb") as profile:
                        if profile.read(1) != b"{":
                            raise ValueError("profile does not start with a JSON object")
                else:
                    path = Path(state["metadata"]["diagnostic_path"])
                    if not path.is_file() or path.stat().st_size == 0:
                        raise ValueError("missing/empty gdb diagnostic output")
            except (OSError, EOFError, ValueError) as error:
                state["errors"].append("missing/invalid profile artifact: " + str(error))
            if state.get("budget") is not None:
                state["budget"].finish()
            state["metadata"]["finished_unix_ns"] = time.time_ns()
            self._finished = True
            _active_processes.discard(self)
            os.close(self._pidfd)
            _save_metadata(state)
        if state["errors"]:
            raise RuntimeError("; ".join(state["errors"]))
        return self.returncode


def spawn(command, stdout=None, stderr=None, env=None, directory=None, arm=None):
    """Keep default launches untouched; own cleanup for opted-in harness children."""
    global _launching
    environment = os.environ if env is None else env
    if not (any(environment.get(key) for key in (
            "NODE_WRAPPER", "NODE_METRICS_ARM", "NODE_METRICS_BINARIES", "NODE_PROFILE_FALLBACK"))
            or environment.get("NODE_PHASE_DELTAS") == "1"):
        return _spawn(command, stdout, stderr, env, directory, arm)
    _install_cleanup()
    _launching = True
    try:
        process = _spawn(command, stdout, stderr, env, directory, arm)
        # Track filtered controls too: SIGTERM must not orphan their ordinary Popen.
        _active_processes.add(process)
        return process
    finally:
        _launching = False
        if _pending_signal is not None:
            _termination_signal(_pending_signal, None)


def _spawn(command, stdout=None, stderr=None, env=None, directory=None, arm=None):
    """Spawn unchanged Popen unless NODE_WRAPPER, metrics, or explicit GDB opts in."""
    environment = os.environ if env is None else env
    wrapper = environment.get("NODE_WRAPPER", "")
    fallback = environment.get("NODE_PROFILE_FALLBACK", "")
    if fallback not in ("", "gdb") or (fallback and wrapper):
        raise ValueError("NODE_PROFILE_FALLBACK=gdb requires an empty NODE_WRAPPER")
    if not (wrapper or fallback or environment.get("NODE_METRICS_ARM")
            or environment.get("NODE_METRICS_BINARIES") or environment.get("NODE_PHASE_DELTAS") == "1"):
        return subprocess.Popen(command, stdout=stdout, stderr=stderr, env=env)
    if directory is None:
        raise ValueError("opt-in node instrumentation requires directory")
    directory = Path(directory).resolve()
    if not _selected(directory, environment):
        return subprocess.Popen(command, stdout=stdout, stderr=stderr, env=env)
    executable = shutil.which(str(command[0]), path=environment.get("PATH", os.defpath))
    if executable is None:
        raise FileNotFoundError(command[0])
    binary = str(Path(executable).resolve(strict=True))
    arm = _arm(binary, arm)
    metrics_arm = _metrics_arm(binary, environment, arm)
    metrics = metrics_arm is not None
    arm = arm or metrics_arm
    phases = _phase_enabled(directory, environment)
    measurement_mode = "profiled" if wrapper or fallback else "metrics-only"
    if phases:
        if arm not in ARMS:
            raise ValueError("phase deltas require a known arm")
        if environment.get("NODE_PHASE_MODE", measurement_mode) != measurement_mode:
            raise ValueError("NODE_PHASE_MODE must match actual recorder presence")
    if metrics:
        config_args = [i for i, arg in enumerate(command[:-1]) if arg in ("-c", "--config")]
        if len(config_args) != 1:
            raise ValueError("metrics injection requires exactly one -c/--config argument")
        config = Path(command[config_args[0] + 1])
        text = config.read_text()
        updated = prepare_config(text, binary, environment, arm, directory)
        if text != updated:
            config.write_text(updated)
    if not wrapper and not fallback and not metrics and not phases:
        return subprocess.Popen(command, stdout=stdout, stderr=stderr, env=env)
    workload = _workload(directory, environment)
    node_directory = directory
    lifetime = str(uuid.uuid4())
    root = environment.get("NODE_PROFILE_ROOT")
    maximum = environment.get("NODE_PROFILE_MAX_BYTES")
    if bool(root) != bool(maximum):
        raise ValueError("NODE_PROFILE_ROOT and NODE_PROFILE_MAX_BYTES must be set together")
    if root:
        root = Path(root).resolve(strict=True)
        if not root.is_dir() or node_directory.is_relative_to(root) or root.is_relative_to(node_directory):
            raise ValueError("profile root and node state directory must be separate directory trees")
        if not re.fullmatch(r"[0-9]+", maximum) or int(maximum) <= 0:
            raise ValueError("NODE_PROFILE_MAX_BYTES must be a positive integer")
        directory = root / lifetime
        directory.mkdir()
    state = {
        "directory": directory, "metrics": metrics, "phase_deltas": phases, "errors": [],
        "profile": directory / "profile.json.gz" if wrapper else None,
        "metadata": {
            "arm": arm, "binary": binary, "workload": workload,
            "process_lifetime_id": lifetime, "node_directory": str(node_directory),
            "log_path": str(getattr(stdout, "name", "")) or None,
            "capture_scope": "whole-run", "sample_rate_hz": None,
            "metrics_scope": "whole-run cumulative pre-stop scrape" if metrics else None,
            "started_unix_ns": time.time_ns(), "node_returncode": None,
            "sampler": "samply" if wrapper else ("gdb" if fallback else None),
            "fallback": bool(fallback),
            "measurement_mode": measurement_mode,
            "metrics_availability": ("phase-deltas" if metrics else
                                     "unavailable" if arm in PHASE_METRICS else "client-total-only"),
        },
    }
    if not wrapper and not fallback:
        process = None
        try:
            if root:
                ProfileBudget(root, int(maximum), state)
                _link_profile_logs(stdout, stderr, state)
                state["budget"].check()
            process = subprocess.Popen(command, stdout=stdout, stderr=stderr, env=env)
            process._node_state = state
            state["metadata"]["node_pid"] = process.pid
            if root:
                state["budget"].process = process
                state["budget"].start(None)
            _save_metadata(state)
            return process
        except Exception as error:
            state["errors"].append("metrics/phase launch: " + str(error))
            if process is not None:
                process.kill()
                process.wait(timeout=10)
            if state.get("budget") is not None:
                state["budget"].finish()
            _save_metadata(state)
            raise
    if (directory / "profile.json.gz").exists() or (directory / "profile-metadata.json").exists():
        raise FileExistsError("refusing to overwrite a prior node lifetime in " + str(directory))
    if fallback:
        diagnostic_path = directory / "gdb-stacks.txt"
        script = directory / "gdb-commands.txt"
        if diagnostic_path.exists() or script.exists():
            raise FileExistsError("refusing to overwrite a prior gdb lifetime in " + str(directory))
        script.write_text(_gdb_commands(diagnostic_path))
        wrapper_args = ["gdb", "-q", "-nx", "-batch", "-x", str(script), "--args"]
        state["metadata"].update(
            profile_path=None, diagnostic_path=str(diagnostic_path),
            diagnostic_interval_s=1.5, diagnostic_stack_depth=12,
            diagnostic_interrupts_sent=0,
            diagnostic_scope="intrusive all-thread wall-clock stops, not sampled CPU time",
        )
    else:
        wrapper_args = _wrapper_args(wrapper, state["profile"], environment.get("NODE_PROFILE_RATE"))
        state["metadata"]["sample_rate_hz"] = int(wrapper_args[wrapper_args.index("--rate") + 1])
        state["metadata"]["profile_path"] = str(state["profile"])
    recorder_exe = shutil.which(wrapper_args[0], path=environment.get("PATH", os.defpath))
    if recorder_exe is None or not os.access(recorder_exe, os.X_OK):
        raise FileNotFoundError("recorder executable is unavailable: " + wrapper_args[0])
    wrapper_args[0] = str(Path(recorder_exe).resolve(strict=True))
    state["metadata"]["recorder_command"] = wrapper_args + list(command)
    recorder = None
    process = None
    try:
        if root:
            ProfileBudget(root, int(maximum), state)
            _link_profile_logs(stdout, stderr, state)
            state["budget"].check()
        recorder = subprocess.Popen(wrapper_args + list(command), stdout=stdout,
                                    stderr=stderr, env=env, start_new_session=True)
        if root:
            state["budget"].start(recorder)
        state["metadata"]["profiler_pid"] = recorder.pid
        pid, pidfd, identity = _find_node(recorder, binary)
        state["metadata"]["node_pid"] = pid
        process = ProfiledProcess(recorder, command, state, pid, pidfd, identity)
        if state.get("budget") is not None:
            state["budget"].process = process
            # prlimit after the exact child exec leaves its already-inherited
            # limits untouched; samply/GDB still launch the original argv directly.
            hard = resource.prlimit(recorder.pid, resource.RLIMIT_FSIZE)[1]
            limit = state["budget"].details["file_limit_bytes"]
            if hard != resource.RLIM_INFINITY:
                limit = min(limit, hard)
            resource.prlimit(recorder.pid, resource.RLIMIT_FSIZE, (limit, hard))
            core_hard = resource.prlimit(recorder.pid, resource.RLIMIT_CORE)[1]
            resource.prlimit(recorder.pid, resource.RLIMIT_CORE, (0, core_hard))
            state["budget"].details.update(file_limit_bytes=limit,
                                          file_limit_applied_unix_ns=time.time_ns())
            state["budget"].check()
        _active_processes.add(process)
        _save_metadata(state)
        if fallback:
            process._sampler_stop = threading.Event()
            process._sampler_thread = threading.Thread(target=_sample_gdb, args=(process,), daemon=True)
            process._sampler_thread.start()
        return process
    except Exception as error:
        state["errors"].append("profile launch: " + str(error))
        budget = state.get("budget")
        if budget is not None:
            try:
                budget.fail(error)
            finally:
                budget.abort_writers()
        if recorder is not None:
            state["metadata"]["recorder_returncode"] = recorder.poll()
        if process is not None:
            try:
                stop(process)
            except Exception as cleanup_error:
                state["errors"].append("launch cleanup: " + str(cleanup_error))
        elif budget is not None:
            if recorder is not None:
                state["metadata"]["recorder_returncode"] = recorder.wait(timeout=10)
            budget.finish()
        _save_metadata(state)
        raise


def stop(process, timeout=20, kill_timeout=10):
    """Scrape, stop the NODE only, and always finish the recorder, even after exit."""
    if process is None:
        return
    state = getattr(process, "_node_state", None)
    if state:
        state["metadata"].setdefault("shutdown_reason", "controlled-node-stop")
    before_stop(process)
    try:
        if process.poll() is None:
            process.terminate()
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=kill_timeout)
    finally:
        if process.returncode is not None and not isinstance(process, ProfiledProcess):
            _active_processes.discard(process)
        state = getattr(process, "_node_state", None)
        if state and not isinstance(process, ProfiledProcess):
            state["metadata"].update(node_pid=process.pid, node_returncode=process.returncode, finished_unix_ns=time.time_ns())
            if state.get("budget") is not None and process.returncode is not None:
                state["budget"].finish()
            _save_metadata(state)
    if state and state["errors"]:
        raise RuntimeError("; ".join(state["errors"]))


def self_check():
    """Pure helpers only: no subprocesses, sockets, files, or benchmark work."""
    template = 'samply record --save-only --no-open -o {profile} --rate 4000 --'
    assert _wrapper_args(template, Path('/path with spaces/profile.json.gz'))[-4:] == ['/path with spaces/profile.json.gz', '--rate', '4000', '--']
    assert _wrapper_args(template, Path('/p'), '1000')[-3:] == ['--rate', '1000', '--']
    assert _wrapper_args(template.replace('4000', '1000'), Path('/p'))[-3:] == ['--rate', '1000', '--']
    for bad in (template.replace('4000', '0'), template.replace('4000', '-1'),
                template.replace('4000', '100001'), template.replace('4000', 'nan'),
                template.replace('{profile}', 'shared.json'), template.replace('samply', 'sh'),
                template.replace(' --rate', ' --duration 1 --rate')):
        try:
            _wrapper_args(bad, Path('/profile.json.gz'))
        except ValueError:
            pass
        else:
            raise AssertionError('accepted invalid wrapper: ' + bad)
    maximum = 15 * 1024 ** 3
    allocation, limit = _budget_share(maximum, 0, 0, 0)
    assert BUDGET_FILE_SLOTS * limit + BUDGET_AUX_BYTES <= allocation
    assert allocation * BUDGET_RECORDERS + BUDGET_LEDGER_BYTES <= maximum
    second, _ = _budget_share(maximum, 100, allocation - 100, 1)
    assert second >= allocation  # Already-written bytes are not charged twice.
    for used, reserved, active in ((maximum, 0, 0), (0, maximum, 1), (0, 0, 3)):
        try:
            _budget_share(maximum, used, reserved, active)
        except RuntimeError:
            pass
        else:
            raise AssertionError("accepted exhausted profile reservation")
    assert _selected('/run/explicit-prepared', {'NODE_PROFILE_MODES': 'explicit-prepared,proposal-prepared'})
    assert not _selected('/run/baseline', {'NODE_PROFILE_MODES': 'explicit-prepared,proposal-prepared'})
    assert _selected('/run/n1', {})
    assert _arm('/builds/zebra-main/zebra-stack') == 'zebra-main'
    assert _arm('/builds/zebra-stack/zebrad') == 'zebra-stack'
    fields = ['0'] * 50
    fields[0], fields[19], fields[49] = 'Z', '1234', str(15)
    assert _stat_fields('42 (name ) with spaces) ' + ' '.join(fields)) == ('Z', 1234, 15)
    tcp = 'header\n0: 0100007F:1F90 00000000:0000 0A 0 0 0 0 0 123\n1: 00000000:1F91 00000000:0000 0A 0 0 0 0 0 124\n'
    assert _tcp_ports(tcp, {'123', '124'}) == [8080]
    commands = _gdb_commands(Path('/output/gdb-stacks.txt'))
    assert 'thread apply all bt 12' in commands
    assert r'printf "=== GDB SNAPSHOT %d ===\n", $node_profile_snapshot' in commands
    assert 'handle SIGTERM nostop noprint pass' in commands
    assert prepare_config('[network]\n', '/binary', {}) == '[network]\n'
    assert _workload(Path('/run/n1'), {'NODE_WORKLOAD': 'node-gbtsapling'}) == 'setup:node-gbtsapling'
    assert _workload(Path('/run/n2'), {'NODE_WORKLOAD': 'node-gbtsapling'}) == 'node-gbtsapling'
    assert _workload(Path('/run/sapling16'), {'NODE_WORKLOAD': 'node-coldsubmit'}) == 'node-coldsubmit:sapling16'
    assert _workload(Path('/run/producer-chain'), {'NODE_WORKLOAD': 'node-coldsubmit'}).startswith('setup:')
    phase_env = {"NODE_PHASE_DELTAS": "1", "NODE_PROFILE_MODES": "explicit-prepared,proposal-prepared"}
    assert _phase_enabled("/run/explicit-prepared", phase_env)
    assert _phase_enabled("/run/proposal-prepared", phase_env)
    assert not _phase_enabled("/run/explicit", phase_env)
    assert not _phase_enabled("/run/explicit-prepared", {})
    assert begin_operation(None, "submitblock") is None
    assert end_operation(None, None, 1) is None
    metrics_env = {"NODE_METRICS_BINARIES": '{"zebra-stack":"/sealed/stack","zakura-head":"/sealed/head"}'}
    assert _metrics_arm("/sealed/head", metrics_env, "zakura-head") == "zakura-head"
    assert not _metrics_enabled("/sealed/main", metrics_env, "zebra-main")
    assert _metrics_enabled("/sealed/stack", {"NODE_METRICS_ARM": "zebra-stack", "NODE_METRICS_BINARY": "/sealed/stack"})
    assert 'endpoint_addr = "127.0.0.1:0"' in prepare_config("[network]\n", "/sealed/head", metrics_env, "zakura-head")
    metric = "state_block_writer_phase_duration_seconds"
    sample = (f'{metric}_count{{phase="queue"}} 1\n'
              f'{metric}_sum{{phase="queue"}} 0.125\n'
              f'{metric}{{phase="queue",quantile="0.5"}} NaN\n')
    one = _phase_values(sample, "zebra-stack")
    two = _phase_values(sample.replace(" 1\n", " 2\n").replace(" 0.125\n", " 0.375\n"), "zebra-stack")
    rows, errors, warnings, missing = _phase_deltas(one, two, "zebra-stack")
    assert not errors and rows[0]["count_delta"] == 1 and rows[0]["sum_delta_seconds"] == .25
    assert rows[0]["mean_seconds"] == .25 and not rows[0]["baseline_assumed_zero"]
    assert any(item["labels"].get("phase") == "contextual_commit" for item in missing)
    rows, errors, warnings, _ = _phase_deltas({}, one, "zebra-stack")
    assert not errors and rows[0]["baseline_assumed_zero"] and warnings
    assert _phase_deltas(two, one, "zebra-stack")[1]  # Resets are errors, never clamped.
    assert _phase_deltas(one, one, "zebra-stack")[0][0]["mean_seconds"] is None
    disappeared = _phase_deltas(one, {}, "zebra-stack")
    assert not disappeared[0] and disappeared[1] and disappeared[3]
    assert _phase_deltas({}, {}, "zebra-main") == ([], [], [], [])
    for bad in (sample.replace(" 0.125\n", " NaN\n"), sample.replace(" 1\n", " 1.5\n"),
                sample.replace(" 0.125\n", " -1\n"), sample.splitlines()[0], sample + sample):
        try:
            _phase_values(bad, "zebra-stack")
        except ValueError:
            pass
        else:
            raise AssertionError("accepted invalid phase sample")
    zakura = "\n".join(f"{name}_count 1\n{name}_sum 0.01" for name in PHASE_METRICS["zakura-head"])
    zakura_values = _phase_values(zakura, "zakura-head")
    assert len(zakura_values) == len(PHASE_METRICS["zakura-head"])
    assert not _phase_deltas({}, zakura_values, "zakura-head")[3]
    print('node_process pure helper checks passed')


if __name__ == '__main__':
    if sys.argv[1:] in ([], ['--help'], ['-h']):
        print(__doc__)
        raise SystemExit(0)
    if sys.argv[1:] != ['--self-check']:
        raise SystemExit('usage: node_process.py --self-check')
    self_check()
