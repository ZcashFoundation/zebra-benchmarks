#!/usr/bin/env python3
"""Shared provenance, receipts and generated-config node lifecycle for front benchmarks."""

import hashlib
import http.client
import json
import os
from pathlib import Path
import platform
import re
import signal
import subprocess
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

from node_bench import RPC, Node, binary_preloads, env as node_env, resources, summary, next_port


class CheckedConnection(http.client.HTTPConnection):
    def getresponse(self):
        response = super().getresponse()
        if not 200 <= response.status < 300:
            response.close()
            self.close()
            raise RuntimeError(f"RPC HTTP status {response.status}")
        return response


class CheckedRPC(RPC):
    def __init__(self, address, timeout=30):
        super().__init__(address, timeout)
        self.conn = CheckedConnection(self.conn.host, self.conn.port, timeout=timeout)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def file_hash(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def save_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + "\n")


@dataclass(frozen=True)
class Arm:
    label: str
    path: Path
    sha256: str
    provenance: dict = field(default_factory=dict)


def load_arms(provenance_path, arm_specs):
    provenance = json.loads(Path(provenance_path).read_text())
    if not arm_specs:
        raise ValueError("at least one --arm label=path is required")
    arms, seen = [], set()
    for spec in arm_specs:
        label, separator, path = spec.partition("=")
        if not separator or not path or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", label):
            raise ValueError("invalid --arm label=path: " + spec)
        if label in seen or label not in provenance:
            raise ValueError("duplicate or unknown arm: " + label)
        seen.add(label)
        metadata = provenance[label]
        binary = Path(path).expanduser().resolve(strict=True)
        digest = file_hash(binary)
        if digest != metadata.get("sha256"):
            raise ValueError("binary sha256 does not match provenance: " + label)
        if not os.access(binary, os.X_OK):
            raise ValueError("binary is not executable: " + str(binary))
        arms.append(Arm(label, binary, digest, metadata))
    return arms


def interleave(labels, repetition):
    labels = list(labels)
    if not labels:
        return []
    shift = repetition % len(labels)
    return labels[shift:] + labels[:shift]


def generate_config(binary, path):
    path = Path(path)
    subprocess.run([str(binary), "generate", "-o", str(path)], check=True,
                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120,
                   env=clean_env())
    return path.read_text()


def set_key(text, section, key, value_literal):
    """Edit a generated scalar/array entry without leaving array continuation lines."""
    block = re.search(r"^\[" + re.escape(section) + r"\][ \t]*\n(.*?)(?=^\[|\Z)",
                      text, re.M | re.S)
    if not block:
        raise ValueError("missing generated-config section: " + section)
    body = block[1]
    entry = re.search(r"^[ \t]*(?:#[ \t]*)?" + re.escape(key) + r"[ \t]*=[ \t]*(.*)(?:\n|\Z)",
                      body, re.M)
    line = f"{key} = {value_literal}\n"
    if entry:
        end = entry.end()
        if entry[1].lstrip().startswith("[") and "]" not in entry[1]:
            closing = re.search(r"^[ \t]*(?:#[ \t]*)?\][^\n]*(?:\n|\Z)", body[end:], re.M)
            if not closing:
                raise ValueError("unterminated generated-config array: " + key)
            end += closing.end()
        body = body[:entry.start()] + line + body[end:]
    else:
        body = line + body
    return text[:block.start(1)] + body + text[block.end(1):]


def reflink_copy(src, dst):
    src, dst = Path(src).resolve(strict=True), Path(dst).absolute()
    if not src.is_dir() or dst.exists() or dst.is_symlink() or src in dst.parents:
        raise ValueError("reflink source must be a directory; destination must be new and outside source")
    subprocess.run(["cp", "-a", "--reflink=always", "--", str(src), str(dst)],
                   check=True, timeout=3600)


class Campaign:
    def __init__(self, output_dir, args, arms):
        self.output = Path(output_dir).expanduser().resolve()
        self.output.mkdir(parents=True, exist_ok=False)
        (self.output / "nodes").mkdir()
        (self.output / "failures").mkdir()
        self.arms = {arm.label: arm for arm in arms}
        self.observations = self.failed_observations = self.failures = 0
        self.lock = threading.Lock()
        self.manifest = dict(
            schema=1, args=args,
            arms={arm.label: dict(path=str(arm.path), sha256=arm.sha256,
                                  binary_preloads=binary_preloads(str(arm.path.resolve())),
                                  provenance=arm.provenance) for arm in arms},
            host=dict(platform=platform.platform(), machine=platform.machine(),
                      cpu_count=os.cpu_count(), affinity=sorted(os.sched_getaffinity(0))),
            clock=vars(time.get_clock_info("perf_counter")),
            start_utc=utc_now(), end_utc=None,
        )
        save_json(self.output / "manifest.json", self.manifest)
        self.stream = (self.output / "observations.jsonl").open("a")

    def observe(self, row):
        row = dict(row)
        if "arm" in row:
            digest = self.arms[row["arm"]].sha256
            row.setdefault("binary_sha256", digest)
            row.setdefault("arm_sha256", digest)
        with self.lock:
            self.stream.write(json.dumps(row, sort_keys=True, default=str) + "\n")
            self.stream.flush()
            self.observations += 1
            self.failed_observations += row.get("status") in ("error", "failed")
        return row

    def fail(self, name, info):
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", name):
            raise ValueError("unsafe failure receipt name")
        with self.lock:
            path = self.output / "failures" / (name + ".json")
            with path.open("x") as stream:
                json.dump(info, stream, indent=2, sort_keys=True, default=str)
                stream.write("\n")
            self.failures += 1

    def complete(self, success):
        with self.lock:
            self.stream.close()
            self.manifest["end_utc"] = utc_now()
            save_json(self.output / "manifest.json", self.manifest)
            completion = dict(
                success=bool(success and not self.failures and not self.failed_observations),
                observations=self.observations, failed_observations=self.failed_observations,
                failure_receipts=self.failures,
                manifest_sha256=file_hash(self.output / "manifest.json"),
                observations_sha256=file_hash(self.output / "observations.jsonl"),
                end_utc=self.manifest["end_utc"],
            )
            save_json(self.output / "completion.json", completion)
        return completion


def clean_env():
    environment = node_env()
    environment.pop("RAYON_NUM_THREADS", None)
    return environment


class MainnetNode(Node):
    def __init__(self, binary, directory, edits=(), cpus=None, env=None):
        self.binary, self.dir = str(binary), Path(directory).resolve()
        self.dir.mkdir(parents=True, exist_ok=True)
        self.config = self.dir / "config.toml"
        text = generate_config(self.binary, self.config)
        defaults = [
            ("network", "network", '"Mainnet"'),
            ("network", "listen_addr", json.dumps(f"127.0.0.1:{next_port()}")),
            ("network", "external_addr", '"0.0.0.0:0"'),
            ("network", "initial_mainnet_peers", "[]"),
            ("network", "initial_testnet_peers", "[]"),
            ("network", "cache_dir", json.dumps(str(self.dir / "network"))),
            ("state", "cache_dir", json.dumps(str(self.dir / "state"))),
            ("state", "ephemeral", "false"),
            ("rpc", "listen_addr", json.dumps(f"127.0.0.1:{next_port()}")),
            ("rpc", "enable_cookie_auth", "false"),
            ("tracing", "use_color", "false"),
            ("tracing", "force_use_color", "false"),
        ]
        for section, key, value in [*defaults, *edits]:
            text = set_key(text, section, key, value)
        self.config.write_text(text)
        self.log_path = self.dir / "zebrad.log"
        self.env = clean_env() if env is None else env
        self.cpus = cpus
        self.proc = self.log = self._client = None
        self.rpc_addr = self.p2p_addr = None
        self.exec_ns = self.ready_ns = self.exit_ns = None
        self.exit_resources = None

    @property
    def pid(self):
        return self.proc.pid if self.proc else None

    def start(self):
        if self.proc is not None:
            raise RuntimeError("node instances cannot be restarted")
        command = [self.binary, "-c", str(self.config), "start"]
        if self.cpus is not None:
            cpus = self.cpus
            if cpus == "all":
                cpus = ",".join(map(str, sorted(os.sched_getaffinity(0))))
            command = ["taskset", "-c", cpus, *command]
        self.init_preload()
        self.log = self.log_path.open("wb")
        self.exec_ns = time.perf_counter_ns()
        try:
            self.proc = subprocess.Popen(command, stdout=self.log,
                                         stderr=subprocess.STDOUT, env=self.env)
        except BaseException:
            self.log.close()
            raise
        return self

    def rpc(self, method, params=None):
        if self._client is None:
            self._scan()
            if not self.rpc_addr:
                raise ConnectionError("RPC endpoint not open yet")
            self._client = CheckedRPC(self.rpc_addr, timeout=1)
        return self._client.call(method, params)

    def wait_rpc_ready(self, timeout):
        deadline, last = time.monotonic() + timeout, None
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"node exited before RPC readiness: {self.proc.returncode}")
            try:
                self.rpc("getblockcount")
                self.ready_ns = time.perf_counter_ns()
                break
            except Exception as error:
                last = error
            # Startup latency resolution: the poll interval bounds how late readiness is observed.
            time.sleep(0.005)
        else:
            raise TimeoutError(f"RPC not ready: {last}")
        self.wait_preload()
        return self.ready_ns

    def sample(self):
        sample = resources(self.pid)
        return dict(cpu_s=sample["cpu_s"], rss_kib=sample["rss"] // 1024,
                    hwm_kib=sample["hwm"] // 1024)

    def wait_exit(self, timeout):
        deadline = time.monotonic() + timeout
        while self.proc.returncode is None:
            exited = os.waitid(os.P_PID, self.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
            if exited is not None:
                self.exit_ns = time.perf_counter_ns()
                try:
                    final_sample = self.sample()
                except (OSError, ValueError):
                    final_sample = None
                _, status, usage = os.wait4(self.pid, 0)
                self.proc.returncode = os.waitstatus_to_exitcode(status)
                self.exit_resources = dict(cpu_s=usage.ru_utime + usage.ru_stime,
                                           hwm_kib=usage.ru_maxrss, proc_sample=final_sample)
                break
            if time.monotonic() >= deadline:
                raise subprocess.TimeoutExpired(self.proc.args, timeout)
            time.sleep(min(0.02, max(0, deadline - time.monotonic())))
        return self.proc.returncode

    def stop(self, timeout=30):
        try:
            if self.proc is not None and self.proc.poll() is None:
                self.proc.send_signal(signal.SIGINT)
                try:
                    self.proc.wait(timeout)
                except subprocess.TimeoutExpired:
                    self.proc.kill()
                    self.proc.wait(10)
            return self.proc.returncode if self.proc else None
        finally:
            if self._client:
                self._client.conn.close()
            if self.log:
                self.log.close()


def summarize(rows, keys, value):
    groups = {}
    for row in rows:
        if row.get("status", "ok") != "ok" or row.get(value) is None:
            continue
        groups.setdefault(tuple(row[key] for key in keys), []).append(row[value])
    result = []
    for group, values in sorted(groups.items()):
        stats = summary(values)
        result.append(dict(zip(keys, group), metric=value,
                           **{key: stats[key] for key in ("n", "median", "min", "max")}))
    return result


def print_summary(rows, keys, value):
    for row in summarize(rows, keys, value):
        label = " / ".join(str(row[key]) for key in keys)
        print(f"{label}: {value} {row['median']:.6g} [{row['min']:.6g}, {row['max']:.6g}] n={row['n']}")
