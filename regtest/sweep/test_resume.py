#!/usr/bin/env python3
"""Pure file/selection checks: no binary execution, containers, sockets or nodes."""
import ast
import copy
import json
from pathlib import Path
import tempfile
import tomllib
from types import SimpleNamespace
import unittest
from unittest.mock import patch

try:
    import numpy as np
except ModuleNotFoundError as error:
    if error.name != "numpy":
        raise
    np = None

from build_fixtures import select, checkpoint_changes
from common import (ARMS, ENDPOINTS, GENERATE_ENDPOINT, GENERATE_BLOCKS, NODE_POLICY,
                    MEMPOOL_TX_COST_LIMIT, Node, exact_template, exact_mempool, file_hash, save)
from run_sweep import checkpoint_manifest, load_receipt, needs_attempt, write_observations
from ablation import (BASELINE, PRIMARY, ORCHARD_MINER, GENERATE_ID, select_fixtures,
                      declare_protocol, generated_fixture, case_specs)
if np is not None:
    from analyze import analyze_ablation, ablation_table, analyze_rows, holm_intervals
import run_sweep


def rejected(function):
    try:
        function()
    except RuntimeError:
        return
    raise AssertionError("invalid checkpoint/receipt accepted")


def ablation_check():
    fixtures = [dict(id=f"{kind}-{target}", filling=kind, target_bytes=target,
                     bytes=actual, transaction_count=1 if kind == "empty" else 3)
                for kind, target, actual in (("empty", 2000, 1600),
                                            ("transparent", 1900000, 1899900),
                                            ("orchard", 1900000, 1895000),
                                            ("mixed", 250000, 249900))]
    arms = sorted([BASELINE, *PRIMARY])
    protocol = declare_protocol(arms, fixtures, 20261015, 20261016, 5, 2, ORCHARD_MINER)
    assert select_fixtures(fixtures, ["empty", "orchard-1900000"]) == [fixtures[0], fixtures[2]]
    rejected(lambda: select_fixtures(fixtures, ["missing"]))
    rejected(lambda: select_fixtures(fixtures, ["empty", "empty-2000"]))
    rejected(lambda: declare_protocol(arms, fixtures, 1, 1, 5, 2, ORCHARD_MINER))

    def campaign(stage):
        primary_ids = {item["fixture"] for item in protocol["primaries"].values()}
        normal = fixtures if stage == 1 else [item for item in fixtures if item["id"] in primary_ids]
        measured = [*normal, generated_fixture("sealed-chain")]
        specs = case_specs(arms, normal, protocol, stage)
        manifest = dict(rounds=5, repetitions=2, seed=protocol[f"stage{stage}"]["seed"],
                        ablation_stage=stage, ablation_protocol=protocol,
                        arms={arm: {"sha256": arm} for arm in arms}, sources={"script": "same"},
                        node_policy=NODE_POLICY, fixture_manifest_sha256="sealed-fixtures",
                        fixture_manifest={"fixtures": fixtures}, measurement_fixtures=measured, case_specs=specs)
        rows = []
        lookup = {item["id"]: item for item in measured}
        for spec in specs:
            fixture = lookup[spec["fixture"]]
            for endpoint in spec["endpoints"]:
                for arm in spec["arms"]:
                    factor = 1.0
                    if arm != BASELINE:
                        factor = 1.2
                        if stage == 1 and arm == "minus-single-pass-codec" and fixture["filling"] == "empty" and endpoint == "cold-submitblock":
                            factor = 0.5
                        if stage == 2 and arm == "minus-batch-flush":
                            factor = 1.0
                        if stage == 2 and arm == "minus-template-tx-reuse":
                            factor = 0.9
                    for round_index in range(5):
                        for repetition in range(2):
                            duration = int(1_000_000 * (round_index + 1) * (repetition + 1) * factor)
                            row = dict(round=round_index, repetition=repetition, arm=arm, endpoint=endpoint,
                                       fixture=fixture["id"], filling=fixture["filling"],
                                       block_bytes=fixture["bytes"], transaction_count=fixture["transaction_count"],
                                       binary_sha256=arm, status="ok", latency_ns=duration,
                                       candidate_sha256=fixture["id"])
                            if endpoint == GENERATE_ENDPOINT:
                                row.update(candidate_sha256=None, setup_chain_sha256="sealed-chain",
                                           standalone_getblocktemplate_calls=0,
                                           generate_samples=[dict(start_ns=i * (duration + 1),
                                                                  end_ns=i * (duration + 1) + duration,
                                                                  latency_ns=duration, block_bytes=9165)
                                                             for i in range(GENERATE_BLOCKS)])
                            else:
                                row.update(start_ns=1, end_ns=duration + 1)
                            rows.append(row)
        return manifest, rows

    first, first_rows = campaign(1)
    second, second_rows = campaign(2)
    result = analyze_ablation(first_rows, first, second_rows, second)
    decisions = {row["arm"]: row for row in result["verdicts"]}
    assert decisions["minus-decode-reuse"]["verdict"] == "keep"
    assert decisions["minus-orchard-lazy"]["verdict"] == "keep"
    assert decisions["minus-precompute-on-demand"]["verdict"] == "keep"
    assert decisions["minus-single-pass-codec"]["verdict"] == "remove"
    assert decisions["minus-single-pass-codec"]["regression_check"] == "regression"
    assert decisions["minus-batch-flush"]["verdict"] == "remove"
    assert decisions["minus-template-tx-reuse"]["verdict"] == "remove"
    assert all(len([cell for cell in result["stage2_results"] if cell["arm"] == arm]) == 1 for arm in PRIMARY)
    assert all(line.startswith("|") for line in ablation_table(result).splitlines())
    assert len(ablation_table(result).splitlines()) == len(PRIMARY) + 2
    rejected(lambda: analyze_ablation(first_rows, first, second_rows[:-1], second))
    rejected(lambda: analyze_ablation(first_rows, first, second_rows, dict(second, seed=first["seed"])))
    marginal = np.concatenate((np.full(9800, 0.9), np.full(200, 1.1)))
    assert holm_intervals([marginal])[0]["regression"]
    assert not holm_intervals([marginal, np.ones(10000)])[0]["regression"]
    assert holm_intervals([np.full(10000, 0.5), marginal])[1]["regression"]

    generic = dict(rounds=5, repetitions=2, fixture_manifest={"fixtures": [fixtures[1]]},
                   arms={BASELINE: {"sha256": BASELINE}, "custom-zebra": {"sha256": "custom-zebra"}})
    selected = []
    for row in first_rows:
        if row["fixture"] == fixtures[1]["id"] and row["arm"] in (BASELINE, "minus-decode-reuse"):
            selected.append(dict(row, arm="custom-zebra", binary_sha256="custom-zebra")
                            if row["arm"] != BASELINE else row)
    assert all("custom-zebra_over_optimized" in row["ratios"] for row in analyze_rows(selected, generic))


def nominer_check():
    fixture = dict(parent="parent", hash="candidate", raw="cc",
                   corpus=[dict(txid="tx1", raw="aa", bytes=1), dict(txid="tx2", raw="bb", bytes=1)])
    chain = [dict(hash="parent", raw="00")]
    args = SimpleNamespace(timeout=1, poll_ms=0)

    for endpoint in ("cold-submitblock-nominer", "warm-mempool-submitblock"):
        warm = endpoint == "warm-mempool-submitblock"
        calls, pending, state = [], [], dict(tip="genesis", polls=0)

        def rpc(method, params=None):
            calls.append((method, params))
            if method == "submitblock":
                if params == ["00"]:
                    state["tip"] = "parent"
                else:
                    assert params == [fixture["raw"]]
                    assert pending == (["tx1", "tx2"] if warm else [])
                    if warm:
                        assert state["polls"] == 2
                        assert calls[-2] == ("getbestblockhash", None)
                    state["tip"] = "candidate"
                return None
            if method == "getbestblockhash":
                return state["tip"]
            if method == "getblock":
                assert params == ["candidate", 0]
                return fixture["raw"]
            if method == "sendrawtransaction":
                txid = {"aa": "tx1", "bb": "tx2"}[params[0]]
                pending.append(txid)
                return txid
            if method == "getrawmempool":
                if pending:
                    state["polls"] += 1
                    return pending[:1] if state["polls"] == 1 else pending
                return []
            raise AssertionError(f"unexpected RPC, including templates/proposals: {method}")

        with patch.object(run_sweep, "Node") as constructor:
            constructor.return_value.__enter__.return_value = SimpleNamespace(
                rpc=SimpleNamespace(call=rpc),
                preload_receipt=lambda start: dict(binary_preloads=True, preload_wait=True,
                                                   preload_done_relative_ns=-1))
            row = run_sweep.case("zebra-main", "/unused", Path("/unused"), fixture, chain, args, endpoint)
            assert constructor.call_args.kwargs == {"miner": None}
        assert row["latency_ns"] > 0 and "reply" not in row
        assert row["preload"]["preload_done_relative_ns"] == -1
        assert len([method for method, _ in calls if method == "sendrawtransaction"]) == (2 if warm else 0)
        assert row.get("mempool_polls") == (2 if warm else None)
        assert ("preparation_ns" in row) == warm
        assert calls[-2:] == [("getbestblockhash", None), ("getblock", ["candidate", 0])]

    for mempool, parent in ((["tx1", "tx2", "unrelated"], "parent"), (["tx1", "tx2"], "changed")):
        node = SimpleNamespace(rpc=SimpleNamespace(call=lambda method:
                               mempool if method == "getrawmempool" else parent))
        rejected(lambda: exact_mempool(node, fixture["corpus"], "parent", 1, 0))
    try:
        exact_mempool(node, fixture["corpus"], "parent", 0)
    except TimeoutError:
        pass
    else:
        raise AssertionError("mempool polling must have a deadline")


def main():
    for path in Path(__file__).parent.glob("*.py"):
        ast.parse(path.read_text(), filename=str(path))
    with tempfile.TemporaryDirectory(prefix="size-sweep-resume-") as directory:
        root = Path(directory)
        output = root / "run"
        manifest = dict(arms={"zebra-main": {"sha256": "binary"}},
                        fixture_manifest_sha256="fixtures", sources={"driver": "source"},
                        seed=20261015, schedule=[{"round": 0, "arms": ["zebra-main"]}])
        digest = checkpoint_manifest(output, manifest)
        assert checkpoint_manifest(output, manifest) == digest
        for key in manifest:
            changed = copy.deepcopy(manifest)
            changed[key] = "changed"
            rejected(lambda: checkpoint_manifest(output, changed))
        assert file_hash(output / "manifest.json") == digest
        partial = root / "unsealed"
        partial.mkdir()
        (partial / "existing-evidence").write_text("keep")
        rejected(lambda: checkpoint_manifest(partial, manifest))
        assert (partial / "existing-evidence").read_text() == "keep"

        identity = dict(round=0, repetition=0, arm="zebra-main", fixture="empty-2000",
                        endpoint="cold-submitblock", manifest_sha256=digest)
        path = output / "observations" / "case.json"
        assert load_receipt(path, identity) is None
        assert needs_attempt(None, False) and needs_attempt(None, True)
        failed = dict(identity, status="error", error="synthetic unit-test traceback", attempt="first")
        historical = output / "failures" / "case-first.json"
        save(historical, failed)
        save(path, failed)
        assert load_receipt(path, identity) == failed
        assert not needs_attempt(failed, False) and needs_attempt(failed, True)
        historical_hash = file_hash(historical)
        successful = dict(identity, status="ok", start_ns=10, end_ns=40, latency_ns=30, attempt="second")
        save(path, successful)
        assert load_receipt(path, identity) == successful
        assert not needs_attempt(successful, False) and not needs_attempt(successful, True)
        assert file_hash(historical) == historical_hash
        rejected(lambda: load_receipt(path, dict(identity, arm="zakura-main")))
        save(path, dict(successful, latency_ns=0))
        rejected(lambda: load_receipt(path, identity))
        save(path, successful)
        # An interrupted aggregate never replaces the canonical endpoint receipts.
        (output / "observations.jsonl").write_text('{"partial":')
        write_observations(output, [successful])
        assert [json.loads(line) for line in (output / "observations.jsonl").read_text().splitlines()] == [successful]
        assert not list(output.glob("*.tmp"))
        previous = dict(producer={"sha256": "producer"}, sizes=[2000, 1900000],
                        wallet_image="pinned", sources={"builder": "old"})
        current = dict(previous, sources={"builder": "new"}, node_policy=NODE_POLICY)
        preserved = copy.deepcopy(previous)
        rejected(lambda: checkpoint_changes(previous, current, False))
        assert checkpoint_changes(previous, current, True) == ["node_policy", "sources"]
        rejected(lambda: checkpoint_changes(previous, dict(current, producer={"sha256": "changed"}), True))
        assert previous == preserved
        for arm in (*ARMS, "minus-decode-reuse"):
            node = Node("/unused-unit-check-binary", root / arm, arm)
            assert node.proc is None
            config = tomllib.loads(node.config.read_text())
            assert config["mempool"]["tx_cost_limit"] == MEMPOOL_TX_COST_LIMIT == 400_000_000
            if arm == "zakura-main":
                assert config["network"]["p2p_stack"] == "legacy"
            no_miner = Node("/unused-unit-check-binary", root / (arm + "-nominer"), arm, miner=None)
            assert "miner_address" not in tomllib.loads(no_miner.config.read_text())["mining"]
        log = root / "diagnostic.log"
        log.write_text("synthetic evidence: transaction evicted\n")
        calls = []
        def rpc(method):
            calls.append(method)
            return {"getblocktemplate": {"previousblockhash": "parent", "transactions": []},
                    "getrawmempool": [], "getmempoolinfo": {"size": 0, "bytes": 0}}[method]
        node = SimpleNamespace(log_path=log, rpc=SimpleNamespace(call=rpc))
        try:
            exact_template(node, [{"txid": "expected", "raw": "00", "bytes": 1}], "parent", 1800)
        except RuntimeError as error:
            diagnostic = json.loads(str(error))
            assert diagnostic["chosen_count"] == diagnostic["missing_accepted_count"] == 1
            assert diagnostic["mempool_count"] == 0 and diagnostic["template_calls"] == 1
            assert diagnostic["configured_tx_cost_limit"] == 400_000_000
            assert "evicted" in diagnostic["log_evidence"][0]
        else:
            raise AssertionError("missing accepted transaction did not fail immediately")
        assert calls.count("getblocktemplate") == 1

    corpus = ([dict(kind="transparent", bytes=104)] * 20000
              + [dict(kind="orchard", bytes=4600)] * 500)
    for target in (25000, 100000, 250000, 500000, 1000000, 1500000, 1900000):
        for filling in ("transparent", "orchard", "mixed"):
            indices = select(corpus, filling, target, 1600)
            chosen = [corpus[i] for i in indices]
            assert len(indices) == len(set(indices))
            assert abs(1602 + sum(t["bytes"] for t in chosen) - target) <= max(t["bytes"] for t in chosen)
            assert 1602 + sum(t["bytes"] for t in chosen) <= 2_000_000
            if filling == "mixed":
                transparent = sum(t["bytes"] for t in chosen if t["kind"] == "transparent")
                orchard = sum(t["bytes"] for t in chosen if t["kind"] == "orchard")
                assert abs(transparent - orchard) <= 4600 + 104
    print("pure checks passed: resume/seals, 400M mempool policy, immediate missing-ID diagnostic, selection, syntax")

class ResumeTests(unittest.TestCase):
    def test_resume_and_selection(self):
        main()

    def test_no_miner_submission(self):
        nominer_check()

    def test_burst_times_serial_admission_through_exact_template(self):
        corpus = [dict(txid='a', raw='aa', bytes=1), dict(txid='b', raw='bb', bytes=1)]
        fixture = dict(parent='parent', corpus=corpus)
        calls, admitted = [], []
        template_calls = 0

        def rpc(method, params=None):
            nonlocal template_calls
            calls.append(method)
            if method == 'sendrawtransaction':
                self.assertEqual(clock.call_count, 1, 'admission must be inside the timer')
                tx = corpus[len(admitted)]
                self.assertEqual(params, [tx['raw']])
                admitted.append(tx['txid'])
                return tx['txid']
            if method == 'getrawmempool':
                return admitted
            self.assertEqual(method, 'getblocktemplate')
            self.assertEqual(admitted, ['a', 'b'])
            template_calls += 1
            selected = corpus[:1] if template_calls == 1 else corpus
            return dict(previousblockhash='parent', sizelimit=2_000_000,
                        transactions=[dict(hash=tx['txid'], data=tx['raw']) for tx in selected])

        with patch.object(run_sweep, 'Node') as constructor, \
                patch.object(run_sweep.time, 'perf_counter_ns', side_effect=[100, 900]) as clock, \
                patch.object(run_sweep.time, 'sleep'):
            node = constructor.return_value.__enter__.return_value
            node.rpc.call.side_effect = rpc
            node.replay.side_effect = lambda chain: self.assertEqual(clock.call_count, 0)
            node.preload_receipt.return_value = {}
            row = run_sweep.case('stack', '/unused', Path('/unused'), fixture, [],
                                 SimpleNamespace(timeout=1, poll_ms=1), 'admission-to-template')
        self.assertEqual((row['start_ns'], row['end_ns'], row['latency_ns']), (100, 900, 800))
        self.assertEqual(row['template_polls'], 2)
        self.assertEqual(row['admitted_transactions'], row['template_transactions'])
        self.assertEqual(calls, ['sendrawtransaction', 'sendrawtransaction',
                                'getblocktemplate', 'getrawmempool', 'getblocktemplate'])

    @unittest.skipIf(np is None, "numpy is not installed; analysis checks only")
    def test_ablation_analysis(self):
        ablation_check()


if __name__ == "__main__":
    unittest.main()
