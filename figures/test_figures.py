#!/usr/bin/env python3
"""Raw-source checks, no nodes or rendering.

Run from figures/: python3 -B -m unittest
Or from the repository root: python3 -B figures/test_figures.py
"""
import json
import unittest
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import figures as f


def rejects(call):
    try:
        call()
    except (ValueError, FileNotFoundError):
        return
    raise AssertionError("invalid or incomplete v5 data accepted")


def self_test():
    with patch.object(f, "render_v5") as render, patch("sys.stdout", new_callable=StringIO) as stdout:
        try:
            f.main(["--help"])
        except SystemExit as result:
            assert result.code == 0
        else:
            raise AssertionError("--help did not exit")
        render.assert_not_called()
        assert "--input" in stdout.getvalue() and "--output" in stdout.getvalue()
        f.main(["--input", "campaign", "--output", "rendered"])
        render.assert_called_once_with(Path("campaign"), Path("rendered"))

    sweep = []
    for filling in (*f.FILLINGS, "transparent-signed"):
        sizes = (250000, 1900000) if filling == "transparent-signed" else (
            25000, 100000, 250000, 500000, 1000000, 1500000, 1900000)
        for size in sizes:
            fixture = f"{filling}-{size}"
            for endpoint in ("admission-to-template", "prepared-submitblock", "cold-submitblock"):
                for arm in f.V5_ARMS:
                    for sample in range(9):
                        sweep.append(dict(arm=arm, fixture=fixture, endpoint=endpoint, filling=filling,
                                          block_bytes=size - 7, candidate_sha256=fixture,
                                          setup_chain_sha256="sealed-chain", latency_ns=(sample + 1) * 1_000_000))
    summarized = f.v5_sweep(sweep)
    assert len(summarized) == 69
    assert summarized[0]["release_latency_ms_median"] == 5
    assert summarized[0]["stack_latency_ms_min"] == 1
    assert summarized[0]["crypto_latency_ms_max"] == 9
    assert summarized[0]["block_bytes"] == 24993
    rejects(lambda: f.v5_sweep(sweep[:-1]))
    rejects(lambda: f.v5_sweep([dict(sweep[0], candidate_sha256="other-block"), *sweep[1:]]))
    rejects(lambda: f.v5_sweep([dict(sweep[0], arm="unknown-arm"), *sweep[1:]]))
    rejects(lambda: f.v5_sweep([dict(sweep[0], latency_ns=float("nan")), *sweep[1:]]))
    core_sweep = [row for row in sweep if row["fixture"].endswith(("-250000", "-1900000"))
                  and row["latency_ns"] <= 5_000_000]
    assert len(f.v5_sweep(core_sweep, core=True)) == 24
    rejects(lambda: f.v5_sweep(core_sweep[:-1], core=True))

    empty = [dict(mode=mode, round=r, arm=arm, latency_ns=latency * 1_000_000)
             for mode in ("local", "peer", "settled") for r in range(6) for arm in f.V5_ARMS
             for latency in (r + 1, r + 2, 1000)]
    assert f.v5_empty_tip(empty)["local", "release"] == 4.5  # Median of round medians, not pooled samples.
    rejects(lambda: f.v5_empty_tip(empty[:-1]))
    signed = [dict(arm=arm, hop=hop) for arm in f.V5_ARMS for hop in (-4, -2)]
    assert f.v5_medians(f.v5_groups(signed, (), lambda row: row["hop"], repetitions=2))[()]["crypto"] == -3
    rejects(lambda: f.v5_groups(signed, (), lambda row: row["hop"], arms=f.V5_ZEBRA))
    assert f.v5_template_window(dict(latency_ns=7_000_000, first_template_latency_ns=2_000_000)) == 5
    rejects(lambda: f.v5_template_window(dict(latency_ns=1, first_template_latency_ns=2, _source="sample")))
    core_empty = [dict(mode=mode, round=0, arm=arm, latency_ns=n * 1_000_000)
                  for mode in ("local", "settled") for arm in f.V5_ARMS for n in range(1, 6)]
    assert f.v5_empty_tip(core_empty, core=True)["settled", "release"] == 3
    fixtures = ("empty-2000", *(f"{filling}-{size}" for filling in
                ("transparent-signed", "orchard", "mixed") for size in (250000, 1900000)))
    tips = [dict(arm=arm, fixture=fixture, temperature="warm", settle_seconds=20,
                 miner="transparent", latency_ns=100, first_template_latency_ns=50)
            for fixture in fixtures for arm in f.V5_ARMS for _ in range(5)]
    miners = [dict(row, miner="orchard") for row in tips if row["arm"] in f.V5_ZEBRA
              and row["fixture"] in ("empty-2000", "mixed-250000", "mixed-1900000")]
    chains = [dict(row, temperature="cold", b_best_tip_latency_ns=20,
                   c_best_tip_latency_ns=60, b_to_c_observed_ns=40)
              for row in tips if row["fixture"] == "empty-2000" or row["fixture"].endswith("-1900000")]
    plotted = []

    def check_panel(records, dimensions, labels, metrics, name, arms=f.V5_ARMS, **kwargs):
        for _, metric in metrics:
            f.v5_groups(records, dimensions, metric, arms=arms, expected=labels, repetitions=5)
        plotted.append(name)

    with patch.object(f, "v5_metric_panels", side_effect=check_panel):
        f.v5_mining(tips, miners, chains, core=True)
    assert set(plotted) == {"tip-template-settled-warm", "tip-miner-transparent",
                            "tip-miner-orchard", "relay-chain-cold"}

    with TemporaryDirectory() as temporary:
        root = Path(temporary) / "v5"
        root.mkdir()
        rejects(lambda: f.v5_receipts(root, "sweep"))
        arms = {arm: dict(path=f"binaries/{arm}", sha256=f"hash-{arm}") for arm in f.V5_ARMS}
        manifest = dict(profile="core", arms=arms, pieces=["rpc", "empty-gbt", "empty-admission-0"],
                        snapshot_height=2316084, replay_heights=[1750000, 1752000])
        (root / "campaign.json").write_text(json.dumps(manifest))
        rejects(lambda: f.v5_campaign(root))
        (root / "completed").mkdir()
        marker = root / "completed/rpc.json"
        marker.write_text(json.dumps(dict(success=False)))
        rejects(lambda: f.v5_campaign(root))
        marker.write_text(json.dumps(dict(success=True)))
        assert f.v5_campaign(root) == arms
        manifest["pieces"].append("inclusion")
        arms["zakura-main"]["source"] = f.V5_TIMEOUT_SOURCE
        (root / "campaign.json").write_text(json.dumps(manifest))
        assert f.v5_failed_arms(arms) == {"inclusion": "zakura-main"}
        assert f.v5_campaign(root) == arms
        arms["zakura-main"]["source"] = "f5c20398c629c8179364b07f059da3c814af78ec"
        (root / "campaign.json").write_text(json.dumps(manifest))
        assert f.v5_failed_arms(arms) == {}
        rejects(lambda: f.v5_campaign(root))  # New builds cannot inherit historical failures.
        (root / "completed/inclusion.json").write_text(json.dumps(dict(success=True)))
        assert f.v5_campaign(root) == arms
        (root / "rpc").mkdir()
        path = root / "rpc/observations.jsonl"
        row = dict(arm="release", scenario="rpc-load", repetition=0, concurrency=1,
                   binary_sha256="hash-release", status="ok", successful_rps=12.5)
        request = dict(arm="release", scenario="rpc-request", status="ok")
        path.write_text(json.dumps(request) + "\n" + json.dumps(row) + "\n")
        completion = root / "rpc/completion.json"
        completion.write_text(json.dumps(dict(success=True, observations=2)))
        select = lambda record: record.get("scenario") == "rpc-load"
        records = f.v5_receipts(root, "rpc", select, arms=arms)
        assert len(records) == 1 and records[0]["successful_rps"] == 12.5
        assert records[0]["_source"] == "rpc/observations.jsonl"
        for invalid in (dict(row, status="error"), dict(row, binary_sha256="old-binary")):
            path.write_text(json.dumps(request) + "\n" + json.dumps(invalid) + "\n")
            rejects(lambda: f.v5_receipts(root, "rpc", select, arms=arms))
        path.write_text((json.dumps(row) + "\n") * 2)
        rejects(lambda: f.v5_receipts(root, "rpc", select, arms=arms))
        path.write_text(json.dumps(row) + "\n")
        rejects(lambda: f.v5_receipts(root, "rpc", select, arms=arms))  # Completion count no longer matches.
        # A documented failed arm is dropped only if it timed out once per repetition of the others.
        timeout = dict(row, arm="zakura-main", binary_sha256="hash-zakura-main", success=False,
                       stop_reason="timeout")

        def documented(*rows, receipt=True):
            path.write_text("".join(json.dumps(r) + "\n" for r in rows))
            completion.unlink(missing_ok=True)
            if receipt:
                completion.write_text(json.dumps(dict(success=False, observations=len(rows))))
            return lambda: f.v5_receipts(root, "rpc", select, arms=arms, failed_arm="zakura-main")

        assert [r["arm"] for r in documented(request, row, timeout)()] == ["release"]
        rejects(lambda: f.v5_receipts(root, "rpc", select, arms=arms))  # Undocumented failure.
        rejects(documented(request, row, timeout, receipt=False))
        rejects(documented(request, row))  # No Zakura run at all.
        rejects(documented(request, row, timeout, timeout))  # Duplicate run.
        rejects(documented(request, row, dict(timeout, repetition=1)))  # Not the others' repetition.
        rejects(documented(request, row, dict(timeout, success=True)))  # It did not fail.
        rejects(documented(request, row, dict(timeout, stop_reason="error")))  # Failed differently.
        rejects(documented(request, row, dict(timeout, binary_sha256="old-binary")))
        rejects(documented(dict(request, status="error"), row, timeout))  # Failure outside the arm.


class FigureTests(unittest.TestCase):
    def test_v5_sources_and_cli(self):
        self_test()


if __name__ == "__main__":
    unittest.main()
