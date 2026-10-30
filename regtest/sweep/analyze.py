#!/usr/bin/env python3
"""Paired round-cluster bootstrap of median latency ratios; PNG and SVG figures."""
import argparse
import csv
import json
from pathlib import Path
import random
import tempfile

try:
    import numpy as np
except ModuleNotFoundError as error:
    if error.name != "numpy":
        raise
    np = None

from common import (ARMS, ENDPOINTS, MAIN_REVISIONS, GENERATE_ENDPOINT,
                    file_hash, require, save, validate_timing)
from ablation import BASELINE, GENERATE_ID, case_specs, load_completed, stage1_reference

SEED, RESAMPLES = 20261015, 10_000
FILLINGS = ("empty", "transparent", "orchard", "mixed", "transparent-signed")
COMPARISONS = {
    "zakura_over_optimized": ("zakura-main", "zebra-optimized"),
    "zakura_over_zebra_main": ("zakura-main", "zebra-main"),
    "main_over_optimized": ("zebra-main", "zebra-optimized"),
}


def bootstrap_medians(values, draws):
    """values[round, repetition]; resample whole matched rounds, never lone arms."""
    require(values.ndim == 2 and np.all(np.isfinite(values)) and np.all(values > 0),
            "positive finite matched observations")
    return np.median(values[draws].reshape(len(draws), -1), axis=1)


def interval(values, draws):
    boot = bootstrap_medians(values, draws)
    low, high = np.quantile(boot, [0.025, 0.975])
    return dict(median=float(np.median(values)), low=float(low), high=float(high))


def comparisons(arms):
    result = {name: pair for name, pair in COMPARISONS.items() if set(pair) <= set(arms)}
    result.update({arm + "_over_optimized": (arm, BASELINE)
                   for arm in arms if arm not in ARMS})
    return result


def indexed_rows(rows, manifest):
    rounds, repetitions = manifest["rounds"], manifest["repetitions"]
    require(rounds >= 2 and repetitions >= 1, "at least two process rounds for intervals; one round is smoke only")
    fixtures = manifest.get("measurement_fixtures", manifest["fixture_manifest"]["fixtures"])
    known = {fixture["id"]: fixture for fixture in fixtures}
    require(len(known) == len(fixtures)
            and all(fixture["filling"] in (*FILLINGS, "orchard-coinbase") for fixture in fixtures),
            "distinct supported fixtures")
    specs = manifest.get("case_specs", [dict(fixture=name, arms=list(manifest["arms"]),
                                            endpoints=list(ENDPOINTS)) for name in known])
    cells = [(spec["fixture"], arm, endpoint) for spec in specs
             for arm in spec["arms"] for endpoint in spec["endpoints"]]
    require(len(cells) == len(set(cells)) and all(name in known and arm in manifest["arms"]
            and endpoint in (*ENDPOINTS, GENERATE_ENDPOINT) for name, arm, endpoint in cells),
            "unique declared endpoint/fixture/arm cells")
    expected = {(r, n, arm, name, endpoint) for r in range(rounds) for n in range(repetitions)
                for name, arm, endpoint in cells}
    indexed, candidates = {}, {}
    for row in rows:
        key = (row["round"], row["repetition"], row["arm"], row["fixture"], row["endpoint"])
        require(key in expected and key not in indexed, "unknown or duplicate observation: " + str(key))
        fixture = known[row["fixture"]]
        require(row["status"] == "ok", "only successful observations")
        validate_timing(row)
        if row["endpoint"] == GENERATE_ENDPOINT:
            require(fixture["kind"] == "generated" and row["candidate_sha256"] is None
                    and row["setup_chain_sha256"] == fixture["setup_chain_sha256"]
                    and row["standalone_getblocktemplate_calls"] == 0, "declared generation workload")
        require(row["block_bytes"] == fixture["bytes"] and row["filling"] == fixture["filling"]
                and row["transaction_count"] == fixture["transaction_count"], "actual fixture dimensions")
        require(row["binary_sha256"] == manifest["arms"][row["arm"]]["sha256"], "observed binary identity")
        identity = candidates.setdefault(row["fixture"], row["candidate_sha256"])
        require(identity == row["candidate_sha256"], "paired arms must submit identical candidate bytes")
        indexed[key] = row["latency_ns"]
    require(set(indexed) == expected, "incomplete observation matrix; failed/missing cases cannot be discarded")
    return fixtures, indexed, candidates


def analyze_rows(rows, manifest):
    fixtures, indexed, candidates = indexed_rows(rows, manifest)
    rounds, repetitions = manifest["rounds"], manifest["repetitions"]
    pairs = comparisons(manifest["arms"])
    draws = np.random.default_rng(SEED).integers(rounds, size=(RESAMPLES, rounds))
    results = []
    for endpoint in ENDPOINTS:
        for fixture in sorted(fixtures, key=lambda f: (f["filling"], f["bytes"])):
            arrays = {arm: np.array([[indexed[(r, n, arm, fixture["id"], endpoint)]
                                     for n in range(repetitions)] for r in range(rounds)], dtype=float)
                      for arm in manifest["arms"]}
            results.append(dict(
                endpoint=endpoint, fixture=fixture["id"], filling=fixture["filling"],
                block_bytes=fixture["bytes"], target_bytes=fixture["target_bytes"],
                transaction_count=fixture["transaction_count"], candidate_sha256=candidates[fixture["id"]],
                paired_observations=rounds * repetitions,
                ratios={name: interval(arrays[numerator] / arrays[denominator], draws)
                        for name, (numerator, denominator) in pairs.items()},
                latency_ms={arm: interval(values / 1e6, draws) for arm, values in arrays.items()}))
    return results


def ablation_cells(rows, manifest):
    fixtures, indexed, _ = indexed_rows(rows, manifest)
    rounds, repetitions = manifest["rounds"], manifest["repetitions"]
    draws = np.random.default_rng(SEED).integers(rounds, size=(RESAMPLES, rounds))
    cells = []
    for arm in manifest["ablation_protocol"]["primaries"]:
        for fixture in fixtures:
            for endpoint in (*ENDPOINTS, GENERATE_ENDPOINT):
                if (0, 0, arm, fixture["id"], endpoint) not in indexed:
                    continue
                arrays = {name: np.array([[indexed[(r, n, name, fixture["id"], endpoint)]
                                          for n in range(repetitions)] for r in range(rounds)], dtype=float)
                          for name in (BASELINE, arm)}
                ratios = arrays[arm] / arrays[BASELINE]
                boot = bootstrap_medians(ratios, draws)
                low, high = np.quantile(boot, [0.025, 0.975])
                cells.append((dict(
                    arm=arm, fixture=fixture["id"], endpoint=endpoint, filling=fixture["filling"],
                    target_bytes=fixture["target_bytes"], block_bytes=fixture["bytes"],
                    paired_observations=rounds * repetitions,
                    ratio=dict(median=float(np.median(ratios)), low=float(low), high=float(high)),
                    median_latency_ms={name: float(np.median(values) / 1e6) for name, values in arrays.items()}), boot))
    return cells


def holm_intervals(bootstraps, alpha=0.05):
    """Invert two-sided percentile-bootstrap tests with Holm's step-down correction.

    Other cells' null-ratio tail probabilities are held fixed during each inversion.
    Thus an adjusted interval excludes 1 only when that cell's Holm test rejects;
    these are test-inverted intervals at the predeclared null, not joint confidence
    bands over arbitrary effect vectors.
    """
    count = len(bootstraps)
    require(count > 0, "nonempty regression family")
    probabilities = np.array([
        min(1.0, 2 * min((np.count_nonzero(samples <= 1) + 1) / (len(samples) + 1),
                         (np.count_nonzero(samples >= 1) + 1) / (len(samples) + 1)))
        for samples in bootstraps])
    order = np.argsort(probabilities, kind="stable")
    adjusted = np.empty(count)
    adjusted[order] = np.minimum(1, np.maximum.accumulate(
        probabilities[order] * np.arange(count, 0, -1)))
    results = []
    for index, samples in enumerate(bootstraps):
        passed = 0
        for probability in sorted(np.delete(probabilities, index)):
            if probability > alpha / (count - passed):
                break
            passed += 1
        critical_alpha = alpha / (count - passed)
        low, high = np.quantile(samples, [critical_alpha / 2, 1 - critical_alpha / 2])
        if adjusted[index] > alpha:
            # Respect the finite-resample +1 tail correction at a quantile boundary.
            low, high = min(low, 1.0), max(high, 1.0)
        results.append(dict(low=float(low), high=float(high), adjusted_p=float(adjusted[index]),
                            unadjusted_p=float(probabilities[index]), critical_alpha=critical_alpha,
                            family_size=count, regression=bool(high < 1)))
    return results


def analyze_ablation(stage1_rows, stage1, stage2_rows, stage2):
    protocol = stage1["ablation_protocol"]
    require(stage1["ablation_stage"] == 1 and stage2["ablation_stage"] == 2
            and protocol == stage2["ablation_protocol"], "identical predeclared two-stage protocol")
    require(all(stage1[key] == stage2[key] for key in
                ("arms", "sources", "node_policy", "fixture_manifest_sha256")),
            "identical binaries, source hashes, node policy and fixture seal across stages")
    for number, manifest in ((1, stage1), (2, stage2)):
        declaration = protocol[f"stage{number}"]
        require(all(manifest[key] == declaration[key] for key in ("seed", "rounds", "repetitions")),
                "stage matches its predeclared seed and sample counts")
        normal = [fixture for fixture in manifest["measurement_fixtures"] if fixture.get("kind") != "generated"]
        require(manifest["case_specs"] == case_specs(sorted(manifest["arms"]), normal, protocol, number),
                "stage matrix matches the predeclared exploratory/primary-only plan")
    require(stage1["seed"] != stage2["seed"], "new confirmatory randomization seed")
    exploratory = ablation_cells(stage1_rows, stage1)
    confirmatory = ablation_cells(stage2_rows, stage2)
    verdicts = []
    for arm, primary in protocol["primaries"].items():
        family = [(cell, boot) for cell, boot in exploratory if cell["arm"] == arm]
        adjusted = holm_intervals([boot for _, boot in family])
        for (cell, _), result in zip(family, adjusted):
            cell["holm_regression_interval"] = result
        def is_primary(cell):
            return cell["arm"] == arm and cell["fixture"] == primary["fixture"] and cell["endpoint"] == primary["endpoint"]
        first = [cell for cell, _ in family if is_primary(cell)]
        second = [cell for cell, _ in confirmatory if is_primary(cell)]
        require(len(first) == len(second) == 1
                and sum(cell["arm"] == arm for cell, _ in confirmatory) == 1,
                "exactly one predeclared confirmatory primary per optimization")
        regressions = [cell for cell, _ in family if cell["holm_regression_interval"]["regression"]]
        keep = second[0]["ratio"]["low"] > 1 and not regressions
        verdicts.append(dict(arm=arm, primary=primary, stage1_primary=first[0]["ratio"],
                             stage2_primary=second[0]["ratio"], regression_check="regression" if regressions else "clear",
                             regression_family_size=len(family), regression_cells=regressions,
                             verdict="keep" if keep else "remove",
                             reason="adjusted regression blocks keep pending investigation" if regressions else
                                    ("confirmatory primary lower CI > 1" if keep else "confirmatory primary lower CI is not > 1")))
    return dict(protocol=protocol, ratio_direction="latency(minus arm) / latency(zebra-optimized)",
                stage1_results=[cell for cell, _ in exploratory],
                stage2_results=[cell for cell, _ in confirmatory], verdicts=verdicts)


def ablation_table(result):
    first_seed, second_seed = (result["protocol"][stage]["seed"] for stage in ("stage1", "stage2"))
    lines = [f"| Optimization removed | Predeclared primary | Stage 1 ratio [95% CI], seed {first_seed} | Stage 2 ratio [95% CI], seed {second_seed} | Stage-1 Holm regression check | Verdict |",
             "| --- | --- | --- | --- | --- | --- |"]
    def ratio(value):
        return f"{value['median']:.3f}× [{value['low']:.3f}, {value['high']:.3f}]"
    for row in result["verdicts"]:
        primary = row["primary"]
        label = ("generate 1 ×20 / Orchard-only UA / no standalone GBT" if primary["endpoint"] == GENERATE_ENDPOINT else
                 f"{primary['endpoint']} / {primary['filling']} / {primary['target_bytes']:,} B target")
        check = f"{row['regression_check']} ({len(row['regression_cells'])}/{row['regression_family_size']} cells)"
        lines.append(f"| {row['arm']} | {label} | {ratio(row['stage1_primary'])} | {ratio(row['stage2_primary'])} | {check} | {row['verdict']} |")
    return "\n".join(lines) + "\n"


def read_campaign(directory):
    manifest, completion = load_completed(directory)
    for relative, digest in completion.get("failure_receipts", {}).items():
        path = (directory / relative).resolve()
        require(path.is_relative_to(directory.resolve()) and file_hash(path) == digest,
                "sealed historical failure receipt: " + relative)
    for arm, revision in MAIN_REVISIONS.items():
        if arm in manifest["arms"]:
            require(manifest["arms"][arm]["source"] == revision, "current-main source identity")
    rows = [json.loads(line) for line in (directory / "observations.jsonl").read_text().splitlines()]
    require(len(rows) == completion["observations"], "completion count matches raw observations")
    return manifest, completion, rows


def plot(results, output, rounds, failed_attempts=0):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter

    colors = dict(empty="#606060", transparent="#0072B2", orchard="#CC79A7", mixed="#009E73")
    colors["transparent-signed"] = "#D55E00"
    arm_colors = {"zebra-main": "#0072B2", "zebra-optimized": "#009E73", "zakura-main": "#D55E00"}
    def size_label(value, _):
        return f"{value / 1e6:g} MB" if value >= 1e6 else f"{value / 1000:g} KB"
    def curve(ax, points, values, color, label):
        x = [p["block_bytes"] for p in points]
        y, low, high = ([v[k] for v in values] for k in ("median", "low", "high"))
        ax.plot(x, y, "o-", color=color, label=label, markersize=4)
        ax.fill_between(x, low, high, color=color, alpha=0.18)
        if len(x) == 1:
            ax.errorbar(x, y, yerr=[[y[0] - low[0]], [high[0] - y[0]]], color=color, capsize=4)
    def finish(fig, stem):
        if failed_attempts:
            fig.text(0.5, 0.01, f"Successful observations only; {failed_attempts} failed attempts retained in run evidence.",
                     ha="center", fontsize=8)
        fig.tight_layout(rect=(0, 0.03 if failed_attempts else 0, 1, 1))
        fig.savefig(output / (stem + ".png"), dpi=180)
        fig.savefig(output / (stem + ".svg"))
        plt.close(fig)
    for endpoint in ENDPOINTS:
        rows = [r for r in results if r["endpoint"] == endpoint]
        fig, ax = plt.subplots(figsize=(9, 5.5))
        for filling in FILLINGS:
            points = sorted((r for r in rows if r["filling"] == filling), key=lambda r: r["block_bytes"])
            if points:
                curve(ax, points, [p["ratios"]["zakura_over_optimized"] for p in points], colors[filling], filling)
        ax.axhline(1, color="black", linestyle="--", linewidth=1)
        ax.set(xscale="log", xlabel="Actual serialized block size (decimal bytes)",
               ylabel="Speedup: Zakura-main latency / Zebra-optimized latency",
               title=f"{endpoint}\n95% paired round-bootstrap CI; {rounds} rounds; Regtest")
        ax.xaxis.set_major_formatter(FuncFormatter(size_label))
        ax.grid(True, which="both", alpha=0.2)
        ax.legend(title="Block filling")
        finish(fig, "speedup-" + endpoint)

        panels = [filling for filling in FILLINGS if any(r["filling"] == filling for r in rows)]
        panel_rows = max(1, (len(panels) + 1) // 2)
        fig, axes = plt.subplots(panel_rows, 2, figsize=(11, 4 * panel_rows), squeeze=False)
        for ax in list(axes.flat)[len(panels):]:
            ax.set_visible(False)
        for ax, filling in zip(axes.flat, panels):
            points = sorted((r for r in rows if r["filling"] == filling), key=lambda r: r["block_bytes"])
            for arm in ARMS:
                if points:
                    curve(ax, points, [p["latency_ms"][arm] for p in points], arm_colors[arm], arm)
            ax.set(xscale="log", yscale="log", title=filling,
                   xlabel="Actual serialized block size", ylabel="Latency (ms)")
            ax.xaxis.set_major_formatter(FuncFormatter(size_label))
            ax.grid(True, which="both", alpha=0.2)
            ax.legend(fontsize="small")
        fig.suptitle(f"{endpoint}: absolute latency\n95% round-bootstrap CI; {rounds} rounds; Regtest")
        finish(fig, "latency-" + endpoint)


def self_check():
    fixtures = [dict(id=f"{kind}-{size}", filling=kind, bytes=size,
                     target_bytes=size, transaction_count=1 if kind == "empty" else 3)
                for kind, size in (("empty", 1600), ("transparent", 25000),
                                   ("transparent", 100000), ("orchard", 25000), ("mixed", 25000))]
    manifest = dict(rounds=5, repetitions=2, fixture_manifest=dict(fixtures=fixtures),
                    arms={arm: dict(sha256=arm) for arm in ARMS})
    rows = []
    for r in range(5):
        for n in range(2):
            for f in fixtures:
                for endpoint in ENDPOINTS:
                    for arm, scale in zip(ARMS, (2, 1, 4)):
                        latency = (r + 1) * (n + 1) * scale * f["bytes"]
                        rows.append(dict(round=r, repetition=n, arm=arm, fixture=f["id"], endpoint=endpoint,
                                         status="ok", latency_ns=latency, start_ns=1, end_ns=latency + 1,
                                         block_bytes=f["bytes"], filling=f["filling"], transaction_count=f["transaction_count"],
                                         candidate_sha256=f["id"], binary_sha256=arm))
    result = analyze_rows(rows, manifest)
    for row in result:
        for name, ratio in (("zakura_over_optimized", 4), ("zakura_over_zebra_main", 2),
                            ("main_over_optimized", 2)):
            assert row["ratios"][name] == dict(median=ratio, low=ratio, high=ratio)
    shuffled = list(rows)
    random.Random(10).shuffle(shuffled)
    assert analyze_rows(shuffled, manifest) == result
    for invalid in (rows[:-1], rows + rows[:1], [dict(rows[0], latency_ns=0)] + rows[1:],
                    [dict(rows[0], candidate_sha256="different")] + rows[1:]):
        try:
            analyze_rows(invalid, manifest)
        except RuntimeError:
            pass
        else:
            raise AssertionError("invalid/missing/duplicate/mismatched observation accepted")
    # Repeated observations from one round stay a cluster: no artificial repetition count.
    draws = np.random.default_rng(SEED).integers(3, size=(RESAMPLES, 3))
    single = np.array([[1.0], [3.0], [9.0]])
    assert interval(single, draws) == interval(np.repeat(single, 5, axis=1), draws)
    with tempfile.TemporaryDirectory(prefix="size-sweep-analysis-") as directory:
        path = Path(directory)
        plot(result, path, 5)
        assert len(list(path.glob("*.png"))) == len(list(path.glob("*.svg"))) == 8
        assert all(p.stat().st_size > 100 for p in path.iterdir())
    print("analysis self-check passed: paired direction, round clustering, completeness, reproducibility, PNG/SVG (synthetic data only)")


def main():
    parser = argparse.ArgumentParser(description=__doc__, epilog="Analysis requires numpy; rendering also requires matplotlib.")
    parser.add_argument("--run", type=Path)
    parser.add_argument("--stage1", type=Path, help="exploratory run for a completed confirmatory --run")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    if np is None:
        parser.error("analysis requires numpy (rendering also requires matplotlib)")
    if args.self_check:
        self_check()
        return
    require(args.run is not None and args.output is not None, "--run and --output required")
    manifest, completion, rows = read_campaign(args.run)
    failure_receipts = completion.get("failure_receipts", {})
    if manifest.get("ablation_stage"):
        require(manifest["ablation_stage"] == 2,
                "verdicts require completed stage 2; use --run STAGE2 --stage1 STAGE1")
        reference = manifest["stage1_reference"]
        first_path = args.stage1 or Path(reference["path"])
        first, first_completion, first_rows = read_campaign(first_path)
        require(first_completion["manifest_sha256"] == reference["manifest_sha256"]
                and first_completion["observations_sha256"] == reference["observations_sha256"]
                and file_hash(first_path / "completion.json") == reference["completion_sha256"],
                "confirmatory run references this exact completed exploratory run")
        result = analyze_ablation(first_rows, first, rows, manifest)
        result.update(seed=SEED, bootstrap_resamples=RESAMPLES, confidence=0.95,
                      inputs={"stage1": first_completion, "stage2": completion},
                      arms=manifest["arms"], analysis_sha256=file_hash(__file__),
                      historical_failure_counts={
                          "stage1": len(first_completion.get("failure_receipts", {})),
                          "stage2": len(failure_receipts)},
                      scope="Successful observations after explicit retries. Regtest loopback; primary confirmations use fresh rounds, not exploratory selection. Bootstrap intervals are approximate; leave-one-out effects depend on the remaining optimizations.")
        args.output.mkdir(parents=True, exist_ok=False)
        save(args.output / "ablation.json", result)
        (args.output / "ablation.md").write_text(ablation_table(result))
        return
    require(args.stage1 is None, "--stage1 only applies to a confirmatory ablation run")
    result = analyze_rows(rows, manifest)
    args.output.mkdir(parents=True, exist_ok=False)
    save(args.output / "summary.json", dict(
        seed=SEED, bootstrap_resamples=RESAMPLES, confidence=0.95,
        estimator="median of within-(round,repetition,fixture,endpoint) latency ratios; percentile interval",
        sampling_unit="paired process round; keep every repetition and all arms together; identical draws across points",
        ratio_direction=comparisons(manifest["arms"]), rounds=manifest["rounds"], repetitions=manifest["repetitions"],
        scope="Pointwise exploratory intervals, not simultaneous/multiplicity-adjusted. Regtest real proofs; legacy TCP relay; cold is process/candidate-cold, not OS-cache-cold. Empty has one actual-size point.",
        historical_failure_count=len(failure_receipts),
        retry_scope="Ratios describe successful observations after any explicit retries; failed attempts remain sealed evidence, not zero-latency samples.",
        arms=manifest["arms"], inputs=completion, analysis_sha256=file_hash(__file__), results=result))
    with (args.output / "summary.csv").open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["endpoint", "filling", "block_bytes", "transaction_count", "metric", "median", "ci_low", "ci_high"])
        for row in result:
            for metric, value in {**row["ratios"], **{a + "_ms": v for a, v in row["latency_ms"].items()}}.items():
                writer.writerow([row["endpoint"], row["filling"], row["block_bytes"], row["transaction_count"],
                                 metric, value["median"], value["low"], value["high"]])
    if set(ARMS) <= set(manifest["arms"]):
        plot(result, args.output, manifest["rounds"], len(failure_receipts))


if __name__ == "__main__":
    main()
