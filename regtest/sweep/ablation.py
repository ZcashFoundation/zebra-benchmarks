#!/usr/bin/env python3
"""Predeclared two-stage leave-one-out protocol and pure campaign planning."""
from common import ENDPOINTS, GENERATE_ENDPOINT, GENERATE_BLOCKS, file_hash, require
import json
from pathlib import Path

BASELINE = "zebra-optimized"
GENERATE_ID = "orchard-generate-20"
# Orchard-only Regtest UA from Zebra's config/mining.rs Orchard address vector.
ORCHARD_MINER = "uregtest1pszqlgxaf5w8mu2yd9uygg8cswp0ec4f7eejqnqc35tztw4tk0sxnt3pym2f3s2872cy2ruuc5n8y9cen5q6ngzlmzu8ztrjesv8zm9j"
PRIMARY = {
    "minus-decode-reuse": ("prepared-submitblock", "orchard", 1_900_000),
    "minus-single-pass-codec": ("submit-to-peer-tip", "transparent", 1_900_000),
    "minus-batch-flush": ("cold-submitblock", "orchard", 1_900_000),
    "minus-template-tx-reuse": ("admission-to-template", "transparent", 1_900_000),
    "minus-orchard-lazy": ("admission-to-template", "orchard", 1_900_000),
    "minus-precompute-on-demand": (GENERATE_ENDPOINT, "orchard-coinbase", None),
}
DEFAULT_SUBSET = ["empty-2000", *[f"{kind}-{size}" for size in (250_000, 1_900_000)
                                  for kind in ("transparent", "orchard", "mixed")]]


def select_fixtures(fixtures, requested):
    known = {fixture["id"]: fixture for fixture in fixtures}
    require(len(known) == len(fixtures), "unique fixture ids")
    if requested is None:
        return list(fixtures)
    wanted = []
    for name in requested:
        if name == "empty":
            empty = [fixture["id"] for fixture in fixtures if fixture["filling"] == "empty"]
            require(len(empty) == 1, "empty alias requires exactly one coinbase-only fixture")
            name = empty[0]
        require(name in known and name not in wanted, "unknown or duplicate fixture id: " + name)
        wanted.append(name)
    require(wanted, "nonempty fixture subset")
    return [fixture for fixture in fixtures if fixture["id"] in wanted]


def generated_fixture(chain_sha256):
    return dict(id=GENERATE_ID, filling="orchard-coinbase", bytes=None, target_bytes=None,
                transaction_count=1, kind="generated", setup_chain_sha256=chain_sha256)


def declare_protocol(arms, fixtures, seed, confirm_seed, rounds, repetitions, miner):
    minus = sorted(name for name in arms if name != BASELINE)
    require(BASELINE in arms and minus and all(name in PRIMARY for name in minus),
            "two-stage ablations require optimized plus arms with a predeclared primary: " + ", ".join(PRIMARY))
    require(seed != confirm_seed, "stage 2 must use a new predeclared randomization seed")
    require(miner.startswith("uregtest1") and miner.isalnum() and miner == miner.lower(),
            "Regtest unified miner address required")
    primaries = {}
    for arm in minus:
        endpoint, filling, size = PRIMARY[arm]
        if endpoint == GENERATE_ENDPOINT:
            fixture_id, actual_bytes = GENERATE_ID, None
        else:
            matching = [fixture for fixture in fixtures
                        if fixture["filling"] == filling and fixture["target_bytes"] == size]
            require(len(matching) == 1, "stage-1 subset must contain the predeclared primary for " + arm)
            fixture_id, actual_bytes = matching[0]["id"], matching[0]["bytes"]
        primaries[arm] = dict(endpoint=endpoint, fixture=fixture_id, filling=filling,
                              target_bytes=size, actual_block_bytes=actual_bytes)
    return dict(
        version=1, baseline=BASELINE, primaries=primaries,
        stage1=dict(seed=seed, rounds=rounds, repetitions=repetitions,
                    fixture_ids=[fixture["id"] for fixture in fixtures]),
        stage2=dict(seed=confirm_seed, rounds=rounds, repetitions=repetitions,
                    scope="fresh processes and rounds; predeclared primary cells only"),
        generate=dict(endpoint=GENERATE_ENDPOINT, blocks_per_process=GENERATE_BLOCKS,
                      miner_address=miner, statistic="median of 20 generate-1 RPC latencies per process",
                      demand="no standalone getblocktemplate calls; generate internally constructs templates",
                      fixture_scope="same sealed funding prefix and Orchard-only UA; generated proof/block bytes differ"),
        inference=dict(seed=20261015, bootstrap_resamples=10_000, confidence=0.95,
                       sampling_unit="paired process round; repetitions remain in their round; generated calls first reduced to process medians",
                       regression_family="all stage-1 endpoint x fixture cells for each individual minus arm, including its generation cell",
                       regression_method="two-sided percentile-bootstrap tail tests at ratio 1; Holm step-down adjusted p-values and test-inverted percentile intervals; finite-resample +1 correction",
                       regression_rule="flag if a Holm-adjusted upper confidence bound is below 1; any flag blocks keep",
                       keep_rule="stage-2 predeclared primary paired median ratio minus/optimized has 95% lower bound > 1 and no stage-1 adjusted regression",
                       otherwise="remove", final_check="benchmark the retained combination fresh in the primary three-arm sweep"))


def case_specs(arms, fixtures, protocol=None, stage=None):
    if stage != 2:
        specs = [dict(fixture=fixture["id"], endpoints=list(ENDPOINTS), arms=list(arms)) for fixture in fixtures]
    else:
        specs = []
        for arm, primary in protocol["primaries"].items():
            specs.append(dict(fixture=primary["fixture"], endpoints=[primary["endpoint"]], arms=[BASELINE, arm]))
    if stage == 1 and "minus-precompute-on-demand" in protocol["primaries"]:
        specs.append(dict(fixture=GENERATE_ID, endpoints=[GENERATE_ENDPOINT], arms=[BASELINE, "minus-precompute-on-demand"]))
    return specs


def load_completed(directory):
    directory = Path(directory)
    completion = json.loads((directory / "completion.json").read_text())
    require(completion["success"], "completed campaign required: " + str(directory))
    for name, suffix in (("manifest", ".json"), ("observations", ".jsonl")):
        require(file_hash(directory / (name + suffix)) == completion[name + "_sha256"], "sealed campaign " + name)
    manifest = json.loads((directory / "manifest.json").read_text())
    return manifest, completion


def stage1_reference(directory):
    directory = Path(directory).resolve()
    manifest, completion = load_completed(directory)
    require(manifest.get("ablation_stage") == 1, "reference must be a completed exploratory stage 1")
    return manifest, dict(path=str(directory), manifest_sha256=completion["manifest_sha256"],
                          observations_sha256=completion["observations_sha256"],
                          completion_sha256=file_hash(directory / "completion.json"))
