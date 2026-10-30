#!/usr/bin/env python3
"""Offline signature and selection checks; optional pinned public ZIP-243 vectors.

No network access during unittest discovery. Supply ZIP243_VECTORS or --vectors
to test an existing vector file; --fetch-vectors explicitly downloads the pinned
public file. Only signature checks skip when cryptography >=43 is unavailable.
"""
import argparse
import os
import unittest
import json
from pathlib import Path
import sys
import urllib.request


from build_fixtures import checkpoint_changes, select
from common import h, sha256
import transparent_signed as signed

VECTOR_URL = ("https://raw.githubusercontent.com/zcash/zcash-test-vectors/"
              "78321beacb0e0477e33cd002b56585a107c2708c/test-vectors/json/zip_0243.json")
VECTOR_SHA256 = "3179a8f2773bbc92dd0a509b6281dc36b8b96b41687cd4eae0fbccfea58a64c9"


def vectors_check(vectors):
    assert sha256(vectors) == VECTOR_SHA256, "official vector content changed"
    vectors = json.loads(vectors)[2:]
    assert len(vectors) == 10 and sum(v[2] >= 0 for v in vectors) == 2
    for raw, script, index, hash_type, amount, branch, expected in vectors:
        actual = signed.sighash(bytes.fromhex(raw), None if index == -1 else index,
                                bytes.fromhex(script), amount, branch, hash_type)
        assert actual.hex() == expected

    print("PASS: 10 pinned official ZIP-243 vectors (2 transparent-input cases)")


def signatures_check():
    from cryptography.exceptions import InvalidSignature
    from cryptography.hazmat.primitives.asymmetric import ec, utils
    _, signing = signed.signing_key()
    corpus = []
    for group in range(48):  # Same 9,600 independent outputs as the signed builder's default sizes.
        source = (sha256(str(group).encode()), 0, 625_000_000)
        each = (source[2] - h.FANOUT_FEE) // 200
        funding_raw, funding_id = signed.fanout(source, [each] * 200)
        funding, end = signed.protocol_fixture.parse_v4(funding_raw)
        assert end == len(funding_raw) and funding_id == h.dsha(funding_raw)[::-1].hex()
        assert funding["inputs"][0]["script"] == "0151"
        assert all(out["script"] == signed.SCRIPT.hex() and out["value"] == each
                   for out in funding["outputs"])
        for index in range(200):
            source = (funding_id, index, each)
            tx = signed.spend(source)
            raw = bytes.fromhex(tx["raw"])
            parsed, end = signed.protocol_fixture.parse_v4(raw)
            assert end == tx["bytes"] == len(raw)
            assert len(parsed["inputs"]) == len(parsed["outputs"]) == tx["transparent_inputs"] == 1
            assert parsed["inputs"][0]["txid"] == funding_id and parsed["inputs"][0]["index"] == index
            assert parsed["outputs"][0]["value"] == each - h.FEE
            assert parsed["outputs"][0]["script"] == signed.SCRIPT.hex()
            assert parsed["expiry"] == 0 and not any(parsed[k] for k in
                ("sapling_spends", "sapling_outputs", "joinsplits", "valuebalance"))
            script = bytes.fromhex(parsed["inputs"][0]["script"])
            signature = script[1:1 + script[0]]
            assert signature[-1] == 1 and script[1 + script[0]:] == b"\x21" + signed.PUBLIC_KEY
            r, s = utils.decode_dss_signature(signature[:-1])
            assert utils.encode_dss_signature(r, s) == signature[:-1] and 0 < s <= signed.ORDER // 2
            public = ec.EllipticCurvePublicKey.from_encoded_point(ec.SECP256K1(), script[-33:])
            digest = signed.sighash(raw, 0, signed.SCRIPT, each)
            public.verify(signature[:-1], digest, signing)
            if not corpus:
                assert signed.spend(source) == tx, "RFC6979 determinism"
                for wrong in (signed.sighash(raw, 0, signed.SCRIPT, each + 1),
                              signed.sighash(raw, 0, signed.SCRIPT, each, 0xF5B9230B),
                              signed.sighash(raw, 0, b"\x51", each)):
                    try:
                        public.verify(signature[:-1], wrong, signing)
                    except InvalidSignature:
                        pass
                    else:
                        raise AssertionError("amount, branch and prevout script must be signed")
            corpus.append(tx)
    assert len({tx["txid"] for tx in corpus}) == 9600
    # This overhead is a sizing example, NOT a produced/accepted block measurement.
    for target in (250_000, 1_900_000):
        selected = select(corpus, "transparent-signed", target, 1600)
        chosen = [corpus[i] for i in selected]
        total = 1602 + sum(tx["bytes"] for tx in chosen)
        assert abs(total - target) <= max(tx["bytes"] for tx in chosen)
        assert sum(tx["transparent_inputs"] for tx in chosen) == len(chosen)
        print(f"Sizing example (1600-byte empty block): target={target}, bytes={total}, "
              f"signed_txs=inputs={len(chosen)}, block_txs={len(chosen) + 1}")
    previous = dict(producer="same", sizes=[2000, 250000, 1900000], wallet_image="same", sources={})
    assert checkpoint_changes(previous, previous, False) == []
    try:
        checkpoint_changes(previous, dict(previous, transparent_signed=signed.LAYOUT), True)
    except RuntimeError:
        pass
    else:
        raise AssertionError("signed filling must not reuse historical funding")
    print(f"PASS: 9600 low-S DER "
          f"signature round trips; tx byte range {min(t['bytes'] for t in corpus)}..{max(t['bytes'] for t in corpus)}; "
          "funding, deterministic signing, mutation rejection, size selection and checkpoint isolation")


class SignedTests(unittest.TestCase):
    def test_vectors(self):
        path = os.environ.get("ZIP243_VECTORS")
        if not path:
            self.skipTest("set ZIP243_VECTORS to the pinned public vector file")
        vectors_check(Path(path).read_bytes())

    def test_signatures(self):
        try:
            import cryptography
            from cryptography.exceptions import UnsupportedAlgorithm
        except ImportError:
            self.skipTest("cryptography >=43 is not installed")
        if int(cryptography.__version__.split(".")[0]) < 43:
            self.skipTest("cryptography >=43 is required for deterministic signing")
        try:
            signed.spend(("00" * 32, 0, 1_000_000))
        except UnsupportedAlgorithm as error:
            self.skipTest(str(error))
        signatures_check()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--vectors", type=Path, help="existing pinned ZIP-243 JSON; also ZIP243_VECTORS")
    group.add_argument("--fetch-vectors", action="store_true", help="explicitly permit a public HTTPS fetch")
    args = parser.parse_args()
    if args.vectors:
        os.environ["ZIP243_VECTORS"] = str(args.vectors)
    if args.fetch_vectors:
        with urllib.request.urlopen(VECTOR_URL, timeout=30) as response:
            vectors_check(response.read())
    unittest.main(argv=[sys.argv[0]])


if __name__ == "__main__":
    main()
