"""Regtest-only P2PKH fixtures: one input, one output, ZIP-243 SIGHASH_ALL.

The public fixture key is deliberately not a wallet key. Never fund it on Mainnet.
Requires cryptography >=43 and OpenSSL deterministic ECDSA support.
"""
from functools import cache
import hashlib
import struct

from common import h, require, sha256
from protocol import fixture as protocol_fixture

NU5_BRANCH = 0xC2D6D0B4
ORDER = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141
PUBLIC_KEY = bytes.fromhex("0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798")
SCRIPT = b"\x76\xa9\x14" + hashlib.new("ripemd160", hashlib.sha256(PUBLIC_KEY).digest()).digest() + b"\x88\xac"
LAYOUT = dict(inputs_per_transaction=1, outputs_per_transaction=1,
              script_pubkey=SCRIPT.hex(), public_key=PUBLIC_KEY.hex(),
              script_sig="push(strict-DER low-S ECDSA signature + 01) push(33-byte compressed public key)",
              sighash="ZIP-243 SIGHASH_ALL", consensus_branch_id=f"{NU5_BRANCH:08x}",
              deterministic_signing=True)


@cache
def signing_key():
    """Load optional cryptography only when generating signed transactions."""
    import cryptography
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import ec, utils
    if int(cryptography.__version__.split(".")[0]) < 43:
        raise RuntimeError("signed generation requires cryptography >=43")
    # Prehashed passes the ZIP-243 digest unchanged; SHA256 specifies only its length.
    signing = ec.ECDSA(utils.Prehashed(hashes.SHA256()), deterministic_signing=True)
    return ec.derive_private_key(1, ec.SECP256K1()), signing


def blake(data, person):
    return hashlib.blake2b(data, digest_size=32, person=person).digest()


def sighash(raw, index, script_code, amount, branch=NU5_BRANCH, hash_type=1):
    """ZIP-243, including the other modes/bundles exercised by the official vectors.

    Fixture construction below uses only transparent-only V4 and SIGHASH_ALL.
    https://zips.z.cash/zip-0243
    """
    tx, end = protocol_fixture.parse_v4(raw)
    require(end == len(raw), "exact V4 transaction for signature hash")
    require(index is None or 0 <= index < len(tx["inputs"]), "sighash input index")
    prevouts, sequences = [], []
    pos = 8 + len(h.compact(len(tx["inputs"])))
    for inp in tx["inputs"]:
        prevouts.append(bytes.fromhex(inp["txid"])[::-1] + struct.pack("<I", inp["index"]))
        sequences.append(bytes.fromhex(inp["sequence"]))
        pos = inp["script_range"][1] + 4
    pos += len(h.compact(len(tx["outputs"])))
    outputs = []
    for out in tx["outputs"]:
        script = bytes.fromhex(out["script"])
        outputs.append(struct.pack("<q", out["value"]) + h.compact(len(script)) + script)
        pos += len(outputs[-1])
    tail, pos = protocol_fixture.take(raw, pos, 16)
    count, pos = protocol_fixture.varint(raw, pos)
    spends, pos = protocol_fixture.take(raw, pos, count * 384)
    spends = b"".join(spends[i:i + 320] for i in range(0, len(spends), 384))
    count, pos = protocol_fixture.varint(raw, pos)
    shielded_outputs, pos = protocol_fixture.take(raw, pos, count * 948)
    count, pos = protocol_fixture.varint(raw, pos)
    joinsplits, _ = protocol_fixture.take(raw, pos, count * 1698 + (32 if count else 0))
    zero, mode, anyone = bytes(32), hash_type & 31, hash_type & 128
    output_hash = zero
    if mode not in (2, 3):
        output_hash = blake(b"".join(outputs), b"ZcashOutputsHash")
    elif mode == 3 and index is not None and index < len(outputs):
        output_hash = blake(outputs[index], b"ZcashOutputsHash")
    preimage = (raw[:8]
                + (zero if anyone else blake(b"".join(prevouts), b"ZcashPrevoutHash"))
                + (zero if anyone or mode in (2, 3) else blake(b"".join(sequences), b"ZcashSequencHash"))
                + output_hash
                + (blake(joinsplits, b"ZcashJSplitsHash") if joinsplits else zero)
                + (blake(spends, b"ZcashSSpendsHash") if spends else zero)
                + (blake(shielded_outputs, b"ZcashSOutputHash") if shielded_outputs else zero)
                + tail + struct.pack("<I", hash_type))
    if index is not None:
        preimage += (prevouts[index] + h.compact(len(script_code)) + script_code
                     + struct.pack("<q", amount) + sequences[index])
    return blake(preimage, b"ZcashSigHash" + struct.pack("<I", branch))


def serialize(source, values, script_sig):
    require(source[2] > sum(values) and all(v >= 100_000 for v in values),
            "positive fee and dust-safe P2PKH outputs")
    raw = (struct.pack("<II", 0x80000004, 0x892F2085) + b"\x01"
           + bytes.fromhex(source[0])[::-1] + struct.pack("<I", source[1])
           + h.compact(len(script_sig)) + script_sig + b"\xff\xff\xff\xff"
           + h.compact(len(values)))
    raw += b"".join(struct.pack("<q", value) + h.compact(len(SCRIPT)) + SCRIPT for value in values)
    return raw + struct.pack("<IIq", 0, 0, 0) + b"\x00\x00\x00"


def fanout(source, values):
    """Spend the existing OP_TRUE funding output into real P2PKH outputs."""
    raw = serialize(source, values, b"\x01\x51")
    return raw, h.dsha(raw)[::-1].hex()


def spend(source):
    from cryptography.hazmat.primitives.asymmetric import utils
    key, signing = signing_key()
    values = [source[2] - h.FEE]
    unsigned = serialize(source, values, b"")
    digest = sighash(unsigned, 0, SCRIPT, source[2])
    r, s = utils.decode_dss_signature(key.sign(digest, signing))
    signature = utils.encode_dss_signature(r, min(s, ORDER - s)) + b"\x01"
    script_sig = bytes([len(signature)]) + signature + bytes([len(PUBLIC_KEY)]) + PUBLIC_KEY
    raw = serialize(source, values, script_sig)
    return dict(txid=h.dsha(raw)[::-1].hex(), raw=raw.hex(), kind="transparent-signed",
                bytes=len(raw), sha256=sha256(raw), orchard_actions=0, fee_zat=h.FEE,
                transparent_inputs=1, prevout_value_zat=source[2])
