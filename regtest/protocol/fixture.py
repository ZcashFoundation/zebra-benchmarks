"""Bounded V4 transaction reader for Regtest fixtures and ZIP-243 hashing.

Implemented from the public Zcash transaction encoding (protocol section 7.1),
not an archived benchmark parser. This is a byte-layout reader, not consensus
validation. Returned offsets are absolute byte offsets in the supplied buffer.
"""
import struct


def take(raw, pos, count):
    """Return (bytes, next offset), rejecting negative or truncated ranges."""
    if pos < 0 or count < 0 or pos > len(raw) or count > len(raw) - pos:
        raise ValueError("truncated or invalid transaction range")
    return raw[pos:pos + count], pos + count


def varint(raw, pos):
    """Read canonical Bitcoin CompactSize and return (value, next offset)."""
    tag, pos = take(raw, pos, 1)
    if tag[0] < 253:
        return tag[0], pos
    size = {253: 2, 254: 4, 255: 8}[tag[0]]
    encoded, pos = take(raw, pos, size)
    value = int.from_bytes(encoded, "little")
    if value < {2: 253, 4: 65536, 8: 4294967296}[size]:
        raise ValueError("noncanonical CompactSize")
    return value, pos


def parse_v4(raw, pos=0):
    """Return the V4 fields used by fixture callers, and the next byte offset.

    Shielded bundles are bounds-checked and represented by their counts; callers
    hashing them consume their serialized bytes separately. Trailing data is
    permitted so the returned offset can delimit a transaction in a larger buffer.
    """
    header, pos = take(raw, pos, 8)
    if struct.unpack("<II", header) != (0x80000004, 0x892F2085):
        raise ValueError("expected overwintered V4 Sapling transaction")
    inputs = []
    count, pos = varint(raw, pos)
    if count > (len(raw) - pos) // 41:
        raise ValueError("truncated transparent inputs")
    for _ in range(count):
        outpoint, pos = take(raw, pos, 36)
        length, pos = varint(raw, pos)
        start = pos
        script, pos = take(raw, pos, length)
        end = pos
        sequence, pos = take(raw, pos, 4)
        inputs.append(dict(txid=outpoint[:32][::-1].hex(),
                           index=int.from_bytes(outpoint[32:], "little"),
                           script=script.hex(), script_range=(start, end),
                           sequence=sequence.hex()))
    outputs = []
    count, pos = varint(raw, pos)
    if count > (len(raw) - pos) // 9:
        raise ValueError("truncated transparent outputs")
    for _ in range(count):
        value, pos = take(raw, pos, 8)
        length, pos = varint(raw, pos)
        script, pos = take(raw, pos, length)
        outputs.append(dict(value=int.from_bytes(value, "little", signed=True),
                            script=script.hex()))
    tail, pos = take(raw, pos, 16)
    locktime, expiry, valuebalance = struct.unpack("<IIq", tail)
    spends, pos = varint(raw, pos)
    _, pos = take(raw, pos, spends * 384)
    shielded_outputs, pos = varint(raw, pos)
    _, pos = take(raw, pos, shielded_outputs * 948)
    joinsplits, pos = varint(raw, pos)
    _, pos = take(raw, pos, joinsplits * 1698)
    if joinsplits:
        _, pos = take(raw, pos, 32 + 64)  # JoinSplit public key and signature.
    if spends or shielded_outputs:
        _, pos = take(raw, pos, 64)  # Sapling binding signature.
    return dict(inputs=inputs, outputs=outputs, locktime=locktime, expiry=expiry,
                valuebalance=valuebalance, sapling_spends=spends,
                sapling_outputs=shielded_outputs, joinsplits=joinsplits), pos
