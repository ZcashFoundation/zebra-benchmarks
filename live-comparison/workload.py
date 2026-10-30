"""Bounded Zcash wire-layout reader; counts, not consensus/proof validation.

Layouts: https://zips.z.cash/protocol/protocol.pdf#txnencoding.
Version 6 Orchard then Ironwood follows Zebra/librustzcash's read_v6.
Taxonomy edges are an explicit local policy, not consensus rules.
"""
import hashlib
import re

MAX_BLOCK_BYTES = 2_000_000
COMPONENTS = ("transparent_inputs", "transparent_outputs", "sprout_joinsplits",
              "sapling_spends", "sapling_outputs", "orchard_actions", "ironwood_actions")
GROUP_IDS = {3: 0x03C48270, 4: 0x892F2085, 5: 0x26A7270A, 6: 0xD884B698}
THRESHOLDS = {"inferred": True, "size_upper_bytes": [16384, 131072, 1048576],
              "transaction_upper_counts": [1, 10, 100], "component_high_min": 100}


class ParseError(ValueError):
    """Malformed or explicitly unsupported block encoding."""


class Reader:
    def __init__(self, data):
        self.data = memoryview(data)
        self.pos = 0

    def take(self, length):
        if length < 0 or length > len(self.data) - self.pos:
            raise ParseError(f"truncated field at byte {self.pos}: need {length} bytes")
        start = self.pos
        self.pos += length
        return self.data[start:self.pos]

    def uint(self, length):
        return int.from_bytes(self.take(length), "little")

    def compact(self, minimum_size=1):
        tag = self.uint(1)
        value = self.uint({253: 2, 254: 4, 255: 8}[tag]) if tag >= 253 else tag
        if tag >= 253 and value < {253: 253, 254: 65536, 255: 4294967296}[tag]:
            raise ParseError("noncanonical CompactSize")
        if value > (len(self.data) - self.pos) // minimum_size:
            raise ParseError(f"CompactSize exceeds remaining bytes at {self.pos}")
        return value

    def vector(self, item_size):
        count = self.compact(item_size)
        self.take(count * item_size)
        return count


def orchard(reader, ironwood=False):
    count = reader.vector(820)
    if count:
        flags = reader.uint(1)
        if flags & ~(7 if ironwood else 3):
            raise ParseError("reserved Orchard/Ironwood flag bits")
        reader.take(8 + 32)
        reader.take(reader.compact())  # Halo2 proof bytes, never a fixed size.
        reader.take(64 * count + 64)
    return count


def transaction(reader):
    counts = dict.fromkeys(COMPONENTS, 0)
    header = reader.uint(4)
    version = header & 0x7FFFFFFF
    if version not in range(1, 7) or bool(header >> 31) != (version >= 3):
        raise ParseError(f"unsupported transaction header 0x{header:08x}")
    if version >= 3 and reader.uint(4) != GROUP_IDS[version]:
        raise ParseError(f"unsupported v{version} version group")
    if version >= 5:
        branch = reader.uint(4)
        # Restrict v6 to the known Ironwood/NU7 layout, not older draft ZIP-233.
        if version == 6 and branch not in (0x37A5165B, 0x77190AD9):
            raise ParseError(f"unsupported v6 consensus branch 0x{branch:08x}")
        reader.take(8)  # lockTime and expiryHeight
    inputs = reader.compact(41)
    coinbase = False
    for i in range(inputs):
        prevout = reader.take(36)
        if inputs == 1 and i == 0:
            coinbase = prevout == bytes(32) + b"\xff" * 4
        reader.take(reader.compact())
        reader.take(4)
    counts["transparent_inputs"] = inputs
    outputs = reader.compact(9)
    for _ in range(outputs):
        reader.take(8)
        reader.take(reader.compact())
    counts["transparent_outputs"] = outputs
    if version <= 4:
        reader.take(4 + (4 if version >= 3 else 0))
        if version == 4:
            reader.take(8)
            counts["sapling_spends"] = reader.vector(384)
            counts["sapling_outputs"] = reader.vector(948)
        if version >= 2:
            counts["sprout_joinsplits"] = reader.vector(1698 if version == 4 else 1802)
            if counts["sprout_joinsplits"]:
                reader.take(96)
        if counts["sapling_spends"] or counts["sapling_outputs"]:
            reader.take(64)
    else:
        spends = counts["sapling_spends"] = reader.vector(96)
        outputs = counts["sapling_outputs"] = reader.vector(756)
        if spends or outputs:
            reader.take(8 + (32 if spends else 0) + spends * 256 + outputs * 192 + 64)
        counts["orchard_actions"] = orchard(reader)
        if version == 6:
            counts["ironwood_actions"] = orchard(reader, ironwood=True)
    return counts, coinbase, version


def labels(features):
    size, count = features["size_bytes"], features["tx_count"]
    size_label = next((name for edge, name in zip(THRESHOLDS["size_upper_bytes"],
                      ("small", "medium", "large")) if size <= edge), "very_large")
    tx_label = "coinbase_only" if count == 1 else "few" if count <= 10 else "moderate" if count <= 100 else "many"
    pools = {"transparent": features["transparent_inputs"] + features["transparent_outputs"],
             "sprout": features["sprout_joinsplits"],
             "sapling": features["sapling_spends"] + features["sapling_outputs"],
             "orchard": features["orchard_actions"], "ironwood": features["ironwood_actions"]}
    active = [pool for pool, amount in pools.items() if amount]
    composition = "empty" if not active else active[0] if len(active) == 1 else "mixed"
    result = ["arrival:unknown", f"composition:{composition}", f"size:{size_label}", f"transactions:{tx_label}"]
    for key in ("transparent_inputs", "sapling_spends", "sapling_outputs", "orchard_actions", "ironwood_actions"):
        amount = features[key]
        result.append(f"{key}:" + ("none" if amount == 0 else "present" if amount < 100 else "high"))
    return sorted(result)


def parse_block(raw_hex):
    """Decode one entire block, rejecting unknown layouts and trailing bytes.

    Counts exclude the coinbase transaction's components, while tx_count and
    transaction_versions include it. Cryptographic fields are only length-checked.
    """
    if not isinstance(raw_hex, str) or len(raw_hex) > MAX_BLOCK_BYTES * 2 or len(raw_hex) % 2 or not re.fullmatch(r"[0-9a-fA-F]+", raw_hex):
        raise ParseError("raw block must be even-length hex, at most 2000000 bytes")
    data = bytes.fromhex(raw_hex)
    reader = Reader(data)
    reader.take(140)
    reader.take(reader.compact())  # Equihash solution
    digest = hashlib.sha256(hashlib.sha256(reader.data[:reader.pos]).digest()).digest()[::-1].hex()
    count = reader.compact(10)
    if not count:
        raise ParseError("block has no coinbase transaction")
    result = dict.fromkeys(COMPONENTS, 0)
    versions = {}
    for index in range(count):
        components, coinbase, version = transaction(reader)
        if coinbase != (index == 0):
            raise ParseError("coinbase must be the first and only coinbase transaction")
        versions[str(version)] = versions.get(str(version), 0) + 1
        if not coinbase:
            for key, value in components.items():
                result[key] += value
    if reader.pos != len(data):
        raise ParseError(f"trailing bytes after transactions: {len(data) - reader.pos}")
    result.update(hash=digest, size_bytes=len(data), tx_count=count,
                  transaction_versions=versions, schema_version=1,
                  source="raw_block_hex", component_scope="non_coinbase",
                  observed_parent_gap_ms=None, parse_status="success", detail=None)
    result["labels"] = labels(result)
    return result
