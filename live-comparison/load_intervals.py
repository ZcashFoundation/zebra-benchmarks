#!/usr/bin/env python3
"""Validate and merge optional JSON load intervals for dashboard shading.

Input and output are arrays of [start_ms, end_ms] pairs: closed intervals in
Unix epoch milliseconds. Omit --input to write an empty array. Record unrelated
host work explicitly; this command does not discover running processes.
"""
import argparse
import json
from pathlib import Path
import sys


def merge_intervals(intervals, padding=0):
    """Validate, widen and merge closed millisecond intervals."""
    if not isinstance(intervals, list):
        raise ValueError("load intervals must be a JSON array")
    merged = []
    for pair in intervals:
        if (not isinstance(pair, (list, tuple)) or len(pair) != 2 or
                any(type(value) is not int or value < 0 for value in pair) or pair[0] > pair[1]):
            raise ValueError("load intervals must be [start_ms, end_ms] nonnegative integer pairs, start <= end")
    for start, end in sorted(intervals):
        start, end = max(0, start - padding), end + padding
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(end, merged[-1][1])
        else:
            merged.append([start, end])
    return merged


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--input", type=Path,
                        help="JSON [start_ms,end_ms] pairs; default: no recorded load intervals")
    args = parser.parse_args()
    try:
        intervals = merge_intervals(json.loads(args.input.read_text(encoding="utf-8")) if args.input else [])
        args.output.parent.mkdir(parents=True, exist_ok=True)
        temporary = args.output.with_name(args.output.name + ".tmp")
        temporary.write_text(json.dumps(intervals, separators=(",", ":")) + "\n", encoding="utf-8")
        temporary.replace(args.output)
    except (OSError, ValueError) as error:
        print(f"load_intervals: {error}", file=sys.stderr)
        return 1
    print(f"Exported {len(intervals)} anonymous load intervals")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
