#!/usr/bin/env python3
"""Usage: phase-summary.py <jsonl>...

Print medians per binary, separately for each fixture. Phase rows are seconds;
latency_ms is milliseconds. Missing metrics are '-', never zero. A partial
median shows [available/total]; nested phase medians must not be summed.
Parser/median check: python3 -m doctest phase-summary.py
"""

import argparse
from collections import defaultdict
import json
from pathlib import Path
import statistics


def median_cell(values):
    """Format present observations without treating missing metrics as zero.

    >>> median_cell([1.0, 3.0])
    '2'
    >>> median_cell([0.0, None])
    '0 [1/2]'
    >>> median_cell([None])
    '-'
    """
    present = [value for value in values if value is not None]
    if not present:
        return "-"
    result = f"{statistics.median(present):.6g}"
    if len(present) != len(values):
        result += f" [{len(present)}/{len(values)}]"
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("jsonl", nargs="+", type=Path)
    args = parser.parse_args()
    groups = defaultdict(lambda: defaultdict(list))
    for path in args.jsonl:
        with path.open() as source:
            for number, line in enumerate(source, 1):
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                    row["latency_ms"], row["delta_seconds"]
                    groups[row["fixture"]][row["binary"]].append(row)
                except (ValueError, KeyError, TypeError) as error:
                    raise ValueError(f"{path}:{number}: invalid phase-probe row") from error
    if not groups:
        parser.error("no samples in input files")
    for fixture, binaries in sorted(groups.items()):
        names = sorted(binaries)
        print(f"\nFixture: {fixture}")
        for index, name in enumerate(names, 1):
            print(f"  B{index}: {name}")
        phases = sorted({key for rows in binaries.values() for row in rows
                         for key in row["delta_seconds"]})
        table = [["median", *[f"B{i} (n={len(binaries[name])})"
                              for i, name in enumerate(names, 1)]]]
        table.append(["latency_ms", *[median_cell([r["latency_ms"] for r in binaries[name]])
                                      for name in names]])
        for phase in phases:
            table.append([phase + " [s]", *[
                median_cell([row["delta_seconds"].get(phase) for row in binaries[name]])
                for name in names]])
        widths = [max(map(len, column)) for column in zip(*table)]
        for row in table:
            print("  ".join(cell.ljust(width) for cell, width in zip(row, widths)))


if __name__ == "__main__":
    main()
