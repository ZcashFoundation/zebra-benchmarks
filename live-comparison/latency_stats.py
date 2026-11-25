"""Mean-led latency statistics; half-sample mode describes the typical cluster.

The mean includes slow rounds, while the median and half-sample mode expose their effect.
"""
import math
import random
import statistics


def half_sample_mode(values):
    xs = sorted(values)
    if not xs:
        raise ValueError("no samples")
    while len(xs) > 3:
        half = math.ceil(len(xs) / 2)
        start = min(range(len(xs) - half + 1), key=lambda i: xs[i + half - 1] - xs[i])
        xs = xs[start:start + half]
    if len(xs) == 3:
        left, right = xs[1] - xs[0], xs[2] - xs[1]
        if left < right:
            return (xs[0] + xs[1]) / 2
        if right < left:
            return (xs[1] + xs[2]) / 2
        return xs[1]
    return statistics.fmean(xs)


def summary(values):
    return dict(mean=statistics.fmean(values), median=statistics.median(values),
                mode=half_sample_mode(values))


def paired(diffs, rng=None, resamples=5000):
    """Signed paired mean/median differences and percentile bootstrap mean CI."""
    rng = rng if rng is not None else random.Random(0)
    result = {"mean": statistics.fmean(diffs), "median": statistics.median(diffs)}
    boots = sorted(statistics.fmean(rng.choices(diffs, k=len(diffs))) for _ in range(resamples))
    low, high = round(.025 * (resamples - 1)), round(.975 * (resamples - 1))
    result["mean_ci95"] = [boots[low], boots[high]]
    return result
