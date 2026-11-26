"""Mean, median and mode of latency samples, and paired differences with bootstrap intervals.

The mean is the headline: miners pay for slow rounds too. The median and mode show whether a
few slow rounds drive it. Latencies are continuous, so the mode is the half-sample mode
(Bickel and Frühwirth, 2006): repeatedly keep the densest half of the sorted samples.
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
    """Summary of per-round differences, with percentile bootstrap 95% intervals."""
    rng = rng or random.Random(0)
    result = summary(diffs)
    boots = {"mean": [], "median": []}
    for _ in range(resamples):
        sample = rng.choices(diffs, k=len(diffs))
        boots["mean"].append(statistics.fmean(sample))
        boots["median"].append(statistics.median(sample))
    low, high = round(0.025 * (resamples - 1)), round(0.975 * (resamples - 1))
    for name, values in boots.items():
        values.sort()
        result[name + "_ci95"] = [values[low], values[high]]
    return result


if __name__ == "__main__":
    assert half_sample_mode([1.0, 2.0, 2.1, 2.5, 9.0]) == (2.0 + 2.1) / 2
    assert half_sample_mode([5.0]) == 5.0
    assert half_sample_mode([1.0, 1.0, 1.0, 50.0, 90.0]) == 1.0
    stats = summary([44.0, 45.0, 46.0, 47.0, 90.0])
    assert stats["median"] == 46.0 and stats["mean"] == 54.4
    result = paired([-1.0, -2.0, -1.5, -1.2, 3.0], random.Random(1), 200)
    assert result["mean_ci95"][0] <= result["mean"] <= result["mean_ci95"][1]
    print("latency_stats: ok")
