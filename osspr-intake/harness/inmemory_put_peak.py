"""Peak memory of one weighted InMemoryBucket.put.

Workload: a single put of weight 300_000 into an empty in-memory bucket whose
limit is 300_000 per day (a bulk job charging a whole daily quota at once).
The weight>1 branch materializes a temporary list of that many references
before extending the log.
"""

from __future__ import annotations

import statistics
import sys
import tracemalloc
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pyrate_limiter.abstracts.rate import Duration, Rate, RateItem
from pyrate_limiter.buckets.in_memory_bucket import InMemoryBucket

WEIGHT = 300_000
RUNS = 5
TIMESTAMP = 1_700_000_000_000


def measure_once() -> int:
    bucket = InMemoryBucket([Rate(WEIGHT, int(Duration.DAY))])
    item = RateItem("bulk-import", TIMESTAMP, WEIGHT)
    tracemalloc.start()
    try:
        tracemalloc.clear_traces()
        tracemalloc.reset_peak()
        admitted = bucket.put(item)
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    if admitted is not True or bucket.count() != WEIGHT or bucket.peek(0) is not item:
        raise RuntimeError("weighted put did not retain the expected log")
    return int(peak)


def main() -> None:
    samples = [measure_once() for _ in range(RUNS)]
    median = statistics.median(samples)
    noise_pct = (max(samples) - min(samples)) / median * 100.0
    print(f"metric=peak_memory_bytes samples={','.join(str(s) for s in samples)}")
    print(f"median={median}")
    print(f"noise_pct={noise_pct:.4f}")


if __name__ == "__main__":
    main()
