"""Live allocations from constructing a full in-memory window of RateItems.

Workload: 100_000 RateItem values, one per consumed unit, which is what
InMemoryBucket stores for a sliding window of 100_000 per minute. Timestamps
are created before tracing so the counted blocks are the RateItem instances
and the per-instance dicts allocated when RateItem.__init__ assigns its fields.
"""

from __future__ import annotations

import statistics
import sys
import tracemalloc
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pyrate_limiter.abstracts.rate import RateItem

ITEMS = 100_000
RUNS = 5
BASE_TS = 1_700_000_000_000


def measure_once() -> int:
    timestamps = [BASE_TS + i for i in range(ITEMS)]
    tracemalloc.start()
    try:
        tracemalloc.clear_traces()
        items = [RateItem("user", timestamp, 1) for timestamp in timestamps]
        if (
            len(items) != ITEMS
            or items[0].name != "user"
            or items[0].weight != 1
            or items[-1].timestamp != timestamps[-1]
        ):
            raise RuntimeError("RateItem workload did not retain the expected log")
        snapshot = tracemalloc.take_snapshot()
        return sum(stat.count for stat in snapshot.statistics("lineno"))
    finally:
        tracemalloc.stop()


def main() -> None:
    samples = [measure_once() for _ in range(RUNS)]
    median = statistics.median(samples)
    noise_pct = (max(samples) - min(samples)) / median * 100.0
    print(f"metric=allocations samples={','.join(str(s) for s in samples)}")
    print(f"median={median}")
    print(f"noise_pct={noise_pct:.4f}")


if __name__ == "__main__":
    main()
