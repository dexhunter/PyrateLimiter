"""Runtime of one SQLiteBucket._peek_timestamp on a full persisted log.

Workload: 1_000_000 in-window rows, the log for Rate(1_000_000, per day).
A denied weight-1 put asks for offset limit-1, the oldest row, through
ORDER BY item_timestamp DESC LIMIT 1 OFFSET 999999.
"""

from __future__ import annotations

import gc
import sqlite3
import statistics
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pyrate_limiter.abstracts.rate import Duration, Rate
from pyrate_limiter.buckets.sqlite_bucket import SQLiteBucket

ROWS = 1_000_000
RUNS = 5
BASE_TS = 1_700_000_000_000


def _bucket() -> tuple[SQLiteBucket, sqlite3.Connection]:
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE rate_bucket (name VARCHAR, item_timestamp INTEGER)")
    conn.execute(
        "CREATE INDEX idx_rate_bucket_rate_item_timestamp ON rate_bucket (item_timestamp)"
    )
    batch = 200_000
    for start in range(0, ROWS, batch):
        conn.executemany(
            "INSERT INTO rate_bucket (name, item_timestamp) VALUES (?, ?)",
            (("req", BASE_TS + i) for i in range(start, min(ROWS, start + batch))),
        )
    conn.commit()
    bucket = SQLiteBucket([Rate(ROWS, int(Duration.DAY))], conn, "rate_bucket")
    return bucket, conn


def measure_once(bucket: SQLiteBucket) -> float:
    gc.collect()
    gc.disable()
    try:
        started = time.perf_counter()
        timestamp = bucket._peek_timestamp(ROWS - 1)
        elapsed_ms = (time.perf_counter() - started) * 1000.0
    finally:
        gc.enable()
    if timestamp != BASE_TS:
        raise RuntimeError(f"peek returned {timestamp}, expected oldest timestamp {BASE_TS}")
    return elapsed_ms


def main() -> None:
    bucket, conn = _bucket()
    try:
        # The first lookups pay for cold pages. Time the steady calls.
        for _ in range(4):
            measure_once(bucket)
        samples = [measure_once(bucket) for _ in range(RUNS)]
    finally:
        conn.close()
    median = statistics.median(samples)
    noise_pct = (max(samples) - min(samples)) / median * 100.0
    rendered = ",".join(f"{sample:.4f}" for sample in samples)
    print(f"metric=runtime_ms samples={rendered}")
    print(f"median={median:.4f}")
    print(f"noise_pct={noise_pct:.4f}")


if __name__ == "__main__":
    main()
