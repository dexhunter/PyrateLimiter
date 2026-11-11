"""Runtime of one SQLiteBucket.leak on a large persisted log.

Workload: an in-memory SQLite bucket holding 120_000 rows (a durable sliding
window after a busy period). leak() expires the oldest 100_000 of them.
Setup and the connection copy sit outside the timed call.
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

ROWS = 120_000
EXPIRE = 100_000
RUNS = 5
BASE_TS = 1_700_000_000_000


def _source() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE rate_bucket (name VARCHAR, item_timestamp INTEGER)")
    conn.execute(
        "CREATE INDEX idx_rate_bucket_rate_item_timestamp ON rate_bucket (item_timestamp)"
    )
    conn.executemany(
        "INSERT INTO rate_bucket (name, item_timestamp) VALUES (?, ?)",
        (("req", BASE_TS + i) for i in range(ROWS)),
    )
    conn.commit()
    return conn


def measure_once(source: sqlite3.Connection) -> float:
    conn = sqlite3.connect(":memory:")
    source.backup(conn)
    bucket = SQLiteBucket([Rate(ROWS, int(Duration.DAY))], conn, "rate_bucket")
    now = BASE_TS + EXPIRE + int(Duration.DAY)
    gc.collect()
    gc.disable()
    try:
        started = time.perf_counter()
        removed = bucket.leak(now)
        elapsed_ms = (time.perf_counter() - started) * 1000.0
    finally:
        gc.enable()
    left = bucket.count()
    conn.close()
    if removed != EXPIRE or left != ROWS - EXPIRE:
        raise RuntimeError(f"leak removed {removed}, left {left}")
    return elapsed_ms


def main() -> None:
    source = _source()
    try:
        measure_once(source)  # warm the allocator; not part of the sample
        samples = [measure_once(source) for _ in range(RUNS)]
    finally:
        source.close()
    median = statistics.median(samples)
    noise_pct = (max(samples) - min(samples)) / median * 100.0
    rendered = ",".join(f"{sample:.4f}" for sample in samples)
    print(f"metric=runtime_ms samples={rendered}")
    print(f"median={median:.4f}")
    print(f"noise_pct={noise_pct:.4f}")


if __name__ == "__main__":
    main()
