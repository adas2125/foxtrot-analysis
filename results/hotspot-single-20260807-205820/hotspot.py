#!/usr/bin/env python3

"""
Adapted from the FoxTrot Repo
"""

import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from redis.cluster import RedisCluster


if len(sys.argv) not in (4, 5):
    print(
        f"Usage: {sys.argv[0]} <host> <key> <duration_seconds> [password]",
        flush=True,
    )
    sys.exit(1)

REDIS_HOST = sys.argv[1]
KEY = sys.argv[2]
DURATION_SECONDS = int(sys.argv[3])

THREADS = int(os.environ.get("HOTSPOT_THREADS", "10"))

connection_options = {
    "host": REDIS_HOST,
    "port": 6379,
    "decode_responses": True,
    "skip_full_coverage_check": True,
    "socket_connect_timeout": 3,
    "socket_timeout": 3,
    "retry_on_timeout": True,
}

print(
    f"Connecting to {REDIS_HOST}:6379; "
    f"key={KEY}; threads={THREADS}; duration={DURATION_SECONDS}s",
    flush=True,
)

rc = RedisCluster(**connection_options)
rc.ping()
rc.set(KEY, "hotvalue")

slot = rc.keyslot(KEY)
print(f"Connected. Preloaded key={KEY}; slot={slot}", flush=True)

counts = [0 for _ in range(THREADS)]
errors = [0 for _ in range(THREADS)]
deadline = time.monotonic() + DURATION_SECONDS


def worker(index):
    """
    Worker function that repeatedly reads the hot key from Redis
    """
    local_count, local_errors = 0, 0
    while time.monotonic() < deadline:
        try:
            value = rc.get(KEY)
            if value != "hotvalue":
                local_errors += 1
            else:
                local_count += 1
        except Exception as error:
            local_errors += 1
            if local_errors <= 3:
                print(
                    f"Worker {index} error: {type(error).__name__}: {error}",
                    flush=True,
                )
            time.sleep(0.01)

        if (local_count + local_errors) % 1000 == 0:
            counts[index] = local_count
            errors[index] = local_errors

    counts[index] = local_count
    errors[index] = local_errors


start = time.monotonic()
previous_total = 0
previous_time = start

with ThreadPoolExecutor(max_workers=THREADS) as executor:
    # submit worker tasks to the executor
    futures = [executor.submit(worker, index) for index in range(THREADS)]
    while time.monotonic() < deadline:
        time.sleep(min(5, max(0, deadline - time.monotonic())))

        now = time.monotonic()
        total = sum(counts)
        total_errors = sum(errors)
        interval_rate = (total - previous_total) / max(now - previous_time, 0.001)

        print(
            f"REPORT elapsed={now-start:.1f}s "
            f"total={total} interval_ops_per_sec={interval_rate:.1f} "
            f"errors={total_errors}",
            flush=True,
        )

        previous_total = total
        previous_time = now

    # after the deadline has passed, wait for all worker threads to complete
    for future in futures:
        future.result()

elapsed = time.monotonic() - start
total = sum(counts)
total_errors = sum(errors)

print(
    f"DONE elapsed={elapsed:.2f}s total={total} "
    f"average_ops_per_sec={total/elapsed:.1f} errors={total_errors}",
    flush=True,
)

rc.close()
