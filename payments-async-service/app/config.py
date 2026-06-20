import os

NUM_SHARDS = int(os.getenv("NUM_SHARDS", "2"))


def _shard_dsn(i):
    return os.getenv(f"SHARD_{i}_DSN", f"postgresql://coffee:coffee@localhost:{5433 + i}/coffee")


SHARD_DSNS = [_shard_dsn(i) for i in range(NUM_SHARDS)]
META_SHARD = 0

REMOTE_BASE_URL = os.getenv("REMOTE_BASE_URL", "http://localhost:9091")
REMOTE_TIMEOUT = float(os.getenv("REMOTE_TIMEOUT", "3.0"))
REMOTE_MAX_ATTEMPTS = int(os.getenv("REMOTE_MAX_ATTEMPTS", "5"))

WORKER_POLL_SECONDS = float(os.getenv("WORKER_POLL_SECONDS", "1.0"))
WORKER_BATCH = int(os.getenv("WORKER_BATCH", "25"))
