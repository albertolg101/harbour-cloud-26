import contextlib
import hashlib

from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import RealDictCursor

from . import config

_pools = {}

PAYMENTS_DDL = """
CREATE TABLE IF NOT EXISTS payments (
    payment_id UUID PRIMARY KEY,
    request_id UUID NOT NULL,
    store_id TEXT NOT NULL,
    coffee_type TEXT NOT NULL,
    price NUMERIC(12, 2) NOT NULL,
    currency TEXT NOT NULL,
    loyalty_card_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    attempts INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_payments_request ON payments(request_id);
CREATE INDEX IF NOT EXISTS idx_payments_status ON payments(status);
"""

REQUESTS_DDL = """
CREATE TABLE IF NOT EXISTS requests (
    request_id UUID PRIMARY KEY,
    store_id TEXT NOT NULL,
    total INT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def init_pools():
    for i, dsn in enumerate(config.SHARD_DSNS):
        _pools[i] = ThreadedConnectionPool(minconn=1, maxconn=10, dsn=dsn)


def close_pools():
    for pool in _pools.values():
        pool.closeall()
    _pools.clear()


def init_schema():
    for shard in range(config.NUM_SHARDS):
        with shard_conn(shard) as conn, conn.cursor() as cur:
            cur.execute(PAYMENTS_DDL)
    with shard_conn(config.META_SHARD) as conn, conn.cursor() as cur:
        cur.execute(REQUESTS_DDL)


def shard_for(key):
    digest = hashlib.md5(key.encode("utf-8")).hexdigest()
    return int(digest, 16) % config.NUM_SHARDS


@contextlib.contextmanager
def shard_conn(shard):
    pool = _pools[shard]
    conn = pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


def dict_cursor(conn):
    return conn.cursor(cursor_factory=RealDictCursor)
