import threading

import requests

from . import config, db

_stop = threading.Event()
_thread = None


def start():
    global _thread
    _reset_stale()
    _stop.clear()
    _thread = threading.Thread(target=_run, name="payment-worker", daemon=True)
    _thread.start()


def stop():
    _stop.set()
    if _thread is not None:
        _thread.join(timeout=5)


def _run():
    while not _stop.is_set():
        worked = False
        try:
            for shard in range(config.NUM_SHARDS):
                if _process_shard(shard):
                    worked = True
            _finalize_requests()
        except Exception as e:
            print(f"[worker] error: {e}")
        if not worked:
            _stop.wait(config.WORKER_POLL_SECONDS)


def _reset_stale():
    for shard in range(config.NUM_SHARDS):
        with db.shard_conn(shard) as conn, conn.cursor() as cur:
            cur.execute("UPDATE payments SET status='PENDING' WHERE status='PROCESSING'")


def _process_shard(shard):
    rows = _claim(shard)
    for r in rows:
        _send_one(shard, r)
    return bool(rows)


def _claim(shard):
    with db.shard_conn(shard) as conn:
        cur = db.dict_cursor(conn)
        cur.execute(
            """
            UPDATE payments SET status='PROCESSING', updated_at=now()
            WHERE payment_id IN (
                SELECT payment_id FROM payments
                WHERE status='PENDING'
                ORDER BY created_at
                FOR UPDATE SKIP LOCKED
                LIMIT %s
            )
            RETURNING payment_id, request_id, store_id, coffee_type, price, currency, loyalty_card_id, attempts
            """,
            (config.WORKER_BATCH,),
        )
        return cur.fetchall()


def _send_one(shard, r):
    payment_id = str(r["payment_id"])
    body = {
        "coffeeType": r["coffee_type"],
        "price": float(r["price"]),
        "currency": r["currency"],
        "loyaltyCardId": r["loyalty_card_id"],
    }
    headers = {
        "Content-Type": "application/json",
        "Store-Id": r["store_id"],
        "Idempotency-Key": payment_id,
    }
    url = f"{config.REMOTE_BASE_URL.rstrip('/')}/api/v1/payments"

    try:
        resp = requests.post(url, json=body, headers=headers, timeout=config.REMOTE_TIMEOUT)
    except requests.RequestException as e:
        _retry_or_fail(shard, payment_id, r["attempts"], f"{type(e).__name__}: {e}")
        return

    if resp.status_code in (200, 201):
        _mark(shard, payment_id, "DONE")
    elif 400 <= resp.status_code < 500 and resp.status_code != 429:
        _mark(shard, payment_id, "FAILED")
    else:
        _retry_or_fail(shard, payment_id, r["attempts"], f"HTTP {resp.status_code}")


def _retry_or_fail(shard, payment_id, attempts, err):
    attempts = (attempts or 0) + 1
    if attempts >= config.REMOTE_MAX_ATTEMPTS:
        _mark(shard, payment_id, "FAILED", attempts=attempts)
    else:
        _mark(shard, payment_id, "PENDING", attempts=attempts)


def _mark(shard, payment_id, status, attempts=None):
    sets = ["status=%s", "updated_at=now()"]
    params = [status]
    if attempts is not None:
        sets.append("attempts=%s")
        params.append(attempts)
    params.append(payment_id)
    with db.shard_conn(shard) as conn, conn.cursor() as cur:
        cur.execute(f"UPDATE payments SET {', '.join(sets)} WHERE payment_id=%s", params)


def _finalize_requests():
    with db.shard_conn(config.META_SHARD) as conn:
        cur = db.dict_cursor(conn)
        cur.execute("SELECT request_id FROM requests WHERE status='PENDING'")
        pending = [str(row["request_id"]) for row in cur.fetchall()]

    for request_id in pending:
        remaining = 0
        for shard in range(config.NUM_SHARDS):
            with db.shard_conn(shard) as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM payments WHERE request_id=%s AND status IN ('PENDING','PROCESSING')",
                    (request_id,),
                )
                remaining += cur.fetchone()[0]
        if remaining == 0:
            with db.shard_conn(config.META_SHARD) as conn, conn.cursor() as cur:
                cur.execute("UPDATE requests SET status='DONE' WHERE request_id=%s", (request_id,))
