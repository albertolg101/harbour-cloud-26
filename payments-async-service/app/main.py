import uuid
from collections import defaultdict
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from . import config, db, worker


class PaymentIn(BaseModel):
    coffeeType: str
    price: float
    currency: str
    loyaltyCardId: str


class BulkRequest(BaseModel):
    storeId: str
    payments: list[PaymentIn]


@asynccontextmanager
async def lifespan(app):
    db.init_pools()
    db.init_schema()
    worker.start()
    yield
    worker.stop()
    db.close_pools()


app = FastAPI(title="Async sharded payments", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok", "shards": config.NUM_SHARDS}


@app.post("/requests", status_code=202)
def create_request(req: BulkRequest):
    if not req.payments:
        raise HTTPException(status_code=400, detail="payments must not be empty")

    request_id = str(uuid.uuid4())

    with db.shard_conn(config.META_SHARD) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO requests (request_id, store_id, total, status) VALUES (%s, %s, %s, 'PENDING')",
            (request_id, req.storeId, len(req.payments)),
        )

    rows_by_shard = defaultdict(list)
    for p in req.payments:
        payment_id = str(uuid.uuid4())
        shard = db.shard_for(payment_id)
        rows_by_shard[shard].append((
            payment_id, request_id, req.storeId, p.coffeeType,
            float(p.price), p.currency, p.loyaltyCardId,
        ))

    for shard, rows in rows_by_shard.items():
        with db.shard_conn(shard) as conn, conn.cursor() as cur:
            cur.executemany(
                """INSERT INTO payments
                   (payment_id, request_id, store_id, coffee_type, price, currency, loyalty_card_id)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                rows,
            )

    distribution = {i: len(rows_by_shard.get(i, [])) for i in range(config.NUM_SHARDS)}
    return {"requestId": request_id, "total": len(req.payments), "shardDistribution": distribution}


@app.get("/requests/{request_id}")
def get_request(request_id: str):
    with db.shard_conn(config.META_SHARD) as conn:
        cur = db.dict_cursor(conn)
        cur.execute(
            "SELECT request_id, store_id, total, status, created_at FROM requests WHERE request_id=%s",
            (request_id,),
        )
        request_row = cur.fetchone()
    if request_row is None:
        raise HTTPException(status_code=404, detail="unknown request id")

    counts = {"PENDING": 0, "PROCESSING": 0, "DONE": 0, "FAILED": 0}
    for shard in range(config.NUM_SHARDS):
        with db.shard_conn(shard) as conn:
            cur = db.dict_cursor(conn)
            cur.execute(
                "SELECT status, COUNT(*) AS c FROM payments WHERE request_id=%s GROUP BY status",
                (request_id,),
            )
            for row in cur.fetchall():
                counts[row["status"]] = counts.get(row["status"], 0) + row["c"]

    return {
        "requestId": request_id,
        "status": request_row["status"],
        "total": request_row["total"],
        "done": counts["DONE"],
        "pending": counts["PENDING"] + counts["PROCESSING"],
        "failed": counts["FAILED"],
    }
