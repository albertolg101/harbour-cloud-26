# Async sharded payments service

Homework 1 sent each payment synchronously. This service makes the flow
asynchronous and database-backed, and stores the payments in a relational
database that is sharded across more than one Postgres instance.

## Flow

1. `POST /requests` receives a bulk batch of payments, stores them in the
   database and immediately returns a `requestId` (HTTP 202).
2. `GET /requests/{id}` returns the status of that request.
3. A background worker takes the stored payments and, for each one, creates an
   entry in the remote system (the StarHarbour Payments Service from homework 1,
   reached through Toxiproxy on `:9091`). When every payment of a request is
   processed, the request is marked `DONE`.

## Sharding

- The `payments` table is spread across `NUM_SHARDS` Postgres instances
  (2 by default), routed by `md5(payment_id) % NUM_SHARDS`.
- The number of shards is configured statically via `NUM_SHARDS`; no resharding.
- The small `requests` table lives on a single shard (`META_SHARD`); the status
  endpoint fans out across all shards to aggregate progress.

## Run

```bash
docker compose up -d
pip install -r requirements.txt
uvicorn app.main:app --host 127.0.0.1 --port 8088
python send_bulk.py ../payments.csv store-barcelona-01
```

The remote StarHarbour service (the coffee app in this repo) must be running on
`:9091`.

## Configuration

| Var | Default | Meaning |
|-----|---------|---------|
| `NUM_SHARDS` | `2` | number of database shards (static) |
| `SHARD_0_DSN`, `SHARD_1_DSN`, ... | localhost:5433, 5434 | per-shard connection string |
| `REMOTE_BASE_URL` | `http://localhost:9091` | the remote system |
| `REMOTE_MAX_ATTEMPTS` | `5` | retries before a payment is marked FAILED |
| `WORKER_BATCH` | `25` | payments claimed per shard per loop |

## Inspecting the shards

```bash
docker compose exec shard0 psql -U coffee -d coffee -c "SELECT status, count(*) FROM payments GROUP BY status;"
docker compose exec shard1 psql -U coffee -d coffee -c "SELECT status, count(*) FROM payments GROUP BY status;"
```
