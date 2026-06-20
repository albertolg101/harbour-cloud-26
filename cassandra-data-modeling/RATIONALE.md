# StarHarbour — Cassandra design rationale

Query-first model: one table per access pattern (Q1–Q7). Cassandra cannot join
and only reads efficiently by partition key + clustering order, so each query
gets a table whose partition key matches its `WHERE` and whose clustering matches
its `ORDER BY`. Data is duplicated on purpose to avoid joins.

## 1. Query → predicate mapping

| Query | Table | Partition key | Clustering (order) | Application predicate |
|-------|-------|---------------|--------------------|-----------------------|
| Q1 | `orders_by_customer` | `customer_id` | `order_ts DESC, order_id` | `WHERE customer_id = ?` |
| Q2 | `order_items_by_order` | `order_id` | `product_id` | `WHERE order_id = ?` |
| Q3 | `customers_by_id` | `customer_id` | — | `WHERE customer_id = ?` |
| Q4 | `customers_by_email` | `email` | — | `WHERE email = ?` |
| Q5 | `orders_by_store_day` | `(store_id, order_date)` | `order_ts DESC, order_id` | `WHERE store_id = ? AND order_date = ?` |
| Q6 | `product_sales_by_store_month` | `(store_id, year_month)` | `product_id` | `WHERE store_id = ? AND year_month = ?` then sort by `units_sold` desc app-side |
| Q7 | `reviews_by_product` | `(product_id, bucket)` | `review_ts DESC, review_id` | `WHERE product_id = ? AND bucket = ?` (page back over buckets) |

Every read hits exactly one partition. No secondary indexes, no `ALLOW FILTERING`.

## 2. Partition-size analysis

Scale (Section 2): 1,500 stores, 300 SKUs, 8M customers, 1,200 orders/store/day
(≈1.8M orders/day chain-wide), ~3 items/order, popular drinks → 100k+ reviews.
Target: well under ~100k rows and ~100 MB per partition.

| Table | Rows per partition | Approx size | Bounded? | Reasoning |
|-------|--------------------|-------------|----------|-----------|
| `orders_by_customer` | lifetime orders of one customer; active member ≈260/yr → ~2.6k over 10 yr; heavy user ~11k | ~1–2 MB | Yes (slow growth) | Grows with tenure only; a register does 1,200/day but a *person* does not. See note below for the tail. |
| `order_items_by_order` | ~3 (catering outlier dozens) | < 1 KB | Yes | One order’s items; naturally tiny. |
| `customers_by_id` | 1 | < 1 KB | Yes | One profile per customer. |
| `customers_by_email` | 1 | < 1 KB | Yes | Email is unique. |
| `orders_by_store_day` | 1,200 (one store, one day) | ~150 KB | Yes (day bucket) | Without the day bucket, store traffic accumulates forever → unbounded. Bucketing by day caps it at one day of sales. |
| `product_sales_by_store_month` | ≤ 300 (active SKUs) | ~30 KB | Yes (month + finite SKUs) | A store sells at most the menu; month bucket prevents lifetime accumulation. |
| `reviews_by_product` | per product per month | ≤ ~tens of thousands | < ~15 MB | **Flagged & redesigned.** Unbucketed, a viral drink reaches 100k+ rows/MBs of `comment` text → unbounded. Bucketing by month spreads it; even a spike month stays < 100k. |

**Unbounded partitions flagged and fixed by bucketing:** `orders_by_store_day`
(bucket by **day**) and `reviews_by_product` (bucket by **month**). Bucket
granularity is chosen so the busiest partition stays under the limits: a store
does ~1,200 orders/day (day bucket is plenty), and a top drink can exceed 100k
reviews per *year* but not per *month*.

**Tail note for `orders_by_customer`:** realistic consumption keeps this bounded,
but if a corporate/account customer placed thousands of orders per year, switch
the key to `((customer_id, year), order_ts, order_id)` and have the app read the
current year then page back. Stated as an assumption rather than applied, to keep
Q1 a single-partition read for the common case.

## 3. Denormalization

Columns deliberately duplicated and why:

- **Order header** (`store_id, employee_id, order_ts, status, total_amount,
  payment_method`) lives in both `orders_by_customer` (Q1) and
  `orders_by_store_day` (Q5). Each query needs the header without a join.
- **Customer profile** lives in both `customers_by_id` (Q3) and
  `customers_by_email` (Q4). Login and id-lookup must each be a single read.
- **`product_name`** is copied into `order_items_by_order` (Q2). A line item is a
  point-in-time snapshot — the receipt should show the name *as sold*, so this
  copy is intentionally never back-updated when a product is renamed.
- **`customer_name`** is copied into `reviews_by_product` (Q7) so the review list
  renders without a customer lookup.

### Write fan-out for a new order (N line items)

| Table | Operation | Rows |
|-------|-----------|------|
| `orders_by_customer` | INSERT header | 1 |
| `orders_by_store_day` | INSERT header | 1 |
| `order_items_by_order` | INSERT item | N |
| `product_sales_by_store_month` | UPDATE counter `units_sold += qty` | N |

So one order = `2 + 2N` writes (≈ 8 for the average 3-item order). The two header
inserts can go in a `BATCH` keyed for atomicity; the counter updates are separate
(counters are not allowed in a logged batch with non-counter tables).

## 4. Update handling (application perspective)

Cassandra updates the same way it inserts, so the app must touch **every table
that holds a copy** of the changed column:

- **Profile edit (name/phone/tier/points):** update `customers_by_id` **and**
  `customers_by_email`.
- **Email change:** `customers_by_email` is keyed by email, so DELETE the old
  email row and INSERT the new one; also update the `email` column in
  `customers_by_id`.
- **Order status change** (e.g. `PLACED → COMPLETED`): update `orders_by_customer`
  and `orders_by_store_day`. Both need the full clustering key
  (`order_ts`, `order_id`, plus `store_id, order_date` for Q5), so the app keeps
  those keys with the order or reads them first.
- **Loyalty points after an order:** update both customer tables.
- **Product rename / price change:** *not* propagated to historical
  `order_items_by_order` rows (they are point-in-time). Future menu reads use the
  products reference data; only forward writes see the new value.

## 5. Key decisions

**Alternate key (email, Q4).** Chosen: duplicate the *full* profile into
`customers_by_email` so login is one read. Alternative — store only
`email → customer_id` and do a second read against `customers_by_id` — halves the
duplication but doubles login latency on the hottest path. With only 8M tiny
1-row partitions the storage cost is negligible, so full duplication wins. Cost:
profile writes hit two tables and email changes need a delete+insert.

**Aggregates (Q6).** Chosen: a `counter` table incremented at sale time
(`product_sales_by_store_month`). Reads are a single ≤300-row partition sorted
app-side for top-N (you cannot `ORDER BY` a counter, and 300 rows is trivial to
sort). Caveat: counters are **not idempotent**, so order processing must be
exactly-once or deduplicated, otherwise a retried write double-counts. If strict
accuracy is required, treat `order_items_by_order` as the source of truth and
recompute the monthly leaderboard in a batch job instead of live counters.
Product names for the leaderboard are resolved from the 300-row products
reference data (easily cached in the app).

**Many-to-many (`product_supplier`) and unused entities.** None of Q1–Q7 query
suppliers, inventory, employees, or the store directory, so — query-first — they
get **no table** here; their fields appear only where denormalized (e.g.
`employee_id` on an order). If a supplier access pattern appeared later (e.g.
"suppliers of a product" / "products from a supplier"), it would be modeled as
two tables, `suppliers_by_product` and `products_by_supplier`, duplicating the
link both ways — never a join table to be joined.

## 6. Assumptions

- UUID surrogate keys for all ids; `order_ts`/`review_ts` are `timestamp`;
  money is `decimal`.
- `order_date` is the calendar day of `order_ts`; `year_month` / review `bucket`
  are `'YYYY-MM'` strings derived by the app at write time.
- RF=3 on `datacenter1` to match the 3-node cluster. Typical ops:
  reads/writes at `LOCAL_QUORUM` for strong-enough consistency on this RF.

## How to apply

```bash
docker compose up -d
# wait for the seed to report UN (up/normal)
docker exec -it cassandra-1 cqlsh -e "DESCRIBE keyspaces"
docker exec -i cassandra-1 cqlsh < schema.cql
docker exec -it cassandra-1 cqlsh -e "USE starharbour; DESCRIBE tables"
```
