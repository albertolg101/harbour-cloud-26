-- StarHarbour / BeanFlow — query-first Cassandra schema. One table per access
-- pattern (Q1-Q7). RF 3 matches the 3-node datacenter1 cluster. See RATIONALE.md.

CREATE KEYSPACE IF NOT EXISTS starharbour
WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 3};

USE starharbour;

-- Q1: a customer's order history, most recent first.
CREATE TABLE IF NOT EXISTS orders_by_customer (
    customer_id    uuid,
    order_ts       timestamp,
    order_id       uuid,
    store_id       uuid,
    employee_id    uuid,
    status         text,
    total_amount   decimal,
    payment_method text,
    PRIMARY KEY ((customer_id), order_ts, order_id)
) WITH CLUSTERING ORDER BY (order_ts DESC, order_id ASC);

-- Q2: all line items for a given order.
CREATE TABLE IF NOT EXISTS order_items_by_order (
    order_id     uuid,
    product_id   uuid,
    product_name text,
    quantity     int,
    unit_price   decimal,
    PRIMARY KEY ((order_id), product_id)
);

-- Q3: customer profile by customer_id.
CREATE TABLE IF NOT EXISTS customers_by_id (
    customer_id    uuid,
    full_name      text,
    email          text,
    phone          text,
    signup_date    date,
    loyalty_tier   text,
    loyalty_points int,
    PRIMARY KEY ((customer_id))
);

-- Q4: customer profile by email (login).
CREATE TABLE IF NOT EXISTS customers_by_email (
    email          text,
    customer_id    uuid,
    full_name      text,
    phone          text,
    signup_date    date,
    loyalty_tier   text,
    loyalty_points int,
    PRIMARY KEY ((email))
);

-- Q5: all orders at a store on a given day, newest first (day bucket).
CREATE TABLE IF NOT EXISTS orders_by_store_day (
    store_id       uuid,
    order_date     date,
    order_ts       timestamp,
    order_id       uuid,
    customer_id    uuid,
    employee_id    uuid,
    status         text,
    total_amount   decimal,
    payment_method text,
    PRIMARY KEY ((store_id, order_date), order_ts, order_id)
) WITH CLUSTERING ORDER BY (order_ts DESC, order_id ASC);

-- Q6: top-selling products for a store in a given month (counter, sorted app-side).
CREATE TABLE IF NOT EXISTS product_sales_by_store_month (
    store_id   uuid,
    year_month text,
    product_id uuid,
    units_sold counter,
    PRIMARY KEY ((store_id, year_month), product_id)
);

-- Q7: all reviews for a product, newest first (month bucket).
CREATE TABLE IF NOT EXISTS reviews_by_product (
    product_id    uuid,
    bucket        text,
    review_ts     timestamp,
    review_id     uuid,
    customer_id   uuid,
    customer_name text,
    rating        int,
    comment       text,
    PRIMARY KEY ((product_id, bucket), review_ts, review_id)
) WITH CLUSTERING ORDER BY (review_ts DESC, review_id ASC);
