-- PostgreSQL Performance Diagnostic Toolkit - schema and seed data.

-- Tracks normalized query-level execution metrics used by slow-query analysis.
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Provides tuple-level storage statistics used in bloat investigations.
CREATE EXTENSION IF NOT EXISTS pgstattuple;

-- Customer master data.
CREATE TABLE IF NOT EXISTS customers (
  customer_id  SERIAL PRIMARY KEY,
  email        VARCHAR(255) UNIQUE NOT NULL,
  full_name    VARCHAR(100) NOT NULL,
  country      VARCHAR(50) NOT NULL,
  created_at   TIMESTAMPTZ DEFAULT NOW(),
  is_premium   BOOLEAN DEFAULT FALSE
);

COMMENT ON TABLE customers IS
'Customer dimension table: one row per shopper account.';

-- Product catalog data.
CREATE TABLE IF NOT EXISTS products (
  product_id   SERIAL PRIMARY KEY,
  name         VARCHAR(200) NOT NULL,
  category     VARCHAR(50) NOT NULL,
  price        DECIMAL(10,2) NOT NULL,
  stock_qty    INT NOT NULL DEFAULT 0,
  created_at   TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE products IS
'Product catalog table used by order line items.';

-- High-volume transactional order data.
CREATE TABLE IF NOT EXISTS orders (
  order_id     SERIAL PRIMARY KEY,
  customer_id  INT REFERENCES customers(customer_id),
  product_id   INT REFERENCES products(product_id),
  quantity     INT NOT NULL,
  unit_price   DECIMAL(10,2) NOT NULL,
  total_amount DECIMAL(10,2) NOT NULL,
  status       VARCHAR(20) CHECK (status IN
               ('PENDING','PROCESSING','SHIPPED','DELIVERED','CANCELLED')),
  order_date   TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE orders IS
'High-volume order fact table used for query plan and index diagnostics.';

-- Persisted findings emitted by diagnostic procedures.
CREATE TABLE IF NOT EXISTS diagnostic_log (
  log_id         SERIAL PRIMARY KEY,
  check_name     VARCHAR(100) NOT NULL,
  severity       VARCHAR(10) CHECK (severity IN ('OK','WARN','CRITICAL')),
  finding        TEXT NOT NULL,
  recommendation TEXT,
  checked_at     TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE diagnostic_log IS
'Persistent audit table for diagnostic findings and DBA recommendations.';

-- Large seed volume is intentional so EXPLAIN ANALYZE produces realistic plans.
-- Exact targets: 10,000 customers, 500 products, 50,000 orders (last 180 days).
-- Non-PK indexes on common order filters are intentionally omitted here to
-- support before/after optimization demos.

-- Seed customers: exactly 10,000 across 5 countries.
INSERT INTO customers (email, full_name, country, created_at, is_premium)
SELECT
  'customer' || gs || '@example.com' AS email,
  'Customer ' || gs AS full_name,
  (ARRAY['USA','India','Germany','Brazil','Japan'])[1 + ((gs - 1) % 5)] AS country,
  NOW() - ((random() * 365)::INT || ' days')::INTERVAL AS created_at,
  (random() < 0.22) AS is_premium
FROM generate_series(1, 10000) AS gs;

-- Seed products: exactly 500 across 8 categories.
INSERT INTO products (name, category, price, stock_qty, created_at)
SELECT
  'Product ' || gs AS name,
  (ARRAY['Electronics','Books','Home','Beauty','Sports','Toys','Fashion','Grocery'])[1 + ((gs - 1) % 8)] AS category,
  ROUND((5 + random() * 995)::NUMERIC, 2) AS price,
  (10 + (random() * 490)::INT) AS stock_qty,
  NOW() - ((random() * 365)::INT || ' days')::INTERVAL AS created_at
FROM generate_series(1, 500) AS gs;

-- Seed orders: exactly 50,000 spread over the last 180 days.
INSERT INTO orders (customer_id, product_id, quantity, unit_price, total_amount, status, order_date)
SELECT
  (1 + (random() * 9999)::INT) AS customer_id,
  p.product_id,
  q.quantity,
  p.price AS unit_price,
  ROUND((p.price * q.quantity)::NUMERIC, 2) AS total_amount,
  q.status,
  NOW()
    - ((random() * 180)::INT || ' days')::INTERVAL
    - ((random() * 86400)::INT || ' seconds')::INTERVAL AS order_date
FROM (
  SELECT
    (1 + (random() * 499)::INT) AS product_id,
    (1 + (random() * 4)::INT) AS quantity,
    (ARRAY['PENDING','PROCESSING','SHIPPED','DELIVERED','CANCELLED'])[1 + (random() * 4)::INT] AS status
  FROM generate_series(1, 50000)
) AS q
JOIN products p ON p.product_id = q.product_id;

ANALYZE customers;
ANALYZE products;
ANALYZE orders;
ANALYZE diagnostic_log;
