-- ============================================================================
-- ddl.sql — Data warehouse schema: staging -> dimensions -> facts
-- Target: SQLite (portfolio-friendly). Snowflake / Redshift equivalents for
-- the key warehouse objects are included as comments so the design reads
-- like a production dbt-style warehouse.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- STAGING  (dbt-style: stg__source__entity)
-- 1:1 with raw source files, minimally conformed (types, trimmed strings).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg_customers (
    customer_id  TEXT,
    full_name    TEXT,
    email        TEXT,
    phone        TEXT,
    city         TEXT,
    state        TEXT,
    signup_date  TEXT,      -- ISO YYYY-MM-DD after conforming
    loyalty_tier TEXT,
    batch_id     INTEGER
);

CREATE TABLE IF NOT EXISTS stg_orders (
    order_id     TEXT,
    customer_id  TEXT,
    product_id   TEXT,
    order_date   TEXT,       -- ISO YYYY-MM-DD after conforming
    quantity     INTEGER,
    unit_price   REAL,
    discount_pct REAL,
    order_status TEXT,
    batch_id     INTEGER
);

CREATE TABLE IF NOT EXISTS stg_products (
    product_id   TEXT,
    product_name TEXT,
    category     TEXT,
    subcategory  TEXT,
    list_price   REAL,
    cost         REAL,
    batch_id     INTEGER
);

-- ----------------------------------------------------------------------------
-- DIMENSIONS (dbt-style: dim__entity)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_sk  INTEGER PRIMARY KEY,   -- surrogate key
    customer_id  TEXT    NOT NULL,      -- natural key
    full_name    TEXT,
    email        TEXT,
    phone        TEXT,
    city         TEXT,
    state        TEXT,
    signup_date  TEXT,
    loyalty_tier TEXT,
    effective_date TEXT NOT NULL,       -- SCD Type 2 bookkeeping
    end_date       TEXT NOT NULL,
    is_current     INTEGER NOT NULL     -- 1 = current row, 0 = history
);
-- Snowflake equivalent:
-- CREATE OR REPLACE TABLE dim_customer (
--     customer_sk NUMBER AUTOINCREMENT PRIMARY KEY,
--     customer_id VARCHAR NOT NULL, ...
-- );
-- Redshift equivalent: customer_sk BIGINT IDENTITY(1,1) PRIMARY KEY, with DISTKEY(customer_id).

CREATE TABLE IF NOT EXISTS dim_product (
    product_sk   INTEGER PRIMARY KEY,
    product_id   TEXT NOT NULL UNIQUE,
    product_name TEXT,
    category     TEXT,
    subcategory  TEXT,
    list_price   REAL,
    cost         REAL
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_sk      INTEGER PRIMARY KEY,   -- YYYYMMDD
    date_value   TEXT NOT NULL UNIQUE,
    year         INTEGER,
    quarter      INTEGER,
    month        INTEGER,
    month_name   TEXT,
    day          INTEGER,
    day_of_week  TEXT,
    week_of_year INTEGER
);

-- ----------------------------------------------------------------------------
-- FACTS (dbt-style: fct__entity)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_orders (
    order_sk     INTEGER PRIMARY KEY,
    order_id     TEXT    NOT NULL,
    customer_sk  INTEGER NOT NULL REFERENCES dim_customer(customer_sk),
    product_sk   INTEGER NOT NULL REFERENCES dim_product(product_sk),
    date_sk      INTEGER NOT NULL REFERENCES dim_date(date_sk),
    quantity     INTEGER NOT NULL,
    unit_price   REAL    NOT NULL,
    discount_pct REAL    NOT NULL,
    line_total   REAL    NOT NULL,      -- quantity * unit_price * (1 - discount)
    line_cost    REAL    NOT NULL,      -- quantity * cost
    line_margin  REAL    NOT NULL,      -- line_total - line_cost
    order_status TEXT,
    batch_id     INTEGER
);
-- Snowflake equivalent adds clustering: CLUSTER BY (date_sk);
-- Redshift equivalent: DISTKEY(customer_sk) SORTKEY(date_sk).

CREATE INDEX IF NOT EXISTS idx_fact_orders_customer ON fact_orders(customer_sk);
CREATE INDEX IF NOT EXISTS idx_fact_orders_product  ON fact_orders(product_sk);
CREATE INDEX IF NOT EXISTS idx_fact_orders_date     ON fact_orders(date_sk);
CREATE INDEX IF NOT EXISTS idx_dim_customer_nk      ON dim_customer(customer_id, is_current);
