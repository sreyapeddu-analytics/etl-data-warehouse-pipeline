"""load.py — load staging, dimensions and facts into the SQLite warehouse."""

import sqlite3
from pathlib import Path

DDL_PATH = Path(__file__).resolve().parent.parent / "sql" / "ddl.sql"


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    with open(DDL_PATH) as f:
        conn.executescript(f.read())
    conn.commit()


def load_staging(conn: sqlite3.Connection, staging: dict, batch_id: int) -> None:
    for entity, df in staging.items():
        frame = df.copy()
        frame["batch_id"] = batch_id
        frame.to_sql(f"stg_{entity}", conn, if_exists="append", index=False)
    conn.commit()


def load_dim_product(conn: sqlite3.Connection, dim_product) -> None:
    dim_product.to_sql("dim_product", conn, if_exists="append", index=False)
    conn.commit()


def load_dim_date(conn: sqlite3.Connection, dim_date) -> None:
    dim_date.to_sql("dim_date", conn, if_exists="append", index=False)
    # idempotent: ignore dates that already exist on incremental loads
    conn.execute(
        """DELETE FROM dim_date WHERE rowid NOT IN
           (SELECT MIN(rowid) FROM dim_date GROUP BY date_sk)""")
    conn.commit()


def load_fact(conn: sqlite3.Connection, fact) -> int:
    cur = conn.cursor()
    max_sk = cur.execute("SELECT COALESCE(MAX(order_sk), 0) FROM fact_orders").fetchone()[0]
    frame = fact.copy()
    frame.insert(0, "order_sk", range(max_sk + 1, max_sk + 1 + len(frame)))
    frame.to_sql("fact_orders", conn, if_exists="append", index=False)
    conn.commit()
    return len(frame)


def row_counts(conn: sqlite3.Connection) -> dict:
    tables = ["stg_customers", "stg_orders", "stg_products",
              "dim_customer", "dim_product", "dim_date", "fact_orders"]
    return {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in tables}
