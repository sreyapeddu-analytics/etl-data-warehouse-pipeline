"""run_pipeline.py — orchestrate extract -> validate -> transform -> load.

Usage:
    python src/run_pipeline.py            # from the repo root

Runs batch 1 (full load) then batch 2 (incremental load with SCD Type 2
dim_customer updates), printing the data-quality before/after report and
per-layer row counts. Rebuilds warehouse.db from scratch each run.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from extract import extract_batch
from transform import (clean_customers, clean_orders, clean_products,
                       build_dim_product, build_dim_date,
                       scd2_upsert_customers, build_fact_orders)
from data_quality import profile_raw, profile_clean, print_comparison
from load import connect, init_db, load_staging, load_dim_product, load_dim_date, load_fact, row_counts

REPO_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = REPO_ROOT / "warehouse.db"

BATCH1_DATE = "2025-07-01"   # effective date for the initial load
BATCH2_DATE = "2025-10-01"   # effective date for the incremental load


def run() -> dict:
    if DB_PATH.exists():
        DB_PATH.unlink()
    conn = connect(str(DB_PATH))
    init_db(conn)
    notes: dict = {}

    # ---------------- batch 1: full load ----------------
    raw1 = extract_batch(1)
    dq_raw = profile_raw(raw1)

    stg1 = {
        "customers": clean_customers(raw1["customers"], notes),
        "orders": clean_orders(raw1["orders"], notes),
        "products": clean_products(raw1["products"], notes),
    }
    dq_clean = profile_clean(stg1)
    print_comparison(dq_raw, dq_clean)

    load_staging(conn, stg1, batch_id=1)
    dim_product = build_dim_product(stg1["products"])
    load_dim_product(conn, dim_product)
    scd_stats1 = scd2_upsert_customers(conn, stg1["customers"], BATCH1_DATE)
    dim_date = build_dim_date(stg1["orders"]["order_date"])
    load_dim_date(conn, dim_date)
    fact1, orphans1 = build_fact_orders(stg1["orders"], conn, batch_id=1)
    n_fact1 = load_fact(conn, fact1)
    print(f"[load] batch 1: dim_customer {scd_stats1}, fact rows loaded: {n_fact1:,}, "
          f"orphan orders excluded: {orphans1:,}")

    # ---------------- batch 2: incremental load (SCD Type 2) ----------------
    raw2 = extract_batch(2)
    stg2 = {
        "customers": clean_customers(raw2["customers"], notes),
        "orders": clean_orders(raw2["orders"], notes),
    }
    load_staging(conn, stg2, batch_id=2)
    scd_stats2 = scd2_upsert_customers(conn, stg2["customers"], BATCH2_DATE)
    dim_date2 = build_dim_date(stg2["orders"]["order_date"])
    load_dim_date(conn, dim_date2)
    fact2, orphans2 = build_fact_orders(stg2["orders"], conn, batch_id=2)
    n_fact2 = load_fact(conn, fact2)
    print(f"[load] batch 2: dim_customer {scd_stats2}, fact rows loaded: {n_fact2:,}, "
          f"orphan orders excluded: {orphans2:,}")

    counts = row_counts(conn)
    print("\n=========== ROW COUNTS PER LAYER ===========")
    for table, n in counts.items():
        print(f"  {table:15s}: {n:,}")
    print("==========================================\n")
    conn.close()

    return {"dq_raw": dq_raw, "dq_clean": dq_clean, "notes": notes,
            "counts": counts, "db_path": str(DB_PATH)}


if __name__ == "__main__":
    run()
    print("Pipeline complete -> warehouse.db")
