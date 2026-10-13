"""transform.py — cleaning, conforming, surrogate keys, SCD Type 2.

Cleaning rules (documented for the portfolio; every rule reports a count):
  customers:
    - drop exact duplicate rows
    - dedupe natural key customer_id, keeping the row with fewest nulls
    - strip whitespace; title-case names/cities; state -> UPPER
    - emails -> lower; regex-validate; invalid -> NULL (counted)
    - signup_date parsed across MM/DD/YYYY, YYYY-MM-DD, DD-MM-YYYY -> ISO date
      (unparseable or null customer_id -> row dropped, counted)
    - loyalty_tier canonicalised to {Gold, Silver, Bronze, Platinum}; null -> "Unknown"
  products:
    - drop exact duplicates; dedupe product_id keeping most complete row
    - category/subcategory -> title case
    - null cost -> imputed with category median (counted)
    - cost > list_price -> capped at list_price, margin = 0 (counted)
  orders:
    - drop exact duplicates; dedupe order_id keeping first
    - order_date parsed across 3 formats -> ISO (failures dropped, counted)
    - quantity: null or <= 0 -> row dropped (counted); cast to int
    - unit_price null -> row dropped (counted)
    - discount_pct null -> 0; > 100 -> capped at 100 (counted)
    - order_status -> title case, canonical set
    - orphan customer_id / product_id rows excluded from the fact (counted,
      kept in staging for the DQ report)

SCD Type 2 (dim_customer): tracked attributes are
full_name, email, phone, city, loyalty_tier. A change closes the current row
(end_date = batch_date - 1 day, is_current = 0) and inserts a new row with a
new surrogate key (effective_date = batch_date, end_date = 9999-12-31).
"""

import re
from datetime import date, timedelta

import numpy as np
import pandas as pd

EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")


def _val(v):
    """Coerce a pandas/numpy scalar to a sqlite-bindable Python value (None-safe)."""
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return float(v)
    return v
TIER_CANON = {"Gold": "Gold", "Silver": "Silver", "Bronze": "Bronze", "Platinum": "Platinum"}
STATUS_CANON = {"Pending", "Shipped", "Delivered", "Cancelled", "Returned"}
TRACKED_ATTRS = ["full_name", "email", "phone", "city", "loyalty_tier"]
END_OF_TIME = "9999-12-31"


def parse_mixed_date(series: pd.Series) -> pd.Series:
    """Parse dates given in mixed formats; return ISO strings, NaT on failure."""
    out = pd.Series(pd.NaT, index=series.index)
    remaining = series.copy()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        parsed = pd.to_datetime(remaining, format=fmt, errors="coerce")
        out = out.fillna(parsed)
        remaining = remaining[parsed.isna()]
        if remaining.empty:
            break
    return out.dt.strftime("%Y-%m-%d").where(out.notna(), None)


def clean_customers(df: pd.DataFrame, notes: dict) -> pd.DataFrame:
    df = df.copy()
    notes["customers_raw_rows"] = len(df)
    df = df.drop_duplicates()
    notes["customers_exact_dupes_dropped"] = notes["customers_raw_rows"] - len(df)

    df = df[df["customer_id"].notna() & (df["customer_id"].str.strip() != "")]
    # dedupe natural key: keep the most complete record
    df["_nulls"] = df.isna().sum(axis=1)
    df = df.sort_values("_nulls").drop_duplicates("customer_id", keep="first").drop(columns="_nulls")
    notes["customers_after_dedupe"] = len(df)

    for c in ["full_name", "city", "state", "loyalty_tier"]:
        df[c] = df[c].str.strip()
    df["full_name"] = df["full_name"].str.title()
    df["city"] = df["city"].str.title()
    df["state"] = df["state"].str.upper()
    df["email"] = df["email"].str.strip().str.lower()
    bad_email = df["email"].notna() & ~df["email"].str.match(EMAIL_RE)
    notes["customers_invalid_emails_nulled"] = int(bad_email.sum())
    df.loc[bad_email, "email"] = None

    df["signup_date"] = parse_mixed_date(df["signup_date"])
    bad_date = df["signup_date"].isna()
    notes["customers_bad_dates_dropped"] = int(bad_date.sum())
    df = df[~bad_date]

    df["loyalty_tier"] = df["loyalty_tier"].map(
        lambda t: TIER_CANON.get(str(t).strip().title(), "Unknown") if pd.notna(t) else "Unknown"
    )
    notes["customers_clean_rows"] = len(df)
    return df.reset_index(drop=True)


def clean_products(df: pd.DataFrame, notes: dict) -> pd.DataFrame:
    df = df.copy()
    notes["products_raw_rows"] = len(df)
    df = df.drop_duplicates()
    notes["products_exact_dupes_dropped"] = notes["products_raw_rows"] - len(df)

    df = df[df["product_id"].notna()]
    df["_nulls"] = df.isna().sum(axis=1)
    df = df.sort_values("_nulls").drop_duplicates("product_id", keep="first").drop(columns="_nulls")

    for c in ["product_name", "category", "subcategory"]:
        df[c] = df[c].str.strip().str.title()
    df["list_price"] = pd.to_numeric(df["list_price"], errors="coerce")
    df["cost"] = pd.to_numeric(df["cost"], errors="coerce")

    null_cost = df["cost"].isna()
    notes["products_null_cost_imputed"] = int(null_cost.sum())
    df.loc[null_cost, "cost"] = df.groupby("category")["cost"].transform("median")

    bad_margin = df["cost"] > df["list_price"]
    notes["products_invalid_margin_capped"] = int(bad_margin.sum())
    df.loc[bad_margin, "cost"] = df.loc[bad_margin, "list_price"]  # margin -> 0

    notes["products_clean_rows"] = len(df)
    return df.reset_index(drop=True)


def clean_orders(df: pd.DataFrame, notes: dict) -> pd.DataFrame:
    df = df.copy()
    notes["orders_raw_rows"] = len(df)
    df = df.drop_duplicates()
    notes["orders_exact_dupes_dropped"] = notes["orders_raw_rows"] - len(df)

    df = df[df["order_id"].notna()].drop_duplicates("order_id", keep="first")
    notes["orders_after_dedupe"] = len(df)

    df["order_date"] = parse_mixed_date(df["order_date"])
    bad_date = df["order_date"].isna()
    notes["orders_bad_dates_dropped"] = int(bad_date.sum())
    df = df[~bad_date]

    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    bad_qty = df["quantity"].isna() | (df["quantity"] <= 0)
    notes["orders_bad_quantity_dropped"] = int(bad_qty.sum())
    df = df[~bad_qty]
    df["quantity"] = df["quantity"].astype(int)

    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    bad_price = df["unit_price"].isna() | (df["unit_price"] <= 0)
    notes["orders_bad_price_dropped"] = int(bad_price.sum())
    df = df[~bad_price]

    df["discount_pct"] = pd.to_numeric(df["discount_pct"], errors="coerce").fillna(0.0)
    bad_disc = df["discount_pct"] > 100
    notes["orders_discount_capped"] = int(bad_disc.sum())
    df.loc[bad_disc, "discount_pct"] = 100.0

    df["order_status"] = df["order_status"].str.strip().str.title()
    df.loc[~df["order_status"].isin(STATUS_CANON), "order_status"] = "Pending"

    notes["orders_clean_rows"] = len(df)
    return df.reset_index(drop=True)


# ----------------------------------------------------------------------------
# Dimensions & facts
# ----------------------------------------------------------------------------

def build_dim_product(products: pd.DataFrame) -> pd.DataFrame:
    dim = products.copy()
    dim.insert(0, "product_sk", range(1, len(dim) + 1))
    return dim[["product_sk", "product_id", "product_name", "category",
                "subcategory", "list_price", "cost"]]


def build_dim_date(order_dates: pd.Series) -> pd.DataFrame:
    dates = pd.to_datetime(pd.Series(order_dates).unique()).sort_values()
    full = pd.date_range(dates.min(), dates.max(), freq="D")
    df = pd.DataFrame({"date_value": full.strftime("%Y-%m-%d")})
    ts = pd.to_datetime(df["date_value"])
    df["date_sk"] = ts.dt.strftime("%Y%m%d").astype(int)
    df["year"] = ts.dt.year
    df["quarter"] = ts.dt.quarter
    df["month"] = ts.dt.month
    df["month_name"] = ts.dt.month_name()
    df["day"] = ts.dt.day
    df["day_of_week"] = ts.dt.day_name()
    df["week_of_year"] = ts.dt.isocalendar().week.astype(int)
    return df[["date_sk", "date_value", "year", "quarter", "month",
               "month_name", "day", "day_of_week", "week_of_year"]]


def scd2_upsert_customers(conn, customers: pd.DataFrame, batch_date: str) -> dict:
    """SCD Type 2 load of dim_customer. Returns counts of inserted/updated/unchanged."""
    cur = conn.cursor()
    stats = {"inserted": 0, "scd2_updated": 0, "unchanged": 0}
    existing = pd.read_sql(
        "SELECT * FROM dim_customer WHERE is_current = 1", conn)
    max_sk = pd.read_sql("SELECT COALESCE(MAX(customer_sk), 0) AS m FROM dim_customer",
                         conn)["m"].iloc[0]

    existing_map = {r["customer_id"]: r for _, r in existing.iterrows()}
    prev_day = (pd.to_datetime(batch_date) - timedelta(days=1)).strftime("%Y-%m-%d")

    for _, row in customers.iterrows():
        cid = row["customer_id"]
        if cid in existing_map:
            old = existing_map[cid]
            changed = any(
                (str(old[a]) if pd.notna(old[a]) else None) !=
                (str(row[a]) if pd.notna(row[a]) else None)
                for a in TRACKED_ATTRS
            )
            if changed:
                cur.execute(
                    "UPDATE dim_customer SET end_date = ?, is_current = 0 "
                    "WHERE customer_sk = ?", (prev_day, int(old["customer_sk"])))
                max_sk += 1
                cur.execute(
                    """INSERT INTO dim_customer
                       (customer_sk, customer_id, full_name, email, phone, city, state,
                        signup_date, loyalty_tier, effective_date, end_date, is_current)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)""",
                    (_val(max_sk), _val(cid), _val(row["full_name"]), _val(row["email"]),
                     _val(row["phone"]), _val(row["city"]), _val(row["state"]),
                     _val(row["signup_date"]), _val(row["loyalty_tier"]),
                     batch_date, END_OF_TIME))
                stats["scd2_updated"] += 1
            else:
                stats["unchanged"] += 1
        else:
            max_sk += 1
            cur.execute(
                """INSERT INTO dim_customer
                   (customer_sk, customer_id, full_name, email, phone, city, state,
                    signup_date, loyalty_tier, effective_date, end_date, is_current)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)""",
                (_val(max_sk), _val(cid), _val(row["full_name"]), _val(row["email"]),
                 _val(row["phone"]), _val(row["city"]), _val(row["state"]),
                 _val(row["signup_date"]), _val(row["loyalty_tier"]),
                 batch_date, END_OF_TIME))
            stats["inserted"] += 1
    conn.commit()
    return stats


def build_fact_orders(orders: pd.DataFrame, conn, batch_id: int) -> tuple:
    """Join orders to current dim rows; return (fact_df, orphans_dropped)."""
    cust = pd.read_sql(
        "SELECT customer_sk, customer_id FROM dim_customer WHERE is_current = 1", conn)
    prod = pd.read_sql("SELECT product_sk, product_id, cost FROM dim_product", conn)
    dmap = pd.read_sql("SELECT date_sk, date_value FROM dim_date", conn)

    fact = orders.merge(cust, on="customer_id", how="left") \
                 .merge(prod, on="product_id", how="left") \
                 .merge(dmap, left_on="order_date", right_on="date_value", how="left")
    orphans = fact[fact[["customer_sk", "product_sk"]].isna().any(axis=1)]
    fact = fact.dropna(subset=["customer_sk", "product_sk", "date_sk"]).copy()

    fact["line_total"] = (fact["quantity"] * fact["unit_price"]
                          * (1 - fact["discount_pct"] / 100)).round(2)
    fact["line_cost"] = (fact["quantity"] * fact["cost"]).round(2)
    fact["line_margin"] = (fact["line_total"] - fact["line_cost"]).round(2)
    fact["batch_id"] = batch_id
    fact = fact[["order_id", "customer_sk", "product_sk", "date_sk", "quantity",
                 "unit_price", "discount_pct", "line_total", "line_cost",
                 "line_margin", "order_status", "batch_id"]]
    return fact.reset_index(drop=True), len(orphans)
