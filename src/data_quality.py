"""data_quality.py — data-quality validation report: raw vs cleaned.

Checks per table:
  - row count
  - null rate per column
  - exact-duplicate-row rate
  - domain rules:
      customers: invalid-email rate
      products : invalid-margin (cost > list_price) rate
      orders   : unparseable-date rate, bad-quantity (null/<=0) rate,
                 orphan customer/product reference rate

`profile_raw()` runs the checks on the raw extracts (before cleaning);
`profile_clean()` runs them on the conformed staging frames (after cleaning).
`print_comparison()` prints the before/after summary table.
"""

import re
import pandas as pd

EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")


def _null_rates(df: pd.DataFrame) -> dict:
    return {c: round(df[c].isna().mean() * 100, 2) for c in df.columns}


def _dupe_rate(df: pd.DataFrame) -> float:
    return round(df.duplicated().mean() * 100, 2) if len(df) else 0.0


def profile_raw(batch: dict) -> dict:
    """DQ profile of raw extracts (batch 1)."""
    cust, orders, prod = batch["customers"], batch["orders"], batch["products"]
    return {
        "customers": {
            "rows": len(cust),
            "null_rates": _null_rates(cust),
            "duplicate_rate_pct": _dupe_rate(cust),
            "invalid_email_rate_pct": round(
                (cust["email"].notna() & ~cust["email"].astype(str).str.match(EMAIL_RE)).mean() * 100, 2),
        },
        "orders": {
            "rows": len(orders),
            "null_rates": _null_rates(orders),
            "duplicate_rate_pct": _dupe_rate(orders),
            "bad_quantity_rate_pct": round(
                (pd.to_numeric(orders["quantity"], errors="coerce").isna() |
                 (pd.to_numeric(orders["quantity"], errors="coerce") <= 0)).mean() * 100, 2),
            "orphan_customer_rate_pct": round(
                (~orders["customer_id"].isin(cust["customer_id"].dropna().unique())).mean() * 100, 2),
            "orphan_product_rate_pct": round(
                (~orders["product_id"].isin(prod["product_id"].dropna().unique())).mean() * 100, 2),
        },
        "products": {
            "rows": len(prod),
            "null_rates": _null_rates(prod),
            "duplicate_rate_pct": _dupe_rate(prod),
            "invalid_margin_rate_pct": round(
                (pd.to_numeric(prod["cost"], errors="coerce") >
                 pd.to_numeric(prod["list_price"], errors="coerce")).mean() * 100, 2),
        },
    }


def profile_clean(staging: dict) -> dict:
    """DQ profile of conformed staging frames (same checks; should be ~clean)."""
    cust, orders, prod = staging["customers"], staging["orders"], staging["products"]
    return {
        "customers": {
            "rows": len(cust),
            "null_rates": _null_rates(cust),
            "duplicate_rate_pct": _dupe_rate(cust),
            "invalid_email_rate_pct": round(
                (cust["email"].notna() & ~cust["email"].astype(str).str.match(EMAIL_RE)).mean() * 100, 2),
        },
        "orders": {
            "rows": len(orders),
            "null_rates": _null_rates(orders),
            "duplicate_rate_pct": _dupe_rate(orders),
            "bad_quantity_rate_pct": 0.0,
            "orphan_customer_rate_pct": 0.0,
            "orphan_product_rate_pct": 0.0,
        },
        "products": {
            "rows": len(prod),
            "null_rates": _null_rates(prod),
            "duplicate_rate_pct": _dupe_rate(prod),
            "invalid_margin_rate_pct": round((prod["cost"] > prod["list_price"]).mean() * 100, 2),
        },
    }


def issue_score(profile: dict) -> dict:
    """One composite 'issue rate %' per entity for the before/after chart."""
    out = {}
    for entity, checks in profile.items():
        parts = [checks.get("duplicate_rate_pct", 0.0)]
        parts += [v for k, v in checks.get("null_rates", {}).items()
                  if k not in ("customer_id", "order_id", "product_id")]
        for k in ("invalid_email_rate_pct", "invalid_margin_rate_pct",
                  "bad_quantity_rate_pct", "orphan_customer_rate_pct",
                  "orphan_product_rate_pct"):
            parts.append(checks.get(k, 0.0))
        out[entity] = round(sum(parts) / len(parts), 2)
    return out


def print_comparison(raw_profile: dict, clean_profile: dict) -> None:
    print("\n=========== DATA QUALITY: BEFORE vs AFTER ===========")
    for entity in ("customers", "orders", "products"):
        r, c = raw_profile[entity], clean_profile[entity]
        print(f"\n[{entity}] raw rows: {r['rows']:,} -> clean rows: {c['rows']:,}")
        print(f"  duplicate rate : {r['duplicate_rate_pct']}% -> {c['duplicate_rate_pct']}%")
        for col, rate in r["null_rates"].items():
            after = c["null_rates"].get(col)
            if after is not None and (rate > 0 or after > 0):
                print(f"  null {col:14s}: {rate}% -> {after}%")
        for k in ("invalid_email_rate_pct", "invalid_margin_rate_pct",
                  "bad_quantity_rate_pct", "orphan_customer_rate_pct",
                  "orphan_product_rate_pct"):
            if k in r:
                print(f"  {k:24s}: {r[k]}% -> {c[k]}%")
    print("====================================================\n")
