# Dashboard — what the warehouse enables

This project ships the warehouse, not a BI file: any BI tool can connect to
`warehouse.db` (SQLite) and build on the marts below. The notebook
(`notebooks/analysis.ipynb`) already demonstrates every query.

## KPI definitions now answerable from the marts

| KPI | Definition | Mart query |
|---|---|---|
| Revenue | `SUM(fact_orders.line_total)` | by tier / category / month |
| Margin | `SUM(fact_orders.line_margin)` | by tier / category |
| Average order value | `AVG(fact_orders.line_total)` | by loyalty tier |
| Units sold | `COUNT(*)` / `SUM(quantity)` | by category → subcategory |
| Tier migration | customers with >1 `dim_customer` version | SCD2 history (`effective_date`/`end_date`/`is_current`) |
| Monthly trend | revenue by `dim_date.year, month` | 2024-01 → 2025-09 |

## Suggested BI build (Power BI / Tableau)

1. Connect to `warehouse.db` (SQLite ODBC/driver) or export the marts to CSV.
2. Model: `fact_orders` → `dim_customer` (on `customer_sk`, filter
   `is_current = 1` for current-state reporting), `dim_product`, `dim_date`.
3. Pages: **Overview** (revenue, margin, AOV cards + monthly trend),
   **Customers** (revenue/margin by loyalty tier + SCD2 tier-migration table),
   **Products** (category/subcategory matrix, margin %).
4. Row-level note: `dim_customer` carries both current and history rows — use
   the `is_current` flag so history never double-counts.

All figures are synthetic (seeded demo data).
