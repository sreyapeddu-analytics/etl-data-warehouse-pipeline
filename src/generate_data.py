"""
generate_data.py — create synthetic *messy* raw retail data for the ETL pipeline.

All data is SYNTHETIC and generated from a fixed seed (random_state=42), so the
pipeline run is fully reproducible. The raw files are intentionally dirty:
  - nulls in emails, cities, costs, quantities, discounts
  - exact duplicate rows
  - inconsistent casing ("Gold" vs "GOLD", "shipped" vs "Shipped")
  - mixed date formats (MM/DD/YYYY, YYYY-MM-DD, DD-MM-YYYY)
  - invalid emails (missing @ / domain), negative quantities, discount > 100%
  - orphan order rows referencing customers/products that do not exist
  - list_price < cost on some products

Outputs (data/raw/):
  customers_raw.csv, orders_raw.csv, products_raw.csv          (batch 1)
  batch2/customers_batch2.csv, batch2/orders_batch2.csv        (batch 2, incremental)

Batch 2 drives the SCD Type 2 demo: some customers change city/loyalty_tier,
and brand-new customers + orders arrive.
"""

import random
from pathlib import Path

import numpy as np
import pandas as pd

SEED = 42
RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

FIRST_NAMES = [
    "Aarav", "Maya", "Liam", "Sofia", "Noah", "Priya", "Ethan", "Zara",
    "Lucas", "Anya", "Mason", "Ishita", "Oliver", "Riya", "Elijah", "Diya",
    "James", "Kavya", "Benjamin", "Meera", "Daniel", "Aisha", "Henry", "Tara",
    "Alexander", "Nisha", "Owen", "Rohan", "Jack", "Sana",
]
LAST_NAMES = [
    "Sharma", "Patel", "Garcia", "Kim", "Nguyen", "Singh", "Chen", "Lopez",
    "Khan", "Reddy", "Johnson", "Brown", "Davis", "Miller", "Wilson", "Moore",
    "Taylor", "Anderson", "Thomas", "Jackson", "White", "Harris", "Martin",
    "Gupta", "Iyer", "Nair", "Das", "Kulkarni", "Menon", "Joshi",
]
CITIES = [
    "Dallas", "Plano", "Austin", "Houston", "Chicago", "Seattle", "Denver",
    "Atlanta", "Phoenix", "Miami", "Boston", "San Jose", "Irving", "Frisco",
]
STATES = ["TX", "TX", "TX", "TX", "IL", "WA", "CO", "GA", "AZ", "FL", "MA", "CA", "TX", "TX"]
TIERS = ["Gold", "Silver", "Bronze", "Platinum"]

PRODUCTS = [
    ("Wireless Headphones", "Electronics", "Audio", 129.99, 61.20),
    ("Smart Watch", "Electronics", "Wearables", 249.99, 118.40),
    ("Bluetooth Speaker", "Electronics", "Audio", 79.99, 36.10),
    ("Laptop Stand", "Electronics", "Accessories", 49.99, 19.75),
    ("USB-C Hub", "Electronics", "Accessories", 39.99, 14.20),
    ("Mechanical Keyboard", "Electronics", "Accessories", 119.99, 54.30),
    ("4K Monitor", "Electronics", "Displays", 349.99, 172.60),
    ("Noise Cancelling Earbuds", "Electronics", "Audio", 149.99, 70.15),
    ("Portable Charger", "Electronics", "Accessories", 29.99, 11.40),
    ("Webcam HD", "Electronics", "Accessories", 89.99, 38.90),
    ("Cotton T-Shirt", "Clothing", "Tops", 24.99, 8.30),
    ("Denim Jeans", "Clothing", "Bottoms", 59.99, 22.10),
    ("Winter Jacket", "Clothing", "Outerwear", 149.99, 58.40),
    ("Running Shoes", "Clothing", "Footwear", 109.99, 44.75),
    ("Hoodie", "Clothing", "Tops", 44.99, 16.20),
    ("Dress Shirt", "Clothing", "Tops", 39.99, 13.60),
    ("Yoga Pants", "Clothing", "Bottoms", 34.99, 12.10),
    ("Sneakers", "Clothing", "Footwear", 89.99, 35.80),
    ("Wool Sweater", "Clothing", "Tops", 69.99, 26.40),
    ("Baseball Cap", "Clothing", "Accessories", 19.99, 6.50),
    ("Coffee Maker", "Home & Kitchen", "Appliances", 89.99, 38.20),
    ("Air Fryer", "Home & Kitchen", "Appliances", 119.99, 52.60),
    ("Knife Set", "Home & Kitchen", "Kitchenware", 79.99, 30.15),
    ("Bedding Set", "Home & Kitchen", "Bedding", 99.99, 41.30),
    ("Vacuum Cleaner", "Home & Kitchen", "Appliances", 199.99, 94.80),
    ("Blender", "Home & Kitchen", "Appliances", 69.99, 28.40),
    ("Cookware Set", "Home & Kitchen", "Kitchenware", 159.99, 66.20),
    ("Desk Lamp", "Home & Kitchen", "Decor", 34.99, 13.10),
    ("Throw Pillows", "Home & Kitchen", "Decor", 29.99, 10.75),
    ("Bath Towels", "Home & Kitchen", "Bath", 39.99, 14.90),
    ("Yoga Mat", "Sports", "Fitness", 29.99, 10.20),
    ("Dumbbell Set", "Sports", "Fitness", 149.99, 68.40),
    ("Tennis Racket", "Sports", "Racquet Sports", 129.99, 55.30),
    ("Basketball", "Sports", "Team Sports", 34.99, 13.60),
    ("Camping Tent", "Sports", "Outdoor", 199.99, 92.10),
    ("Hiking Backpack", "Sports", "Outdoor", 119.99, 51.70),
    ("Resistance Bands", "Sports", "Fitness", 24.99, 8.15),
    ("Soccer Ball", "Sports", "Team Sports", 29.99, 11.30),
    ("Golf Clubs Set", "Sports", "Golf", 399.99, 188.50),
    ("Water Bottle", "Sports", "Accessories", 19.99, 6.80),
    ("Tablet", "Electronics", "Computers", 329.99, 158.20),
    ("E-Reader", "Electronics", "Computers", 139.99, 64.10),
    ("Gaming Mouse", "Electronics", "Accessories", 59.99, 24.30),
    ("Monitor Arm", "Electronics", "Accessories", 79.99, 31.60),
    ("Graphic T-Shirt", "Clothing", "Tops", 29.99, 9.80),
    ("Cargo Shorts", "Clothing", "Bottoms", 39.99, 14.20),
    ("Rain Jacket", "Clothing", "Outerwear", 119.99, 47.90),
    ("Trail Shoes", "Clothing", "Footwear", 129.99, 53.40),
    ("Slow Cooker", "Home & Kitchen", "Appliances", 59.99, 24.10),
    ("Cutting Board", "Home & Kitchen", "Kitchenware", 24.99, 8.90),
    ("Duvet Cover", "Home & Kitchen", "Bedding", 69.99, 27.30),
    ("Wall Clock", "Home & Kitchen", "Decor", 44.99, 17.20),
    ("Jump Rope", "Sports", "Fitness", 14.99, 4.60),
    ("Pickleball Set", "Sports", "Racquet Sports", 89.99, 36.80),
    ("Cooler", "Sports", "Outdoor", 99.99, 42.10),
    ("Tennis Balls", "Sports", "Racquet Sports", 12.99, 4.10),
    ("Smart Speaker", "Electronics", "Audio", 99.99, 45.30),
    ("Power Strip", "Electronics", "Accessories", 24.99, 9.40),
    ("Polo Shirt", "Clothing", "Tops", 34.99, 12.60),
    ("Chinos", "Clothing", "Bottoms", 49.99, 18.30),
]


def _messy_case(rng, value: str) -> str:
    style = rng.choice(["title", "upper", "lower", "title"], p=[0.55, 0.15, 0.10, 0.20])
    if style == "upper":
        return value.upper()
    if style == "lower":
        return value.lower()
    return value.title()


def _messy_date(rng, date: pd.Timestamp) -> str:
    fmt = rng.choice(["%m/%d/%Y", "%Y-%m-%d", "%d-%m-%Y"], p=[0.5, 0.3, 0.2])
    return date.strftime(fmt)


def _pad(s: str) -> str:
    # random leading/trailing whitespace to mimic source-system sloppiness
    r = random.random()
    if r < 0.08:
        return "  " + s
    if r < 0.15:
        return s + " "
    return s


def make_customers(rng: np.random.Generator, n: int, start_id: int = 1) -> pd.DataFrame:
    ids = [f"CUST-{i:04d}" for i in range(start_id, start_id + n)]
    first = rng.choice(FIRST_NAMES, n)
    last = rng.choice(LAST_NAMES, n)
    names = [_pad(_messy_case(rng, f"{f} {l}")) for f, l in zip(first, last)]

    emails = []
    for i, (f, l) in enumerate(zip(first, last)):
        r = rng.random()
        if r < 0.05:  # invalid email: missing @ or domain
            emails.append(rng.choice([
                f"{f.lower()}{l.lower()}{i}example.com",
                f"{f.lower()} {l.lower()}@{i}",
                f"{f.lower()}.{l.lower()}@",
            ]))
        elif r < 0.13:  # null email
            emails.append(None)
        else:
            emails.append(_pad(_messy_case(rng, f"{f.lower()}.{l.lower()}{i}@example.com")))

    city_idx = rng.integers(0, len(CITIES), n)
    cities = [
        None if rng.random() < 0.05 else _pad(_messy_case(rng, CITIES[j]))
        for j in city_idx
    ]
    states = [STATES[j] for j in city_idx]
    phones = [
        None if rng.random() < 0.10
        else rng.choice(["(214) 555-", "214-555-", "+1 214 555-"]) + f"{rng.integers(1000, 9999)}"
        for _ in range(n)
    ]

    signup = pd.to_datetime(rng.integers(
        pd.Timestamp("2022-01-01").value // 10**9,
        pd.Timestamp("2024-12-31").value // 10**9, n), unit="s")
    signup_str = [_messy_date(rng, d) for d in signup]
    for k in rng.choice(n, size=max(1, n // 100), replace=False):  # a few future dates
        signup_str[k] = _messy_date(rng, pd.Timestamp("2026-03-15") + pd.Timedelta(days=int(k)))

    tiers = [
        None if rng.random() < 0.05 else _pad(_messy_case(rng, rng.choice(TIERS)))
        for _ in range(n)
    ]

    df = pd.DataFrame({
        "customer_id": ids, "full_name": names, "email": emails, "phone": phones,
        "city": cities, "state": states, "signup_date": signup_str, "loyalty_tier": tiers,
    })
    # exact duplicates (~8%)
    dupes = df.sample(frac=0.08, random_state=SEED).copy()
    df = pd.concat([df, dupes], ignore_index=True)
    return df.sample(frac=1.0, random_state=SEED).reset_index(drop=True)


def make_products(rng: np.random.Generator) -> pd.DataFrame:
    n = len(PRODUCTS)
    ids = [f"PROD-{i:03d}" for i in range(1, n + 1)]
    rows = []
    for pid, (name, cat, sub, price, cost) in zip(ids, PRODUCTS):
        r = rng.random()
        cost_val = None if r < 0.07 else round(cost * rng.uniform(0.95, 1.05), 2)
        if r >= 0.07 and rng.random() < 0.05:  # invalid margin: cost above list price
            cost_val = round(price * rng.uniform(1.05, 1.30), 2)
        rows.append({
            "product_id": pid,
            "product_name": _pad(_messy_case(rng, name)),
            "category": _pad(_messy_case(rng, cat)),
            "subcategory": _pad(_messy_case(rng, sub)),
            "list_price": round(price * rng.uniform(0.97, 1.03), 2),
            "cost": cost_val,
        })
    df = pd.DataFrame(rows)
    dupes = df.sample(frac=0.05, random_state=SEED).copy()
    df = pd.concat([df, dupes], ignore_index=True)
    return df.sample(frac=1.0, random_state=SEED).reset_index(drop=True)


def make_orders(rng: np.random.Generator, n: int, start_id: int,
                customer_ids: list, product_ids: list,
                start_date: str, end_date: str, messy: bool = True) -> pd.DataFrame:
    ids = [f"ORD-{i:06d}" for i in range(start_id, start_id + n)]
    cust = rng.choice(customer_ids, n).tolist()
    if messy:  # ~3% orphan customers
        for k in rng.choice(n, size=max(1, n * 3 // 100), replace=False):
            cust[k] = f"CUST-{rng.integers(9000, 9999):04d}"
    prod = rng.choice(product_ids, n).tolist()
    if messy:  # ~2% orphan products
        for k in rng.choice(n, size=max(1, n * 2 // 100), replace=False):
            prod[k] = f"PROD-{rng.integers(900, 999):03d}"

    lo = pd.Timestamp(start_date).value // 10**9
    hi = pd.Timestamp(end_date).value // 10**9
    dates = pd.to_datetime(rng.integers(lo, hi, n), unit="s")
    date_str = [_messy_date(rng, d) for d in dates]

    qty = rng.integers(1, 6, n).astype(float)
    if messy:
        for k in rng.choice(n, size=max(1, n * 2 // 100), replace=False):
            qty[k] = np.nan
        for k in rng.choice(n, size=max(1, n // 100), replace=False):  # negative qty
            qty[k] = -rng.integers(1, 4)

    unit_price = np.round(rng.uniform(9.99, 399.99, n), 2)
    if messy:
        for k in rng.choice(n, size=max(1, n // 100), replace=False):
            unit_price[k] = np.nan

    disc = np.round(rng.uniform(0, 30, n), 1)
    if messy:
        for k in rng.choice(n, size=max(1, n * 3 // 100), replace=False):
            disc[k] = np.nan
        for k in rng.choice(n, size=max(1, n // 200), replace=False):  # invalid >100%
            disc[k] = round(rng.uniform(101, 150), 1)

    statuses = ["Pending", "Shipped", "Delivered", "Cancelled", "Returned"]
    status = [_pad(_messy_case(rng, rng.choice(statuses))) for _ in range(n)]

    df = pd.DataFrame({
        "order_id": ids, "customer_id": cust, "product_id": prod,
        "order_date": date_str, "quantity": qty, "unit_price": unit_price,
        "discount_pct": disc, "order_status": status,
    })
    if messy:
        dupes = df.sample(frac=0.04, random_state=SEED).copy()
        df = pd.concat([df, dupes], ignore_index=True)
    return df.sample(frac=1.0, random_state=SEED).reset_index(drop=True)


def main() -> None:
    random.seed(SEED)
    rng = np.random.default_rng(SEED)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    (RAW_DIR / "batch2").mkdir(parents=True, exist_ok=True)

    customers = make_customers(rng, 500, start_id=1)
    products = make_products(rng)
    orders = make_orders(
        rng, 2000, start_id=1,
        customer_ids=customers["customer_id"].unique().tolist(),
        product_ids=products["product_id"].unique().tolist(),
        start_date="2024-01-01", end_date="2025-06-30", messy=True,
    )
    customers.to_csv(RAW_DIR / "customers_raw.csv", index=False)
    products.to_csv(RAW_DIR / "products_raw.csv", index=False)
    orders.to_csv(RAW_DIR / "orders_raw.csv", index=False)

    # ---- batch 2: incremental load (SCD Type 2 demo) ----
    batch2_changed = customers.drop_duplicates("customer_id").sample(
        n=30, random_state=SEED + 1).copy()
    for idx in batch2_changed.index:  # customers move cities / change tiers
        batch2_changed.loc[idx, "city"] = _pad(_messy_case(rng, rng.choice(CITIES)))
        batch2_changed.loc[idx, "loyalty_tier"] = _pad(_messy_case(rng, rng.choice(TIERS)))
    batch2_new = make_customers(rng, 30, start_id=501)
    customers_b2 = pd.concat([batch2_changed, batch2_new], ignore_index=True)
    customers_b2.to_csv(RAW_DIR / "batch2" / "customers_batch2.csv", index=False)

    all_customers = pd.concat(
        [customers[["customer_id"]].drop_duplicates(), customers_b2[["customer_id"]].drop_duplicates()],
        ignore_index=True,
    )["customer_id"].unique().tolist()
    orders_b2 = make_orders(
        rng, 500, start_id=2001, customer_ids=all_customers,
        product_ids=products["product_id"].unique().tolist(),
        start_date="2025-07-01", end_date="2025-09-30", messy=True,
    )
    orders_b2.to_csv(RAW_DIR / "batch2" / "orders_batch2.csv", index=False)

    for p in ["customers_raw.csv", "orders_raw.csv", "products_raw.csv",
              "batch2/customers_batch2.csv", "batch2/orders_batch2.csv"]:
        rows = pd.read_csv(RAW_DIR / p).shape[0]
        print(f"wrote data/raw/{p}: {rows:,} rows")


if __name__ == "__main__":
    main()
