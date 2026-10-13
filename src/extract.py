"""extract.py — read raw CSV batches into pandas DataFrames.

Extract is intentionally thin: it only reads files, validates they exist,
and reports row counts. All type coercion / cleaning lives in transform.py.
"""

from pathlib import Path
import pandas as pd

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

BATCH_FILES = {
    1: {
        "customers": RAW_DIR / "customers_raw.csv",
        "orders": RAW_DIR / "orders_raw.csv",
        "products": RAW_DIR / "products_raw.csv",
    },
    2: {
        "customers": RAW_DIR / "batch2" / "customers_batch2.csv",
        "orders": RAW_DIR / "batch2" / "orders_batch2.csv",
        "products": None,  # no new products in batch 2 (Type 1 dim already built)
    },
}


def extract_batch(batch_id: int) -> dict:
    """Read one raw batch. Returns {entity: DataFrame}; products is None for batch 2."""
    files = BATCH_FILES[batch_id]
    out = {}
    for entity, path in files.items():
        if path is None:
            out[entity] = None
            continue
        if not path.exists():
            raise FileNotFoundError(
                f"Raw file missing: {path}. Run `python src/generate_data.py` first."
            )
        df = pd.read_csv(path, dtype=str).replace({"": None})
        out[entity] = df
        print(f"[extract] batch {batch_id} {entity}: {len(df):,} raw rows from {path.name}")
    return out
