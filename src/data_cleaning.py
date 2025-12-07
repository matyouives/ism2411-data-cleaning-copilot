#!/usr/bin/env python3
"""
Simple data cleaning script for sales data.

Loads `data/raw/sales_data_raw.csv`, applies basic cleaning steps (standardize
column names, strip whitespace, handle missing prices/quantities, remove
invalid rows), and writes the cleaned output to
`data/processed/sales_data_clean.csv`.
"""

from pathlib import Path
import re
import pandas as pd
import numpy as np


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize column names to lowercase with underscores.

    Why: Makes subsequent column lookups predictable and consistent.
    """
    new_cols = []
    for col in df.columns:
        c = col.strip().lower()
        c = re.sub(r"\s+", "_", c)
        c = re.sub(r"[^0-9a-zA-Z_]+", "", c)
        new_cols.append(c)
    df.columns = new_cols
    return df


def find_column(df: pd.DataFrame, candidates):
    """Return the first matching column name from candidates present in df."""
    for cand in candidates:
        if cand in df.columns:
            return cand
    return None


# Copilot-assisted function: load_data
# What: Load a CSV file into a pandas DataFrame given a file path.
# Why: Encapsulates file loading so it can be reused and tested separately.
def load_data(file_path: str) -> pd.DataFrame:
    p = Path(file_path)
    return pd.read_csv(p)


# Copilot-assisted function: clean_column_names
# What: Normalize DataFrame column names to lowercase with underscores.
# Why: Provides a small wrapper so column-name cleaning is separated from other logic.
def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    return standardize_column_names(df)


# Copilot-assisted function: handle_missing_values
# What: Consistently handle missing/invalid price and quantity values in the DataFrame.
# Why: Centralizes missing-value rules (median fill for price, zero for quantity)
# so other code can rely on predictable numeric values.
def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        median_price = df["price"].replace([np.inf, -np.inf], np.nan).median(skipna=True)
        if pd.isna(median_price):
            median_price = 0.0
        df["price"] = df["price"].fillna(median_price)

    if "quantity" in df.columns:
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
        df["quantity"] = df["quantity"].fillna(0)

    return df


# Copilot-assisted function: remove_invalid_rows
# What: Drop rows that contain clearly invalid numeric values (negative price/quantity).
# Why: Negative values are assumed to be data-entry errors and should not be used
# in downstream aggregations.
def remove_invalid_rows(df: pd.DataFrame) -> pd.DataFrame:
    if "price" in df.columns:
        before = len(df)
        df = df[df["price"] >= 0]
        removed = before - len(df)
        if removed:
            print(f"Removed {removed} rows with negative price")

    if "quantity" in df.columns:
        before = len(df)
        df = df[df["quantity"] >= 0]
        removed = before - len(df)
        if removed:
            print(f"Removed {removed} rows with negative quantity")

    return df


def clean_sales_data(src_path: Path, out_path: Path) -> pd.DataFrame:
    # Load data
    # What: Read the raw CSV from disk
    # Why: Start with the original dataset to perform deterministic cleaning
    df = load_data(str(src_path))

    # Standardize column names
    # What: Normalize column names to all-lowercase and underscores
    # Why: Prevent subtle bugs due to capitalization/spaces in headers
    df = clean_column_names(df)

    # Detect key columns (be tolerant of slightly different header names)
    product_col = find_column(df, ["product", "product_name", "name"])
    category_col = find_column(df, ["category", "cat", "product_category"])
    price_col = find_column(df, ["price", "unit_price", "price_usd", "price_$"]) 
    quantity_col = find_column(df, ["quantity", "qty", "amount", "units"]) 

    # If some expected columns are missing, warn but continue.
    missing = []
    for key, col in ("product", product_col), ("category", category_col), ("price", price_col), ("quantity", quantity_col):
        if col is None:
            missing.append(key)
    if missing:
        print(f"Warning: could not find expected columns: {missing}")

    # Rename the detected columns to a standard set so downstream code is predictable
    rename_map = {}
    if product_col:
        rename_map[product_col] = "product"
    if category_col:
        rename_map[category_col] = "category"
    if price_col:
        rename_map[price_col] = "price"
    if quantity_col:
        rename_map[quantity_col] = "quantity"
    if rename_map:
        df = df.rename(columns=rename_map)

    # Trim whitespace from product and category fields
    # What: Strip leading/trailing whitespace and collapse repeated spaces
    # Why: Inconsistent spacing causes grouping and join problems
    for col in ("product", "category"):
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()

    # Handle missing values and remove invalid rows using helper functions
    df = handle_missing_values(df)
    df = remove_invalid_rows(df)

    # Final: reset index for cleanliness
    df = df.reset_index(drop=True)

    # Save cleaned data
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)

    # Print a short summary
    print(f"Cleaned data written to {out_path} — {len(df)} rows")
    return df


def main():
    repo_root = Path(__file__).resolve().parents[1]
    src_path = repo_root / "data" / "raw" / "sales_data_raw.csv"
    out_path = repo_root / "data" / "processed" / "sales_data_clean.csv"

    if not src_path.exists():
        raise FileNotFoundError(f"Raw data file not found: {src_path}")

    clean_sales_data(src_path, out_path)


if __name__ == "__main__":
    main()
