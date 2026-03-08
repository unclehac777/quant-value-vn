"""
Data cleaning and validation.

Functions:
- clean_data(df) → cleaned DataFrame with validated columns and types
- validate_row(d) → True if a row has minimum required fields

Ensures all downstream modules receive consistent data.
"""

import logging
from typing import Dict

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


REQUIRED_FIELDS = {"ticker", "revenue", "total_assets", "equity"}

NUMERIC_COLS = [
    "revenue", "gross_profit", "operating_profit", "net_income",
    "net_income_parent", "eps", "interest_expense", "cogs",
    "selling_expense", "admin_expense", "sga", "pretax_profit",
    "tax_expense", "financial_income", "financial_expense",
    "cash", "short_term_investments", "total_assets",
    "short_term_debt", "long_term_debt", "total_liabilities", "equity",
    "receivables", "inventory", "ppe", "current_assets",
    "current_liabilities", "depreciation_accumulated",
    "operating_cash_flow", "capex", "depreciation", "price",
]


def validate_row(d: Dict) -> bool:
    """Check if a data dict has the minimum required fields."""
    for f in REQUIRED_FIELDS:
        if not d.get(f):
            return False
    return True


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean raw ingested data.

    Steps:
    1. Drop rows missing critical fields (ticker, revenue, total_assets)
    2. Coerce numeric columns to float
    3. Remove duplicate tickers (keep first)
    4. Reset index

    Returns cleaned DataFrame.
    """
    initial = len(df)

    # Ensure numeric columns are float
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows without minimum requirements
    df = df.dropna(subset=["ticker", "total_assets"])
    df = df[df["total_assets"] > 0]

    # Remove duplicate tickers
    df = df.drop_duplicates(subset="ticker", keep="first")
    df = df.reset_index(drop=True)

    removed = initial - len(df)
    if removed > 0:
        logger.info("Cleaning: removed %d rows (%d → %d)", removed, initial, len(df))

    return df
