"""
Ranking engine for Quantitative Value screener.

Functions:
- filter_universe(df) → DataFrame filtered for investable universe
- remove_sectors(df)  → DataFrame with excluded sectors removed
- rank_stocks(df)     → DataFrame with final combined ranking

Steps:
1. Filter: positive EBIT, positive EV, market cap >= threshold, D/E < 3
2. Value rank: rank by Acquirer's Multiple (ascending — lower = cheaper)
3. Re-rank quality within filtered set
4. Combined rank = value_rank + quality_rank
5. Sort by lowest combined_rank
"""

import logging

import numpy as np
import pandas as pd
from scipy.stats import rankdata

from quant_value_vn.config import (
    MIN_MARKET_CAP, MAX_ACQUIRERS_MULTIPLE, MAX_DEBT_EQUITY, EXCLUDED_SECTORS,
)

logger = logging.getLogger(__name__)


def remove_sectors(df: pd.DataFrame) -> pd.DataFrame:
    """Remove financials, banks, insurance — metrics unreliable for these."""
    if "sector" not in df.columns:
        return df
    initial = len(df)
    mask = ~df["sector"].fillna("").str.lower().str.strip().isin(EXCLUDED_SECTORS)
    df = df[mask].copy()
    removed = initial - len(df)
    if removed > 0:
        logger.info("Sector filter: removed %d stocks", removed)
    return df


def filter_universe(
    df: pd.DataFrame,
    min_mcap: float = MIN_MARKET_CAP,
    max_am: float = MAX_ACQUIRERS_MULTIPLE,
    max_de: float = MAX_DEBT_EQUITY,
) -> pd.DataFrame:
    """
    Filter for investable universe.

    Criteria:
    - EBIT > 0
    - Enterprise Value > 0
    - Acquirer's Multiple in [0, max_am]
    - Market cap >= min_mcap
    - Debt/Equity < max_de (or NaN)
    """
    initial = len(df)
    m = pd.Series(True, index=df.index)

    m &= df["ebit"] > 0
    m &= df["enterprise_value"] > 0
    if "acquirers_multiple" in df.columns:
        m &= df["acquirers_multiple"].between(0, max_am, inclusive="both")
    m &= df["market_cap"] >= min_mcap
    m &= (df["debt_equity"].fillna(0) < max_de) | df["debt_equity"].isna()

    df = df[m].copy()
    logger.info("Universe filter: %d → %d stocks", initial, len(df))
    return df


def rank_stocks(df: pd.DataFrame) -> pd.DataFrame:
    """
    Final ranking per the book.

    value_rank     = rank(acquirers_multiple, ascending — lower AM = rank 1)
    quality_rank   = re-ranked within filtered set
    combined_rank  = value_rank + quality_rank (lowest = best)
    """
    if df.empty:
        return df
    df = df.copy()

    # Value rank: lower AM = cheaper = rank 1
    df["value_rank"] = df["acquirers_multiple"].rank(ascending=True, method="average")

    # Re-rank quality within filtered set
    for col, asc in [
        ("roa", False),
        ("gross_profitability", False),
        ("cfo_to_assets", False),
        ("accruals", True),
    ]:
        if col in df.columns:
            vals = df[col].fillna(df[col].median() if df[col].notna().any() else 0)
            df[f"{col}_rank_f"] = rankdata(vals if asc else -vals, method="average")

    rank_cols = [c for c in df.columns if c.endswith("_rank_f")]
    if rank_cols:
        df["quality_rank"] = df[rank_cols].sum(axis=1)
        # Re-normalise quality_score in filtered set
        qr = df["quality_rank"]
        qr_min, qr_max = qr.min(), qr.max()
        if qr_max > qr_min:
            df["quality_score"] = (
                (qr_max - qr) / (qr_max - qr_min) * 100
            ).round(1)
        df.drop(columns=rank_cols, inplace=True)

    # Combined rank
    df["combined_score"] = df["value_rank"] + df["quality_rank"]
    df["combined_rank"] = df["combined_score"].rank(method="min").astype(int)

    df = df.sort_values("combined_rank").reset_index(drop=True)
    logger.info("Ranking done: %d stocks ranked", len(df))
    return df
