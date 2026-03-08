"""
Compute quality factors for the Quantitative Value screener.

Functions:
- compute_quality_rank(df) → DataFrame with quality_rank and quality_score

Quality composite rank per the book:
  quality_rank = rank(ROA, desc) + rank(gross_profitability, desc)
               + rank(cfo_to_assets, desc) + rank(accruals, asc)

Lower quality_rank = higher quality.
quality_score is normalised 0-100 (100 = best).
"""

import logging

import numpy as np
import pandas as pd
from scipy.stats import rankdata

logger = logging.getLogger(__name__)


def compute_quality_rank(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rank stocks by quality factors.

    Factors (per the book):
    - ROA: higher is better (rank descending)
    - Gross profitability: higher is better (rank descending)
    - CFO/TA: higher is better (rank descending)
    - Accruals: lower is better (rank ascending)

    Returns DataFrame with quality_rank and quality_score columns.
    """
    df = df.copy()
    n = len(df)
    if n == 0:
        return df

    # Ensure columns exist
    for col in ("roa", "gross_profitability", "cfo_to_assets", "accruals"):
        if col not in df.columns:
            df[col] = np.nan

    # Rank each factor (lower rank = better)
    def _rank(series, ascending=True):
        filled = series.fillna(series.median() if series.notna().any() else 0)
        return rankdata(filled if ascending else -filled, method="average")

    df["roa_rank"] = _rank(df["roa"], ascending=False)
    df["gp_rank"] = _rank(df["gross_profitability"], ascending=False)
    df["cfo_rank"] = _rank(df["cfo_to_assets"], ascending=False)
    df["accruals_rank"] = _rank(df["accruals"], ascending=True)

    # Composite quality rank (lower = better quality)
    df["quality_rank"] = (
        df["roa_rank"] + df["gp_rank"] + df["cfo_rank"] + df["accruals_rank"]
    )

    # Normalise to 0-100 score (100 = best quality)
    qr_min, qr_max = df["quality_rank"].min(), df["quality_rank"].max()
    if qr_max > qr_min:
        df["quality_score"] = (
            (qr_max - df["quality_rank"]) / (qr_max - qr_min) * 100
        ).round(1)
    else:
        df["quality_score"] = 50.0

    logger.info("Quality ranking done for %d stocks", n)
    return df
