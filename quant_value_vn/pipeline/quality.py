"""
Compute quality factors for the Quantitative Value screener.

Functions:
- compute_quality_rank(df) → DataFrame with quality_rank and quality_score

Quality composite rank per metrics.md (Vietnam adapted):
  quality_rank = rank(ROA_5yr_avg, desc) + rank(ROC_5yr_avg, desc)
               + rank(FCF_assets_5yr_avg, desc) + rank(GM_stability, asc)

Note: metrics.md specifies 8-year averages, but Vietnam data history often
limited to 5 years. Using 5-year averages as practical adaptation.

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
    Rank stocks by quality factors (Vietnam adapted).

    Factors (per metrics.md):
    - ROA_5yr_avg: higher is better (rank descending)
    - ROC_5yr_avg: higher is better (rank descending)
    - FCF_assets_5yr_avg: higher is better (rank descending)
    - GM_stability: lower std dev is better (rank ascending)

    Note: Original book uses ROA, gross_profitability, CFO/TA, accruals.
    Vietnam adaptation uses 5-year averages for stability.
    Minimum 2 years of data required for ROA/ROC/FCF, 3 years for GM stability.

    Returns DataFrame with quality_rank and quality_score columns.
    """
    df = df.copy()
    n = len(df)
    if n == 0:
        return df

    # Ensure columns exist (Vietnam-adapted factors)
    for col in ("ROA_5yr_avg", "ROC_5yr_avg", "FCF_assets_5yr_avg", "GM_stability"):
        if col not in df.columns:
            df[col] = np.nan

    # Rank each factor (lower rank = better)
    def _rank(series, ascending=True):
        filled = series.fillna(series.median() if series.notna().any() else 0)
        return rankdata(filled if ascending else -filled, method="average")

    # Vietnam-adapted quality factors (5-year averages)
    # Note: GM_stability uses ascending=True (lower std dev = better)
    df["_roa_rank"] = _rank(df["ROA_5yr_avg"], ascending=False)
    df["_roc_rank"] = _rank(df["ROC_5yr_avg"], ascending=False)
    df["_fcf_rank"] = _rank(df["FCF_assets_5yr_avg"], ascending=False)
    df["_gm_rank"] = _rank(df["GM_stability"], ascending=True)  # Lower std = better

    # Composite quality rank (lower = better quality)
    df["quality_rank"] = (
        df["_roa_rank"] + df["_roc_rank"] + df["_fcf_rank"] + df["_gm_rank"]
    )

    # Normalise to 0-100 score (100 = best quality)
    qr_min, qr_max = df["quality_rank"].min(), df["quality_rank"].max()
    if qr_max > qr_min:
        df["quality_score"] = (
            (qr_max - df["quality_rank"]) / (qr_max - qr_min) * 100
        ).round(1)
    else:
        df["quality_score"] = 50.0

    # Drop temp columns
    df.drop(columns=["_roa_rank", "_roc_rank", "_fcf_rank", "_gm_rank"], inplace=True)

    # Log data quality
    quality_data_available = (
        df["ROA_5yr_avg"].notna().sum(),
        df["ROC_5yr_avg"].notna().sum(),
        df["FCF_assets_5yr_avg"].notna().sum(),
        df["GM_stability"].notna().sum(),
    )
    logger.info(
        "Quality ranking: %d stocks. Data availability: ROA=%d, ROC=%d, FCF=%d, GM=%d",
        n, *quality_data_available
    )
    return df
