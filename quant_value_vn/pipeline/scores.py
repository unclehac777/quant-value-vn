"""
Financial health and quality scores for risk assessment.

Implements:
- Altman Z-Score: Bankruptcy prediction (Edward Altman, 1968)
- Piotroski F-Score: Financial strength (Joseph Piotroski, 2000)

Both are used as safety checks in the Quantitative Value framework.
"""

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════
# ALTMAN Z-SCORE
# ══════════════════════════════════════════════════════════════════════
#
# Formula for manufacturing firms (original 1968 model):
#   Z = 1.2*X1 + 1.4*X2 + 3.3*X3 + 0.6*X4 + 1.0*X5
#
# Where:
#   X1 = Working Capital / Total Assets
#   X2 = Retained Earnings / Total Assets
#   X3 = EBIT / Total Assets
#   X4 = Market Value of Equity / Total Liabilities
#   X5 = Sales / Total Assets
#
# Interpretation:
#   Z > 2.99  → Safe zone (low bankruptcy risk)
#   1.81 < Z < 2.99 → Grey zone (moderate risk)
#   Z < 1.81  → Distress zone (high bankruptcy risk)
#
# Note: For non-manufacturing and private firms, use Z' or Z'' variants.
# We use the original for public companies with the modified threshold.


def compute_altman_zscore(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate Altman Z-Score for each stock.

    Required columns:
    - working_capital (current_assets - current_liabilities)
    - retained_earnings
    - ebit
    - market_cap
    - total_liabilities
    - revenue
    - total_assets

    Returns DataFrame with:
    - altman_zscore: raw Z-Score value
    - altman_zone: 'Safe', 'Grey', or 'Distress'
    """
    df = df.copy()

    ta = df["total_assets"]
    wc = df.get("working_capital", pd.Series(dtype=float))
    re = df.get("retained_earnings", pd.Series(dtype=float))
    ebit = df.get("ebit", pd.Series(dtype=float))
    mc = df.get("market_cap", pd.Series(dtype=float))
    tl = df.get("total_liabilities", pd.Series(dtype=float))
    rev = df.get("revenue", pd.Series(dtype=float))

    # Calculate Z-Score components
    x1 = np.where(ta > 0, wc / ta, np.nan)           # Working Capital / TA
    x2 = np.where(ta > 0, re / ta, np.nan)           # Retained Earnings / TA
    x3 = np.where(ta > 0, ebit / ta, np.nan)         # EBIT / TA
    x4 = np.where(tl > 0, mc / tl, np.nan)           # Market Cap / Total Liab
    x5 = np.where(ta > 0, rev / ta, np.nan)          # Revenue / TA (asset turnover)

    # Original Altman Z-Score
    z = 1.2 * x1 + 1.4 * x2 + 3.3 * x3 + 0.6 * x4 + 1.0 * x5
    df["altman_zscore"] = np.round(z, 2)

    # Classify into zones
    df["altman_zone"] = np.where(
        df["altman_zscore"] > 2.99, "Safe",
        np.where(df["altman_zscore"] >= 1.81, "Grey", "Distress")
    )

    # Count results
    n_safe = (df["altman_zone"] == "Safe").sum()
    n_grey = (df["altman_zone"] == "Grey").sum()
    n_distress = (df["altman_zone"] == "Distress").sum()
    n_na = df["altman_zscore"].isna().sum()

    logger.info(
        "Altman Z-Score: %d safe, %d grey, %d distress, %d N/A",
        n_safe, n_grey, n_distress, n_na
    )

    return df


def remove_distressed(
    df: pd.DataFrame, min_zscore: float = 1.5
) -> pd.DataFrame:
    """
    Remove stocks with high bankruptcy risk (Z-Score < threshold).
    
    Default threshold 1.81 = below grey zone (distress zone).
    Set to 2.99 to only keep "safe" stocks.
    """
    before = len(df)
    mask = df["altman_zscore"].isna() | (df["altman_zscore"] >= min_zscore)
    df = df[mask].copy()
    after = len(df)
    logger.info("Distress filter: %d → %d (removed %d)", before, after, before - after)
    return df


# ══════════════════════════════════════════════════════════════════════
# PIOTROSKI F-SCORE
# ══════════════════════════════════════════════════════════════════════
#
# 9-point scoring system for financial strength:
#
# PROFITABILITY (4 points):
#   1. ROA > 0 (positive net income)
#   2. CFO > 0 (positive operating cash flow)
#   3. ∆ROA > 0 (ROA improved vs prior year)
#   4. CFO > Net Income (accruals quality)
#
# LEVERAGE & LIQUIDITY (3 points):
#   5. ∆Long-term Debt < 0 (decreased leverage)
#   6. ∆Current Ratio > 0 (improved liquidity)
#   7. No new share issuance (dilution)
#
# OPERATING EFFICIENCY (2 points):
#   8. ∆Gross Margin > 0 (improved margin)
#   9. ∆Asset Turnover > 0 (improved efficiency)
#
# Interpretation:
#   F-Score 8-9 → Strong (buy candidates)
#   F-Score 5-7 → Average
#   F-Score 0-4 → Weak (avoid)


def compute_piotroski_fscore(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate Piotroski F-Score (9-point financial strength score).

    Uses _prev suffix columns for prior year comparisons.
    
    Required columns:
    - net_income (or net_income_parent)
    - operating_cash_flow
    - total_assets, total_assets_prev
    - long_term_debt, long_term_debt_prev
    - current_assets, current_liabilities, *_prev variants
    - shares (or shares_prev for dilution check)
    - revenue, revenue_prev
    - gross_profit, gross_profit_prev

    Returns DataFrame with:
    - piotroski_fscore: 0-9 integer score
    - piotroski_zone: 'Strong' (8-9), 'Average' (5-7), 'Weak' (0-4)
    """
    df = df.copy()
    n = len(df)

    # Helper to safely get columns
    def _col(name: str) -> pd.Series:
        return df.get(name, pd.Series(np.nan, index=df.index))

    # Current year values
    ni = _col("net_income_parent").fillna(_col("net_income"))
    ocf = _col("operating_cash_flow")
    ta = df["total_assets"]
    ta_prev = _col("total_assets_prev")
    lt_debt = _col("long_term_debt")
    lt_debt_prev = _col("long_term_debt_prev")
    ca = _col("current_assets")
    cl = _col("current_liabilities")
    ca_prev = _col("current_assets_prev")
    cl_prev = _col("current_liabilities_prev")
    shares = _col("shares")
    shares_prev = _col("shares_prev")  # May not exist
    rev = _col("revenue")
    rev_prev = _col("revenue_prev")
    gp = _col("gross_profit")
    gp_prev = _col("gross_profit_prev")

    # ── PROFITABILITY (4 points) ─────────────────────────────────
    # 1. ROA > 0
    roa = ni / ta.replace(0, np.nan)
    f1 = (roa > 0).astype(int)

    # 2. CFO > 0
    f2 = (ocf > 0).astype(int)

    # 3. ∆ROA > 0 (ROA improved)
    roa_prev = _col("net_income_parent_prev").fillna(_col("net_income_prev")) / ta_prev.replace(0, np.nan)
    f3 = (roa > roa_prev).astype(int)

    # 4. CFO > Net Income (quality of earnings)
    f4 = (ocf > ni).astype(int)

    # ── LEVERAGE & LIQUIDITY (3 points) ──────────────────────────
    # 5. ∆Long-term Debt < 0 (reduced leverage)
    # If debt decreased or stayed same = good
    f5 = (lt_debt <= lt_debt_prev).astype(int)

    # 6. ∆Current Ratio > 0 (improved liquidity)
    cr = ca / cl.replace(0, np.nan)
    cr_prev = ca_prev / cl_prev.replace(0, np.nan)
    f6 = (cr > cr_prev).astype(int)

    # 7. No new shares issued (no dilution)
    # If shares stayed same or decreased = good (1 point)
    # If we don't have prior shares, assume no dilution
    f7 = np.where(
        shares_prev.notna() & (shares_prev > 0),
        (shares <= shares_prev).astype(int),
        1  # Default to 1 if no prior data
    )

    # ── OPERATING EFFICIENCY (2 points) ──────────────────────────
    # 8. ∆Gross Margin > 0
    gm = gp / rev.replace(0, np.nan)
    gm_prev = gp_prev / rev_prev.replace(0, np.nan)
    f8 = (gm > gm_prev).astype(int)

    # 9. ∆Asset Turnover > 0
    at = rev / ta.replace(0, np.nan)
    at_prev = rev_prev / ta_prev.replace(0, np.nan)
    f9 = (at > at_prev).astype(int)

    # Sum all 9 factors (NaN comparisons yield 0)
    df["piotroski_fscore"] = (
        f1.fillna(0) + f2.fillna(0) + f3.fillna(0) + f4.fillna(0) +
        f5.fillna(0) + f6.fillna(0) + f7 + f8.fillna(0) + f9.fillna(0)
    ).astype(int)

    # Store individual components for debugging
    df["f_roa_positive"] = f1
    df["f_cfo_positive"] = f2
    df["f_roa_improved"] = f3
    df["f_accrual_quality"] = f4
    df["f_leverage_improved"] = f5
    df["f_liquidity_improved"] = f6
    df["f_no_dilution"] = f7
    df["f_margin_improved"] = f8
    df["f_turnover_improved"] = f9

    # Classify into zones
    df["piotroski_zone"] = np.where(
        df["piotroski_fscore"] >= 8, "Strong",
        np.where(df["piotroski_fscore"] >= 5, "Average", "Weak")
    )

    n_strong = (df["piotroski_zone"] == "Strong").sum()
    n_avg = (df["piotroski_zone"] == "Average").sum()
    n_weak = (df["piotroski_zone"] == "Weak").sum()

    logger.info(
        "Piotroski F-Score: %d strong, %d average, %d weak",
        n_strong, n_avg, n_weak
    )

    return df


def remove_weak_fscore(
    df: pd.DataFrame, min_fscore: int = 5
) -> pd.DataFrame:
    """
    Remove stocks with weak financial strength (F-Score < threshold).
    
    Default threshold 5 = average or above.
    Set to 8 to only keep "strong" stocks.
    """
    before = len(df)
    df = df[df["piotroski_fscore"] >= min_fscore].copy()
    after = len(df)
    logger.info("F-Score filter: %d → %d (removed %d)", before, after, before - after)
    return df


# ══════════════════════════════════════════════════════════════════════
# COMBINED SAFETY CHECK
# ══════════════════════════════════════════════════════════════════════

def compute_safety_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute all safety/quality scores: Altman Z-Score + Piotroski F-Score.
    
    Returns DataFrame with all score columns added.
    """
    df = compute_altman_zscore(df)
    df = compute_piotroski_fscore(df)
    return df


def apply_safety_filters(
    df: pd.DataFrame,
    min_zscore: float = 1.5,
    min_fscore: int = 5,
    strict: bool = False,
) -> pd.DataFrame:
    """
    Apply safety filters using Z-Score and F-Score.
    
    Args:
        min_zscore: Minimum Altman Z-Score (1.81 = above distress)
        min_fscore: Minimum F-Score (5 = average or better)
        strict: If True, use stricter thresholds (2.99 Z-Score, 8 F-Score)
    
    Returns filtered DataFrame.
    """
    if strict:
        min_zscore = 2.99
        min_fscore = 8

    df = remove_distressed(df, min_zscore=min_zscore)
    df = remove_weak_fscore(df, min_fscore=min_fscore)
    return df
