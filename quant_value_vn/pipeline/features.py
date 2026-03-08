"""
Compute financial features from raw scraped data.

Functions:
- compute_features(df) → DataFrame with derived financial metrics

Computed features:
- shares_outstanding
- market_cap
- ebit
- total_debt, total_cash
- enterprise_value
- roa, roe, roic
- gross_profitability  (GP / Total Assets — strong return predictor)
- accruals             ((NI - CFO) / TA — earnings quality)
- cfo_to_assets        (Operating CF / TA)
- fcf_yield
- gross_margin, net_margin
- pe, pb, debt_equity
"""

import logging

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate all derived financial metrics from raw data.

    Uses vectorized Pandas operations for speed.
    Returns DataFrame with new feature columns added.
    """
    df = df.copy()

    ni = df["net_income_parent"].fillna(df.get("net_income", pd.Series(dtype=float)))
    eps = df.get("eps", pd.Series(dtype=float))
    ta = df["total_assets"]
    eq = df.get("equity", pd.Series(dtype=float))
    rev = df.get("revenue", pd.Series(dtype=float))
    gp = df.get("gross_profit", pd.Series(dtype=float))
    ocf = df.get("operating_cash_flow", pd.Series(dtype=float))

    # ── Shares outstanding ───────────────────────────────────────
    df["shares"] = np.where(
        (eps > 0) & (ni > 0),
        ni / eps,
        np.nan,
    )

    # ── Market capitalisation ────────────────────────────────────
    price = df.get("price", pd.Series(dtype=float))
    df["market_cap"] = np.where(
        df["shares"].notna() & price.notna(),
        price * df["shares"],
        np.nan,
    )

    # ── EBIT ─────────────────────────────────────────────────────
    op = df.get("operating_profit", pd.Series(dtype=float))
    ie = df.get("interest_expense", pd.Series(dtype=float)).fillna(0)
    ie = ie.clip(lower=0)  # interest_expense should be positive addback
    df["ebit"] = np.where(op.notna(), op + ie, np.nan)

    # ── Debt & Cash aggregates ───────────────────────────────────
    df["total_debt"] = (
        df.get("short_term_debt", pd.Series(0.0, index=df.index)).fillna(0)
        + df.get("long_term_debt", pd.Series(0.0, index=df.index)).fillna(0)
    )
    df["total_cash"] = (
        df.get("cash", pd.Series(0.0, index=df.index)).fillna(0)
        + df.get("short_term_investments", pd.Series(0.0, index=df.index)).fillna(0)
    )

    # ── Enterprise Value ─────────────────────────────────────────
    mc = df["market_cap"]
    df["enterprise_value"] = np.where(
        mc > 0,
        mc + df["total_debt"] - df["total_cash"],
        np.nan,
    )

    # ── Profitability ────────────────────────────────────────────
    df["roa"] = np.where(ta > 0, ni / ta, np.nan)
    df["roe"] = np.where(eq > 0, ni / eq, np.nan)

    # Gross profitability (GP / Total Assets) — Novy-Marx quality signal
    df["gross_profitability"] = np.where(
        (ta > 0) & gp.notna(), gp / ta, np.nan
    )

    # ROIC = NOPAT / Invested Capital
    ebit = df["ebit"]
    pretax = df.get("pretax_profit", pd.Series(dtype=float))
    tax_rate = np.where(
        (pretax > 0) & ni.notna(),
        1 - ni / pretax,
        0.20,
    )
    invested_capital = eq.fillna(0) + df["total_debt"] - df["total_cash"]
    df["roic"] = np.where(
        (ebit > 0) & (invested_capital > 0),
        ebit * (1 - tax_rate) / invested_capital,
        np.nan,
    )

    # ── Earnings quality ─────────────────────────────────────────
    df["cfo_to_assets"] = np.where(
        (ta > 0) & ocf.notna(), ocf / ta, np.nan
    )
    df["accruals"] = np.where(
        (ta > 0) & ni.notna() & ocf.notna(),
        (ni - ocf) / ta,
        np.nan,
    )

    # ── FCF yield ────────────────────────────────────────────────
    df["fcf_yield"] = np.where(
        (ebit > 0) & (mc > 0),
        ebit * (1 - tax_rate) / mc,
        np.nan,
    )

    # ── Margins ──────────────────────────────────────────────────
    df["gross_margin"] = np.where(
        (rev > 0) & gp.notna(), gp / rev, np.nan
    )
    df["net_margin"] = np.where(
        (rev > 0) & ni.notna(), ni / rev, np.nan
    )

    # ── Valuation multiples ──────────────────────────────────────
    df["debt_equity"] = np.where(eq > 0, df["total_debt"] / eq, np.nan)
    df["pe"] = np.where((mc > 0) & (ni > 0), mc / ni, np.nan)
    df["pb"] = np.where((mc > 0) & (eq > 0), mc / eq, np.nan)

    # ── Working capital for Altman Z-Score ───────────────────────
    ca = df.get("current_assets", pd.Series(dtype=float))
    cl = df.get("current_liabilities", pd.Series(dtype=float))
    df["working_capital"] = np.where(
        ca.notna() & cl.notna(), ca - cl, np.nan
    )

    logger.info("Computed features for %d stocks", len(df))
    return df
