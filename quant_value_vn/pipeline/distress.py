"""
PFD Model (Probability of Financial Distress) — Vietnam Adapted.

Based on Campbell, Hilscher, and Szilagyi (2008) model from metrics.md.
Requires market data (price, volatility, returns) from external sources.

Formula:
LPFD = -20.26×NIMTAAVG + 1.42×TLMTA - 7.13×EXRETAVG + 1.41×SIGMA
       - 0.045×RSIZE - 2.13×CASHMTA + 0.075×MB - 0.058×PRICE - 9.16
PFD = 1 / (1 + exp(-LPFD))

Note: This model requires external market data (HOSE/HNX API) which is
NOT available in financial statements. Implementation provided for
future integration with market data feeds.
"""

import logging
from typing import Dict, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def calculate_pfd(
    book_equity: float,
    book_debt: float,
    market_cap: float,
    stock_price: float,
    volatility_3m: float,
    stock_return_12m: float,
    vnindex_return_12m: float,
    total_market_cap: float,
    cash: float = 0,
) -> Optional[float]:
    """
    Calculate PFD (Probability of Financial Distress) for a single stock.

    Args:
        book_equity: Shareholders' equity (VAS code 400)
        book_debt: Total debt (341 + 342 + 343)
        market_cap: Market capitalization (VND)
        stock_price: Current stock price (VND)
        volatility_3m: 3-month annualized volatility (std * sqrt(252))
        stock_return_12m: 12-month stock return
        vnindex_return_12m: 12-month VNI-Index return (market benchmark)
        total_market_cap: Total market cap of HOSE+HNX+UPCOM
        cash: Cash and equivalents (110)

    Returns:
        PFD probability (0-1), or None if insufficient data
    """
    try:
        # Market Value of Total Assets (approximation)
        mta = book_equity + book_debt
        if mta <= 0:
            return None

        # NIMTAAVG: Net Income / Market Value of Total Assets
        # Note: In practice, use trailing 12-month net income
        # For now, this is a placeholder — actual implementation needs NI_TTM
        nimta_avg = 0.05 / mta  # Placeholder: assume 5% ROA

        # TLMTA: Total Liabilities / Market Value of Total Assets
        tlmta = book_debt / mta

        # EXRETAVG: Excess Return (stock - market)
        exret_avg = stock_return_12m - vnindex_return_12m

        # SIGMA: Annualized volatility (3-month)
        sigma = volatility_3m

        # RSIZE: Relative size (log of market cap ratio)
        if market_cap <= 0 or total_market_cap <= 0:
            return None
        rsize = np.log(market_cap / total_market_cap)

        # CASHMTA: Cash / Market Value of Total Assets
        cashmta = cash / mta if mta > 0 else 0

        # MB: Market-to-Book ratio
        # Simplified: Market Cap / Book Equity
        if book_equity <= 0:
            return None
        mb = market_cap / book_equity

        # PRICE: Log of stock price (capped at ~$15 USD = 375,000 VND)
        price = np.log(min(stock_price, 375000))

        # LPFD formula (Campbell et al. 2008)
        lpfd = (
            -20.26 * nimta_avg
            + 1.42 * tlmta
            - 7.13 * exret_avg
            + 1.41 * sigma
            - 0.045 * rsize
            - 2.13 * cashmta
            + 0.075 * mb
            - 0.058 * price
            - 9.16
        )

        # PFD = logistic function
        pfd = 1.0 / (1.0 + np.exp(-lpfd))

        return round(pfd, 4)

    except (ZeroDivisionError, ValueError, TypeError) as e:
        logger.warning("PFD calculation failed: %s", e)
        return None


def compute_pfd_scores(
    df: pd.DataFrame,
    market_data: Dict[str, Dict],
    vnindex_return_12m: float = 0.10,
    total_market_cap: float = 5000000e9,  # ~5,000T VND for all exchanges
) -> pd.DataFrame:
    """
    Compute PFD scores for all stocks in DataFrame.

    Args:
        df: DataFrame with financial data (book_equity, book_debt, cash, market_cap)
        market_data: Dict mapping ticker → {price, volatility_3m, return_12m}
        vnindex_return_12m: VNI-Index 12-month return (default 10%)
        total_market_cap: Total market cap of Vietnam exchanges

    Returns:
        DataFrame with pfd_score and pfd_zone columns added
    """
    df = df.copy()
    pfd_scores = []

    for idx, row in df.iterrows():
        ticker = row.get("ticker")
        if ticker not in market_data:
            pfd_scores.append(np.nan)
            continue

        mkt = market_data[ticker]
        pfd = calculate_pfd(
            book_equity=row.get("equity", 0),
            book_debt=row.get("total_debt", 0),
            market_cap=row.get("market_cap", 0),
            stock_price=mkt.get("price", 0),
            volatility_3m=mkt.get("volatility_3m", 0.3),  # Default 30% vol
            stock_return_12m=mkt.get("return_12m", 0),
            vnindex_return_12m=vnindex_return_12m,
            total_market_cap=total_market_cap,
            cash=row.get("cash", 0),
        )
        pfd_scores.append(pfd)

    df["pfd_score"] = pfd_scores

    # Classify into zones
    # Per research: PFD > 0.05 = high distress risk (top 5%)
    df["pfd_zone"] = np.where(
        df["pfd_score"].isna(), "N/A",
        np.where(df["pfd_score"] > 0.05, "High Risk",
            np.where(df["pfd_score"] > 0.02, "Moderate", "Low")))

    n_high = (df["pfd_zone"] == "High Risk").sum()
    n_mod = (df["pfd_zone"] == "Moderate").sum()
    n_low = (df["pfd_zone"] == "Low").sum()
    n_na = df["pfd_score"].isna().sum()

    logger.info(
        "PFD scores: %d low, %d moderate, %d high risk, %d N/A",
        n_low, n_mod, n_high, n_na
    )

    return df


def remove_high_pfd(
    df: pd.DataFrame,
    threshold: float = 0.05,
) -> pd.DataFrame:
    """
    Remove stocks with high distress risk (PFD > threshold).

    Per metrics.md: Exclude top 5% risk (PFD > 0.05).

    Args:
        df: DataFrame with pfd_score column
        threshold: PFD cutoff (default 0.05 = 5% distress probability)

    Returns:
        Filtered DataFrame
    """
    before = len(df)
    # Keep stocks with PFD <= threshold OR missing data (conservative)
    mask = df["pfd_score"].isna() | (df["pfd_score"] <= threshold)
    df = df[mask].copy()
    after = len(df)
    removed = before - after

    if removed > 0:
        logger.info(
            "PFD filter (>.%.2f): %d → %d (removed %d high-risk stocks)",
            threshold, before, after, removed
        )

    return df
