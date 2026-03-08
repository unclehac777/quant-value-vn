"""
Price Momentum for Quantitative Value screener (Chapter 11).

Implements the 2-12 month momentum signal from Carlisle's framework:
- Calculate 12-month total return
- Skip the most recent month (avoid short-term mean reversion)
- Use months 2-12 return as the momentum signal

The momentum overlay improves returns by:
- Avoiding "falling knives" (cheap stocks still declining)
- Timing entry after the worst of the decline has passed

Integration:
- Compute momentum after value+quality ranking
- Use as a secondary filter or tiebreaker
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def compute_momentum_from_prices(
    prices: List[Dict],
) -> Optional[Dict]:
    """
    Calculate momentum metrics from a list of daily price records.

    Args:
        prices: List of dicts with 'date' and 'close' keys,
                ordered most recent first.

    Returns dict with:
    - mom_2_12: 2-12 month return (skip month 1, use months 2-12)
    - mom_12: full 12-month return
    - mom_1: most recent 1-month return (for reference)
    - price_now: current price
    - price_1m: price 1 month ago
    - price_12m: price 12 months ago
    """
    if not prices or len(prices) < 20:  # Need at least ~1 month of data
        return None

    # Approximate trading days: ~21/month, ~252/year
    DAYS_1M = 21
    DAYS_12M = 252

    n = len(prices)

    price_now = prices[0].get("close")
    if not price_now or price_now <= 0:
        return None

    # Price ~1 month ago (skip recent month for 2-12 signal)
    idx_1m = min(DAYS_1M, n - 1)
    price_1m = prices[idx_1m].get("close")

    # Price ~12 months ago
    idx_12m = min(DAYS_12M, n - 1)
    price_12m = prices[idx_12m].get("close")

    if not price_1m or price_1m <= 0:
        return None

    result = {
        "price_now": price_now,
        "price_1m": price_1m,
    }

    # 1-month return (most recent month — excluded from signal)
    result["mom_1"] = (price_now / price_1m) - 1

    if price_12m and price_12m > 0:
        result["price_12m"] = price_12m
        # Full 12-month return
        result["mom_12"] = (price_now / price_12m) - 1
        # 2-12 month return (THE signal): price 1m ago vs price 12m ago
        result["mom_2_12"] = (price_1m / price_12m) - 1
    else:
        result["mom_12"] = np.nan
        result["mom_2_12"] = np.nan

    return result


def compute_momentum_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add momentum columns to the DataFrame.

    Expects mom_2_12, mom_12, mom_1 columns (populated during ingestion).
    Adds:
    - momentum_rank: rank by mom_2_12 (higher return = lower rank = better)
    - momentum_zone: 'Positive' or 'Negative'
    """
    df = df.copy()

    if "mom_2_12" not in df.columns:
        logger.warning("No momentum data (mom_2_12). Skipping momentum ranking.")
        df["mom_2_12"] = np.nan
        df["momentum_rank"] = np.nan
        df["momentum_zone"] = "N/A"
        return df

    # Rank by 2-12 month momentum (higher = better → rank descending)
    valid = df["mom_2_12"].notna()
    if valid.sum() > 0:
        df.loc[valid, "momentum_rank"] = df.loc[valid, "mom_2_12"].rank(
            ascending=False, method="average"
        )
    else:
        df["momentum_rank"] = np.nan

    # Classify
    df["momentum_zone"] = np.where(
        df["mom_2_12"].isna(), "N/A",
        np.where(df["mom_2_12"] > 0, "Positive", "Negative")
    )

    n_pos = (df["momentum_zone"] == "Positive").sum()
    n_neg = (df["momentum_zone"] == "Negative").sum()
    n_na = (df["momentum_zone"] == "N/A").sum()

    logger.info(
        "Momentum: %d positive, %d negative, %d N/A",
        n_pos, n_neg, n_na,
    )

    return df


def remove_negative_momentum(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove stocks with negative 2-12 month momentum (falling knives).

    Per Carlisle Ch 11: momentum overlay avoids declining stocks
    even if they look cheap on value metrics.
    
    Stocks with missing momentum data are kept (NaN = no signal).
    """
    before = len(df)
    mask = df["mom_2_12"].isna() | (df["mom_2_12"] >= 0)
    df = df[mask].copy()
    after = len(df)
    removed = before - after
    if removed > 0:
        logger.info(
            "Momentum filter: %d → %d (removed %d falling knives)",
            before, after, removed,
        )
    return df
