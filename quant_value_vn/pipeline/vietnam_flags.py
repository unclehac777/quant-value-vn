"""
Vietnam-specific validation flags for Quantitative Value screener.

Per metrics.md Part 5.3: Add checks to filter out "Value Traps" specific
to Vietnam market conditions.

Flags:
- HIGH_RELATED_PARTY_RISK: >30% revenue from related parties
- STATE_OWNED_ENTERPRISE: >50% state ownership
- SEASONAL_BUSINESS: Agriculture/Seafood/Sugar sectors
- KHCN_FUND_ACTIVE: Science & Technology fund active (distorts tax/equity)
- LOW_FREE_FLOAT: Free float <15% (liquidity risk)
- HIGH_FOREX_EXPOSURE: Significant forex gains/losses (volatility)
"""

import logging
from typing import Dict, List

import pandas as pd

logger = logging.getLogger(__name__)


# Vietnam-specific risky sectors (seasonal, state-dominated)
SEASONAL_SECTORS = {
    "sugar", "đường",  # Sugar (harvest Oct-May)
    "agriculture", "nông nghiệp",  # Agriculture
    "seafood", "thủy sản",  # Seafood
    "forestry", "lâm nghiệp",  # Forestry
}

STATE_OWNED_SECTORS = {
    "oil", "dầu khí",  # Oil & Gas
    "utilities", "tiện ích",  # Utilities
    "telecom", "viễn thông",  # Telecom
    "transportation", "giao thông",  # Transportation infrastructure
}


def validate_vietnam_flags(row: pd.Series) -> List[str]:
    """
    Check Vietnam-specific risk flags for a single stock.

    Args:
        row: DataFrame row with financial and market data

    Returns:
        List of flag strings (empty if no flags)
    """
    flags = []

    # Get sector (lowercase for comparison)
    sector = (row.get("sector") or "").lower().strip()

    # 1. Related Party Transactions (Giao dịch với các bên liên quan)
    # Per metrics.md: High exposure (>30% revenue) distorts earnings quality
    rpt_volume = row.get("related_party_revenue", 0) or 0
    revenue = row.get("revenue", 0) or 0
    if revenue > 0 and rpt_volume > revenue * 0.30:
        flags.append("HIGH_RELATED_PARTY_RISK")

    # 2. State Ownership (Sở hữu nhà nước)
    # SOEs may prioritize employment over shareholder value
    state_ownership = row.get("state_ownership_pct", 0) or 0
    if state_ownership > 0.50:
        flags.append("STATE_OWNED_ENTERPRISE")

    # 3. Seasonality (Tính thời vụ)
    # QNS finding: Sugar harvest is Oct-May, creates seasonal cash flow spikes
    if any(s in sector for s in SEASONAL_SECTORS):
        flags.append("SEASONAL_BUSINESS")

    # 4. Science & Technology Fund (Quỹ Phát triển KH&CN)
    # Per metrics.md: Can distort tax and equity calculations
    khcn_fund = row.get("khcn_fund", 0) or 0
    if khcn_fund > 0:
        flags.append("KHCN_FUND_ACTIVE")

    # 5. Low Free Float (Cổ phiếu tự do lưu hành thấp)
    # Liquidity risk: <15% free float = hard to exit position
    free_float = row.get("free_float_pct", 1.0) or 1.0
    if free_float < 0.15:
        flags.append("LOW_FREE_FLOAT")

    # 6. High Forex Exposure (Rủi ro tỷ giá)
    # Significant forex gains/losses indicate volatility
    forex_gain_loss = abs(row.get("forex_gain_loss", 0) or 0)
    if forex_gain_loss > revenue * 0.10:  # >10% of revenue
        flags.append("HIGH_FOREX_EXPOSURE")

    # 7. High Financial Leverage (beyond standard debt/equity)
    # Check if financial expense >20% of operating profit
    fin_expense = row.get("financial_expense", 0) or 0
    op_profit = row.get("operating_profit", 0) or 0
    if op_profit > 0 and fin_expense > op_profit * 0.20:
        flags.append("HIGH_FINANCIAL_LEVERAGE")

    # 8. Auditor Qualification (Kiểm toán ngoại trừ)
    # If auditor issued qualified opinion = red flag
    auditor_opinion = row.get("auditor_opinion", "unqualified") or "unqualified"
    if auditor_opinion.lower() not in ("unqualified", "clean"):
        flags.append("AUDITOR_QUALIFICATION")

    return flags


def add_vietnam_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add Vietnam-specific validation flags to DataFrame.

    Args:
        df: DataFrame with stock data

    Returns:
        DataFrame with 'vn_flags' column (list of flags per stock)
        and 'vn_flag_count' column (number of flags)
    """
    df = df.copy()

    flags_list = []
    for idx, row in df.iterrows():
        flags = validate_vietnam_flags(row)
        flags_list.append(flags)

    df["vn_flags"] = flags_list
    df["vn_flag_count"] = df["vn_flags"].apply(len)

    # Log summary
    all_flags = [f for flags in flags_list for f in flags]
    flag_counts = pd.Series(all_flags).value_counts() if all_flags else pd.Series()

    if not flag_counts.empty:
        logger.info("Vietnam flags detected:")
        for flag, count in flag_counts.items():
            logger.info("  %s: %d stocks", flag, count)
    else:
        logger.info("No Vietnam-specific flags detected")

    return df


def filter_by_vietnam_flags(
    df: pd.DataFrame,
    exclude_flags: List[str] = None,
    max_flags: int = 2,
) -> pd.DataFrame:
    """
    Filter stocks based on Vietnam-specific flags.

    Args:
        df: DataFrame with vn_flags and vn_flag_count columns
        exclude_flags: List of flags that trigger automatic exclusion
                       (e.g., ["AUDITOR_QUALIFICATION", "HIGH_RELATED_PARTY_RISK"])
        max_flags: Maximum number of flags allowed (default 2)

    Returns:
        Filtered DataFrame
    """
    if exclude_flags is None:
        exclude_flags = ["AUDITOR_QUALIFICATION"]

    df = df.copy()
    before = len(df)

    # Exclude stocks with specific critical flags
    def has_critical_flag(flags):
        return any(f in exclude_flags for f in flags)

    mask = ~df["vn_flags"].apply(has_critical_flag)
    mask &= df["vn_flag_count"] <= max_flags

    df = df[mask].copy()
    after = len(df)
    removed = before - after

    if removed > 0:
        logger.info(
            "Vietnam flags filter: %d → %d (removed %d stocks)",
            before, after, removed
        )

    return df
