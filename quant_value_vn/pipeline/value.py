"""
Compute value factors for the Quantitative Value screener.

Functions:
- compute_acquirers_multiple(df) → DataFrame with acquirers_multiple and ebit_ev

Acquirer's Multiple = Enterprise Value / EBIT
Lower values indicate cheaper companies.
"""

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def compute_acquirers_multiple(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate Acquirer's Multiple (EV/EBIT) and its inverse (EBIT/EV).

    Requires: enterprise_value, ebit columns.
    Only computed where both EBIT > 0 and EV > 0.
    """
    df = df.copy()

    ok = (df["ebit"] > 0) & (df["enterprise_value"] > 0)

    df["acquirers_multiple"] = np.where(
        ok, df["enterprise_value"] / df["ebit"], np.nan
    )
    df["ebit_ev"] = np.where(
        ok, df["ebit"] / df["enterprise_value"], np.nan
    )

    computed = ok.sum()
    logger.info("Acquirer's Multiple: %d/%d computed", computed, len(df))
    return df
