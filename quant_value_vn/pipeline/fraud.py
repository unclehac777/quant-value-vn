"""
Fraud detection models from Quantitative Value (Chapters 3-4).

Functions:
- compute_beneish_mscore(d) → M-Score float for one stock dict
- compute_probm(mscore)     → probability of manipulation
- compute_fraud_scores(df)  → DataFrame with beneish_mscore & probm columns
- remove_manipulators(df)   → DataFrame with likely manipulators removed

Beneish M-Score > -1.78 indicates likely earnings manipulation.
"""

import logging
from typing import Dict, Optional

import numpy as np
import pandas as pd

from quant_value_vn.config import BENEISH_THRESHOLD

logger = logging.getLogger(__name__)


def compute_beneish_mscore(d: Dict) -> Optional[float]:
    """
    Compute Beneish M-Score from 2-year financial data.

    Variables:
      DSRI = (Receivables_t / Revenue_t) / (Receivables_t-1 / Revenue_t-1)
      GMI  = Gross_Margin_t-1 / Gross_Margin_t
      AQI  = (1 - (CA_t + PPE_t) / TA_t) / (1 - (CA_t-1 + PPE_t-1) / TA_t-1)
      SGI  = Revenue_t / Revenue_t-1
      DEPI = Depreciation_Rate_t-1 / Depreciation_Rate_t
      SGAI = (SGA_t / Revenue_t) / (SGA_t-1 / Revenue_t-1)
      LVGI = Leverage_t / Leverage_t-1
      TATA = (NI - CFO) / TA

    Returns M-Score value. Companies with M > -1.78 are likely manipulators.
    """
    try:
        rev = d.get("revenue")
        rev_prev = d.get("revenue_prev")
        if not rev or not rev_prev or rev <= 0 or rev_prev <= 0:
            return None

        ta = d.get("total_assets")
        ta_prev = d.get("total_assets_prev")
        if not ta or not ta_prev or ta <= 0 or ta_prev <= 0:
            return None

        # DSRI
        rec = d.get("receivables") or 0
        rec_prev = d.get("receivables_prev") or 0
        dsri_num = rec / rev if rev else 0
        dsri_den = rec_prev / rev_prev if rev_prev else 0
        dsri = dsri_num / dsri_den if dsri_den > 0 else 1.0

        # GMI
        gp = d.get("gross_profit")
        gp_prev = d.get("gross_profit_prev")
        if gp is not None and gp_prev is not None and rev > 0 and rev_prev > 0:
            gm = gp / rev
            gm_prev = gp_prev / rev_prev
            gmi = gm_prev / gm if gm > 0 else 1.0
        else:
            gmi = 1.0

        # AQI
        ca = d.get("current_assets") or 0
        ca_prev = d.get("current_assets_prev") or 0
        ppe = d.get("ppe") or 0
        ppe_prev = d.get("ppe_prev") or 0
        aq = 1 - (ca + ppe) / ta if ta > 0 else 0
        aq_prev = 1 - (ca_prev + ppe_prev) / ta_prev if ta_prev > 0 else 0
        aqi = aq / aq_prev if aq_prev != 0 else 1.0

        # SGI
        sgi = rev / rev_prev

        # DEPI
        dep = abs(d.get("depreciation") or d.get("depreciation_accumulated") or 0)
        dep_prev = abs(d.get("depreciation_prev") or d.get("depreciation_accumulated_prev") or 0)
        dep_rate = dep / (dep + ppe) if (dep + ppe) > 0 else 0
        dep_rate_prev = dep_prev / (dep_prev + ppe_prev) if (dep_prev + ppe_prev) > 0 else 0
        depi = dep_rate_prev / dep_rate if dep_rate > 0 else 1.0

        # SGAI
        sga = d.get("sga") or 0
        sga_prev = d.get("sga_prev") or 0
        sgai_num = sga / rev if rev > 0 else 0
        sgai_den = sga_prev / rev_prev if rev_prev > 0 else 0
        sgai = sgai_num / sgai_den if sgai_den > 0 else 1.0

        # LVGI
        tl = d.get("total_liabilities") or 0
        tl_prev = d.get("total_liabilities_prev") or 0
        lev = tl / ta if ta > 0 else 0
        lev_prev = tl_prev / ta_prev if ta_prev > 0 else 0
        lvgi = lev / lev_prev if lev_prev > 0 else 1.0

        # TATA
        ni = d.get("net_income_parent") or d.get("net_income") or 0
        ocf = d.get("operating_cash_flow")
        tata = (ni - ocf) / ta if (ocf is not None and ta > 0) else 0

        # M-Score formula (Beneish 1999)
        m = (
            -4.84
            + 0.920 * dsri
            + 0.528 * gmi
            + 0.404 * aqi
            + 0.892 * sgi
            + 0.115 * depi
            - 0.172 * sgai
            + 4.679 * tata
            - 0.327 * lvgi
        )

        # Store component values for transparency
        d["beneish_dsri"] = round(dsri, 3)
        d["beneish_gmi"] = round(gmi, 3)
        d["beneish_aqi"] = round(aqi, 3)
        d["beneish_sgi"] = round(sgi, 3)
        d["beneish_depi"] = round(depi, 3)
        d["beneish_sgai"] = round(sgai, 3)
        d["beneish_lvgi"] = round(lvgi, 3)
        d["beneish_tata"] = round(tata, 4)

        return round(m, 3)

    except (ZeroDivisionError, TypeError):
        return None


def compute_probm(mscore: float) -> float:
    """
    Compute probability of manipulation from M-Score.

    PROBM = 1 / (1 + e^(-M))
    """
    try:
        return round(1.0 / (1.0 + np.exp(-mscore)), 4)
    except (OverflowError, ValueError):
        return 0.0


def compute_fraud_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute Beneish M-Score and PROBM for all stocks.

    Adds columns: beneish_mscore, probm, beneish_* components.
    """
    df = df.copy()
    mscores = []
    probms = []

    for idx, row in df.iterrows():
        d = row.to_dict()
        ms = compute_beneish_mscore(d)
        if ms is not None:
            mscores.append(ms)
            probms.append(compute_probm(ms))
            for k in [
                "beneish_dsri", "beneish_gmi", "beneish_aqi", "beneish_sgi",
                "beneish_depi", "beneish_sgai", "beneish_lvgi", "beneish_tata",
            ]:
                if k in d:
                    df.at[idx, k] = d[k]
        else:
            mscores.append(np.nan)
            probms.append(np.nan)

    df["beneish_mscore"] = mscores
    df["probm"] = probms

    computed = df["beneish_mscore"].notna().sum()
    logger.info("Fraud scores: %d/%d computed", computed, len(df))
    return df


def remove_manipulators(
    df: pd.DataFrame, threshold: float = BENEISH_THRESHOLD
) -> pd.DataFrame:
    """
    Remove companies with M-Score > threshold (likely manipulators).

    Keeps stocks with M-Score <= threshold OR NaN (insufficient data).
    """
    initial = len(df)
    mask = df["beneish_mscore"].isna() | (df["beneish_mscore"] <= threshold)
    df = df[mask].copy()
    removed = initial - len(df)
    if removed > 0:
        logger.info("Fraud filter (M > %.2f): removed %d stocks", threshold, removed)
    return df
