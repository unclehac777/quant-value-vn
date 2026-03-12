"""
Compute financial features from raw scraped data.

Functions:
- compute_features(df) → DataFrame with derived financial metrics

Computed features (TTM for valuation, Annual for Quality):
- market_cap, enterprise_value
- ebit (TTM), revenue (TTM), fcf_yield (TTM)
- roa (Annual), roe (Annual), roic (Annual)
- gross_profitability (Annual), accruals (Annual), cfo_to_assets (Annual)
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
    
    Uses TTM data for Valuation/Pricing metrics to ensure the freshest data.
    Uses Annual data for Quality metrics to ensure stability (per Quantitative Value).

    Uses vectorized Pandas operations for speed.
    Returns DataFrame with new feature columns added.
    """
    df = df.copy()

    # ── 1. Base Metrics (Annual) ──
    ni_ann = df["net_income_parent"].fillna(df.get("net_income", pd.Series(dtype=float)))
    ta = df["total_assets"]  # Balance sheet is snapshot
    eq = df.get("equity", pd.Series(dtype=float))
    rev_ann = df.get("revenue", pd.Series(dtype=float))
    gp_ann = df.get("gross_profit", pd.Series(dtype=float))
    ocf_ann = df.get("operating_cash_flow", pd.Series(dtype=float))
    
    # ── 2. Base Metrics (TTM) ──
    ni_ttm = df.get("net_income_parent_ttm", pd.Series(np.nan, index=df.index)).fillna(
        df.get("net_income_ttm", ni_ann)
    )
    rev_ttm = df.get("revenue_ttm", rev_ann)
    gp_ttm = df.get("gross_profit_ttm", gp_ann)
    ocf_ttm = df.get("operating_cash_flow_ttm", ocf_ann)
    
    # Expose TTM metrics as the primary flow columns so the UI shows the freshest data
    df["revenue"] = rev_ttm
    df["net_income"] = ni_ttm
    df["operating_cash_flow"] = ocf_ttm
    
    # ── Strict EBIT (Operating Earnings) ──
    # EBIT = Revenue - COGS - SGA - Depreciation - Amortization
    def _calc_ebit(rev, gp, sga, dep, am):
        cogs = rev - gp
        # Fill missing with 0 for expenses
        sga = sga.fillna(0)
        dep = dep.fillna(0)
        am = am.fillna(0)
        return rev - cogs - sga - dep - am

    sga_ann = df.get("sga", pd.Series(0.0, index=df.index)).fillna(
        df.get("selling_expense", pd.Series(0.0, index=df.index)).fillna(0) + 
        df.get("admin_expense", pd.Series(0.0, index=df.index)).fillna(0)
    )
    dep_ann = abs(df.get("depreciation", pd.Series(0.0, index=df.index))).fillna(0)
    am_ann = abs(df.get("amortization", pd.Series(0.0, index=df.index))).fillna(0)
    
    ebit_ann = _calc_ebit(rev_ann, gp_ann, sga_ann, dep_ann, am_ann)
    
    sga_ttm = df.get("sga_ttm", sga_ann)
    dep_ttm = abs(df.get("depreciation_ttm", dep_ann)).fillna(0)
    am_ttm = abs(df.get("amortization_ttm", am_ann)).fillna(0)
    
    ebit_ttm = _calc_ebit(rev_ttm, gp_ttm, sga_ttm, dep_ttm, am_ttm)
    
    df["ebit"] = ebit_ttm

    # ── Shares outstanding ──
    eps_ttm = df.get("eps_ttm", df.get("eps", pd.Series(dtype=float)))
    df["eps"] = eps_ttm
    
    # Prioritize CafeF shares, fallback to NI/EPS calculation
    computed_shares = pd.Series(
        np.where((eps_ttm > 0) & (ni_ttm > 0), ni_ttm / eps_ttm, np.nan),
        index=df.index
    )
    df["shares"] = df.get("shares_cafef", pd.Series(np.nan, index=df.index)).fillna(computed_shares)

    # ── Market capitalisation ──
    price = df.get("price", pd.Series(dtype=float))
    
    # Prioritize CafeF Market Cap (already converted to VND in ingest.py if available)
    mc_cafef = df.get("market_cap_cafef", pd.Series(np.nan, index=df.index))
    
    # Fallback to Price * Shares
    computed_mc = pd.Series(
        np.where(df["shares"].notna() & price.notna(), price * df["shares"], np.nan),
        index=df.index
    )
    
    df["market_cap"] = mc_cafef.fillna(computed_mc)

    # ── Debt & Cash aggregates ──
    df["total_debt"] = (
        df.get("short_term_debt", pd.Series(0.0, index=df.index)).fillna(0)
        + df.get("long_term_debt", pd.Series(0.0, index=df.index)).fillna(0)
    )
    df["total_cash"] = (
        df.get("cash", pd.Series(0.0, index=df.index)).fillna(0)
        + df.get("short_term_investments", pd.Series(0.0, index=df.index)).fillna(0)
    )

    # ── Enterprise Value ──
    mc = df["market_cap"]
    mi = df.get("minority_interest", pd.Series(0.0, index=df.index)).fillna(0)
    ps = df.get("preferred_stock", pd.Series(0.0, index=df.index)).fillna(0)
    td = df.get("total_debt", pd.Series(0.0, index=df.index)).fillna(0)
    tc = df.get("total_cash", pd.Series(0.0, index=df.index)).fillna(0)
    
    ev = mc + td + mi + ps - tc
    df["enterprise_value"] = np.where(mc > 0, ev.clip(lower=0), np.nan)

    # ── Working Capital ──
    ca = df.get("current_assets", pd.Series(0.0, index=df.index)).fillna(0)
    cl = df.get("current_liabilities", pd.Series(0.0, index=df.index)).fillna(0)
    df["working_capital"] = ca - cl

    # ── Invested Capital ──
    ppe = df.get("ppe", pd.Series(dtype=float)).fillna(0)
    df["invested_capital"] = df["working_capital"] + ppe
    invested_capital = df["invested_capital"]

    # ── Standard Quality Metrics (Use Annual) ──
    df["roa"] = np.where(ta > 0, ni_ann / ta, np.nan)
    df["roe"] = np.where(eq > 0, ni_ann / eq, np.nan)
    df["gross_profitability"] = np.where(
        (ta > 0) & gp_ann.notna(), gp_ann / ta, np.nan
    )
    pretax_ann_std = df.get("pretax_profit", pd.Series(dtype=float))
    tax_rate_ann_std = np.where(
        (pretax_ann_std > 0) & ni_ann.notna(),
        1 - ni_ann / pretax_ann_std,
        0.20,
    )
    df["roic"] = np.where(
        (ebit_ann > 0) & (invested_capital > 0),
        ebit_ann * (1 - tax_rate_ann_std) / invested_capital,
        np.nan,
    )
    df["cfo_to_assets"] = np.where(
        (ta > 0) & ocf_ann.notna(), ocf_ann / ta, np.nan
    )
    df["accruals"] = np.where(
        (ta > 0) & ni_ann.notna() & ocf_ann.notna(),
        (ni_ann - ocf_ann) / ta,
        np.nan,
    )

    # ── EBITDA (Use TTM) ──
    # EBITDA = EBIT + Depreciation & Amortization
    dep_ttm = abs(df.get("depreciation_ttm", df.get("depreciation", pd.Series(0.0, index=df.index)))).fillna(0)
    df["ebitda"] = ebit_ttm + dep_ttm


    # ── Quality Metrics (5-Year Averages) ──
    # Helper to calculate n-year averages safely
    def _avg_5yr(base_col: str) -> pd.Series:
        cols = [base_col] + [f"{base_col}_y{i}" for i in range(1, 5)]
        valid_cols = [c for c in cols if c in df.columns]
        if not valid_cols:
            return pd.Series(np.nan, index=df.index)
        # Average across the columns that exist for each row (skipping NaNs)
        return df[valid_cols].mean(axis=1)

    def _std_5yr(base_col: str) -> pd.Series:
        cols = [base_col] + [f"{base_col}_y{i}" for i in range(1, 5)]
        valid_cols = [c for c in cols if c in df.columns]
        if not valid_cols:
            return pd.Series(np.nan, index=df.index)
        return df[valid_cols].std(axis=1)

    # 1. ROA 5-year average
    # Compute ROA for each year first, then average
    roa_cols = []
    for i in range(5):
        suffix = "" if i == 0 else f"_y{i}"
        ni = df.get(f"net_income_parent{suffix}", df.get(f"net_income{suffix}", pd.Series(dtype=float)))
        ta_yr = df.get(f"total_assets{suffix}", pd.Series(dtype=float))
        if ni is not None and ta_yr is not None and not ta_yr.empty:
            df[f"_roa_tmp{suffix}"] = np.where(ta_yr > 0, ni / ta_yr, np.nan)
            roa_cols.append(f"_roa_tmp{suffix}")
    
    df["ROA_5yr_avg"] = df[roa_cols].mean(axis=1) if roa_cols else np.nan

    # 2. ROC 5-year average
    roc_cols = []
    for i in range(5):
        suffix = "" if i == 0 else f"_y{i}"
        
        op = df.get(f"operating_profit{suffix}", pd.Series(dtype=float))
        ie = df.get(f"interest_expense{suffix}", pd.Series(dtype=float)).fillna(0).clip(lower=0)
        ebit_yr = np.where(op.notna(), op + ie, np.nan)
        
        ca_yr = df.get(f"current_assets{suffix}", pd.Series(dtype=float)).fillna(0)
        cl_yr = df.get(f"current_liabilities{suffix}", pd.Series(dtype=float)).fillna(0)
        wc_yr = np.where(ca_yr.notna() & cl_yr.notna(), ca_yr - cl_yr, np.nan)
        
        ppe_yr = df.get(f"ppe{suffix}", pd.Series(dtype=float)).fillna(0)
        ic_yr = wc_yr + ppe_yr
        
        pretax = df.get(f"pretax_profit{suffix}", pd.Series(dtype=float))
        ni = df.get(f"net_income_parent{suffix}", df.get(f"net_income{suffix}", pd.Series(dtype=float)))
        
        tax_rate = np.where(
            (pretax > 0) & ni.notna(),
            1 - ni / pretax,
            0.20,
        )
        
        if ebit_yr is not None and not np.isnan(ebit_yr).all():
            df[f"_roc_tmp{suffix}"] = np.where(
                (ebit_yr > 0) & (ic_yr > 0),
                ebit_yr * (1 - tax_rate) / ic_yr,
                np.nan
            )
            roc_cols.append(f"_roc_tmp{suffix}")
            
    df["ROC_5yr_avg"] = df[roc_cols].mean(axis=1) if roc_cols else np.nan

    # 3. FCF / Assets 5-year average
    # FCF = Operating Cash Flow - Capex
    fcf_cols = []
    for i in range(5):
        suffix = "" if i == 0 else f"_y{i}"
        ocf = df.get(f"operating_cash_flow{suffix}", pd.Series(dtype=float)).fillna(0)
        capex = df.get(f"capex{suffix}", pd.Series(0.0, index=df.index)).fillna(0)
        fcf = ocf - capex
        ta_yr = df.get(f"total_assets{suffix}", pd.Series(dtype=float))
        
        if ta_yr is not None and not ta_yr.empty:
            df[f"_fcf_tmp{suffix}"] = np.where(ta_yr > 0, fcf / ta_yr, np.nan)
            fcf_cols.append(f"_fcf_tmp{suffix}")
            
    df["FCF_assets_5yr_avg"] = df[fcf_cols].mean(axis=1) if fcf_cols else np.nan

    # 4. Gross Margin Stability (5-year standard deviation)
    gm_cols = []
    for i in range(5):
        suffix = "" if i == 0 else f"_y{i}"
        rev = df.get(f"revenue{suffix}", pd.Series(dtype=float))
        gp = df.get(f"gross_profit{suffix}", pd.Series(dtype=float))
        
        if rev is not None and gp is not None and not rev.empty:
            df[f"_gm_tmp{suffix}"] = np.where((rev > 0) & gp.notna(), gp / rev, np.nan)
            gm_cols.append(f"_gm_tmp{suffix}")
            
    # Calculate stability: we want LOWER std to match HIGHER rank
    # Use standard deviation across the calculated yearly margin columns
    if gm_cols:
        df["GM_stability"] = df[gm_cols].std(axis=1)
    else:
        df["GM_stability"] = np.nan
        
    # Drop temp cols to keep dataframe clean
    tmp_cols = roa_cols + roc_cols + fcf_cols + gm_cols
    df.drop(columns=[c for c in tmp_cols if c in df.columns], inplace=True)

    # Note: Accruals formula doesn't have a 5-year average rule in the checklist, 
    # but the old checklist had it. The new rank drops it entirely in favor of 
    # the 4 factors above. We will drop accruals computation to match.

    # ── Valuation Metrics (Use TTM) ──
    # FCF = Operating Cash Flow - Capex
    capex_ttm = abs(df.get("capex_ttm", df.get("capex", pd.Series(0.0, index=df.index)))).fillna(0)
    fcf_ttm = ocf_ttm - capex_ttm
    df["fcf"] = fcf_ttm

    df["fcf_yield"] = np.where(
        (mc > 0),
        fcf_ttm / mc,
        np.nan,
    )

    # ── Acquirer's Multiple (EV/EBIT) ──
    df["acquirers_multiple"] = np.where(
        (df["ebit"] > 0) & (df["enterprise_value"] > 0),
        df["enterprise_value"] / df["ebit"],
        np.nan
    )

    # ── EV/EBITDA (Keep as secondary metric) ──
    df["ev_ebitda"] = np.where(
        (df["ebitda"] > 0) & (df["enterprise_value"] > 0),
        df["enterprise_value"] / df["ebitda"],
        np.nan
    )

    # ── Margins (Use TTM for freshest performance view) ──
    df["gross_margin"] = np.where(
        (rev_ttm > 0) & gp_ttm.notna(), gp_ttm / rev_ttm, np.nan
    )
    df["net_margin"] = np.where(
        (rev_ttm > 0) & ni_ttm.notna(), ni_ttm / rev_ttm, np.nan
    )

    # ── Valuation multiples (Use TTM) ──
    df["debt_equity"] = np.where(eq > 0, df["total_debt"] / eq, np.nan)
    df["pe"] = np.where((mc > 0) & (ni_ttm > 0), mc / ni_ttm, np.nan)
    df["pb"] = np.where((mc > 0) & (eq > 0), mc / eq, np.nan)

    # ── Acquirer's Multiple is calculated separately in ranking.py ──
    # ranking.py uses df['enterprise_value'] and df['ebit'], which now point to TTM!

    # ── Utility columns for Frontend (Billions) ──
    df["market_cap_b"] = df["market_cap"] / 1e9
    df["ev_b"] = df["enterprise_value"] / 1e9

    logger.info("Computed features for %d stocks", len(df))
    return df

