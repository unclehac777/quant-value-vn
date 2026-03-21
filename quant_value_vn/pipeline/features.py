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
    
    # EBIT = Gross Profit - SGA
    # Note: In VAS, depreciation is already included in COGS and SGA, 
    # so subtracting it again would be double-counting.
    def _calc_ebit(gp, sga):
        # Fill missing with 0 for expenses
        sga = sga.fillna(0)
        return gp - sga

    sga_ann = df.get("sga", pd.Series(np.nan, index=df.index)).fillna(
        df.get("selling_expense", pd.Series(0.0, index=df.index)).fillna(0) + 
        df.get("admin_expense", pd.Series(0.0, index=df.index)).fillna(0)
    )
    ebit_ann = _calc_ebit(gp_ann, sga_ann)
    
    sga_ttm = df.get("sga_ttm", sga_ann)
    
    ebit_ttm = _calc_ebit(gp_ttm, sga_ttm)
    
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
    # Include short-term investments in cash (VAS codes 111, 112, 128)
    df["total_debt"] = (
        df.get("short_term_debt", pd.Series(0.0, index=df.index)).fillna(0)
        + df.get("long_term_debt", pd.Series(0.0, index=df.index)).fillna(0)
    )
    # Per metrics.md: For Vietnam, conservative approach treats all cash as excess
    df["total_cash"] = (
        df.get("cash", pd.Series(0.0, index=df.index)).fillna(0)
        + df.get("short_term_investments", pd.Series(0.0, index=df.index)).fillna(0)
    )

    # ── Enterprise Value (TEV) — Vietnam Adapted ──
    # Per metrics.md: TEV = Market Cap + Total Debt - Excess Cash + Minority Interest
    # Vietnam adaptation: All cash treated as excess (conservative)
    # Note: Include finance lease liabilities (343) in debt if available
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
    # Net Invested Capital = Working Capital + Net Fixed Assets (Tangible + Intangible)
    ppe = df.get("ppe", pd.Series(0.0, index=df.index)).fillna(0)
    intangibles = df.get("intangible_assets", pd.Series(0.0, index=df.index)).fillna(0)
    df["invested_capital"] = df["working_capital"] + ppe + intangibles
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

    # ── Accruals: Scaled Total Accruals (STA) — Vietnam Adapted ──
    # Per metrics.md: STA = (ΔCA - ΔCash - ΔCL + ΔSTD - ΔTaxPayable - Depreciation) / TA(t-1)
    # This is more sophisticated than simple (NI - CFO) / TA
    ta_prev = df.get("total_assets_prev", pd.Series(dtype=float))
    ca = df.get("current_assets", pd.Series(0.0, index=df.index)).fillna(0)
    ca_prev = df.get("current_assets_prev", pd.Series(0.0, index=df.index)).fillna(0)
    cash = df.get("cash", pd.Series(0.0, index=df.index)).fillna(0)
    cash_prev = df.get("cash_prev", pd.Series(0.0, index=df.index)).fillna(0)
    cl = df.get("current_liabilities", pd.Series(0.0, index=df.index)).fillna(0)
    cl_prev = df.get("current_liabilities_prev", pd.Series(0.0, index=df.index)).fillna(0)
    std = (
        df.get("short_term_debt", pd.Series(0.0, index=df.index)).fillna(0) +
        df.get("long_term_debt", pd.Series(0.0, index=df.index)).fillna(0)  # Include all debt if ST not separated
    )
    std_prev = (
        df.get("short_term_debt_prev", pd.Series(0.0, index=df.index)).fillna(0) +
        df.get("long_term_debt_prev", pd.Series(0.0, index=df.index)).fillna(0)
    )
    tax_payable = df.get("tax_payable", pd.Series(0.0, index=df.index)).fillna(0)
    tax_payable_prev = df.get("tax_payable_prev", pd.Series(0.0, index=df.index)).fillna(0)
    dep = df.get("depreciation", pd.Series(0.0, index=df.index)).fillna(0)

    delta_ca = ca - ca_prev
    delta_cash = cash - cash_prev
    delta_cl = cl - cl_prev
    delta_std = std - std_prev
    delta_tax = tax_payable - tax_payable_prev

    # STA numerator: ΔCA - ΔCash - ΔCL + ΔSTD - ΔTaxPayable - Depreciation
    sta_numer = delta_ca - delta_cash - delta_cl + delta_std - delta_tax - dep
    # Denominator: prior year total assets
    df["accruals"] = np.where(
        (ta_prev > 0) & ta_prev.notna(),
        sta_numer / ta_prev,
        np.nan,
    )

    # ── EBITDA (Use TTM) ──
    # EBITDA = EBIT + Depreciation & Amortization
    dep_ttm = abs(df.get("depreciation_ttm", df.get("depreciation", pd.Series(0.0, index=df.index)))).fillna(0)
    df["ebitda"] = ebit_ttm + dep_ttm


    # ── Quality Metrics (5-Year Averages) — Vietnam Adapted ──
    # Note: CafeF typically provides only 4 years of historical data.
    # We calculate averages using whatever years are available (min 2 years).
    # metrics.md specifies 8-year averages, but Vietnam data history often limited.

    def _count_available_years(base_col: str, max_years: int = 5) -> tuple[list[str], int]:
        """Count how many years of data are available for a column."""
        cols = []
        for i in range(max_years):
            suffix = "" if i == 0 else f"_y{i}"
            key = f"{base_col}{suffix}"
            if key in df.columns and df[key].notna().any():
                cols.append(key)
        return cols, len(cols)

    # 1. ROA 5-year average
    # Compute ROA for each year first, then average
    # Note: Uses whatever years are available (min 2 years for valid average)
    roa_cols = []
    for i in range(5):
        suffix = "" if i == 0 else f"_y{i}"
        ni = df.get(f"net_income_parent{suffix}", df.get(f"net_income{suffix}", pd.Series(dtype=float)))
        ta_yr = df.get(f"total_assets{suffix}", pd.Series(dtype=float))
        if ni is not None and ta_yr is not None and not ta_yr.empty:
            df[f"_roa_tmp{suffix}"] = np.where(ta_yr > 0, ni / ta_yr, np.nan)
            roa_cols.append(f"_roa_tmp{suffix}")

    if len(roa_cols) >= 2:
        df["ROA_5yr_avg"] = df[roa_cols].mean(axis=1)
        df["ROA_years_count"] = len(roa_cols)
    else:
        df["ROA_5yr_avg"] = np.nan
        df["ROA_years_count"] = len(roa_cols)

    # 2. ROC 5-year average (Greenblatt) — Vietnam Adapted
    # Per metrics.md: ROC = EBIT / (Net Fixed Assets + NWC excluding cash)
    # Where EBIT = Net Income + Tax + Interest (Vietnam adaptation)
    roc_cols = []
    for i in range(5):
        suffix = "" if i == 0 else f"_y{i}"

        # EBIT calculation (Vietnam adapted): Net Income + Tax + Interest
        ni_yr = df.get(f"net_income_parent{suffix}", df.get(f"net_income{suffix}", pd.Series(dtype=float)))
        tax_yr = df.get(f"tax_expense{suffix}", pd.Series(0.0, index=df.index)).fillna(0)
        # Interest expense: try specific field, fallback to financial expense
        interest_yr = df.get(f"interest_expense{suffix}", df.get(f"financial_expense{suffix}", pd.Series(0.0, index=df.index))).fillna(0)
        ebit_yr = ni_yr.fillna(0) + tax_yr + interest_yr

        # Net Fixed Assets: PPE (net of depreciation)
        # Per metrics.md: Net Fixed Assets = Gross PPE - Accumulated Depreciation
        ppe_gross = df.get(f"ppe{suffix}", df.get(f"fixed_assets{suffix}", pd.Series(0.0, index=df.index))).fillna(0)
        dep_accum = df.get(f"depreciation_accumulated{suffix}", pd.Series(0.0, index=df.index)).fillna(0)
        net_ppe = ppe_gross - dep_accum

        # NWC excluding cash: (Current Assets - Cash) - Current Liabilities
        ca_yr = df.get(f"current_assets{suffix}", pd.Series(dtype=float)).fillna(0)
        cash_yr = df.get(f"cash{suffix}", pd.Series(0.0, index=df.index)).fillna(0)
        cl_yr = df.get(f"current_liabilities{suffix}", pd.Series(dtype=float)).fillna(0)
        nwc_yr = (ca_yr - cash_yr) - cl_yr

        # Invested Capital = Net Fixed Assets + NWC (excluding cash)
        ic_yr = net_ppe + nwc_yr

        if not ebit_yr.isna().all():
            # ROC = EBIT / Invested Capital (pre-tax, per Greenblatt)
            df[f"_roc_tmp{suffix}"] = np.where(
                (ebit_yr > 0) & (ic_yr > 0),
                ebit_yr / ic_yr,
                np.nan
            )
            roc_cols.append(f"_roc_tmp{suffix}")

    if len(roc_cols) >= 2:
        df["ROC_5yr_avg"] = df[roc_cols].mean(axis=1)
        df["ROC_years_count"] = len(roc_cols)
    else:
        df["ROC_5yr_avg"] = np.nan
        df["ROC_years_count"] = len(roc_cols)

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

    if len(fcf_cols) >= 2:
        df["FCF_assets_5yr_avg"] = df[fcf_cols].mean(axis=1)
        df["FCF_years_count"] = len(fcf_cols)
    else:
        df["FCF_assets_5yr_avg"] = np.nan
        df["FCF_years_count"] = len(fcf_cols)

    # 4. Gross Margin Stability (5-year standard deviation)
    # Note: Lower std dev = more stable margins = higher quality
    # Requires min 3 years for meaningful std dev calculation
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
    if len(gm_cols) >= 3:
        df["GM_stability"] = df[gm_cols].std(axis=1)
        df["GM_years_count"] = len(gm_cols)
    else:
        df["GM_stability"] = np.nan
        df["GM_years_count"] = len(gm_cols)

    # Drop temp cols to keep dataframe clean
    tmp_cols = roa_cols + roc_cols + fcf_cols + gm_cols
    df.drop(columns=[c for c in tmp_cols if c in df.columns], inplace=True)

    # Log data quality summary
    logger.info("Quality metrics: ROA=%d yrs, ROC=%d yrs, FCF=%d yrs, GM=%d yrs (avg available)",
                df["ROA_years_count"].mean(), df["ROC_years_count"].mean(),
                df["FCF_years_count"].mean(), df["GM_years_count"].mean())

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

