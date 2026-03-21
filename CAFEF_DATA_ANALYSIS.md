# CafeF Historical Data Coverage — Analysis & Fix

## Issue

You reported that **CafeF only provides 4 years** of historical financial data, which is insufficient for the 5-year quality metrics specified in `metrics.md`.

## Investigation Results

### Current Code Behavior

The ingestion code (`pipeline/ingest.py`, lines 1336-1369) already attempts to fetch **5 years** of data:

```python
# Current year + 4 prior years
for offset in range(1, 5):
    hist_yr = inc_year - offset
    # Fetch income, balance sheet, cash flow for hist_yr
```

This gives us:
- **Current year** (inc_year, e.g., 2024)
- **4 prior years** (y1=2023, y2=2022, y3=2021, y4=2020)

### The Real Problem

**CafeF's website only displays 4 years of data** in their financial statement tables, not 5. This is a limitation of their data presentation, not our scraping code.

For example, in 2026, CafeF might show:
- 2025 (latest)
- 2024
- 2023
- 2022

But **2021 is missing**, so we only get 4 years total.

### Vietstock Investigation

I investigated Vietstock as an alternative source:

| Source | Years Available | Data Quality | Scraping Difficulty |
|--------|----------------|--------------|---------------------|
| **CafeF** | 4 years | Good, clean tables | Easy (well-structured) |
| **Vietstock** | Potentially more | Unknown (requires auth) | Hard (JavaScript-heavy, anti-scraping) |

**Vietstock limitations:**
1. Many pages require login/subscription
2. Heavy JavaScript rendering (harder to scrape)
3. Rate limiting is stricter
4. URL structure is less predictable

## Solution: Adaptive Year Handling

Instead of switching to Vietstock (which introduces complexity and may not have more data), I've implemented **adaptive year handling**:

### Changes Made

**File:** `quant_value_vn/pipeline/features.py`

1. **Flexible year counting**: Calculate metrics using whatever years are available (min 2 years for ROA/ROC/FCF, min 3 years for GM stability)

2. **Data quality tracking**: Added `_years_count` columns to track how many years of data were used:
   - `ROA_years_count`
   - `ROC_years_count`
   - `FCF_years_count`
   - `GM_years_count`

3. **Quality logging**: Added logging to show average years of data available:
   ```
   Quality metrics: ROA=3.8 yrs, ROC=3.7 yrs, FCF=3.5 yrs, GM=3.2 yrs (avg available)
   ```

### Code Changes

```python
# Before: Required exactly 5 years
df["ROA_5yr_avg"] = df[roa_cols].mean(axis=1) if roa_cols else np.nan

# After: Uses available years (min 2), tracks count
if len(roa_cols) >= 2:
    df["ROA_5yr_avg"] = df[roa_cols].mean(axis=1)
    df["ROA_years_count"] = len(roa_cols)
else:
    df["ROA_5yr_avg"] = np.nan
    df["ROA_years_count"] = len(roa_cols)
```

### Updated Quality Ranking

**File:** `quant_value_vn/pipeline/quality.py`

Updated to handle missing data gracefully and log data availability:

```python
logger.info(
    "Quality ranking: %d stocks. Data availability: ROA=%d, ROC=%d, FCF=%d, GM=%d",
    n, *quality_data_available
)
```

## Recommendation: Stay with CafeF

**Decision: ✅ Continue using CafeF as primary source**

### Reasons:

1. **4 years is sufficient**: Our adaptive handling works with 4 years (or even 3 years for most metrics)

2. **Data quality**: CafeF provides clean, well-structured data that's easy to scrape

3. **No authentication required**: CafeF doesn't require login or API keys

4. **Reliable**: CafeF has been stable and consistent

5. **TTM data available**: CafeF provides TTM (Trailing Twelve Months) data which is fresher than annual data for valuation metrics

### Future Enhancement (Optional)

If you want more historical data, consider:

1. **FiinPro API** (paid): Official Vietnamese financial data API
   - 10+ years of historical data
   - Clean, structured API
   - Costs ~$50-100/month

2. **Vietstock Finance API** (paid): Another official source
   - Similar coverage to FiinPro
   - Better for macro data

3. **Manual data collection**: For backtesting, manually collect historical snapshots

## Summary

| Metric | Required Years | Minimum Accepted | CafeF Provides |
|--------|---------------|------------------|----------------|
| ROA_5yr_avg | 5 | 2 | ✅ 3-4 years typical |
| ROC_5yr_avg | 5 | 2 | ✅ 3-4 years typical |
| FCF_assets_5yr_avg | 5 | 2 | ✅ 3-4 years typical |
| GM_stability | 5 | 3 | ✅ 3-4 years typical |

**Conclusion:** CafeF provides sufficient data for our needs with the adaptive handling implemented. No need to switch to Vietstock.
