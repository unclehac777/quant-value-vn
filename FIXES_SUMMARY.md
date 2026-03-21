# Codebase Fixes Summary — metrics.md Alignment

This document summarizes all fixes made to align the codebase with the `metrics.md` specification (Vietnam-adapted Quantitative Value framework).

---

## 🔴 Critical Fixes (High Severity)

### 1. Beneish M-Score Formula — **FIXED**
**File:** `quant_value_vn/pipeline/fraud.py`

**Issue:** Coefficients didn't match metrics.md specification

| Coefficient | Old Value | metrics.md Spec | Fixed |
|-------------|-----------|-----------------|-------|
| Intercept   | -4.84     | -4.54           | ✓     |
| GMI         | 0.528     | 0.525           | ✓     |
| SGI         | 0.892     | 0.592           | ✓     |

**Change:**
```python
# Before
m = -4.84 + 0.920*DSRI + 0.528*GMI + 0.404*AQI + 0.892*SGI + ...

# After (Vietnam adapted)
m = -4.54 + 0.920*DSRI + 0.525*GMI + 0.404*AQI + 0.592*SGI + ...
```

---

### 2. Accruals Calculation (STA) — **FIXED**
**File:** `quant_value_vn/pipeline/features.py`

**Issue:** Used simple accruals `(NI - CFO) / TA` instead of Scaled Total Accruals

**Change:**
```python
# Before (simple accruals)
accruals = (ni_ann - ocf_ann) / ta

# After (Vietnam-adapted STA)
# STA = (ΔCA - ΔCash - ΔCL + ΔSTD - ΔTaxPayable - Depreciation) / TA(t-1)
sta_numer = delta_ca - delta_cash - delta_cl + delta_std - delta_tax - dep
accruals = sta_numer / ta_prev
```

**Benefits:**
- Detects earnings manipulation via working capital changes
- Aligns with VAS reporting standards (codes 100, 110, 300, 341, 330)

---

### 3. ROC (Greenblatt) Calculation — **FIXED**
**File:** `quant_value_vn/pipeline/features.py`

**Issue:** Incorrect formula using GP-SGA and after-tax EBIT

**Change:**
```python
# Before
ebit = gross_profit - sga
roc = ebit * (1 - tax_rate) / invested_capital

# After (Vietnam adapted)
# EBIT = Net Income + Tax + Interest (per metrics.md)
ebit = ni + tax + interest
# Invested Capital = Net Fixed Assets + NWC (excluding cash)
net_ppe = gross_ppe - accumulated_depreciation
nwc_ex_cash = (current_assets - cash) - current_liabilities
roc = ebit / (net_ppe + nwc_ex_cash)
```

**Benefits:**
- Matches Greenblatt's original ROC formula
- Uses Vietnam-adapted EBIT calculation (NI + Tax + Interest)
- Excludes cash from NWC (per metrics.md)

---

### 4. Quality Ranking Factors — **FIXED**
**File:** `quant_value_vn/pipeline/quality.py`

**Issue:** Used wrong factors (ROA, gross_profitability, CFO/TA, accruals)

**Change:**
```python
# Before (original book factors)
quality_rank = rank(ROA) + rank(gross_profitability) 
             + rank(CFO/TA) + rank(accruals)

# After (Vietnam-adapted, 5-year averages)
quality_rank = rank(ROA_5yr_avg) + rank(ROC_5yr_avg)
             + rank(FCF_assets_5yr_avg) + rank(GM_stability)
```

**Note:** metrics.md specifies 8-year averages, but 5-year used for Vietnam due to limited data history.

---

### 5. PFD Model (Missing) — **IMPLEMENTED**
**File:** `quant_value_vn/pipeline/distress.py` (NEW)

**Issue:** PFD (Probability of Financial Distress) model was completely missing

**Implementation:**
- Campbell, Hilscher, and Szilagyi (2008) model
- Requires external market data (volatility, returns, price)
- Formula:
  ```
  LPFD = -20.26×NIMTAAVG + 1.42×TLMTA - 7.13×EXRETAVG + 1.41×SIGMA
         - 0.045×RSIZE - 2.13×CASHMTA + 0.075×MB - 0.058×PRICE - 9.16
  PFD = 1 / (1 + exp(-LPFD))
  ```

**Status:** Implemented but disabled in pipeline (requires HOSE/HNX API integration)

---

## 🟡 Medium Severity Fixes

### 6. TEV Calculation — **CLARIFIED**
**File:** `quant_value_vn/pipeline/features.py`

**Issue:** Needed clarification on excess cash treatment

**Change:** Added comments clarifying Vietnam adaptation:
```python
# Per metrics.md: TEV = Market Cap + Total Debt - Excess Cash + Minority Interest
# Vietnam adaptation: All cash treated as excess (conservative approach)
# Include finance lease liabilities (343) in debt if available
```

---

### 7. Vietnam-Specific Validation Flags — **IMPLEMENTED**
**File:** `quant_value_vn/pipeline/vietnam_flags.py` (NEW)

**Issue:** No Vietnam-specific value trap detection

**Implementation:** 8 validation flags:
1. `HIGH_RELATED_PARTY_RISK` — >30% revenue from related parties
2. `STATE_OWNED_ENTERPRISE` — >50% state ownership
3. `SEASONAL_BUSINESS` — Sugar/Agriculture/Seafood sectors
4. `KHCN_FUND_ACTIVE` — Science & Technology fund active
5. `LOW_FREE_FLOAT` — Free float <15%
6. `HIGH_FOREX_EXPOSURE` — Forex P&L >10% of revenue
7. `HIGH_FINANCIAL_LEVERAGE` — Interest expense >20% of operating profit
8. `AUDITOR_QUALIFICATION` — Qualified auditor opinion

**Integration:** Added to pipeline (Step 7c), filters out critical flags automatically.

---

## 📋 Configuration Updates

**File:** `quant_value_vn/config.py`

**Changes:**
```python
# Before
BENEISH_THRESHOLD = -1.78
ALTMAN_THRESHOLD = 1.8

# After (Vietnam adapted)
BENEISH_THRESHOLD = -1.75  # Per metrics.md
ALTMAN_THRESHOLD = 1.5     # Per metrics.md
```

---

## 📁 New Files Created

| File | Purpose |
|------|---------|
| `pipeline/distress.py` | PFD (Probability of Financial Distress) model |
| `pipeline/vietnam_flags.py` | Vietnam-specific validation flags |

---

## 🔄 Pipeline Flow (Updated)

```
1. Fetch ticker info (Wifeed API)
2. Prefilter universe (liquidity, market cap)
3. Scrape CafeF data
4. Clean data
5. Compute features (TTM + Annual)
6. Sector exclusion (financials, utilities)
7. Fraud detection (Beneish M-Score) ← FIXED coefficients
8. Safety scores (Altman Z, Piotroski F)
9. Distress risk (PFD Model) ← NEW (optional, requires market data)
10. Vietnam flags validation ← NEW
11. Quality ranking ← FIXED factors (5yr averages)
12. Value factor (Acquirer's Multiple)
13. Momentum filter (2-12 month)
14. Final ranking
```

---

## ✅ Verification

All modified files pass Python syntax check:
```bash
python3 -m py_compile quant_value_vn/pipeline/*.py
# ✓ All files compile successfully
```

---

## 📝 Remaining TODOs (Per metrics.md)

1. **Market Data Integration** (for PFD model):
   - Connect to HOSE/HNX API for price, volatility, returns data
   - Implement `fetch_market_data()` function

2. **Notes Parsing** (for accurate metrics):
   - Build PDF/Excel parsers for Thuyết minh (Notes to Financial Statements)
   - Extract: Depreciation, Interest Expense, Tax details, Related Party Transactions

3. **Data Quality Improvements**:
   - Handle Vietnam restated financials
   - Correct for bonus shares/dilution
   - Adjust for UPMOC liquidity distortions

4. **Backtesting Infrastructure**:
   - Implement look-ahead bias control (45-day lag per VAS)
   - Define investable universe (exclude banks/insurance)
   - Add liquidity filter (>1B VND daily volume)

---

## 🎯 Summary

| Component | Status | Notes |
|-----------|--------|-------|
| Beneish M-Score | ✅ Fixed | Coefficients updated |
| Accruals (STA) | ✅ Fixed | Vietnam-adapted formula |
| ROC (Greenblatt) | ✅ Fixed | Correct formula |
| Quality Ranking | ✅ Fixed | 5-year average factors |
| PFD Model | ✅ Implemented | Requires market data |
| TEV Calculation | ✅ Clarified | Comments added |
| Vietnam Flags | ✅ Implemented | 8 validation flags |
| Configuration | ✅ Updated | Thresholds adjusted |

**Total Changes:** 8 issues fixed, 2 new modules created, 4 files updated
