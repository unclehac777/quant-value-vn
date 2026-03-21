# Quantitative Value Framework: Vietnam Edition (v2.0)
### Adapted for Vietnamese Accounting Standards (VAS/Thông tư 200) & Market Structure

**Based on:** *Quantitative Value* by Gray & Carlisle  
**Validated Against:** QNS 2025 Audited Financial Report (Công ty Cổ phần Đường Quảng Ngãi)  
**Target Market:** HOSE, HNX, UPCOM (Vietnam)

---

## 📘 Executive Summary & Critical Updates

This version incorporates critical findings from actual Vietnamese financial reports (specifically QNS). The primary changes from standard US GAAP adaptations include:
1.  **Account Code Corrections:** Aligned with Circular 200/2014/TT-BTC (e.g., Short-term debt is 341, not 310).
2.  **Data Location:** Depreciation and Tax details are often in **Notes (Thuyết minh)** or **Cash Flow Statements**, not the Income Statement face.
3.  **Tax Complexity:** "Taxes Payable" (330) includes VAT/CIT/others; CIT must be isolated.
4.  **Market Data Dependency:** Distress models (PFD) require external price data not found in financial statements.
5.  **Vietnam Specifics:** Adjustments for State Ownership, Related Party Transactions, and Seasonal Businesses (e.g., Sugar harvest cycles).

---

## 🏗 Part 1: Data Infrastructure & VAS Mapping

### 1.1 Core Account Code Mapping (Thông tư 200)
| Metric Component | US GAAP Term | VAS Code (Balance Sheet - CĐKT) | VAS Code (Income Statement - KQKD) | VAS Code (Cash Flow - LCTT) |
| :--- | :--- | :--- | :--- | :--- |
| **Cash & Equivalents** | Cash & ST Investments | **111, 112, 128** (ngắn hạn) | - | - |
| **Accounts Receivable** | Trade Receivables | **131** (Phải thu khách hàng) | - | - |
| **Inventory** | Inventory | **151, 152, 153, 154, 155, 156, 157** | - | - |
| **Current Assets** | Total Current Assets | **100** (Tổng TS ngắn hạn) | - | - |
| **Total Assets** | Total Assets | **200** (Tổng cộng tài sản) | - | - |
| **Short-term Debt** | ST Borrowings | **341** (Vay và nợ thuê tài chính ngắn hạn) | - | - |
| **Finance Lease Liab.** | Finance Leases | **343** (Nợ thuê tài chính) | - | - |
| **Current Liabilities** | Total Current Liab. | **300** (Tổng nợ ngắn hạn) | - | - |
| **Total Liabilities** | Total Liabilities | **300 + 400** (or **300** if LT not separated) | - | - |
| **Equity** | Shareholders' Equity | **400** (Vốn chủ sở hữu) | - | - |
| **Revenue** | Revenue | - | **10** (Doanh thu thuần) | - |
| **COGS** | Cost of Goods Sold | - | **11** (Giá vốn hàng bán) | - |
| **Fin. Expenses** | Interest Expense | - | **22** (Chi phí tài chính) *See Note 1* | - |
| **Depreciation** | Depreciation | - | - | **03** (Khấu hao TSCĐ) or Notes |
| **Tax Expense** | Income Tax Expense | - | **29** (Chi phí thuế TNDN) | - |
| **Tax Paid** | Cash Tax Paid | - | - | **31** (Thuế TNDN đã nộp) |

> **⚠️ Note 1 (Interest Expense):** In Vietnam, `Chi phí tài chính (22)` includes interest, forex losses, and investment losses. You must extract **Interest Expense** from **Notes (Thuyết minh)** or estimate as 80-90% of Financial Expenses for non-financial firms.

### 1.2 Data Source Hierarchy
1.  **Primary:** Consolidated Financial Statements (Báo cáo tài chính hợp nhất) - *Preferred for economic reality.*
2.  **Secondary:** Separate Financial Statements (Báo cáo tài chính riêng) - *Use for legal debt covenants.*
3.  **Critical Supplements:** Notes to Financial Statements (Thuyết minh BCTC) - *Required for Depreciation, Tax Breakdown, Related Parties.*
4.  **External:** HOSE/HNX APIs - *Required for Market Cap, Price, Volatility.*

---

## 🛡 Part 2: Risk Filters (Fraud & Distress)

*Goal: Eliminate the bottom 5% of stocks with highest risk of permanent capital loss.*

### 2.1 Scaled Total Accruals (STA) - Vietnam Adapted
**Purpose:** Detect earnings manipulation via working capital changes.

**Formula:**
```python
STA = (ΔCurrentAssets - ΔCash - ΔCurrentLiabilities + ΔShortTermDebt - ΔTaxPayable - Depreciation) / TotalAssets(t-1)
```

**Vietnamese Implementation:**
```python
def calculate_STA_vn(bs_curr, bs_prior, cf_stmt, notes):
    # 1. Current Assets & Cash (Codes 100, 110)
    delta_ca = bs_curr['tai_san_ngan_han_100'] - bs_prior['tai_san_ngan_han_100']
    delta_cash = bs_curr['tien_tuong_duong_tien_110'] - bs_prior['tien_tuong_duong_tien_110']
    
    # 2. Current Liabilities (Code 300)
    delta_cl = bs_curr['no_ngan_han_300'] - bs_prior['no_ngan_han_300']
    
    # 3. Short Term Debt (Code 341 + 343) - QNS Finding: Include Finance Leases
    debt_curr = bs_curr.get('vay_ngan_han_341', 0) + bs_curr.get('no_thue_tc_ngan_han_343', 0)
    debt_prior = bs_prior.get('vay_ngan_han_341', 0) + bs_prior.get('no_thue_tc_ngan_han_343', 0)
    delta_std = debt_curr - debt_prior
    
    # 4. Income Tax Payable (Code 330) - QNS Finding: Isolate CIT
    # Option A: From Notes (Thuyết minh 18)
    if 'thue_tndn_phai_nop' in notes:
        delta_itp = notes['thue_tndn_phai_nop_curr'] - notes['thue_tndn_phai_nop_prior']
    # Option B: Estimate (Tax Expense - Cash Tax Paid)
    else:
        tax_expense = cf_stmt.get('chi_phi_thue_tndn', 0) # From Income Stmt logic
        tax_paid = cf_stmt.get('thue_tndn_da_nop_31', 0) # From Cash Flow
        delta_itp = tax_expense - tax_paid
    
    # 5. Depreciation - QNS Finding: Not on Income Statement Face
    # Extract from Cash Flow (Code 03) or Notes (Thuyết minh 13/14)
    depreciation = cf_stmt.get('khau_hao_tscd_03', 0) 
    if depreciation == 0 and 'khau_hao' in notes:
        depreciation = notes['khau_hao']
    
    # 6. Calculation
    accruals = delta_ca - delta_cash - delta_cl + delta_std - delta_itp - depreciation
    sta = accruals / bs_prior['tong_tai_san_200']
    return sta
```

### 2.2 Scaled Net Operating Assets (SNOA)
**Purpose:** Detect balance sheet bloating.

**Formula:**
```python
SNOA = (OperatingAssets - OperatingLiabilities) / TotalAssets
```

**Vietnamese Implementation:**
```python
def calculate_SNOA_vn(bs):
    # Operating Assets = Total Assets - Cash
    op_assets = bs['tong_tai_san_200'] - bs['tien_tuong_duong_tien_110']
    
    # Operating Liabilities = Total Assets - Debt - Equity
    # In Vietnam, often easier: Total Liabilities - Debt
    total_debt = (bs.get('vay_ngan_han_341', 0) + bs.get('vay_dai_han_342', 0) + 
                  bs.get('no_thue_tc_343', 0))
    op_liabilities = bs['tong_no_phai_tra'] - total_debt
    
    snoa = (op_assets - op_liabilities) / bs['tong_tai_san_200']
    return snoa
```

### 2.3 PROBM Model (Probability of Manipulation)
**Purpose:** Comprehensive fraud score. Requires Notes parsing.

**Variable Adaptations:**
| Variable | Vietnam Data Source | Calculation Note |
| :--- | :--- | :--- |
| **DSRI** | Receivables (131) / Revenue (10) | Use Net Receivables (after provision 229) |
| **GMI** | Gross Margin | (Revenue 10 - COGS 11) / Revenue 10 |
| **AQI** | Asset Quality | 1 - ((PPE 211+212 + Current Assets 100) / Total Assets 200) |
| **SGI** | Sales Growth | Revenue (t) / Revenue (t-1) |
| **DEPI** | Depreciation Rate | Depreciation / (Depreciation + Gross PPE). **Get Dep from Notes.** |
| **SGAI** | SG&A Index | (Selling 641 + Admin 642) / Revenue. **Combined in VN.** |
| **LVGI** | Leverage Index | (Debt 341+342+343) / Total Assets |
| **TATA** | Total Accruals | (Net Income - CFO) / Total Assets. **CFO from Cash Flow Stmt.** |

**Implementation Warning:** If Depreciation or Tax details are missing from Notes (common in smaller caps), flag the stock as `DATA_QUALITY_LOW` and exclude from PROBM calculation.

### 2.4 PFD Model (Probability of Financial Distress)
**Purpose:** Predict bankruptcy within 12 months.
**Critical Requirement:** Requires **Market Data** (Price, Market Cap, Volatility) which is NOT in the Financial Report.

**Formula:**
```python
LPFD = -20.26×NIMTAAVG + 1.42×TLMTA - 7.13×EXRETAVG + 1.41×SIGMA 
       - 0.045×RSIZE - 2.13×CASHMTA + 0.075×MB - 0.058×PRICE - 9.16
PFD = 1 / (1 + exp(-LPFD))
```

**Vietnam Data Integration:**
```python
def calculate_PFD_vn(fs_data, market_data):
    # 1. Book Values from Financials (VAS)
    book_equity = fs_data['von_chu_so_huu_400']
    book_debt = fs_data['tong_no_phai_tra'] # Or sum 341+342+343
    mta = book_equity + book_debt # Market Value of Total Assets Approximation
    
    # 2. Market Data from API (HOSE/HNX)
    market_cap = market_data['market_cap_vnd']
    stock_price = market_data['close_price_vnd']
    volatility_3m = market_data['volatility_annualized'] # StdDev * sqrt(252)
    vnindex_return = market_data['vnindex_return_12m']
    stock_return = market_data['stock_return_12m']
    total_market_cap = market_data['total_market_cap_hose_hnx']
    
    # 3. Variables
    nimta = fs_data['loi_nuan_sau_thue'] / mta
    tlmta = book_debt / mta
    exret = stock_return - vnindex_return
    rsize = np.log(market_cap / total_market_cap)
    cashmta = fs_data['tien_tuong_duong_tien_110'] / mta
    mb = mta / (book_equity + 0.1 * (market_cap - book_equity))
    price = np.log(min(stock_price, 375000)) # Cap at ~$15 USD equivalent
    
    # 4. Calculate LPFD & PFD
    # ... (Apply formula)
    return pfd
```

---

## 💎 Part 3: Quality Metrics (Franchise & Strength)

### 3.1 Franchise Power (Long-Term Quality)
*Use 8-year geometric averages where data allows. For Vietnam, 5 years is often more realistic due to data history.*

| Metric | Formula | Vietnam Adaptation |
| :--- | :--- | :--- |
| **ROA** | Net Income / Total Assets | Lợi nhuận sau thuế (420) / Tổng tài sản (200) |
| **ROC** | EBIT / (Net Fixed Assets + NWC) | See Detailed Calculation Below |
| **CFOA** | 8yr Sum(FCF) / Total Assets | FCF = CFO (20) - Capex (211 in Investing CF) |
| **Margin Max** | Max(Margin Growth, Margin Stability) | Gross Margin = (10 - 11) / 10 |

**ROC (Greenblatt) Vietnam Calculation:**
```python
def calculate_ROC_vn(income_stmt, balance_sheet, notes):
    # 1. EBIT Approximation
    # Net Income + Tax + Interest
    net_income = income_stmt['loi_nuan_sau_thue_60']
    tax_expense = income_stmt['chi_phi_thue_tndn_29']
    
    # Interest Extraction (Critical Step)
    # Check Notes for "Chi phí lãi vay"
    if 'chi_phi_lai_vay' in notes:
        interest = notes['chi_phi_lai_vay']
    else:
        # Fallback: 80% of Financial Expenses (22)
        interest = income_stmt.get('chi_phi_tai_chinh_22', 0) * 0.80
        
    ebit = net_income + tax_expense + interest
    
    # 2. Capital
    # Net Fixed Assets (211 + 212 - 214) + Net Working Capital (100 - 300)
    # Note: Exclude Cash from NWC
    gross_ppe = balance_sheet.get('ts_cdh_huu_hinh_nguyen_gia', 0)
    accum_dep = balance_sheet.get('hao_mon_luy_ke', 0)
    net_ppe = gross_ppe - accum_dep
    
    current_assets = balance_sheet['tai_san_ngan_han_100']
    current_liab = balance_sheet['no_ngan_han_300']
    cash = balance_sheet['tien_tuong_duong_tien_110']
    
    nwc = (current_assets - cash) - current_liab
    capital = net_ppe + nwc
    
    return ebit / capital if capital > 0 else 0
```

### 3.2 Financial Strength Score (FS_SCORE)
*Adapted from Piotroski F_SCORE. Range 0-10.*

**Vietnam Specific Adjustments:**
1.  **Equity Issuance (NEQISS):** Share buybacks are rare in Vietnam. Focus on **Capital Increases** (Tăng vốn điều lệ).
2.  **Leverage:** Include Finance Lease Liabilities (343).
3.  **Margins:** Use Gross Margin (Biên gộp) as Operating Margin data is less standardized.

**Scoring Logic:**
```python
def calculate_FS_SCORE_vn(curr, prior, cash_flow):
    score = 0
    
    # 1. Profitability
    if curr['loi_nuan_sau_thue'] > 0: score += 1 # ROA > 0
    fcf = cash_flow['luu_chuyen_tien_tu_hdkd_20'] - abs(cash_flow['dau_tu_tscd_211'])
    if fcf / curr['tong_tai_san'] > 0: score += 1 # FCF > 0
    if cash_flow['luu_chuyen_tien_tu_hdkd_20'] > curr['loi_nuan_sau_thue']: score += 1 # CFO > NI
    
    # 2. Stability
    lever_curr = (curr['vay_341'] + curr['vay_342']) / curr['tong_tai_san']
    lever_prior = (prior['vay_341'] + prior['vay_342']) / prior['tong_tai_san']
    if lever_curr < lever_prior: score += 1 # Leverage Decreased
    
    liquid_curr = curr['tai_san_ngan_han'] / curr['no_ngan_han']
    liquid_prior = prior['tai_san_ngan_han'] / prior['no_ngan_han']
    if liquid_curr > liquid_prior: score += 1 # Liquidity Improved
    
    # Equity Issuance (Check Change in Contributed Capital 411)
    equity_change = curr['von_gop_chu_so_huu'] - prior['von_gop_chu_so_huu']
    if equity_change <= 0: score += 1 # No Dilution (Rare in VN, but possible)
    
    # 3. Operational Improvement
    if (curr['loi_nuan_sau_thue']/curr['tong_tai_san']) > (prior['loi_nuan_sau_thue']/prior['tong_tai_san']): score += 1
    if fcf/curr['tong_tai_san'] > prior_fcf/prior['tong_tai_san']: score += 1
    if (curr['doanh_thu'] - curr['gia_von'])/curr['doanh_thu'] > prior_margin: score += 1
    if (curr['doanh_thu']/curr['tong_tai_san']) > (prior['doanh_thu']/prior['tong_tai_san']): score += 1
    
    return score
```

---

## 💰 Part 4: Price Metrics

### 4.1 Total Enterprise Value (TEV) - Vietnam Adapted
**Formula:**
```python
TEV = Market Cap + Total Debt - Excess Cash + Minority Interest + Preferred Equity
```

**Vietnam Calculation:**
```python
def calculate_TEV_vn(market_data, balance_sheet):
    market_cap = market_data['market_cap_vnd']
    
    # Total Debt (Include Finance Leases 343)
    total_debt = (balance_sheet.get('vay_ngan_han_341', 0) + 
                  balance_sheet.get('vay_dai_han_342', 0) + 
                  balance_sheet.get('no_thue_tc_343', 0))
    
    # Excess Cash (Conservative: All Cash is Excess)
    excess_cash = balance_sheet.get('tien_tuong_duong_tien_110', 0)
    
    # Minority Interest (Often missing in Separate FS, check Consolidated)
    minority_interest = balance_sheet.get('loi_ich_co_dong_thieu_so', 0)
    
    tev = market_cap + total_debt - excess_cash + minority_interest
    return max(tev, 1)
```

### 4.2 Primary Price Ratio: EBIT/TEV
*The "Official Winner" from the book, adapted for Vietnam.*

```python
def calculate_EBIT_TEV_vn(income_stmt, notes, tev):
    # EBIT Calculation (Same as ROC section)
    net_income = income_stmt['loi_nuan_sau_thue']
    tax = income_stmt['chi_phi_thue_tndn']
    interest = notes.get('chi_phi_lai_vay', income_stmt.get('chi_phi_tai_chinh', 0) * 0.8)
    
    ebit = net_income + tax + interest
    return ebit / tev
```

---

## 📡 Part 5: Market Signals (Vietnam Context)

### 5.1 Net Equity Issuance (Buyback Signal)
*Reality Check:* Share buybacks are rare in Vietnam. Capital increases (Issuance) are common.
*Signal:* **Negative** Net Issuance is a strong buy signal (rare). **Positive** Net Issuance is a neutral/negative signal (common).

```python
def calculate_NEQISS_vn(equity_section_curr, equity_section_prior):
    # Track Vốn điều lệ (411) + Thặng dư vốn cổ phần (412)
    equity_curr = equity_section_curr['von_dieu_le'] + equity_section_curr['thang_du_von']
    equity_prior = equity_section_prior['von_dieu_le'] + equity_section_prior['thang_du_von']
    
    # Track Treasury Stock (Cổ phiếu quỹ - 419) if any
    treasury_curr = equity_section_curr.get('co_phieu_quy', 0)
    treasury_prior = equity_section_prior.get('co_phieu_quy', 0)
    
    net_issuance = (equity_curr - equity_prior) - (treasury_curr - treasury_prior)
    return net_issuance
```

### 5.2 Insider Trading (Người nội bộ)
*Data Source:* HOSE/HNX Disclosure Portal (Thông tin giao dịch của người nội bộ).
*Signal:* Focus on **Board Members (HĐQT)** and **Major Shareholders (Cổ đông lớn >5%)**.
*Limitation:* Data is often delayed or incomplete compared to US SEC Form 4. Use as a secondary corroborative signal only.

### 5.3 Vietnam-Specific Validation Flags
*Add these checks to filter out "Value Traps" specific to Vietnam.*

```python
def validate_vietnam_flags(company_data, notes):
    flags = []
    
    # 1. Related Party Transactions (Giao dịch với các bên liên quan)
    # QNS Finding: High exposure can distort earnings
    rpt_volume = notes.get('tong_giao_dich_ben_lien_quan', 0)
    revenue = company_data['doanh_thu_thuan']
    if rpt_volume > revenue * 0.30:
        flags.append('HIGH_RELATED_PARTY_RISK')
        
    # 2. State Ownership (Sở hữu nhà nước)
    # SOEs may prioritize employment over shareholder value
    if company_data.get('state_ownership_pct', 0) > 0.50:
        flags.append('STATE_OWNED_ENTERPRISE')
        
    # 3. Seasonality (Tính thời vụ)
    # QNS Finding: Sugar harvest is Oct-May. Annual reports smooth this, but quarterly data spikes.
    if company_data['industry'] in ['Sugar', 'Agriculture', 'Seafood']:
        flags.append('SEASONAL_BUSINESS')
        
    # 4. Science & Technology Fund (Quỹ KH&CN)
    # QNS Finding: Can distort Tax and Equity calculations
    if notes.get('quy_phat_trien_khcn', 0) > 0:
        flags.append('KHCN_FUND_ACTIVE')
        
    return flags
```

---

## 🎯 Part 6: Final Quantitative Value Score (QV_SCORE)

### 6.1 Scoring Algorithm
```python
def calculate_QV_SCORE_vn(stock_id, financial_db, market_db):
    # 1. Risk Filter (Pass/Fail)
    sta = calculate_STA_vn(...)
    probm = calculate_PROBM_vn(...)
    pfd = calculate_PFD_vn(...)
    
    # Exclude top 5% risk
    if sta > percentile_95 or probm > 0.05 or pfd > 0.05:
        return {'score': 0, 'status': 'EXCLUDED_RISK'}
    
    # 2. Price Score (50% Weight)
    # Rank EBIT/TEV against universe
    ebit_tev = calculate_EBIT_TEV_vn(...)
    price_rank = percentile_rank(ebit_tev, universe='VN_STOCKS')
    
    # 3. Quality Score (50% Weight)
    # Franchise (25%)
    roa_8yr = calculate_8yr_ROA_vn(...)
    roc_8yr = calculate_8yr_ROC_vn(...)
    franchise_rank = percentile_rank(average(roa_8yr, roc_8yr))
    
    # Financial Strength (25%)
    fs_score = calculate_FS_SCORE_vn(...)
    fs_rank = fs_score / 10.0 # Normalize 0-10 to 0-1
    
    quality_rank = 0.5 * franchise_rank + 0.5 * fs_rank
    
    # 4. Final Score
    qv_score = 0.5 * price_rank + 0.5 * quality_rank
    
    return {
        'qv_score': qv_score,
        'price_rank': price_rank,
        'quality_rank': quality_rank,
        'flags': validate_vietnam_flags(...)
    }
```

---

## 🛠 Part 7: Implementation Checklist for Vietnam

### Phase 1: Data Pipeline (Months 1-3)
- [ ] **API Integration:** Connect to FiinGroup, Vietstock, or Scrape HOSE/HNX portals.
- [ ] **Parser Development:** Build PDF/Excel parsers for **Notes (Thuyết minh)** to extract Depreciation, Interest, and Tax details (Critical for PROBM/ROC).
- [ ] **Database Schema:** Design tables matching VAS Codes (111, 131, 341, etc.).
- [ ] **Consolidation Logic:** Ensure ability to switch between Separate (Riêng) and Consolidated (Hợp nhất) reports.

### Phase 2: Metric Calculation (Months 4-6)
- [ ] **Implement STA/SNOA:** Test against known fraud cases in Vietnam (if any public data exists).
- [ ] **Implement FS_SCORE:** Validate against bankrupt/delisted companies (e.g., those removed from HOSE/HNX).
- [ ] **Market Data Sync:** Ensure price data aligns with financial report release dates (avoid look-ahead bias).

### Phase 3: Backtesting & Validation (Months 7-9)
- [ ] **Universe Definition:** Exclude Financials (Banks/Insurance) initially (different accounting).
- [ ] **Liquidity Filter:** Minimum avg daily volume > 1 Billion VND (avoid illiquid traps).
- [ ] **Look-Ahead Bias Control:** Assume financials available 45 days after quarter end (per VAS regulations).
- [ ] **Paper Trading:** Run model for 3 months live before deploying capital.

### Phase 4: Live Deployment
- [ ] **Rebalance Frequency:** Quarterly (aligns with VN reporting cycle).
- [ ] **Position Sizing:** Max 5% per stock (Vietnam market can be volatile).
- [ ] **Manual Review:** Check `validate_vietnam_flags` output before buying (e.g., check Related Party transactions manually).

---

## ⚠️ Important Vietnam-Specific Warnings

1.  **Data Quality:** Vietnamese financial reporting has improved but inconsistencies remain. **Always cross-check Notes.**
2.  **Look-Ahead Bias:** Financial statements are legally due 45 days (quarterly) to 90 days (annual) after period end. Do not assume data is available on Day 1.
3.  **Liquidity:** Many small-cap "value" stocks in Vietnam are illiquid. Enforce volume filters strictly.
4.  **State Ownership:** SOEs (Doanh nghiệp Nhà nước) may not maximize shareholder value. Treat governance signals with caution.
5.  **Seasonality:** Agricultural/Commodity firms (like QNS) have seasonal cash flows. Use TTM (Trailing Twelve Months) data where possible.
6.  **Circular 200 vs. 245:** Banks use Circular 245. Exclude Banks from this specific framework unless you build a separate bank model.

---

## 📑 Appendix: Sample Python Class Structure

```python
class VietnamQuantValue:
    def __init__(self, ticker, financial_db, market_db):
        self.ticker = ticker
        self.fin_db = financial_db
        self.mkt_db = market_db
        
    def get_financials(self, year):
        # Retrieve VAS mapped data
        return self.fin_db.get_report(self.ticker, year, type='consolidated')
    
    def get_market_data(self, date):
        # Retrieve Price, Vol, Market Cap
        return self.mkt_db.get_snapshot(self.ticker, date)
    
    def calculate_all_metrics(self):
        # Run all formulas from Parts 2, 3, 4
        pass
    
    def generate_signal(self):
        # Return Buy/Sell/Hold based on QV_SCORE
        pass
```

This framework provides the complete, updated roadmap for building your Quantitative Value app in Vietnam, incorporating the real-world constraints and data structures found in the QNS financial report.

