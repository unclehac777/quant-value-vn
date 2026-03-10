Below is an **adapted specification for the Vietnam equity market (HOSE, HNX, UPCOM)**. The core structure remains the same as in **Quantitative Value: A Practitioner’s Guide to Automating Intelligent Investment and Eliminating Behavioral Errors**, but adjustments are required because Vietnam has:

* different liquidity characteristics
* quarterly financial reporting
* frequent share issuances
* accounting differences (VAS vs IFRS)
* sector concentration in banks and real estate
* less reliable historical data

The goal is to **make the system robust for Vietnamese datasets**.

---

# Vietnam Quantitative Value System Specification

Markets covered

```
HOSE
HNX
UPCOM
```

Financial reporting frequency

```
quarterly + annual
```

Minimum history required

```
5 years (preferred)
3 years minimum fallback
```

Vietnam data rarely provides **8 clean years**.

---

# 0. Required Data Inputs (Vietnam Mapping)

### Market Data

```
ticker
exchange
market_cap
share_price
shares_outstanding
avg_daily_value_20d
trading_days_last_60
```

Vietnam-specific:

```
free_float_ratio
foreign_room
```

Optional but useful.

---

# 1. Universe Filter (Vietnam)

Vietnam requires **strong liquidity filtering**.

### 1.1 Exchange Filter

```
exchange in ["HOSE","HNX","UPCOM"]
```

---

### 1.2 Market Capitalization

```
market_cap = share_price * shares_outstanding
```

Requirement

```
market_cap >= 500B VND
```



UPCOM contains many illiquid microcaps.

---

### 1.3 Liquidity Filter

```
ADV20 = avg_daily_value_20d
```

Requirement

```
ADV20 >= 5B VND
```

Additional trading frequency check

```
trading_days_last_60 >= 50
```

Removes suspended stocks.

---

### 1.4 Sector Exclusions

Vietnam banks distort value metrics.

Exclude sectors:

```
banks
securities
insurance
financial services
real estate developers
utilities
```

Reason:

* high leverage
* regulatory capital structures
* EV meaningless

---

# 2. Risk Screens

---

# 2.1 Beneish M-Score (unchanged)

Vietnam manipulation risk is **higher**, so this screen is important.

Formula remains identical.

Filter

```
M <= -1.78
```


---

# 2.2 Altman Z-Score (Adjusted)

Vietnam companies often have high leverage.

Standard formula:

```
Z =
1.2*X1 +
1.4*X2 +
3.3*X3 +
0.6*X4 +
1.0*X5
```

Variables

```
X1 = working_capital / total_assets
X2 = retained_earnings / total_assets
X3 = ebit / total_assets
X4 = market_cap / total_liabilities
X5 = revenue / total_assets
```

Vietnam filter

```
Z > 1.5
```


---

# 2.3 Piotroski F-Score

Unchanged formula.

Signals:

```
9 signals
range 0–9
```

Vietnam filter

```
F_SCORE >= 5
```


---

# 3. Quality Metrics

Historical window:

```
5 years
```

instead of 8.

---

# 3.1 Return on Assets

```
ROA =
net_income / total_assets
```

Metric

```
ROA_5yr_avg
```

---

# 3.2 Return on Capital

Vietnam adjustment:

Working capital often negative.

Use:

```
invested_capital =
net_working_capital +
ppe
```

Where

```
net_working_capital =
current_assets - current_liabilities
```

NOPAT

```
NOPAT = ebit * (1 - tax_rate)
```

ROC

```
ROC =
NOPAT / invested_capital
```

Metric

```
ROC_5yr_avg
```

---

# 3.3 Free Cash Flow to Assets

```
FCF =
operating_cash_flow - capital_expenditures
```

```
FCF_assets =
FCF / total_assets
```

Metric

```
FCF_assets_5yr_avg
```

---

# 3.4 Gross Margin

```
gross_margin =
(revenue - cogs) / revenue
```

Metric

```
GM_5yr_avg
```

---

# 3.5 Gross Margin Growth

```
GM_growth =
gross_margin_t - gross_margin_t-5
```

---

# 3.6 Gross Margin Stability

```
GM_stability =
std_dev(gross_margin_5yr_series)
```

Lower = better.

---

# 4. Price Ranking

---

# 4.1 Enterprise Value

Vietnam adjustment:

Many companies have **large cash positions**.

Formula

```
EV =
market_cap
+ short_term_debt
+ long_term_debt
+ minority_interest
+ preferred_stock
- cash_and_equivalents
```

Optional guard

```
EV >= 0
```

---

# 4.2 Operating Earnings

```
operating_earnings = ebit
```

Exclude companies where

```
ebit <= 0
```

---

# 4.3 Acquirer’s Multiple

```
AM =
EV / ebit
```

Constraint

```
AM <= 50
```

Outlier cap.

---

# 5. Composite Ranking

Quality score

Rank by

```
ROA_5yr_avg
ROC_5yr_avg
FCF_assets_5yr_avg
GM_stability
```

Combine

```
QUALITY_SCORE
```

---

Value score

```
VALUE_SCORE = rank(AM)
```

Lower AM = higher rank.

---

Signals score

```
SIGNAL_SCORE = rank(F_SCORE)
```

---

Composite score

```
COMPOSITE =
rank(VALUE_SCORE)
+
rank(QUALITY_SCORE)
+
rank(SIGNAL_SCORE)
```

Sort ascending.

---

# 6. Portfolio Construction

### Selection

```
select lowest AM decile
```

Typical Vietnam universe

```
250–350 stocks
```

Final portfolio

```
top 25–30 stocks
```

---

### Weighting

```
equal_weight
```

---

### Position Size

```
weight = 1 / portfolio_size
```

Example

```
30 stocks → 3.33%
```

---

### Rebalance

Vietnam liquidity suggests

```
rebalance_frequency = 12 months
```

Alternative

```
6 months
```

---

# 7. Execution Pipeline

Vietnam optimized pipeline

```
1. load tickers from HOSE/HNX/UPCOM
2. apply exchange filter
3. apply liquidity filters
4. apply sector exclusions
5. download financial statements
6. compute Beneish M-score
7. compute Altman Z-score
8. compute Piotroski F-score
9. remove risky companies
10. compute quality metrics
11. compute enterprise value
12. compute Acquirer’s Multiple
13. build composite ranking
14. select top decile
15. construct equal weight portfolio
16. rebalance annually
```

---

# Important Vietnam Implementation Issues

A working system must also handle:

### 1. Stock dilution

Vietnam companies frequently issue shares.

Track

```
shares_outstanding_change
```

---

### 2. Trading suspensions

Remove stocks if

```
trading_days_last_60 < 50
```

---



### 4. Accounting differences

Vietnam uses **VAS accounting**, so:

* minority interest inconsistent
* depreciation sometimes understated
* cash flow statements noisy

---

# Result

Typical universe

```
~1600 tickers
```

After filters

```
250–350 investable
```

Final portfolio

```
25–30 stocks
```

Expected holding period

```
12 months
```

---

