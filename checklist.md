Below is a **clean pseudo-code specification** for implementing the **Acquirer’s Multiple quantitative system** from
Quantitative Value: A Practitioner’s Guide to Automating Intelligent Investment and Eliminating Behavioral Errors.

The syntax is written in **Python-style pseudocode** so tools like Copilot can easily translate it into production code.
Adjustments are included for the **Vietnam stock market (HOSE, HNX, UPCOM)**.

---

# 0. Global Constants (Vietnam Adaptation)

```python
# Exchanges
EXCHANGES = ["HOSE", "HNX", "UPCOM"]

# Universe filters
MIN_MARKET_CAP = 500_000_000_000      # 500B VND
MIN_ADV20 = 5_000_000_000             # 5B VND daily trading value
MIN_TRADING_DAYS = 50                 # last 60 days

# Risk thresholds
BENEISH_THRESHOLD = -1.75
ALTMAN_THRESHOLD = 1.5
MIN_FSCORE = 6

# Portfolio
PORTFOLIO_SIZE = 30
REBALANCE_MONTH = 7        # July rebalance
DATA_LAG_DAYS = 150        # 6-month lag

# Sector exclusions (Vietnam)
EXCLUDED_SECTORS = [
    "banks",
    "insurance",
    "securities",
    "financial services",
    "real estate",
    "utilities"
]
```

---

# 1. Core Valuation Metric — Acquirer’s Multiple

```python
def operating_earnings(financials):
    """
    Strict EBIT formula per Quantitative Value.
    """
    EBIT = (
        financials["revenue"]
        - financials["cogs"]
        - financials["sga"]
        - financials["depreciation"]
        - financials["amortization"]
    )

    return EBIT

def enterprise_value(market_cap,
                     short_term_debt,
                     long_term_debt,
                     preferred_stock,
                     minority_interest,
                     cash):

    total_debt = short_term_debt + long_term_debt

    EV = (
        market_cap
        + total_debt
        + preferred_stock
        + minority_interest
        - cash
    )

    return EV
```

```python
def acquirers_multiple(EV, EBIT):

    if EBIT <= 0:
        return None

    AM = EV / EBIT

    return AM
```

---

# 2. Beneish M-Score (Fraud Risk)

```python
def beneish_m_score(data_t, data_t1):

    DSRI = (data_t["receivables"] / data_t["sales"]) / \
           (data_t1["receivables"] / data_t1["sales"])

    gross_margin_t = (data_t["sales"] - data_t["cogs"]) / data_t["sales"]
    gross_margin_t1 = (data_t1["sales"] - data_t1["cogs"]) / data_t1["sales"]

    GMI = gross_margin_t1 / gross_margin_t

    AQI = (
        (1 - (data_t["current_assets"] + data_t["ppe"]) / data_t["total_assets"])
        /
        (1 - (data_t1["current_assets"] + data_t1["ppe"]) / data_t1["total_assets"])
    )

    SGI = data_t["sales"] / data_t1["sales"]

    DEPI = (
        (data_t1["depreciation"] /
         (data_t1["ppe"] + data_t1["depreciation"]))
        /
        (data_t["depreciation"] /
         (data_t["ppe"] + data_t["depreciation"]))
    )

    SGAI = (data_t["sga"] / data_t["sales"]) / \
           (data_t1["sga"] / data_t1["sales"])

    TATA = (
        (data_t["net_income"] - data_t["operating_cash_flow"])
        /
        data_t["total_assets"]
    )

    debt_t = data_t["short_term_debt"] + data_t["long_term_debt"]
    debt_t1 = data_t1["short_term_debt"] + data_t1["long_term_debt"]

    LVGI = (debt_t / data_t["total_assets"]) / \
           (debt_t1 / data_t1["total_assets"])

    M = (
        -4.54
        + 0.92 * DSRI
        + 0.525 * GMI
        + 0.404 * AQI
        + 0.592 * SGI
        + 0.115 * DEPI
        - 0.172 * SGAI
        + 4.679 * TATA
        - 0.327 * LVGI
    )

    return M
```

Filter rule:

```python
if M > BENEISH_THRESHOLD:
    reject_stock()
```

---

# 3. Altman Z-Score (Bankruptcy Risk)

```python
def altman_z_score(data, market_cap):

    working_capital = data["current_assets"] - data["current_liabilities"]

    X1 = working_capital / data["total_assets"]
    X2 = data["retained_earnings"] / data["total_assets"]
    X3 = data["ebit"] / data["total_assets"]
    X4 = market_cap / data["total_liabilities"]
    X5 = data["sales"] / data["total_assets"]

    Z = (
        1.2 * X1 +
        1.4 * X2 +
        3.3 * X3 +
        0.6 * X4 +
        1.0 * X5
    )

    return Z
```

Filter:

```python
if Z <= ALTMAN_THRESHOLD:
    reject_stock()
```

---

# 4. Piotroski F-Score

```python
def piotroski_f_score(data_t, data_t1):

    score = 0

    roa_t = data_t["net_income"] / data_t["total_assets"]
    roa_t1 = data_t1["net_income"] / data_t1["total_assets"]

    cfo = data_t["operating_cash_flow"]

    if roa_t > 0:
        score += 1

    if cfo > 0:
        score += 1

    if roa_t > roa_t1:
        score += 1

    if cfo > data_t["net_income"]:
        score += 1

    leverage_t = data_t["long_term_debt"] / data_t["total_assets"]
    leverage_t1 = data_t1["long_term_debt"] / data_t1["total_assets"]

    if leverage_t < leverage_t1:
        score += 1

    current_ratio_t = data_t["current_assets"] / data_t["current_liabilities"]
    current_ratio_t1 = data_t1["current_assets"] / data_t1["current_liabilities"]

    if current_ratio_t > current_ratio_t1:
        score += 1

    if data_t["shares_outstanding"] <= data_t1["shares_outstanding"]:
        score += 1

    gross_margin_t = (data_t["sales"] - data_t["cogs"]) / data_t["sales"]
    gross_margin_t1 = (data_t1["sales"] - data_t1["cogs"]) / data_t1["sales"]

    if gross_margin_t > gross_margin_t1:
        score += 1

    asset_turnover_t = data_t["sales"] / data_t["total_assets"]
    asset_turnover_t1 = data_t1["sales"] / data_t1["total_assets"]

    if asset_turnover_t > asset_turnover_t1:
        score += 1

    return score
```

Filter:

```python
if F_SCORE < MIN_FSCORE:
    reject_stock()
```

---

# 5. Quality Metrics

### ROA

```python
ROA = net_income / total_assets
```

Use:

```python
ROA_5yr_avg = mean(ROA over last 5 years)
```

---

### Return on Capital

```python
def return_on_capital(data):

    NWC = data["current_assets"] - data["current_liabilities"]

    NFA = data["ppe"] - data["accumulated_depreciation"]

    invested_capital = NWC + NFA

    tax_rate = data["tax_rate"]

    NOPAT = data["ebit"] * (1 - tax_rate)

    ROC = NOPAT / invested_capital

    return ROC
```

Use:

```python
ROC_5yr_avg = mean(ROC last 5 years)
```

---

### Free Cash Flow to Assets

```python
FCF = operating_cash_flow - capital_expenditures

FCF_to_assets = FCF / total_assets
```

Use:

```python
FCF_assets_5yr_avg = mean(last 5 years)
```

---

### Gross Margin Stability

```python
gross_margin = (revenue - cogs) / revenue

GM_std = std_dev(gross_margin over 5 years)
```

Lower value = better.

---

# 6. Price Metrics

### Earnings Yield

```python
earnings_yield = EBIT / EV
```

---

### Free Cash Flow Yield

```python
FCF_yield = FCF / market_cap
```

---

### Price to Earnings

```python
PE = market_cap / net_income
```

---

### EV / EBITDA

```python
EV_EBITDA = EV / EBITDA
```

---

### Price to Book

```python
PB = market_cap / shareholder_equity
```

---

# 7. Ranking System

Quality score:

```python
QUALITY_SCORE =
rank_desc(ROA_5yr_avg) +
rank_desc(ROC_5yr_avg) +
rank_desc(FCF_assets_5yr_avg) +
rank_asc(GM_std)
```

Value score:

```python
VALUE_SCORE = rank_asc(AM)
```

Signal score:

```python
SIGNAL_SCORE = rank_desc(F_SCORE)
```

Composite:

```python
COMPOSITE_SCORE =
QUALITY_SCORE +
VALUE_SCORE +
SIGNAL_SCORE
```

---

# 5. Vietnam Quant Pipeline

```python
def quant_pipeline():

    universe = load_all_stocks(EXCHANGES)

    universe = filter_market_cap(universe, MIN_MARKET_CAP)

    universe = filter_liquidity(universe, MIN_ADV20)

    universe = remove_sectors(universe, EXCLUDED_SECTORS)

    for stock in universe:

        financials = load_financials(stock, lag_days=DATA_LAG_DAYS)

        M = beneish_m_score(financials_t, financials_t1)

        if M > BENEISH_THRESHOLD:
            continue

        Z = altman_z_score(financials_t)

        if Z <= ALTMAN_THRESHOLD:
            continue

        F = piotroski_f_score(financials_t, financials_t1)

        if F < MIN_FSCORE:
            continue

        EV = enterprise_value(...)

        AM = acquirers_multiple(EV, EBIT)

        store_metrics(stock, AM, F, quality_metrics)

    ranked = rank_stocks()

    portfolio = select_top(ranked, PORTFOLIO_SIZE)

    return portfolio
```

---

# 9. Rebalance Logic

```python
def rebalance_portfolio():

    if current_month == REBALANCE_MONTH:

        portfolio = quant_pipeline()

        equal_weight(portfolio)

        execute_trades(portfolio)
```

---

If useful, the **next step for your system** is building the **Vietnam-specific data normalization layer** (this is where most quant systems break), including:

* mapping **CafeF financial fields → standardized factors**
* handling **Vietnam share dilution / bonus shares**
* correcting **UPCOM liquidity distortions**
* cleaning **Vietnam restated financial statements**.
