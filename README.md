# Vietnam Quantitative Value Stock Screener

A Python stock screener + **Streamlit dashboard** implementing Tobias Carlisle & Wesley Gray's **"Quantitative Value"** framework, adapted for Vietnam (HOSE + HNX + UpCom). Data persisted in **Supabase** (PostgreSQL).

## Features

- 📊 **Interactive dashboard** — sortable tables, filters, CSV export
- 📈 **Charts** — Value vs Quality scatter, distributions, radar comparison, correlation matrix
- 📅 **Historical tracking** — compare screening runs over time, track rank changes
- ⭐ **Watchlist & Portfolio** — save favourites, track buy price / P&L
- 🔄 **Run screener from UI** — configure and run directly in the browser
- 💾 **Supabase persistence** — all data stored in cloud PostgreSQL
- 🚀 **Momentum Overlay** — Avoid falling knives with 2-12 month return momentum filter

## Data Source

All financial data scraped from **CafeF.vn** — free, no API key needed.

| Data | CafeF Endpoint |
|------|---------------|
| Income Statement | `/bao-cao-tai-chinh/{ticker}/IncSta/{year}/0/0/0/...` |
| Balance Sheet | `/bao-cao-tai-chinh/{ticker}/BSheet/{year}/0/0/0/...` |
| Stock Price | `/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol=...` |

## Setup

### 1. Install dependencies

```bash
cd stock_screener
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure Supabase

1. Create a free project at [supabase.com](https://supabase.com)
2. Go to **SQL Editor** and run the contents of `quant_value_vn/database/schema.sql`
3. Copy your project URL and anon/service key from **Settings → API**
4. Create `.env`:

```bash
cp .env.example .env
# Edit .env with your Supabase credentials
```

### Dependencies

- `requests`, `beautifulsoup4`, `lxml` — Web scraping
- `pandas`, `numpy` — Data processing
- `streamlit`, `plotly` — Dashboard & charts
- `supabase`, `python-dotenv` — Database

## Usage



```bash
python -m quant_value_vn.pipeline.run_pipeline

streamlit run quant_value_vn/dashboard/streamlit_app.py

uvicorn quant_value_vn.app.main:app --host 0.0.0.0 --port 8000

```

Opens at `http://localhost:8501` with all features: view results, run screener, charts, watchlist, historical comparison.



### Auto-Scheduler

The `scheduler.py` script is not yet implemented. Consider using GitHub Actions or a cron job for automation.

## Output

- **Console**: Top 30 stocks ranked by combined Value + Quality
- **CSV file**: `vietnam_value_screen_YYYY-MM-DD.csv`

### Key Output Columns

| Column | Description |
|--------|-------------|
| `ticker` | Stock symbol |
| `combined_rank` | Final ranking (1 = best) within the cheapest 40% subset |
| `acquirers_multiple` | EV / EBIT (lower = cheaper) |
| `quality_score` | Normalised quality 0-100 (higher = better) |
| `piotroski_fscore` | Financial strength signal (0-9) |
| `market_cap_B` | Market cap in billion VND |
| `pe` | Price / Earnings |
| `pb` | Price / Book |
| `ROA_5yr_avg` | 5-Year Average Return on Assets |
| `ROC_5yr_avg` | 5-Year Average Return on Capital |
| `FCF_assets_5yr_avg` | 5-Year Avg Free Cash Flow to Assets |
| `GM_stability` | 5-Year Gross Margin Standard Deviation (Lower = better) |

## Methodology (Tobias Carlisle & Wesley Gray specs)

### 1. Enterprise Value (EV)
```
EV = Market Cap + Total Debt + Minority Interest + Preferred Stock − Cash & Equivalents
```
- Market Cap = Latest Price × Shares Outstanding
- Strictly guards against negative EV values.

### 2. The Value Screen (Acquirer's Multiple)
```
AM = EV / EBIT
```
- **The Cutoff:** The screener evaluates all investable stocks and strictly slices the universe down to the **cheapest 40%** by Acquirer's Multiple. All remaining stocks are discarded regardless of quality.

### 3. Quality & Fraud Screens
Applied *only* to the cheapest 40% subset to rank the final portfolio:
- **Franchise Power (Quality):** Ranked by `ROA_5yr_avg`, `ROC_5yr_avg`, `FCF_assets_5yr_avg`, and `GM_stability`.
- **Financial Strength (Signals):** Ranked by the **Piotroski F-Score** (must be ≥ 5).
- **Fraud Detection:** Companies with a **Beneish M-Score > -1.78** are eliminated.
- **Distress Risk:** Companies with an **Altman Z-Score < 1.5** are eliminated.

### 4. Combined Ranking
```
Combined Rank = rank(Quality Rank + Signal Rank)
```
- The cheapest 40% bucket is re-sorted internally by the intersection of their 5-year Quality and F-Score Signal rankings. Rank 1 = Best combination of cheapness, quality, and safety.

### 5. Momentum Overlay
```
Momentum = 2-12 month return
```
- Excludes stocks with negative momentum to avoid catching falling knives.

## Filters

| Filter | Threshold | Purpose |
|--------|-----------|---------|
| EBIT | > 0 | Exclude unprofitable companies |
| Average Daily Volume | ≥ 5B VND | Strict Vietnam Liquidity check |
| Trading Frequency | ≥ 50/60 days | Exclude suspended/illiquid stocks |
| Market Cap | ≥ 500B VND | Exclude micro-caps |
| Sector | Excluded | Removes Financials, Real Estate, Utilities |
| Beneish M-Score | ≤ -1.78 | Eliminate earnings manipulation |
| Altman Z-Score | > 1.5 | Eliminate bankruptcy risk |
| Piotroski F-Score| ≥ 5 | Ensure baseline financial health |

## Interpretation Guide

| Acquirer's Multiple | Reading |
|--------------------|---------|
| < 5 | Very cheap |
| 5–10 | Fair value |
| 10–15 | Moderate |
| > 15 | Expensive |

| Quality Score | Reading |
|---------------|---------|
| > 70 | High quality |
| 50–70 | Average |
| < 50 | Low quality |

## Notes

- **Coverage**: ~130 curated HOSE + HNX stocks (banks may be skipped due to different financial statement format)
- **Rate limiting**: 0.5s delay between HTTP requests (~2s per stock for 3 requests)
- **Runtime**: Full scan ≈ 4-5 min, quick mode ≈ 1 min
- **Year selection**: Tries latest annual report first, falls back to prior year
- **Number format**: Vietnamese financial numbers use dots as thousand separators (e.g. `61.012.074.147.764` = 61 trillion VND)

## Disclaimer

This tool is for **educational and research purposes only**. It is NOT financial advice. Always conduct your own due diligence before making investment decisions. Past performance does not guarantee future results.

## References

- Carlisle, T. & Gray, W. (2012). *Quantitative Value: A Practitioner's Guide to Automating Intelligent Investment and Eliminating Behavioral Errors*
- Greenblatt, J. (2005). *The Little Book That Beats the Market*
- Carlisle, T. (2014). *Deep Value*

## License

MIT License — Free for personal and commercial use.
