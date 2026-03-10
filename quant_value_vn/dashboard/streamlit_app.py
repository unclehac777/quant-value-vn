"""
Vietnam Quantitative Value Stock Screener — Streamlit Dashboard
===============================================================
Based on Tobias Carlisle & Wesley Gray's "Quantitative Value" framework.

**Architecture**: Streamlit → FastAPI (httpx) → Supabase
The dashboard NEVER touches the database directly.

Pages:
  1. Dashboard           — KPIs, top 10, Value vs Quality scatter
  2. Screening Results   — Full filterable table
  3. Factor Distribution — Charts: AM, ROA, Gross Prof, Accruals, M-Score
  4. Stock Detail        — Per-ticker factor view, ranking history
  5. Model Portfolio     — Top 30 equal-weight, sector exposure
  6. Charts & Analysis   — Scatter, correlation, comparison
  7. Historical Compare  — Run comparison, rank changes
  8. Watchlist/Portfolio  — Track holdings, P&L
  9. Run Screener        — Launch scan from UI
  10. Settings           — DB info, about

Run:  streamlit run quant_value_vn/dashboard/streamlit_app.py
"""

import sys
import os

# Ensure project root is on path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from pathlib import Path
import glob
import httpx

from quant_value_vn.config import PORTFOLIO_SIZE, API_PORT

# ── API Base URL ─────────────────────────────────────────────────────

API_BASE = f"http://localhost:{API_PORT}"

# ── Page Config ──────────────────────────────────────────────────────

st.set_page_config(
    page_title="VN Quantitative Value",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    .block-container { padding-top: 1rem; }
    div[data-testid="stMetricValue"] { font-size: 1.4rem; }
</style>
""", unsafe_allow_html=True)


# ── API helpers ──────────────────────────────────────────────────────

@st.cache_data(ttl=30)
def api_get(path: str) -> dict:
    """GET request to FastAPI backend. Returns parsed JSON."""
    try:
        r = httpx.get(f"{API_BASE}{path}", timeout=30)
        r.raise_for_status()
        return r.json()
    except httpx.HTTPError as e:
        st.error(f"API error: {e}")
        return {}


def api_post(path: str, json: dict = None) -> dict:
    try:
        r = httpx.post(f"{API_BASE}{path}", json=json, timeout=60)
        r.raise_for_status()
        return r.json()
    except httpx.HTTPError as e:
        st.error(f"API error: {e}")
        return {}


def api_delete(path: str) -> dict:
    try:
        r = httpx.delete(f"{API_BASE}{path}", timeout=15)
        r.raise_for_status()
        return r.json()
    except httpx.HTTPError as e:
        st.error(f"API error: {e}")
        return {}


def get_rankings_df() -> pd.DataFrame:
    """Fetch latest rankings from API → DataFrame."""
    data = api_get("/rankings")
    records = data.get("data", [])
    return pd.DataFrame(records) if records else pd.DataFrame()


def get_run_results_df(run_id: int) -> pd.DataFrame:
    data = api_get(f"/rankings/{run_id}")
    records = data.get("data", [])
    return pd.DataFrame(records) if records else pd.DataFrame()


def get_runs_df() -> pd.DataFrame:
    data = api_get("/runs")
    records = data.get("data", [])
    return pd.DataFrame(records) if records else pd.DataFrame()


def get_watchlist_df() -> pd.DataFrame:
    """Fetch watchlist directly without caching so UI updates instantly."""
    try:
        r = httpx.get(f"{API_BASE}/watchlist", timeout=10)
        r.raise_for_status()
        data = r.json()
        records = data.get("data", [])
        return pd.DataFrame(records) if records else pd.DataFrame()
    except httpx.HTTPError:
        return pd.DataFrame()


def get_stock_detail(ticker: str) -> dict:
    return api_get(f"/stock/{ticker}")


# ── Formatting helpers ───────────────────────────────────────────────

def safe_fmt(val, fmt_str):
    try:
        if val is None or pd.isna(val):
            return "---"
        return fmt_str.format(val)
    except (ValueError, TypeError):
        return str(val)


FMT = {
    "ebit_ev": "{:.1%}", "acquirers_multiple": "{:.2f}",
    "quality_score": "{:.0f}", "beneish_mscore": "{:.2f}", "probm": "{:.2%}",
    "market_cap_b": "{:,.0f}B", "ev_b": "{:,.0f}", "pe": "{:.1f}", "pb": "{:.2f}",
    "roa": "{:.1%}", "roe": "{:.1%}", "roic": "{:.1%}",
    "gross_profitability": "{:.1%}", "accruals": "{:.2%}",
    "cfo_to_assets": "{:.1%}", "operating_cash_flow": "{:,.0f}",
    "fcf_yield": "{:.1%}", "debt_equity": "{:.2f}",
    "gross_margin": "{:.1%}", "net_margin": "{:.1%}",
    "price": "{:,.0f}", "eps": "{:,.0f}", "ebit": "{:,.0f}", "revenue": "{:,.0f}",
    "weight": "{:.1f}%",
}


def fmt_for(cols):
    return {k: v for k, v in FMT.items() if k in cols}


# ── Sidebar ──────────────────────────────────────────────────────────

st.sidebar.title("📈 VN Quantitative Value")
page = st.sidebar.radio(
    "Navigate",
    [
        "🏠 Dashboard", "📊 Screening Results", "📉 Factor Distribution",
        "🔍 Stock Detail", "💼 Model Portfolio",
        "📈 Charts & Analysis", "📅 Historical Comparison",
        "⭐ Watchlist & Portfolio", "🔄 Run Screener", "⚙️ Settings",
    ],
    index=0,
)


# ====================================================================
# 🏠 DASHBOARD
# ====================================================================
if page == "🏠 Dashboard":
    st.title("🇻🇳 Vietnam Quantitative Value Screener")
    st.caption("Carlisle & Gray 'Quantitative Value' framework — CafeF.vn data")

    df = get_rankings_df()
    runs = get_runs_df()

    if df.empty:
        st.info("No screening data yet. Go to **🔄 Run Screener** or import a CSV.")
        csvs = sorted(glob.glob("vietnam_value_screen_*.csv"))
        if csvs:
            st.subheader("📥 Found existing CSV files")
            for csv_path in csvs:
                c1, c2 = st.columns([3, 1])
                c1.text(csv_path)
                if c2.button("Import", key=csv_path):
                    # CSV import still needs direct DB call (no API endpoint yet)
                    from quant_value_vn.database import queries as db_import
                    rid = db_import.import_csv(csv_path)
                    st.success(f"Imported as run #{rid}")
                    st.rerun()
    else:
        # KPI row
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Stocks Screened",
                   runs.iloc[0]["total_stocks"] if not runs.empty else len(df))
        c2.metric("Passed Filters",
                   runs.iloc[0]["passed_filter"] if not runs.empty else len(df))
        c3.metric("Total Runs", len(runs))

        best = df.iloc[0]
        am_val = best.get("acquirers_multiple", 0)
        c4.metric("Top Pick", best["ticker"],
                   f"AM={am_val:.1f}x" if pd.notna(am_val) else "")

        if "beneish_mscore" in df.columns:
            clean = df["beneish_mscore"].dropna()
            n_clean = (clean <= -1.78).sum()
            c5.metric("Clean (M-Score)", f"{n_clean}/{len(clean)}")
        else:
            c5.metric("Best Quality",
                       df.loc[df["quality_score"].idxmax(), "ticker"]
                       if "quality_score" in df.columns else "---")

        st.divider()

        # Top 10 table
        st.subheader("🏆 Top 10 Value Stocks")
        show_cols = [
            "ticker", "combined_rank", "acquirers_multiple", "ebit_ev",
            "quality_score", "beneish_mscore", "probm",
            "data_source_ttm", "data_year_ttm", "data_source_annual", "data_year_annual",
            "market_cap_b", "pe", "pb",
            "roa", "gross_profitability", "accruals", "cfo_to_assets", "roic", "debt_equity",
        ]
        avail = [c for c in show_cols if c in df.columns]
        st.dataframe(
            df[avail].head(10).style.format(fmt_for(avail), na_rep="---"),
            use_container_width=True, hide_index=True,
        )

        # Value vs Quality scatter
        st.subheader("💎 Value vs Quality Map")
        if "quality_score" in df.columns and "acquirers_multiple" in df.columns:
            color_col = "beneish_mscore" if "beneish_mscore" in df.columns else "quality_score"
            scale = "RdYlGn_r" if color_col == "beneish_mscore" else "RdYlGn"
            fig = px.scatter(
                df, x="acquirers_multiple", y="quality_score", text="ticker",
                size="market_cap_b" if "market_cap_b" in df.columns else None,
                color=color_col, color_continuous_scale=scale,
                labels={
                    "acquirers_multiple": "Acquirer's Multiple (lower = cheaper)",
                    "quality_score": "Quality Score (0-100)",
                },
                height=500,
            )
            fig.update_traces(textposition="top center", textfont_size=9)
            fig.add_vline(x=10, line_dash="dash", line_color="gray")
            fig.add_hline(y=50, line_dash="dash", line_color="gray")
            st.plotly_chart(fig, use_container_width=True)


# ====================================================================
# 📊 SCREENING RESULTS
# ====================================================================
elif page == "📊 Screening Results":
    st.title("📊 Screening Results")

    runs = get_runs_df()
    if runs.empty:
        st.info("No screening data. Run a screen or import CSV first.")
    else:
        run_options = {
            f"Run #{r['id']}  —  {r['run_date']}  ({r['passed_filter']} stocks)": r["id"]
            for _, r in runs.iterrows()
        }
        selected = st.selectbox("Select screening run", list(run_options.keys()))
        run_id = run_options[selected]
        df = get_run_results_df(run_id)

        if df.empty:
            st.warning("No results for this run.")
        else:
            # Sidebar filters
            st.sidebar.subheader("🔍 Filters")
            if "acquirers_multiple" in df.columns:
                am_max = float(max(df["acquirers_multiple"].max() or 50.0, 50.0))
                am_range = st.sidebar.slider("Acquirer's Multiple", 0.0, am_max, (0.0, am_max))
            else:
                am_range = (0.0, 100.0)
            qs_range = st.sidebar.slider("Quality Score (0-100)", 0, 100, (0, 100))
            max_pe = st.sidebar.number_input("Max P/E", value=999.0, step=5.0)
            mscore_filter = st.sidebar.checkbox("Only M-Score <= -1.78", value=False)

            mask = pd.Series(True, index=df.index)
            if "acquirers_multiple" in df.columns:
                mask &= df["acquirers_multiple"].between(*am_range)
            if "quality_score" in df.columns:
                mask &= df["quality_score"].between(*qs_range)
            if "pe" in df.columns:
                mask &= df["pe"].fillna(0) <= max_pe
            if mscore_filter and "beneish_mscore" in df.columns:
                mask &= df["beneish_mscore"].isna() | (df["beneish_mscore"] <= -1.78)

            filtered = df[mask]
            st.caption(f"Showing {len(filtered)} / {len(df)} stocks")

            display_cols = [
                "ticker", "combined_rank", "acquirers_multiple", "ebit_ev",
                "quality_score", "beneish_mscore", "probm",
                "data_source_ttm", "data_year_ttm", "data_source_annual", "data_year_annual",
                "market_cap_b", "ev_b", "ebit", "revenue",
                "pe", "pb", "roa", "roe", "roic",
                "gross_profitability", "accruals", "cfo_to_assets",
                "operating_cash_flow", "fcf_yield",
                "debt_equity", "gross_margin", "net_margin", "price", "eps",
            ]
            avail = [c for c in display_cols if c in filtered.columns]
            st.dataframe(
                filtered[avail].style.format(fmt_for(avail), na_rep="---"),
                use_container_width=True, hide_index=True, height=600,
            )

            csv = filtered[avail].to_csv(index=False)
            st.download_button("📥 Download CSV", csv, f"screener_{run_id}.csv", "text/csv")

            st.subheader("⭐ Quick Add to Watchlist")
            
            wl_df = get_watchlist_df()
            existing_wl = wl_df["ticker"].tolist() if not wl_df.empty else []
            tickers = [t for t in filtered["ticker"].tolist() if t not in existing_wl]
            
            if not tickers:
                st.info("All stocks in this result set are already in your watchlist.")
            else:
                pick = st.multiselect("Select tickers", tickers)
                if st.button("Add to Watchlist") and pick:
                    for tk in pick:
                        api_post("/watchlist", {"ticker": tk})
                    st.success(f"Added {len(pick)} ticker(s)")
                    st.rerun()


# ====================================================================
# 📉 FACTOR DISTRIBUTION
# ====================================================================
elif page == "📉 Factor Distribution":
    st.title("📉 Factor Distribution")
    st.caption("Distribution of key factors from the Quantitative Value framework")

    df = get_rankings_df()
    if df.empty:
        st.info("No data. Run a screen first.")
    else:
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "Acquirer's Multiple", "ROA", "Gross Profitability",
            "Accruals", "Beneish M-Score",
        ])

        def _factor_chart(df, col, title, ascending=True, n=20, fmt="{:.2f}"):
            if col not in df.columns:
                st.warning(f"Column '{col}' not available.")
                return
            valid = df.dropna(subset=[col])
            if valid.empty:
                st.warning(f"No valid data for {col}.")
                return
            c1, c2 = st.columns(2)
            with c1:
                fig = px.histogram(valid, x=col, nbins=25, marginal="box",
                                   title=f"Distribution of {title}", height=400)
                st.plotly_chart(fig, use_container_width=True)
            with c2:
                sorted_df = valid.nsmallest(n, col) if ascending else valid.nlargest(n, col)
                fig = px.bar(sorted_df, x="ticker", y=col, color=col,
                             color_continuous_scale="RdYlGn" if not ascending else "RdYlGn_r",
                             title=f"Top {n} by {title}", height=400)
                st.plotly_chart(fig, use_container_width=True)
            st.markdown(
                f"**Median**: {fmt.format(valid[col].median())}  |  "
                f"**Mean**: {fmt.format(valid[col].mean())}  |  "
                f"**Std**: {fmt.format(valid[col].std())}"
            )

        with tab1:
            _factor_chart(df, "acquirers_multiple", "Acquirer's Multiple (EV/EBIT)",
                          ascending=True, fmt="{:.1f}")
        with tab2:
            _factor_chart(df, "roa", "Return on Assets (ROA)",
                          ascending=False, fmt="{:.1%}")
        with tab3:
            _factor_chart(df, "gross_profitability", "Gross Profitability (GP/TA)",
                          ascending=False, fmt="{:.1%}")
        with tab4:
            _factor_chart(df, "accruals", "Accruals ((NI-CFO)/TA)",
                          ascending=True, fmt="{:.2%}")
        with tab5:
            if "beneish_mscore" in df.columns:
                valid = df.dropna(subset=["beneish_mscore"])
                if not valid.empty:
                    c1, c2 = st.columns(2)
                    with c1:
                        fig = px.histogram(valid, x="beneish_mscore", nbins=25,
                                           marginal="box",
                                           title="Beneish M-Score Distribution", height=400)
                        fig.add_vline(x=-1.78, line_dash="dash", line_color="red",
                                      annotation_text="Threshold (-1.78)")
                        st.plotly_chart(fig, use_container_width=True)
                    with c2:
                        valid = valid.copy()
                        valid["fraud_risk"] = np.where(
                            valid["beneish_mscore"] > -1.78, "High Risk", "Low Risk")
                        fig = px.pie(valid, names="fraud_risk",
                                     title="Fraud Risk Classification",
                                     color="fraud_risk",
                                     color_discrete_map={"High Risk": "red", "Low Risk": "green"},
                                     height=400)
                        st.plotly_chart(fig, use_container_width=True)
                    n_high = (valid["beneish_mscore"] > -1.78).sum()
                    st.info(f"**{n_high}** out of **{len(valid)}** stocks flagged "
                            f"as potential manipulators (M > -1.78)")


# ====================================================================
# 🔍 STOCK DETAIL
# ====================================================================
elif page == "🔍 Stock Detail":
    st.title("🔍 Stock Detail")

    df = get_rankings_df()
    if df.empty:
        st.info("No screening data available.")
    else:
        tickers = df["ticker"].tolist()
        selected_ticker = st.selectbox("Select a stock", tickers)

        if selected_ticker:
            detail = get_stock_detail(selected_ticker)
            stock_data = detail.get("current", {})
            history_records = detail.get("history", [])
            hist = pd.DataFrame(history_records) if history_records else pd.DataFrame()
            stock = pd.Series(stock_data) if stock_data else df[df["ticker"] == selected_ticker].iloc[0]

            rank = int(stock.get("combined_rank", 0)) if pd.notna(stock.get("combined_rank")) else "---"
            st.markdown(f"### {selected_ticker}  |  Rank #{rank}")

            c1, c2, c3, c4, c5, c6 = st.columns(6)
            c1.metric("AM (EV/EBIT)", safe_fmt(stock.get("acquirers_multiple"), "{:.1f}x"))
            c2.metric("EBIT/EV", safe_fmt(stock.get("ebit_ev"), "{:.1%}"))
            c3.metric("Quality Score", safe_fmt(stock.get("quality_score"), "{:.0f}"))
            c4.metric("M-Score", safe_fmt(stock.get("beneish_mscore"), "{:.2f}"))
            c5.metric("PROBM", safe_fmt(stock.get("probm"), "{:.1%}"))
            c6.metric("Market Cap (B)", safe_fmt(stock.get("market_cap_b"), "{:,.0f}"))

            st.divider()

            col1, col2 = st.columns(2)
            with col1:
                st.subheader("📊 Quality Factors")
                qd = {
                    "ROA": safe_fmt(stock.get("roa"), "{:.1%}"),
                    "ROE": safe_fmt(stock.get("roe"), "{:.1%}"),
                    "ROIC": safe_fmt(stock.get("roic"), "{:.1%}"),
                    "Gross Profitability (GP/TA)": safe_fmt(stock.get("gross_profitability"), "{:.1%}"),
                    "Accruals ((NI-CFO)/TA)": safe_fmt(stock.get("accruals"), "{:.2%}"),
                    "CFO/TA": safe_fmt(stock.get("cfo_to_assets"), "{:.1%}"),
                    "FCF Yield": safe_fmt(stock.get("fcf_yield"), "{:.1%}"),
                }
                st.table(pd.DataFrame(qd.items(), columns=["Factor", "Value"]))

            with col2:
                st.subheader("💰 Valuation")
                vd = {
                    "Acquirer's Multiple": safe_fmt(stock.get("acquirers_multiple"), "{:.2f}"),
                    "P/E": safe_fmt(stock.get("pe"), "{:.1f}"),
                    "P/B": safe_fmt(stock.get("pb"), "{:.2f}"),
                    "D/E": safe_fmt(stock.get("debt_equity"), "{:.2f}"),
                    "Gross Margin": safe_fmt(stock.get("gross_margin"), "{:.1%}"),
                    "Net Margin": safe_fmt(stock.get("net_margin"), "{:.1%}"),
                    "Price": safe_fmt(stock.get("price"), "{:,.0f} VND"),
                    "EPS": safe_fmt(stock.get("eps"), "{:,.0f}"),
                }
                st.table(pd.DataFrame(vd.items(), columns=["Metric", "Value"]))

            # Ranking within cohort
            st.subheader("📈 Ranking Position")
            rank_cols = ["value_rank", "quality_rank", "combined_rank"]
            avail_ranks = [c for c in rank_cols if c in df.columns]
            if avail_ranks:
                n = len(df)
                rank_data = []
                for rc in avail_ranks:
                    val = stock.get(rc)
                    if pd.notna(val):
                        percentile = (1 - val / n) * 100 if n > 0 else 0
                        rank_data.append({
                            "Rank Type": rc.replace("_", " ").title(),
                            "Rank": int(val), "Percentile": f"{percentile:.0f}%", "Out Of": n,
                        })
                if rank_data:
                    st.dataframe(pd.DataFrame(rank_data), hide_index=True)

            # Radar chart
            st.subheader("🕸️ Factor Profile vs Median")
            radar_cols = ["roa", "gross_profitability", "cfo_to_assets", "accruals",
                          "acquirers_multiple", "debt_equity"]
            avail_radar = [c for c in radar_cols if c in df.columns and pd.notna(stock.get(c))]
            if len(avail_radar) >= 3:
                stock_vals = [float(stock[c]) for c in avail_radar]
                median_vals = [float(df[c].median()) for c in avail_radar]
                fig = go.Figure()
                fig.add_trace(go.Scatterpolar(r=stock_vals, theta=avail_radar,
                                               fill="toself", name=selected_ticker))
                fig.add_trace(go.Scatterpolar(r=median_vals, theta=avail_radar,
                                               fill="toself", name="Median", opacity=0.4))
                fig.update_layout(polar=dict(radialaxis=dict(visible=True)),
                                  height=450, title=f"{selected_ticker} vs Cohort Median")
                st.plotly_chart(fig, use_container_width=True)

            # Ranking history
            st.subheader("📅 History Across Runs")
            if not hist.empty and "run_date" in hist.columns:
                metric = st.selectbox("Track metric",
                    ["combined_rank", "acquirers_multiple", "quality_score",
                     "beneish_mscore", "price", "roa", "roic"], key="detail_metric")
                if metric in hist.columns:
                    fig = px.line(hist, x="run_date", y=metric,
                                  title=f"{selected_ticker} — {metric}", markers=True, height=350)
                    if metric == "combined_rank":
                        fig.update_yaxes(autorange="reversed")
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.caption("No historical data (need multiple runs).")


# ====================================================================
# 💼 MODEL PORTFOLIO
# ====================================================================
elif page == "💼 Model Portfolio":
    st.title("💼 Model Portfolio")
    st.caption(f"Top {PORTFOLIO_SIZE} stocks, equal weight (~{100/PORTFOLIO_SIZE:.1f}% each)")

    df = get_rankings_df()
    if df.empty:
        st.info("No screening data. Run a screen first.")
    else:
        top = df.head(PORTFOLIO_SIZE).copy()
        n = len(top)
        top["weight"] = round(100.0 / n, 2) if n > 0 else 0

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Holdings", n)
        c2.metric("Weight Each", f"{100/n:.1f}%" if n > 0 else "---")
        avg_am = top["acquirers_multiple"].mean() if "acquirers_multiple" in top.columns else 0
        c3.metric("Avg AM", f"{avg_am:.1f}x" if pd.notna(avg_am) else "---")
        avg_qs = top["quality_score"].mean() if "quality_score" in top.columns else 0
        c4.metric("Avg Quality", f"{avg_qs:.0f}" if pd.notna(avg_qs) else "---")

        st.divider()
        show_cols = [
            "ticker", "combined_rank", "weight", "acquirers_multiple",
            "quality_score", "beneish_mscore", "market_cap_b",
            "roa", "gross_profitability", "pe", "pb", "price",
        ]
        avail = [c for c in show_cols if c in top.columns]
        st.dataframe(
            top[avail].style.format(fmt_for(avail), na_rep="---"),
            use_container_width=True, hide_index=True, height=500,
        )

        col1, col2 = st.columns(2)
        with col1:
            if "market_cap_b" in top.columns:
                fig = px.histogram(top, x="market_cap_b", nbins=10,
                                   title="Market Cap Distribution (B VND)", height=350)
                st.plotly_chart(fig, use_container_width=True)
        with col2:
            factor_cols = ["roa", "gross_profitability", "accruals", "cfo_to_assets"]
            avail_f = [c for c in factor_cols if c in top.columns]
            if avail_f:
                melted = top[avail_f].melt(var_name="Factor", value_name="Value")
                fig = px.box(melted, x="Factor", y="Value",
                             title="Quality Factor Ranges", height=350)
                st.plotly_chart(fig, use_container_width=True)

        csv = top[avail].to_csv(index=False)
        st.download_button("📥 Download Portfolio CSV", csv,
                            f"portfolio_{datetime.now():%Y%m%d}.csv", "text/csv")


# ====================================================================
# 📈 CHARTS & ANALYSIS
# ====================================================================
elif page == "📈 Charts & Analysis":
    st.title("📈 Charts & Analysis")

    df = get_rankings_df()
    if df.empty:
        st.info("No data. Run a screen first.")
    else:
        tab1, tab2, tab3, tab4 = st.tabs([
            "Value vs Quality", "Metric Distributions",
            "Correlation Matrix", "Stock Comparison",
        ])

        with tab1:
            if "acquirers_multiple" in df.columns and "quality_score" in df.columns:
                color_col = "beneish_mscore" if "beneish_mscore" in df.columns else "quality_score"
                scale = "RdYlGn_r" if color_col == "beneish_mscore" else "RdYlGn"
                fig = px.scatter(
                    df, x="acquirers_multiple", y="quality_score", text="ticker",
                    size="market_cap_b" if "market_cap_b" in df.columns else None,
                    color=color_col, color_continuous_scale=scale, height=600,
                    labels={"acquirers_multiple": "AM (lower=cheaper)", "quality_score": "Quality (0-100)"},
                )
                fig.update_traces(textposition="top center", textfont_size=9)
                fig.add_hline(y=50, line_dash="dash", line_color="gray")
                fig.add_vline(x=10, line_dash="dash", line_color="gray")
                st.plotly_chart(fig, use_container_width=True)

        with tab2:
            all_metrics = [
                "acquirers_multiple", "ebit_ev", "quality_score", "beneish_mscore", "probm",
                "roa", "gross_profitability", "accruals", "cfo_to_assets",
                "pe", "pb", "roe", "roic", "fcf_yield", "debt_equity", "gross_margin", "net_margin",
            ]
            avail_metrics = [m for m in all_metrics if m in df.columns]
            metric = st.selectbox("Select metric", avail_metrics)
            if metric in df.columns:
                c1, c2 = st.columns(2)
                with c1:
                    fig = px.histogram(df, x=metric, nbins=20, marginal="box",
                                       title=f"Distribution of {metric}", height=400)
                    st.plotly_chart(fig, use_container_width=True)
                with c2:
                    fig = px.bar(df.nsmallest(15, "combined_rank"), x="ticker", y=metric,
                                  color=metric, color_continuous_scale="RdYlGn",
                                  title=f"Top 15 — {metric}", height=400)
                    st.plotly_chart(fig, use_container_width=True)
                st.metric(f"Median {metric}", f"{df[metric].median():.3f}")

        with tab3:
            num_cols = [
                "acquirers_multiple", "quality_score", "beneish_mscore",
                "roa", "gross_profitability", "accruals", "cfo_to_assets",
                "roe", "roic", "fcf_yield", "pe", "pb", "debt_equity",
            ]
            avail = [c for c in num_cols if c in df.columns]
            corr = df[avail].corr()
            fig = px.imshow(corr, text_auto=".2f", color_continuous_scale="RdBu_r",
                            title="Metric Correlation Matrix", height=600)
            st.plotly_chart(fig, use_container_width=True)

        with tab4:
            tickers = df["ticker"].tolist()
            selected = st.multiselect("Compare stocks", tickers,
                                       default=tickers[:3] if len(tickers) >= 3 else tickers)
            if selected:
                cmp = df[df["ticker"].isin(selected)]
                metrics = ["acquirers_multiple", "quality_score", "roa",
                           "gross_profitability", "accruals", "roic", "pe", "debt_equity"]
                avail_m = [m for m in metrics if m in cmp.columns]
                fig = go.Figure()
                for _, row in cmp.iterrows():
                    vals = [float(row[m]) if pd.notna(row[m]) else 0 for m in avail_m]
                    fig.add_trace(go.Scatterpolar(
                        r=vals, theta=avail_m, fill="toself", name=row["ticker"]))
                fig.update_layout(polar=dict(radialaxis=dict(visible=True)),
                                  title="Radar Comparison", height=500)
                st.plotly_chart(fig, use_container_width=True)
                st.dataframe(cmp[["ticker"] + avail_m], use_container_width=True, hide_index=True)


# ====================================================================
# 📅 HISTORICAL COMPARISON
# ====================================================================
elif page == "📅 Historical Comparison":
    st.title("📅 Historical Comparison")

    runs = get_runs_df()
    if runs.empty:
        st.info("Need at least one screening run.")
    else:
        tab1, tab2 = st.tabs(["Compare Runs", "Track a Stock"])

        with tab1:
            if len(runs) < 2:
                st.info("Need at least 2 runs to compare.")
            else:
                c1, c2 = st.columns(2)
                run_labels = {f"#{r['id']} — {r['run_date']}": r["id"] for _, r in runs.iterrows()}
                labels = list(run_labels.keys())
                old_label = c1.selectbox("Older run", labels, index=min(1, len(labels) - 1))
                new_label = c2.selectbox("Newer run", labels, index=0)

                old_df = get_run_results_df(run_labels[old_label])
                new_df = get_run_results_df(run_labels[new_label])

                if not old_df.empty and not new_df.empty:
                    merge_cols = ["ticker", "combined_rank", "acquirers_multiple",
                                  "quality_score", "beneish_mscore", "price"]
                    avail_merge = [c for c in merge_cols if c in old_df.columns]
                    merged = new_df.merge(old_df[avail_merge].dropna(axis=1, how="all"),
                                          on="ticker", suffixes=("", "_prev"), how="left")
                    if "combined_rank_prev" in merged.columns:
                        merged["rank_change"] = merged["combined_rank_prev"] - merged["combined_rank"]
                    if "price_prev" in merged.columns:
                        merged["price_change_pct"] = (
                            (merged["price"] - merged["price_prev"]) / merged["price_prev"] * 100
                        ).round(1)

                    show = ["ticker", "combined_rank", "rank_change",
                            "acquirers_multiple", "quality_score", "beneish_mscore",
                            "price", "price_change_pct"]
                    avail = [c for c in show if c in merged.columns]
                    st.dataframe(merged[avail].sort_values("combined_rank"),
                                 use_container_width=True, hide_index=True, height=500)

                    if "rank_change" in merged.columns:
                        c1, c2 = st.columns(2)
                        with c1:
                            st.subheader("📈 Biggest Improvers")
                            st.dataframe(merged.nlargest(5, "rank_change")[
                                ["ticker", "rank_change", "combined_rank"]], hide_index=True)
                        with c2:
                            st.subheader("📉 Biggest Decliners")
                            st.dataframe(merged.nsmallest(5, "rank_change")[
                                ["ticker", "rank_change", "combined_rank"]], hide_index=True)

        with tab2:
            ticker = st.text_input("Enter ticker (e.g. VNM, FPT, DPM)").upper()
            if ticker:
                detail = get_stock_detail(ticker)
                history_records = detail.get("history", [])
                hist = pd.DataFrame(history_records) if history_records else pd.DataFrame()
                if hist.empty:
                    st.warning(f"No data for {ticker}")
                else:
                    st.subheader(f"📊 {ticker} — History")
                    metric = st.selectbox("Track metric",
                        ["combined_rank", "acquirers_multiple", "quality_score",
                         "beneish_mscore", "price", "roa", "roic", "pe"])
                    if "run_date" in hist.columns and metric in hist.columns:
                        fig = px.line(hist, x="run_date", y=metric,
                                      title=f"{ticker} — {metric}", markers=True, height=400)
                        if metric == "combined_rank":
                            fig.update_yaxes(autorange="reversed")
                        st.plotly_chart(fig, use_container_width=True)
                    show_cols = ["run_date", "combined_rank", "acquirers_multiple",
                                 "quality_score", "beneish_mscore", "price", "roa", "roic", "pe"]
                    avail = [c for c in show_cols if c in hist.columns]
                    st.dataframe(hist[avail], use_container_width=True, hide_index=True)


# ====================================================================
# ⭐ WATCHLIST & PORTFOLIO
# ====================================================================
elif page == "⭐ Watchlist & Portfolio":
    st.title("⭐ Watchlist & Portfolio")

    tab1, tab2 = st.tabs(["Watchlist", "Portfolio Tracker"])

    with tab1:
        with st.expander("➕ Add to Watchlist", expanded=False):
            c1, c2 = st.columns([1, 3])
            new_tick = c1.text_input("Ticker", placeholder="VNM").upper()
            new_notes = c2.text_input("Notes", placeholder="Why I like this stock...")
            if st.button("Add") and new_tick:
                resp = api_post("/watchlist", {"ticker": new_tick, "notes": new_notes})
                if resp.get("message"):
                    st.success(f"Added {new_tick}")
                    st.rerun()
                else:
                    st.warning(f"{new_tick} may already be in watchlist")

        wl = get_watchlist_df()
        if wl.empty:
            st.info("Watchlist is empty.")
        else:
            latest = get_rankings_df()
            if not latest.empty:
                enrich_cols = [
                    "ticker", "combined_rank", "acquirers_multiple",
                    "quality_score", "beneish_mscore", "price", "pe", "roa", "roic",
                ]
                avail_e = [c for c in enrich_cols if c in latest.columns]
                enriched = wl.merge(latest[avail_e], on="ticker", how="left")
            else:
                enriched = wl
            st.dataframe(enriched, use_container_width=True, hide_index=True)

            st.subheader("🗑️ Remove from Watchlist")
            to_remove = st.multiselect("Select tickers to remove", wl["ticker"].tolist())
            if st.button("Remove Selected") and to_remove:
                for tk in to_remove:
                    api_delete(f"/watchlist/{tk}")
                st.success(f"Removed {len(to_remove)} ticker(s)")
                st.rerun()

    with tab2:
        st.subheader("💰 Portfolio Tracker")
        wl = get_watchlist_df()
        if wl.empty:
            st.info("Add stocks to your watchlist first.")
        else:
            for _, row in wl.iterrows():
                with st.expander(f"📌 {row['ticker']}"):
                    c1, c2, c3 = st.columns(3)
                    bp = c1.number_input("Buy Price", value=float(row.get("buy_price") or 0),
                                         step=100.0, key=f"bp_{row['ticker']}")
                    sh = c2.number_input("Shares", value=float(row.get("shares") or 0),
                                         step=100.0, key=f"sh_{row['ticker']}")
                    notes = c3.text_input("Notes", value=row.get("notes", ""),
                                          key=f"nt_{row['ticker']}")
                    if st.button("Save", key=f"sv_{row['ticker']}"):
                        # watchlist update needs a PATCH/PUT endpoint — use direct DB for now
                        from quant_value_vn.database import queries as db_wl
                        db_wl.update_watchlist(row["ticker"], notes=notes, buy_price=bp, shares=sh)
                        st.success(f"Updated {row['ticker']}")
                        st.rerun()

            latest = get_rankings_df()
            if not latest.empty:
                wl = get_watchlist_df()
                portfolio = wl.merge(latest[["ticker", "price"]], on="ticker", how="left")
                portfolio["current_value"] = portfolio["price"] * portfolio["shares"].fillna(0)
                portfolio["cost_basis"] = portfolio["buy_price"].fillna(0) * portfolio["shares"].fillna(0)
                portfolio["pnl"] = portfolio["current_value"] - portfolio["cost_basis"]
                portfolio["pnl_pct"] = np.where(
                    portfolio["cost_basis"] > 0,
                    portfolio["pnl"] / portfolio["cost_basis"] * 100, 0)

                if portfolio["shares"].fillna(0).sum() > 0:
                    st.divider()
                    st.subheader("📊 Portfolio Summary")
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Total Value", f"{portfolio['current_value'].sum():,.0f} VND")
                    c2.metric("Total Cost", f"{portfolio['cost_basis'].sum():,.0f} VND")
                    total_pnl = portfolio["pnl"].sum()
                    total_cost = portfolio["cost_basis"].sum()
                    c3.metric("Total P&L", f"{total_pnl:,.0f} VND",
                              f"{total_pnl/total_cost*100:.1f}%" if total_cost > 0 else "")

                    show = ["ticker", "buy_price", "shares", "price",
                            "current_value", "cost_basis", "pnl", "pnl_pct"]
                    avail = [c for c in show if c in portfolio.columns]
                    st.dataframe(
                        portfolio[avail].style.format({
                            "buy_price": "{:,.0f}", "price": "{:,.0f}",
                            "current_value": "{:,.0f}", "cost_basis": "{:,.0f}",
                            "pnl": "{:,.0f}", "pnl_pct": "{:.1f}%",
                        }, na_rep="---"),
                        use_container_width=True, hide_index=True,
                    )


# ====================================================================
# 🔄 RUN SCREENER
# ====================================================================
elif page == "🔄 Run Screener":
    st.title("🔄 Run Screener")

    tab1, tab2 = st.tabs(["Run New Screen", "Import CSV"])

    with tab1:
        st.subheader("Configure & Run")
        st.markdown("""
        **Pipeline** (per the Quantitative Value book):
        1. Scrape financials (2-year for Beneish M-Score)
        2. Remove financial sector companies
        3. Compute & filter Beneish M-Score (remove manipulators > -1.78)
        4. Calculate quality factors (ROA, Gross Prof, Accruals, CFO/TA)
        5. Rank by Acquirer's Multiple (value) + Quality (composite)
        6. Output Top 30 model portfolio
        """)

        c1, c2, c3 = st.columns(3)
        max_stocks = c1.slider("Max stocks to scan", 50, 2000, 1500, step=50)
        workers = c2.slider("Parallel workers", 1, 50, 30, step=1)
        delay_val = c3.slider("Request delay (sec)", 0.1, 1.0, 0.2, step=0.1)

        c4, c5, c6 = st.columns(3)
        min_mcap = c4.number_input("Min Market Cap (B VND)", value=500, step=100) * 1e9
        max_am = c5.number_input("Max Acquirer's Multiple", value=50.0, step=5.0)
        skip_prefilter = c6.checkbox("Skip pre-filter (scan all exchanges)", value=False)

        est_time = max_stocks * 3.0 / 60 / workers * 2
        st.caption(f"Estimated time: ~{max(1, est_time):.0f} min")

        if st.button("🚀 Run Full QV Pipeline", type="primary"):
            progress = st.progress(0, text="Starting...")

            # Run pipeline directly (needs progress callback — can't go through API)
            from quant_value_vn.pipeline.run_pipeline import run_pipeline

            def _progress(completed, total, ok_count):
                progress.progress(
                    completed / total,
                    text=f"Scanning... ({completed}/{total}) — {ok_count} ok",
                )

            result = run_pipeline(
                max_stocks=max_stocks,
                workers=workers,
                min_mcap=min_mcap,
                max_am=max_am,
                skip_prefilter=skip_prefilter,
                save_to_db=True,
                progress_callback=_progress,
            )

            if result is None:
                st.error("No stocks passed filters. Check network or adjust thresholds.")
            else:
                progress.progress(1.0, text="Done!")
                st.success(f"✅ Pipeline complete — {len(result)} stocks ranked")
                show = [
                    "ticker", "combined_rank", "acquirers_multiple",
                    "quality_score", "beneish_mscore", "market_cap_B", "pe",
                    "roa", "gross_profitability", "accruals", "roic",
                ]
                avail = [c for c in show if c in result.columns]
                st.dataframe(result[avail], use_container_width=True, hide_index=True)

    with tab2:
        st.subheader("📥 Import CSV")
        uploaded = st.file_uploader("Upload screening CSV", type=["csv"])
        if uploaded:
            udf = pd.read_csv(uploaded)
            st.dataframe(udf.head(), use_container_width=True, hide_index=True)
            if st.button("Import to Database"):
                tmp = Path("_tmp_import.csv")
                tmp.write_bytes(uploaded.getvalue())
                from quant_value_vn.database import queries as db_csv
                rid = db_csv.import_csv(str(tmp))
                tmp.unlink()
                st.success(f"Imported as Run #{rid}")
                st.rerun()

        csvs = sorted(glob.glob("vietnam_value_screen_*.csv"))
        if csvs:
            st.divider()
            st.subheader("📂 Local CSV Files")
            for path in csvs:
                c1, c2 = st.columns([4, 1])
                c1.code(path)
                if c2.button("Import", key=f"imp_{path}"):
                    from quant_value_vn.database import queries as db_csv2
                    rid = db_csv2.import_csv(path)
                    st.success(f"Imported as Run #{rid}")
                    st.rerun()


# ====================================================================
# ⚙️ SETTINGS
# ====================================================================
elif page == "⚙️ Settings":
    st.title("⚙️ Settings")

    tab1, tab2 = st.tabs(["Database", "About"])

    with tab1:
        from quant_value_vn.config import SUPABASE_URL, SUPABASE_KEY
        st.subheader("🗄️ Supabase Connection")
        st.text(f"URL: {SUPABASE_URL[:30]}..." if len(SUPABASE_URL) > 30 else f"URL: {SUPABASE_URL or 'Not set'}")
        st.text(f"Key: {'✅ Set' if SUPABASE_KEY else '❌ Not set'}")

        st.divider()
        st.subheader("📊 Screening Runs")
        runs = get_runs_df()
        if not runs.empty:
            st.dataframe(runs, use_container_width=True, hide_index=True)
            to_delete = st.multiselect(
                "Select runs to delete",
                [f"#{r['id']} — {r['run_date']}" for _, r in runs.iterrows()])
            if st.button("Delete Selected") and to_delete:
                for label in to_delete:
                    rid = int(label.split("#")[1].split("—")[0].strip())
                    api_delete(f"/runs/{rid}")
                st.success("Deleted!")
                st.rerun()

    with tab2:
        st.subheader("About")
        st.markdown("""
        **Vietnam Quantitative Value Stock Screener v2.0**

        Implements the full methodology from Tobias Carlisle & Wesley Gray's
        *Quantitative Value* (Wiley, 2012).

        **Pipeline Order** (matches the book):
        1. **Universe Selection** — Market cap >= 500B VND, EBIT > 0, EV > 0
        2. **Sector Exclusion** — Remove banks, insurance, securities, finance
        3. **Fraud Detection** — Beneish M-Score (8 variables), remove M > -1.78
        4. **Quality Ranking** — ROA + Gross Profitability + CFO/TA + low Accruals
        5. **Value Ranking** — Acquirer's Multiple (EV/EBIT), ascending
        6. **Combined Rank** — value_rank + quality_rank, lowest = best

        | Metric | Formula | Interpretation |
        |--------|---------|----------------|
        | Acquirer's Multiple | EV / EBIT | Lower = cheaper |
        | Quality Score | Composite rank (0-100) | Higher = better |
        | Beneish M-Score | 8-variable fraud model | <= -1.78 = clean |
        | Gross Profitability | GP / Total Assets | Higher = better |
        | Accruals | (NI - CFO) / TA | Lower = higher quality |
        | CFO/TA | Operating CF / TA | Higher = stronger |

        **Architecture:** Pipeline → Supabase → FastAPI → Streamlit
        **Data Source:** CafeF.vn | **Tickers:** Wifeed.vn API
        """)
