"""
Database queries for the Quantitative Value screener.

Functions:
- save_run(df, total_stocks, passed_filter) → run_id
- get_runs()                  → DataFrame of all screening runs
- get_run_results(run_id)     → DataFrame of results for a run
- get_latest_run_results()    → DataFrame from most recent run
- get_ticker_history(ticker)  → DataFrame of ticker across all runs
- delete_run(run_id)          → None
- save_portfolio(run_id, tickers)
- get_portfolio(run_id)       → DataFrame
- get_latest_portfolio()      → DataFrame
- get_watchlist()             → DataFrame
- add_to_watchlist / remove_from_watchlist / update_watchlist
- import_csv(path)            → run_id
"""

import logging
from datetime import datetime
from typing import List, Optional

import pandas as pd

from quant_value_vn.database.supabase_client import get_client
from quant_value_vn.config import PORTFOLIO_SIZE

logger = logging.getLogger(__name__)


# ── Screening Runs ───────────────────────────────────────────────────

def save_run(
    df: pd.DataFrame,
    total_stocks: int,
    passed_filter: int,
    max_stocks: int = 150,
) -> int:
    """Save a screening run and its results. Returns run_id."""
    sb = get_client()

    # Create run record
    run_row = sb.table("screening_runs").insert({
        "run_date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "total_stocks": total_stocks,
        "passed_filter": passed_filter,
        "max_stocks": max_stocks,
    }).execute()
    run_id = run_row.data[0]["id"]

    # Insert results in batches
    cols = [
        "ticker", "combined_rank", "acquirers_multiple", "ebit_ev",
        "quality_score", "beneish_mscore", "probm",
        "market_cap", "enterprise_value", "ebit", "revenue",
        "roa", "roe", "roic", "gross_profitability",
        "accruals", "cfo_to_assets", "operating_cash_flow",
        "fcf_yield", "pe", "pb", "debt_equity", "gross_margin", "net_margin",
        "value_rank", "quality_rank", "price", "eps", "market_cap_B", "ev_B",
    ]
    int_cols = {"combined_rank", "value_rank", "quality_rank"}
    avail = [c for c in cols if c in df.columns]

    rows = []
    for _, row in df.iterrows():
        rec = {"run_id": run_id}
        for c in avail:
            v = row[c]
            key = c.lower()
            rec[key] = None if pd.isna(v) else (
                int(v) if key in int_cols
                else float(v) if isinstance(v, (int, float)) else v
            )
        rows.append(rec)

    batch_size = 50
    for i in range(0, len(rows), batch_size):
        sb.table("screening_results").insert(rows[i:i + batch_size]).execute()

    logger.info("Saved run #%d with %d results", run_id, len(rows))
    return run_id


def get_runs() -> pd.DataFrame:
    """List all screening runs, newest first."""
    data = (
        get_client().table("screening_runs")
        .select("*")
        .order("id", desc=True)
        .execute()
    )
    return pd.DataFrame(data.data) if data.data else pd.DataFrame()


def get_run_results(run_id: int) -> pd.DataFrame:
    """Get results for a specific run."""
    data = (
        get_client().table("screening_results")
        .select("*")
        .eq("run_id", run_id)
        .order("combined_rank")
        .execute()
    )
    return pd.DataFrame(data.data) if data.data else pd.DataFrame()


def get_latest_run_results() -> Optional[pd.DataFrame]:
    """Get results from the most recent run."""
    runs = (
        get_client().table("screening_runs")
        .select("id")
        .order("id", desc=True)
        .limit(1)
        .execute()
    )
    if not runs.data:
        return None
    return get_run_results(runs.data[0]["id"])


def get_ticker_history(ticker: str) -> pd.DataFrame:
    """Get historical screening data for a single ticker across all runs."""
    results = (
        get_client().table("screening_results")
        .select("*")
        .eq("ticker", ticker)
        .order("run_id")
        .execute()
    )
    if not results.data:
        return pd.DataFrame()

    df = pd.DataFrame(results.data)
    run_ids = df["run_id"].unique().tolist()

    runs = (
        get_client().table("screening_runs")
        .select("id, run_date")
        .in_("id", run_ids)
        .execute()
    )
    runs_df = pd.DataFrame(runs.data)
    merged = df.merge(runs_df, left_on="run_id", right_on="id", suffixes=("", "_run"))
    return merged.sort_values("run_id")


def delete_run(run_id: int) -> None:
    """Delete a screening run (cascade deletes results & portfolio)."""
    sb = get_client()
    sb.table("portfolio_history").delete().eq("run_id", run_id).execute()
    sb.table("screening_results").delete().eq("run_id", run_id).execute()
    sb.table("screening_runs").delete().eq("id", run_id).execute()


# ── Portfolio History ────────────────────────────────────────────────

def save_portfolio(
    run_id: int, tickers: List[str], n: int = PORTFOLIO_SIZE
) -> None:
    """Save model portfolio (top N equal-weight) for a run."""
    sb = get_client()
    weight = round(1.0 / n, 4) if n > 0 else 0
    rows = [
        {"run_id": run_id, "ticker": tk, "weight": weight, "rank": i + 1}
        for i, tk in enumerate(tickers[:n])
    ]
    if rows:
        batch_size = 50
        for i in range(0, len(rows), batch_size):
            sb.table("portfolio_history").insert(rows[i:i + batch_size]).execute()


def get_portfolio(run_id: int) -> pd.DataFrame:
    """Get portfolio for a specific run."""
    data = (
        get_client().table("portfolio_history")
        .select("*")
        .eq("run_id", run_id)
        .order("rank")
        .execute()
    )
    return pd.DataFrame(data.data) if data.data else pd.DataFrame()


def get_latest_portfolio() -> Optional[pd.DataFrame]:
    """Get most recent model portfolio."""
    runs = (
        get_client().table("screening_runs")
        .select("id")
        .order("id", desc=True)
        .limit(1)
        .execute()
    )
    if not runs.data:
        return None
    return get_portfolio(runs.data[0]["id"])


# ── Watchlist ────────────────────────────────────────────────────────

def get_watchlist() -> pd.DataFrame:
    data = (
        get_client().table("watchlist")
        .select("*")
        .order("added_at", desc=True)
        .execute()
    )
    return pd.DataFrame(data.data) if data.data else pd.DataFrame()


def add_to_watchlist(
    ticker: str, notes: str = "", buy_price: float = None, shares: float = None
) -> bool:
    """Add ticker. Returns True if added, False if already exists."""
    try:
        rec = {"ticker": ticker.upper(), "notes": notes}
        if buy_price is not None:
            rec["buy_price"] = buy_price
        if shares is not None:
            rec["shares"] = shares
        get_client().table("watchlist").insert(rec).execute()
        return True
    except Exception:
        return False


def remove_from_watchlist(ticker: str) -> None:
    get_client().table("watchlist").delete().eq("ticker", ticker.upper()).execute()


def update_watchlist(
    ticker: str, notes: str = None, buy_price: float = None, shares: float = None
) -> None:
    update = {}
    if notes is not None:
        update["notes"] = notes
    if buy_price is not None:
        update["buy_price"] = buy_price
    if shares is not None:
        update["shares"] = shares
    if update:
        get_client().table("watchlist").update(update).eq(
            "ticker", ticker.upper()
        ).execute()


def get_watchlist_tickers() -> List[str]:
    data = get_client().table("watchlist").select("ticker").execute()
    return [r["ticker"] for r in data.data] if data.data else []


# ── Import CSV ───────────────────────────────────────────────────────

def import_csv(path: str) -> int:
    """Import a previously saved CSV into the database. Returns run_id."""
    df = pd.read_csv(path)
    return save_run(df, total_stocks=len(df), passed_filter=len(df))
