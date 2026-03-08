"""
Service layer — thin wrapper between API routes and database queries.

Keeps routes focussed on HTTP concerns (validation, serialisation)
while services handle business logic + DB access.
"""

import logging
from typing import Dict, List, Optional

import pandas as pd

from quant_value_vn.database import queries as db
from quant_value_vn.config import PORTFOLIO_SIZE

logger = logging.getLogger(__name__)


# ── Rankings / Screening Runs ────────────────────────────────────────

def get_latest_rankings() -> List[Dict]:
    """Return latest screening results as list of dicts (empty list if none)."""
    df = db.get_latest_run_results()
    if df is None or df.empty:
        return []
    return _df_to_records(df)


def get_run_rankings(run_id: int) -> List[Dict]:
    """Return screening results for a specific run."""
    df = db.get_run_results(run_id)
    if df.empty:
        return []
    return _df_to_records(df)


def list_runs() -> List[Dict]:
    """All screening runs, newest first."""
    df = db.get_runs()
    return _df_to_records(df)


def delete_run(run_id: int) -> None:
    db.delete_run(run_id)


# ── Stock Detail ─────────────────────────────────────────────────────

def get_stock_detail(ticker: str) -> Optional[Dict]:
    """
    Return current snapshot + historical data for *ticker*.
    Returns None if ticker is not in the latest run.
    """
    df = db.get_latest_run_results()
    if df is None or df.empty:
        return None
    match = df[df["ticker"] == ticker.upper()]
    if match.empty:
        return None

    history = db.get_ticker_history(ticker.upper())
    return {
        "current": _row_to_dict(match.iloc[0]),
        "history": _df_to_records(history),
    }


# ── Portfolio ────────────────────────────────────────────────────────

def get_latest_portfolio() -> List[Dict]:
    """Latest model portfolio (top N equal-weight)."""
    df = db.get_latest_portfolio()
    if df is None or df.empty:
        return []
    return _df_to_records(df)


# ── Watchlist ────────────────────────────────────────────────────────

def get_watchlist() -> List[Dict]:
    df = db.get_watchlist()
    return _df_to_records(df)


def add_to_watchlist(
    ticker: str, notes: str = "", buy_price: float = None, shares: float = None,
) -> bool:
    """True if added, False if duplicate."""
    return db.add_to_watchlist(ticker, notes, buy_price, shares)


def remove_from_watchlist(ticker: str) -> None:
    db.remove_from_watchlist(ticker)


def update_watchlist_item(
    ticker: str, notes: str = None, buy_price: float = None, shares: float = None,
) -> None:
    db.update_watchlist(ticker, notes=notes, buy_price=buy_price, shares=shares)


# ── Pipeline ─────────────────────────────────────────────────────────

def run_pipeline_bg(
    max_stocks: int = 9999,
    workers: int = 10,
    min_mcap: float = 500e9,
    max_am: float = 50.0,
) -> None:
    """Run the pipeline (blocking) — intended for a background task."""
    from quant_value_vn.pipeline.run_pipeline import run_pipeline
    run_pipeline(
        max_stocks=max_stocks,
        workers=workers,
        min_mcap=min_mcap,
        max_am=max_am,
    )


# ── CSV Import ───────────────────────────────────────────────────────

def import_csv(path: str) -> int:
    """Import CSV into DB. Returns new run_id."""
    return db.import_csv(path)


# ── Private helpers ──────────────────────────────────────────────────

def _sanitize_value(v):
    """Convert NaN/Inf to None for JSON compatibility."""
    if v is None:
        return None
    if isinstance(v, float):
        import math
        if math.isnan(v) or math.isinf(v):
            return None
    return v


def _df_to_records(df: pd.DataFrame) -> List[Dict]:
    """Convert DataFrame to JSON-safe list of dicts (NaN → None)."""
    if df is None or df.empty:
        return []
    records = df.to_dict(orient="records")
    return [
        {k: _sanitize_value(v) for k, v in rec.items()}
        for rec in records
    ]


def _row_to_dict(row: pd.Series) -> Dict:
    """Convert a single Series to a dict (NaN → None)."""
    d = row.to_dict()
    return {k: (None if pd.isna(v) else v) for k, v in d.items()}
