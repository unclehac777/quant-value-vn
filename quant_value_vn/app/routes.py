"""
API routes for the Quantitative Value screener.

All endpoints return JSON.
Routes are thin — business logic lives in app.services.
"""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel

from quant_value_vn.app import services as svc

logger = logging.getLogger(__name__)
router = APIRouter()


# ── Request/Response models ──────────────────────────────────────────

class WatchlistAdd(BaseModel):
    ticker: str
    notes: str = ""
    buy_price: Optional[float] = None
    shares: Optional[float] = None


class RunConfig(BaseModel):
    max_stocks: int = 9999
    workers: int = 10
    min_mcap: float = 500e9
    max_am: float = 50.0


# ── Rankings ─────────────────────────────────────────────────────────

@router.get("/rankings")
def get_latest_rankings():
    """Get the latest screening results."""
    data = svc.get_latest_rankings()
    if not data:
        return {"data": [], "message": "No screening data available"}
    return {"data": data}


@router.get("/rankings/{run_id}")
def get_run_rankings(run_id: int):
    """Get screening results for a specific run."""
    data = svc.get_run_rankings(run_id)
    if not data:
        raise HTTPException(404, f"Run #{run_id} not found")
    return {"data": data}


# ── Runs ─────────────────────────────────────────────────────────────

@router.get("/runs")
def list_runs():
    """List all screening runs."""
    return {"data": svc.list_runs()}


@router.delete("/runs/{run_id}")
def delete_run(run_id: int):
    """Delete a screening run and its results."""
    svc.delete_run(run_id)
    return {"message": f"Run #{run_id} deleted"}


# ── Stock Detail ─────────────────────────────────────────────────────

@router.get("/stock/{ticker}")
def get_stock_detail(ticker: str):
    """Get latest screening data and history for a ticker."""
    result = svc.get_stock_detail(ticker)
    if result is None:
        raise HTTPException(404, f"Ticker {ticker} not found in latest run")
    return result


# ── Portfolio ────────────────────────────────────────────────────────

@router.get("/portfolio")
def get_portfolio():
    """Get the latest model portfolio."""
    data = svc.get_latest_portfolio()
    if not data:
        return {"data": [], "message": "No portfolio data"}
    return {"data": data}


# ── Watchlist ────────────────────────────────────────────────────────

@router.get("/watchlist")
def get_watchlist():
    """Get all watchlist items."""
    return {"data": svc.get_watchlist()}


@router.post("/watchlist")
def add_watchlist_item(item: WatchlistAdd):
    """Add a ticker to the watchlist."""
    ok = svc.add_to_watchlist(item.ticker, item.notes, item.buy_price, item.shares)
    if not ok:
        raise HTTPException(409, f"{item.ticker} already in watchlist")
    return {"message": f"Added {item.ticker}"}


@router.delete("/watchlist/{ticker}")
def remove_watchlist_item(ticker: str):
    """Remove a ticker from the watchlist."""
    svc.remove_from_watchlist(ticker)
    return {"message": f"Removed {ticker}"}


# ── Run Pipeline ─────────────────────────────────────────────────────

@router.post("/run")
def trigger_pipeline(config: RunConfig, background_tasks: BackgroundTasks):
    """Trigger a new pipeline run in the background."""
    background_tasks.add_task(
        svc.run_pipeline_bg,
        max_stocks=config.max_stocks,
        workers=config.workers,
        min_mcap=config.min_mcap,
        max_am=config.max_am,
    )
    return {"message": "Pipeline started", "config": config.dict()}
