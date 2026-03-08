"""
Main pipeline runner for Vietnam Quantitative Value screener.

Execution order (matches the book):
1. Ingest CafeF data (parallel scraping)
2. Clean financial data
3. Compute features (market cap, EV, ratios)
4. Exclude financial sector
5. Run fraud detection (Beneish M-Score)
6. Compute quality factors
7. Compute value factor (Acquirer's Multiple)
8. Filter investable universe
9. Rank stocks
10. Store results in Supabase

Usage:
    python -m quant_value_vn.pipeline.run_pipeline [--max-stocks N] [--workers N] [--quick]
"""

import argparse
import logging
import time
from datetime import datetime
from typing import Optional

import pandas as pd

from quant_value_vn.pipeline.ingest import fetch_tickers, fetch_ticker_info, prefilter_universe, ingest_all
from quant_value_vn.pipeline.clean import clean_data
from quant_value_vn.pipeline.features import compute_features
from quant_value_vn.pipeline.fraud import compute_fraud_scores, remove_manipulators
from quant_value_vn.pipeline.quality import compute_quality_rank
from quant_value_vn.pipeline.value import compute_acquirers_multiple
from quant_value_vn.pipeline.ranking import remove_sectors, filter_universe, rank_stocks
from quant_value_vn.pipeline.scores import compute_safety_scores, apply_safety_filters
from quant_value_vn.pipeline.momentum import compute_momentum_scores, remove_negative_momentum
from quant_value_vn.config import (
    MIN_MARKET_CAP, MAX_ACQUIRERS_MULTIPLE, PORTFOLIO_SIZE,
    MIN_ADV20, MIN_TRADING_DAYS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def format_output(df: pd.DataFrame) -> pd.DataFrame:
    """Round numeric columns for display and add convenience fields."""
    out = df.copy()
    for c in (
        "ebit_ev", "acquirers_multiple", "roa", "roe", "roic",
        "gross_profitability", "accruals", "cfo_to_assets",
        "fcf_yield", "pe", "pb", "debt_equity", "gross_margin",
        "net_margin", "beneish_mscore", "probm", "altman_zscore",
        "mom_2_12", "mom_12", "mom_1",
    ):
        if c in out.columns:
            out[c] = out[c].round(2)
    for c in ("combined_rank", "value_rank"):
        if c in out.columns:
            out[c] = out[c].fillna(0).astype(int)
    if "quality_score" in out.columns:
        out["quality_score"] = out["quality_score"].round(1)
    if "quality_rank" in out.columns:
        out["quality_rank"] = out["quality_rank"].round(0).fillna(0).astype(int)
    if "piotroski_fscore" in out.columns:
        out["piotroski_fscore"] = out["piotroski_fscore"].fillna(0).astype(int)

    # Convenience columns
    if "market_cap" in out.columns:
        out["market_cap_B"] = (out["market_cap"] / 1e9).round(0)
    if "enterprise_value" in out.columns:
        out["ev_B"] = (out["enterprise_value"] / 1e9).round(0)

    return out


def run_pipeline(
    max_stocks: int = 9999,
    workers: int = 10,
    min_mcap: float = MIN_MARKET_CAP,
    max_am: float = MAX_ACQUIRERS_MULTIPLE,
    min_adv20: float = MIN_ADV20,
    min_trading_days: int = MIN_TRADING_DAYS,
    skip_prefilter: bool = False,
    save_to_db: bool = True,
    progress_callback=None,
) -> Optional[pd.DataFrame]:
    """
    Execute the complete Quantitative Value pipeline.

    Pre-filters (applied before scraping to save time):
    - Sector exclusion (financials, utilities, banks)
    - Market cap >= min_mcap
    - ADV20 >= min_adv20 (liquidity)
    - Trading days >= min_trading_days

    Returns formatted DataFrame of top-ranked stocks, or None on failure.
    """
    start = time.time()

    # ── Step 1: Fetch ticker info ────────────────────────────────
    logger.info("=== Step 1: Fetching ticker info ===")
    
    if skip_prefilter:
        # Old behavior: fetch all tickers, no prefilter
        tickers = fetch_tickers()[:max_stocks]
        logger.info("Skipping prefilter. Analysing %d stocks", len(tickers))
    else:
        # New behavior: prefilter before scraping
        ticker_info = fetch_ticker_info()
        logger.info("Found %d total tickers across all exchanges", len(ticker_info))
        
        logger.info("=== Step 1b: Pre-filtering universe ===")
        tickers = prefilter_universe(
            ticker_info,
            min_mcap=min_mcap,
            min_adv20=min_adv20,
            min_trading_days=min_trading_days,
            max_workers=workers * 2,
        )[:max_stocks]
        logger.info("After prefilter: %d stocks to analyse", len(tickers))
    
    if not tickers:
        logger.error("No tickers passed prefilter criteria.")
        return None

    logger.info("=== Step 2: Scraping CafeF ===")
    all_data, failed = ingest_all(tickers, max_workers=workers, progress_callback=progress_callback)
    if not all_data:
        logger.error("No data retrieved. Check network connectivity.")
        return None

    # ── Step 3: Clean ────────────────────────────────────────────
    logger.info("=== Step 3: Cleaning data ===")
    df = pd.DataFrame(all_data)
    df = clean_data(df)
    logger.info("Data for %d / %d stocks", len(df), len(tickers))

    # ── Step 4: Features ─────────────────────────────────────────
    logger.info("=== Step 4: Computing features ===")
    df = compute_features(df)

    # ── Step 5: Sector exclusion ─────────────────────────────────
    logger.info("=== Step 5: Sector exclusion ===")
    n_before_sector = len(df)
    df = remove_sectors(df)
    n_after_sector = len(df)

    # ── Step 6: Fraud detection ──────────────────────────────────
    logger.info("=== Step 6: Fraud detection (Beneish M-Score) ===")
    df = compute_fraud_scores(df)
    df = remove_manipulators(df)
    n_after_fraud = len(df)

    # ── Step 7: Safety scores ────────────────────────────────────
    logger.info("=== Step 7: Safety scores (Altman Z, Piotroski F) ===")
    df = compute_safety_scores(df)
    n_before_safety = len(df)
    df = apply_safety_filters(df, min_zscore=1.81, min_fscore=5)
    logger.info("Safety filters: %d → %d", n_before_safety, len(df))

    # ── Step 8: Quality ranking ──────────────────────────────────
    logger.info("=== Step 8: Quality ranking ===")
    df = compute_quality_rank(df)

    # ── Step 9: Value factor ─────────────────────────────────────
    logger.info("=== Step 9: Value factor (Acquirer's Multiple) ===")
    df = compute_acquirers_multiple(df)

    # ── Step 10: Momentum ────────────────────────────────────────
    logger.info("=== Step 10: Momentum (2-12 month signal) ===")
    df = compute_momentum_scores(df)
    n_before_mom = len(df)
    df = remove_negative_momentum(df)
    logger.info("Momentum filter: %d → %d", n_before_mom, len(df))

    # ── Step 11: Filter universe ─────────────────────────────────
    logger.info("=== Step 11: Universe filters ===")
    filtered = filter_universe(df, min_mcap=min_mcap, max_am=max_am)

    # ── Step 12: Final ranking ───────────────────────────────────
    logger.info("=== Step 12: Final ranking ===")
    ranked = rank_stocks(filtered)

    if ranked.empty:
        logger.warning("No stocks passed all filters.")
        return None

    result = format_output(ranked.head(PORTFOLIO_SIZE))

    # ── Save to database ─────────────────────────────────────────
    if save_to_db:
        try:
            from quant_value_vn.database.queries import save_run, save_portfolio
            run_id = save_run(result, total_stocks=len(all_data), passed_filter=len(filtered))
            save_portfolio(run_id, result["ticker"].tolist())
            logger.info("Saved as Run #%d", run_id)
        except Exception as exc:
            logger.warning("DB save failed: %s", exc)

    # ── Save CSV ─────────────────────────────────────────────────
    fname = f"vietnam_value_screen_{datetime.now():%Y-%m-%d}.csv"
    result.to_csv(fname, index=False, encoding="utf-8-sig")
    logger.info("Saved CSV: %s", fname)

    elapsed = time.time() - start
    logger.info(
        "Pipeline complete in %.0fs: %d pre-filtered → %d scraped → %d after sectors → "
        "%d after fraud → %d filtered → top %d ranked",
        elapsed, len(tickers), len(all_data), n_after_sector, n_after_fraud,
        len(filtered), len(result),
    )

    return result


def main():
    """CLI entry point."""
    ap = argparse.ArgumentParser(description="Vietnam Quantitative Value Stock Screener")
    ap.add_argument("--max-stocks", type=int, default=9999, help="Max stocks to analyse")
    ap.add_argument("--workers", "-w", type=int, default=10, help="Parallel workers")
    ap.add_argument("--quick", action="store_true", help="Quick mode: only 30 stocks")
    ap.add_argument("--no-db", action="store_true", help="Skip saving to Supabase")
    ap.add_argument("--skip-prefilter", action="store_true", 
                    help="Skip liquidity/sector prefilter (slower, scans all stocks)")
    ap.add_argument("--min-mcap", type=float, default=MIN_MARKET_CAP / 1e9,
                    help="Minimum market cap in billion VND (default: 500)")
    ap.add_argument("--min-adv", type=float, default=MIN_ADV20 / 1e9,
                    help="Minimum 20-day avg daily value in billion VND (default: 5)")
    ap.add_argument("--min-days", type=int, default=MIN_TRADING_DAYS,
                    help="Minimum trading days out of 60 (default: 50)")
    args = ap.parse_args()

    max_stocks = 30 if args.quick else args.max_stocks

    result = run_pipeline(
        max_stocks=max_stocks,
        workers=args.workers,
        min_mcap=args.min_mcap * 1e9,
        min_adv20=args.min_adv * 1e9,
        min_trading_days=args.min_days,
        skip_prefilter=args.skip_prefilter,
        save_to_db=not args.no_db,
    )

    if result is not None:
        show = [
            "ticker", "combined_rank", "acquirers_multiple", "quality_score",
            "beneish_mscore", "market_cap_B", "pe", "pb",
            "roa", "gross_profitability", "accruals", "roic", "debt_equity",
        ]
        avail = [c for c in show if c in result.columns]

        pd.set_option("display.max_columns", 15)
        pd.set_option("display.width", 160)
        pd.set_option("display.float_format", "{:.2f}".format)

        print("=" * 70)
        print("  TOP 30 QUANTITATIVE VALUE STOCKS — VIETNAM")
        print("=" * 70)
        print(result[avail].to_string(index=False))


if __name__ == "__main__":
    main()
