"""
Test script to compare CafeF vs Vietstock for historical financial data.

Checks how many years of data each source provides for a sample of stocks.
"""

import asyncio
import logging
import httpx
from quant_value_vn.pipeline.ingest import (
    fetch_fundamental_data,
    fetch_income_statement,
    fetch_balance_sheet,
    fetch_cash_flow,
    _HEADERS,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Test tickers (large caps with likely complete data)
TEST_TICKERS = ["VNM", "VIC", "VHM", "VCB", "HPG", "FPT", "MWG", "GAS", "SAB", "VRE"]


async def test_cafef_years(ticker: str) -> dict:
    """Test how many years of data CafeF provides."""
    years_found = {
        "income": [],
        "balance": [],
        "cashflow": [],
    }

    async with httpx.AsyncClient(headers=_HEADERS, follow_redirects=True) as client:
        # Test income statement years 2020-2026
        for year in range(2020, 2027):
            inc_result = await fetch_income_statement(client, ticker, year)
            if inc_result:
                years_found["income"].append(year)

            bs_result = await fetch_balance_sheet(client, ticker, year)
            if bs_result:
                years_found["balance"].append(year)

            cf_result = await fetch_cash_flow(client, ticker, year)
            if cf_result:
                years_found["cashflow"].append(year)

    return years_found


async def test_full_data(ticker: str) -> dict:
    """Test full fundamental data fetch."""
    async with httpx.AsyncClient(headers=_HEADERS, follow_redirects=True) as client:
        data = await fetch_fundamental_data(client, ticker)
        return data


def count_years(data: dict) -> dict:
    """Count how many years of data we have for key metrics."""
    revenue_years = []
    for suffix in [""] + [f"_y{i}" for i in range(1, 5)]:
        key = f"revenue{suffix}"
        if data.get(key):
            revenue_years.append(suffix or "current")

    return {
        "revenue_years": revenue_years,
        "revenue_count": len(revenue_years),
    }


async def main():
    logger.info("=" * 70)
    logger.info("Testing CafeF Historical Data Coverage")
    logger.info("=" * 70)

    # Test 1: Check year coverage for each ticker
    logger.info("\n### Test 1: Year Coverage by Statement Type")
    logger.info("-" * 70)

    for ticker in TEST_TICKERS:
        years = await test_cafef_years(ticker)
        logger.info(
            f"{ticker}: Income={len(years['income'])} years {years['income']}, "
            f"Balance={len(years['balance'])} years, "
            f"Cashflow={len(years['cashflow'])} years"
        )

    # Test 2: Full data fetch
    logger.info("\n### Test 2: Full Fundamental Data Fetch")
    logger.info("-" * 70)

    for ticker in TEST_TICKERS[:5]:  # Just first 5 for brevity
        data = await test_full_data(ticker)
        if data:
            counts = count_years(data)
            logger.info(
                f"{ticker}: {counts['revenue_count']} years of revenue data "
                f"({counts['revenue_years']})"
            )
            logger.info(f"  Data year: {data.get('data_year_annual')}")
        else:
            logger.warning(f"{ticker}: No data retrieved")

    logger.info("\n" + "=" * 70)
    logger.info("Test Complete")
    logger.info("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
