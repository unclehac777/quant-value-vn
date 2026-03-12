"""
Data ingestion from CafeF.vn — **async** with httpx + asyncio.

Functions:
- fetch_tickers()                  → list of tickers (Wifeed, sync)
- async fetch_income_statement()   → income statement dict
- async fetch_balance_sheet()      → balance sheet dict
- async fetch_cash_flow()          → cash flow dict
- async fetch_price()              → latest adjusted close
- async fetch_fundamental_data()   → combined 2-year data for one ticker
- ingest_all()                     → run async ingestion, return sync result

Data source: CafeF.vn (web scraping — no API key required)
Ticker source: Wifeed.vn API (free, no auth for ticker list)

Speed: ~10x faster than sync requests thanks to httpx + asyncio.gather
       with configurable concurrency via asyncio.Semaphore.
"""

import re
import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional

import httpx
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup

from quant_value_vn.config import (
    CAFEF_BASE_URL, WIFEED_TICKER_URL, DEFAULT_EXCHANGES,
    SCRAPER_DELAY, SCRAPER_TIMEOUT, SCRAPER_WORKERS, FALLBACK_TICKERS,
    EXCLUDED_SECTORS, MIN_MARKET_CAP, MIN_ADV20, MIN_TRADING_DAYS,
    PREFILTER_LOOKBACK_DAYS,
)

logger = logging.getLogger(__name__)

# ── Shared headers ───────────────────────────────────────────────────

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
}


# ── Helpers ──────────────────────────────────────────────────────────

def parse_vn_number(text: str) -> Optional[float]:
    """Parse Vietnamese formatted number (e.g. '1.234.567,89' or '(123)')."""
    if not text or not text.strip():
        return None
    t = text.strip()
    neg = False
    if t.startswith("(") and t.endswith(")"):
        neg = True
        t = t[1:-1].strip()
    if t.startswith("-"):
        neg = True
        t = t[1:].strip()
    t = t.replace(".", "").replace(",", ".")
    try:
        v = float(t)
        return -v if neg else v
    except ValueError:
        return None


def parse_us_number(text: str) -> Optional[float]:
    """Parse US formatted number (e.g. '1,234,567.89'). Used for JSON APIs."""
    if not text or not text.strip():
        return None
    t = text.strip().replace(",", "")
    try:
        return float(t)
    except ValueError:
        return None


async def _http_get(
    client: httpx.AsyncClient, url: str, timeout: int = SCRAPER_TIMEOUT,
) -> Optional[httpx.Response]:
    """Async HTTP GET with retry and polite delay."""
    await asyncio.sleep(SCRAPER_DELAY)
    for _ in range(2):
        try:
            r = await client.get(url, timeout=timeout, follow_redirects=True)
            if r.status_code == 200:
                return r
        except httpx.HTTPError:
            await asyncio.sleep(1)
    return None


def _biggest_table(soup: BeautifulSoup, min_rows: int = 15):
    """Return the table element with the most rows (>= min_rows)."""
    best, best_n = None, 0
    for tbl in soup.find_all("table"):
        n = len(tbl.find_all("tr"))
        if n > best_n and n >= min_rows:
            best, best_n = tbl, n
    return best


# ── Ticker List with Metadata (sync — one request per exchange) ─────

def fetch_ticker_info(exchanges: List[str] = None) -> List[Dict]:
    """
    Fetch Vietnamese stock tickers with sector and market cap from Wifeed API.
    Falls back to hardcoded list if API is unreachable.
    
    Returns list of dicts: {ticker, sector, market_cap, exchange}
    """
    if exchanges is None:
        exchanges = DEFAULT_EXCHANGES
    
    all_stocks = []
    
    try:
        for exchange in exchanges:
            resp = httpx.get(
                WIFEED_TICKER_URL,
                params={"loaidn": 1, "san": exchange},
                headers={"Accept": "application/json"},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            
            if "data" in data:
                for item in data["data"]:
                    if item.get("code") and item.get("san") == exchange:
                        all_stocks.append({
                            "ticker": item["code"],
                            "sector": (item.get("nganh") or item.get("sector") or "").lower().strip(),
                            "market_cap": float(item.get("vonhoa") or 0) * 1e9,  # API returns billions
                            "exchange": exchange,
                        })
                logger.info("Fetched %d tickers from %s", 
                           sum(1 for i in data["data"] if i.get("san") == exchange), 
                           exchange)
        
        if not all_stocks:
            return [{"ticker": t, "sector": "", "market_cap": 0, "exchange": ""} 
                    for t in FALLBACK_TICKERS]
            
        logger.info("Total: %d tickers from %s", len(all_stocks), ", ".join(exchanges))
        return sorted(all_stocks, key=lambda x: x["ticker"])
        
    except Exception as exc:
        logger.warning("Wifeed API failed (%s), using fallback list", exc)
        return [{"ticker": t, "sector": "", "market_cap": 0, "exchange": ""} 
                for t in FALLBACK_TICKERS]


def fetch_tickers(exchanges: List[str] = None) -> List[str]:
    """
    Fetch Vietnamese stock tickers from Wifeed API.
    Falls back to hardcoded list if API is unreachable.
    
    Fetches from all requested exchanges (default: HOSE, HNX, UPCOM).
    Returns list of ticker symbols only (for backward compatibility).
    """
    info = fetch_ticker_info(exchanges)
    return [item["ticker"] for item in info]


# ── Income Statement ─────────────────────────────────────────────────

async def fetch_income_statement(
    client: httpx.AsyncClient, ticker: str, year: int,
) -> Optional[tuple[int, Dict]]:
    """
    Scrape income statement from CafeF for one ticker/year.

    Returns tuple (actual_year_found, dict) with: revenue, gross_profit,
    operating_profit, interest_expense, pretax_profit, net_income,
    net_income_parent, eps, selling_expense, admin_expense, cogs,
    tax_expense, sga.

    Returns None if no data found for the requested year.
    """
    url = (
        f"{CAFEF_BASE_URL}/bao-cao-tai-chinh/{ticker}/IncSta/{year}"
        f"/0/0/0/ket-qua-hoat-dong-kinh-doanh-.chn"
    )
    r = await _http_get(client, url)
    if not r:
        return None

    soup = BeautifulSoup(r.text, "lxml")
    
    # ── Find column index for requested year ──
    # CafeF puts years in `<td class="h_t">2026</td>` inside `tblGridData`
    year_col_idx = None
    grid_tbl = soup.find("table", id="tblGridData")
    if grid_tbl:
        header_row = grid_tbl.find("tr")
        if header_row:
            h_cells = header_row.find_all("td")
            for i, cell in enumerate(h_cells):
                cell_text = cell.get_text(strip=True)
                if str(year) in cell_text:
                    year_col_idx = i
                    break

    # If the requested year isn't in the header, no data for that year
    if year_col_idx is None:
        return None

    tbl = _biggest_table(soup)
    if not tbl:
        return None

    res: Dict = {}
    for row in tbl.find_all("tr"):
        cells = row.find_all(["td", "th"])
        # ensure we have enough columns
        if len(cells) <= year_col_idx:
            continue
            
        lbl = cells[0].get_text(strip=True).lower()
        val = parse_vn_number(cells[year_col_idx].get_text(strip=True))

        m = re.match(r"^(\d+)\.", lbl)
        rn = int(m.group(1)) if m else None

        if rn == 1 and "doanh thu bán hàng" in lbl:
            res["gross_revenue"] = val
        elif rn == 3 and "doanh thu thuần" in lbl:
            res["revenue"] = val
        elif rn == 4 and "giá vốn" in lbl:
            res["cogs"] = val
        elif rn == 5 and "lợi nhuận gộp" in lbl:
            res["gross_profit"] = val
        elif rn == 6 and "doanh thu" in lbl:
            res["financial_income"] = val
        elif rn == 7 and "chi phí tài chính" in lbl:
            res["financial_expense"] = val
        elif "chi phí lãi vay" in lbl:
            res["interest_expense"] = val
        elif rn == 8 and ("chi phí bán hàng" in lbl or "bán hàng" in lbl):
            res["selling_expense"] = val
        elif rn == 9 and ("chi phí quản lý" in lbl or "quản lý doanh nghiệp" in lbl):
            res["admin_expense"] = val
        elif "chi phí bán hàng" in lbl and "selling_expense" not in res:
            res["selling_expense"] = val
        elif ("chi phí quản lý" in lbl or "chi phí quản lý doanh nghiệp" in lbl) and "admin_expense" not in res:
            res["admin_expense"] = val
        elif rn == 11 and "lợi nhuận thuần" in lbl:
            res["operating_profit"] = val
        elif rn == 15 and "trước thuế" in lbl:
            res["pretax_profit"] = val
        elif rn == 17 and "chi phí thuế" in lbl:
            res["tax_expense"] = val
        elif rn == 18 and "lợi nhuận sau thuế" in lbl:
            res["net_income"] = val
        elif rn == 19:
            if "lợi nhuận sau thuế" in lbl and "công ty mẹ" in lbl:
                res["net_income_parent"] = val
        elif rn == 21 and "lãi cơ bản" in lbl:
            res["eps"] = val

        # Bank-specific fallbacks
        elif "thu nhập lãi thuần" in lbl and "revenue" not in res:
            res["revenue"] = val
        elif "tổng thu nhập hoạt động" in lbl and "revenue" not in res:
            res["revenue"] = val
        elif "lợi nhuận trước thuế" in lbl and "pretax_profit" not in res:
            res["pretax_profit"] = val
        elif "lãi cơ bản" in lbl and "eps" not in res:
            res["eps"] = val

    # SGA = Selling + Admin
    se = abs(res.get("selling_expense") or 0)
    ae = abs(res.get("admin_expense") or 0)
    if se or ae:
        res["sga"] = se + ae

    # Fallback for net_income_parent
    if "net_income_parent" not in res and "net_income" in res:
        res["net_income_parent"] = res["net_income"]

    if not (res.get("revenue") or res.get("operating_profit")):
        return None

    return (year, res)


# ── Balance Sheet ─────────────────────────────────────────────────────

async def fetch_balance_sheet(
    client: httpx.AsyncClient, ticker: str, year: int,
) -> Optional[tuple[int, Dict]]:
    """
    Scrape balance sheet from CafeF for one ticker/year.

    Returns tuple (actual_year_found, dict) with: cash,
    short_term_investments, total_assets, short_term_debt, long_term_debt,
    total_liabilities, equity, receivables, inventory, ppe,
    current_assets, current_liabilities, depreciation_accumulated.

    Returns None if no data found for the requested year.
    """
    url = (
        f"{CAFEF_BASE_URL}/bao-cao-tai-chinh/{ticker}/BSheet/{year}"
        f"/0/0/0/bao-cao-tai-chinh-.chn"
    )
    r = await _http_get(client, url)
    if not r:
        return None

    soup = BeautifulSoup(r.text, "lxml")
    
    year_col_idx = None
    grid_tbl = soup.find("table", id="tblGridData")
    if grid_tbl:
        header_row = grid_tbl.find("tr")
        if header_row:
            h_cells = header_row.find_all("td")
            for i, cell in enumerate(h_cells):
                if str(year) in cell.get_text(strip=True):
                    year_col_idx = i
                    break

    if year_col_idx is None:
        return None

    tbl = _biggest_table(soup, min_rows=30)
    if not tbl:
        return None

    res: Dict = {}
    for row in tbl.find_all("tr"):
        cells = row.find_all(["td", "th"])
        if len(cells) <= year_col_idx:
            continue
            
        lbl = cells[0].get_text(strip=True).lower()
        val = parse_vn_number(cells[year_col_idx].get_text(strip=True))

        if "tiền và các khoản tương đương" in lbl and "cash" not in res:
            res["cash"] = val
        elif "đầu tư tài chính ngắn hạn" in lbl and lbl.lstrip().startswith("ii"):
            res["short_term_investments"] = val
        elif ("phải thu ngắn hạn" in lbl and lbl.lstrip().startswith("iii")
              and "receivables" not in res):
            res["receivables"] = val
        elif "phải thu khách hàng" in lbl and "receivables" not in res:
            res["receivables"] = val
        elif "hàng tồn kho" in lbl and "inventory" not in res:
            res["inventory"] = val
        elif lbl.lstrip().startswith("a") and "tài sản ngắn hạn" in lbl:
            res["current_assets"] = val
        elif "tài sản cố định" in lbl and ("hữu hình" in lbl or "vô hình" in lbl or lbl.lstrip().startswith("ii")):
            if "hữu hình" in lbl and "nguyên giá" not in lbl:
                res["ppe"] = val
            elif "vô hình" in lbl and "nguyên giá" not in lbl:
                res["intangible_assets"] = val
            elif lbl.lstrip().startswith("ii") and "fixed_assets" not in res:
                res["fixed_assets"] = val
        elif ("hao mòn lũy kế" in lbl or "khấu hao lũy kế" in lbl) and "depreciation_accumulated" not in res:
            res["depreciation_accumulated"] = val
        elif "vay và nợ" in lbl and "ngắn hạn" in lbl:
            res["short_term_debt"] = val
        elif "vay và nợ" in lbl and "dài hạn" in lbl:
            res["long_term_debt"] = val
        elif "nợ ngắn hạn" in lbl and "current_liabilities" not in res:
            res["current_liabilities"] = val
        elif "tổng cộng tài sản" in lbl:
            res["total_assets"] = val
        elif lbl.lstrip().startswith("c") and "nợ phải trả" in lbl:
            res["total_liabilities"] = val
        elif lbl.lstrip().startswith("d") and "vốn chủ sở hữu" in lbl:
            res["equity"] = val
        elif "lợi ích cổ đông không kiểm soát" in lbl and "minority_interest" not in res:
            res["minority_interest"] = val
        elif "cơ cấu vốn" in lbl and "ưu đãi" in lbl:
             res["preferred_stock"] = val
        elif "lợi nhuận sau thuế chưa phân phối" in lbl and "retained_earnings" not in res:
            res["retained_earnings"] = val
        elif "lợi nhuận chưa phân phối" in lbl and "retained_earnings" not in res:
            res["retained_earnings"] = val

    if not res.get("total_assets"):
        return None

    return (year, res)


# ── Cash Flow Statement ──────────────────────────────────────────────

async def fetch_cash_flow(
    client: httpx.AsyncClient, ticker: str, year: int,
) -> Optional[tuple[int, Dict]]:
    """
    Scrape cash flow statement from CafeF for one ticker/year.

    Returns tuple (actual_year_found, dict) with: operating_cash_flow,
    capex, depreciation.

    Returns None if no data found for the requested year.
    """
    url = (
        f"{CAFEF_BASE_URL}/bao-cao-tai-chinh/{ticker}/CashFlow/{year}"
        f"/0/0/0/bao-cao-tai-chinh-.chn"
    )
    r = await _http_get(client, url)
    if not r:
        return None

    soup = BeautifulSoup(r.text, "lxml")
    
    year_col_idx = None
    grid_tbl = soup.find("table", id="tblGridData")
    if grid_tbl:
        header_row = grid_tbl.find("tr")
        if header_row:
            h_cells = header_row.find_all("td")
            for i, cell in enumerate(h_cells):
                if str(year) in cell.get_text(strip=True):
                    year_col_idx = i
                    break

    if year_col_idx is None:
        return None

    tbl = _biggest_table(soup, min_rows=8)
    if not tbl:
        return None

    res: Dict = {}
    for row in tbl.find_all("tr"):
        cells = row.find_all(["td", "th"])
        if len(cells) <= year_col_idx:
            continue
            
        lbl = cells[0].get_text(strip=True).lower()
        val = parse_vn_number(cells[year_col_idx].get_text(strip=True))

        if "lưu chuyển tiền thuần từ hoạt động kinh doanh" in lbl:
            res["operating_cash_flow"] = val
        elif "khấu hao" in lbl and "depreciation" not in res:
            res["depreciation"] = val
        elif ("mua sắm" in lbl or "xây dựng" in lbl) and (
            "tài sản cố định" in lbl or "tscd" in lbl.replace(" ", "")
        ):
            res["capex"] = val

    if not res:
        return None

    return (year, res)


# ── Quarterly Data & TTM ─────────────────────────────────────────────

# Income statement fields that should be SUMMED across quarters (flow metrics)
_FLOW_FIELDS = {
    "revenue", "gross_revenue", "cogs", "gross_profit", "financial_income",
    "financial_expense", "interest_expense", "selling_expense", "admin_expense",
    "operating_profit", "pretax_profit", "tax_expense", "net_income",
    "net_income_parent", "sga",
}

# Cash flow fields to sum
_CF_FLOW_FIELDS = {"operating_cash_flow", "capex", "depreciation"}


def _sum_quarterly_dicts(quarters: list[Dict]) -> Dict:
    """Sum flow-metric values across quarterly dicts (for TTM)."""
    result: Dict = {}
    for q in quarters:
        for k, v in q.items():
            if v is None:
                continue
            if k in _FLOW_FIELDS or k in _CF_FLOW_FIELDS:
                result[k] = (result.get(k) or 0) + v
            else:
                # For non-flow fields (like EPS), take the latest
                result[k] = v
    # Recompute SGA from summed components
    se = abs(result.get("selling_expense") or 0)
    ae = abs(result.get("admin_expense") or 0)
    if se or ae:
        result["sga"] = se + ae
    return result


def _parse_income_row(lbl: str, rn: int | None) -> str | None:
    """Map income statement row label to field name."""
    if rn == 1 and "doanh thu bán hàng" in lbl:
        return "gross_revenue"
    elif rn == 3 and "doanh thu thuần" in lbl:
        return "revenue"
    elif rn == 4 and "giá vốn" in lbl:
        return "cogs"
    elif rn == 5 and "lợi nhuận gộp" in lbl:
        return "gross_profit"
    elif rn == 6 and "doanh thu" in lbl:
        return "financial_income"
    elif rn == 7 and "chi phí tài chính" in lbl:
        return "financial_expense"
    elif "chi phí lãi vay" in lbl:
        return "interest_expense"
    elif rn == 8 and ("chi phí bán hàng" in lbl or "bán hàng" in lbl):
        return "selling_expense"
    elif rn == 9 and ("chi phí quản lý" in lbl or "quản lý doanh nghiệp" in lbl):
        return "admin_expense"
    elif "chi phí bán hàng" in lbl:
        return "selling_expense"
    elif "chi phí quản lý" in lbl:
        return "admin_expense"
    elif rn == 11 and "lợi nhuận thuần" in lbl:
        return "operating_profit"
    elif rn == 15 and "trước thuế" in lbl:
        return "pretax_profit"
    elif rn == 17 and "chi phí thuế" in lbl:
        return "tax_expense"
    elif rn == 18 and "lợi nhuận sau thuế" in lbl:
        return "net_income"
    elif rn == 19 and "lợi nhuận sau thuế" in lbl and "công ty mẹ" in lbl:
        return "net_income_parent"
    elif rn == 21 and "lãi cơ bản" in lbl:
        return "eps"
    # Bank fallbacks
    elif "thu nhập lãi thuần" in lbl:
        return "revenue"
    elif "tổng thu nhập hoạt động" in lbl:
        return "revenue"
    elif "lợi nhuận trước thuế" in lbl:
        return "pretax_profit"
    return None


async def fetch_quarterly_income(
    client: httpx.AsyncClient, ticker: str, year: int, quarter: int = 4,
) -> Optional[tuple[int, list[Dict]]]:
    """
    Fetch quarterly income statement from CafeF.

    Returns (year, [q1_dict, q2_dict, q3_dict, q4_dict]) for the 4 quarters
    shown on the page. Returns None if page has no data.
    """
    url = (
        f"{CAFEF_BASE_URL}/bao-cao-tai-chinh/{ticker}/IncSta/{year}"
        f"/{quarter}/0/0/ket-qua-hoat-dong-kinh-doanh-.chn"
    )
    r = await _http_get(client, url)
    if not r:
        return None

    soup = BeautifulSoup(r.text, "lxml")

    # Find quarter column indices from header
    # Headers look like: "Quý 1- 2025", "Quý 2- 2025", etc.
    grid_tbl = soup.find("table", id="tblGridData")
    if not grid_tbl:
        return None

    header_row = grid_tbl.find("tr")
    if not header_row:
        return None

    h_cells = header_row.find_all("td")
    q_col_indices = []  # list of (quarter_num, col_idx)
    for i, cell in enumerate(h_cells):
        text = cell.get_text(strip=True)
        qm = re.search(r"Quý\s*(\d+)\s*-\s*(\d{4})", text)
        if qm:
            q_col_indices.append((int(qm.group(1)), int(qm.group(2)), i))

    if len(q_col_indices) < 4:
        return None

    # Parse data table
    tbl = _biggest_table(soup)
    if not tbl:
        return None

    # Build a dict per quarter column
    q_dicts: list[Dict] = [{} for _ in range(len(q_col_indices))]
    has_data = [False] * len(q_col_indices)

    for row in tbl.find_all("tr"):
        cells = row.find_all(["td", "th"])
        if len(cells) < 2:
            continue

        lbl = cells[0].get_text(strip=True).lower()
        m = re.match(r"^(\d+)\.", lbl)
        rn = int(m.group(1)) if m else None
        field = _parse_income_row(lbl, rn)
        if not field:
            continue

        for qi, (qn, qy, ci) in enumerate(q_col_indices):
            if ci < len(cells):
                val = parse_vn_number(cells[ci].get_text(strip=True))
                if val is not None and val != 0:
                    has_data[qi] = True
                if field not in q_dicts[qi]:  # don't overwrite
                    q_dicts[qi][field] = val

    # Compute SGA for each quarter
    for qd in q_dicts:
        se = abs(qd.get("selling_expense") or 0)
        ae = abs(qd.get("admin_expense") or 0)
        if se or ae:
            qd["sga"] = se + ae
        if "net_income_parent" not in qd and "net_income" in qd:
            qd["net_income_parent"] = qd["net_income"]

    # Check we have meaningful data
    if not any(has_data):
        return None

    # Return the actual year of the data (from the last quarter column)
    actual_year = q_col_indices[-1][1]  # year from last column header
    return (actual_year, q_dicts)


async def fetch_quarterly_balance_sheet(
    client: httpx.AsyncClient, ticker: str, year: int, quarter: int = 4,
) -> Optional[tuple[int, Dict]]:
    """
    Fetch quarterly balance sheet — returns latest quarter's data.

    Balance sheet is a snapshot, not summed across quarters.
    """
    url = (
        f"{CAFEF_BASE_URL}/bao-cao-tai-chinh/{ticker}/BSheet/{year}"
        f"/{quarter}/0/0/bao-cao-tai-chinh-.chn"
    )
    r = await _http_get(client, url)
    if not r:
        return None

    soup = BeautifulSoup(r.text, "lxml")

    # Find the latest quarter column
    grid_tbl = soup.find("table", id="tblGridData")
    if not grid_tbl:
        return None

    header_row = grid_tbl.find("tr")
    if not header_row:
        return None

    h_cells = header_row.find_all("td")
    last_q_idx = None
    last_q_year = None
    for i, cell in enumerate(h_cells):
        text = cell.get_text(strip=True)
        qm = re.search(r"Quý\s*(\d+)\s*-\s*(\d{4})", text)
        if qm:
            last_q_idx = i
            last_q_year = int(qm.group(2))

    if last_q_idx is None or last_q_year is None:
        return None

    tbl = _biggest_table(soup, min_rows=30)
    if not tbl:
        return None

    res: Dict = {}
    for row in tbl.find_all("tr"):
        cells = row.find_all(["td", "th"])
        if len(cells) <= last_q_idx:
            continue

        lbl = cells[0].get_text(strip=True).lower()
        val = parse_vn_number(cells[last_q_idx].get_text(strip=True))

        if "tiền và các khoản tương đương" in lbl and "cash" not in res:
            res["cash"] = val
        elif "đầu tư tài chính ngắn hạn" in lbl and lbl.lstrip().startswith("ii"):
            res["short_term_investments"] = val
        elif ("phải thu ngắn hạn" in lbl and lbl.lstrip().startswith("iii")
              and "receivables" not in res):
            res["receivables"] = val
        elif "phải thu khách hàng" in lbl and "receivables" not in res:
            res["receivables"] = val
        elif "hàng tồn kho" in lbl and "inventory" not in res:
            res["inventory"] = val
        elif lbl.lstrip().startswith("a") and "tài sản ngắn hạn" in lbl:
            res["current_assets"] = val
        elif "tài sản cố định" in lbl and ("hữu hình" in lbl or "vô hình" in lbl or lbl.lstrip().startswith("ii")):
            if "hữu hình" in lbl and "nguyên giá" not in lbl:
                res["ppe"] = val
            elif "vô hình" in lbl and "nguyên giá" not in lbl:
                res["intangible_assets"] = val
            elif lbl.lstrip().startswith("ii") and "fixed_assets" not in res:
                res["fixed_assets"] = val
        elif ("hao mòn lũy kế" in lbl or "khấu hao lũy kế" in lbl) and "depreciation_accumulated" not in res:
            res["depreciation_accumulated"] = val
        elif "vay và nợ" in lbl and "ngắn hạn" in lbl:
            res["short_term_debt"] = val
        elif "vay và nợ" in lbl and "dài hạn" in lbl:
            res["long_term_debt"] = val
        elif "nợ ngắn hạn" in lbl and "current_liabilities" not in res:
            res["current_liabilities"] = val
        elif "tổng cộng tài sản" in lbl:
            res["total_assets"] = val
        elif lbl.lstrip().startswith("c") and "nợ phải trả" in lbl:
            res["total_liabilities"] = val
        elif lbl.lstrip().startswith("d") and "vốn chủ sở hữu" in lbl:
            res["equity"] = val
        elif "lợi ích cổ đông không kiểm soát" in lbl and "minority_interest" not in res:
            res["minority_interest"] = val
        elif "cơ cấu vốn" in lbl and "ưu đãi" in lbl:
             res["preferred_stock"] = val
        elif "lợi nhuận sau thuế chưa phân phối" in lbl and "retained_earnings" not in res:
            res["retained_earnings"] = val
        elif "lợi nhuận chưa phân phối" in lbl and "retained_earnings" not in res:
            res["retained_earnings"] = val

    if not res.get("total_assets"):
        return None

    return (last_q_year, res)


async def fetch_quarterly_cash_flow(
    client: httpx.AsyncClient, ticker: str, year: int, quarter: int = 4,
) -> Optional[tuple[int, list[Dict]]]:
    """
    Fetch quarterly cash flow — returns all 4 quarters for TTM summing.
    """
    url = (
        f"{CAFEF_BASE_URL}/bao-cao-tai-chinh/{ticker}/CashFlow/{year}"
        f"/{quarter}/0/0/bao-cao-tai-chinh-.chn"
    )
    r = await _http_get(client, url)
    if not r:
        return None

    soup = BeautifulSoup(r.text, "lxml")

    grid_tbl = soup.find("table", id="tblGridData")
    if not grid_tbl:
        return None

    header_row = grid_tbl.find("tr")
    if not header_row:
        return None

    h_cells = header_row.find_all("td")
    q_col_indices = []
    for i, cell in enumerate(h_cells):
        text = cell.get_text(strip=True)
        qm = re.search(r"Quý\s*(\d+)\s*-\s*(\d{4})", text)
        if qm:
            q_col_indices.append((int(qm.group(1)), int(qm.group(2)), i))

    if len(q_col_indices) < 4:
        return None

    tbl = _biggest_table(soup, min_rows=8)
    if not tbl:
        return None

    q_dicts: list[Dict] = [{} for _ in range(len(q_col_indices))]
    has_data = [False] * len(q_col_indices)

    for row in tbl.find_all("tr"):
        cells = row.find_all(["td", "th"])
        if len(cells) < 2:
            continue

        lbl = cells[0].get_text(strip=True).lower()
        field = None
        if "lưu chuyển tiền thuần từ hoạt động kinh doanh" in lbl:
            field = "operating_cash_flow"
        elif "khấu hao" in lbl:
            field = "depreciation"
        elif ("mua sắm" in lbl or "xây dựng" in lbl) and (
            "tài sản cố định" in lbl or "tscd" in lbl.replace(" ", "")
        ):
            field = "capex"

        if not field:
            continue

        for qi, (qn, qy, ci) in enumerate(q_col_indices):
            if ci < len(cells):
                val = parse_vn_number(cells[ci].get_text(strip=True))
                if val is not None and val != 0:
                    has_data[qi] = True
                if field not in q_dicts[qi]:
                    q_dicts[qi][field] = val

    if not any(has_data):
        return None

    actual_year = q_col_indices[-1][1]
    return (actual_year, q_dicts)


async def fetch_ttm_data(
    client: httpx.AsyncClient, ticker: str,
) -> Optional[tuple[Dict, int]]:
    """
    Fetch TTM (Trailing Twelve Months) data using quarterly reports.

    Returns (data_dict, data_year) or None if quarterly data unavailable.
    Data dict includes both current TTM and _prev TTM fields.
    """
    now = datetime.now()
    yr = now.year
    month = now.month

    # Determine the latest quarter likely to have been REPORTED
    # Vietnamese companies typically report ~2 months after quarter end:
    # Q4 (Dec) → reported by Feb-Mar, Q1 (Mar) → by May, etc.
    # So in March 2026, Q4/2025 data should be available.
    # Build search order: most recent likely quarter first
    search_quarters = []
    
    def is_likely_reported(q_year: int, q_num: int) -> bool:
        q_end_month = q_num * 3
        months_since = (yr - q_year) * 12 + month - q_end_month
        return months_since >= 2

    # Current year (2026)
    for qnum in range(4, 0, -1):
        if is_likely_reported(yr, qnum):
            search_quarters.append((yr, qnum))
            
    # Last year (2025)
    for qnum in range(4, 0, -1):
        if is_likely_reported(yr - 1, qnum):
            search_quarters.append((yr - 1, qnum))
            
    # Year before (2024)
    for qnum in range(4, 0, -1):
        search_quarters.append((yr - 2, qnum))

    if not search_quarters:
        # Early in year, add prior year quarters
        for qnum in range(4, 0, -1):
            search_quarters.append((yr - 1, qnum))

    inc_quarters = None
    inc_year = None
    for try_year, try_q in search_quarters:
        result = await fetch_quarterly_income(client, ticker, try_year, try_q)
        if result:
            inc_year, inc_quarters = result
            break

    if not inc_quarters or inc_year is None:
        return None

    # TTM = sum of all 4 quarters from the page
    d: Dict = {"ticker": ticker}
    ttm_inc = _sum_quarterly_dicts(inc_quarters)
    if not (ttm_inc.get("revenue") or ttm_inc.get("operating_profit")):
        return None
    d.update(ttm_inc)

    # Balance sheet — latest quarter snapshot (use same search order)
    for try_year, try_q in search_quarters:
        result = await fetch_quarterly_balance_sheet(client, ticker, try_year, try_q)
        if result:
            _, bs = result
            d.update(bs)
            break

    if not d.get("total_assets"):
        return None

    # Cash flow — TTM sum
    for try_year, try_q in search_quarters:
        result = await fetch_quarterly_cash_flow(client, ticker, try_year, try_q)
        if result:
            _, cf_quarters = result
            ttm_cf = _sum_quarterly_dicts(cf_quarters)
            d.update(ttm_cf)
            break

    # _prev TTM: fetch the year before the TTM year
    prev_year = inc_year - 1 if inc_year is not None else yr - 1
    for try_q in [4, 3, 2, 1]:
        result = await fetch_quarterly_income(client, ticker, prev_year, try_q)
        if result:
            _, prev_quarters = result
            prev_ttm = _sum_quarterly_dicts(prev_quarters)
            for k, v in prev_ttm.items():
                d[f"{k}_prev"] = v
            break

    # _prev balance sheet
    for try_q in [4, 3, 2, 1]:
        result = await fetch_quarterly_balance_sheet(client, ticker, prev_year, try_q)
        if result:
            _, bs_prev = result
            for k, v in bs_prev.items():
                d[f"{k}_prev"] = v
            break

    d["data_source"] = "TTM"
    d["data_year"] = inc_year
    return (d, inc_year)


# ── Stock Price ───────────────────────────────────────────────────────

async def fetch_price(client: httpx.AsyncClient, ticker: str) -> Optional[float]:
    """Latest adjusted close price from CafeF price history API."""
    now = datetime.now()
    url = (
        f"{CAFEF_BASE_URL}/Ajax/PageNew/DataHistory/PriceHistory.ashx"
        f"?Symbol={ticker}"
        f"&StartDate=01/01/{now.year}"
        f"&EndDate={now:%d/%m/%Y}"
        f"&PageIndex=1&PageSize=1"
    )
    r = await _http_get(client, url, timeout=10)
    if not r:
        return None
    try:
        data = r.json()
        if data.get("Success") and data["Data"]["Data"]:
            item = data["Data"]["Data"][0]
            p = float(item.get("GiaDieuChinh") or item.get("GiaDongCua"))
            return p * 1000
    except Exception:
        pass
    return None


async def fetch_cafef_indices(client: httpx.AsyncClient, ticker: str) -> Dict:
    """
    Fetch official financial indices from CafeF (Market Cap, Shares Outstanding).
    
    Source: https://cafef.vn/du-lieu/Ajax/PageNew/ChiSoTaiChinh.ashx?Symbol=<TICKER>
    
    Returns dict: {market_cap_cafef, shares_cafef}
    Market Cap is in Billion VND, converted to VND.
    """
    url = f"https://cafef.vn/du-lieu/Ajax/PageNew/ChiSoTaiChinh.ashx?Symbol={ticker}"
    res = {"market_cap_cafef": np.nan, "shares_cafef": np.nan}
    
    r = await _http_get(client, url, timeout=10)
    if not r:
        return res
        
    try:
        data = r.json().get("Data", [])
        for item in data:
            code = item.get("Code")
            val = parse_us_number(item.get("Value"))
            if val is None:
                continue
            
            if code == "VonHoaThiTruong":
                res["market_cap_cafef"] = val * 1e9  # CafeF returns billions
            elif code == "KlcpLuuHanh":
                res["shares_cafef"] = val
    except Exception:
        pass
        
    return res


async def fetch_price_history(
    client: httpx.AsyncClient, ticker: str, days: int = 300,
) -> Optional[List[Dict]]:
    """
    Fetch ~12 months of daily adjusted close prices from CafeF.

    Returns list of dicts [{'date': str, 'close': float}, ...],
    ordered most recent first (same order as API returns).
    """
    now = datetime.now()
    start = now - pd.Timedelta(days=days + 60)  # buffer for weekends/holidays
    url = (
        f"{CAFEF_BASE_URL}/Ajax/PageNew/DataHistory/PriceHistory.ashx"
        f"?Symbol={ticker}"
        f"&StartDate={start:%d/%m/%Y}"
        f"&EndDate={now:%d/%m/%Y}"
        f"&PageIndex=1&PageSize={days}"
    )
    r = await _http_get(client, url, timeout=10)
    if not r:
        return None
    try:
        data = r.json()
        if not data.get("Success") or not data["Data"]["Data"]:
            return None
        records = data["Data"]["Data"]
        prices = []
        for rec in records:
            p = float(rec.get("GiaDieuChinh") or rec.get("GiaDongCua") or 0)
            if p > 0:
                prices.append({
                    "date": rec.get("Ngay", ""),
                    "close": p * 1000,
                })
        return prices if len(prices) >= 20 else None
    except Exception:
        return None


# ── Liquidity Data (ADV20, trading days) ─────────────────────────────

async def fetch_liquidity(
    client: httpx.AsyncClient, ticker: str, lookback_days: int = PREFILTER_LOOKBACK_DAYS
) -> Optional[Dict]:
    """
    Fetch liquidity metrics for pre-filtering.
    
    Returns dict with:
    - adv20: 20-day average daily trading value (VND)
    - trading_days: number of trading days in lookback period
    - avg_volume: average daily volume (shares)
    """
    now = datetime.now()
    # Fetch last N calendar days of price history
    start_date = now - pd.Timedelta(days=lookback_days + 30)  # buffer for weekends/holidays
    url = (
        f"{CAFEF_BASE_URL}/Ajax/PageNew/DataHistory/PriceHistory.ashx"
        f"?Symbol={ticker}"
        f"&StartDate={start_date:%d/%m/%Y}"
        f"&EndDate={now:%d/%m/%Y}"
        f"&PageIndex=1&PageSize={lookback_days + 10}"
    )
    r = await _http_get(client, url, timeout=10)
    if not r:
        return None
    
    try:
        data = r.json()
        if not data.get("Success") or not data["Data"]["Data"]:
            return None
        
        records = data["Data"]["Data"][:lookback_days]  # most recent N days
        if len(records) < 10:  # need minimum data
            return None
        
        trading_days = len(records)
        
        # Calculate average daily value
        # API returns: GiaTriKhopLenh (daily trading value), KhoiLuongKhopLenh (volume)
        daily_values = []
        daily_volumes = []
        for rec in records:
            try:
                # GiaTriKhopLenh is already the daily trading value in VND
                daily_value = float(rec.get("GiaTriKhopLenh") or 0)
                volume = float(rec.get("KhoiLuongKhopLenh") or rec.get("KhoiLuong") or 0)
                if daily_value > 0:
                    daily_values.append(daily_value)
                if volume > 0:
                    daily_volumes.append(volume)
            except (ValueError, TypeError):
                continue
        
        if len(daily_values) < 10:
            return None
        
        # ADV20 = average of most recent 20 trading days
        recent_values = daily_values[:20]
        adv20 = sum(recent_values) / min(20, len(recent_values))
        avg_volume = sum(daily_volumes) / len(daily_volumes) if daily_volumes else 0
        
        return {
            "adv20": adv20,
            "trading_days": trading_days,
            "avg_volume": avg_volume,
        }
        
    except Exception:
        return None


# ── Sector Data (from Vietstock profile) ─────────────────────────────

VIETSTOCK_PROFILE_URL = "https://finance.vietstock.vn/{ticker}/profile.htm"

async def fetch_sector(
    client: httpx.AsyncClient, ticker: str,
) -> Optional[str]:
    """
    Fetch sector/industry classification from Vietstock profile page.
    
    Returns lowercase sector name (e.g., 'tài chính', 'công nghệ thông tin').
    Used for pre-filtering financials, utilities, etc.
    """
    url = VIETSTOCK_PROFILE_URL.format(ticker=ticker)
    r = await _http_get(client, url, timeout=10)
    if not r:
        return None
    
    try:
        soup = BeautifulSoup(r.text, "lxml")
        
        # Find sector-level div containing industry classification
        sector_div = soup.find("div", class_="sector-level")
        if sector_div:
            # Get first sector link (top-level sector like "Tài chính", "Công nghiệp")
            link = sector_div.find("a", class_="title-link")
            if link:
                return link.get_text(strip=True).lower()
        return None
        
    except Exception:
        return None


# ── Pre-Filter Universe ──────────────────────────────────────────────

async def _fetch_prefilter_data(
    tickers: List[str],
    max_workers: int = SCRAPER_WORKERS * 2,  # lighter requests, more parallelism
    progress_callback=None,
) -> Dict[str, Dict]:
    """
    Fetch liquidity AND sector data for multiple tickers in parallel.
    
    Returns dict: {ticker: {"adv20": ..., "trading_days": ..., "sector": ...}}
    """
    sem = asyncio.Semaphore(max_workers)
    results: Dict[str, Dict] = {}
    total = len(tickers)
    completed = 0
    
    async with httpx.AsyncClient(
            headers=_HEADERS, follow_redirects=True,
            limits=httpx.Limits(max_connections=200, max_keepalive_connections=100),
    ) as client:
        tasks = []
        for tk in tickers:
            async def _fetch(t=tk):
                async with sem:
                    # Fetch both liquidity and sector in parallel for each ticker
                    liq_task = fetch_liquidity(client, t)
                    sector_task = fetch_sector(client, t)
                    liq, sector = await asyncio.gather(liq_task, sector_task)
                    return t, liq, sector
            tasks.append(_fetch())
        
        for coro in asyncio.as_completed(tasks):
            ticker, liq_data, sector = await coro
            completed += 1
            
            if liq_data:
                results[ticker] = liq_data
                results[ticker]["sector"] = sector  # may be None
            
            if completed % 100 == 0:
                logger.info("Pre-filter fetch: [%d/%d]", completed, total)
            
            if progress_callback:
                progress_callback(completed, total, len(results))
    
    return results


def prefilter_universe(
    ticker_info: List[Dict],
    min_mcap: float = MIN_MARKET_CAP,
    min_adv20: float = MIN_ADV20,
    min_trading_days: int = MIN_TRADING_DAYS,
    excluded_sectors: set = None,
    max_workers: int = SCRAPER_WORKERS * 2,
    progress_callback=None,
) -> List[str]:
    """
    Pre-filter stocks BEFORE scraping financials to dramatically reduce workload.
    
    Filters applied (in order):
    1. Market cap >= min_mcap (if available from API — Wifeed doesn't provide this)
    2. Fetch liquidity + sector data from CafeF + Vietstock
    3. Sector exclusion (financials, utilities, banks, etc.)
    4. ADV20 >= min_adv20 (liquidity check)
    5. Trading days >= min_trading_days (liquidity check)
    
    Returns list of ticker symbols that passed all filters.
    """
    if excluded_sectors is None:
        excluded_sectors = EXCLUDED_SECTORS
    
    initial_count = len(ticker_info)
    candidates = ticker_info
    
    # ── Step 1: Market cap filter (if data available from API) ───
    has_mcap_data = any(t.get("market_cap", 0) > 0 for t in candidates)
    if has_mcap_data:
        passed_mcap = [
            t for t in candidates 
            if t.get("market_cap", 0) >= min_mcap or t.get("market_cap", 0) == 0
        ]
        logger.info("Pre-filter: Market cap >= %.0fB: %d → %d stocks", 
                    min_mcap / 1e9, len(candidates), len(passed_mcap))
        candidates = passed_mcap
    
    # ── Step 2: Fetch liquidity + sector data ────────────────────
    tickers_to_check = [t["ticker"] for t in candidates]
    
    if not tickers_to_check:
        return []
    
    logger.info("Pre-filter: Fetching liquidity + sector for %d stocks...", len(tickers_to_check))
    
    # Run async fetch for both liquidity and sector
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    
    if loop and loop.is_running():
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(1) as pool:
            prefilter_data = pool.submit(
                asyncio.run,
                _fetch_prefilter_data(tickers_to_check, max_workers, progress_callback),
            ).result()
    else:
        prefilter_data = asyncio.run(
            _fetch_prefilter_data(tickers_to_check, max_workers, progress_callback)
        )
    
    logger.info("Pre-filter: Got data for %d / %d stocks", 
                len(prefilter_data), len(tickers_to_check))
    
    # ── Step 3: Apply sector + liquidity filters ─────────────────
    passed_all = []
    excluded_by_sector = 0
    excluded_by_liquidity = 0
    excluded_no_data = 0
    
    for t in candidates:
        tk = t["ticker"]
        data = prefilter_data.get(tk)
        
        if data is None:
            # No data — likely very illiquid or delisted
            excluded_no_data += 1
            continue
        
        # Sector filter
        sector = data.get("sector", "")
        if type(sector) is not str:
            sector = ""
            
        if sector:
            is_excluded_sector = (
                sector in excluded_sectors
                or any(excl in sector for excl in excluded_sectors)
            )
            if is_excluded_sector:
                excluded_by_sector += 1
                continue
        
        # Liquidity filter
        adv20 = float(data.get("adv20", 0))
        trading_days = int(data.get("trading_days", 0))
        
        if adv20 < min_adv20 or trading_days < min_trading_days:
            excluded_by_liquidity += 1
            continue
        
        passed_all.append(tk)
    
    logger.info(
        "Pre-filter: Sector exclusion removed %d stocks (financials, utilities, etc.)",
        excluded_by_sector
    )
    logger.info(
        "Pre-filter: Liquidity filter (ADV20 >= %.0fB, days >= %d) removed %d stocks",
        min_adv20 / 1e9, min_trading_days, excluded_by_liquidity
    )
    logger.info(
        "Pre-filter: No data (illiquid/delisted): %d stocks",
        excluded_no_data
    )
    logger.info(
        "Pre-filter COMPLETE: %d → %d stocks (%.0f%% reduction)",
        initial_count, len(passed_all),
        (1 - len(passed_all) / initial_count) * 100 if initial_count else 0
    )
    
    return passed_all


# ── Combined Fundamental Data (2-year, async) ────────────────────────

async def fetch_fundamental_data(
    client: httpx.AsyncClient, ticker: str,
) -> Optional[Dict]:
    """
    Fetch both Annual (latest completed year) and TTM (trailing 12 months) data.
    
    Annual data is used for long-term Quality metrics (ROA, GP/TA, Accruals).
    TTM data is used for Valuation ratios (Acquirer's Multiple, P/E) to be as fresh as possible.
    
    Returns unified dict with:
    - Annual fields: revenue, ebit, total_assets, etc.
    - TTM fields: revenue_ttm, ebit_ttm, etc.
    """
    now = datetime.now()
    yr = now.year

    # 1. First, always fetch ANNUAL data (Quality metrics need this)
    ann_dict: Dict = {"ticker": ticker}
    
    # ── Fetch 5 Years of Historical Data ──
    # Income statement - search latest available annual
    inc_year = None
    for try_yr in [yr, yr - 1, yr - 2]:
        result = await fetch_income_statement(client, ticker, try_yr)
        if result:
            inc_year, inc_data = result
            ann_dict.update(inc_data)
            break
            
    if inc_year is None:
        return None

    # Balance sheet - match income year
    bs_result = await fetch_balance_sheet(client, ticker, inc_year)
    if bs_result:
        _, bs_data = bs_result
        ann_dict.update(bs_data)
    else:
        return None  # BS is critical

    # Cash flow - match income year
    cf_result = await fetch_cash_flow(client, ticker, inc_year)
    if cf_result:
        _, cf_data = cf_result
        ann_dict.update(cf_data)

    # Fetch prior 4 years for historical averages (y1 to y4)
    for offset in range(1, 5):
        hist_yr = inc_year - offset
        suffix = f"_y{offset}"
        
        # We need `_prev` to equal `_y1` for Piotroski F-Score compatibility
        # without breaking existing downstream logic.
        
        hist_inc = await fetch_income_statement(client, ticker, hist_yr)
        if hist_inc:
            for k, v in hist_inc[1].items():
                ann_dict[f"{k}{suffix}"] = v
                if offset == 1:
                    ann_dict[f"{k}_prev"] = v
                
        hist_bs = await fetch_balance_sheet(client, ticker, hist_yr)
        if hist_bs:
            for k, v in hist_bs[1].items():
                ann_dict[f"{k}{suffix}"] = v
                if offset == 1:
                    ann_dict[f"{k}_prev"] = v
                    
        hist_cf = await fetch_cash_flow(client, ticker, hist_yr)
        if hist_cf:
            for k, v in hist_cf[1].items():
                ann_dict[f"{k}{suffix}"] = v
                if offset == 1:
                    ann_dict[f"{k}_prev"] = v

    # Store Annual provenance
    ann_dict["data_source_annual"] = "Annual"
    ann_dict["data_year_annual"] = inc_year

    # 2. Attempt to fetch TTM data for freshest Valuation metrics
    ttm_result = await fetch_ttm_data(client, ticker)
    if ttm_result:
        ttm_data, ttm_year = ttm_result
        
        # Merge TTM into the main dict with _ttm suffix
        for k, v in ttm_data.items():
            if k in ("ticker", "data_source", "data_year"):
                pass  # Ignore original provenance names
            elif k == "data_year_ttm":
                ann_dict["data_year_ttm"] = v
            else:
                ann_dict[f"{k}_ttm"] = v
                
        ann_dict["data_source_ttm"] = "TTM"
    else:
        # 3. Fallback: If no TTM available, clone Annual fields into TTM keys
        ann_dict["data_source_ttm"] = "Annual (Fallback)"
        ann_dict["data_year_ttm"] = inc_year
        
        # Clone all numeric fields
        for k, v in list(ann_dict.items()):
            if isinstance(v, (int, float)) and not k.endswith("_prev"):
                ann_dict[f"{k}_ttm"] = v
                
        # Clone _prev fields
        for k, v in list(ann_dict.items()):
            if k.endswith("_prev"):
                base_key = k[:-5]
                ann_dict[f"{base_key}_ttm_prev"] = v

    # 4. Price & momentum (common to both TTM and annual)
    price = await fetch_price(client, ticker)
    if price:
        ann_dict["price"] = price

    # 5. Fetch official CafeF indices (Market Cap, Shares)
    indices = await fetch_cafef_indices(client, ticker)
    ann_dict.update(indices)

    price_hist = await fetch_price_history(client, ticker, days=280)
    if price_hist:
        from quant_value_vn.pipeline.momentum import compute_momentum_from_prices
        mom = compute_momentum_from_prices(price_hist)
        if mom:
            ann_dict["mom_2_12"] = mom.get("mom_2_12")
            ann_dict["mom_12"] = mom.get("mom_12")
            ann_dict["mom_1"] = mom.get("mom_1")

    # Sector (from Vietstock profile)
    sector = await fetch_sector(client, ticker)
    if sector:
        ann_dict["sector"] = sector

    return ann_dict


# ── Async Parallel Ingestion ─────────────────────────────────────────

async def _ingest_one(
    sem: asyncio.Semaphore,
    client: httpx.AsyncClient,
    ticker: str,
) -> tuple[str, Optional[Dict], Optional[Exception]]:
    """Fetch one ticker with semaphore-controlled concurrency."""
    async with sem:
        try:
            data = await fetch_fundamental_data(client, ticker)
            # Save to cache immediately (provides resumability)
            if data:
                from quant_value_vn.pipeline.cache import set_cached
                set_cached(ticker, data)
            return ticker, data, None
        except Exception as exc:
            return ticker, None, exc


async def _ingest_all_async(
    tickers: List[str],
    max_workers: int = SCRAPER_WORKERS,
    progress_callback=None,
    use_cache: bool = True,
) -> tuple[List[Dict], List[str]]:
    """
    Core async ingestion — fetch all tickers concurrently.
    Concurrency is controlled by asyncio.Semaphore(max_workers).

    When use_cache is True, checks local cache first and only scrapes
    tickers with stale or missing cache entries. Provides resumability.
    """
    from quant_value_vn.pipeline.cache import get_cached

    all_data: List[Dict] = []
    failed: List[str] = []
    to_scrape: List[str] = []
    cached_count = 0

    # Check cache first
    if use_cache:
        for tk in tickers:
            cached = get_cached(tk)
            if cached:
                all_data.append(cached)
                cached_count += 1
            else:
                to_scrape.append(tk)
        if cached_count:
            logger.info("Cache hit: %d/%d tickers (scraping %d)",
                        cached_count, len(tickers), len(to_scrape))
    else:
        to_scrape = list(tickers)

    if not to_scrape:
        logger.info("All %d tickers served from cache", cached_count)
        return all_data, failed

    # Scrape remaining tickers
    sem = asyncio.Semaphore(max_workers)
    total = len(to_scrape)
    completed = 0

    async with httpx.AsyncClient(
            headers=_HEADERS, follow_redirects=True,
            limits=httpx.Limits(max_connections=200, max_keepalive_connections=100),
    ) as client:
        tasks = [_ingest_one(sem, client, tk) for tk in to_scrape]
        for coro in asyncio.as_completed(tasks):
            tk, data, err = await coro
            completed += 1
            if data:
                all_data.append(data)
            else:
                failed.append(tk)
            if completed % 50 == 0:
                logger.info("[%d/%d] %d ok", completed, total, len(all_data))
            if progress_callback:
                progress_callback(completed + cached_count, len(tickers), len(all_data))

    logger.info("Ingestion done: %d cached + %d scraped, %d failed",
                cached_count, len(all_data) - cached_count, len(failed))
    return all_data, failed


def ingest_all(
    tickers: List[str],
    max_workers: int = SCRAPER_WORKERS,
    progress_callback=None,
    use_cache: bool = True,
) -> tuple[List[Dict], List[str]]:
    """
    Fetch fundamental data for all tickers via async httpx.

    Sync wrapper around the async implementation — safe to call from
    synchronous code (run_pipeline, CLI, Streamlit).

    When use_cache is True, serves fresh data from local cache and only
    scrapes stale/missing tickers. Set to False (--no-cache) to force
    re-scraping everything.

    Returns (successful_data_list, failed_tickers_list).
    Optional progress_callback(completed, total, ok_count) for UI updates.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        # Already inside an event loop (e.g. Jupyter, FastAPI).
        # Create a new thread to run the async code.
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(1) as pool:
            result = pool.submit(
                asyncio.run,
                _ingest_all_async(tickers, max_workers, progress_callback, use_cache),
            ).result()
        return result
    else:
        return asyncio.run(
            _ingest_all_async(tickers, max_workers, progress_callback, use_cache)
        )
