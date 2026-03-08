"""
Configuration for Vietnam Quantitative Value Stock Screener.

All constants, thresholds, and environment variables in one place.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── Supabase ─────────────────────────────────────────────────────────
SUPABASE_URL: str = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY: str = os.environ.get("SUPABASE_KEY", "")

# ── Scraper ──────────────────────────────────────────────────────────
CAFEF_BASE_URL = "https://s.cafef.vn"
WIFEED_TICKER_URL = "https://wifeed.vn/api/thong-tin-co-phieu/danh-sach-ma-chung-khoan"
SCRAPER_DELAY = 0.2          # seconds between requests
SCRAPER_TIMEOUT = 15         # HTTP timeout
SCRAPER_WORKERS = 10         # parallel threads for ingestion

# ── Exchanges ────────────────────────────────────────────────────────
DEFAULT_EXCHANGES = ["HOSE", "HNX", "UPCOM"]

# ── Universe Filters ─────────────────────────────────────────────────
MIN_MARKET_CAP = 500e9        # 500 billion VND
MAX_ACQUIRERS_MULTIPLE = 50   # cap AM outliers
MAX_DEBT_EQUITY = 3.0         # leverage limit

# ── Pre-Filters (applied BEFORE scraping financials) ─────────────────
MIN_ADV20 = 5e9               # 5 billion VND average daily value (20-day)
MIN_TRADING_DAYS = 50         # minimum trading days out of 60
PREFILTER_LOOKBACK_DAYS = 60  # days to look back for liquidity check

# ── Fraud Detection ──────────────────────────────────────────────────
BENEISH_THRESHOLD = -1.78     # M-Score cutoff (> threshold = manipulator)

# ── Portfolio ────────────────────────────────────────────────────────
PORTFOLIO_SIZE = 30           # top-N equal weight

# ── Excluded Sectors ─────────────────────────────────────────────────
# Vietstock sector names (Vietnamese) — exclude financial & utility sectors
EXCLUDED_SECTORS = {
    # English labels (legacy)
    "financials", "banks", "banking", "insurance", "securities",
    "brokerage", "real estate", "reit", "utilities", "financial services",
    "leasing", "finance", "investment",
    # Vietnamese labels (from Vietstock profile page)
    "tài chính",        # Financials (banks, insurance, securities)
    "tiện ích",         # Utilities (electricity, gas, water)
    "bất động sản",     # Real Estate
    # Sub-sectors to catch edge cases
    "ngân hàng", "bảo hiểm", "chứng khoán",
}

# ── FastAPI ──────────────────────────────────────────────────────────
API_HOST = "0.0.0.0"
API_PORT = 8000

# ── Fallback tickers (if Wifeed API is down) ────────────────────────
FALLBACK_TICKERS = [
    "VNM", "VIC", "VHM", "VCB", "BID", "CTG", "HPG", "MSN", "VPB", "TCB",
    "MBB", "ACB", "FPT", "MWG", "VRE", "PLX", "GAS", "SAB", "BVH", "VJC",
    "NVL", "VCG", "REE", "DGC", "GVR", "POW", "BCM", "KDH", "DPM", "VGC",
    "PNJ", "TPB", "HDB", "STB", "EIB", "OCB", "SSI", "VND", "VCI", "HCM",
    "DCM", "PVD", "PVS", "GMD", "VTP", "VSH", "NT2", "PHR", "SBT", "GEX",
    "HAG", "HNG", "DXG", "HDG", "KBC", "IJC", "BWE", "LPB", "SHB", "HVN",
    "CTD", "FCN", "PC1", "PPC", "CTR", "VPI", "DIG", "PDR", "TCH", "VGI",
    "HSG", "NKG", "SMC", "TLG", "VIB", "NAB", "KLB", "BAF", "AGG", "DBC",
    "ANV", "ASM", "CII", "CSV", "CTI", "EVF", "FRT", "HBC", "HDC", "HT1",
    "IMP", "KDC", "LCG", "MSH", "NLG", "OGC", "PAN", "PET", "PGI", "PTB",
    "RAL", "SCS", "SKG", "SZC", "TDC", "TMS", "TNH", "VHC", "VIX", "VOS",
    "SHS", "IDC", "CEO", "PVI", "MBS", "NVB", "HUT", "TNG", "PLC", "VCS",
    "VEA", "NDN", "THD",
]
