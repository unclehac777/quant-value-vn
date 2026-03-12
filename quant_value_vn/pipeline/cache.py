"""
Local JSON cache for scraped financial data.

Stores each ticker's raw scraped data as a JSON file with timestamps.
Avoids redundant re-scraping when data is still fresh (default: 24h TTL).

Functions:
- get_cached(ticker, max_age_hours) → dict | None
- set_cached(ticker, data) → None
- clear_cache() → None
"""

import json
import logging
import os
import time
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)

CACHE_DIR = Path.home() / ".quant_value_vn" / "cache"
DEFAULT_MAX_AGE_HOURS = 24


def _ensure_dir():
    """Create cache directory if it doesn't exist."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _cache_path(ticker: str) -> Path:
    return CACHE_DIR / f"{ticker.upper()}.json"


def get_cached(ticker: str, max_age_hours: float = DEFAULT_MAX_AGE_HOURS) -> Optional[Dict]:
    """
    Return cached data for a ticker if it exists and is fresh.

    Returns None if cache miss or data is older than max_age_hours.
    """
    path = _cache_path(ticker)
    if not path.exists():
        return None

    try:
        with open(path, "r", encoding="utf-8") as f:
            entry = json.load(f)

        cached_at = entry.get("_cached_at", 0)
        age_hours = (time.time() - cached_at) / 3600

        if age_hours > max_age_hours:
            return None
            
        # Ensure cache contains the new 5-year schema data and CafeF indices
        if "revenue_y1" not in entry or "market_cap_cafef" not in entry:
            return None

        # Remove internal metadata before returning
        data = {k: v for k, v in entry.items() if not k.startswith("_cached_")}
        return data

    except (json.JSONDecodeError, OSError):
        return None


def set_cached(ticker: str, data: Dict) -> None:
    """Save scraped data to cache with timestamp."""
    _ensure_dir()
    entry = dict(data)
    entry["_cached_at"] = time.time()

    path = _cache_path(ticker)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(entry, f, ensure_ascii=False)
    except OSError as e:
        logger.warning("Cache write failed for %s: %s", ticker, e)


def clear_cache() -> None:
    """Remove all cached data files."""
    if not CACHE_DIR.exists():
        return
    count = 0
    for f in CACHE_DIR.glob("*.json"):
        f.unlink()
        count += 1
    logger.info("Cleared %d cached files", count)


def cache_stats() -> Dict:
    """Return cache statistics."""
    if not CACHE_DIR.exists():
        return {"total": 0, "fresh": 0, "stale": 0}

    now = time.time()
    total = fresh = stale = 0
    for f in CACHE_DIR.glob("*.json"):
        total += 1
        try:
            with open(f, "r") as fh:
                entry = json.load(fh)
            age_hours = (now - entry.get("_cached_at", 0)) / 3600
            if age_hours <= DEFAULT_MAX_AGE_HOURS:
                fresh += 1
            else:
                stale += 1
        except Exception:
            stale += 1

    return {"total": total, "fresh": fresh, "stale": stale}
