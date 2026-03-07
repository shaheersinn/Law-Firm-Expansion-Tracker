"""
BaseScraper — shared HTTP helpers, rate limiting, signal factory.
All scrapers inherit from this.
"""

import logging
import random
import time
import hashlib
from datetime import datetime, timezone, timedelta

import requests
from bs4 import BeautifulSoup

from config import Config

_config = Config()

# Hard age gate — never save signals older than this many days
_LOOKBACK_DAYS = int(__import__("os").getenv("SIGNAL_LOOKBACK_DAYS", "21"))

_SESSION = requests.Session()
_SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-CA,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
})


class BaseScraper:
    name: str = "BaseScraper"

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    # ── HTTP ──────────────────────────────────────────────────────────

    def get(self, url: str, timeout: int = None, **kwargs) -> requests.Response | None:
        try:
            resp = _SESSION.get(
                url,
                timeout=timeout or _config.REQUEST_TIMEOUT,
                **kwargs
            )
            resp.raise_for_status()
            self._throttle()
            return resp
        except requests.RequestException as e:
            self.logger.debug(f"GET {url} failed: {e}")
            return None

    def get_soup(self, url: str, **kwargs) -> BeautifulSoup | None:
        resp = self.get(url, **kwargs)
        if resp is None:
            return None
        return BeautifulSoup(resp.text, "lxml")

    def _throttle(self):
        delay = random.uniform(_config.MIN_DELAY_SECS, min(_config.MAX_DELAY_SECS, 5.0))
        time.sleep(delay)

    # ── Age gate ─────────────────────────────────────────────────────

    def is_recent(self, date_str: str) -> bool:
        """Return True if date_str represents a date within the lookback window."""
        if not date_str:
            return True  # unknown date — allow through
        cutoff = datetime.now(timezone.utc) - timedelta(days=_LOOKBACK_DAYS)
        for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d", "%a, %d %b %Y %H:%M:%S %z",
                    "%a, %d %b %Y %H:%M:%S %Z"):
            try:
                dt = datetime.strptime(date_str[:30], fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt >= cutoff
            except ValueError:
                continue
        return True

    # ── Signal factory ────────────────────────────────────────────────

    def _make_signal(
        self,
        firm_id: str,
        firm_name: str,
        signal_type: str,
        title: str,
        body: str = "",
        url: str = "",
        department: str = "",
        department_score: float = 0.0,
        matched_keywords: list = None,
    ) -> dict:
        return {
            "firm_id":          firm_id,
            "firm_name":        firm_name,
            "signal_type":      signal_type,
            "title":            title[:200],
            "body":             body[:800],
            "url":              url,
            "department":       department,
            "department_score": round(department_score, 3),
            "matched_keywords": matched_keywords or [],
            "collected_at":     datetime.now(timezone.utc).isoformat(),
        }

    # ── Subclass interface ────────────────────────────────────────────

    def fetch(self, firm: dict) -> list[dict]:
        raise NotImplementedError
