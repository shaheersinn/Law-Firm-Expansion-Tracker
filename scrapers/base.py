"""
Base scraper — all scrapers inherit from this.
Provides: logging, rate-limiting, signal construction, HTTP helpers.
"""

import logging
import random
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

_SESSION = None


def _get_session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
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
    return _SESSION


class BaseScraper(ABC):
    name = "BaseScraper"

    def __init__(self):
        self.logger = logging.getLogger(f"scrapers.{self.name}")
        self.session = _get_session()

    @abstractmethod
    def fetch(self, firm: dict) -> list[dict]:
        """Fetch signals for a given firm dict."""

    # ------------------------------------------------------------------ #
    #  Signal factory
    # ------------------------------------------------------------------ #

    def _make_signal(
        self,
        *,
        firm_id: str,
        firm_name: str,
        signal_type: str,
        title: str,
        body: str,
        url: str,
        department: str,
        department_score: float,
        matched_keywords: list,
        source: str | None = None,
        published_at: str | None = None,
    ) -> dict:
        return {
            "firm_id": firm_id,
            "firm_name": firm_name,
            "signal_type": signal_type,
            "title": title[:300],
            "body": body[:1200],
            "url": url,
            "department": department,
            "department_score": round(department_score, 3),
            "matched_keywords": matched_keywords[:20],
            "source": source or self.name,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "published_at": published_at or "",
        }

    # ------------------------------------------------------------------ #
    #  HTTP helpers
    # ------------------------------------------------------------------ #

    def _get(self, url: str, timeout: int = 20, params: dict | None = None,
             extra_headers: dict | None = None):
        """GET with rate-limit delay. Returns Response or None."""
        self._delay()
        try:
            resp = self.session.get(url, timeout=timeout, params=params,
                                    headers=extra_headers or None)
            resp.raise_for_status()
            return resp
        except Exception as e:
            self.logger.debug(f"GET {url}: {e}")
            return None

    def _soup(self, url: str, timeout: int = 20) -> BeautifulSoup | None:
        """Fetch URL and return BeautifulSoup, or None on failure."""
        resp = self._get(url, timeout=timeout)
        if resp is None:
            return None
        return BeautifulSoup(resp.text, "lxml")

    def _delay(self, min_s: float = 1.0, max_s: float = 3.0):
        time.sleep(random.uniform(min_s, max_s))

    # ------------------------------------------------------------------ #
    #  Text helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    def _clean(text: str) -> str:
        return " ".join(text.split()) if text else ""

    @staticmethod
    def _firm_mentioned(text: str, firm: dict) -> bool:
        lower = text.lower()
        names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])
        return any(n.lower() in lower for n in names)
