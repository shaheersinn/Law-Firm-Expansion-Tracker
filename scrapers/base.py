"""
Base scraper — shared HTTP session, retry logic, polite delays,
user-agent rotation, and the standard _make_signal() contract.

All scrapers inherit from BaseScraper and call:
  self._get(url)          → requests.Response | None
  self._make_signal(...)  → dict

Never call requests directly from a scraper — always go through _get()
so that delays, retries, and error-handling stay consistent.
"""

import time
import random
import logging
import hashlib
import requests
from abc import ABC, abstractmethod

logger = logging.getLogger("scrapers.base")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
]

# Shared session — connection pooling across all scrapers
_SESSION = requests.Session()
_SESSION.headers.update({"Accept-Language": "en-CA,en;q=0.9"})


class BaseScraper(ABC):
    name: str = "BaseScraper"

    def __init__(
        self,
        timeout: int = 20,
        min_delay: float = 1.0,
        max_delay: float = 3.0,
        max_retries: int = 2,
    ):
        self.timeout = timeout
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.max_retries = max_retries
        self.logger = logging.getLogger(f"scrapers.{self.name}")

    @abstractmethod
    def fetch(self, firm: dict) -> list[dict]:
        """
        Fetch signals for a single firm.
        Returns a list of signal dicts (see _make_signal).
        """

    # ------------------------------------------------------------------ #
    #  HTTP
    # ------------------------------------------------------------------ #

    def _get(self, url: str, params: dict = None) -> requests.Response | None:
        """
        GET with retry and polite delay.
        Returns Response on success, None on failure (logged).
        """
        headers = {"User-Agent": random.choice(USER_AGENTS)}

        for attempt in range(1, self.max_retries + 1):
            try:
                time.sleep(random.uniform(self.min_delay, self.max_delay))
                resp = _SESSION.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=self.timeout,
                    allow_redirects=True,
                )
                if resp.status_code == 200:
                    return resp
                elif resp.status_code == 429:
                    wait = int(resp.headers.get("Retry-After", 30))
                    self.logger.warning(f"Rate-limited on {url} — waiting {wait}s")
                    time.sleep(wait)
                elif resp.status_code in (403, 404):
                    self.logger.debug(f"HTTP {resp.status_code} for {url}")
                    return None
                else:
                    self.logger.warning(f"HTTP {resp.status_code} for {url} (attempt {attempt})")
            except requests.exceptions.Timeout:
                self.logger.warning(f"Timeout on {url} (attempt {attempt})")
            except requests.exceptions.ConnectionError as e:
                self.logger.warning(f"Connection error on {url}: {e} (attempt {attempt})")
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Request failed on {url}: {e}")
                return None

        return None

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
        body: str = "",
        url: str = "",
        department: str = "",
        department_score: float = 1.0,
        matched_keywords: list[str] = None,
    ) -> dict:
        return {
            "firm_id":           firm_id,
            "firm_name":         firm_name,
            "signal_type":       signal_type,
            "title":             title[:300],
            "body":              body[:1000],
            "url":               url,
            "department":        department,
            "department_score":  round(float(department_score), 4),
            "matched_keywords":  matched_keywords or [],
        }
