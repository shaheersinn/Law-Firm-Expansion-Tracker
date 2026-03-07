"""
Base scraper — shared HTTP session, retry logic, polite delays,
user-agent rotation, and the standard _make_signal() contract.

All scrapers inherit from BaseScraper and call:
  self._get(url)          → requests.Response | None
  self._make_signal(...)  → dict

Never call requests directly from a scraper — always go through _get()
so that delays, retries, and error-handling stay consistent.
"""

import os
import socket
import time
import random
import logging
import hashlib
import requests
from abc import ABC, abstractmethod
from datetime import datetime, timezone, timedelta

# Hard backstop: no socket operation (including SSL handshakes) can take more than 25s
# This prevents silent hangs that bypass the requests timeout parameter
socket.setdefaulttimeout(25)

# Only accept signals published within this many days — filters out 2019+ stale content
SIGNAL_LOOKBACK_DAYS = int(os.environ.get("SIGNAL_LOOKBACK_DAYS", "21"))

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

# Per-domain rate-limit memory — once a domain 429s/persistently-500s we skip it
# This stops mccarthy.ca-style loops where 15+ URL variants are tried one by one
_DOMAIN_RATE_LIMITED: set[str] = set()

# Track consecutive 500s per domain — after 2 distinct URLs 500, block the domain
_DOMAIN_500_URLS: dict[str, set] = {}


class BaseScraper(ABC):
    name: str = "BaseScraper"

    def __init__(
        self,
        timeout: int = 15,  # read timeout; connect timeout is always 8s
        min_delay: float = float(os.environ.get('SCRAPER_MIN_DELAY', '0.3')),
        max_delay: float = float(os.environ.get('SCRAPER_MAX_DELAY', '0.8')),
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
        # Skip instantly if we already hit a 429 from this domain this run
        from urllib.parse import urlparse
        _domain = urlparse(url).netloc
        if _domain in _DOMAIN_RATE_LIMITED:
            self.logger.debug(f"Domain {_domain} previously rate-limited — skipping {url}")
            return None

        headers = {"User-Agent": random.choice(USER_AGENTS)}

        for attempt in range(1, self.max_retries + 1):
            try:
                time.sleep(random.uniform(self.min_delay, self.max_delay))
                resp = _SESSION.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=(8, self.timeout),  # (connect, read) — prevents SSL/DNS hangs
                    allow_redirects=True,
                )
                if resp.status_code == 200:
                    return resp
                elif resp.status_code == 429:
                    # Track domain as rate-limited so sibling URLs are skipped instantly
                    from urllib.parse import urlparse
                    _domain = urlparse(url).netloc
                    _DOMAIN_RATE_LIMITED.add(_domain)
                    wait = min(int(resp.headers.get("Retry-After", 5)), 5)
                    self.logger.warning(f"Rate-limited on {url} — skipping after {wait}s")
                    time.sleep(wait)
                    return None  # abort immediately, don't burn remaining retries
                elif resp.status_code in (403, 404):
                    self.logger.debug(f"HTTP {resp.status_code} for {url}")
                    return None
                else:
                    self.logger.warning(f"HTTP {resp.status_code} for {url} (attempt {attempt})")
                    if resp.status_code == 500 and attempt == self.max_retries:
                        # Track this domain's 500 URLs; block after 2 distinct URLs fail
                        _DOMAIN_500_URLS.setdefault(_domain, set()).add(url)
                        if len(_DOMAIN_500_URLS[_domain]) >= 2:
                            _DOMAIN_RATE_LIMITED.add(_domain)
                            self.logger.warning(
                                f"Domain {_domain} blocked — {len(_DOMAIN_500_URLS[_domain])} URLs returned 500"
                            )
            except requests.exceptions.Timeout:
                self.logger.warning(f"Timeout on {url} (attempt {attempt})")
            except requests.exceptions.ConnectionError as e:
                err_str = str(e)
                # DNS failure (NameResolutionError) → no point retrying any URL on this domain
                if "NameResolutionError" in err_str or "Failed to resolve" in err_str or "Errno -3" in err_str:
                    _DOMAIN_RATE_LIMITED.add(_domain)
                    self.logger.warning(
                        f"DNS failure for {_domain} — blocking all further requests to this domain"
                    )
                    return None
                self.logger.warning(f"Connection error on {url}: {e} (attempt {attempt})")
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Request failed on {url}: {e}")
                return None

        return None

    # ------------------------------------------------------------------ #
    #  Signal factory
    # ------------------------------------------------------------------ #

    @staticmethod
    def is_recent(date_str: str) -> bool:
        """
        Return True if the date string falls within SIGNAL_LOOKBACK_DAYS.
        Accepts RFC 822 (RSS), ISO 8601, and loose date strings.
        Returns True when date cannot be parsed (fail-open = don't drop uncertain items).
        """
        if not date_str:
            return True
        cutoff = datetime.now(timezone.utc) - timedelta(days=SIGNAL_LOOKBACK_DAYS)
        for fmt in (
            "%a, %d %b %Y %H:%M:%S %z",   # RFC 822  — used by Google News RSS
            "%a, %d %b %Y %H:%M:%S %Z",   # RFC 822 with tz name
            "%Y-%m-%dT%H:%M:%S%z",         # ISO 8601 with offset
            "%Y-%m-%dT%H:%M:%SZ",          # ISO 8601 UTC
            "%Y-%m-%d",                    # plain date
            "%d %b %Y",                    # e.g. 12 Jan 2024
            "%B %d, %Y",                   # e.g. January 12, 2024
        ):
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt >= cutoff
            except ValueError:
                continue
        return True  # unknown format — keep the item

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
