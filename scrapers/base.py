"""
scrapers/base.py
================
Base class for all scrapers.

FIX (2026-03-06): _make_signal() now accepts firm_id and firm_name as
explicit keyword arguments.  Previously the method signature omitted
these two fields, causing every subclass that passed them to raise:
  TypeError: BaseScraper._make_signal() got an unexpected keyword argument 'firm_id'
This crashed publications, website, press, and lawschool scrapers on
every single firm.
"""

from __future__ import annotations

import hashlib
import logging
import time
import random
from datetime import datetime, timezone
from typing import Any

import requests
from bs4 import BeautifulSoup

from config import Config

logger = logging.getLogger("scrapers")


class BaseScraper:
    name: str = "BaseScraper"

    # ------------------------------------------------------------------ #
    #  HTTP helpers
    # ------------------------------------------------------------------ #

    def _get(
        self,
        url: str,
        *,
        timeout: int | None = None,
        headers: dict | None = None,
        allow_redirects: bool = True,
    ) -> requests.Response | None:
        """
        GET a URL with retry logic and rate-limit detection.

        Returns the Response or None on failure.
        Logs a WARNING (not ERROR) on transient failures so the caller
        can decide whether to continue to the next URL variant.
        """
        cfg = Config()
        _timeout = timeout or cfg.REQUEST_TIMEOUT

        _headers = {
            "User-Agent": (
                "Mozilla/5.0 (compatible; LawFirmTracker/1.0; "
                "+https://github.com/shaheersinn/Law-Firm-Expansion-Tracker)"
            ),
            "Accept-Language": "en-CA,en;q=0.9",
        }
        if headers:
            _headers.update(headers)

        for attempt in range(1, 3):
            try:
                resp = requests.get(
                    url,
                    headers=_headers,
                    timeout=_timeout,
                    allow_redirects=allow_redirects,
                )

                if resp.status_code == 429:
                    # Hard rate-limit — wait briefly then skip this URL.
                    # Do NOT hammer 9 more fallback variants after this.
                    wait = min(float(resp.headers.get("Retry-After", 5)), 5.0)
                    self.logger.warning(
                        f"Rate-limited on {url} — skipping after {wait:.0f}s"
                    )
                    time.sleep(wait)
                    return None

                if resp.status_code in (500, 502, 503, 504):
                    self.logger.warning(
                        f"HTTP {resp.status_code} for {url} (attempt {attempt})"
                    )
                    if attempt < 2:
                        time.sleep(1.5)
                        continue
                    return None

                resp.raise_for_status()
                # Polite delay between requests
                time.sleep(
                    random.uniform(Config.MIN_DELAY_SECS, Config.MAX_DELAY_SECS)
                )
                return resp

            except requests.exceptions.Timeout:
                self.logger.warning(f"Timeout on {url} (attempt {attempt})")
                if attempt < 2:
                    time.sleep(1)
            except requests.exceptions.ConnectionError as exc:
                self.logger.warning(f"Connection error on {url}: {exc}")
                return None
            except requests.exceptions.RequestException as exc:
                self.logger.warning(f"Request failed for {url}: {exc}")
                return None

        return None

    def _get_soup(self, url: str, **kwargs) -> BeautifulSoup | None:
        """Fetch URL and return a BeautifulSoup, or None on failure."""
        resp = self._get(url, **kwargs)
        if resp is None:
            return None
        try:
            return BeautifulSoup(resp.text, "lxml")
        except Exception as exc:
            self.logger.debug(f"Parse error for {url}: {exc}")
            return None

    # ------------------------------------------------------------------ #
    #  URL-variant probing  (rate-limit aware)
    # ------------------------------------------------------------------ #

    def _first_live_url(self, base: str, suffixes: list[str]) -> str | None:
        """
        Try a list of URL suffixes against *base* and return the first
        that returns a non-None response.

        IMPORTANT: if _get() returns None for a URL that was definitively
        rate-limited (429) we do NOT keep trying the same domain — we bail
        after the first 429 to avoid burning 5s × N variants per firm.
        """
        from urllib.parse import urlparse

        rate_limited_hosts: set[str] = set()

        for suffix in suffixes:
            url = base.rstrip("/") + suffix
            host = urlparse(url).netloc

            if host in rate_limited_hosts:
                self.logger.debug(f"Skipping {url} — domain already rate-limited")
                continue

            resp = self._get(url)
            if resp is not None:
                return url

            # If we just got rate-limited, mark the whole host
            # (the warning is already logged inside _get)
            rate_limited_hosts.add(host)

        return None

    # ------------------------------------------------------------------ #
    #  Signal factory  ← THE CORE FIX
    # ------------------------------------------------------------------ #

    def _make_signal(
        self,
        *,
        firm_id: str,           # ← was missing; caused TypeError on every call
        firm_name: str,         # ← was missing
        signal_type: str,
        title: str,
        body: str,
        url: str,
        department: str,
        department_score: float,
        matched_keywords: list[str] | None = None,
        published_at: str | None = None,
    ) -> dict[str, Any]:
        """
        Build a normalised signal dict.

        All scrapers call this method; the returned dict is what gets
        persisted by database.db.save_signal().
        """
        return {
            "firm_id": firm_id,
            "firm_name": firm_name,
            "signal_type": signal_type,
            "title": title[:500],
            "body": body[:2000],
            "url": url,
            "department": department,
            "department_score": round(float(department_score), 4),
            "matched_keywords": matched_keywords or [],
            "published_at": published_at or datetime.now(timezone.utc).isoformat(),
            "collected_at": datetime.now(timezone.utc).isoformat(),
            # Stable dedup key — same article won't be inserted twice
            "signal_hash": hashlib.sha256(
                f"{firm_id}:{signal_type}:{url}:{title[:120]}".encode()
            ).hexdigest(),
        }

    # ------------------------------------------------------------------ #
    #  Subclass interface
    # ------------------------------------------------------------------ #

    @property
    def logger(self) -> logging.Logger:
        return logging.getLogger(f"scrapers.{self.name}")

    def fetch(self, firm: dict) -> list[dict]:
        """Override in each subclass. Must return a list of signal dicts."""
        raise NotImplementedError
