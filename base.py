"""
Base scraper class.
All scrapers inherit from this. Provides:
  - _get(url) with retry, backoff, user-agent rotation
  - _head(url) for cheap existence checks before full GET
  - _make_signal() standard signal dict builder
  - Structured logging with per-request timing

Changelog (Cycle 2):
  - Fixed signal_hash to use title+type+body fingerprint (not URL)
    → prevents false dedup when url="" on multiple signals
  - Added _head() for cheap 200/404/301 checks before expensive GETs
  - Added jitter to all retry backoffs (prevents thundering herd)
  - Added per-request elapsed timing at DEBUG level
  - Raise timeout to 25s (was 20s) — some court/gov sites are slow
  - Added _post() helper for form-based endpoints
"""

import logging
import random
import time
import hashlib
from datetime import datetime, timezone
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
]

DEFAULT_HEADERS = {
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-CA,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
    "DNT":             "1",
}


class BaseScraper:
    name: str = "BaseScraper"

    def __init__(self):
        self.logger = logging.getLogger(self.name)
        self._session = self._build_session()
        self._delay_min = 1.5
        self._delay_max = 4.0
        self._timeout   = 25   # raised from 20s — gov/court sites can be slow
        self._max_retries = 3

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=1.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD", "POST"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://",  adapter)
        return session

    def _get(self, url: str, params: dict = None, extra_headers: dict = None) -> Optional[requests.Response]:
        headers = {**DEFAULT_HEADERS, "User-Agent": random.choice(USER_AGENTS)}
        if extra_headers:
            headers.update(extra_headers)

        for attempt in range(self._max_retries):
            t0 = time.monotonic()
            try:
                resp = self._session.get(
                    url,
                    headers=headers,
                    params=params,
                    timeout=self._timeout,
                    allow_redirects=True,
                )
                elapsed = time.monotonic() - t0
                if resp.status_code == 200:
                    self.logger.debug(f"GET {url} → 200 ({elapsed:.1f}s)")
                    time.sleep(random.uniform(self._delay_min, self._delay_max))
                    return resp
                elif resp.status_code == 404:
                    self.logger.debug(f"GET {url} → 404")
                    return None
                elif resp.status_code == 429:
                    # Honour Retry-After header if present
                    retry_after = int(resp.headers.get("Retry-After", 10 * (attempt + 1)))
                    wait = min(retry_after, 60) + random.uniform(0, 3)
                    self.logger.warning(f"Rate limited on {url} — sleeping {wait:.0f}s")
                    time.sleep(wait)
                else:
                    self.logger.debug(f"HTTP {resp.status_code} for {url} ({elapsed:.1f}s)")
            except requests.Timeout:
                self.logger.debug(f"Timeout on {url} (attempt {attempt+1}/{self._max_retries})")
            except requests.ConnectionError as e:
                self.logger.debug(f"Connection error {url}: {type(e).__name__}")
            except requests.RequestException as e:
                self.logger.debug(f"Request error {url}: {e}")

            if attempt < self._max_retries - 1:
                # Exponential backoff with jitter to avoid thundering herd
                backoff = (2 ** attempt) + random.uniform(0, 1.5)
                time.sleep(backoff)

        return None

    def _head(self, url: str) -> Optional[int]:
        """
        Cheap HTTP HEAD check. Returns status code or None on error.
        Use before expensive GETs to verify a URL actually exists.
        """
        headers = {**DEFAULT_HEADERS, "User-Agent": random.choice(USER_AGENTS)}
        try:
            resp = self._session.head(url, headers=headers, timeout=10, allow_redirects=True)
            return resp.status_code
        except requests.RequestException:
            return None

    def _post(self, url: str, data: dict = None, json_body: dict = None,
              extra_headers: dict = None) -> Optional[requests.Response]:
        """POST helper for form-based or JSON API endpoints."""
        headers = {**DEFAULT_HEADERS, "User-Agent": random.choice(USER_AGENTS)}
        if extra_headers:
            headers.update(extra_headers)
        try:
            resp = self._session.post(
                url, data=data, json=json_body,
                headers=headers, timeout=self._timeout, allow_redirects=True,
            )
            if resp.status_code == 200:
                time.sleep(random.uniform(self._delay_min, self._delay_max))
                return resp
        except requests.RequestException as e:
            self.logger.debug(f"POST error {url}: {e}")
        return None

    @staticmethod
    def _make_signal(
        firm_id: str,
        firm_name: str,
        signal_type: str,
        title: str,
        body: str,
        url: str,
        department: str,
        department_score: float,
        matched_keywords: list,
        published_date: str = None,
    ) -> dict:
        # Use title + signal_type + firm_id as the dedup key.
        # BUG FIX: Prior version used url in the hash — when url is blank
        # multiple different signals collapsed to the same hash, causing
        # false deduplication. Title is more stable and unique.
        # We still include a body fingerprint to distinguish signals with
        # identical titles (e.g. "New Partner Joins" from two sources).
        body_fingerprint = body[:80].strip() if body else ""
        raw_key = f"{firm_id}:{signal_type}:{title.lower().strip()}:{body_fingerprint}"
        signal_hash = hashlib.sha256(raw_key.encode()).hexdigest()[:20]

        return {
            "firm_id":          firm_id,
            "firm_name":        firm_name,
            "signal_type":      signal_type,
            "title":            title[:300],
            "body":             body[:1000],
            "url":              url,
            "department":       department,
            "department_score": round(float(department_score), 3),
            "matched_keywords": matched_keywords[:10],
            "published_date":   published_date or datetime.now(timezone.utc).isoformat(),
            "signal_hash":      signal_hash,
        }

    def fetch(self, firm: dict) -> list[dict]:
        raise NotImplementedError
