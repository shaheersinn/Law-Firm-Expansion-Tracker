"""
Base scraper class.
All scrapers inherit from this. Provides:
  - _get(url) with retry, backoff, user-agent rotation
  - _make_signal() standard signal dict builder
  - Structured logging
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
        self._timeout   = 20
        self._max_retries = 3

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=1.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
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
            try:
                resp = self._session.get(
                    url,
                    headers=headers,
                    params=params,
                    timeout=self._timeout,
                    allow_redirects=True,
                )
                if resp.status_code == 200:
                    time.sleep(random.uniform(self._delay_min, self._delay_max))
                    return resp
                elif resp.status_code == 404:
                    return None
                elif resp.status_code == 429:
                    wait = 10 * (attempt + 1)
                    self.logger.warning(f"Rate limited on {url} — sleeping {wait}s")
                    time.sleep(wait)
                else:
                    self.logger.debug(f"HTTP {resp.status_code} for {url}")
            except requests.Timeout:
                self.logger.debug(f"Timeout on {url} (attempt {attempt+1})")
            except requests.RequestException as e:
                self.logger.debug(f"Request error {url}: {e}")

            if attempt < self._max_retries - 1:
                time.sleep(2 ** attempt)

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
            "signal_hash":      hashlib.sha256(f"{firm_id}:{title}:{url}".encode()).hexdigest()[:16],
        }

    def fetch(self, firm: dict) -> list[dict]:
        raise NotImplementedError
