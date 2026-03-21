"""
signals/canlii_litigation.py
────────────────────────────
Strategy 1 — "Follow the Work" (Litigation)

Uses the official CanLII REST API to:
  • Fetch recent ABQB (Alberta Court of King's Bench) decisions
  • Parse the "counsel" metadata block to attribute appearances to Calgary firms
  • Aggregate by firm over a 30-day rolling window
  • Z-score spike detection: if a firm's volume jumps ≥ 1.5 SD above its
    4-week baseline, fire a TIER-1 alert

CanLII API docs: https://github.com/canlii/API_documentation
Rate limit: 1 req/sec (enforced by config CANLII_RATE_LIMIT_S)

NOTE: Only uses the official CanLII API — never bulk HTML scraping.
"""

import time
import logging
import re
from datetime import datetime, timedelta, date
from collections import defaultdict

import requests
import numpy as np

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config_calgary import (
    CANLII_API_KEY, CANLII_BASE_URL, CANLII_ABQB_DB, CANLII_ABCA_DB,
    CANLII_RATE_LIMIT_S, CANLII_LOOKBACK_DAYS, CALGARY_FIRMS,
    FIRM_ALIASES, BIGLAW_FIRMS, SIGNAL_WEIGHTS, ZSCORE_ALERT_THRESHOLD,
    APPEARANCE_MA_DAYS,
)
from database.db import (
    upsert_canlii_appearance, insert_signal,
    get_recent_appearances, upsert_spillage_edge,
)

log = logging.getLogger(__name__)

# ─── CanLII API Client ────────────────────────────────────────────────────────

class CanLIIClient:
    """Thin wrapper around the CanLII v1 REST API with rate-limiting."""

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("CANLII_API_KEY is not set. Register at https://canlii.org/en/info/about.html")
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers["Accept"] = "application/json"
        self._last_call = 0.0

    def _get(self, path: str, params: dict = None) -> dict:
        elapsed = time.time() - self._last_call
        if elapsed < CANLII_RATE_LIMIT_S:
            time.sleep(CANLII_RATE_LIMIT_S - elapsed)
        params = params or {}
        params["api_key"] = self.api_key
        url = f"{CANLII_BASE_URL}{path}"
        resp = self.session.get(url, params=params, timeout=15)
        self._last_call = time.time()
        resp.raise_for_status()
        return resp.json()

    def list_recent_cases(self, db_id: str, offset: int = 0, count: int = 100,
                          published_after: str = None) -> dict:
        """
        GET /v1/caseBrowse/en/{databaseId}/
        Returns list of recent case metadata objects.
        """
        params = {"offset": offset, "resultCount": count}
        if published_after:
            params["publishedAfter"] = published_after
        return self._get(f"/v1/caseBrowse/en/{db_id}/", params)

    def get_case_metadata(self, db_id: str, case_id: str) -> dict:
        """
        GET /v1/caseBrowse/en/{databaseId}/{caseId}/
        Returns full metadata including keywords, counsel, docketNumber.
        """
        return self._get(f"/v1/caseBrowse/en/{db_id}/{case_id}/")


# ─── Counsel parsing ──────────────────────────────────────────────────────────

# Known firm name fragments used to match counsel strings
_FIRM_PATTERNS = {}
for _firm in CALGARY_FIRMS:
    _patterns = [re.escape(a) for a in [_firm["name"]] + _firm["aliases"]]
    _FIRM_PATTERNS[_firm["id"]] = re.compile(
        "|".join(_patterns), re.IGNORECASE
    )


def extract_firms_from_counsel(counsel_text: str) -> list[str]:
    """
    Returns a list of firm_ids mentioned in the counsel text block.
    Counsel text typically looks like:
      'J. Smith of Blakes, for the applicant; A. Doe of BDP, for the respondent'
    """
    if not counsel_text:
        return []
    found = []
    for firm_id, pattern in _FIRM_PATTERNS.items():
        if pattern.search(counsel_text):
            found.append(firm_id)
    return list(set(found))


# Classify file type from title keywords
_COMMERCIAL_KEYWORDS = re.compile(
    r"\b(contract|commercial|corporation|shareholder|partnership|joint venture|"
    r"merger|acquisition|securities|fraud|oppression|injunction|receivership|"
    r"insolvency|restructur|energy|oil|gas|pipeline|royalt)\b",
    re.IGNORECASE,
)
_LARGE_FILE_KEYWORDS = re.compile(
    r"\b(billion|million|\$\d{2,}[Mm]|class action|receivership|CCAA|"
    r"Companies.Creditors Arrangement|oppression)\b",
    re.IGNORECASE,
)


def classify_file(title: str, keywords: str = "") -> tuple[str, bool]:
    """
    Returns (file_type: str, is_large: bool).
    'Large' = case likely requiring significant junior doc-review hours.
    """
    text = f"{title} {keywords}"
    is_large = bool(_LARGE_FILE_KEYWORDS.search(text))
    if _COMMERCIAL_KEYWORDS.search(text):
        return "commercial", is_large
    return "general", is_large


# ─── Spike Detection ──────────────────────────────────────────────────────────

def compute_weekly_counts(appearances: list[dict]) -> dict[str, int]:
    """
    Given a list of appearance dicts (with 'decision_date'), bucket by ISO week.
    Returns {week_label: count}.
    """
    counts = defaultdict(int)
    for a in appearances:
        try:
            d = date.fromisoformat(a["decision_date"])
            week = d.strftime("%Y-W%W")
            counts[week] += 1
        except Exception:
            pass
    return dict(counts)


def compute_30day_counts(appearances: list[dict]) -> dict[str, int]:
    """
    Bucket appearances by calendar day for the 30-day rolling window.
    Returns {date_str: count}.
    """
    counts = defaultdict(int)
    today = date.today()
    window_start = today - timedelta(days=APPEARANCE_MA_DAYS)
    for a in appearances:
        try:
            d = date.fromisoformat(a["decision_date"])
            if d >= window_start:
                counts[d.isoformat()] += 1
        except Exception:
            pass
    return dict(counts)


def zscore_spike(recent_count: int, historical_counts: list[int]) -> float:
    """
    Compute z-score of recent_count against a historical distribution.
    Returns float; ≥ ZSCORE_ALERT_THRESHOLD is a spike.
    """
    if len(historical_counts) < 2:
        return 0.0
    arr = np.array(historical_counts, dtype=float)
    mu  = arr.mean()
    sd  = arr.std()
    if sd == 0:
        return 0.0
    return float((recent_count - mu) / sd)


# ─── Main scraper class ───────────────────────────────────────────────────────

class CanLIILitigationTracker:
    """
    Pulls recent ABQB decisions, attributes appearances to Calgary firms,
    detects volume spikes, and updates the spillage graph.
    """

    def __init__(self):
        self.client = CanLIIClient(CANLII_API_KEY)
        self.new_signals: list[dict] = []

    def run(self, databases: list[str] = None):
        databases = databases or [CANLII_ABQB_DB, CANLII_ABCA_DB]
        cutoff = (datetime.utcnow() - timedelta(days=CANLII_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
        log.info("[CanLII] Fetching cases published after %s", cutoff)

        for db_id in databases:
            self._process_database(db_id, cutoff)

        self._detect_spikes()
        log.info("[CanLII] Done. %d new signals generated.", len(self.new_signals))
        return self.new_signals

    def _process_database(self, db_id: str, published_after: str):
        """Paginate through recent cases and process each one."""
        offset = 0
        page_size = 100

        while True:
            try:
                data = self.client.list_recent_cases(
                    db_id, offset=offset, count=page_size,
                    published_after=published_after
                )
            except requests.HTTPError as e:
                log.error("[CanLII] HTTP error listing cases: %s", e)
                break

            cases = data.get("cases", [])
            if not cases:
                break

            log.info("[CanLII][%s] Processing %d cases (offset=%d)", db_id, len(cases), offset)

            for case_stub in cases:
                self._process_case(db_id, case_stub)

            if len(cases) < page_size:
                break
            offset += page_size

    def _process_case(self, db_id: str, stub: dict):
        """Fetch full metadata for one case and attribute counsel to firms."""
        case_id   = stub.get("caseId", {}).get("en", "")
        title     = stub.get("title", "")
        citation  = stub.get("citation", "")

        if not case_id:
            return

        # Fetch full metadata (contains keywords, counsel text if available)
        try:
            meta = self.client.get_case_metadata(db_id, case_id)
        except requests.HTTPError as e:
            log.debug("[CanLII] Metadata fetch failed for %s: %s", case_id, e)
            return

        decision_date = meta.get("decisionDate", "")
        keywords      = meta.get("keywords", "")
        # CanLII doesn't always expose counsel in the public API;
        # we use title + keywords for firm matching and file classification.
        counsel_raw   = keywords   # best available proxy via API

        firm_ids_in_case = extract_firms_from_counsel(f"{title} {keywords}")

        if not firm_ids_in_case:
            return  # no Calgary firm involved — skip

        file_type, is_large = classify_file(title, keywords)

        # ── Spillage graph update ─────────────────────────────────────────
        biglaw_in_case    = [f for f in firm_ids_in_case if f in BIGLAW_FIRMS]
        boutique_in_case  = [f for f in firm_ids_in_case if f not in BIGLAW_FIRMS]
        for bl in biglaw_in_case:
            for bt in boutique_in_case:
                upsert_spillage_edge(bl, bt, source="canlii")

        # ── Persist each firm's appearance ───────────────────────────────
        for firm_id in firm_ids_in_case:
            upsert_canlii_appearance({
                "firm_id":       firm_id,
                "case_id":       f"{db_id}_{case_id}",
                "case_title":    title,
                "citation":      citation,
                "decision_date": decision_date,
                "court":         db_id.upper(),
                "counsel_raw":   counsel_raw,
                "file_type":     file_type,
            })

            # Large file = immediate Tier-2 signal even without a spike
            if is_large:
                self.new_signals.append({
                    "firm_id":     firm_id,
                    "signal_type": "canlii_new_large_file",
                    "weight":      SIGNAL_WEIGHTS["canlii_new_large_file"],
                    "title":       f"Large {file_type} file: {title[:80]}",
                    "description": f"Appeared on {citation} ({decision_date}). Keywords: {keywords[:120]}",
                    "source_url":  meta.get("url", ""),
                    "raw_data":    meta,
                })
                insert_signal(
                    firm_id=firm_id,
                    signal_type="canlii_new_large_file",
                    weight=SIGNAL_WEIGHTS["canlii_new_large_file"],
                    title=f"Large {file_type} file: {title[:80]}",
                    description=f"Appeared on {citation} ({decision_date})",
                    source_url=meta.get("url", ""),
                    raw_data=meta,
                )

    def _detect_spikes(self):
        """
        For each Calgary firm, compute the 30-day MA and compare to the
        prior 4-week baseline. Fire a TIER-1 spike alert if z ≥ threshold.
        """
        for firm in CALGARY_FIRMS:
            fid = firm["id"]
            appearances = get_recent_appearances(fid, days=CANLII_LOOKBACK_DAYS)

            if not appearances:
                continue

            day_counts = compute_30day_counts(appearances)
            recent_30  = sum(day_counts.values())

            # Baseline: bucket into 7-day periods before the 30-day window
            # (use days 31–62 as baseline if available)
            baseline_appearances = get_recent_appearances(fid, days=62)
            baseline_day_counts  = compute_30day_counts(baseline_appearances)
            # Weeks in the baseline period
            baseline_weekly = defaultdict(int)
            today = date.today()
            for d_str, cnt in baseline_day_counts.items():
                d = date.fromisoformat(d_str)
                days_ago = (today - d).days
                if days_ago > APPEARANCE_MA_DAYS:           # prior period only
                    week_idx = days_ago // 7
                    baseline_weekly[week_idx] += cnt

            if len(baseline_weekly) < 2:
                log.debug("[CanLII] Not enough baseline data for %s", fid)
                continue

            baseline_vals = list(baseline_weekly.values())
            z = zscore_spike(recent_30, baseline_vals)

            log.info("[CanLII] %s  30-day appearances=%d  z=%.2f", firm["name"], recent_30, z)

            if z >= ZSCORE_ALERT_THRESHOLD:
                msg = (f"30-day court appearance spike: {recent_30} appearances "
                       f"(z={z:.2f}, baseline avg={np.mean(baseline_vals):.1f}). "
                       f"Firm likely drowning in litigation — prime hire opportunity.")
                self.new_signals.append({
                    "firm_id":     fid,
                    "signal_type": "canlii_appearance_spike",
                    "weight":      SIGNAL_WEIGHTS["canlii_appearance_spike"],
                    "title":       f"Appearance SPIKE at {firm['name']}",
                    "description": msg,
                    "source_url":  f"https://www.canlii.org/en/ab/abqb/",
                    "raw_data":    {"recent_30": recent_30, "zscore": z,
                                    "baseline": baseline_vals},
                })
                insert_signal(
                    firm_id=fid,
                    signal_type="canlii_appearance_spike",
                    weight=SIGNAL_WEIGHTS["canlii_appearance_spike"],
                    title=f"Appearance SPIKE at {firm['name']}",
                    description=msg,
                    source_url=f"https://www.canlii.org/en/ab/abqb/",
                    raw_data={"recent_30": recent_30, "zscore": z},
                )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    tracker = CanLIILitigationTracker()
    signals = tracker.run()
    for s in signals:
        print(f"  [{s['signal_type']}] {s['firm_id']}: {s['title']}")
