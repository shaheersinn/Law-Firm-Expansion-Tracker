"""
rss_diagnostics.py  —  Diagnose why RSS feeds go silent.

Root-cause analysis of the 2026-03-10 run:
  • All 32 RSS feeds returned 0 signals across all 26 firms.
  • The log shows: "[FirmName] RSS total: 0 signal(s)" for every single firm.
  • Possible causes:
      A) Feed URLs have changed / returned 404 / are behind a paywall
      B) All entries are older than SIGNAL_LOOKBACK_DAYS (21 days) and being
         correctly filtered but nothing is surfacing as "new"
      C) The keyword taxonomy doesn't match the current feed content
      D) Network timeout in CI — feeds return empty due to DNS/TLS errors
         that are silently caught and swallowed

This module runs a standalone RSS health check, printing a per-feed report
so you can diagnose which of A/B/C/D applies.

Usage:
    python -m utils.rss_diagnostics            # check all feeds
    python -m utils.rss_diagnostics --firm Osler   # one firm
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import feedparser
import requests

log = logging.getLogger(__name__)

# ── Feed registry ─────────────────────────────────────────────────────────────
# Add / update URLs here when feeds go stale.
# Format: (firm_key, label, url)

RSS_FEEDS: list[tuple[str, str, str]] = [
    # ── National majors ───────────────────────────────────────────────────────
    ("Davies",          "Davies Insights",         "https://www.dwpv.com/en/Insights/RSS"),
    ("Blakes",          "Blakes Bulletin",         "https://www.blakes.com/rss/insights"),
    ("McCarthy",        "McCarthy Insights",       "https://www.mccarthy.ca/en/insights/rss"),
    ("Osler",           "Osler Insights",          "https://www.osler.com/en/resources/regulations/rss"),
    ("Stikeman",        "Stikeman Insights",       "https://www.stikeman.com/en-ca/insights/rss"),
    ("Torys",           "Torys Insights",          "https://www.torys.com/insights/rss"),
    ("Goodmans",        "Goodmans Insights",       "https://www.goodmans.ca/insights/rss"),
    ("BLG",             "BLG Insights",            "https://blg.com/en/news-and-publications/_feeds/rss"),
    ("Fasken",          "Fasken Insights",         "https://www.fasken.com/en/knowledge/feed/"),
    ("Bennett Jones",   "BJ Publications",         "https://www.bennettjones.com/Publications-Section/RSS"),
    ("NRF",             "NRF Insights",            "https://www.nortonrosefulbright.com/en-ca/knowledge/rss"),
    ("Dentons",         "Dentons Canada",          "https://www.dentons.com/en/insights/rss?country=Canada"),
    ("Cassels",         "Cassels Insights",        "https://cassels.com/insights/rss/"),
    ("McMillan",        "McMillan Insights",       "https://mcmillan.ca/insights/rss/"),
    ("Gowling WLG",     "Gowling Insights",        "https://gowlingwlg.com/en/insights-resources/rss/"),
    ("Aird & Berlis",   "Aird Insights",           "https://www.airdberlis.com/insights/rss"),
    ("Miller Thomson",  "MT Insights",             "https://www.millerthomson.com/en/publications/rss/"),
    ("WeirFoulds",      "WeirFoulds News",         "https://www.weirfoulds.com/news-and-publications/rss"),
    # ── Industry feeds ───────────────────────────────────────────────────────
    ("__INDUSTRY__",    "Precedent Magazine",      "https://www.precedentjd.com/feed/"),
    ("__INDUSTRY__",    "Canadian Lawyer",         "https://www.canadianlawyermag.com/rss"),
    ("__INDUSTRY__",    "Law Times",               "https://www.lawtimesnews.com/rss"),
    ("__INDUSTRY__",    "The Lawyer's Daily",      "https://www.thelawyersdaily.ca/rss"),
    ("__INDUSTRY__",    "LegalFeed",               "https://legalfeeds.ca/feed"),
]

TIMEOUT = 12  # seconds
LOOKBACK_DAYS = 21


@dataclass
class FeedResult:
    label: str
    url: str
    firm: str
    status: str          # ok | http_error | timeout | parse_error | empty | stale
    http_code: Optional[int] = None
    entry_count: int = 0
    recent_entries: int = 0   # entries within LOOKBACK_DAYS
    newest_entry_date: Optional[str] = None
    error_detail: Optional[str] = None
    keyword_matches: int = 0


def _fetch_feed(url: str) -> tuple[Optional[feedparser.FeedParserDict], Optional[str], Optional[int]]:
    """Returns (parsed_feed, error_message, http_status_code)."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (compatible; LawFirmTracker/3.0; "
            "+https://github.com/shaheersinn/Law-Firm-Expansion-Tracker)"
        )
    }
    try:
        resp = requests.get(url, headers=headers, timeout=TIMEOUT)
        if resp.status_code != 200:
            return None, f"HTTP {resp.status_code}", resp.status_code
        parsed = feedparser.parse(resp.content)
        return parsed, None, resp.status_code
    except requests.exceptions.Timeout:
        return None, "Timeout", None
    except Exception as exc:
        return None, str(exc), None


def _cutoff_dt() -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)


def check_feed(firm: str, label: str, url: str) -> FeedResult:
    result = FeedResult(label=label, url=url, firm=firm, status="ok")
    parsed, err, code = _fetch_feed(url)
    result.http_code = code

    if err:
        result.status = "timeout" if "Timeout" in err else "http_error"
        result.error_detail = err
        return result

    if not parsed or not parsed.entries:
        result.status = "empty"
        return result

    result.entry_count = len(parsed.entries)
    cutoff = _cutoff_dt()
    newest = None

    for entry in parsed.entries:
        # Parse date
        pub = None
        if hasattr(entry, "published_parsed") and entry.published_parsed:
            try:
                pub = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
            except Exception:
                pass

        if pub:
            if newest is None or pub > newest:
                newest = pub
            if pub >= cutoff:
                result.recent_entries += 1

    if newest:
        result.newest_entry_date = newest.strftime("%Y-%m-%d")

    if result.recent_entries == 0 and result.entry_count > 0:
        result.status = "stale"
    elif result.entry_count == 0:
        result.status = "empty"

    return result


def run_diagnostics(firm_filter: Optional[str] = None) -> list[FeedResult]:
    results = []
    feeds_to_check = RSS_FEEDS
    if firm_filter:
        feeds_to_check = [(f, l, u) for f, l, u in RSS_FEEDS if firm_filter.lower() in f.lower()]

    for firm, label, url in feeds_to_check:
        log.info("Checking: %s — %s", label, url)
        r = check_feed(firm, label, url)
        results.append(r)
        time.sleep(0.5)  # polite crawl delay

    return results


def print_report(results: list[FeedResult]) -> None:
    ok     = [r for r in results if r.status == "ok"]
    stale  = [r for r in results if r.status == "stale"]
    empty  = [r for r in results if r.status == "empty"]
    errors = [r for r in results if r.status in ("http_error", "timeout", "parse_error")]

    print(f"\n{'='*70}")
    print(f"RSS DIAGNOSTICS REPORT  —  {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*70}")
    print(f"  Total feeds checked : {len(results)}")
    print(f"  ✅ Active (recent)  : {len(ok)}")
    print(f"  🟡 Stale (> {LOOKBACK_DAYS}d)   : {len(stale)}")
    print(f"  ⚪ Empty            : {len(empty)}")
    print(f"  🔴 Errors           : {len(errors)}")

    if errors:
        print(f"\n{'─'*70}")
        print("ERRORS (fix required):")
        for r in errors:
            print(f"  [{r.firm}] {r.label}")
            print(f"    URL: {r.url}")
            print(f"    Error: {r.error_detail}  HTTP: {r.http_code}")

    if stale:
        print(f"\n{'─'*70}")
        print(f"STALE FEEDS (no entries in last {LOOKBACK_DAYS} days):")
        for r in stale:
            print(f"  [{r.firm}] {r.label}")
            print(f"    Entries: {r.entry_count}  Newest: {r.newest_entry_date}  URL: {r.url}")

    if ok:
        print(f"\n{'─'*70}")
        print("ACTIVE FEEDS:")
        for r in ok:
            print(
                f"  ✅ [{r.firm}] {r.label}  "
                f"{r.recent_entries}/{r.entry_count} recent entries  "
                f"(newest: {r.newest_entry_date})"
            )

    print(f"\n{'='*70}\n")

    # Actionable recommendations
    if stale or errors:
        print("RECOMMENDATIONS:")
        for r in stale:
            print(f"  • Update/replace RSS URL for [{r.firm}] {r.label}")
            print(f"    Current: {r.url}")
        for r in errors:
            print(f"  • Fix broken feed [{r.firm}] {r.label}: {r.error_detail}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--firm", help="Filter to a specific firm key")
    args = parser.parse_args()
    results = run_diagnostics(firm_filter=args.firm)
    print_report(results)
    bad = [r for r in results if r.status in ("stale", "http_error", "timeout", "empty")]
    sys.exit(1 if bad else 0)
