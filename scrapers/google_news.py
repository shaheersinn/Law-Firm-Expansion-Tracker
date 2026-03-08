"""
GoogleNewsScraper — searches Google News RSS per firm name.

This is the KEY fix for zero RSS signals. Instead of scanning general feeds
for firm mentions (rare), this builds a firm-specific Google News search URL
which surfaces articles that actually mention the firm.

Examples:
  "Blakes" → news.google.com/rss/search?q="Blakes+law+firm"&hl=en-CA...
  "Davies Ward" → news.google.com/rss/search?q="Davies+Ward"...

Produces lateral_hire, press_release, and practice_page signal types.
"""

import re
import time as _time
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from urllib.parse import quote_plus

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

try:
    import feedparser
    HAS_FEEDPARSER = True
except ImportError:
    HAS_FEEDPARSER = False

classifier = DepartmentClassifier()

GOOGLE_NEWS_BASE = (
    "https://news.google.com/rss/search"
    "?q={query}&hl=en-CA&gl=CA&ceid=CA:en"
)

LATERAL_PHRASES = [
    "joins", "joined", "has joined", "welcomes", "new partner", "lateral hire",
    "lateral move", "expands team", "named partner", "appointed partner",
    "hires", "recruits", "new associate", "new counsel",
]

EXPANSION_PHRASES = [
    "opens office", "new office", "expands to", "launches practice",
    "new practice group", "establishes", "strategic alliance", "merger",
    "acquires", "acquired by",
]

DEAL_PHRASES = [
    "advises", "advised", "acts as counsel", "counsel to",
    "represents", "successfully completed", "closes acquisition",
]

LOOKBACK_DAYS = 21


def _parse_date(entry: dict) -> datetime | None:
    for key in ("published_parsed", "updated_parsed"):
        ts = entry.get(key)
        if ts:
            try:
                return datetime.fromtimestamp(_time.mktime(ts), tz=timezone.utc)
            except Exception:
                pass
    for key in ("published", "updated"):
        raw = entry.get(key, "")
        if not raw:
            continue
        try:
            return parsedate_to_datetime(raw).astimezone(timezone.utc)
        except Exception:
            pass
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass
    return None


def _is_recent(entry: dict) -> bool:
    dt = _parse_date(entry)
    if dt is None:
        return True   # unknown — pass through
    return dt >= datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)


class GoogleNewsScraper(BaseScraper):
    name = "GoogleNewsScraper"

    def fetch(self, firm: dict) -> list[dict]:
        if not HAS_FEEDPARSER:
            self.logger.warning("feedparser not installed — skipping GoogleNewsScraper")
            return []

        signals = []
        seen: set[str] = set()

        # Build search queries — 3 angles per firm
        queries = self._build_queries(firm)

        for q, weight_mult in queries:
            encoded = quote_plus(q)
            url     = GOOGLE_NEWS_BASE.format(query=encoded)
            sigs    = self._fetch_feed(firm, url, weight_mult, seen)
            signals.extend(sigs)
            seen.update(s.get("url", "") for s in sigs)

        return signals

    def _build_queries(self, firm: dict) -> list[tuple[str, float]]:
        short = firm["short"]
        name  = firm["name"]
        # First word of full name (e.g. "McCarthy" from "McCarthy Tétrault LLP")
        first = name.split()[0]

        queries = [
            (f'"{short}" law firm',           2.0),
            (f'"{short}" lawyer OR counsel',  2.5),
            (f'"{first}" Canadian law',       1.5),
        ]

        # Add alt names if present
        for alt in firm.get("alt_names", [])[:1]:
            queries.append((f'"{alt}"', 1.8))

        return queries

    def _fetch_feed(self, firm, url, weight_mult, seen):
        signals = []
        try:
            feed = feedparser.parse(url, request_headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalTracker/1.0)"
            })
        except Exception as e:
            self.logger.debug(f"Google News RSS error: {e}")
            return signals

        firm_names_lower = [
            firm["short"].lower(),
            firm["name"].split()[0].lower(),
        ] + [a.lower() for a in firm.get("alt_names", [])]

        for entry in (feed.entries or [])[:20]:
            if not _is_recent(entry):
                continue

            title   = entry.get("title", "")
            summary = entry.get("summary", "")
            link    = entry.get("link", url)

            if link in seen:
                continue

            full  = f"{title} {summary}"
            lower = full.lower()

            # Verify firm is actually mentioned
            if not any(n in lower for n in firm_names_lower):
                continue

            is_lateral   = any(p in lower for p in LATERAL_PHRASES)
            is_expansion = any(p in lower for p in EXPANSION_PHRASES)
            is_deal      = any(p in lower for p in DEAL_PHRASES)

            if not (is_lateral or is_expansion or is_deal or len(full) > 120):
                continue

            if is_lateral:
                sig_type, base_mult = "lateral_hire",  3.0
            elif is_expansion:
                sig_type, base_mult = "practice_page", 2.5
            elif is_deal:
                sig_type, base_mult = "press_release", 2.0
            else:
                sig_type, base_mult = "press_release", 1.2

            cls = classifier.classify(full, top_n=1)
            if not cls:
                continue
            c = cls[0]

            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type=sig_type,
                title=f"[GNews] {title[:160]}",
                body=summary[:500],
                url=link,
                department=c["department"],
                department_score=c["score"] * base_mult * weight_mult,
                matched_keywords=c["matched_keywords"],
            ))

        return signals
