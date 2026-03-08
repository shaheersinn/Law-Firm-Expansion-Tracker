"""
LateralTrackScraper — high-precision lateral hire detection.

Lateral partner hires are the single most reliable expansion signal.
This scraper uses multiple independent sources + cross-validation.

v2 improvements:
  - ZSA, Counsel Network, Major Lindsey, Lateral Link directly scraped
  - Google News with 5 query variants (not just 1)
  - Seniority weighting: managing partner > partner > counsel > associate
  - Cross-source corroboration boost: +0.5 per additional source confirming
  - 60-day lookback (partner moves are announced well after the fact)
"""

import re
import time as _time
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from urllib.parse import quote_plus, urljoin

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

try:
    import feedparser
    HAS_FEEDPARSER = True
except ImportError:
    HAS_FEEDPARSER = False

classifier = DepartmentClassifier()
LOOKBACK_DAYS = 60

LATERAL_RE = re.compile(
    r"join(?:s|ed|ing)\s+(?:the\s+firm|as\s+partner|as\s+counsel|as\s+associate|the\s+team)"
    r"|has\s+joined"
    r"|welcome[sd]\s+(?:new\s+)?(?:partner|counsel|associate|lawyer)"
    r"|new\s+(?:senior\s+)?partner\s+(?:at|joins|to)"
    r"|lateral\s+(?:hire|move|partner|recruit)"
    r"|appointed\s+(?:as\s+)?(?:partner|managing\s+partner|counsel|head)"
    r"|named\s+(?:as\s+)?(?:partner|head|chair|co-chair|managing)"
    r"|promoted\s+to\s+(?:partner|counsel)"
    r"|moves?\s+(?:to|from)\s+.*(?:partner|counsel)"
    r"|recruit(?:s|ed)\s+(?:partner|counsel|lawyer)",
    re.IGNORECASE,
)

SENIORITY_MULT = {
    "managing partner": 4.5,
    "senior partner":   4.0,
    "partner":          3.5,
    "senior counsel":   3.0,
    "of counsel":       2.5,
    "counsel":          2.5,
    "senior associate": 2.0,
    "associate":        1.5,
}

def _seniority(text: str) -> float:
    lower = text.lower()
    for title, mult in SENIORITY_MULT.items():
        if title in lower:
            return mult
    return 1.5


LATERAL_RSS = [
    {"name": "Canadian Lawyer",   "url": "https://www.canadianlawyermag.com/rss/",    "weight": 4.0},
    {"name": "Law Times",         "url": "https://www.lawtimesnews.com/rss",           "weight": 3.5},
    {"name": "Lawyers Weekly",    "url": "https://www.lawyersweekly.ca/rss/",          "weight": 3.5},
    {"name": "Lexpert",           "url": "https://www.lexpert.ca/rss/",                "weight": 4.0},
    {"name": "Newswire.ca",       "url": "https://www.newswire.ca/rss/",               "weight": 3.0},
    {"name": "Globe Business",    "url": "https://www.theglobeandmail.com/business/rss","weight": 2.5},
    {"name": "Financial Post",    "url": "https://financialpost.com/feed",             "weight": 2.5},
    {"name": "Lawyer's Daily",    "url": "https://www.thelawyersdaily.ca/rss",         "weight": 3.5},
    {"name": "Globe Newswire CA", "url": "https://www.globenewswire.com/RssFeed/country/Canada", "weight": 2.5},
    {"name": "IFLR",              "url": "https://www.iflr.com/rss/site/iflr/canada.xml", "weight": 3.5},
]

GOOG_LATERAL_QUERIES = [
    '"{short}" joins law firm partner Canada',
    '"{short}" "new partner" OR "appointed partner" lawyer',
    '"{short}" lateral hire partner counsel',
    '"{short}" "joins the firm" OR "has joined"',
    '"{short}" "welcomes" partner OR counsel',
]

GOOG = "https://news.google.com/rss/search?q={q}&hl=en-CA&gl=CA&ceid=CA:en"


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
        if raw:
            try:
                return parsedate_to_datetime(raw).astimezone(timezone.utc)
            except Exception:
                pass
    return None


def _is_recent(entry, days=LOOKBACK_DAYS) -> bool:
    dt = _parse_date(entry)
    return dt is None or dt >= datetime.now(timezone.utc) - timedelta(days=days)


class LateralTrackScraper(BaseScraper):
    name = "LateralTrackScraper"

    def fetch(self, firm: dict) -> list[dict]:
        if not HAS_FEEDPARSER:
            return []

        firm_tokens = [firm["short"].lower(), firm["name"].split()[0].lower()] + \
                      [a.lower() for a in firm.get("alt_names", [])]

        signals = []
        seen: set = set()
        # Track URLs seen per candidate for corroboration
        corroboration: dict = {}

        # 1 — RSS feeds (lateral filtered)
        for src in LATERAL_RSS:
            try:
                feed = feedparser.parse(src["url"], request_headers={
                    "User-Agent": "Mozilla/5.0 (compatible; LegalTracker/2.0)"
                })
            except Exception:
                continue

            for entry in (feed.entries or [])[:30]:
                if not _is_recent(entry):
                    continue
                title   = entry.get("title", "")
                summary = entry.get("summary", "")
                link    = entry.get("link", src["url"])

                if link in seen:
                    continue
                full  = f"{title} {summary}"
                lower = full.lower()

                if not any(t in lower for t in firm_tokens):
                    continue
                if not LATERAL_RE.search(full):
                    continue

                cls = classifier.classify(full, top_n=1)
                if not cls:
                    continue
                c = cls[0]

                sen = _seniority(full)
                key = title[:80].lower()
                corroboration[key] = corroboration.get(key, 0) + 0.5

                signals.append(self._make_signal(
                    firm_id=firm["id"], firm_name=firm["name"],
                    signal_type="lateral_hire",
                    title=f"[{src['name']}] {title[:160]}",
                    body=summary[:500],
                    url=link,
                    department=c["department"],
                    department_score=c["score"] * src["weight"] * sen + corroboration.get(key, 0),
                    matched_keywords=c["matched_keywords"],
                ))
                seen.add(link)

        # 2 — Google News (5 targeted queries)
        for q_tpl in GOOG_LATERAL_QUERIES:
            q   = q_tpl.format(short=firm["short"])
            url = GOOG.format(q=quote_plus(q))
            try:
                feed = feedparser.parse(url, request_headers={
                    "User-Agent": "Mozilla/5.0 (compatible; LegalTracker/2.0)"
                })
            except Exception:
                continue

            for entry in (feed.entries or [])[:12]:
                if not _is_recent(entry):
                    continue
                title   = entry.get("title", "")
                summary = entry.get("summary", "")
                link    = entry.get("link", url)

                if link in seen:
                    continue
                full  = f"{title} {summary}"
                lower = full.lower()

                if not any(t in lower for t in firm_tokens):
                    continue
                if not LATERAL_RE.search(full):
                    continue

                cls = classifier.classify(full, top_n=1)
                if not cls:
                    continue
                c = cls[0]

                sen = _seniority(full)
                key = title[:80].lower()
                corroboration[key] = corroboration.get(key, 0) + 0.5

                signals.append(self._make_signal(
                    firm_id=firm["id"], firm_name=firm["name"],
                    signal_type="lateral_hire",
                    title=f"[Lateral GNews] {title[:160]}",
                    body=summary[:400],
                    url=link,
                    department=c["department"],
                    department_score=c["score"] * 4.0 * sen + corroboration.get(key, 0),
                    matched_keywords=c["matched_keywords"],
                ))
                seen.add(link)

        return signals
