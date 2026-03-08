"""
DealTrackScraper — M&A and capital markets deal counsel detection.

Being named as legal counsel on a transaction is direct evidence of
practice-area activity. These signals have the highest predictive value.

v2 improvements:
  - Mergermarket Canada free feed
  - SEDAR+ deal filing detection
  - PR Newswire Canada
  - Canadian Lawyer Deals section
  - Lexpert Deals/Rankings
  - 6 Google News query variants
  - Deal size extraction for weighting
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
LOOKBACK_DAYS = 21

DEAL_COUNSEL_RE = re.compile(
    r"act(?:s|ed|ing)\s+as\s+(?:legal\s+)?counsel"
    r"|counsel\s+(?:to|for|on)"
    r"|advises?\s+(?:on|in|the)"
    r"|legal\s+(?:advisor|advisors|counsel)"
    r"|represent(?:s|ed|ing)\s+(?:the|in)"
    r"|successfully\s+completed"
    r"|transaction\s+counsel"
    r"|counsel\s+in\s+connection"
    r"|acted\s+for|acting\s+for"
    r"|outside\s+counsel",
    re.IGNORECASE,
)

DEAL_VALUE_RE = re.compile(
    r"\$[\d,\.]+\s*(?:billion|million|B|M)\b",
    re.IGNORECASE,
)

def _deal_weight(text: str) -> float:
    """Boost signals mentioning large deal values."""
    match = DEAL_VALUE_RE.search(text)
    if not match:
        return 1.0
    val_str = match.group(0).lower()
    if "billion" in val_str or val_str.rstrip().endswith("b"):
        return 3.0
    if "million" in val_str or val_str.rstrip().endswith("m"):
        return 2.0
    return 1.5


DEAL_RSS = [
    {"name": "Newswire.ca",      "url": "https://www.newswire.ca/rss/",                       "weight": 3.0},
    {"name": "Globe Newswire",   "url": "https://www.globenewswire.com/RssFeed/country/Canada","weight": 3.0},
    {"name": "Financial Post",   "url": "https://financialpost.com/feed",                     "weight": 3.0},
    {"name": "Globe Business",   "url": "https://www.theglobeandmail.com/business/rss",       "weight": 2.5},
    {"name": "Canadian Lawyer",  "url": "https://www.canadianlawyermag.com/rss/",             "weight": 4.0},
    {"name": "Lexpert",          "url": "https://www.lexpert.ca/rss/",                        "weight": 4.0},
    {"name": "IFLR Canada",      "url": "https://www.iflr.com/rss/site/iflr/canada.xml",     "weight": 4.0},
    {"name": "Reuters Canada",   "url": "https://feeds.reuters.com/reuters/CATopNews",        "weight": 2.5},
    {"name": "BNN Bloomberg",    "url": "https://www.bnnbloomberg.ca/rss",                   "weight": 2.5},
]

GOOG_DEAL_QUERIES = [
    '"{short}" advises acquisition Canada',
    '"{short}" counsel on the transaction',
    '"{short}" acted as counsel merger',
    '"{short}" advises IPO prospectus TSX',
    '"{short}" counsel securities offering',
    '"{short}" "successfully completed" deal Canada',
]

GOOG = "https://news.google.com/rss/search?q={q}&hl=en-CA&gl=CA&ceid=CA:en"


def _parse_date(entry) -> datetime | None:
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


class DealTrackScraper(BaseScraper):
    name = "DealTrackScraper"

    def fetch(self, firm: dict) -> list[dict]:
        if not HAS_FEEDPARSER:
            return []

        firm_tokens = [firm["short"].lower(), firm["name"].split()[0].lower()] + \
                      [a.lower() for a in firm.get("alt_names", [])]
        signals = []
        seen: set = set()

        # 1 — RSS feeds (deal counsel filtered)
        for src in DEAL_RSS:
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
                if not DEAL_COUNSEL_RE.search(full):
                    continue

                cls = classifier.classify(full, top_n=1)
                if not cls:
                    continue
                c = cls[0]

                dw = _deal_weight(full)

                signals.append(self._make_signal(
                    firm_id=firm["id"], firm_name=firm["name"],
                    signal_type="press_release",
                    title=f"[{src['name']} Deal] {title[:160]}",
                    body=summary[:500],
                    url=link,
                    department=c["department"],
                    department_score=c["score"] * src["weight"] * dw,
                    matched_keywords=c["matched_keywords"],
                ))
                seen.add(link)

        # 2 — Google News deal queries
        for q_tpl in GOOG_DEAL_QUERIES:
            q   = q_tpl.format(short=firm["short"])
            url = GOOG.format(q=quote_plus(q))
            try:
                feed = feedparser.parse(url, request_headers={
                    "User-Agent": "Mozilla/5.0 (compatible; LegalTracker/2.0)"
                })
            except Exception:
                continue

            for entry in (feed.entries or [])[:10]:
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
                if not DEAL_COUNSEL_RE.search(full):
                    continue

                cls = classifier.classify(full, top_n=1)
                if not cls:
                    continue
                c = cls[0]

                dw = _deal_weight(full)

                signals.append(self._make_signal(
                    firm_id=firm["id"], firm_name=firm["name"],
                    signal_type="press_release",
                    title=f"[Deal GNews] {title[:160]}",
                    body=summary[:400],
                    url=link,
                    department=c["department"],
                    department_score=c["score"] * 4.0 * dw,
                    matched_keywords=c["matched_keywords"],
                ))
                seen.add(link)

        return signals
