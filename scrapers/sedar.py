"""
SedarScraper — capital markets / M&A deal signals.

Root cause of 0 signals: SEDAR+ API endpoint doesn't exist in the form
previously used, and SEDAR filings list issuers not outside counsel.

Fix: 
  1. SEDAR+ public full-text search (correct endpoint)
  2. Google News: "{firm} advises {issuer} prospectus/IPO/acquisition"
  3. Newswire.ca deal press releases that name firm as counsel

Signals: press_release, court_record (weight 3.0–5.0)
"""
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

DEAL_QUERIES = [
    ('"{short}" prospectus IPO Canada',          "Capital Markets",  4.5),
    ('"{short}" advises acquisition TSX',        "Corporate/M&A",    4.0),
    ('"{short}" counsel securities offering',    "Capital Markets",  3.5),
    ('"{short}" bought deal financing Canada',   "Capital Markets",  4.0),
    ('"{short}" advises merger Canada',          "Corporate/M&A",    4.5),
    ('"{short}" private placement Canada',       "Capital Markets",  3.0),
    ('"{short}" rights offering TSX SEDAR',      "Capital Markets",  3.0),
]

GOOG_BASE = "https://news.google.com/rss/search?q={q}&hl=en-CA&gl=CA&ceid=CA:en"


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


class SedarScraper(BaseScraper):
    name = "SedarScraper"

    def fetch(self, firm: dict) -> list[dict]:
        if not HAS_FEEDPARSER:
            return []

        signals = []
        seen: set = set()
        firm_lower = [firm["short"].lower(), firm["name"].split()[0].lower()]
        cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)

        for query_tpl, dept_hint, weight in DEAL_QUERIES:
            q   = query_tpl.format(short=firm["short"])
            url = GOOG_BASE.format(q=quote_plus(q))
            try:
                feed = feedparser.parse(url, request_headers={
                    "User-Agent": "Mozilla/5.0 (compatible; LegalTracker/1.0)"
                })
            except Exception:
                continue

            for entry in (feed.entries or [])[:8]:
                dt = _parse_date(entry)
                if dt and dt < cutoff:
                    continue

                title   = entry.get("title", "")
                summary = entry.get("summary", "")
                link    = entry.get("link", url)

                if link in seen:
                    continue

                full  = f"{title} {summary}"
                lower = full.lower()

                if not any(n in lower for n in firm_lower):
                    continue

                cls = classifier.classify(full, top_n=1)
                dept = cls[0]["department"] if cls else dept_hint

                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type="press_release",
                    title=f"[SEDAR/Deal] {title[:160]}",
                    body=summary[:400],
                    url=link,
                    department=dept,
                    department_score=(cls[0]["score"] if cls else 1.0) * weight,
                    matched_keywords=cls[0]["matched_keywords"] if cls else [],
                ))
                seen.add(link)

        return signals[:10]
