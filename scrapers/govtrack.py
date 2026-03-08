"""
GovTrackScraper — regulatory signals.

Root cause of 0 signals: Government RSS feeds (Canada Gazette, OSC) don't
mention law firms by name — they list issuers/companies, not outside counsel.

Fix: Use Google News RSS to search for firm + regulatory body mentions.
These DO surface when a firm advises on a regulatory matter.

Signals: court_record, press_release (weight 2.5–3.5)
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
LOOKBACK_DAYS = 30

# Google News queries that surface regulatory work per firm
REG_QUERIES = [
    ('"{short}" competition bureau',     "Competition",      3.0),
    ('"{short}" OSC securities',         "Capital Markets",  3.0),
    ('"{short}" OSFI regulatory',        "Financial Services", 2.5),
    ('"{short}" privacy commissioner',   "Data Privacy",     3.0),
    ('"{short}" CRTC',                   "Financial Services", 2.5),
    ('"{short}" tribunal appeal',        "Litigation",       2.5),
    ('"{short}" class action settlement',"Litigation",       3.5),
    ('"{short}" merger review',          "Corporate/M&A",    3.0),
    ('"{short}" antitrust Canada',       "Competition",      3.0),
    ('"{short}" regulatory approval',    "Corporate/M&A",    2.5),
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


class GovTrackScraper(BaseScraper):
    name = "GovTrackScraper"

    def fetch(self, firm: dict) -> list[dict]:
        if not HAS_FEEDPARSER:
            return []

        signals = []
        seen: set = set()
        firm_lower = [firm["short"].lower(), firm["name"].split()[0].lower()]
        cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)

        for query_tpl, dept_hint, weight in REG_QUERIES:
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
                    signal_type="court_record",
                    title=f"[GovTrack] {title[:160]}",
                    body=summary[:400],
                    url=link,
                    department=dept,
                    department_score=(cls[0]["score"] if cls else 1.0) * weight,
                    matched_keywords=cls[0]["matched_keywords"] if cls else [],
                ))
                seen.add(link)

        return signals[:10]
