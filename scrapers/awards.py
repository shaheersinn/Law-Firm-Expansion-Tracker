"""
AwardsScraper — rankings/awards signals.

Root cause of 0 signals: Best Lawyers, Lexpert etc. are JavaScript-rendered
and block scraping. Fix: use Google News RSS to find award mentions per firm.

Signals: ranking (weight 3.5) — high value because external recognition
explicitly names the firm and a practice area.
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
LOOKBACK_DAYS = 90  # rankings are annual — look back further

RANKING_SOURCES = [
    {"name": "Best Lawyers",     "query": '"{short}" "best lawyers"',            "weight": 3.5},
    {"name": "Lexpert",          "query": '"{short}" lexpert ranked',             "weight": 3.5},
    {"name": "Chambers",         "query": '"{short}" chambers ranked band',       "weight": 3.5},
    {"name": "Legal 500",        "query": '"{short}" "legal 500" ranked',         "weight": 3.0},
    {"name": "Benchmark",        "query": '"{short}" benchmark litigation',        "weight": 3.0},
    {"name": "Canadian Lawyer",  "query": '"{short}" top law firm Canada',         "weight": 3.0},
    {"name": "IFLR",             "query": '"{short}" IFLR ranked',                "weight": 3.0},
    {"name": "RSG Canada",       "query": '"{short}" top boutique canada ranked', "weight": 2.5},
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


class AwardsScraper(BaseScraper):
    name = "AwardsScraper"

    def fetch(self, firm: dict) -> list[dict]:
        if not HAS_FEEDPARSER:
            return []

        signals = []
        seen: set = set()
        firm_lower = [firm["short"].lower(), firm["name"].split()[0].lower()]

        for src in RANKING_SOURCES:
            q   = src["query"].format(short=firm["short"])
            url = GOOG_BASE.format(q=quote_plus(q))
            try:
                feed = feedparser.parse(url, request_headers={
                    "User-Agent": "Mozilla/5.0 (compatible; LegalTracker/1.0)"
                })
            except Exception:
                continue

            for entry in (feed.entries or [])[:8]:
                dt = _parse_date(entry)
                if dt and dt < datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS):
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
                if not cls:
                    continue
                c = cls[0]

                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type="ranking",
                    title=f"[{src['name']}] {title[:160]}",
                    body=summary[:400],
                    url=link,
                    department=c["department"],
                    department_score=c["score"] * src["weight"],
                    matched_keywords=c["matched_keywords"],
                ))
                seen.add(link)

        return signals[:10]
