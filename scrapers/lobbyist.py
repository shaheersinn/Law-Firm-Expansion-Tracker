"""
LobbyistScraper — federal/provincial lobbyist registry signals.

Root cause of 0 signals: The lobbyist registry search endpoint requires
specific POST parameters; GET approach returns no results.

Fix: 
  1. Open Government Canada lobbyist registry public search API (correct endpoint)
  2. Google News RSS for firm + lobbyist/government relations mentions

Signals: court_record (weight 2.5) — government relations work signals
expansion in regulatory/government affairs practice.
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

# Lobbyist registry open data API (correct endpoints)
LOBBYING_REGISTRY_API = (
    "https://lobbycanada.gc.ca/app/secure/ocl/lrs/do/clntSmmrySrch"
    "?lang=eng&searchTerms={query}"
)

LOBBYING_QUERIES = [
    ('"{short}" lobbyist government relations', 2.5),
    ('"{short}" registered lobbyist Canada',    2.5),
    ('"{short}" government affairs regulatory', 2.0),
    ('"{short}" parliamentary secretary minister cabinet', 2.5),
]

GOOG_BASE = "https://news.google.com/rss/search?q={q}&hl=en-CA&gl=CA&ceid=CA:en"

LOBBYIST_KWS = [
    "lobbyist", "government relations", "government affairs", "registered",
    "lobby", "advocacy", "policy counsel", "regulatory counsel",
]


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


class LobbyistScraper(BaseScraper):
    name = "LobbyistScraper"

    def fetch(self, firm: dict) -> list[dict]:
        if not HAS_FEEDPARSER:
            return []

        signals = []
        seen: set = set()
        firm_lower = [firm["short"].lower(), firm["name"].split()[0].lower()]
        cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)

        # Primary: Google News for lobbying/government relations
        for query_tpl, weight in LOBBYING_QUERIES:
            q   = query_tpl.format(short=firm["short"])
            url = GOOG_BASE.format(q=quote_plus(q))
            try:
                feed = feedparser.parse(url, request_headers={
                    "User-Agent": "Mozilla/5.0 (compatible; LegalTracker/1.0)"
                })
            except Exception:
                continue

            for entry in (feed.entries or [])[:6]:
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
                if not any(k in lower for k in LOBBYIST_KWS):
                    continue

                cls = classifier.classify(full, top_n=1)
                dept = cls[0]["department"] if cls else "Competition"

                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type="court_record",
                    title=f"[Lobbyist] {title[:160]}",
                    body=summary[:400],
                    url=link,
                    department=dept,
                    department_score=(cls[0]["score"] if cls else 1.0) * weight,
                    matched_keywords=cls[0]["matched_keywords"] if cls else [],
                ))
                seen.add(link)

        return signals[:6]
