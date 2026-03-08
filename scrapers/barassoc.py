"""
BarAssociationScraper — BUG-8 FIX.

Old bug: scraped CBA/OBA section listing pages that list TOPICS (not lawyers),
         so firm names never appeared → 0 signals for all 26 firms.

New approach:
  1. Google News RSS: "[firm] bar association" / "[firm] appointed chair"
  2. CBA/OBA board/officers pages (these DO name lawyers + their firms)
  3. Bencher election results pages (LSO)
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

LOOKBACK_DAYS = 60   # bar appointments are less frequent — use 60-day window

# These pages actually list officer names WITH firm affiliations
OFFICER_PAGES = [
    {"name": "OBA Board",      "url": "https://www.oba.org/About/Board-of-Directors",        "weight": 3.5},
    {"name": "Advocates Soc.", "url": "https://www.advocates.ca/about/board-of-directors",   "weight": 3.0},
    {"name": "CCCA Board",     "url": "https://ccca-caj.ca/en/about/board-of-directors/",    "weight": 2.5},
    {"name": "ACC Canada",     "url": "https://www.acc.com/chapters/canada",                 "weight": 2.0},
    {"name": "LSO Benchers",   "url": "https://lso.ca/about-lso/governance/benchers",        "weight": 3.5},
]

LEADERSHIP_KWS = [
    "chair", "vice-chair", "president", "director", "executive committee",
    "board member", "elected", "appointed", "co-chair", "past president",
    "treasurer", "secretary", "bencher", "governor", "counsel",
]

APPOINTMENT_PHRASES = [
    "appointed", "elected to", "named chair", "named president",
    "joins board", "new chair", "new president", "bar association",
    "bencher", "section chair", "bar leadership", "governance",
]

# Google News queries for bar appointments
BAR_NEWS_QUERIES = [
    ('"{short}" bar association appointed', 3.0),
    ('"{short}" bencher elected',           3.5),
    ('"{short}" law society',               2.5),
]


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
    return None


def _is_recent(entry, days=LOOKBACK_DAYS) -> bool:
    dt = _parse_date(entry)
    if dt is None:
        return True
    return dt >= datetime.now(timezone.utc) - timedelta(days=days)


class BarAssociationScraper(BaseScraper):
    name = "BarAssociationScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        firm_names_lower = [
            firm["short"].lower(),
            firm["name"].split()[0].lower(),
        ] + [a.lower() for a in firm.get("alt_names", [])]

        # Source 1: scrape officer/board pages
        for page in OFFICER_PAGES:
            soup = self.get_soup(page["url"])
            if not soup:
                continue

            page_text = soup.get_text(" ", strip=True).lower()
            if not any(n in page_text for n in firm_names_lower):
                continue

            for tag in soup.find_all(["li", "p", "td", "div", "article", "tr"], limit=400):
                text  = tag.get_text(" ", strip=True)
                lower = text.lower()

                if not any(n in lower for n in firm_names_lower):
                    continue
                if len(text) < 15 or len(text) > 500:
                    continue

                is_leader = any(kw in lower for kw in LEADERSHIP_KWS)
                sig_type  = "bar_leadership" if is_leader else "bar_speaking"
                w_mult    = 1.5 if is_leader else 1.0

                cls  = classifier.top_department(text)
                dept = cls["department"] if cls else "Corporate/M&A"

                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type=sig_type,
                    title=f"[{page['name']}] {text[:160]}",
                    url=page["url"],
                    department=dept,
                    department_score=(cls["score"] if cls else 1.0) * page["weight"] * w_mult,
                    matched_keywords=cls["matched_keywords"] if cls else [],
                ))

        # Source 2: Google News RSS for bar appointments
        if HAS_FEEDPARSER:
            for query_tpl, w_mult in BAR_NEWS_QUERIES:
                q   = query_tpl.format(short=firm["short"])
                url = (
                    "https://news.google.com/rss/search"
                    f"?q={quote_plus(q)}&hl=en-CA&gl=CA&ceid=CA:en"
                )
                try:
                    feed = feedparser.parse(url, request_headers={
                        "User-Agent": "Mozilla/5.0 (compatible; LegalTracker/1.0)"
                    })
                except Exception:
                    continue

                for entry in (feed.entries or [])[:10]:
                    if not _is_recent(entry):
                        continue

                    title   = entry.get("title", "")
                    summary = entry.get("summary", "")
                    full    = f"{title} {summary}"
                    lower   = full.lower()

                    if not any(n in lower for n in firm_names_lower):
                        continue
                    if not any(p in lower for p in APPOINTMENT_PHRASES):
                        continue

                    cls = classifier.classify(full, top_n=1)
                    if not cls:
                        continue
                    c = cls[0]

                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="bar_leadership",
                        title=f"[Bar News] {title[:160]}",
                        body=summary[:400],
                        url=entry.get("link", url),
                        department=c["department"],
                        department_score=c["score"] * w_mult,
                        matched_keywords=c["matched_keywords"],
                    ))

        return signals[:15]


try:
    from urllib.parse import quote_plus
except ImportError:
    pass
