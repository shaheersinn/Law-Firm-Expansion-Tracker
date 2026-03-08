"""
OfficeTracker — detects office openings, new city presence, strategic alliances,
and firm mergers. These are the highest-value expansion signals.

Sources:
  - Firm website "About / Offices" pages (new city detection)
  - Google News: office opening + merger queries per firm
  - Canadian Lawyer / Precedent / Law Times (via media.py complement)
  - Newswire.ca press releases
  - PR Newswire Canada (cision)
  - LinkedIn company updates (limited)

Signal types:
  practice_page  — office opening, new city presence (weight 5.0)
  press_release  — strategic alliance / merger announcement (weight 4.5)
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
LOOKBACK_DAYS = 90  # office openings are rarer — look further back

OFFICE_RE = re.compile(
    r"open(?:s|ed|ing)\s+(?:new\s+)?(?:office|location|presence)\s+in"
    r"|(?:new\s+)?office\s+in\s+\w+"
    r"|expands?\s+(?:to|into)\s+\w+"
    r"|establishes?\s+(?:presence|office)\s+in"
    r"|launches?\s+(?:in|new\s+)(?:office|location)"
    r"|relocat(?:es?|ed|ing)\s+(?:to|its)",
    re.IGNORECASE,
)

MERGER_RE = re.compile(
    r"merges?\s+with"
    r"|combination\s+with"
    r"|joins?\s+forces\s+with"
    r"|strategic\s+(?:alliance|combination|partnership|merger)"
    r"|combined\s+firm"
    r"|lateral\s+combination"
    r"|new\s+(?:firm|legal\s+brand)",
    re.IGNORECASE,
)

CANADIAN_CITIES = [
    "toronto", "montreal", "vancouver", "calgary", "ottawa", "edmonton",
    "winnipeg", "quebec city", "hamilton", "kitchener", "london",
    "victoria", "halifax", "saskatoon", "regina", "fredericton",
    "charlottetown", "whitehorse", "yellowknife", "iqaluit",
    # US cities (Canadian firms expanding cross-border)
    "new york", "chicago", "los angeles", "houston", "washington",
]

OFFICE_QUERIES = [
    ('"{short}" opens office',                5.0),
    ('"{short}" new office Canada',           5.0),
    ('"{short}" expands to city',             5.0),
    ('"{short}" merges with law firm',        5.0),
    ('"{short}" strategic alliance firm',     4.5),
    ('"{short}" new practice group launch',   4.0),
    ('"{short}" office opening announcement', 5.0),
]

GOOG = "https://news.google.com/rss/search?q={q}&hl=en-CA&gl=CA&ceid=CA:en"

# RSS sources with office news
NEWSWIRE_URL = "https://www.newswire.ca/rss/"
CISION_URL   = "https://www.cision.com/ca/rss/"


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


class OfficeTracker(BaseScraper):
    name = "OfficeTracker"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        seen: set = set()
        firm_tokens = [firm["short"].lower(), firm["name"].split()[0].lower()] + \
                      [a.lower() for a in firm.get("alt_names", [])]

        # 1 — Scrape firm's own offices/locations page
        signals.extend(self._scrape_offices_page(firm, seen))

        # 2 — Google News for office and merger events
        if HAS_FEEDPARSER:
            signals.extend(self._google_office_news(firm, firm_tokens, seen))

        # 3 — Newswire.ca press releases
        if HAS_FEEDPARSER:
            signals.extend(self._from_rss(firm, firm_tokens, NEWSWIRE_URL, "Newswire", seen))

        return signals[:10]

    def _scrape_offices_page(self, firm, seen) -> list[dict]:
        """Check firm's offices/locations page for city additions."""
        base = firm["website"].rstrip("/")
        office_urls = [
            f"{base}/offices",
            f"{base}/en/offices",
            f"{base}/en/canada/offices",
            f"{base}/locations",
            f"{base}/about/offices",
            f"{base}/en/about/offices",
        ]
        signals = []
        for url in office_urls:
            soup = self.get_soup(url)
            if not soup:
                continue
            text = soup.get_text(" ", strip=True).lower()

            # Look for city names that suggest new presence
            found_cities = [c for c in CANADIAN_CITIES if c in text]
            if not found_cities:
                continue

            # Check page title for "new" language
            title_tag = soup.find("title")
            page_title = title_tag.get_text() if title_tag else url

            signals.append(self._make_signal(
                firm_id=firm["id"], firm_name=firm["name"],
                signal_type="practice_page",
                title=f"[Offices] {firm['short']} office presence: {', '.join(found_cities[:5])}",
                body=f"Cities found on offices page: {', '.join(found_cities)}",
                url=url,
                department="Corporate/M&A",
                department_score=2.0,
                matched_keywords=found_cities[:5],
            ))
            break  # one page is enough

        return signals

    def _google_office_news(self, firm, firm_tokens, seen) -> list[dict]:
        signals = []
        for q_tpl, weight in OFFICE_QUERIES[:4]:
            q   = q_tpl.format(short=firm["short"])
            url = GOOG.format(q=quote_plus(q))
            try:
                feed = feedparser.parse(url, request_headers={
                    "User-Agent": "Mozilla/5.0 (compatible; LegalTracker/2.0)"
                })
            except Exception:
                continue

            for entry in (feed.entries or [])[:8]:
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
                if not (OFFICE_RE.search(full) or MERGER_RE.search(full)):
                    continue

                sig_type = "press_release" if MERGER_RE.search(full) else "practice_page"
                cls = classifier.classify(full, top_n=1)
                dept = cls[0]["department"] if cls else "Corporate/M&A"

                signals.append(self._make_signal(
                    firm_id=firm["id"], firm_name=firm["name"],
                    signal_type=sig_type,
                    title=f"[Office/Merger] {title[:160]}",
                    body=summary[:400],
                    url=link,
                    department=dept,
                    department_score=(cls[0]["score"] if cls else 1.0) * weight,
                    matched_keywords=cls[0]["matched_keywords"] if cls else [],
                ))
                seen.add(link)
        return signals

    def _from_rss(self, firm, firm_tokens, rss_url, src_name, seen) -> list[dict]:
        signals = []
        try:
            feed = feedparser.parse(rss_url, request_headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalTracker/2.0)"
            })
        except Exception:
            return []

        for entry in (feed.entries or [])[:30]:
            if not _is_recent(entry):
                continue
            title   = entry.get("title", "")
            summary = entry.get("summary", "")
            link    = entry.get("link", rss_url)
            if link in seen:
                continue
            full  = f"{title} {summary}"
            lower = full.lower()
            if not any(t in lower for t in firm_tokens):
                continue
            if not (OFFICE_RE.search(full) or MERGER_RE.search(full)):
                continue

            sig_type = "press_release" if MERGER_RE.search(full) else "practice_page"
            cls = classifier.classify(full, top_n=1)

            signals.append(self._make_signal(
                firm_id=firm["id"], firm_name=firm["name"],
                signal_type=sig_type,
                title=f"[{src_name}] {title[:160]}",
                body=summary[:400],
                url=link,
                department=cls[0]["department"] if cls else "Corporate/M&A",
                department_score=(cls[0]["score"] if cls else 1.0) * 4.5,
                matched_keywords=cls[0]["matched_keywords"] if cls else [],
            ))
            seen.add(link)
        return signals
