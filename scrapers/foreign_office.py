"""
ForeignOfficeScraper
====================
Tracks international office opening, relocation, and expansion
announcements. Canadian firms expanding into the US, UK, or Asia
Pacific generate strong signals for corporate/M&A and capital markets.

Sources:
  - Canadian Lawyer Magazine RSS
  - The Lawyer (UK) RSS — covers Canadian firms internationally
  - Globe & Mail Business RSS
  - Financial Post RSS
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

FOREIGN_FEEDS = [
    {"url": "https://www.canadianlawyermag.com/rss/",       "name": "Canadian Lawyer"},
    {"url": "https://www.thelawyersdaily.ca/rss",           "name": "Lawyer's Daily"},
    {"url": "https://www.theglobeandmail.com/business/rss", "name": "Globe B&M"},
    {"url": "https://financialpost.com/feed",               "name": "Financial Post"},
]

FOREIGN_PHRASES = [
    "opens office", "new office", "office opening", "expands to",
    "international office", "New York office", "London office",
    "Hong Kong office", "Singapore office", "Dubai office",
    "US expansion", "UK expansion", "global expansion",
    "international expansion", "cross-border", "opens in",
    "sets up office", "establishes office",
]

FOREIGN_WEIGHT = 3.5


class ForeignOfficeScraper(BaseScraper):
    name = "ForeignOfficeScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        try:
            import feedparser
        except ImportError:
            return signals

        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        for feed_meta in FOREIGN_FEEDS:
            try:
                feed = feedparser.parse(feed_meta["url"])
                for entry in (feed.entries or [])[:20]:
                    title   = entry.get("title", "")
                    summary = entry.get("summary", entry.get("description", ""))
                    link    = entry.get("link", feed_meta["url"])
                    pub     = entry.get("published", "")
                    full    = f"{title} {summary}"
                    lower   = full.lower()

                    if not any(n.lower() in lower for n in firm_names):
                        continue
                    if not any(p.lower() in lower for p in FOREIGN_PHRASES):
                        continue

                    dept, score, kw = _clf.top_department(full)
                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="office_lease",
                        title=f"[{feed_meta['name']}] {title[:160]}",
                        body=summary[:600],
                        url=link,
                        department=dept or "Corporate / M&A",
                        department_score=score * FOREIGN_WEIGHT,
                        matched_keywords=kw,
                        published_at=pub,
                    ))
            except Exception as e:
                self.logger.debug(f"ForeignOffice {feed_meta['url']}: {e}")

        return signals
