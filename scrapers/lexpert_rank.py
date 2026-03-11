"""
LexpertRankScraper
==================
Monitors Lexpert (part of the Thomson Reuters Canada Legal family)
rankings, "Rising Stars", and special reports. Lexpert rankings are
widely cited for Canadian law firm practice area strength and partner quality.

Sources:
  - Lexology Canada RSS  https://www.lexology.com/rss/feed/canada.xml
  - Canadian Lawyer RSS  (covers Lexpert results extensively)
  - The Lawyer's Daily RSS
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

LEXPERT_FEEDS = [
    {"url": "https://www.lexology.com/rss/feed/canada.xml",     "name": "Lexology CA"},
    {"url": "https://www.canadianlawyermag.com/rss/",           "name": "Canadian Lawyer"},
    {"url": "https://www.thelawyersdaily.ca/rss",               "name": "Lawyer's Daily"},
]

LEXPERT_PHRASES = [
    "lexpert", "lexpert ranked", "lexpert recognized", "lexpert special edition",
    "lexpert rising stars", "lexpert guide", "lexpert rating",
    "ranked by lexpert", "recognized by lexpert", "lexpert directory",
    "BTI client service", "Canadian Legal Lexpert",
]

LEXPERT_WEIGHT = 2.5


class LexpertRankScraper(BaseScraper):
    name = "LexpertRankScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        try:
            import feedparser
        except ImportError:
            return signals

        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        for feed_meta in LEXPERT_FEEDS:
            try:
                feed = feedparser.parse(feed_meta["url"])
                for entry in (feed.entries or [])[:25]:
                    title   = entry.get("title", "")
                    summary = entry.get("summary", entry.get("description", ""))
                    link    = entry.get("link", feed_meta["url"])
                    pub     = entry.get("published", "")
                    full    = f"{title} {summary}"
                    lower   = full.lower()

                    if not any(n.lower() in lower for n in firm_names):
                        continue
                    if not any(p.lower() in lower for p in LEXPERT_PHRASES):
                        continue

                    dept, score, kw = _clf.top_department(full)
                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="ranking",
                        title=f"[{feed_meta['name']}] {title[:160]}",
                        body=summary[:600],
                        url=link,
                        department=dept,
                        department_score=score * LEXPERT_WEIGHT,
                        matched_keywords=kw,
                        published_at=pub,
                    ))
            except Exception as e:
                self.logger.debug(f"LexpertRank {feed_meta['url']}: {e}")

        return signals[:10]
