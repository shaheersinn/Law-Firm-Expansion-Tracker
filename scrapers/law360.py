"""
Law360CanadaScraper
===================
Monitors Law360 Canada news coverage for firm mentions, deal coverage,
and practice-group highlights. Law360 Canada (formerly Law360 CA) is
a premier paid legal newswire; its RSS feed is publicly accessible.

Source:
  - Law360 Canada RSS  https://www.law360.ca/rss
  - Lexology Canada RSS https://www.lexology.com/rss/feed/canada.xml
  - Mondaq Canada RSS   https://www.mondaq.com/rss/canada/rss
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

LAW360_FEEDS = [
    {"url": "https://www.law360.ca/rss",                        "name": "Law360 CA"},
    {"url": "https://www.lexology.com/rss/feed/canada.xml",     "name": "Lexology CA"},
    {"url": "https://www.mondaq.com/rss/canada/rss",            "name": "Mondaq CA"},
]

LAW360_WEIGHT = 2.0

RELEVANCE_PHRASES = [
    "law firm", "legal", "counsel", "partner", "practice", "litigation",
    "transaction", "merger", "acquisition", "regulatory",
]


class Law360CanadaScraper(BaseScraper):
    name = "Law360CanadaScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        try:
            import feedparser
        except ImportError:
            return signals

        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        for feed_meta in LAW360_FEEDS:
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
                    if not any(p in lower for p in RELEVANCE_PHRASES):
                        continue

                    dept, score, kw = _clf.top_department(full)
                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="press_release",
                        title=f"[{feed_meta['name']}] {title[:160]}",
                        body=summary[:600],
                        url=link,
                        department=dept,
                        department_score=score * LAW360_WEIGHT,
                        matched_keywords=kw,
                        published_at=pub,
                    ))
            except Exception as e:
                self.logger.debug(f"Law360 {feed_meta['url']}: {e}")

        return signals[:15]
