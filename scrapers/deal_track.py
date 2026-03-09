"""
DealTrackScraper
Tracks deal tombstones and transaction announcements.
When a firm acts as counsel on major transactions, it signals
practice group activity and future staffing demand.

Sources:
  - Cision Newswire RSS
  - CNW Group RSS
  - Globe B&M RSS
  - Financial Post RSS
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

DEAL_FEEDS = [
    {"url": "https://www.newswire.ca/rss/", "name": "Cision"},
    {"url": "https://financialpost.com/feed", "name": "Financial Post"},
    {"url": "https://www.theglobeandmail.com/business/rss", "name": "Globe B&M"},
]

DEAL_PHRASES = [
    "advises", "advised", "acts as counsel", "acted as counsel",
    "counsel to", "represented", "legal counsel", "successfully completed",
    "closes", "closed", "announces completion", "transaction",
]

DEAL_WEIGHT = 2.0


class DealTrackScraper(BaseScraper):
    name = "DealTrackScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        try:
            import feedparser
        except ImportError:
            return signals

        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        for feed_meta in DEAL_FEEDS:
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
                    if not any(p in lower for p in DEAL_PHRASES):
                        continue

                    dept, score, kw = _clf.top_department(full)
                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="deal_record",
                        title=f"[{feed_meta['name']}] {title[:160]}",
                        body=summary[:600],
                        url=link,
                        department=dept,
                        department_score=score * DEAL_WEIGHT,
                        matched_keywords=kw,
                        published_at=pub,
                    ))
            except Exception as e:
                self.logger.debug(f"DealTrack {feed_meta['url']}: {e}")

        return signals
