"""
InhouseMoveScraper
==================
Tracks in-house corporate counsel hiring moves: when general counsel
or corporate legal directors join or leave a tracked firm as clients,
it signals practice expansion or major client acquisition.

Sources:
  - Canadian Lawyer In-House RSS
  - ACC Canada news (https://www.acc.com/canada)
  - Globe & Mail B&M RSS
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

INHOUSE_FEEDS = [
    {"url": "https://www.canadianlawyermag.com/rss/",           "name": "CL In-House"},
    {"url": "https://www.theglobeandmail.com/business/rss",     "name": "Globe B&M"},
    {"url": "https://financialpost.com/feed",                   "name": "Financial Post"},
]

INHOUSE_PHRASES = [
    "general counsel", "chief legal officer", "CLO", "GC",
    "in-house counsel", "corporate counsel", "legal department",
    "head of legal", "VP legal", "vice president legal",
    "legal director", "associate general counsel",
    "hires", "appoints", "names", "promoted to general counsel",
]

INHOUSE_WEIGHT = 2.0


class InhouseMoveScraper(BaseScraper):
    name = "InhouseMoveScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        try:
            import feedparser
        except ImportError:
            return signals

        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        for feed_meta in INHOUSE_FEEDS:
            try:
                feed = feedparser.parse(feed_meta["url"])
                for entry in (feed.entries or [])[:20]:
                    title   = entry.get("title", "")
                    summary = entry.get("summary", entry.get("description", ""))
                    link    = entry.get("link", feed_meta["url"])
                    pub     = entry.get("published", "")
                    full    = f"{title} {summary}"
                    lower   = full.lower()

                    # Look for in-house lawyers coming FROM or going TO the firm
                    if not any(n.lower() in lower for n in firm_names):
                        continue
                    if not any(p.lower() in lower for p in INHOUSE_PHRASES):
                        continue

                    dept, score, kw = _clf.top_department(full)
                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="lateral_hire",
                        title=f"[{feed_meta['name']}] {title[:160]}",
                        body=summary[:600],
                        url=link,
                        department=dept,
                        department_score=score * INHOUSE_WEIGHT,
                        matched_keywords=kw,
                        published_at=pub,
                    ))
            except Exception as e:
                self.logger.debug(f"InhouseMove {feed_meta['url']}: {e}")

        return signals
