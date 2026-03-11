"""
InfrastructureTrackScraper
==========================
Monitors infrastructure, P3 (Public-Private Partnership), and major
project financing activity in Canada. Firms advising on large
infrastructure projects show strong signals for energy, real estate,
and project finance practice groups.

Sources:
  - Infrastructure Ontario newsroom
  - Canadian Infrastructure Magazine RSS
  - Globe & Mail Business RSS
  - Financial Post (infrastructure/energy section)
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

INFRA_FEEDS = [
    {"url": "https://financialpost.com/feed",                   "name": "Financial Post"},
    {"url": "https://www.theglobeandmail.com/business/rss",     "name": "Globe B&M"},
    {"url": "https://www.newswire.ca/rss/",                     "name": "Cision"},
    {"url": "https://www.canadianlawyermag.com/rss/",           "name": "Canadian Lawyer"},
]

INFRA_PHRASES = [
    "P3", "public-private partnership", "infrastructure project",
    "project finance", "project financing", "concession agreement",
    "availability payment", "design-build-finance-operate",
    "DBFO", "DBFOM", "construction law", "transit", "transit corridor",
    "hospital", "transit project", "infrastructure Ontario",
    "Metrolinx", "LRT", "transit expansion",
    "renewable energy project", "energy project", "wind farm", "solar farm",
    "hydroelectric", "transmission line",
]

INFRA_WEIGHT = 2.5


class InfrastructureTrackScraper(BaseScraper):
    name = "InfrastructureTrackScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        try:
            import feedparser
        except ImportError:
            return signals

        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        for feed_meta in INFRA_FEEDS:
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
                    if not any(p.lower() in lower for p in INFRA_PHRASES):
                        continue

                    dept, score, kw = _clf.top_department(full)
                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="deal_record",
                        title=f"[{feed_meta['name']}] {title[:160]}",
                        body=summary[:600],
                        url=link,
                        department=dept or "Infrastructure & Projects",
                        department_score=score * INFRA_WEIGHT,
                        matched_keywords=kw,
                        published_at=pub,
                    ))
            except Exception as e:
                self.logger.debug(f"InfrastructureTrack {feed_meta['url']}: {e}")

        return signals[:12]
