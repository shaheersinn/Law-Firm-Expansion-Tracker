"""
ImmigrationTrackScraper
=======================
Monitors Canadian immigration law developments and firm-specific
immigration practice activity. With Canada's high immigration targets
through 2027, immigration is a fast-growing practice area.

Sources:
  - CIC News RSS  https://www.cicnews.com/feed
  - Canadian Immigration Report (cbcnews immigration tag)
  - Globe & Mail RSS (immigration stories)
  - Canada Gazette (immigration regulations)
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

IMMIGRATION_FEEDS = [
    {"url": "https://www.cicnews.com/feed",                     "name": "CIC News"},
    {"url": "https://www.cbc.ca/cmlink/rss-canada",             "name": "CBC Canada"},
    {"url": "https://www.canadianlawyermag.com/rss/",           "name": "Canadian Lawyer"},
    {"url": "https://gazette.gc.ca/rss/p1-eng.xml",            "name": "Canada Gazette"},
]

IMMIGRATION_PHRASES = [
    "immigration", "immigration law", "immigration lawyer",
    "work permit", "LMIA", "Labour Market Impact Assessment",
    "permanent residency", "express entry", "provincial nominee",
    "refugee law", "asylum", "IRCC", "Immigration Canada",
    "corporate immigration", "business immigration", "investor visa",
    "skilled worker", "immigration policy", "immigration reform",
    "IRPA", "immigration consultant",
]

IMMIGRATION_WEIGHT = 2.0


class ImmigrationTrackScraper(BaseScraper):
    name = "ImmigrationTrackScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        try:
            import feedparser
        except ImportError:
            return signals

        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        for feed_meta in IMMIGRATION_FEEDS:
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
                    if not any(p.lower() in lower for p in IMMIGRATION_PHRASES):
                        continue

                    dept, score, kw = _clf.top_department(full)
                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="thought_leadership",
                        title=f"[{feed_meta['name']}] {title[:160]}",
                        body=summary[:600],
                        url=link,
                        department="Immigration",
                        department_score=max(score, 1.0) * IMMIGRATION_WEIGHT,
                        matched_keywords=kw,
                        published_at=pub,
                    ))
            except Exception as e:
                self.logger.debug(f"ImmigrationTrack {feed_meta['url']}: {e}")

        return signals[:10]
