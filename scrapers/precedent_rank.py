"""
PrecedentRankScraper
====================
Monitors Precedent Magazine rankings, awards, and the annual Precedent
Setter awards for Canadian law firms. Precedent focuses on Bay Street
law firm culture, diversity, and emerging talent — strong signal for
hiring intentions and brand positioning.

Source:
  - Precedent Magazine RSS  https://www.precedentmagazine.com/feed/
  - Precedent rankings page https://www.precedentmagazine.com/category/rankings/
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

PRECEDENT_FEEDS = [
    {"url": "https://www.precedentmagazine.com/feed/",          "name": "Precedent"},
    {"url": "https://www.canadianlawyermag.com/rss/",           "name": "Canadian Lawyer"},
]

RANK_PHRASES = [
    "ranked", "ranking", "top firm", "best firm", "award", "recognition",
    "precedent setter", "top 30", "top 10", "canada's top", "top-ranked",
    "practice area", "practice group", "rising star", "most innovative",
]

PRECEDENT_WEIGHT = 2.0


class PrecedentRankScraper(BaseScraper):
    name = "PrecedentRankScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        try:
            import feedparser
        except ImportError:
            return signals

        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        for feed_meta in PRECEDENT_FEEDS:
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
                    if not any(p in lower for p in RANK_PHRASES):
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
                        department_score=score * PRECEDENT_WEIGHT,
                        matched_keywords=kw,
                        published_at=pub,
                    ))
            except Exception as e:
                self.logger.debug(f"PrecedentRank {feed_meta['url']}: {e}")

        return signals[:10]
