"""
CounselMoveScraper
==================
Tracks corporate counsel departing private practice to join client
organisations, or general counsel returning to private practice at
a tracked firm. These moves reveal client relationships and signal
which practice groups are winning (or losing) major institutional clients.

Sources:
  - Canadian Lawyer In-House RSS
  - Globe & Mail Business RSS
  - ACC Canada (Association of Corporate Counsel)
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

COUNSEL_FEEDS = [
    {"url": "https://www.canadianlawyermag.com/rss/",           "name": "Canadian Lawyer"},
    {"url": "https://www.theglobeandmail.com/business/rss",     "name": "Globe B&M"},
    {"url": "https://www.thelawyersdaily.ca/rss",               "name": "Lawyer's Daily"},
]

COUNSEL_PHRASES = [
    "general counsel", "chief legal officer", "CLO", "legal officer",
    "in-house", "corporate counsel", "joins as counsel", "named counsel",
    "appointed counsel", "legal department head", "VP legal",
    "legal affairs", "legal team",
]

COUNSEL_WEIGHT = 2.0


class CounselMoveScraper(BaseScraper):
    name = "CounselMoveScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        try:
            import feedparser
        except ImportError:
            return signals

        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        for feed_meta in COUNSEL_FEEDS:
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
                    if not any(p.lower() in lower for p in COUNSEL_PHRASES):
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
                        department_score=score * COUNSEL_WEIGHT,
                        matched_keywords=kw,
                        published_at=pub,
                    ))
            except Exception as e:
                self.logger.debug(f"CounselMove {feed_meta['url']}: {e}")

        return signals[:10]
