"""
CBASectionScraper
=================
Monitors Canadian Bar Association (CBA) section and committee activity:
leadership appointments, speaking invitations, and committee memberships
from tracked firms signal thought leadership and practice group strength.

Sources:
  - CBA National Magazine RSS  https://www.cbamag.ca/feed
  - CBA News RSS               https://www.cba.org/rss/news
  - CBA Section events page    (static HTML, uses firm keyword search)
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

CBA_FEEDS = [
    {"url": "https://www.cbamag.ca/feed",   "name": "CBA National"},
    {"url": "https://www.cba.org/rss/news", "name": "CBA News"},
    {"url": "https://www.slaw.ca/feed/",    "name": "Slaw"},
]

CBA_PHRASES = [
    "CBA section", "CBA committee", "Canadian Bar Association",
    "section chair", "section executive", "CBA council",
    "bar association leadership", "bar committee chair",
    "section member", "CBA president", "provincial bar",
    "Law Society", "bencher", "elected bencher",
]

CBA_WEIGHT = 2.5


class CBASectionScraper(BaseScraper):
    name = "CBASectionScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        try:
            import feedparser
        except ImportError:
            return signals

        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        for feed_meta in CBA_FEEDS:
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
                    if not any(p.lower() in lower for p in CBA_PHRASES):
                        continue

                    dept, score, kw = _clf.top_department(full)
                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="bar_leadership",
                        title=f"[{feed_meta['name']}] {title[:160]}",
                        body=summary[:600],
                        url=link,
                        department=dept,
                        department_score=score * CBA_WEIGHT,
                        matched_keywords=kw,
                        published_at=pub,
                    ))
            except Exception as e:
                self.logger.debug(f"CBASectionScraper {feed_meta['url']}: {e}")

        return signals[:10]
