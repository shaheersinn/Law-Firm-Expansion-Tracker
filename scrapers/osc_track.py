"""
OSCTrackScraper
===============
Monitors Ontario Securities Commission (OSC) enforcement actions,
policy notices, and proceedings. Firms acting as securities counsel
generate high-value capital markets / financial-services signals.

Sources:
  - OSC Newsroom RSS  (https://www.osc.ca/en/rss/newsroom)
  - OSC Decisions RSS (https://www.osc.ca/en/rss/decisions)
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

OSC_FEEDS = [
    {"url": "https://www.osc.ca/en/rss/newsroom",   "name": "OSC Newsroom"},
    {"url": "https://www.osc.ca/en/rss/decisions",  "name": "OSC Decisions"},
    {"url": "https://www.bcsc.bc.ca/enforcement/decisions/rss", "name": "BCSC Decisions"},
]

COUNSEL_PHRASES = [
    "counsel", "represented by", "legal counsel", "securities counsel",
    "advised by", "represented", "acted for",
]

OSC_WEIGHT = 2.5


class OSCTrackScraper(BaseScraper):
    name = "OSCTrackScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        try:
            import feedparser
        except ImportError:
            return signals

        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        for feed_meta in OSC_FEEDS:
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

                    dept, score, kw = _clf.top_department(full)
                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="court_record",
                        title=f"[{feed_meta['name']}] {title[:160]}",
                        body=summary[:600],
                        url=link,
                        department=dept or "Capital Markets",
                        department_score=score * OSC_WEIGHT,
                        matched_keywords=kw,
                        published_at=pub,
                    ))
            except Exception as e:
                self.logger.debug(f"OSCTrack {feed_meta['url']}: {e}")

        return signals
