"""
MergerNewsScraper
=================
Tracks law firm combination, merger, and strategic-alliance announcements.
When major Canadian firms announce mergers or formal combinations this is
the strongest possible expansion signal.

Sources:
  - The Lawyer's Daily  (RSS)
  - Canadian Lawyer     (RSS)
  - Law Times           (RSS)
  - Cision newswire     (RSS)
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

MERGER_FEEDS = [
    {"url": "https://www.thelawyersdaily.ca/rss",           "name": "Lawyer's Daily"},
    {"url": "https://www.canadianlawyermag.com/rss/",       "name": "Canadian Lawyer"},
    {"url": "https://www.lawtimesnews.com/rss",             "name": "Law Times"},
    {"url": "https://www.newswire.ca/rss/",                 "name": "Cision"},
]

MERGER_PHRASES = [
    "merger", "merges with", "combination", "combines with",
    "strategic alliance", "joins forces", "affiliate", "combines practices",
    "law firm merger", "law firm combination", "verein", "swiss verein",
]

MERGER_WEIGHT = 4.0   # highest-conviction expansion signal


class MergerNewsScraper(BaseScraper):
    name = "MergerNewsScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        try:
            import feedparser
        except ImportError:
            return signals

        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        for feed_meta in MERGER_FEEDS:
            try:
                feed = feedparser.parse(feed_meta["url"])
                for entry in (feed.entries or [])[:30]:
                    title   = entry.get("title", "")
                    summary = entry.get("summary", entry.get("description", ""))
                    link    = entry.get("link", feed_meta["url"])
                    pub     = entry.get("published", "")
                    full    = f"{title} {summary}"
                    lower   = full.lower()

                    if not any(n.lower() in lower for n in firm_names):
                        continue
                    if not any(p in lower for p in MERGER_PHRASES):
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
                        department_score=score * MERGER_WEIGHT,
                        matched_keywords=kw,
                        published_at=pub,
                    ))
            except Exception as e:
                self.logger.debug(f"MergerNews {feed_meta['url']}: {e}")

        return signals
