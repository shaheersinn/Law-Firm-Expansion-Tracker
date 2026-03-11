"""
BNNTrackScraper
===============
Monitors BNN Bloomberg Canada coverage of legal proceedings, M&A deals,
and regulatory actions. BNN Bloomberg is Canada's primary financial news
network and covers law firm activity extensively in its M&A and regulatory
beat.

Source:
  - BNN Bloomberg RSS  https://www.bnnbloomberg.ca/rss
  - Bloomberg Canada Markets RSS  https://feeds.bloomberg.com/markets/news.rss
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

BNN_FEEDS = [
    {"url": "https://www.bnnbloomberg.ca/rss",                      "name": "BNN Bloomberg"},
    {"url": "https://feeds.bloomberg.com/markets/news.rss",         "name": "Bloomberg Mkts"},
    {"url": "https://financialpost.com/feed",                       "name": "Financial Post"},
]

LEGAL_PHRASES = [
    "law firm", "legal counsel", "counsel", "lawyer", "attorney",
    "legal proceeding", "merger", "acquisition", "deal", "transaction",
    "regulatory", "securities", "litigat", "class action",
]

BNN_WEIGHT = 1.8


class BNNTrackScraper(BaseScraper):
    name = "BNNTrackScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        try:
            import feedparser
        except ImportError:
            return signals

        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        for feed_meta in BNN_FEEDS:
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
                    if not any(p in lower for p in LEGAL_PHRASES):
                        continue
                    if len(title) < 15:
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
                        department_score=score * BNN_WEIGHT,
                        matched_keywords=kw,
                        published_at=pub,
                    ))
            except Exception as e:
                self.logger.debug(f"BNNTrack {feed_meta['url']}: {e}")

        return signals[:12]
