"""
PrivateEquityTrackScraper
=========================
Monitors private equity deal flow and fund formation activities where
tracked firms appear as fund counsel, portfolio company counsel, or
management-side advisors.

Sources:
  - PE Hub Canada / Canadian PE news
  - Globe & Mail (PE/venture section)
  - Financial Post (PE deals)
  - Cision newswire (fund closes, LP notices)
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

PE_FEEDS = [
    {"url": "https://financialpost.com/feed",                   "name": "Financial Post"},
    {"url": "https://www.theglobeandmail.com/business/rss",     "name": "Globe B&M"},
    {"url": "https://www.newswire.ca/rss/",                     "name": "Cision"},
    {"url": "https://feeds.bloomberg.com/markets/news.rss",     "name": "Bloomberg Mkts"},
]

PE_PHRASES = [
    "private equity", "PE firm", "venture capital", "VC fund",
    "fund close", "fund formation", "fund counsel", "portfolio company",
    "management buyout", "MBO", "leveraged buyout", "LBO",
    "GP counsel", "LP counsel", "carried interest",
    "acquisition financing", "PE acquisition", "growth equity",
    "private credit", "mezzanine", "minority investment",
]

PE_WEIGHT = 2.5


class PrivateEquityTrackScraper(BaseScraper):
    name = "PrivateEquityTrackScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        try:
            import feedparser
        except ImportError:
            return signals

        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        for feed_meta in PE_FEEDS:
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
                    if not any(p.lower() in lower for p in PE_PHRASES):
                        continue

                    dept, score, kw = _clf.top_department(full)
                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="deal_record",
                        title=f"[{feed_meta['name']}] {title[:160]}",
                        body=summary[:600],
                        url=link,
                        department="Private Equity",
                        department_score=max(score, 2.0) * PE_WEIGHT,
                        matched_keywords=kw,
                        published_at=pub,
                    ))
            except Exception as e:
                self.logger.debug(f"PrivateEquityTrack {feed_meta['url']}: {e}")

        return signals[:12]
