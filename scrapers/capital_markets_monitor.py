"""
CapitalMarketsMonitor
=====================
Dedicated capital-markets intelligence scraper. Monitors ECM (equity
capital markets), DCM (debt capital markets), and structured finance
deal flow. Firms with active deal counsel mandates receive the highest
capital markets expansion scores.

Sources:
  - SEDAR+ prospectus filing RSS  (https://www.sedarplus.ca/rss)
  - Financial Post Markets RSS
  - Globe B&M RSS
  - Cision deal newswire
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

CM_FEEDS = [
    {"url": "https://financialpost.com/feed",                   "name": "Financial Post"},
    {"url": "https://www.theglobeandmail.com/business/rss",     "name": "Globe B&M"},
    {"url": "https://www.newswire.ca/rss/",                     "name": "Cision"},
    {"url": "https://feeds.bloomberg.com/markets/news.rss",     "name": "Bloomberg Mkts"},
]

CM_PHRASES = [
    "IPO", "initial public offering", "prospectus", "bought deal",
    "offering", "equity offering", "debt offering", "bond issue",
    "syndicate", "underwriter", "underwriting", "capital markets",
    "ECM", "DCM", "private placement", "rights offering",
    "structured finance", "securitization", "ABS", "MBS",
    "convertible debenture", "credit facility",
]

CM_WEIGHT = 2.5


class CapitalMarketsMonitor(BaseScraper):
    name = "CapitalMarketsMonitor"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        try:
            import feedparser
        except ImportError:
            return signals

        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        for feed_meta in CM_FEEDS:
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
                    if not any(p.lower() in lower for p in CM_PHRASES):
                        continue

                    dept, score, kw = _clf.top_department(full)
                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="deal_record",
                        title=f"[{feed_meta['name']}] {title[:160]}",
                        body=summary[:600],
                        url=link,
                        department="Capital Markets",
                        department_score=max(score, 2.0) * CM_WEIGHT,
                        matched_keywords=kw,
                        published_at=pub,
                    ))
            except Exception as e:
                self.logger.debug(f"CapitalMarketsMonitor {feed_meta['url']}: {e}")

        return signals[:12]
