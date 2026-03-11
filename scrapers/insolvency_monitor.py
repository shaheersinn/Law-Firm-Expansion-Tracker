"""
InsolvencyMonitorScraper
========================
Tracks insolvency, restructuring, and receivership proceedings where
tracked firms appear as counsel. High-volume insolvency dockets signal
major practice group capacity expansions and client acquisition.

Sources:
  - OSB (Office of the Superintendent of Bankruptcy) notices
  - Canada Gazette (insolvency notices section)
  - Cision PR Newswire
  - Canadian Lawyer RSS (covers major restructurings)
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

INSOLVENCY_FEEDS = [
    {"url": "https://gazette.gc.ca/rss/p1-eng.xml",            "name": "Canada Gazette"},
    {"url": "https://www.newswire.ca/rss/",                     "name": "Cision"},
    {"url": "https://www.canadianlawyermag.com/rss/",           "name": "Canadian Lawyer"},
    {"url": "https://www.thelawyersdaily.ca/rss",               "name": "Lawyer's Daily"},
]

INSOLVENCY_PHRASES = [
    "insolvency", "restructuring", "receivership", "CCAA", "BIA",
    "Companies' Creditors Arrangement Act", "Bankruptcy and Insolvency Act",
    "monitor", "interim receiver", "proposal trustee", "trustee in bankruptcy",
    "creditor protection", "debt restructuring", "financial restructuring",
    "court-supervised", "KERP", "DIP financing", "distressed",
    "wind-down", "liquidation", "arrangement",
]

INSOLVENCY_WEIGHT = 3.0


class InsolvencyMonitorScraper(BaseScraper):
    name = "InsolvencyMonitorScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        try:
            import feedparser
        except ImportError:
            return signals

        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        for feed_meta in INSOLVENCY_FEEDS:
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
                    if not any(p.lower() in lower for p in INSOLVENCY_PHRASES):
                        continue

                    dept, score, kw = _clf.top_department(full)
                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="court_record",
                        title=f"[{feed_meta['name']}] {title[:160]}",
                        body=summary[:600],
                        url=link,
                        department="Restructuring & Insolvency",
                        department_score=max(score, 2.0) * INSOLVENCY_WEIGHT,
                        matched_keywords=kw,
                        published_at=pub,
                    ))
            except Exception as e:
                self.logger.debug(f"InsolvencyMonitor {feed_meta['url']}: {e}")

        return signals[:12]
