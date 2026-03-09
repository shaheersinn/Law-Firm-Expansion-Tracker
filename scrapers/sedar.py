"""
SedarScraper
Monitors SEDAR+ for recent securities filings where a tracked firm
is named as legal counsel.

SEDAR+ public search: https://www.sedarplus.ca/
We use the public filing search API (no auth required for public docs).
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

SEDAR_WEIGHT = 3.0
SEDAR_SEARCH = "https://www.sedarplus.ca/landingpage/"

FILING_TYPES_OF_INTEREST = [
    "prospectus", "management information circular", "material change",
    "annual information form", "press release",
]


class SedarScraper(BaseScraper):
    name = "SedarScraper"

    def fetch(self, firm: dict) -> list[dict]:
        """
        SEDAR+ does not expose a public RSS/API, so we use Google News
        to surface press releases that mention the firm as counsel on
        a SEDAR filing. Full SEDAR integration requires paid data access.
        """
        signals = []
        try:
            import feedparser
        except ImportError:
            return signals

        from urllib.parse import quote_plus
        q = quote_plus(f'"{firm["short"]}" sedar OR prospectus OR securities counsel site:sedarplus.ca OR site:newswire.ca')
        url = f"https://news.google.com/rss/search?q={q}&hl=en-CA&gl=CA&ceid=CA:en"
        try:
            feed = feedparser.parse(url)
            for entry in (feed.entries or [])[:8]:
                title   = entry.get("title", "")
                summary = entry.get("summary", "")
                link    = entry.get("link", url)
                pub     = entry.get("published", "")
                full    = f"{title} {summary}"
                lower   = full.lower()

                if not any(t in lower for t in FILING_TYPES_OF_INTEREST + ["counsel", "advises"]):
                    continue

                dept, score, kw = _clf.top_department(full)
                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type="court_record",
                    title=f"[SEDAR/Securities] {title[:160]}",
                    body=summary[:400],
                    url=link,
                    department="Capital Markets",
                    department_score=score * SEDAR_WEIGHT,
                    matched_keywords=kw + ["sedar", "securities"],
                    published_at=pub,
                ))
        except Exception as e:
            self.logger.debug(f"SedarScraper: {e}")

        return signals[:4]
