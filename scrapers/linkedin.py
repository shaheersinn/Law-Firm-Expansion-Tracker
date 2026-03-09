"""
LinkedInScraper
Uses Google News and Google cache to surface LinkedIn signals:
  - Lawyer "joins firm" notifications
  - Partner profile updates
  - Firm company page posts

Direct LinkedIn scraping is blocked; we use:
  1. Google News RSS with LinkedIn site filter
  2. Google search snippets via News RSS
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier
from urllib.parse import quote_plus

_clf = DepartmentClassifier()

LINKEDIN_WEIGHT = 2.5

LATERAL_PHRASES = [
    "joins", "joined", "has joined", "new role", "excited to announce",
    "pleased to welcome", "starting new position", "new partner",
]


class LinkedInScraper(BaseScraper):
    name = "LinkedInScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        try:
            import feedparser
        except ImportError:
            return signals

        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        # Google News for LinkedIn mentions of the firm
        q = quote_plus(f'"{firm["short"]}" joins OR "new partner" site:linkedin.com OR "law firm" Canada')
        url = f"https://news.google.com/rss/search?q={q}&hl=en-CA&gl=CA&ceid=CA:en"

        try:
            feed = feedparser.parse(url)
            for entry in (feed.entries or [])[:15]:
                title   = entry.get("title", "")
                summary = entry.get("summary", "")
                link    = entry.get("link", url)
                pub     = entry.get("published", "")
                full    = f"{title} {summary}"
                lower   = full.lower()

                # Must mention firm name
                if not any(n.lower() in lower for n in firm_names):
                    continue
                if not any(p in lower for p in LATERAL_PHRASES):
                    continue

                dept, score, kw = _clf.top_department(full)
                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type="lateral_hire",
                    title=f"[LinkedIn/News] {title[:160]}",
                    body=summary[:400],
                    url=link,
                    department=dept,
                    department_score=score * LINKEDIN_WEIGHT,
                    matched_keywords=kw,
                    published_at=pub,
                ))
        except Exception as e:
            self.logger.debug(f"LinkedIn scraper: {e}")

        return signals[:6]
