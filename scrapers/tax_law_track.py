"""
TaxLawTrackScraper
==================
Monitors tax law developments, CRA audit proceedings, tax court
decisions, and international tax planning announcements where tracked
firms appear. Tax is a core profit centre for major Canadian firms.

Sources:
  - Canada Revenue Agency newsroom RSS
  - Tax Court of Canada decisions (via CanLII RSS)
  - Tax Notes Canada (Mondaq)
  - Canadian Lawyer RSS
  - Lexology Canada RSS
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

TAX_FEEDS = [
    {"url": "https://www.canada.ca/en/revenue-agency/news/newsroom.rss",
                                                    "name": "CRA Newsroom"},
    {"url": "https://www.canadianlawyermag.com/rss/",   "name": "Canadian Lawyer"},
    {"url": "https://www.lexology.com/rss/feed/canada.xml", "name": "Lexology CA"},
    {"url": "https://www.mondaq.com/rss/canada/rss",    "name": "Mondaq CA"},
]

TAX_PHRASES = [
    "tax counsel", "tax lawyer", "tax dispute", "tax appeal",
    "CRA audit", "transfer pricing", "GAAR", "general anti-avoidance",
    "income tax", "GST", "HST", "provincial tax", "tax planning",
    "estate planning", "trust law", "tax litigation", "tax court",
    "tax avoidance", "FHIR", "international tax", "OECD BEPS",
    "Pillar Two", "global minimum tax", "tax treaty",
]

TAX_WEIGHT = 2.5


class TaxLawTrackScraper(BaseScraper):
    name = "TaxLawTrackScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        try:
            import feedparser
        except ImportError:
            return signals

        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        for feed_meta in TAX_FEEDS:
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
                    if not any(p.lower() in lower for p in TAX_PHRASES):
                        continue

                    dept, score, kw = _clf.top_department(full)
                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="thought_leadership",
                        title=f"[{feed_meta['name']}] {title[:160]}",
                        body=summary[:600],
                        url=link,
                        department="Tax",
                        department_score=max(score, 1.5) * TAX_WEIGHT,
                        matched_keywords=kw,
                        published_at=pub,
                    ))
            except Exception as e:
                self.logger.debug(f"TaxLaw {feed_meta['url']}: {e}")

        return signals[:10]
