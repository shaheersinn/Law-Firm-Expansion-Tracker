"""
HealthcareLawTrackScraper
=========================
Monitors healthcare, life sciences, and pharmaceutical legal activity
in Canada. Firms expanding in healthcare law typically represent large
hospital networks, pharma companies, or medical device manufacturers.

Sources:
  - Canadian Healthcare Network RSS
  - Financial Post (pharma/health section)
  - Globe & Mail RSS
  - Health Law Canada (Mondaq)
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

HEALTH_FEEDS = [
    {"url": "https://financialpost.com/feed",                   "name": "Financial Post"},
    {"url": "https://www.theglobeandmail.com/business/rss",     "name": "Globe B&M"},
    {"url": "https://www.canadianlawyermag.com/rss/",           "name": "Canadian Lawyer"},
    {"url": "https://www.mondaq.com/rss/canada/rss",            "name": "Mondaq CA"},
]

HEALTH_PHRASES = [
    "healthcare law", "health law", "pharmaceutical", "medical device",
    "FDA", "Health Canada", "clinical trial", "pharmaceutical counsel",
    "drug approval", "medical negligence", "malpractice",
    "hospital", "health system", "life sciences", "biotech",
    "regulatory approval", "drug pricing", "pharmaceutical merger",
    "pharmaceutical acquisition", "health data", "patient privacy",
    "PIPEDA health", "personal health information",
]

HEALTH_WEIGHT = 2.0


class HealthcareLawTrackScraper(BaseScraper):
    name = "HealthcareLawTrackScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        try:
            import feedparser
        except ImportError:
            return signals

        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        for feed_meta in HEALTH_FEEDS:
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
                    if not any(p.lower() in lower for p in HEALTH_PHRASES):
                        continue

                    dept, score, kw = _clf.top_department(full)
                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="thought_leadership",
                        title=f"[{feed_meta['name']}] {title[:160]}",
                        body=summary[:600],
                        url=link,
                        department="Healthcare & Life Sciences",
                        department_score=max(score, 1.0) * HEALTH_WEIGHT,
                        matched_keywords=kw,
                        published_at=pub,
                    ))
            except Exception as e:
                self.logger.debug(f"HealthcareLaw {feed_meta['url']}: {e}")

        return signals[:10]
