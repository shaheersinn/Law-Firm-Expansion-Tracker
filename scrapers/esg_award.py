"""
ESGAwardScraper
===============
Monitors Environmental, Social, and Governance (ESG) award announcements,
rankings, and recognitions for tracked firms. ESG practice is one of the
fastest-growing areas in Canadian law.

Sources:
  - Canadian Lawyer Magazine RSS
  - Globe & Mail ESG/sustainability section
  - Corporate Knights (https://www.corporateknights.com/feed)
  - Financial Post ESG
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

ESG_FEEDS = [
    {"url": "https://www.canadianlawyermag.com/rss/",           "name": "Canadian Lawyer"},
    {"url": "https://www.theglobeandmail.com/business/rss",     "name": "Globe B&M"},
    {"url": "https://www.corporateknights.com/feed",            "name": "Corporate Knights"},
    {"url": "https://www.slaw.ca/feed/",                        "name": "Slaw"},
]

ESG_PHRASES = [
    "ESG", "environmental social governance", "sustainability",
    "climate law", "green bond", "sustainability-linked",
    "net zero", "carbon neutral", "indigenous rights", "reconciliation",
    "diversity equity inclusion", "DEI", "gender pay equity",
    "human rights due diligence", "responsible investment",
    "impact investment", "green finance", "transition finance",
    "climate change law", "environmental law", "natural capital",
]

ESG_WEIGHT = 2.0


class ESGAwardScraper(BaseScraper):
    name = "ESGAwardScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        try:
            import feedparser
        except ImportError:
            return signals

        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        for feed_meta in ESG_FEEDS:
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
                    if not any(p.lower() in lower for p in ESG_PHRASES):
                        continue

                    dept, score, kw = _clf.top_department(full)
                    sig_type = "award_signal" if any(
                        w in lower for w in ["award", "recognized", "ranked", "named"]
                    ) else "thought_leadership"

                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type=sig_type,
                        title=f"[{feed_meta['name']}] {title[:160]}",
                        body=summary[:600],
                        url=link,
                        department=dept or "ESG & Regulatory",
                        department_score=score * ESG_WEIGHT,
                        matched_keywords=kw,
                        published_at=pub,
                    ))
            except Exception as e:
                self.logger.debug(f"ESGAward {feed_meta['url']}: {e}")

        return signals[:10]
