"""
LegalTechScraper
================
Tracks legal technology adoption, AI-tool implementations, and legal
innovation announcements. Firms investing in tech are expanding capacity
and signalling ambition in data privacy, IP, and financial services.

Sources:
  - Slaw.ca RSS  (covers Canadian legal tech extensively)
  - Canadian Lawyer Magazine RSS
  - Legal IT Insider RSS
  - Legal Innovation Zone (Ryerson) announcements
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

LEGALTECH_FEEDS = [
    {"url": "https://www.slaw.ca/feed/",                    "name": "Slaw"},
    {"url": "https://www.canadianlawyermag.com/rss/",       "name": "Canadian Lawyer"},
    {"url": "https://www.thelawyersdaily.ca/rss",           "name": "Lawyer's Daily"},
    {"url": "https://www.legalit.media/feed",               "name": "Legal IT Insider"},
]

LEGALTECH_PHRASES = [
    "artificial intelligence", "machine learning", "legal tech", "legaltech",
    "automation", "contract review", "document review", "AI tool",
    "legal innovation", "digital transformation", "e-discovery",
    "contract analytics", "legal AI", "natural language processing",
    "legal management system", "matter management", "practice management",
    "cyber security", "data breach", "data privacy technology",
    "blockchain", "smart contract", "legal operations",
]

LEGALTECH_WEIGHT = 1.8


class LegalTechScraper(BaseScraper):
    name = "LegalTechScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        try:
            import feedparser
        except ImportError:
            return signals

        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        for feed_meta in LEGALTECH_FEEDS:
            try:
                feed = feedparser.parse(feed_meta["url"])
                for entry in (feed.entries or [])[:25]:
                    title   = entry.get("title", "")
                    summary = entry.get("summary", entry.get("description", ""))
                    link    = entry.get("link", feed_meta["url"])
                    pub     = entry.get("published", "")
                    full    = f"{title} {summary}"
                    lower   = full.lower()

                    if not any(n.lower() in lower for n in firm_names):
                        continue
                    if not any(p.lower() in lower for p in LEGALTECH_PHRASES):
                        continue

                    dept, score, kw = _clf.top_department(full)
                    # Legal tech skews toward Data Privacy & Cybersecurity
                    if not dept or dept == "Corporate/M&A":
                        dept = "Data Privacy & Cybersecurity"
                        score = max(score, 1.5)

                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="thought_leadership",
                        title=f"[{feed_meta['name']}] {title[:160]}",
                        body=summary[:600],
                        url=link,
                        department=dept,
                        department_score=score * LEGALTECH_WEIGHT,
                        matched_keywords=kw,
                        published_at=pub,
                    ))
            except Exception as e:
                self.logger.debug(f"LegalTech {feed_meta['url']}: {e}")

        return signals[:12]
