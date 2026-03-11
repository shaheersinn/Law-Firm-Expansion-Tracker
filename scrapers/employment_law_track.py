"""
EmploymentLawTrackScraper
=========================
Monitors employment and labour law signals: collective bargaining,
labour arbitrations, wrongful dismissal class actions, and OHSA proceedings.
Employment practice growth often precedes broader firm expansion.

Sources:
  - Labour Arbitration Online (LAO) database highlights
  - CBC Labour / Business RSS
  - Canadian Lawyer RSS
  - The Lawyer's Daily RSS
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

EMPLOYMENT_FEEDS = [
    {"url": "https://www.cbc.ca/cmlink/rss-business",           "name": "CBC Business"},
    {"url": "https://www.canadianlawyermag.com/rss/",           "name": "Canadian Lawyer"},
    {"url": "https://www.thelawyersdaily.ca/rss",               "name": "Lawyer's Daily"},
    {"url": "https://www.slaw.ca/feed/",                        "name": "Slaw"},
]

EMPLOYMENT_PHRASES = [
    "employment law", "labour law", "wrongful dismissal", "constructive dismissal",
    "collective agreement", "collective bargaining", "labour relations",
    "human rights tribunal", "OHSA", "Occupational Health and Safety",
    "employment standards", "ESA", "OHRC", "HRTO",
    "strike", "lockout", "union", "grievance arbitration",
    "non-compete", "non-solicitation", "severance",
    "workplace harassment", "sexual harassment", "whistleblower",
    "pay equity", "employment class action",
]

EMPLOYMENT_WEIGHT = 2.0


class EmploymentLawTrackScraper(BaseScraper):
    name = "EmploymentLawTrackScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        try:
            import feedparser
        except ImportError:
            return signals

        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        for feed_meta in EMPLOYMENT_FEEDS:
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
                    if not any(p.lower() in lower for p in EMPLOYMENT_PHRASES):
                        continue

                    dept, score, kw = _clf.top_department(full)
                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="thought_leadership",
                        title=f"[{feed_meta['name']}] {title[:160]}",
                        body=summary[:600],
                        url=link,
                        department="Employment & Labour",
                        department_score=max(score, 1.0) * EMPLOYMENT_WEIGHT,
                        matched_keywords=kw,
                        published_at=pub,
                    ))
            except Exception as e:
                self.logger.debug(f"EmploymentLaw {feed_meta['url']}: {e}")

        return signals[:10]
