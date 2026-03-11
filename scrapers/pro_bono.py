"""
ProBonoScraper
==============
Tracks pro bono and access-to-justice initiative announcements.
Firms investing in structured pro bono practices often pair those
announcements with practice-group growth in relevant departments.

Sources:
  - Firm news pages
  - Canadian Lawyer RSS
  - Pro Bono Law Ontario announcements
  - Law Help Ontario
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

PRO_BONO_FEEDS = [
    {"url": "https://www.canadianlawyermag.com/rss/",   "name": "Canadian Lawyer"},
    {"url": "https://www.thelawyersdaily.ca/rss",       "name": "Lawyer's Daily"},
    {"url": "https://www.slaw.ca/feed/",                "name": "Slaw"},
]

PRO_BONO_PHRASES = [
    "pro bono", "access to justice", "legal clinic", "legal aid",
    "community legal", "volunteer legal", "public interest law",
    "pro bono program", "pro bono initiative", "free legal services",
    "pro bono counsel",
]

PRO_BONO_WEIGHT = 1.2


class ProBonoScraper(BaseScraper):
    name = "ProBonoScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        try:
            import feedparser
        except ImportError:
            return signals

        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        for feed_meta in PRO_BONO_FEEDS:
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
                    if not any(p in lower for p in PRO_BONO_PHRASES):
                        continue

                    dept, score, kw = _clf.top_department(full)
                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="press_release",
                        title=f"[{feed_meta['name']}] {title[:160]}",
                        body=summary[:600],
                        url=link,
                        department=dept,
                        department_score=score * PRO_BONO_WEIGHT,
                        matched_keywords=kw,
                        published_at=pub,
                    ))
            except Exception as e:
                self.logger.debug(f"ProBono {feed_meta['url']}: {e}")

        return signals[:10]
