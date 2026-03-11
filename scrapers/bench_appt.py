"""
BenchApptScraper
================
Monitors judicial appointment announcements. When senior partners or
counsel from a tracked firm are appointed to the bench it is simultaneously
a lateral departure signal AND indicates the firm has produced top-tier
counsel — often accompanied by firm press releases.

Sources:
  - Office of the Commissioner for Federal Judicial Affairs
    (https://www.fja.gc.ca/appointments-nominations/judges-juges/index-eng.html)
  - Canada Gazette RSS (https://gazette.gc.ca/rss/p1-eng.xml)
  - Canadian Lawyer RSS
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

BENCH_FEEDS = [
    {"url": "https://gazette.gc.ca/rss/p1-eng.xml",        "name": "Canada Gazette"},
    {"url": "https://www.canadianlawyermag.com/rss/",       "name": "Canadian Lawyer"},
    {"url": "https://www.thelawyersdaily.ca/rss",           "name": "Lawyer's Daily"},
]

BENCH_PHRASES = [
    "appointed to the bench", "appointed judge", "named judge",
    "judicial appointment", "appointed to the court",
    "Queen's Bench", "King's Bench", "Superior Court",
    "Court of Appeal", "Federal Court", "appointed to",
    "sworn in as judge", "appointed justice",
    "appointed to the Federal Court", "appointed to the Ontario Superior Court",
]

BENCH_WEIGHT = 3.0


class BenchApptScraper(BaseScraper):
    name = "BenchApptScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        try:
            import feedparser
        except ImportError:
            return signals

        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        for feed_meta in BENCH_FEEDS:
            try:
                feed = feedparser.parse(feed_meta["url"])
                for entry in (feed.entries or [])[:30]:
                    title   = entry.get("title", "")
                    summary = entry.get("summary", entry.get("description", ""))
                    link    = entry.get("link", feed_meta["url"])
                    pub     = entry.get("published", "")
                    full    = f"{title} {summary}"
                    lower   = full.lower()

                    if not any(n.lower() in lower for n in firm_names):
                        continue
                    if not any(p.lower() in lower for p in BENCH_PHRASES):
                        continue

                    dept, score, kw = _clf.top_department(full)
                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="lateral_hire",
                        title=f"[{feed_meta['name']}] {title[:160]}",
                        body=summary[:600],
                        url=link,
                        department=dept or "Litigation & Disputes",
                        department_score=score * BENCH_WEIGHT,
                        matched_keywords=kw,
                        published_at=pub,
                    ))
            except Exception as e:
                self.logger.debug(f"BenchAppt {feed_meta['url']}: {e}")

        return signals
