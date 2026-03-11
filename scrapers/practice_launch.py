"""
PracticeLaunchScraper
=====================
Detects new practice group or service area launches announced by firms.
When a major firm launches a new practice it is the clearest possible
expansion signal for that department.

Sources:
  - Firm news pages (direct scrape for "new practice", "launches" language)
  - Canadian Lawyer Magazine RSS
  - The Lawyer's Daily RSS
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

LAUNCH_FEEDS = [
    {"url": "https://www.canadianlawyermag.com/rss/",   "name": "Canadian Lawyer"},
    {"url": "https://www.thelawyersdaily.ca/rss",       "name": "Lawyer's Daily"},
    {"url": "https://www.lawtimesnews.com/rss",         "name": "Law Times"},
]

LAUNCH_PHRASES = [
    "new practice group", "launches practice", "new service offering",
    "launches new", "expands practice", "new service line",
    "establishes practice", "new group", "new team",
    "introduces new", "unveils", "launches",
    "new specialty", "adds practice", "creates practice",
]

LAUNCH_WEIGHT = 3.0


class PracticeLaunchScraper(BaseScraper):
    name = "PracticeLaunchScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        try:
            import feedparser
        except ImportError:
            return signals

        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        for feed_meta in LAUNCH_FEEDS:
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
                    if not any(p in lower for p in LAUNCH_PHRASES):
                        continue

                    dept, score, kw = _clf.top_department(full)
                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="practice_page",
                        title=f"[{feed_meta['name']}] {title[:160]}",
                        body=summary[:600],
                        url=link,
                        department=dept,
                        department_score=score * LAUNCH_WEIGHT,
                        matched_keywords=kw,
                        published_at=pub,
                    ))
            except Exception as e:
                self.logger.debug(f"PracticeLaunch {feed_meta['url']}: {e}")

        # Also check firm's news page for launch keywords
        news_url = firm.get("news_url", "")
        if news_url:
            soup = self._soup(news_url)
            if soup:
                for a in (soup.find_all("a", href=True) or [])[:40]:
                    text = self._clean(a.get_text())
                    if not text or len(text) < 10:
                        continue
                    lower = text.lower()
                    if any(p in lower for p in LAUNCH_PHRASES):
                        href = a["href"]
                        if not href.startswith("http"):
                            from urllib.parse import urljoin
                            href = urljoin(news_url, href)
                        dept, score, kw = _clf.top_department(text)
                        signals.append(self._make_signal(
                            firm_id=firm["id"],
                            firm_name=firm["name"],
                            signal_type="practice_page",
                            title=f"[{firm['short']}] {text[:160]}",
                            body=text,
                            url=href,
                            department=dept,
                            department_score=score * LAUNCH_WEIGHT,
                            matched_keywords=kw,
                        ))

        return signals[:15]
