"""
PodcastScraper
Monitors Canadian legal podcast RSS feeds for firm mentions.
Podcast appearances signal thought leadership investment.

Sources:
  - Law Bytes (Michael Geist)
  - The Lawyer's Daily Podcast
  - Canadian Bar Review Podcast
  - Blakes Talk (Blakes firm podcast)
  - Various firm podcast feeds
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

PODCAST_WEIGHT = 1.5

PODCAST_FEEDS = [
    {"name": "Law Bytes",           "url": "https://feeds.buzzsprout.com/1004406.rss"},
    {"name": "Lawyers Daily Pod",   "url": "https://www.thelawyersdaily.ca/rss"},
    {"name": "Slaw Podcast",        "url": "https://www.slaw.ca/feed/"},
]


class PodcastScraper(BaseScraper):
    name = "PodcastScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        try:
            import feedparser
        except ImportError:
            return signals

        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        for src in PODCAST_FEEDS:
            try:
                feed = feedparser.parse(src["url"])
                for entry in (feed.entries or [])[:20]:
                    title   = entry.get("title", "")
                    summary = entry.get("summary", entry.get("description", ""))
                    link    = entry.get("link", src["url"])
                    pub     = entry.get("published", "")
                    full    = f"{title} {summary}"
                    lower   = full.lower()

                    if not any(n.lower() in lower for n in firm_names):
                        continue

                    dept, score, kw = _clf.top_department(full)
                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="thought_leadership",
                        title=f"[{src['name']}] {title[:160]}",
                        body=summary[:400],
                        url=link,
                        department=dept,
                        department_score=score * PODCAST_WEIGHT,
                        matched_keywords=kw,
                        published_at=pub,
                    ))
            except Exception as e:
                self.logger.debug(f"Podcast {src['name']}: {e}")

        return signals[:4]
