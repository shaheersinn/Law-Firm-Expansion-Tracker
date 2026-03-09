"""
MediaScraper
Broad Canadian legal and financial media monitoring.
Catches merger announcements, firm combination news, leadership changes,
and major practice area coverage.

Sources: Google News RSS, Precedent Magazine, Slaw.ca, CBC Business
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier
from urllib.parse import quote_plus

_clf = DepartmentClassifier()

MEDIA_WEIGHT = 1.5

RELEVANT_PHRASES = [
    "law firm", "legal", "counsel", "practice", "partner",
    "associate", "merger", "combination", "expansion",
]


class MediaScraper(BaseScraper):
    name = "MediaScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        try:
            import feedparser
        except ImportError:
            return signals

        # Google News RSS for firm name
        query = quote_plus(f'"{firm["short"]}" law firm')
        gnews_url = f"https://news.google.com/rss/search?q={query}&hl=en-CA&gl=CA&ceid=CA:en"

        static_feeds = [
            {"url": "https://www.slaw.ca/feed/", "name": "Slaw"},
            {"url": "https://www.precedentmagazine.com/feed/", "name": "Precedent"},
            {"url": "https://www.cbc.ca/cmlink/rss-business", "name": "CBC Business"},
        ]

        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        all_feeds = [{"url": gnews_url, "name": "Google News"}] + static_feeds

        for feed_meta in all_feeds:
            try:
                feed = feedparser.parse(feed_meta["url"])
                for entry in (feed.entries or [])[:20]:
                    title   = entry.get("title", "")
                    summary = entry.get("summary", entry.get("description", ""))
                    link    = entry.get("link", feed_meta["url"])
                    pub     = entry.get("published", "")
                    full    = f"{title} {summary}"
                    lower   = full.lower()

                    # Google News feed already filtered; static feeds need name check
                    if feed_meta["name"] != "Google News":
                        if not any(n.lower() in lower for n in firm_names):
                            continue
                    if not any(p in lower for p in RELEVANT_PHRASES):
                        continue
                    if len(title) < 15:
                        continue

                    dept, score, kw = _clf.top_department(full)
                    sig_type = "lateral_hire" if any(
                        p in lower for p in ["joins", "joined", "welcomes", "lateral"]
                    ) else "press_release"

                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type=sig_type,
                        title=f"[{feed_meta['name']}] {title[:160]}",
                        body=summary[:600],
                        url=link,
                        department=dept,
                        department_score=score * MEDIA_WEIGHT,
                        matched_keywords=kw,
                        published_at=pub,
                    ))
            except Exception as e:
                self.logger.debug(f"Media {feed_meta['url']}: {e}")

        return signals[:15]  # cap per firm to avoid noise
