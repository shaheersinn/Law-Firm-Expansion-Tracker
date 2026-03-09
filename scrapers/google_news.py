"""
GoogleNewsScraper
Broad Google News RSS monitoring per firm.
Uses multiple query variations to surface expansion signals
that other scrapers might miss.
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier
from urllib.parse import quote_plus

_clf = DepartmentClassifier()

NEWS_WEIGHT = 1.6

EXPANSION_SIGNALS = [
    "new partner", "lateral hire", "joins", "expands", "new office",
    "new practice", "advises", "counsel", "merger", "combination",
    "ranks", "ranked", "best law", "chambers", "award",
]


class GoogleNewsScraper(BaseScraper):
    name = "GoogleNewsScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        try:
            import feedparser
        except ImportError:
            return signals

        queries = [
            f'"{firm["short"]}" law firm Canada',
            f'"{firm["name"].split()[0]}" legal Canada',
        ]

        seen_urls: set[str] = set()

        for q in queries:
            url = f"https://news.google.com/rss/search?q={quote_plus(q)}&hl=en-CA&gl=CA&ceid=CA:en"
            try:
                feed = feedparser.parse(url)
                for entry in (feed.entries or [])[:15]:
                    title   = entry.get("title", "")
                    summary = entry.get("summary", "")
                    link    = entry.get("link", url)
                    pub     = entry.get("published", "")

                    if link in seen_urls:
                        continue
                    seen_urls.add(link)

                    full  = f"{title} {summary}"
                    lower = full.lower()

                    if not any(k in lower for k in EXPANSION_SIGNALS):
                        continue

                    dept, score, kw = _clf.top_department(full)
                    sig_type = "lateral_hire" if any(
                        p in lower for p in ["joins", "joined", "lateral", "new partner"]
                    ) else "press_release"

                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type=sig_type,
                        title=f"[GNews] {title[:160]}",
                        body=summary[:400],
                        url=link,
                        department=dept,
                        department_score=score * NEWS_WEIGHT,
                        matched_keywords=kw,
                        published_at=pub,
                    ))
            except Exception as e:
                self.logger.debug(f"GoogleNews {q}: {e}")

        return signals[:10]
