"""
AwardsScraper
Monitors Best Lawyers Canada, Lexpert, Benchmark Canada,
Who's Who Legal, and Precedent awards.

Signal research insight:
  "Associate-level recognitions in Chambers — firms highlighting
   'Associate to Watch' and 'Up and Coming' recognitions indicate
   a developing bench and room for junior growth."
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

AWARDS_WEIGHT = 2.5

AWARDS_SOURCES = [
    {
        "name": "Best Lawyers",
        "rss":  None,
        "search": "https://www.bestlawyers.com/canada",
    },
    {
        "name": "Lexpert",
        "rss":  "https://www.lexpert.ca/rss/",
        "search": None,
    },
    {
        "name": "Precedent",
        "rss":  "https://www.precedentmagazine.com/feed/",
        "search": None,
    },
]

AWARDS_KEYWORDS = [
        "best lawyers", "lexpert", "benchmark", "who's who legal",
        "lawyer of the year", "top 40 under 40", "precedent innovator",
        "rising star", "associate to watch", "up and coming",
        "recognized", "named to", "listed in", "award", "ranked",
]


class AwardsScraper(BaseScraper):
    name = "AwardsScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        try:
            import feedparser
        except ImportError:
            return signals

        for src in AWARDS_SOURCES:
            if not src["rss"]:
                continue
            try:
                feed = feedparser.parse(src["rss"])
                for entry in (feed.entries or [])[:25]:
                    title   = entry.get("title", "")
                    summary = entry.get("summary", "")
                    link    = entry.get("link", src["rss"])
                    pub     = entry.get("published", "")
                    full    = f"{title} {summary}"
                    lower   = full.lower()

                    if not any(n.lower() in lower for n in firm_names):
                        continue
                    if not any(k in lower for k in AWARDS_KEYWORDS):
                        continue

                    dept, score, kw = _clf.top_department(full)
                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="ranking",
                        title=f"[{src['name']}] {title[:160]}",
                        body=summary[:400],
                        url=link,
                        department=dept,
                        department_score=score * AWARDS_WEIGHT,
                        matched_keywords=kw,
                        published_at=pub,
                    ))
            except Exception as e:
                self.logger.debug(f"Awards {src['name']}: {e}")

        # Google News for awards
        try:
            from urllib.parse import quote_plus
            q = quote_plus(f'"{firm["short"]}" best lawyers OR lexpert OR ranked 2025 OR 2026')
            url = f"https://news.google.com/rss/search?q={q}&hl=en-CA&gl=CA&ceid=CA:en"
            feed = feedparser.parse(url)
            for entry in (feed.entries or [])[:8]:
                title   = entry.get("title", "")
                summary = entry.get("summary", "")
                link    = entry.get("link", url)
                pub     = entry.get("published", "")
                full    = f"{title} {summary}"
                if not any(k in full.lower() for k in AWARDS_KEYWORDS):
                    continue
                dept, score, kw = _clf.top_department(full)
                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type="ranking",
                    title=f"[Awards] {title[:160]}",
                    body=summary[:400],
                    url=link,
                    department=dept,
                    department_score=score * AWARDS_WEIGHT,
                    matched_keywords=kw,
                    published_at=pub,
                ))
        except Exception as e:
            self.logger.debug(f"Awards news: {e}")

        return signals[:6]
