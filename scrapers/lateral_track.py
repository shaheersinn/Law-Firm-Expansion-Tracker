"""
LateralTrackScraper
Monitors lateral partner hire announcements from:
  - Canadian Lawyer Magazine
  - The Lawyer's Daily
  - Law Times
  - Firm news pages (direct)
Lateral hires are the highest-conviction expansion signal.
"""

import re
from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

LATERAL_SOURCES = [
    "https://www.canadianlawyermag.com/rss/",
    "https://www.thelawyersdaily.ca/rss",
    "https://www.lawtimesnews.com/rss",
]

LATERAL_PHRASES = [
    "joins", "joined", "has joined", "welcomes", "lateral hire",
    "new partner", "expands team", "grows practice",
    "appointed partner", "named partner", "lateral partner",
    "brings on", "adds partner", "recruits partner",
]

# Weight for lateral hire signals
LATERAL_WEIGHT = 3.0


class LateralTrackScraper(BaseScraper):
    name = "LateralTrackScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        try:
            import feedparser
        except ImportError:
            return signals

        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        for feed_url in LATERAL_SOURCES:
            try:
                feed = feedparser.parse(feed_url)
                for entry in (feed.entries or [])[:30]:
                    title   = entry.get("title", "")
                    summary = entry.get("summary", entry.get("description", ""))
                    link    = entry.get("link", feed_url)
                    pub     = entry.get("published", "")
                    full    = f"{title} {summary}"
                    lower   = full.lower()

                    if not any(n.lower() in lower for n in firm_names):
                        continue
                    if not any(p in lower for p in LATERAL_PHRASES):
                        continue

                    dept, score, kw = _clf.top_department(full)
                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="lateral_hire",
                        title=f"[Lateral] {title[:160]}",
                        body=summary[:600],
                        url=link,
                        department=dept,
                        department_score=score * LATERAL_WEIGHT,
                        matched_keywords=kw,
                        published_at=pub,
                    ))
            except Exception as e:
                self.logger.debug(f"LateralTrack feed {feed_url}: {e}")

        # Also check firm's own news page for lateral announce keywords
        news_url = firm.get("news_url", "")
        if news_url:
            soup = self._soup(news_url)
            if soup:
                for a in (soup.find_all("a", href=True) or [])[:40]:
                    title_text = self._clean(a.get_text())
                    if not title_text or len(title_text) < 10:
                        continue
                    lower = title_text.lower()
                    if any(p in lower for p in LATERAL_PHRASES):
                        href = a["href"]
                        if not href.startswith("http"):
                            from urllib.parse import urljoin
                            href = urljoin(news_url, href)
                        dept, score, kw = _clf.top_department(title_text)
                        signals.append(self._make_signal(
                            firm_id=firm["id"],
                            firm_name=firm["name"],
                            signal_type="lateral_hire",
                            title=f"[{firm['short']}] {title_text[:160]}",
                            body=title_text,
                            url=href,
                            department=dept,
                            department_score=score * LATERAL_WEIGHT,
                            matched_keywords=kw,
                        ))

        return signals
