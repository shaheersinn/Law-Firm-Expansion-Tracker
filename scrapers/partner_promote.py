"""
PartnerPromoteScraper
=====================
Tracks partner and senior associate promotion announcements.
Promotions indicate practice group strength and future capacity expansion.

Sources:
  - Firm own news/press pages (uses firm["news_url"] if set)
  - Canadian Lawyer Magazine RSS
  - The Lawyer's Daily RSS
"""

import re
from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

PROMOTE_FEEDS = [
    {"url": "https://www.canadianlawyermag.com/rss/",   "name": "Canadian Lawyer"},
    {"url": "https://www.thelawyersdaily.ca/rss",       "name": "Lawyer's Daily"},
    {"url": "https://www.lawtimesnews.com/rss",         "name": "Law Times"},
]

PROMOTE_PHRASES = [
    "promoted to partner", "named partner", "elected partner",
    "new partner", "appointed partner", "makes partner",
    "senior associate to partner", "promoted associate",
    "advance to partnership", "elevated to partner",
    "senior counsel", "counsel promotion",
]

PROMOTE_WEIGHT = 2.5


class PartnerPromoteScraper(BaseScraper):
    name = "PartnerPromoteScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        try:
            import feedparser
        except ImportError:
            return signals

        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        for feed_meta in PROMOTE_FEEDS:
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
                    if not any(p in lower for p in PROMOTE_PHRASES):
                        continue

                    dept, score, kw = _clf.top_department(full)
                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="lateral_hire",
                        title=f"[{feed_meta['name']}] {title[:160]}",
                        body=summary[:600],
                        url=link,
                        department=dept,
                        department_score=score * PROMOTE_WEIGHT,
                        matched_keywords=kw,
                        published_at=pub,
                    ))
            except Exception as e:
                self.logger.debug(f"PartnerPromote {feed_meta['url']}: {e}")

        # Scrape firm's own news page for promotion keywords
        news_url = firm.get("news_url", "")
        if news_url:
            soup = self._soup(news_url)
            if soup:
                for a in (soup.find_all("a", href=True) or [])[:50]:
                    text = self._clean(a.get_text())
                    if not text or len(text) < 10:
                        continue
                    lower = text.lower()
                    if any(p in lower for p in PROMOTE_PHRASES):
                        href = a["href"]
                        if not href.startswith("http"):
                            from urllib.parse import urljoin
                            href = urljoin(news_url, href)
                        dept, score, kw = _clf.top_department(text)
                        signals.append(self._make_signal(
                            firm_id=firm["id"],
                            firm_name=firm["name"],
                            signal_type="lateral_hire",
                            title=f"[{firm['short']}] {text[:160]}",
                            body=text,
                            url=href,
                            department=dept,
                            department_score=score * PROMOTE_WEIGHT,
                            matched_keywords=kw,
                        ))

        return signals[:20]
