"""
PressScraper
Scrapes firm press-release/news pages plus Canadian legal trade press
for partner promotions, lateral hires, and practice group announcements.
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

PRESS_WEIGHT = 2.0

HIGH_VALUE_PHRASES = [
    "new partner", "partner promotion", "lateral", "joins",
    "new practice group", "expands practice", "named partner",
    "welcomes", "appointed", "hired", "new team",
]

TRADE_PRESS_FEEDS = [
    {"name": "Canadian Lawyer", "url": "https://www.canadianlawyermag.com/rss/"},
    {"name": "Law Times",       "url": "https://www.lawtimesnews.com/rss"},
    {"name": "Lawyers Daily",   "url": "https://www.thelawyersdaily.ca/rss"},
]


class PressScraper(BaseScraper):
    name = "PressScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        # ── Firm's own news page ────────────────────────────────────────
        news_url = firm.get("news_url", "")
        if news_url:
            soup = self._soup(news_url)
            if soup:
                for a in (soup.find_all("a", href=True) or [])[:60]:
                    text = self._clean(a.get_text())
                    if len(text) < 15:
                        continue
                    lower = text.lower()
                    if not any(p in lower for p in HIGH_VALUE_PHRASES):
                        continue
                    href = a["href"]
                    if not href.startswith("http"):
                        from urllib.parse import urljoin
                        href = urljoin(news_url, href)
                    dept, score, kw = _clf.top_department(text)
                    # Lateral hire gets boosted weight
                    sig_type = "lateral_hire" if any(
                        p in lower for p in ["joins", "joined", "lateral", "welcomes"]
                    ) else "press_release"
                    w = 3.0 if sig_type == "lateral_hire" else PRESS_WEIGHT
                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type=sig_type,
                        title=f"[{firm['short']}] {text[:160]}",
                        body=text,
                        url=href,
                        department=dept,
                        department_score=score * w,
                        matched_keywords=kw,
                    ))

        # ── Trade press RSS ─────────────────────────────────────────────
        try:
            import feedparser
            for feed_meta in TRADE_PRESS_FEEDS:
                try:
                    feed = feedparser.parse(feed_meta["url"])
                    for entry in (feed.entries or [])[:25]:
                        title   = entry.get("title", "")
                        summary = entry.get("summary", "")
                        link    = entry.get("link", feed_meta["url"])
                        pub     = entry.get("published", "")
                        full    = f"{title} {summary}"
                        lower   = full.lower()

                        if not any(n.lower() in lower for n in firm_names):
                            continue
                        dept, score, kw = _clf.top_department(full)
                        sig_type = "lateral_hire" if any(
                            p in lower for p in ["joins", "lateral", "welcomes", "new partner"]
                        ) else "press_release"
                        w = 3.0 if sig_type == "lateral_hire" else 1.8

                        signals.append(self._make_signal(
                            firm_id=firm["id"],
                            firm_name=firm["name"],
                            signal_type=sig_type,
                            title=f"[{feed_meta['name']}] {title[:160]}",
                            body=summary[:600],
                            url=link,
                            department=dept,
                            department_score=score * w,
                            matched_keywords=kw,
                            published_at=pub,
                        ))
                except Exception as e:
                    self.logger.debug(f"Press feed {feed_meta['url']}: {e}")
        except ImportError:
            pass

        return signals[:20]
