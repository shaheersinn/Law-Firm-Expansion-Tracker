"""
EventScraper
Monitors firm event pages and industry conference agendas
for speaking engagements and sponsorships.

Signal research insight:
  "Track conference agendas published by OBA, CCCA, Federated Press.
   If a firm has 3–4 lawyers speaking at the same conference,
   that practice group is almost certainly growing."
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier
from urllib.parse import quote_plus

_clf = DepartmentClassifier()

EVENT_WEIGHT = 1.8

SPEAKING_KEYWORDS = [
    "speaker", "panelist", "keynote", "moderator", "presents",
    "speaking", "faculty", "chair", "co-chair", "featured",
]

SPONSOR_KEYWORDS = [
    "sponsor", "presenting sponsor", "supporting sponsor",
    "event partner", "gold sponsor", "silver sponsor",
]

EVENT_SOURCES = [
    {
        "name": "CCCA",
        "url": "https://www.ccca-caj.ca/en/events/",
        "dept_hint": None,
    },
    {
        "name": "PDAC",
        "url": "https://www.pdac.ca/convention/speakers",
        "dept_hint": "Energy",
    },
]


class EventScraper(BaseScraper):
    name = "EventScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []

        # Firm's own events page
        for suffix in ["/events", "/en/events", "/news-events", "/speaking"]:
            url = firm["website"].rstrip("/") + suffix
            soup = self._soup(url, timeout=12)
            if not soup:
                continue
            for tag in soup.find_all(["li", "p", "article", "div"])[:50]:
                text = self._clean(tag.get_text())
                lower = text.lower()
                if len(text) < 15:
                    continue
                is_speaking   = any(k in lower for k in SPEAKING_KEYWORDS)
                is_sponsorship = any(k in lower for k in SPONSOR_KEYWORDS)
                if not (is_speaking or is_sponsorship):
                    continue

                a = tag.find("a", href=True)
                link = url
                if a:
                    href = a["href"]
                    link = href if href.startswith("http") else url

                sig_type = "bar_speaking" if is_speaking else "bar_sponsorship"
                dept, score, kw = _clf.top_department(text)
                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type=sig_type,
                    title=f"[Event] {firm['short']}: {text[:140]}",
                    body=text[:400],
                    url=link,
                    department=dept,
                    department_score=score * EVENT_WEIGHT,
                    matched_keywords=kw,
                ))
                if len(signals) >= 4:
                    return signals
            break  # first working suffix

        # Google News for event speaking
        try:
            import feedparser
            q = quote_plus(f'"{firm["short"]}" speaker OR keynote OR conference 2025 OR 2026')
            url = f"https://news.google.com/rss/search?q={q}&hl=en-CA&gl=CA&ceid=CA:en"
            feed = feedparser.parse(url)
            for entry in (feed.entries or [])[:6]:
                title   = entry.get("title", "")
                summary = entry.get("summary", "")
                link    = entry.get("link", url)
                pub     = entry.get("published", "")
                full    = f"{title} {summary}"
                lower   = full.lower()
                if not any(k in lower for k in SPEAKING_KEYWORDS + SPONSOR_KEYWORDS):
                    continue
                dept, score, kw = _clf.top_department(full)
                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type="bar_speaking",
                    title=f"[Event/News] {title[:160]}",
                    body=summary[:400],
                    url=link,
                    department=dept,
                    department_score=score * EVENT_WEIGHT,
                    matched_keywords=kw,
                    published_at=pub,
                ))
        except Exception as e:
            self.logger.debug(f"EventScraper news: {e}")

        return signals[:4]
