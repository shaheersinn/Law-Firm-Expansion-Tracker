"""
ChambersScraper
Monitors Chambers Canada and Legal 500 Canada rankings.

Signal research insight:
  "When a firm climbs in Chambers Canada rankings, it signals that clients
   are actively recommending them and work volume is growing."
  "A firm gaining a new Band in a practice area = growing client base."

We scrape Chambers and Legal500 guide pages for firm mentions,
and also monitor the firms' own awards pages.
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

CHAMBERS_WEIGHT = 3.0

CHAMBERS_SOURCES = [
    {
        "name": "Chambers Canada",
        "url_template": "https://chambers.com/firm/{slug}",
        "fallback": "https://chambers.com/guide/canada",
    },
    {
        "name": "Legal 500 Canada",
        "url_template": "https://www.legal500.com/firms/{slug}/canada/",
        "fallback": "https://www.legal500.com/c/canada/",
    },
]

RANKING_KEYWORDS = [
    "band 1", "band 2", "band 3", "ranked", "top ranked", "leading firm",
    "notable practitioner", "leading individual", "recognized",
    "chambers canada", "legal 500", "best law firm", "lexpert",
    "up and coming", "associate to watch",
]


class ChambersScraper(BaseScraper):
    name = "ChambersScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []

        # Check firm's own awards/rankings page
        for suffix in ["/awards", "/rankings", "/recognition", "/about/rankings",
                       "/en/about/rankings", "/en/awards"]:
            url = firm["website"].rstrip("/") + suffix
            soup = self._soup(url, timeout=15)
            if not soup:
                continue
            text = soup.get_text(separator=" ")
            lower = text.lower()
            hits = [k for k in RANKING_KEYWORDS if k in lower]
            if not hits:
                break

            # Find any mention of "band" followed by a number near practice area
            for tag in soup.find_all(["li", "p", "div", "h3", "h4"])[:80]:
                tag_text = self._clean(tag.get_text())
                tag_lower = tag_text.lower()
                if not any(k in tag_lower for k in RANKING_KEYWORDS):
                    continue
                if len(tag_text) < 15:
                    continue
                dept, score, kw = _clf.top_department(tag_text)
                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type="ranking",
                    title=f"[Rankings] {firm['short']}: {tag_text[:120]}",
                    body=tag_text[:400],
                    url=url,
                    department=dept,
                    department_score=score * CHAMBERS_WEIGHT,
                    matched_keywords=kw + hits[:3],
                ))
                if len(signals) >= 5:
                    break
            break

        # Google News for Chambers/rankings mentions
        try:
            import feedparser
            from urllib.parse import quote_plus
            q = quote_plus(f'"{firm["short"]}" chambers canada OR "legal 500" 2025 OR 2026')
            url = f"https://news.google.com/rss/search?q={q}&hl=en-CA&gl=CA&ceid=CA:en"
            feed = feedparser.parse(url)
            for entry in (feed.entries or [])[:8]:
                title   = entry.get("title", "")
                summary = entry.get("summary", "")
                link    = entry.get("link", url)
                pub     = entry.get("published", "")
                full    = f"{title} {summary}"
                if not any(k in full.lower() for k in RANKING_KEYWORDS):
                    continue
                dept, score, kw = _clf.top_department(full)
                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type="ranking",
                    title=f"[Chambers/L500] {title[:160]}",
                    body=summary[:400],
                    url=link,
                    department=dept,
                    department_score=score * CHAMBERS_WEIGHT,
                    matched_keywords=kw,
                    published_at=pub,
                ))
        except Exception as e:
            self.logger.debug(f"ChambersScraper news: {e}")

        return signals[:6]
