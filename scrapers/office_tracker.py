"""
OfficeTracker
Detects new office openings, city expansions, and commercial real estate moves.

From the signal research:
  "Law firm office leases are a surprisingly powerful leading indicator.
   Expansions accounted for 36% of all law firm leasing transactions as of Q3 2025
   — the highest share since 2020."

Sources:
  - Firm website "about/offices" pages
  - Google News RSS for "[Firm] office"
  - CBRE Canada / Cushman news RSS (commercial RE)
  - Firm press releases
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier
from urllib.parse import quote_plus

_clf = DepartmentClassifier()

OFFICE_WEIGHT = 3.0

OFFICE_KEYWORDS = [
    "new office", "opens office", "opening office", "expands to",
    "new location", "new presence", "office expansion", "moves to",
    "relocated", "office lease", "additional office", "office space",
    "downtown office", "office opening", "new address",
]

CANADIAN_CITIES = [
    "toronto", "montreal", "vancouver", "calgary", "ottawa",
    "edmonton", "winnipeg", "halifax", "quebec city", "hamilton",
    "waterloo", "kitchener", "london ontario",
]


class OfficeTracker(BaseScraper):
    name = "OfficeTracker"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        try:
            import feedparser
        except ImportError:
            return signals

        # Check firm's about/offices page
        for suffix in ["/offices", "/about/offices", "/about", "/en/about"]:
            url = firm["website"].rstrip("/") + suffix
            soup = self._soup(url, timeout=15)
            if not soup:
                continue
            text = soup.get_text(separator=" ")
            lower = text.lower()
            if any(kw in lower for kw in OFFICE_KEYWORDS):
                city_hits = [c for c in CANADIAN_CITIES if c in lower]
                dept, score, kw = _clf.top_department(text[:500])
                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type="office_lease",
                    title=f"[{firm['short']}] Office/expansion page updated",
                    body=f"Office page mentions: {', '.join(city_hits or ['expansion'])}. {text[:300]}",
                    url=url,
                    department=dept,
                    department_score=score * OFFICE_WEIGHT,
                    matched_keywords=kw + city_hits,
                ))
            break  # stop after first successful fetch

        # Google News RSS for "[Firm] office"
        query = quote_plus(f'"{firm["short"]}" office Canada')
        gnews_url = f"https://news.google.com/rss/search?q={query}&hl=en-CA&gl=CA&ceid=CA:en"
        try:
            feed = feedparser.parse(gnews_url)
            for entry in (feed.entries or [])[:10]:
                title   = entry.get("title", "")
                summary = entry.get("summary", "")
                link    = entry.get("link", gnews_url)
                pub     = entry.get("published", "")
                full    = f"{title} {summary}".lower()

                if not any(kw in full for kw in OFFICE_KEYWORDS):
                    continue

                city_hits = [c for c in CANADIAN_CITIES if c in full]
                dept, score, kw_list = _clf.top_department(f"{title} {summary}")
                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type="office_lease",
                    title=f"[News] {title[:160]}",
                    body=summary[:400],
                    url=link,
                    department=dept,
                    department_score=score * OFFICE_WEIGHT,
                    matched_keywords=kw_list + city_hits,
                    published_at=pub,
                ))
        except Exception as e:
            self.logger.debug(f"OfficeTracker news {firm['short']}: {e}")

        return signals[:5]
