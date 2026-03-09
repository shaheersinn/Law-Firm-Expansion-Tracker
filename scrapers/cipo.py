"""
CIPOScraper
Monitors CIPO (Canadian Intellectual Property Office) for patent
and trademark filing activity by agents associated with tracked firms.

When a firm's IP group is filing heavily, it signals active client work
and likely need for IP associate support.

CIPO public search: https://ised-isde.canada.ca/site/canadian-intellectual-property-office
Open data: https://open.canada.ca/data/en/dataset (CIPO datasets)
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier
from urllib.parse import quote_plus

_clf = DepartmentClassifier()

CIPO_WEIGHT = 2.0


class CIPOScraper(BaseScraper):
    name = "CIPOScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        try:
            import feedparser
        except ImportError:
            return signals

        # Since CIPO API requires registration, use Google News for IP news
        q = quote_plus(f'"{firm["short"]}" patent OR trademark OR "intellectual property" Canada 2025 OR 2026')
        url = f"https://news.google.com/rss/search?q={q}&hl=en-CA&gl=CA&ceid=CA:en"

        try:
            feed = feedparser.parse(url)
            for entry in (feed.entries or [])[:8]:
                title   = entry.get("title", "")
                summary = entry.get("summary", "")
                link    = entry.get("link", url)
                pub     = entry.get("published", "")
                full    = f"{title} {summary}"
                lower   = full.lower()

                if not any(k in lower for k in [
                    "patent", "trademark", "intellectual property", "cipo",
                    "ip", "trade-mark", "copyright infringement",
                ]):
                    continue

                dept, score, kw = _clf.top_department(full)
                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type="ip_filing",
                    title=f"[CIPO/IP] {title[:160]}",
                    body=summary[:400],
                    url=link,
                    department="IP",
                    department_score=score * CIPO_WEIGHT,
                    matched_keywords=kw + ["cipo", "ip"],
                    published_at=pub,
                ))
        except Exception as e:
            self.logger.debug(f"CIPOScraper: {e}")

        # Also check firm's IP practice page
        for suffix in ["/intellectual-property", "/ip", "/en/practices/intellectual-property",
                       "/practices/ip-technology"]:
            ip_url = firm["website"].rstrip("/") + suffix
            soup = self._soup(ip_url, timeout=12)
            if not soup:
                continue
            text = soup.get_text(separator=" ")
            lower = text.lower()
            if "patent" in lower or "trademark" in lower or "intellectual" in lower:
                dept, score, kw = _clf.top_department(text[:800])
                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type="ip_filing",
                    title=f"[CIPO] {firm['short']} IP practice page active",
                    body=text[:400],
                    url=ip_url,
                    department="IP",
                    department_score=score * CIPO_WEIGHT,
                    matched_keywords=kw,
                ))
            break

        return signals[:4]
