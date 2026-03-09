"""
DiversityScraper
Monitors firm EDI (Equity, Diversity & Inclusion) initiatives,
pro bono program announcements, and diversity-related hiring signals.

Signal research insight:
  "Well-resourced firms run active pro bono programs — a healthy pipeline signal."
  "Firms explicitly hiring for Legal Innovation roles often precede associate hires."
  "42% of firms flagged legal tech as a priority skill — growth mindset firms."
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

DIV_WEIGHT = 1.8

EDI_KEYWORDS = [
    "diversity", "equity", "inclusion", "edi", "dei", "indigenous",
    "reconciliation", "pro bono", "legal innovation", "legal tech",
    "knowledge management", "innovation counsel", "lso equity",
    "black law students", "wla", "women in law", "2slgbtq",
]

GROWTH_SIGNALS = [
    "expanding", "growing", "new initiative", "launch", "program",
    "partnership", "commitment", "investment",
]


class DiversityScraper(BaseScraper):
    name = "DiversityScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []

        # Check firm news page for EDI/pro bono announcements
        news_url = firm.get("news_url", "")
        if news_url:
            soup = self._soup(news_url, timeout=15)
            if soup:
                for tag in soup.find_all(["a", "h2", "h3", "p"])[:60]:
                    text = self._clean(tag.get_text())
                    lower = text.lower()
                    if len(text) < 20:
                        continue
                    if not any(k in lower for k in EDI_KEYWORDS):
                        continue

                    has_growth = any(g in lower for g in GROWTH_SIGNALS)
                    dept, score, kw = _clf.top_department(text)

                    a = tag if tag.name == "a" else tag.find("a")
                    href = news_url
                    if a and a.get("href"):
                        raw = a["href"]
                        href = raw if raw.startswith("http") else news_url

                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="diversity_signal",
                        title=f"[EDI/PB] {firm['short']}: {text[:140]}",
                        body=text[:400],
                        url=href,
                        department=dept or "ESG",
                        department_score=(score or 1.0) * DIV_WEIGHT * (1.3 if has_growth else 1.0),
                        matched_keywords=kw,
                    ))
                    if len(signals) >= 3:
                        return signals

        # Google News for diversity signals
        try:
            import feedparser
            from urllib.parse import quote_plus
            q = quote_plus(f'"{firm["short"]}" diversity OR "pro bono" OR "legal innovation" 2025 OR 2026')
            url = f"https://news.google.com/rss/search?q={q}&hl=en-CA&gl=CA&ceid=CA:en"
            feed = feedparser.parse(url)
            for entry in (feed.entries or [])[:6]:
                title   = entry.get("title", "")
                summary = entry.get("summary", "")
                link    = entry.get("link", url)
                pub     = entry.get("published", "")
                full    = f"{title} {summary}"
                lower   = full.lower()
                if not any(k in lower for k in EDI_KEYWORDS):
                    continue
                dept, score, kw = _clf.top_department(full)
                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type="diversity_signal",
                    title=f"[EDI News] {title[:160]}",
                    body=summary[:400],
                    url=link,
                    department=dept or "ESG",
                    department_score=(score or 1.0) * DIV_WEIGHT,
                    matched_keywords=kw,
                    published_at=pub,
                ))
        except Exception as e:
            self.logger.debug(f"DiversityScraper news: {e}")

        return signals[:4]
