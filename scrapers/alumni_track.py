"""
AlumniTrackScraper
Tracks clerkship alumni hire announcements.

Signal research insight:
  "Law clerks finishing their federal or provincial court clerkships
   almost always articulate at or join a firm directly afterward, and firms
   actively recruit them. When a firm publicly announces a clerkship hire,
   it signals they had an open spot — meaning articling class demand was real."

Sources:
  - Google News for "clerk" + firm mentions
  - Firm press releases announcing clerk hires
  - SCC, FCA, ONCA clerkship program news
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier
from urllib.parse import quote_plus

_clf = DepartmentClassifier()

ALUMNI_WEIGHT = 2.5

CLERK_KEYWORDS = [
    "clerk", "clerkship", "law clerk", "articling clerk",
    "scc clerk", "supreme court clerk", "federal court clerk",
    "court of appeal clerk", "former clerk", "ex-clerk",
]

HIRE_PHRASES = [
    "joins", "joined", "has joined", "welcomes", "hired",
    "returns to", "associate at", "starting at",
]


class AlumniTrackScraper(BaseScraper):
    name = "AlumniTrackScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        try:
            import feedparser
        except ImportError:
            return signals

        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        # Google News for clerk hires at this firm
        q = quote_plus(f'"{firm["short"]}" clerk OR clerkship joins associate 2025 OR 2026')
        url = f"https://news.google.com/rss/search?q={q}&hl=en-CA&gl=CA&ceid=CA:en"

        try:
            feed = feedparser.parse(url)
            for entry in (feed.entries or [])[:12]:
                title   = entry.get("title", "")
                summary = entry.get("summary", "")
                link    = entry.get("link", url)
                pub     = entry.get("published", "")
                full    = f"{title} {summary}"
                lower   = full.lower()

                if not any(n.lower() in lower for n in firm_names):
                    continue
                if not any(k in lower for k in CLERK_KEYWORDS):
                    continue

                dept, score, kw = _clf.top_department(full)
                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type="alumni_hire",
                    title=f"[Alumni] {title[:160]}",
                    body=summary[:400],
                    url=link,
                    department=dept,
                    department_score=score * ALUMNI_WEIGHT,
                    matched_keywords=kw + ["clerkship"],
                    published_at=pub,
                ))
        except Exception as e:
            self.logger.debug(f"AlumniTrack: {e}")

        # Vacancy chain: check if firm recently announced promotions (creates open slots)
        news_url = firm.get("news_url", "")
        if news_url and len(signals) < 3:
            soup = self._soup(news_url, timeout=15)
            if soup:
                for a in (soup.find_all("a", href=True) or [])[:40]:
                    text = self._clean(a.get_text()).lower()
                    if "promoted" in text or "counsel" in text or "partner" in text:
                        dept, score, kw = _clf.top_department(text)
                        href = a["href"]
                        if not href.startswith("http"):
                            from urllib.parse import urljoin
                            href = urljoin(news_url, href)
                        signals.append(self._make_signal(
                            firm_id=firm["id"],
                            firm_name=firm["name"],
                            signal_type="alumni_hire",
                            title=f"[Vacancy Chain] {firm['short']}: {text[:120]}",
                            body=text[:300],
                            url=href,
                            department=dept,
                            department_score=score * (ALUMNI_WEIGHT * 0.8),
                            matched_keywords=kw + ["promotion", "vacancy"],
                        ))
                        if len(signals) >= 4:
                            break

        return signals[:5]
