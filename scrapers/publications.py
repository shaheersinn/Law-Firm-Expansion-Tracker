"""
PublicationsScraper
Monitors firm insights pages, Lexology, and Mondaq.
High publication velocity in a practice area = active client work.

Signal research insight:
  "Monitor how actively a firm publishes client alerts — this is a proxy
   for how busy and growing that group is."
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

PUB_WEIGHT = 1.2

LEXOLOGY_BASE = "https://www.lexology.com/library?q={query}&jurisdiction=Canada"
MONDAQ_BASE   = "https://www.mondaq.com/search/?q={query}&country=Canada"


class PublicationsScraper(BaseScraper):
    name = "PublicationsScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []

        # ── Firm's own insights/publications page ──────────────────────
        insights_url = firm.get("news_url", "")
        if insights_url:
            soup = self._soup(insights_url)
            if soup:
                articles = soup.find_all(["article", "li", "div"], limit=60)
                for tag in articles:
                    a = tag.find("a", href=True)
                    if not a:
                        continue
                    text = self._clean(a.get_text())
                    if len(text) < 20:
                        continue
                    href = a["href"]
                    if not href.startswith("http"):
                        from urllib.parse import urljoin
                        href = urljoin(insights_url, href)

                    # look for sibling text (date, summary)
                    parent_text = self._clean(tag.get_text())
                    dept, score, kw = _clf.top_department(parent_text or text)
                    if score < 0.5:
                        continue

                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="publication",
                        title=f"[{firm['short']}] {text[:160]}",
                        body=parent_text[:400],
                        url=href,
                        department=dept,
                        department_score=score * PUB_WEIGHT,
                        matched_keywords=kw,
                    ))
                    if len(signals) >= 10:
                        break

        # ── Lexology RSS ───────────────────────────────────────────────
        try:
            import feedparser
            lex_url = "https://www.lexology.com/rss/feed/canada.xml"
            try:
                feed = feedparser.parse(lex_url)
                firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])
                for entry in (feed.entries or [])[:30]:
                    title   = entry.get("title", "")
                    summary = entry.get("summary", "")
                    link    = entry.get("link", lex_url)
                    pub     = entry.get("published", "")
                    full    = f"{title} {summary}"
                    # filter by firm OR high dept score
                    firm_hit = any(n.lower() in full.lower() for n in firm_names)
                    dept, score, kw = _clf.top_department(full)
                    if not firm_hit or score < 1.0:
                        continue
                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="publication",
                        title=f"[Lexology] {title[:160]}",
                        body=summary[:400],
                        url=link,
                        department=dept,
                        department_score=score * PUB_WEIGHT,
                        matched_keywords=kw,
                        published_at=pub,
                    ))
            except Exception as e:
                self.logger.debug(f"Lexology RSS: {e}")
        except ImportError:
            pass

        return signals[:12]
