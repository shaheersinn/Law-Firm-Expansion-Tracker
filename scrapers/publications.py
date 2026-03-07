"""
PublicationsScraper — scrapes firm insights pages, Lexology, Mondaq.
High volume of publications in a department signals practice investment.
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

classifier = DepartmentClassifier()

LEXOLOGY_URL  = "https://www.lexology.com/library/results.aspx?q={query}&jurisdiction=Canada&sort=3"
MONDAQ_URL    = "https://www.mondaq.com/search/{query}?country=canada"


class PublicationsScraper(BaseScraper):
    name = "PublicationsScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        signals.extend(self._scrape_firm_insights(firm))
        signals.extend(self._scrape_lexology(firm))
        return signals

    def _scrape_firm_insights(self, firm: dict) -> list[dict]:
        url = firm.get("news_url", "")
        if not url:
            return []

        soup = self.get_soup(url)
        if not soup:
            return []

        signals = []
        for tag in soup.find_all(["a", "h3", "h4"], limit=60):
            text = tag.get_text(" ", strip=True)
            if len(text) < 25 or len(text) > 250:
                continue

            # Must look like an article title (has a practice-area keyword)
            cls = classifier.top_department(text)
            if not cls or cls["score"] < 1.5:
                continue

            link = ""
            if tag.name == "a":
                href = tag.get("href", "")
                link = href if href.startswith("http") else firm["website"] + href

            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type="publication",
                title=text[:200],
                url=link or url,
                department=cls["department"],
                department_score=cls["score"] * 1.0,
                matched_keywords=cls["matched_keywords"],
            ))

        # Deduplicate
        seen = set()
        unique = []
        for s in signals:
            if s["title"] not in seen:
                seen.add(s["title"])
                unique.append(s)

        return unique[:12]

    def _scrape_lexology(self, firm: dict) -> list[dict]:
        query = firm["short"].replace(" ", "+")
        url = LEXOLOGY_URL.format(query=query)
        soup = self.get_soup(url)
        if not soup:
            return []

        signals = []
        for article in soup.find_all("div", class_=lambda c: c and "article" in str(c).lower())[:10]:
            title_tag = article.find(["h2", "h3", "a"])
            if not title_tag:
                continue
            title = title_tag.get_text(" ", strip=True)
            if len(title) < 20:
                continue

            cls = classifier.top_department(title)
            if not cls:
                continue

            link_tag = article.find("a", href=True)
            link = link_tag["href"] if link_tag else url

            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type="publication",
                title=f"[Lexology] {title[:160]}",
                url=link,
                department=cls["department"],
                department_score=cls["score"] * 1.5,
                matched_keywords=cls["matched_keywords"],
            ))

        return signals
