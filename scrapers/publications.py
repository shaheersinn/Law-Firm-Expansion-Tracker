"""
Publications & Thought Leadership Scraper
===========================================
When lawyers publish articles, client alerts, or blog posts about an area,
it precedes actual client work by 3-6 months — making it an early-warning
leading indicator.

What we track:
  1. Firm's own insights/publications page
  2. SSRN (legal academic papers with firm affiliation)
  3. Lexology (law firm client alerts aggregator)
  4. Mondaq (legal knowledge management platform)

Signal weight: 1.0 (lowest tier — needs corroboration)
"""

import re
from bs4 import BeautifulSoup
from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

classifier = DepartmentClassifier()

PUB_SOURCES = [
    {
        "name": "Lexology",
        "search_url": "https://www.lexology.com/library/search?q={firm}&type=article&jurisdiction=canada",
    },
    {
        "name": "Mondaq",
        "search_url": "https://www.mondaq.com/canada/search?q={firm}&tags=legal",
    },
]

# Publication recency signals
RECENT_PHRASES = [
    "today", "this week", "this month", "recently", "new", "latest",
    "just published", "hot off the press", "alert",
]


class PublicationsScraper(BaseScraper):
    name = "PublicationsScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        signals.extend(self._scrape_firm_insights(firm))
        signals.extend(self._scrape_lexology(firm))
        return self._deduplicate(signals)

    # ------------------------------------------------------------------ #
    #  Firm's own insights/publications page
    # ------------------------------------------------------------------ #

    def _scrape_firm_insights(self, firm: dict) -> list[dict]:
        signals = []
        base = firm["website"].rstrip("/")

        insight_paths = [
            "/insights", "/publications", "/knowledge", "/resources",
            "/en/insights", "/en-ca/insights", "/en/resources",
            "/our-thinking", "/thought-leadership",
        ]

        for path in insight_paths:
            url = base + path
            response = self._get(url)
            if not response or len(response.text) < 200:
                continue

            soup = BeautifulSoup(response.text, "html.parser")

            articles = soup.find_all(
                ["article", "div", "li"],
                class_=re.compile(r"post|article|item|insight|publication|card|result", re.I)
            )[:30]

            for art in articles:
                text = art.get_text(separator=" ", strip=True)
                if len(text) < 30:
                    continue

                title_tag = art.find(["h2", "h3", "h4", "a"])
                title = title_tag.get_text(strip=True) if title_tag else text[:100]
                if not title or len(title) < 10:
                    continue

                classifications = classifier.classify(text, top_n=1)
                if not classifications:
                    continue

                cls = classifications[0]
                if cls["score"] < 1.0:
                    continue

                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type="publication",
                    title=f"[{firm['short']} Insights] {title}",
                    body=text[:600],
                    url=url,
                    department=cls["department"],
                    department_score=cls["score"],
                    matched_keywords=cls["matched_keywords"],
                ))

            if signals:
                break   # found working insights path

        self.logger.info(f"[{firm['short']}] Firm insights: {len(signals)} signal(s)")
        return signals

    # ------------------------------------------------------------------ #
    #  Lexology
    # ------------------------------------------------------------------ #

    def _scrape_lexology(self, firm: dict) -> list[dict]:
        signals = []
        firm_q = firm["short"].replace(" ", "+").replace("&", "and")
        url = f"https://www.lexology.com/library/search?q={firm_q}&type=article&jurisdiction=canada"

        response = self._get(url)
        if not response:
            return signals

        soup = BeautifulSoup(response.text, "html.parser")

        for card in soup.find_all(
            ["div", "article", "li"],
            class_=re.compile(r"article|result|item|post|entry", re.I)
        )[:20]:
            text = card.get_text(separator=" ", strip=True)
            text_lower = text.lower()

            if (firm["short"].lower() not in text_lower
                    and firm["name"].split()[0].lower() not in text_lower):
                continue

            title_tag = card.find(["h2", "h3", "a"])
            title = title_tag.get_text(strip=True) if title_tag else text[:100]

            classifications = classifier.classify(text, top_n=1)
            if not classifications:
                continue

            cls = classifications[0]
            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type="publication",
                title=f"[Lexology] {title}",
                body=text[:500],
                url=url,
                department=cls["department"],
                department_score=cls["score"],
                matched_keywords=cls["matched_keywords"],
            ))

        self.logger.info(f"[{firm['short']}] Lexology: {len(signals)} signal(s)")
        return signals

    def _deduplicate(self, signals: list[dict]) -> list[dict]:
        seen = set()
        result = []
        for s in signals:
            key = s["title"].lower()[:80]
            if key not in seen:
                seen.add(key)
                result.append(s)
        return result
