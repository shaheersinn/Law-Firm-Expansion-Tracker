"""
Publications & Thought Leadership Scraper
==========================================
Lawyers publish in new areas 3–6 months before hiring begins.
Tracks client alerts, articles, and external publications.

Sources:
  - Firm insights/publications pages
  - SSRN (Social Science Research Network) — Canadian law
  - Lexology
  - Mondaq Canada
"""

import re
from bs4 import BeautifulSoup
from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

classifier = DepartmentClassifier()

INSIGHT_PATH_VARIANTS = [
    "/insights", "/publications", "/knowledge", "/resources",
    "/client-alerts", "/articles", "/en/insights",
    "/en/publications", "/en/resources/client-alerts",
    "/en-ca/insights",
]


class PublicationsScraper(BaseScraper):
    name = "PublicationsScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        signals.extend(self._scrape_firm_insights(firm))
        signals.extend(self._scrape_lexology(firm))
        signals.extend(self._scrape_mondaq(firm))
        return signals

    def _scrape_firm_insights(self, firm: dict) -> list[dict]:
        signals = []
        base = firm["website"].rstrip("/")

        for path in INSIGHT_PATH_VARIANTS:
            url = base + path
            resp = self._get(url)
            if not resp:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            items = soup.find_all(
                ["article", "div", "li"],
                class_=re.compile(r"insight|publication|article|alert|post|resource", re.I)
            )[:25]

            for item in items:
                text = item.get_text(separator=" ", strip=True)
                if len(text) < 30:
                    continue

                title_tag = item.find(["h2", "h3", "h4", "a"])
                title = title_tag.get_text(strip=True) if title_tag else text[:150]

                link_tag = item.find("a", href=True)
                link = link_tag["href"] if link_tag else url
                if link.startswith("/"):
                    link = base + link

                classifications = classifier.classify(f"{title} {text}", top_n=1)
                if not classifications:
                    continue

                cls = classifications[0]
                if cls["score"] < 1.5:
                    continue

                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type="publication",
                    title=f"[{firm['short']} Insights] {title}",
                    body=text[:600],
                    url=link,
                    department=cls["department"],
                    department_score=cls["score"],
                    matched_keywords=cls["matched_keywords"],
                ))

            if signals:
                break

        self.logger.info(f"[{firm['short']}] Firm insights: {len(signals)} signal(s)")
        return signals

    def _scrape_lexology(self, firm: dict) -> list[dict]:
        """Lexology aggregates law firm client alerts — searchable by firm."""
        signals = []
        query = firm["short"].replace(" ", "+")
        url = f"https://www.lexology.com/library?firm={query}&l=canada"
        resp = self._get(url)
        if not resp:
            return signals

        soup = BeautifulSoup(resp.text, "html.parser")
        for item in soup.find_all(class_=re.compile(r"article|item|result|post", re.I))[:15]:
            text = item.get_text(separator=" ", strip=True)
            if len(text) < 30:
                continue

            title_tag = item.find(["h2", "h3", "h4", "a"])
            title = title_tag.get_text(strip=True) if title_tag else text[:150]

            classifications = classifier.classify(f"{title} {text}", top_n=1)
            if not classifications:
                continue

            cls = classifications[0]
            if cls["score"] < 1.0:
                continue

            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type="publication",
                title=f"[Lexology] {title}",
                body=text[:600],
                url=url,
                department=cls["department"],
                department_score=cls["score"] * 1.2,
                matched_keywords=cls["matched_keywords"],
            ))

        self.logger.info(f"[{firm['short']}] Lexology: {len(signals)} signal(s)")
        return signals

    def _scrape_mondaq(self, firm: dict) -> list[dict]:
        signals = []
        query = firm["short"].replace(" ", "%20")
        url = f"https://www.mondaq.com/canada/search/{query}"
        resp = self._get(url)
        if not resp:
            return signals

        soup = BeautifulSoup(resp.text, "html.parser")
        for item in soup.find_all(class_=re.compile(r"article|result|content-item", re.I))[:12]:
            text = item.get_text(separator=" ", strip=True)
            if len(text) < 40:
                continue

            firm_found = (
                firm["short"].lower() in text.lower() or
                any(n.lower() in text.lower() for n in firm.get("alt_names", []))
            )
            if not firm_found:
                continue

            title_tag = item.find(["h2", "h3", "h4", "a"])
            title = title_tag.get_text(strip=True) if title_tag else text[:150]

            classifications = classifier.classify(f"{title} {text}", top_n=1)
            if not classifications:
                continue

            cls = classifications[0]
            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type="publication",
                title=f"[Mondaq] {title}",
                body=text[:600],
                url=url,
                department=cls["department"],
                department_score=cls["score"],
                matched_keywords=cls["matched_keywords"],
            ))

        self.logger.info(f"[{firm['short']}] Mondaq: {len(signals)} signal(s)")
        return signals
