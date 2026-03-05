"""
Awards & Recognition Scraper
==============================
Industry awards and directory nominations are a lagging but high-confidence
signal. A new category nomination = the firm was active enough in that area
to be noticed by the nominating body.

Sources:
  1. Canadian Lawyer Top 25 Most Influential Lawyers
  2. Benchmark Canada (litigation rankings)
  3. Who's Who Legal Canada
  4. Best Lawyers Canada (annual directory)
  5. Lexpert Special Edition rankings
  6. Law360 Canada (new practice area coverage)
  7. LSUC / LSO Certificates of Distinction
  8. Precedent Magazine (innovation awards)
  9. CBA National Awards

Key insight: A lawyer winning "Lawyer of the Year" in a NEW practice area
for their firm = the firm quietly built that practice without announcing it.
This surfaces the signal 6-12 months after the expansion happened.
"""

import re
from bs4 import BeautifulSoup
from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

classifier = DepartmentClassifier()

AWARD_SOURCES = [
    {
        "name": "Best Lawyers Canada",
        "url": "https://www.bestlawyers.com/canada",
        "search_tpl": "https://www.bestlawyers.com/canada/search?firm={query}",
        "weight": 3.0,
    },
    {
        "name": "Benchmark Canada",
        "url": "https://benchmarklitigation.com/canada",
        "search_tpl": "https://benchmarklitigation.com/canada/search?q={query}",
        "weight": 3.0,
    },
    {
        "name": "Lexpert",
        "url": "https://www.lexpert.ca/rankings",
        "search_tpl": "https://www.lexpert.ca/rankings?firm={query}",
        "weight": 3.0,
    },
    {
        "name": "Who's Who Legal",
        "url": "https://whoswholegal.com/canada",
        "search_tpl": "https://whoswholegal.com/search?q={query}&country=canada",
        "weight": 2.5,
    },
    {
        "name": "Canadian Lawyer Top 25",
        "url": "https://www.canadianlawyermag.com/rankings/",
        "weight": 2.5,
    },
    {
        "name": "Precedent Magazine",
        "url": "https://www.precedentmagazine.com/innovation-awards/",
        "weight": 2.0,
    },
    {
        "name": "Law360 Canada",
        "url": "https://www.law360.ca/articles",
        "weight": 2.0,
    },
]

NEW_AWARD_PHRASES = [
    "named", "ranked", "recognized", "awarded", "selected",
    "appointed", "elected", "honoured", "honored", "listed",
    "band 1", "tier 1", "leading", "recommended", "highly recommended",
    "first time", "new entry", "newly ranked", "rising star",
    "lawyer of the year", "firm of the year",
]


class AwardsScraper(BaseScraper):
    name = "AwardsScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        for source in AWARD_SOURCES:
            signals.extend(self._scrape_source(firm, source))
        return signals

    def _scrape_source(self, firm: dict, source: dict) -> list[dict]:
        signals = []

        # Try templated search URL first, fall back to main page
        search_tpl = source.get("search_tpl", "")
        query = firm["short"].replace(" ", "+")
        url = search_tpl.format(query=query) if search_tpl else source["url"]

        resp = self._get(url)
        if not resp:
            # Try the base URL if search failed
            if url != source["url"]:
                resp = self._get(source["url"])
            if not resp:
                return signals

        soup = BeautifulSoup(resp.text, "html.parser")
        page_text = soup.get_text(separator=" ")

        # Check if firm appears at all
        firm_names = [firm["short"]] + firm.get("alt_names", []) + [firm["name"].split()[0]]
        firm_found = any(n.lower() in page_text.lower() for n in firm_names)
        if not firm_found:
            return signals

        # Find items mentioning both the firm and an award phrase
        candidates = soup.find_all(
            ["div", "article", "li", "tr", "section"],
            class_=re.compile(r"result|profile|lawyer|firm|ranking|award|entry|card", re.I)
        )[:30]

        # Also check plain paragraph/list content if no structured results
        if not candidates:
            candidates = soup.find_all(["p", "li"])[:40]

        for item in candidates:
            text = item.get_text(separator=" ", strip=True)
            text_lower = text.lower()

            if len(text) < 20:
                continue

            if not any(n.lower() in text_lower for n in firm_names):
                continue

            has_award_phrase = any(p in text_lower for p in NEW_AWARD_PHRASES)
            if not has_award_phrase:
                continue

            title_tag = item.find(["h2", "h3", "h4", "strong", "a"])
            title = title_tag.get_text(strip=True) if title_tag else text[:160]

            classifications = classifier.classify(text, top_n=1)
            if not classifications:
                continue

            cls = classifications[0]
            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type="ranking",
                title=f"[{source['name']}] {title}",
                body=text[:600],
                url=url,
                department=cls["department"],
                department_score=cls["score"] * source["weight"],
                matched_keywords=cls["matched_keywords"] + [source["name"]],
            ))

        if signals:
            self.logger.info(f"[{firm['short']}] {source['name']}: {len(signals)} signal(s)")
        return signals
