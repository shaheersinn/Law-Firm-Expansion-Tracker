"""
AwardsScraper — scrapes Best Lawyers, Lexpert, Benchmark Canada, Who's Who Legal.
Rankings signal firm reputational investment in specific practice areas.
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

classifier = DepartmentClassifier()

AWARD_SOURCES = [
    {
        "name": "Best Lawyers Canada",
        "url":  "https://www.bestlawyers.com/canada",
        "weight": 3.0,
    },
    {
        "name": "Lexpert",
        "url":  "https://www.lexpert.ca/rankings/",
        "weight": 3.0,
    },
    {
        "name": "Benchmark Litigation Canada",
        "url":  "https://benchmarklitigation.com/rankings/canada",
        "weight": 2.5,
    },
    {
        "name": "Who's Who Legal",
        "url":  "https://whoswholegal.com/canada",
        "weight": 2.5,
    },
]


class AwardsScraper(BaseScraper):
    name = "AwardsScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        firm_names = [firm["short"].lower(), firm["name"].split()[0].lower()]

        for source in AWARD_SOURCES:
            soup = self.get_soup(source["url"])
            if not soup:
                continue

            for tag in soup.find_all(["tr", "li", "div", "p"], limit=200):
                text = tag.get_text(" ", strip=True)
                lower = text.lower()

                if not any(n in lower for n in firm_names):
                    continue
                if len(text) < 20 or len(text) > 500:
                    continue

                cls = classifier.top_department(text)
                dept = cls["department"] if cls else "Corporate/M&A"

                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type="ranking",
                    title=f"[{source['name']}] {text[:160]}",
                    url=source["url"],
                    department=dept,
                    department_score=(cls["score"] if cls else 1.0) * source["weight"],
                    matched_keywords=cls["matched_keywords"] if cls else [],
                ))

            if len(signals) >= 8:
                break

        return signals[:10]
