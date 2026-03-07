"""
BarAssociationScraper — scrapes CBA sections, OBA, LSO, Advocates' Society.
Leadership roles = Tier 1 signals. Speaking = Tier 3 (early indicator).
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

classifier = DepartmentClassifier()

BAR_SOURCES = [
    # CBA section pages
    {"name": "CBA",  "url": "https://www.cba.org/Sections",                     "weight": 3.5, "type": "bar_leadership"},
    {"name": "OBA",  "url": "https://www.oba.org/Sections",                     "weight": 3.0, "type": "bar_leadership"},
    {"name": "LSO",  "url": "https://lso.ca/lawyers/enhance-your-practice",     "weight": 2.5, "type": "bar_speaking"},
    {"name": "CCCA", "url": "https://ccca-caj.ca/en/",                          "weight": 2.0, "type": "bar_leadership"},
    {"name": "Advocates Society", "url": "https://www.advocates.ca/",           "weight": 3.0, "type": "bar_speaking"},
    {"name": "ACC Canada",        "url": "https://www.acc.com/chapters/canada", "weight": 2.5, "type": "bar_speaking"},
]

LEADERSHIP_KEYWORDS = [
    "chair", "vice-chair", "president", "director", "executive committee",
    "board member", "elected", "appointed", "co-chair", "past president",
]


class BarAssociationScraper(BaseScraper):
    name = "BarAssociationScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        firm_names = [firm["short"].lower(), firm["name"].split()[0].lower()]

        for source in BAR_SOURCES:
            soup = self.get_soup(source["url"])
            if not soup:
                continue

            for tag in soup.find_all(["li", "p", "td", "div"], limit=200):
                text = tag.get_text(" ", strip=True)
                lower = text.lower()

                if not any(n in lower for n in firm_names):
                    continue
                if len(text) < 20 or len(text) > 400:
                    continue

                is_leadership = any(kw in lower for kw in LEADERSHIP_KEYWORDS)
                sig_type  = "bar_leadership" if is_leadership else source["type"]
                weight_mult = 1.5 if is_leadership else 1.0

                cls = classifier.top_department(text)
                dept = cls["department"] if cls else "Corporate/M&A"

                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type=sig_type,
                    title=f"[{source['name']}] {text[:160]}",
                    url=source["url"],
                    department=dept,
                    department_score=(cls["score"] if cls else 1.0) * source["weight"] * weight_mult,
                    matched_keywords=cls["matched_keywords"] if cls else [],
                ))

        return signals[:10]
