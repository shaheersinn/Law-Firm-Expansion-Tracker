"""
ConferenceScraper — scrapes LSO CPD, OBA Institute, Osgoode PD,
PDAC, IAPP, Canadian Institute for speaking/sponsorship appearances.
Signals: bar_speaking (1.5), bar_sponsorship (2.5)
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

classifier = DepartmentClassifier()

CONFERENCE_SOURCES = [
    {"name": "LSO CPD",          "url": "https://lso.ca/lawyers/enhance-your-practice/programs",       "weight": 2.0},
    {"name": "OBA Institute",    "url": "https://www.oba.org/Conferences/Upcoming-Conferences",         "weight": 2.0},
    {"name": "PDAC",             "url": "https://www.pdac.ca/convention/sponsors-exhibitors",           "weight": 2.5},
    {"name": "Osgoode PD",       "url": "https://www.osgoode.yorku.ca/professional-development/",       "weight": 1.5},
    {"name": "Canadian Institute","url": "https://www.canadianinstitute.com/",                          "weight": 2.0},
    {"name": "Insight",          "url": "https://www.insightinfo.com/conferences/legal/",               "weight": 1.5},
]

SPONSOR_KEYWORDS = ["presenting sponsor", "gold sponsor", "sponsor", "premier sponsor", "platinum"]
SPEAKER_KEYWORDS = ["speaker", "panelist", "moderator", "chair", "keynote", "faculty"]


class ConferenceScraper(BaseScraper):
    name = "ConferenceScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        firm_names = [firm["short"].lower()] + [n.lower() for n in firm.get("alt_names", [])]

        for source in CONFERENCE_SOURCES:
            soup = self.get_soup(source["url"])
            if not soup:
                continue

            for tag in soup.find_all(["li", "p", "td", "div", "span"], limit=200):
                text = tag.get_text(" ", strip=True)
                lower = text.lower()

                if not any(n in lower for n in firm_names):
                    continue
                if len(text) < 20 or len(text) > 400:
                    continue

                is_sponsor = any(kw in lower for kw in SPONSOR_KEYWORDS)
                is_speaker = any(kw in lower for kw in SPEAKER_KEYWORDS)

                if not (is_sponsor or is_speaker):
                    continue

                sig_type    = "bar_sponsorship" if is_sponsor else "bar_speaking"
                weight_mult = 1.2 if is_sponsor else 1.0

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

        return signals[:8]
