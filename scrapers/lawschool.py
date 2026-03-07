"""
LawSchoolScraper — scrapes recruit postings from student publications
and firm student pages. Articling / summer recruit surges signal growth.
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

classifier = DepartmentClassifier()

SOURCES = [
    {"name": "Ultra Vires",        "url": "https://ultravires.ca/recruit/",       "weight": 2.0},
    {"name": "GreatStudentJobs",   "url": "https://www.greatstudentjobs.com/jobs/?q={short}", "weight": 2.0},
]

RECRUIT_KEYWORDS = [
    "articling", "summer student", "1L", "2L", "law student",
    "articling student", "summer associate", "recruit",
]


class LawSchoolScraper(BaseScraper):
    name = "LawSchoolScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []

        # Firm's own student page
        signals.extend(self._scrape_student_page(firm))

        # External sources
        for source in SOURCES:
            url = source["url"].format(short=firm["short"].replace(" ", "+"))
            soup = self.get_soup(url)
            if not soup:
                continue

            firm_names = [firm["short"].lower(), firm["name"].lower()[:20]]

            for tag in soup.find_all(["a", "h3", "h4", "li"], limit=80):
                text = tag.get_text(" ", strip=True)
                lower = text.lower()

                if not any(n in lower for n in firm_names):
                    continue
                if not any(kw in lower for kw in RECRUIT_KEYWORDS):
                    continue

                link = ""
                if tag.name == "a":
                    href = tag.get("href", "")
                    link = href if href.startswith("http") else url + href

                cls = classifier.top_department(text)
                dept = cls["department"] if cls else "Corporate/M&A"

                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type="recruit_posting",
                    title=f"[{source['name']}] {text[:160]}",
                    url=link or url,
                    department=dept,
                    department_score=(cls["score"] if cls else 1.0) * source["weight"],
                    matched_keywords=cls["matched_keywords"] if cls else [],
                ))

        return signals[:10]

    def _scrape_student_page(self, firm: dict) -> list[dict]:
        # Try common student/articling URL patterns
        base = firm["website"].rstrip("/")
        candidates = [
            firm.get("careers_url", "") + "/students",
            base + "/students",
            base + "/careers/students",
            base + "/articling",
        ]
        for url in candidates:
            if not url or url == "/students":
                continue
            soup = self.get_soup(url)
            if not soup:
                continue

            text = soup.get_text(" ", strip=True)[:1000]
            if not any(kw in text.lower() for kw in RECRUIT_KEYWORDS):
                continue

            cls = classifier.top_department(text)
            return [self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type="recruit_posting",
                title=f"Student / articling page active: {url}",
                url=url,
                department=cls["department"] if cls else "Corporate/M&A",
                department_score=(cls["score"] if cls else 1.0) * 2.0,
                matched_keywords=cls["matched_keywords"] if cls else [],
            )]

        return []
