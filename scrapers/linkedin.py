"""
LinkedInScraper — uses Google-cached LinkedIn pages to detect
lateral hires and partner announcements without hitting LinkedIn directly.
Weight: 2.0–3.5
"""

import re
from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

classifier = DepartmentClassifier()

GOOGLE_CACHE_URL = "https://webcache.googleusercontent.com/search?q=cache:linkedin.com/company/{slug}/posts"
GOOGLE_SEARCH_URL = (
    "https://www.google.com/search?q=site:linkedin.com+%22{firm_name}%22+"
    "%22joins%22+OR+%22new+partner%22+OR+%22lateral%22&tbs=qdr:m"
)

LATERAL_RE = re.compile(
    r"(?:joins|joined|has\s+joined|welcomes?|new\s+partner|lateral|appointed\s+(?:as\s+)?partner)",
    re.IGNORECASE
)


class LinkedInScraper(BaseScraper):
    name = "LinkedInScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        signals.extend(self._google_search(firm))
        return signals

    def _google_search(self, firm: dict) -> list[dict]:
        query = f'site:linkedin.com "{firm["short"]}" "joins" OR "new partner" OR "lateral"'
        url = (
            "https://www.google.com/search?"
            f"q={query.replace(' ', '+')}&tbs=qdr:m&num=10"
        )
        soup = self.get_soup(url)
        if not soup:
            return []

        signals = []
        for tag in soup.find_all("div", class_=re.compile(r"^g$|tF2Cxc|MjjYud"), limit=15):
            title_tag = tag.find("h3")
            snippet_tag = tag.find("span", class_=re.compile(r"aCOpRe|st|IsZvec"))
            if not title_tag:
                continue

            title   = title_tag.get_text(" ", strip=True)
            snippet = snippet_tag.get_text(" ", strip=True) if snippet_tag else ""
            full    = f"{title} {snippet}"

            if not LATERAL_RE.search(full):
                continue

            link_tag = tag.find("a", href=True)
            link = link_tag["href"] if link_tag else ""
            if link.startswith("/url?q="):
                link = link[7:].split("&")[0]

            cls = classifier.top_department(full)
            if not cls:
                continue

            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type="lateral_hire",
                title=f"[LinkedIn] {title[:160]}",
                body=snippet[:400],
                url=link,
                department=cls["department"],
                department_score=cls["score"] * 3.0,
                matched_keywords=cls["matched_keywords"],
            ))

        return signals[:5]
