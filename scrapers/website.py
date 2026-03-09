"""
WebsiteScraper
Scrapes firm practice area pages and detects content changes.
A changed practice page often signals a new group being built out.
"""

import hashlib
from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

PRACTICE_SUFFIXES = [
    "/practice-areas", "/practices", "/services",
    "/en/practice-areas", "/en/services", "/en/practices",
    "/what-we-do", "/our-work",
]

WEB_WEIGHT = 2.5


class WebsiteScraper(BaseScraper):
    name = "WebsiteScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []

        base = firm["website"].rstrip("/")
        careers_url = firm.get("careers_url", "")

        urls_to_check = []
        for suf in PRACTICE_SUFFIXES:
            urls_to_check.append(base + suf)
        if careers_url:
            urls_to_check.append(careers_url)

        for url in urls_to_check[:3]:  # cap at 3 pages per firm
            soup = self._soup(url, timeout=15)
            if not soup:
                continue

            text = soup.get_text(separator=" ")
            dept, score, kw = _clf.top_department(text[:2000])

            content_hash = hashlib.sha256(text.encode()).hexdigest()

            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type="website_snapshot",
                title=f"[{firm['short']}] Practice page: {url.split('/')[-1] or 'home'}",
                body=text[:800],
                url=url,
                department=dept,
                department_score=score * WEB_WEIGHT,
                matched_keywords=kw,
            ))
            break  # one successful page is enough

        return signals
