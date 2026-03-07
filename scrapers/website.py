"""
WebsiteScraper — crawls practice area pages and detects content changes.
A changed page often means a new hire, renamed practice, or new service.
"""

import hashlib
from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

classifier = DepartmentClassifier()

PRACTICE_URL_PATTERNS = [
    "/practice-areas", "/practices", "/services", "/our-work",
    "/expertise", "/what-we-do",
]


class WebsiteScraper(BaseScraper):
    name = "WebsiteScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        base = firm["website"].rstrip("/")

        # Try common practice-area paths
        for path in PRACTICE_URL_PATTERNS:
            url = base + path
            resp = self.get(url)
            if not resp or resp.status_code != 200:
                continue

            content = resp.text
            content_hash = hashlib.md5(content.encode()).hexdigest()

            # Emit a snapshot signal for change detection downstream
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(content, "lxml")
            text = soup.get_text(" ", strip=True)[:1000]

            cls = classifier.top_department(text)
            dept = cls["department"] if cls else "Corporate/M&A"

            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type="website_snapshot",
                title=f"Practice page snapshot: {url}",
                body=content_hash,   # used for change detection
                url=url,
                department=dept,
                department_score=0,  # scored only on change
                matched_keywords=[],
            ))
            break  # only need one successful page

        return signals
