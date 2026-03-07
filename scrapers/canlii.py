"""
CanLIIScraper — queries the CanLII REST API for recent court decisions.
Free API key from https://api.canlii.org

BUG FIXED: f-string nested quote SyntaxError that crashed the entire run.
"""

import os
from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

classifier = DepartmentClassifier()

CANLII_API_BASE = "https://api.canlii.org/v1"

# High-value court/tribunal IDs to search
COURT_IDS = [
    ("csc-scc",  "Supreme Court of Canada"),
    ("fca-caf",  "Federal Court of Appeal"),
    ("fc-cf",    "Federal Court"),
    ("onca",     "Ontario Court of Appeal"),
    ("onsc",     "Ontario Superior Court"),
    ("qcca",     "Quebec Court of Appeal"),
    ("abca",     "Alberta Court of Appeal"),
    ("bcca",     "BC Court of Appeal"),
    ("oncat",    "Ontario Civil Resolution Tribunal"),
    ("chrt-tcdp","Canadian Human Rights Tribunal"),
    ("crtc",     "CRTC"),
    ("opc-cpvp", "Office of the Privacy Commissioner"),
]


class CanLIIScraper(BaseScraper):
    name = "CanLIIScraper"

    def __init__(self):
        super().__init__()
        self.api_key = os.getenv("CANLII_API_KEY", "")

    def fetch(self, firm: dict) -> list[dict]:
        if not self.api_key:
            # FIX: was using nested double-quotes inside f-string — caused SyntaxError
            short = firm["short"]
            self.logger.debug(
                f"[{short}] CanLII skipped — CANLII_API_KEY not set. "
                "Get a free key at https://api.canlii.org"
            )
            return []

        signals = []
        firm_names = [firm["short"]] + firm.get("alt_names", [])

        for court_id, court_name in COURT_IDS:
            for name_variant in firm_names[:2]:
                results = self._search_court(court_id, court_name, name_variant, firm)
                signals.extend(results)

        return signals

    def _search_court(self, court_id: str, court_name: str, query: str, firm: dict) -> list[dict]:
        url = (
            f"{CANLII_API_BASE}/caseBrowse/en/{court_id}/"
            f"?api_key={self.api_key}&resultCount=10&keyword={query}"
        )
        resp = self.get(url)
        if not resp:
            return []

        try:
            data = resp.json()
        except Exception:
            return []

        cases = data.get("cases", [])
        signals = []

        for case in cases:
            title = case.get("title", "")
            case_id = case.get("caseId", {}).get("en", "")
            decision_date = case.get("decisionDate", "")
            url_link = f"https://www.canlii.org/en/{court_id}/{case_id}/doc.html"

            if not self.is_recent(decision_date):
                continue

            full_text = f"{title} {court_name}"
            cls = classifier.top_department(full_text)
            dept = cls["department"] if cls else "Litigation"
            keywords = cls["matched_keywords"] if cls else []
            score = (cls["score"] if cls else 1.0) * 2.5

            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type="court_record",
                title=f"[{court_name}] {title[:160]}",
                body=f"Decision date: {decision_date} | Case: {case_id}",
                url=url_link,
                department=dept,
                department_score=score,
                matched_keywords=keywords,
            ))

        return signals
