"""
CanLIIScraper
Uses the CanLII REST API to find recent court decisions where
a tracked firm appears as counsel.

API docs: https://api.canlii.org/
Free API key required — set CANLII_API_KEY in GitHub Secrets.
Silently skips if no key is set.
"""

import os
from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

CANLII_BASE = "https://api.canlii.org/v1"
CANLII_WEIGHT = 2.5

# Courts to search (high-value signal courts)
COURTS = [
    "onca",   # Ontario Court of Appeal
    "oncj",   # Ontario Court of Justice
    "abca",   # Alberta Court of Appeal
    "bcca",   # BC Court of Appeal
    "scc-csc", # Supreme Court of Canada
    "fca-caf", # Federal Court of Appeal
    "fc-cf",   # Federal Court
]


class CanLIIScraper(BaseScraper):
    name = "CanLIIScraper"

    def __init__(self):
        super().__init__()
        self.api_key = os.getenv("CANLII_API_KEY", "").strip()

    def fetch(self, firm: dict) -> list[dict]:
        if not self.api_key:
            return []

        signals = []
        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        for court in COURTS[:4]:  # limit courts per run to stay within rate limits
            for name in firm_names[:2]:
                url = f"{CANLII_BASE}/caseBrowse/en/{court}/"
                params = {
                    "api_key": self.api_key,
                    "fullText": name,
                    "count": 5,
                    "offset": 0,
                }
                resp = self._get(url, params=params, timeout=20)
                if not resp:
                    continue
                try:
                    data = resp.json()
                    cases = data.get("cases", [])
                    for case in cases:
                        case_title  = case.get("title", "Unknown v Unknown")
                        citation    = case.get("citation", "")
                        case_url    = f"https://www.canlii.org/en/{court}/{case.get('caseId', {}).get('en', '')}/doc.html"
                        dept, score, kw = _clf.top_department(case_title)
                        signals.append(self._make_signal(
                            firm_id=firm["id"],
                            firm_name=firm["name"],
                            signal_type="court_record",
                            title=f"[CanLII/{court.upper()}] {case_title[:160]}",
                            body=f"{citation}. {case_title}",
                            url=case_url,
                            department=dept,
                            department_score=score * CANLII_WEIGHT,
                            matched_keywords=kw + [court],
                        ))
                        if len(signals) >= 8:
                            return signals
                except Exception as e:
                    self.logger.debug(f"CanLII parse {court}/{name}: {e}")

        return signals
