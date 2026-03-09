"""
SignalCrossRefScraper
Cross-references signals already collected this week to surface
corroborated patterns — the most reliable expansion signals.

Logic:
  - If a firm has BOTH a job posting AND a lateral hire in the same department,
    that corroboration boosts the confidence significantly.
  - If a firm has rankings + publications in the same department, flag it.
  - If office_lease + job_posting co-occur, flag it as high-confidence.

This scraper does not make HTTP requests — it reads from the database
of signals collected this run and creates corroboration meta-signals.
"""

import logging
from collections import defaultdict
from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()
logger = logging.getLogger("scrapers.SignalCrossRefScraper")

CROSS_WEIGHT = 3.5

# Pairs that, when both present for same firm+dept, create a corroborated signal
CORROBORATION_RULES = [
    ({"lateral_hire", "job_posting"},        "Lateral hire + active job posting: strong expansion"),
    ({"lateral_hire", "office_lease"},        "Lateral hire + office expansion: firm committed capital"),
    ({"ranking", "job_posting"},             "New ranking + hiring: growing practice"),
    ({"ranking", "lateral_hire"},            "New ranking + lateral: active build-out"),
    ({"office_lease", "job_posting"},        "Office lease + hiring: headcount growth confirmed"),
    ({"alumni_hire", "job_posting"},         "Alumni hire + job posting: articling pipeline active"),
    ({"bar_leadership", "job_posting"},      "Bar leadership + hiring: practice group investing"),
    ({"thought_leadership", "lateral_hire"}, "Content velocity + lateral: practice being built"),
]


class SignalCrossRefScraper(BaseScraper):
    name = "SignalCrossRefScraper"

    def __init__(self, current_run_signals: list[dict] | None = None):
        super().__init__()
        self._run_signals = current_run_signals or []

    def fetch(self, firm: dict) -> list[dict]:
        """
        Generates meta-signals from co-occurrence of signal types
        already seen for this firm in the current run.
        """
        signals = []
        if not self._run_signals:
            return signals

        # Group this firm's signals by department
        firm_signals = [s for s in self._run_signals if s.get("firm_id") == firm["id"]]
        if not firm_signals:
            return signals

        by_dept: dict[str, set[str]] = defaultdict(set)
        dept_urls: dict[str, list[str]] = defaultdict(list)

        for s in firm_signals:
            dept = s.get("department", "Corporate/M&A")
            by_dept[dept].add(s.get("signal_type", ""))
            url = s.get("url", "")
            if url:
                dept_urls[dept].append(url)

        for dept, sig_types in by_dept.items():
            for required_pair, description in CORROBORATION_RULES:
                if required_pair.issubset(sig_types):
                    # Found corroborated signal
                    ref_url = dept_urls[dept][0] if dept_urls[dept] else firm.get("news_url", firm["website"])
                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="lateral_hire",   # elevate to lateral weight
                        title=f"[CrossRef] {firm['short']} {dept}: {description}",
                        body=(
                            f"Corroborated signals in {dept}: "
                            f"{', '.join(sorted(sig_types))}. {description}"
                        ),
                        url=ref_url,
                        department=dept,
                        department_score=CROSS_WEIGHT,
                        matched_keywords=["corroborated", "cross-reference"] + list(required_pair),
                    ))

        return signals[:4]
