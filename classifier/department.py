"""
Department classifier.
Scores text against the taxonomy and returns ranked department matches.
Phrase matches (multi-word) receive a 2.5× boost over single-word keywords.
"""

import re
from classifier.taxonomy import DEPARTMENTS, DEPARTMENT_NAMES

PHRASE_BOOST = 2.5


class DepartmentClassifier:
    def __init__(self, db_path: str | None = None):
        # db_path is accepted for API compatibility but not currently used
        # (taxonomy is loaded from the static DEPARTMENTS dict)
        # Pre-compile patterns for speed
        self._kw_patterns: dict[str, list] = {}
        self._ph_patterns: dict[str, list] = {}
        for dept, cfg in DEPARTMENTS.items():
            self._kw_patterns[dept] = [
                re.compile(r"\b" + re.escape(kw.lower()) + r"\b")
                for kw in cfg["keywords"]
            ]
            self._ph_patterns[dept] = [
                re.compile(r"\b" + re.escape(ph.lower()) + r"\b")
                for ph in cfg["phrases"]
            ]

    def classify(self, text: str, top_n: int = 3) -> list[dict]:
        """
        Returns top_n departments scored by keyword/phrase hits.
        Each result: {"department": str, "score": float, "matched_keywords": list}
        """
        lower = text.lower()
        results = []

        for dept, cfg in DEPARTMENTS.items():
            score = 0.0
            matched = []

            for pat in self._kw_patterns[dept]:
                if pat.search(lower):
                    score += 1.0 * cfg["base_weight"]
                    matched.append(pat.pattern.strip(r"\b"))

            for pat in self._ph_patterns[dept]:
                if pat.search(lower):
                    score += PHRASE_BOOST * cfg["base_weight"]
                    matched.append(pat.pattern.strip(r"\b"))

            if score > 0:
                results.append({
                    "department": dept,
                    "score": round(score, 3),
                    "matched_keywords": matched[:10],
                })

        results.sort(key=lambda x: x["score"], reverse=True)
        return results[:top_n]

    def top_department(self, text: str) -> tuple[str, float, list]:
        """Returns (department, score, matched_keywords) or fallback."""
        results = self.classify(text, top_n=1)
        if results:
            r = results[0]
            return r["department"], r["score"], r["matched_keywords"]
        return "Corporate/M&A", 0.5, []
