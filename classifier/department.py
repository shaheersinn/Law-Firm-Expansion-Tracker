"""
DepartmentClassifier — scores text against 17 practice departments.
Phrase matches get a 2.5× multiplier over single-keyword hits.
"""

import re
from classifier.taxonomy import DEPARTMENTS

PHRASE_BOOST = 2.5


class DepartmentClassifier:
    def __init__(self):
        self._compiled = {}
        for dept, data in DEPARTMENTS.items():
            self._compiled[dept] = {
                "keywords": [k.lower() for k in data.get("keywords", [])],
                "phrases":  [p.lower() for p in data.get("phrases",  [])],
            }

    def classify(self, text: str, top_n: int = 3) -> list[dict]:
        """
        Returns up to top_n department matches, sorted by score descending.
        Each result: {department, score, matched_keywords}
        """
        if not text:
            return []

        lower = text.lower()
        results = []

        for dept, data in self._compiled.items():
            score = 0.0
            matched = []

            for phrase in data["phrases"]:
                if phrase in lower:
                    score += PHRASE_BOOST
                    matched.append(phrase)

            for kw in data["keywords"]:
                if kw not in lower:
                    continue
                # avoid double-counting if already caught by phrase
                if any(kw in p for p in matched):
                    continue
                # word-boundary check for short keywords
                if len(kw) <= 3:
                    if not re.search(r'\b' + re.escape(kw) + r'\b', lower):
                        continue
                score += 1.0
                matched.append(kw)

            if score > 0:
                results.append({
                    "department":      dept,
                    "score":           round(score, 2),
                    "matched_keywords": matched,
                })

        results.sort(key=lambda x: x["score"], reverse=True)
        return results[:top_n]

    def top_department(self, text: str) -> dict | None:
        results = self.classify(text, top_n=1)
        return results[0] if results else None
