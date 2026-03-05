"""
Department classifier.
Multi-label NLP scorer using keyword + phrase matching.
Phrase matches get 2.5× boost over single-word keywords.
Score is normalized by text length to prevent long-document bias.
"""

import re
import logging
from classifier.taxonomy import DEPARTMENTS

logger = logging.getLogger("classifier")

PHRASE_BOOST      = 2.5
MIN_SCORE         = 0.5
NORMALIZE_PER     = 500   # words
MAX_NORMALIZED    = 25.0


class DepartmentClassifier:
    def __init__(self):
        self._compiled = self._compile_patterns()

    def _compile_patterns(self) -> list[dict]:
        compiled = []
        for dept in DEPARTMENTS:
            kw_patterns = [
                (re.compile(r'\b' + re.escape(kw.lower()) + r'\b'), 1.0)
                for kw in dept.get("keywords", [])
            ]
            ph_patterns = [
                (re.compile(re.escape(ph.lower())), PHRASE_BOOST)
                for ph in dept.get("phrases", [])
            ]
            compiled.append({
                "name":     dept["name"],
                "patterns": ph_patterns + kw_patterns,  # phrases first
            })
        return compiled

    def classify(self, text: str, top_n: int = 3) -> list[dict]:
        """
        Returns top_n departments sorted by score descending.
        Each result: {department, score, matched_keywords}
        """
        if not text or not text.strip():
            return []

        text_lower = text.lower()
        word_count = max(len(text_lower.split()), 1)
        norm_factor = NORMALIZE_PER / word_count

        results = []
        for dept in self._compiled:
            raw_score  = 0.0
            matched    = []
            seen_terms = set()

            for pattern, weight in dept["patterns"]:
                match = pattern.search(text_lower)
                if match:
                    term = match.group(0)
                    if term not in seen_terms:
                        raw_score += weight
                        matched.append(term)
                        seen_terms.add(term)

            if raw_score < MIN_SCORE:
                continue

            normalized = min(raw_score * norm_factor, MAX_NORMALIZED)
            results.append({
                "department":       dept["name"],
                "score":            round(normalized, 2),
                "raw_score":        round(raw_score, 2),
                "matched_keywords": matched[:8],
            })

        results.sort(key=lambda r: r["score"], reverse=True)
        return results[:top_n]

    def best(self, text: str) -> str | None:
        """Return the single best department name, or None."""
        results = self.classify(text, top_n=1)
        return results[0]["department"] if results else None
