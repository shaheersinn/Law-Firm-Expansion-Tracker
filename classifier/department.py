"""
Department classifier.
Scores text against the taxonomy and returns ranked department matches.
Phrase matches (multi-word) receive a 2.5× boost over single-word keywords.
"""

import re
from classifier.taxonomy import DEPARTMENTS

PHRASE_BOOST = 2.5
MIN_SCORE = 0.2
MIN_RAW = 0.4
MIN_RAW_SHORT = 0.2
SHORT_TEXT_WORDS = 50
NORMALIZE_PER = 500
MAX_NORMALIZED = 25.0
MAX_TERM_HITS = 3
TITLE_BOOST = 2.0


class DepartmentClassifier:
    def __init__(self, db_path: str | None = None):
        # db_path is accepted for API compatibility but not currently used.
        self._kw_patterns: dict[str, list] = {}
        self._ph_patterns: dict[str, list] = {}
        for dept, cfg in DEPARTMENTS.items():
            self._kw_patterns[dept] = [
                re.compile(r"\b" + re.escape(kw.lower()) + r"\b")
                for kw in cfg["keywords"]
            ]
            self._ph_patterns[dept] = [
                re.compile(re.escape(ph.lower()))
                for ph in cfg["phrases"]
            ]

    def classify(self, text: str, top_n: int = 3, title: str = "") -> list[dict]:
        """
        Returns top_n departments scored by keyword/phrase hits.
        Each result: {"department": str, "score": float, "matched_keywords": list}
        """
        if not text or not text.strip():
            return []

        lower = text.lower()
        title_lower = title.lower() if title else lower[:200]
        word_count = max(len(lower.split()), 1)
        norm_factor = NORMALIZE_PER / word_count
        effective_min_raw = MIN_RAW_SHORT if word_count < SHORT_TEXT_WORDS else MIN_RAW
        results = []

        for dept, cfg in DEPARTMENTS.items():
            raw_score = 0.0
            matched = []
            seen_terms = set()

            for pat in self._kw_patterns[dept]:
                hits = pat.findall(lower)
                if not hits:
                    continue
                term = hits[0]
                if term in seen_terms:
                    continue
                seen_terms.add(term)
                hit_count = min(len(hits), MAX_TERM_HITS)
                boost = TITLE_BOOST if pat.findall(title_lower) else 1.0
                raw_score += hit_count * cfg["base_weight"] * boost
                matched.append(term)

            for pat in self._ph_patterns[dept]:
                hits = pat.findall(lower)
                if not hits:
                    continue
                term = hits[0]
                if term in seen_terms:
                    continue
                seen_terms.add(term)
                hit_count = min(len(hits), MAX_TERM_HITS)
                boost = TITLE_BOOST if pat.findall(title_lower) else 1.0
                raw_score += hit_count * cfg["base_weight"] * PHRASE_BOOST * boost
                matched.append(term)

            if raw_score < effective_min_raw:
                continue

            normalized = min(raw_score * norm_factor, MAX_NORMALIZED)
            if normalized < MIN_SCORE:
                continue

            if normalized >= 5.0:
                confidence = "high"
            elif normalized >= 2.0:
                confidence = "medium"
            else:
                confidence = "low"

            results.append({
                "department": dept,
                "score": round(normalized, 2),
                "raw_score": round(raw_score, 2),
                "confidence": confidence,
                "matched_keywords": matched[:8],
            })

        results.sort(key=lambda x: x["score"], reverse=True)
        return results[:top_n]

    def top_department(self, text: str, title: str = "") -> tuple[str, float, list]:
        """Returns (department, score, matched_keywords) or fallback."""
        results = self.classify(text, top_n=1, title=title)
        if results:
            r = results[0]
            return r["department"], r["score"], r["matched_keywords"]
        return "Corporate/M&A", 0.5, []

    def classify_with_fallback(
        self, text: str, title: str = "", fallback: str = "General"
    ) -> dict:
        """Always return exactly one classification. Falls back to a general bucket."""
        results = self.classify(text, top_n=1, title=title)
        if results:
            return results[0]
        return {
            "department": fallback,
            "score": 0.1,
            "raw_score": 0.0,
            "confidence": "low",
            "matched_keywords": [],
        }
