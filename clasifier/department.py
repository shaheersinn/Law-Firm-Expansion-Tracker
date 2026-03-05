"""
Department classifier — keyword + phrase based multi-label scorer.

Score is normalized per 1,000 words so short snippets and long articles
can be compared fairly.
Phrase matches (multi-word) get a 2.5× boost over single-word keywords.
"""

import re
import logging
from classifier.taxonomy import TAXONOMY

logger = logging.getLogger("classifier.department")

PHRASE_BOOST = 2.5
SCORE_PER_1K_WORDS_FLOOR = 0.3   # minimum score to include a department


def _tokenize(text: str) -> str:
    """Lowercase and normalise whitespace."""
    return re.sub(r"\s+", " ", text.lower().strip())


class DepartmentClassifier:
    def __init__(self):
        # Pre-compile patterns for speed
        self._kw_patterns: dict[str, list[tuple[str, re.Pattern]]] = {}
        self._phrase_patterns: dict[str, list[tuple[str, re.Pattern]]] = {}

        for dept, data in TAXONOMY.items():
            kw_pats = []
            for kw in data.get("keywords", []):
                pat = re.compile(r"\b" + re.escape(kw.lower()) + r"\b")
                kw_pats.append((kw, pat))
            self._kw_patterns[dept] = kw_pats

            ph_pats = []
            for ph in data.get("phrases", []):
                pat = re.compile(re.escape(ph.lower()))
                ph_pats.append((ph, pat))
            self._phrase_patterns[dept] = ph_pats

    def classify(self, text: str, top_n: int = 3) -> list[dict]:
        """
        Score text against all departments.

        Returns a list (up to top_n) of:
          {department, score, matched_keywords}
        sorted by score descending.
        """
        if not text or not text.strip():
            return []

        normalized = _tokenize(text)
        word_count = max(len(normalized.split()), 1)
        scale = 1000 / word_count   # normalize to per-1k-words

        results = []
        for dept in TAXONOMY:
            raw_score = 0.0
            matched = []

            # Phrase matches (higher weight)
            for ph, pat in self._phrase_patterns.get(dept, []):
                hits = len(pat.findall(normalized))
                if hits:
                    raw_score += hits * PHRASE_BOOST
                    matched.append(ph)

            # Keyword matches
            for kw, pat in self._kw_patterns.get(dept, []):
                hits = len(pat.findall(normalized))
                if hits:
                    raw_score += hits
                    matched.append(kw)

            if raw_score == 0:
                continue

            score = round(raw_score * scale, 3)
            if score >= SCORE_PER_1K_WORDS_FLOOR:
                results.append({
                    "department": dept,
                    "score": score,
                    "matched_keywords": list(dict.fromkeys(matched))[:8],  # dedupe, cap at 8
                })

        results.sort(key=lambda x: x["score"], reverse=True)
        return results[:top_n]
