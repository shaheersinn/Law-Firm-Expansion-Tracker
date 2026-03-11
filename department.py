"""
Department classifier.
Multi-label NLP scorer using keyword + phrase matching.
Phrase matches get 2.5× boost over single-word keywords.
Score is normalized by text length to prevent long-document bias.

Changelog (Cycle 3):
  - BUG FIX: was using pattern.search() — only counted first match per term.
    Now uses pattern.findall() to count ALL occurrences, capped at MAX_TERM_HITS
    to prevent single-keyword stuffing from biasing scores.
  - Added title_boost: matches in the title/opening 200 chars score 2× higher.
  - Fixed MIN_RAW threshold guard: discard noisy sub-threshold raw scores
    before normalizing to avoid single-keyword false classifications.
  - classify() now accepts optional title= kwarg for split title/body scoring.
  - best() forwards title= kwarg through to classify().

Changelog (Cycle 6):
  - CRITICAL FIX: Lowered MIN_RAW from 0.8 → 0.4 to capture short RSS snippets.
    RSS titles are short (10-30 words); MIN_RAW=0.8 was too aggressive for them.
  - Short-text adaptive threshold: for texts <50 words use MIN_RAW=0.2.
  - Added 'confidence' field to results (high/medium/low) for caller context.
  - MIN_SCORE also lowered: 0.5 → 0.2 to match reduced MIN_RAW.
  - Added classify_with_fallback() convenience that always returns a result.
"""

import re
import logging
from classifier.taxonomy import DEPARTMENTS

logger = logging.getLogger("classifier")

PHRASE_BOOST   = 2.5
MIN_SCORE      = 0.2    # min normalised score (was 0.5 — too high for short text)
MIN_RAW        = 0.4    # min raw score (was 0.8 — killed all short RSS titles)
MIN_RAW_SHORT  = 0.2    # min raw for texts < SHORT_TEXT_WORDS words
SHORT_TEXT_WORDS = 50   # texts shorter than this use the relaxed threshold
NORMALIZE_PER  = 500    # words per normalisation window
MAX_NORMALIZED = 25.0
MAX_TERM_HITS  = 3      # cap per-term repeat counting to avoid stuffing
TITLE_BOOST    = 2.0    # multiplier for matches in title/opening text


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
                "patterns": ph_patterns + kw_patterns,  # phrases scored first
            })
        return compiled

    def classify(self, text: str, top_n: int = 3, title: str = "") -> list[dict]:
        """
        Returns top_n departments sorted by score descending.
        Each result: {department, score, raw_score, confidence, matched_keywords}

        Args:
            text:   Full body text to classify.
            top_n:  Maximum number of departments to return.
            title:  Optional title — matches here get TITLE_BOOST multiplier.
        """
        if not text or not text.strip():
            return []

        text_lower  = text.lower()
        title_lower = title.lower() if title else text_lower[:200]
        word_count  = max(len(text_lower.split()), 1)
        norm_factor = NORMALIZE_PER / word_count

        # Short texts get a relaxed threshold so RSS titles aren't all dropped
        effective_min_raw = MIN_RAW_SHORT if word_count < SHORT_TEXT_WORDS else MIN_RAW

        results = []
        for dept in self._compiled:
            raw_score  = 0.0
            matched    = []
            seen_terms = set()

            for pattern, weight in dept["patterns"]:
                # Count ALL occurrences in body, capped at MAX_TERM_HITS
                hits = pattern.findall(text_lower)
                if not hits:
                    continue
                term = hits[0]
                if term in seen_terms:
                    continue
                seen_terms.add(term)

                hit_count = min(len(hits), MAX_TERM_HITS)
                # Apply title boost if term also appears in title/opening
                title_hits = pattern.findall(title_lower)
                boost = TITLE_BOOST if title_hits else 1.0
                raw_score += weight * hit_count * boost
                matched.append(term)

            if raw_score < effective_min_raw:
                continue

            normalized = min(raw_score * norm_factor, MAX_NORMALIZED)
            if normalized < MIN_SCORE:
                continue

            # Confidence tier for downstream use
            if normalized >= 5.0:
                confidence = "high"
            elif normalized >= 2.0:
                confidence = "medium"
            else:
                confidence = "low"

            results.append({
                "department":       dept["name"],
                "score":            round(normalized, 2),
                "raw_score":        round(raw_score, 2),
                "confidence":       confidence,
                "matched_keywords": matched[:8],
            })

        results.sort(key=lambda r: r["score"], reverse=True)
        return results[:top_n]

    def best(self, text: str, title: str = "") -> str | None:
        """Return the single best department name, or None."""
        results = self.classify(text, top_n=1, title=title)
        return results[0]["department"] if results else None

    def classify_with_fallback(
        self, text: str, title: str = "", fallback: str = "General"
    ) -> dict:
        """Always return exactly one classification. Falls back to 'General'."""
        results = self.classify(text, top_n=1, title=title)
        if results:
            return results[0]
        return {
            "department":       fallback,
            "score":            0.1,
            "raw_score":        0.0,
            "confidence":       "low",
            "matched_keywords": [],
        }
