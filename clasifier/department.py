"""
Department classifier — keyword + phrase based multi-label scorer.

SELF-LEARNING:
  On every classify() call, keyword weights start from the static taxonomy but
  are boosted/penalised by learned_weights stored in the database.
  The learning/evolution.py module updates those weights nightly based on:
    • Which keywords actually appeared in high-confidence signals that later
      got confirmed (score_boost +0.1 per confirmation, EMA smoothed)
    • Which keywords appeared only in false-positive / low-quality signals
      (score_decay -0.05 per miss)

Score is normalized per 1,000 words so short snippets and long articles
can be compared fairly.
Phrase matches (multi-word) get a 2.5× boost over single-word keywords.
"""

import re
import json
import logging
import sqlite3
import os
from classifier.taxonomy import TAXONOMY

logger = logging.getLogger("classifier.department")

PHRASE_BOOST          = 2.5
SCORE_PER_1K_FLOOR    = 0.3   # minimum score to surface a department
LEARNED_WEIGHT_DB     = os.getenv("DB_PATH", "law_firm_tracker.db")


def _tokenize(text: str) -> str:
    """Lowercase and normalise whitespace."""
    return re.sub(r"\s+", " ", text.lower().strip())


def _load_learned_weights(db_path: str) -> dict:
    """
    Load learned keyword multipliers from the DB.
    Returns {department -> {keyword -> multiplier}} — defaults to 1.0.
    """
    weights = {}
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.execute(
            "SELECT department, keyword, multiplier FROM keyword_weights"
        )
        for dept, kw, mult in cur.fetchall():
            weights.setdefault(dept, {})[kw] = mult
        conn.close()
    except Exception:
        pass   # DB might not have the table yet — fall back to defaults
    return weights


class DepartmentClassifier:
    def __init__(self, db_path: str = None):
        self._db_path = db_path or LEARNED_WEIGHT_DB
        self._refresh()

    def _refresh(self):
        """(Re-)compile patterns and load current learned weights."""
        learned = _load_learned_weights(self._db_path)

        self._kw_patterns:     dict[str, list[tuple[str, float, re.Pattern]]] = {}
        self._phrase_patterns: dict[str, list[tuple[str, float, re.Pattern]]] = {}

        for dept, data in TAXONOMY.items():
            dept_learned = learned.get(dept, {})

            kw_pats = []
            for kw in data.get("keywords", []):
                mult = dept_learned.get(kw, 1.0)
                pat  = re.compile(r"\b" + re.escape(kw.lower()) + r"\b")
                kw_pats.append((kw, mult, pat))
            self._kw_patterns[dept] = kw_pats

            ph_pats = []
            for ph in data.get("phrases", []):
                mult = dept_learned.get(ph, 1.0)
                pat  = re.compile(re.escape(ph.lower()))
                ph_pats.append((ph, mult, pat))
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

        normalized  = _tokenize(text)
        word_count  = max(len(normalized.split()), 1)
        scale       = 1000 / word_count   # normalize to per-1k-words

        results = []
        for dept in TAXONOMY:
            raw_score = 0.0
            matched   = []

            # Phrase matches (higher base weight + learned multiplier)
            for ph, mult, pat in self._phrase_patterns.get(dept, []):
                hits = len(pat.findall(normalized))
                if hits:
                    raw_score += hits * PHRASE_BOOST * mult
                    matched.append(ph)

            # Keyword matches (learned multiplier per keyword)
            for kw, mult, pat in self._kw_patterns.get(dept, []):
                hits = len(pat.findall(normalized))
                if hits:
                    raw_score += hits * mult
                    matched.append(kw)

            if raw_score == 0:
                continue

            score = round(raw_score * scale, 3)
            if score >= SCORE_PER_1K_FLOOR:
                results.append({
                    "department":       dept,
                    "score":            score,
                    "matched_keywords": list(dict.fromkeys(matched))[:8],
                })

        results.sort(key=lambda x: x["score"], reverse=True)
        return results[:top_n]
