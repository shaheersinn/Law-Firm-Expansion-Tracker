"""
Keyword weight learner.

Uses an Exponential Moving Average (EMA) to update per-keyword multipliers
stored in the `keyword_weights` table.

Algorithm per keyword k in department d:
  • hit_rate  = confirmed_count / (confirmed_count + false_positive_count)
  • new_mult  = EMA(prev_mult, target_mult, alpha=0.2)
  • target_mult is in [0.3, 2.5]: low if keyword never confirms, high if it always does

Additionally runs "keyword discovery": scans confirmed signal bodies for
recurring n-grams not yet in the taxonomy and logs them as candidates.
"""

import re
import json
import logging
import sqlite3
from collections import Counter, defaultdict
from classifier.taxonomy import TAXONOMY

logger = logging.getLogger("learning.keyword_learner")

EMA_ALPHA       = 0.20   # smoothing — lower = more stable, higher = faster adaptation
MIN_MULT        = 0.30   # never drop a keyword below 30% weight
MAX_MULT        = 2.50   # never boost above 250%
MIN_SAMPLES     = 5      # don't adjust until we have this many feedback rows for a keyword
DISCOVERY_MIN   = 4      # n-gram must appear ≥ 4 times to be suggested as new keyword

STOPWORDS = {
    "the","a","an","and","or","of","in","to","for","on","at","by","with",
    "this","that","is","are","was","were","be","has","have","had","will",
    "from","as","its","it","their","we","our","can","more","been","not",
    "also","which","but","who","all","any","may","law","firm","legal",
}


def _ngrams(text: str, n: int = 2) -> list[str]:
    """Extract n-grams from text, filtering stopwords."""
    words = [w for w in re.findall(r"\b[a-z]{3,}\b", text.lower()) if w not in STOPWORDS]
    return [" ".join(words[i:i+n]) for i in range(len(words) - n + 1)]


class KeywordLearner:
    def __init__(self, db):
        self._db = db
        self._ensure_tables()

    def _ensure_tables(self):
        self._db.conn.executescript("""
            CREATE TABLE IF NOT EXISTS keyword_weights (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                department  TEXT    NOT NULL,
                keyword     TEXT    NOT NULL,
                multiplier  REAL    NOT NULL DEFAULT 1.0,
                samples     INTEGER DEFAULT 0,
                updated_at  TEXT    DEFAULT (datetime('now')),
                UNIQUE(department, keyword)
            );

            CREATE TABLE IF NOT EXISTS keyword_candidates (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                department  TEXT    NOT NULL,
                ngram       TEXT    NOT NULL,
                frequency   INTEGER DEFAULT 1,
                first_seen  TEXT    DEFAULT (datetime('now')),
                UNIQUE(department, ngram)
            );
        """)
        self._db.conn.commit()

    # ------------------------------------------------------------------ #
    #  Main update
    # ------------------------------------------------------------------ #

    def update_weights(self):
        """
        For every keyword in the taxonomy, recalculate its multiplier
        using feedback data, then save to keyword_weights.
        """
        updated = 0

        # Build feedback map: {(dept, keyword) -> (confirmed, false_pos)}
        feedback = self._build_keyword_feedback_map()

        for dept, data in TAXONOMY.items():
            all_kws = list(data.get("keywords", [])) + list(data.get("phrases", []))
            for kw in all_kws:
                key = (dept, kw.lower())
                confirmed, false_pos = feedback.get(key, (0, 0))
                total = confirmed + false_pos

                # Load existing multiplier
                cur = self._db.conn.execute(
                    "SELECT multiplier, samples FROM keyword_weights WHERE department=? AND keyword=?",
                    (dept, kw.lower())
                )
                row = cur.fetchone()
                prev_mult = row[0] if row else 1.0
                prev_samples = row[1] if row else 0

                new_samples = prev_samples + total
                if total < MIN_SAMPLES:
                    # Not enough data — keep existing, just update sample count
                    if total > 0:
                        self._upsert_weight(dept, kw.lower(), prev_mult, new_samples)
                    continue

                hit_rate = confirmed / total
                # Map hit_rate [0,1] → target_mult [MIN_MULT, MAX_MULT]
                target_mult = MIN_MULT + hit_rate * (MAX_MULT - MIN_MULT)
                # EMA update
                new_mult = round(EMA_ALPHA * target_mult + (1 - EMA_ALPHA) * prev_mult, 4)
                new_mult = max(MIN_MULT, min(MAX_MULT, new_mult))

                self._upsert_weight(dept, kw.lower(), new_mult, new_samples)
                updated += 1

        logger.info(f"Keyword weights updated: {updated} keywords adjusted")
        return updated

    def discover_new_keywords(self):
        """
        Scan confirmed signal bodies for recurring bigrams/trigrams not yet
        in the taxonomy. Saves them to keyword_candidates for manual review.
        """
        cur = self._db.conn.execute("""
            SELECT s.body, s.department
            FROM signals s
            JOIN signal_feedback f ON f.signal_id = s.id
            WHERE f.outcome = 'confirmed' AND s.body IS NOT NULL AND s.body != ''
        """)
        rows = cur.fetchall()

        dept_ngrams: dict[str, Counter] = defaultdict(Counter)
        for body, dept in rows:
            if not dept:
                continue
            for n in (2, 3):
                dept_ngrams[dept].update(_ngrams(body, n))

        # Get existing keywords to avoid re-suggesting them
        existing = set()
        for data in TAXONOMY.values():
            for kw in data.get("keywords", []) + data.get("phrases", []):
                existing.add(kw.lower())

        discovered = 0
        for dept, counter in dept_ngrams.items():
            for ngram, freq in counter.most_common(30):
                if ngram in existing or len(ngram) < 5:
                    continue
                if freq >= DISCOVERY_MIN:
                    try:
                        self._db.conn.execute("""
                            INSERT INTO keyword_candidates (department, ngram, frequency)
                            VALUES (?,?,?)
                            ON CONFLICT(department, ngram)
                            DO UPDATE SET frequency = frequency + excluded.frequency
                        """, (dept, ngram, freq))
                        discovered += 1
                    except Exception:
                        pass
        self._db.conn.commit()
        logger.info(f"Keyword discovery: {discovered} new candidates logged")
        return discovered

    # ------------------------------------------------------------------ #
    #  Helpers
    # ------------------------------------------------------------------ #

    def _build_keyword_feedback_map(self) -> dict:
        """
        Returns {(dept_lower, keyword_lower) -> (confirmed_count, false_pos_count)}.
        Uses signal_feedback.matched_keywords (comma-separated) joined with department.
        """
        cur = self._db.conn.execute("""
            SELECT department, matched_keywords, outcome
            FROM signal_feedback
            WHERE matched_keywords IS NOT NULL AND matched_keywords != ''
        """)
        result: dict[tuple, list[int, int]] = defaultdict(lambda: [0, 0])
        for dept, kws_str, outcome in cur.fetchall():
            if not dept:
                continue
            for kw in kws_str.split(","):
                kw = kw.strip().lower()
                if not kw:
                    continue
                key = (dept, kw)
                if outcome == "confirmed":
                    result[key][0] += 1
                elif outcome == "false_positive":
                    result[key][1] += 1
        return {k: tuple(v) for k, v in result.items()}

    def _upsert_weight(self, dept: str, kw: str, mult: float, samples: int):
        try:
            self._db.conn.execute("""
                INSERT INTO keyword_weights (department, keyword, multiplier, samples)
                VALUES (?,?,?,?)
                ON CONFLICT(department, keyword)
                DO UPDATE SET multiplier=excluded.multiplier,
                              samples=excluded.samples,
                              updated_at=datetime('now')
            """, (dept, kw, mult, samples))
            self._db.conn.commit()
        except Exception as e:
            logger.error(f"Weight upsert error: {e}")
