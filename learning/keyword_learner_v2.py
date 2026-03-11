"""
Advanced Keyword Learner (v2).

Improvements over v1:
  • Recency-weighted EMA — recent feedback (last 7 days) counts 3×
  • Momentum tracking — detects if a weight is oscillating and dampens it
  • Confidence scoring — keywords with many samples get higher confidence
  • Cross-department interference detection — keyword appearing in wrong dept
    gets its weight reduced for that dept specifically
  • Co-occurrence integration — uses the keyword_cooccurrence table from
    feedback_v2 for direct hit-rate calculation
  • Adaptive learning rate — uses schedule.current_alpha() so bootstrap
    phase learns faster

New keywords table columns:
  confidence   REAL  — 0.0–1.0, based on sample count (reaches 1.0 at 50+ samples)
  momentum     REAL  — how much the weight changed last cycle (for oscillation detection)
  prev_mult    REAL  — last cycle's multiplier (for momentum calc)
"""

import re
import logging
import math
from collections import Counter, defaultdict

from classifier.taxonomy import TAXONOMY
from learning.schedule import STABLE_ALPHA

logger = logging.getLogger("learning.keyword_learner_v2")

MIN_MULT      = 0.20
MAX_MULT      = 3.00
MIN_SAMPLES   = 3      # lower than v1 so bootstrap learns from fewer signals
DISCOVERY_MIN = 3      # n-gram appearance threshold for new keyword suggestions

STOPWORDS = {
    "the","a","an","and","or","of","in","to","for","on","at","by","with","this",
    "that","is","are","was","were","be","has","have","had","will","from","as",
    "its","it","their","we","our","can","more","been","not","also","which","but",
    "who","all","any","may","law","firm","legal","canada","canadian","toronto",
    "october","january","february","march","april","june","july","august","september",
    "november","december","monday","tuesday","wednesday","thursday","friday",
}


def _ngrams(text: str, n: int) -> list[str]:
    words = [w for w in re.findall(r"\b[a-z]{3,}\b", text.lower()) if w not in STOPWORDS]
    return [" ".join(words[i:i+n]) for i in range(len(words) - n + 1)]


def _confidence(samples: int) -> float:
    """Sigmoid-like confidence: 0 at 0 samples, ~1.0 at 50+ samples."""
    return round(1 - math.exp(-samples / 15), 3)


class KeywordLearnerV2:
    def __init__(self, db, schedule=None):
        self._db       = db
        self._schedule = schedule
        self._ensure_tables()

    def _ensure_tables(self):
        self._db.conn.executescript("""
            CREATE TABLE IF NOT EXISTS keyword_weights (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                department  TEXT    NOT NULL,
                keyword     TEXT    NOT NULL,
                multiplier  REAL    NOT NULL DEFAULT 1.0,
                prev_mult   REAL    NOT NULL DEFAULT 1.0,
                momentum    REAL    NOT NULL DEFAULT 0.0,
                confidence  REAL    NOT NULL DEFAULT 0.0,
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

    # ================================================================== #
    #  Main weight update
    # ================================================================== #

    def update_weights(self, cooccurrence: dict = None) -> int:
        """
        Update keyword multipliers.
        cooccurrence: optional dict from FeedbackEngine.get_cooccurrence()
        Returns count of adjusted keywords.
        """
        alpha = self._schedule.current_alpha() if self._schedule else STABLE_ALPHA

        # Build feedback map from signal_feedback table
        feedback_map = self._build_feedback_map()

        # Merge with cooccurrence data if provided
        if cooccurrence:
            for (dept, kw), (co_conf, co_fp) in cooccurrence.items():
                key = (dept.lower(), kw.lower())
                existing = feedback_map.get(key, [0, 0])
                feedback_map[key] = [existing[0] + co_conf, existing[1] + co_fp]

        updated = 0
        for dept, data in TAXONOMY.items():
            all_kws = [(kw, False) for kw in data.get("keywords", [])] + \
                      [(ph, True)  for ph in data.get("phrases", [])]

            for kw, is_phrase in all_kws:
                key = (dept, kw.lower())
                confirmed, false_pos = feedback_map.get(key, (0, 0))

                # Weight confirmed feedback by recency (already embedded in feedback_map)
                total = confirmed + false_pos

                # Load current state
                row = self._db.conn.execute("""
                    SELECT multiplier, prev_mult, momentum, samples
                    FROM keyword_weights WHERE department=? AND keyword=?
                """, (dept, kw.lower())).fetchone()

                prev_mult    = row[0] if row else 1.0
                prev_prev    = row[1] if row else 1.0
                prev_samples = row[3] if row else 0

                new_samples = prev_samples + total

                if total < MIN_SAMPLES:
                    if total > 0:
                        self._upsert(dept, kw.lower(), prev_mult, prev_mult, 0.0,
                                     _confidence(new_samples), new_samples)
                    continue

                hit_rate = confirmed / total

                # Phrase bonus: phrases are inherently more specific
                phrase_floor = 0.5 if is_phrase else 0.2
                target_mult  = phrase_floor + hit_rate * (MAX_MULT - phrase_floor)

                # Detect oscillation: if weight has been bouncing, dampen
                oscillating = (prev_mult - prev_prev) * (target_mult - prev_mult) < 0
                effective_alpha = alpha * (0.5 if oscillating else 1.0)

                # Recency-weighted EMA
                new_mult = effective_alpha * target_mult + (1 - effective_alpha) * prev_mult
                new_mult = round(max(MIN_MULT, min(MAX_MULT, new_mult)), 4)

                # Momentum = how much we moved this cycle
                momentum = round(new_mult - prev_mult, 4)

                conf = _confidence(new_samples)
                self._upsert(dept, kw.lower(), new_mult, prev_mult, momentum, conf, new_samples)
                updated += 1

        logger.info(f"[LearnerV2] Updated {updated} keyword weights (alpha={alpha})")
        return updated

    # ================================================================== #
    #  New keyword discovery
    # ================================================================== #

    def discover_new_keywords(self) -> int:
        """
        Mine confirmed signal bodies for recurring n-grams not yet in taxonomy.
        Also mines cross-department: if a keyword fires strongly in dept X but
        we never assigned it there, flag it as a candidate.
        """
        cur = self._db.conn.execute("""
            SELECT s.body, s.department
            FROM signals s
            JOIN signal_feedback f ON f.signal_id = s.id
            WHERE f.outcome = 'confirmed'
              AND s.body IS NOT NULL AND s.body != ''
        """)
        rows = cur.fetchall()

        dept_ngrams: dict[str, Counter] = defaultdict(Counter)
        for body, dept in rows:
            if not dept:
                continue
            for n in (2, 3):
                dept_ngrams[dept].update(_ngrams(body, n))

        existing = set()
        for data in TAXONOMY.values():
            for kw in data.get("keywords", []) + data.get("phrases", []):
                existing.add(kw.lower())

        discovered = 0
        for dept, counter in dept_ngrams.items():
            for ngram, freq in counter.most_common(50):
                if ngram in existing or len(ngram) < 5:
                    continue
                if freq >= DISCOVERY_MIN:
                    try:
                        self._db.conn.execute("""
                            INSERT INTO keyword_candidates (department, ngram, frequency)
                            VALUES (?,?,?)
                            ON CONFLICT(department, ngram)
                            DO UPDATE SET frequency = MAX(frequency, excluded.frequency + 1)
                        """, (dept, ngram, freq))
                        discovered += 1
                    except Exception:
                        pass
        try:
            self._db.conn.commit()
        except Exception:
            pass
        logger.info(f"[LearnerV2] Discovered {discovered} candidate keywords")
        return discovered

    # ================================================================== #
    #  Cross-department interference
    # ================================================================== #

    def penalise_cross_dept_noise(self) -> int:
        """
        If a keyword fires in dept X but the matched signals consistently end up
        as false positives ONLY in dept X while being confirmed in dept Y,
        reduce its weight in dept X by 20% per cycle.
        """
        try:
            cur = self._db.conn.execute("""
                SELECT kc.department, kc.keyword,
                       kc.false_pos AS fp,
                       kc.confirmed AS conf
                FROM keyword_cooccurrence kc
                WHERE kc.false_pos > kc.confirmed * 2 AND kc.false_pos >= 5
            """)
        except Exception as e:
            logger.debug(f"penalise_cross_dept_noise query skipped: {e}")
            return 0

        penalised = 0
        for dept, kw, fp, conf in cur.fetchall():
            row = self._db.conn.execute(
                "SELECT multiplier FROM keyword_weights WHERE department=? AND keyword=?",
                (dept, kw)
            ).fetchone()
            if row:
                new_mult = max(MIN_MULT, round(row[0] * 0.80, 4))
                self._db.conn.execute(
                    "UPDATE keyword_weights SET multiplier=?, updated_at=datetime('now') WHERE department=? AND keyword=?",
                    (new_mult, dept, kw)
                )
                penalised += 1
        try:
            self._db.conn.commit()
        except Exception:
            pass
        if penalised:
            logger.info(f"[LearnerV2] Cross-dept penalised {penalised} keyword/dept pairs")
        return penalised

    # ================================================================== #
    #  Helpers
    # ================================================================== #

    def _build_feedback_map(self) -> dict:
        """
        Returns {(dept, kw_lower): (weighted_confirmed, weighted_false_pos)}.
        Weights each feedback row by its recency_weight column.
        """
        cur = self._db.conn.execute("""
            SELECT department, matched_keywords, outcome, recency_weight
            FROM signal_feedback
            WHERE matched_keywords IS NOT NULL AND matched_keywords != ''
        """)
        result: dict = defaultdict(lambda: [0.0, 0.0])
        for dept, kws_str, outcome, rw in cur.fetchall():
            w = float(rw or 1.0)
            for kw in kws_str.split(","):
                kw = kw.strip().lower()
                if not kw or not dept:
                    continue
                key = (dept, kw)
                if outcome == "confirmed":
                    result[key][0] += w
                elif outcome == "false_positive":
                    result[key][1] += w
        return {k: (round(v[0]), round(v[1])) for k, v in result.items()}

    def _upsert(self, dept, kw, mult, prev_mult, momentum, confidence, samples):
        try:
            self._db.conn.execute("""
                INSERT INTO keyword_weights
                  (department, keyword, multiplier, prev_mult, momentum, confidence, samples)
                VALUES (?,?,?,?,?,?,?)
                ON CONFLICT(department, keyword)
                DO UPDATE SET multiplier =excluded.multiplier,
                              prev_mult  =excluded.prev_mult,
                              momentum   =excluded.momentum,
                              confidence =excluded.confidence,
                              samples    =excluded.samples,
                              updated_at =datetime('now')
            """, (dept, kw, mult, prev_mult, momentum, confidence, samples))
            self._db.conn.commit()
        except Exception as e:
            logger.error(f"Upsert error for ({dept},{kw}): {e}")
