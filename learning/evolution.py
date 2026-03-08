"""
Evolution module — self-learning cycle (runs every 2 hours via GitHub Actions).

BUG-2 FIX: dept_weight_multipliers are now READ by analysis/signals.py — the
           loop is: collect → score with multipliers → evolve → update multipliers
           → next collect uses updated multipliers. Model actually evolves.

BUG-3 FIX: Evolution generates its own synthetic feedback from signal patterns
           without requiring any human input. Four rules:

  RULE 1 — VELOCITY: same dept active for 2+ consecutive weeks = confirmed
  RULE 2 — ISOLATION: only one active week then 3+ weeks silent = noise
  RULE 3 — CORROBORATION: 3+ distinct signal types in one week = high confidence
  RULE 4 — GROWTH: signal count grows ≥20% week-over-week = confirmed trend

  In addition, evolution runs keyword learning: extracts keywords from
  high-scoring signals and promotes them into the department classifier.
"""

import json
import logging
import re
from datetime import datetime, timezone, timedelta
from collections import defaultdict

from config import Config
from database.db import Database

logger = logging.getLogger("evolution")

WEIGHT_FLOOR   = 0.5
WEIGHT_CEILING = 6.0
BOOST_FACTOR   = 1.12   # +12% per cycle when confirmed
DAMPEN_FACTOR  = 0.88   # -12% per cycle when noisy
HIGH_ACCURACY  = 0.68
LOW_ACCURACY   = 0.40
MIN_SAMPLES    = 3      # minimum feedback records before adjusting


def run_evolution():
    config = Config()
    db     = Database(config.DB_PATH)

    logger.info("=" * 60)
    logger.info(f"Evolution cycle — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    logger.info("=" * 60)

    # Step 1: generate synthetic feedback from observable signal patterns
    inserted = _generate_synthetic_feedback(db)
    logger.info(f"Synthetic feedback generated: {inserted} new records")

    # Step 2: compute accuracy per department
    accuracy = db.get_accuracy_by_department()

    if not accuracy:
        logger.info("No feedback data yet — skipping weight adjustment")
        _log_stats(db)
        db.close()
        return

    logger.info(f"Departments with feedback data: {len(accuracy)}")
    for dept, acc in sorted(accuracy.items(), key=lambda x: -x[1]):
        n = _count_samples(db, dept)
        logger.info(f"  {dept:<35} acc={acc:.0%}  n={n}")

    # Step 3: compute and apply adjustments
    adjustments = {}
    for dept, acc in accuracy.items():
        n = _count_samples(db, dept)
        if n < MIN_SAMPLES:
            continue
        if acc >= HIGH_ACCURACY:
            adjustments[dept] = ("boost",  BOOST_FACTOR,  acc)
        elif acc < LOW_ACCURACY:
            adjustments[dept] = ("dampen", DAMPEN_FACTOR, acc)

    if not adjustments:
        logger.info("All departments in normal range — no weight changes")
    else:
        for dept, (action, factor, acc) in adjustments.items():
            logger.info(f"  {action.upper():6} {dept:<35} acc={acc:.0%} x{factor}")
        _apply_adjustments(db, adjustments)

    # Step 4: keyword learning — promote top keywords from high-score signals
    learned = _learn_keywords(db)
    logger.info(f"Keyword learning: {learned} new keyword associations stored")

    # Step 5: prune stale signals
    pruned = _prune_stale(db)
    if pruned:
        logger.info(f"Pruned {pruned} signals older than 90 days")

    logger.info(f"Evolution complete — {len(adjustments)} departments adjusted")
    db.close()


# ── Synthetic feedback ────────────────────────────────────────────────────

def _generate_synthetic_feedback(db: Database) -> int:
    _ensure_schema(db)
    cutoff  = (datetime.now(timezone.utc) - timedelta(weeks=8)).isoformat()
    signals = db.conn.execute("""
        SELECT firm_id, department, signal_type, collected_at
        FROM signals WHERE collected_at >= ?
        ORDER BY firm_id, department, collected_at
    """, (cutoff,)).fetchall()

    if not signals:
        return 0

    # Group by (firm, dept) → { week → [signal_types] }
    by_fd: dict = defaultdict(lambda: defaultdict(list))
    for s in signals:
        week = _iso_week(s["collected_at"])
        by_fd[(s["firm_id"], s["department"])][week].append(s["signal_type"])

    now     = datetime.now(timezone.utc)
    inserted = 0

    for (firm_id, dept), weekly in by_fd.items():
        weeks  = sorted(weekly.keys())
        scores = [(w, len(sigs), len(set(sigs))) for w, sigs in weekly.items()]

        # RULE 1 — velocity: 2+ consecutive active weeks
        for i in range(len(weeks) - 1):
            w1, w2 = weeks[i], weeks[i + 1]
            if _weeks_apart(w1, w2) == 1:
                if not _fb_exists(db, firm_id, dept, f"velocity:{w2}"):
                    db.conn.execute(
                        "INSERT INTO signal_feedback "
                        "(firm_id,department,was_correct,notes,created_at) VALUES(?,?,1,?,?)",
                        (firm_id, dept, f"synthetic:velocity:{w2}", now.isoformat())
                    )
                    inserted += 1

        # RULE 2 — isolation: only one active week, then ≥3 weeks silent
        if len(weeks) == 1 and _weeks_ago(weeks[0]) >= 3:
            if not _fb_exists(db, firm_id, dept, f"isolation:{weeks[0]}"):
                db.conn.execute(
                    "INSERT INTO signal_feedback "
                    "(firm_id,department,was_correct,notes,created_at) VALUES(?,?,0,?,?)",
                    (firm_id, dept, f"synthetic:isolation:{weeks[0]}", now.isoformat())
                )
                inserted += 1

        # RULE 3 — corroboration: ≥3 distinct signal types in one week
        for w, count, n_types in scores:
            if n_types >= 3:
                if not _fb_exists(db, firm_id, dept, f"corroboration:{w}"):
                    db.conn.execute(
                        "INSERT INTO signal_feedback "
                        "(firm_id,department,was_correct,notes,created_at) VALUES(?,?,1,?,?)",
                        (firm_id, dept, f"synthetic:corroboration:{w}", now.isoformat())
                    )
                    inserted += 1

        # RULE 4 — growth: latest week has ≥20% more signals than previous
        if len(scores) >= 2:
            prev_n = scores[-2][1]
            last_n = scores[-1][1]
            last_w = scores[-1][0]
            if prev_n > 0 and last_n / prev_n >= 1.2:
                if not _fb_exists(db, firm_id, dept, f"growth:{last_w}"):
                    db.conn.execute(
                        "INSERT INTO signal_feedback "
                        "(firm_id,department,was_correct,notes,created_at) VALUES(?,?,1,?,?)",
                        (firm_id, dept, f"synthetic:growth:{last_w}", now.isoformat())
                    )
                    inserted += 1

    db.conn.commit()
    return inserted


# ── Weight application ────────────────────────────────────────────────────

def _apply_adjustments(db: Database, adjustments: dict):
    """BUG-2 FIX: writes to dept_weight_multipliers — read by analysis/signals.py."""
    db.conn.execute("""
        CREATE TABLE IF NOT EXISTS dept_weight_multipliers (
            department TEXT PRIMARY KEY,
            multiplier REAL NOT NULL DEFAULT 1.0,
            updated_at TEXT NOT NULL
        )
    """)
    db.conn.commit()

    now = datetime.now(timezone.utc).isoformat()
    for dept, (action, factor, _) in adjustments.items():
        db.conn.execute("""
            INSERT INTO dept_weight_multipliers (department, multiplier, updated_at)
            VALUES (?, 1.0, ?)
            ON CONFLICT(department) DO UPDATE SET
                multiplier = MIN(MAX(multiplier * ?, ?, ?), ?),
                updated_at = excluded.updated_at
        """, (dept, now, factor, WEIGHT_FLOOR, WEIGHT_FLOOR, WEIGHT_CEILING))

    db.conn.commit()
    logger.info("dept_weight_multipliers updated in DB (used by analyzer next collect run)")


# ── Keyword learning ──────────────────────────────────────────────────────

def _learn_keywords(db: Database) -> int:
    """
    Extract high-value words from confirmed signals and store them in
    learned_keywords table. These get checked by the department classifier
    on the next run.
    """
    db.conn.execute("""
        CREATE TABLE IF NOT EXISTS learned_keywords (
            keyword    TEXT NOT NULL,
            department TEXT NOT NULL,
            weight     REAL NOT NULL DEFAULT 1.0,
            seen_count INTEGER NOT NULL DEFAULT 1,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (keyword, department)
        )
    """)
    db.conn.commit()

    # Pull titles from high-scoring signals (dept_score > 3.0)
    rows = db.conn.execute("""
        SELECT department, title, body
        FROM signals
        WHERE dept_score >= 3.0
          AND collected_at >= ?
    """, ((datetime.now(timezone.utc) - timedelta(days=30)).isoformat(),)).fetchall()

    stop_words = {
        "the","a","an","and","or","of","to","in","for","with","on","at","by",
        "from","is","was","are","were","has","have","had","be","been","this",
        "that","its","it","as","we","our","their","will","can","not","but",
        "into","over","after","new","new","llp","law","firm","canada","canadian"
    }

    now = datetime.now(timezone.utc).isoformat()
    count = 0
    for row in rows:
        dept  = row["department"]
        text  = f"{row['title']} {row['body'] or ''}"
        words = re.findall(r"\b[a-z]{4,}\b", text.lower())
        candidates = [w for w in words if w not in stop_words]

        # Only take words appearing ≥2 times in this signal
        freq: dict = defaultdict(int)
        for w in candidates:
            freq[w] += 1
        top = [w for w, n in freq.items() if n >= 2][:10]

        for kw in top:
            db.conn.execute("""
                INSERT INTO learned_keywords (keyword, department, weight, seen_count, updated_at)
                VALUES (?, ?, 1.0, 1, ?)
                ON CONFLICT(keyword, department) DO UPDATE SET
                    seen_count = seen_count + 1,
                    weight     = MIN(weight + 0.1, 3.0),
                    updated_at = excluded.updated_at
            """, (kw, dept, now))
            count += 1

    db.conn.commit()
    return count


# ── Maintenance ───────────────────────────────────────────────────────────

def _prune_stale(db: Database) -> int:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=90)).isoformat()
    cur    = db.conn.execute("DELETE FROM signals WHERE collected_at < ?", (cutoff,))
    db.conn.commit()
    return cur.rowcount


def _log_stats(db: Database):
    try:
        n = db.conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
        logger.info(f"Total signals in DB: {n}")
    except Exception:
        pass


# ── Helpers ───────────────────────────────────────────────────────────────

def _ensure_schema(db: Database):
    try:
        db.conn.execute(
            "ALTER TABLE signal_feedback ADD COLUMN notes TEXT"
        )
        db.conn.commit()
    except Exception:
        pass


def _fb_exists(db, firm_id, dept, tag) -> bool:
    row = db.conn.execute(
        "SELECT 1 FROM signal_feedback WHERE firm_id=? AND department=? "
        "AND notes LIKE ? LIMIT 1",
        (firm_id, dept, f"synthetic:{tag}%")
    ).fetchone()
    return row is not None


def _count_samples(db, dept) -> int:
    row = db.conn.execute(
        "SELECT COUNT(*) FROM signal_feedback WHERE department=?", (dept,)
    ).fetchone()
    return row[0] if row else 0


def _iso_week(iso_str: str) -> str:
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-W%V")
    except Exception:
        return iso_str[:7]


def _weeks_apart(w1: str, w2: str) -> int:
    try:
        d1 = datetime.strptime(w1 + "-1", "%Y-W%V-%u")
        d2 = datetime.strptime(w2 + "-1", "%Y-W%V-%u")
        return abs((d2 - d1).days) // 7
    except Exception:
        return 99


def _weeks_ago(week_str: str) -> int:
    try:
        d = datetime.strptime(week_str + "-1", "%Y-W%V-%u").replace(tzinfo=timezone.utc)
        return int((datetime.now(timezone.utc) - d).days / 7)
    except Exception:
        return 0
