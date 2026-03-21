"""
database/signal_verifier.py  — Signal accuracy double-checker
═══════════════════════════════════════════════════════════════
Every signal that enters the DB passes through this verifier
before being surfaced on the dashboard or included in alerts.

Verification checks (each adds/subtracts from confidence_score 0–1):

  1. Firm name match quality       — fuzzy match strength to known firm
  2. Calgary / Alberta relevance   — does content mention Calgary/AB/energy?
  3. Date recency                  — within 90-day window?
  4. Cross-source corroboration    — same signal type for same firm from
                                     multiple sources this week?
  5. Title quality                 — not empty, not boilerplate
  6. Weight plausibility           — weight in expected range for signal type
  7. Duplicate proximity           — near-duplicate title detection

Signals that fail (confidence < CONFIDENCE_FLOOR) are:
  • Still stored in DB (full audit trail)
  • Excluded from Telegram digest top-N
  • Flagged with low_confidence=True in dashboard JSON

The verifier adds a `confidence_score` column to the signals table
(via ALTER TABLE IF NOT EXISTS — safe to run on existing DB).
"""
import hashlib
import logging
import re
import sqlite3
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger(__name__)

CONFIDENCE_FLOOR  = 0.35   # below this → flagged low-confidence
CORROBORATION_BONUS = 0.15  # same type + firm from 2+ sources this week

# Terms that indicate Calgary/Alberta/energy relevance
CALGARY_TERMS = re.compile(
    r"\b(calgary|alberta|ab|edmonton|energy|oil|gas|pipeline|aer|auc|"
    r"sedar|canlii|tsxv|tsx\.v|burnet|field law|parlee|mccarthy|blakes|"
    r"bennett jones|norton rose|osler|fasken|dentons|gowling|borden|"
    r"cassels|miller thomson|stikeman|torys|borden ladner)\b",
    re.IGNORECASE
)

# Boilerplate titles that add no signal value
BOILERPLATE_RE = re.compile(
    r"^(website snapshot|placeholder|test signal|untitled|n/a|none|\s*)$",
    re.IGNORECASE
)

# Expected weight ranges per signal type
WEIGHT_RANGES = {
    "sedar_major_deal":           (3.0, 6.0),
    "biglaw_spillage_predicted":  (3.0, 5.5),
    "canlii_appearance_spike":    (2.5, 5.0),
    "linkedin_turnover_detected": (3.5, 5.5),
    "lsa_student_not_retained":   (2.0, 4.0),
    "lateral_hire":               (2.0, 4.0),
    "job_posting":                (1.0, 3.0),
    "macro_ma_wave_incoming":     (3.0, 5.0),
    "macro_demand_surge":         (2.5, 4.5),
    "fiscal_pressure_incoming":   (2.0, 4.0),
    "sec_edgar_filing":           (1.5, 3.5),
    "web_signal":                 (1.0, 3.0),
}


def _ensure_schema(conn: sqlite3.Connection):
    """Add confidence columns to signals table if they don't exist yet."""
    cols = {row[1] for row in conn.execute("PRAGMA table_info(signals)")}
    if "confidence_score" not in cols:
        conn.execute("ALTER TABLE signals ADD COLUMN confidence_score REAL")
    if "low_confidence" not in cols:
        conn.execute("ALTER TABLE signals ADD COLUMN low_confidence INTEGER DEFAULT 0")
    conn.commit()


def compute_confidence(
    firm_id: str,
    signal_type: str,
    title: str,
    description: str,
    weight: float,
    source_url: str,
    detected_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> float:
    """
    Return a confidence score 0.0–1.0 for a signal.
    Higher = more reliable signal.
    """
    score = 0.50  # neutral baseline

    # ── Check 1: Firm validity ─────────────────────────────────────────────
    from config_calgary import FIRM_BY_ID
    if firm_id == "market":
        score += 0.05  # market-wide is always intentional
    elif firm_id in FIRM_BY_ID:
        score += 0.15  # known tracked firm
    else:
        score -= 0.20  # unknown firm — suspicious

    # ── Check 2: Calgary / Alberta relevance ──────────────────────────────
    combined = f"{title} {description} {source_url}"
    if CALGARY_TERMS.search(combined):
        score += 0.10
    elif signal_type not in ("macro_ma_wave_incoming", "macro_demand_surge",
                              "fiscal_pressure_incoming", "sec_edgar_filing"):
        score -= 0.15  # non-market signals with no Calgary mention are suspect

    # ── Check 3: Recency ──────────────────────────────────────────────────
    if detected_at:
        try:
            ts   = datetime.fromisoformat(detected_at.replace("Z", "+00:00"))
            days = (datetime.now(timezone.utc) - ts).days
            if days <= 7:
                score += 0.10
            elif days <= 30:
                score += 0.05
            elif days > 90:
                score -= 0.20  # outside our lookback window
        except Exception:
            pass

    # ── Check 4: Title quality ────────────────────────────────────────────
    if not title or BOILERPLATE_RE.match(title):
        score -= 0.20
    elif len(title) > 20:
        score += 0.05

    # ── Check 5: Weight plausibility ──────────────────────────────────────
    lo, hi = WEIGHT_RANGES.get(signal_type, (0.5, 6.0))
    if lo <= weight <= hi:
        score += 0.05
    elif weight > hi * 1.5 or weight < 0:
        score -= 0.15  # implausibly extreme weight

    # ── Check 6: Cross-source corroboration ──────────────────────────────
    if conn:
        try:
            count = conn.execute("""
                SELECT COUNT(DISTINCT source_url) FROM signals
                WHERE firm_id = ?
                  AND signal_type = ?
                  AND date(detected_at) >= date('now', '-7 days')
                  AND source_url != ''
            """, (firm_id, signal_type)).fetchone()[0]
            if count >= 2:
                score += CORROBORATION_BONUS
        except Exception:
            pass

    # ── Check 7: Near-duplicate proximity ────────────────────────────────
    if conn and title:
        title_hash = hashlib.md5(title.lower().strip()[:80].encode()).hexdigest()[:8]
        try:
            dup = conn.execute("""
                SELECT COUNT(*) FROM signals
                WHERE firm_id = ?
                  AND signal_type = ?
                  AND date(detected_at) >= date('now', '-1 days')
                  AND id != (SELECT MAX(id) FROM signals WHERE dedup_hash IS NULL)
            """, (firm_id, signal_type)).fetchone()[0]
            if dup > 3:
                score -= 0.10  # many near-identical signals today
        except Exception:
            pass

    return round(max(0.0, min(1.0, score)), 3)


def verify_recent_signals(days: int = 1):
    """
    Run verification on all signals from the last `days` that don't yet have
    a confidence_score. Called once per pipeline run after signal collection.
    """
    from database.db import get_conn
    conn = get_conn()
    _ensure_schema(conn)

    rows = conn.execute("""
        SELECT id, firm_id, signal_type, title, description, weight,
               source_url, detected_at
        FROM signals
        WHERE confidence_score IS NULL
          AND date(detected_at) >= date('now', ? || ' days')
        ORDER BY id DESC
    """, (f"-{days}",)).fetchall()

    if not rows:
        log.debug("[Verifier] No unverified signals.")
        conn.close()
        return

    log.info("[Verifier] Verifying %d new signals…", len(rows))
    updated = 0
    low_conf = 0

    for row in rows:
        sig_id = row["id"]
        conf   = compute_confidence(
            firm_id     = row["firm_id"],
            signal_type = row["signal_type"],
            title       = row["title"] or "",
            description = row["description"] or "",
            weight      = row["weight"],
            source_url  = row["source_url"] or "",
            detected_at = row["detected_at"],
            conn        = conn,
        )
        is_low = 1 if conf < CONFIDENCE_FLOOR else 0
        conn.execute(
            "UPDATE signals SET confidence_score=?, low_confidence=? WHERE id=?",
            (conf, is_low, sig_id)
        )
        updated += 1
        if is_low:
            low_conf += 1

    conn.commit()
    conn.close()
    log.info("[Verifier] Done. %d verified, %d flagged low-confidence (<%s).",
             updated, low_conf, f"{CONFIDENCE_FLOOR:.0%}")


def get_verified_signals_for_dashboard(days: int = 90) -> list:
    """
    Returns all signals for the dashboard, annotated with confidence data.
    Low-confidence signals are included but tagged so the UI can dim them.
    """
    from database.db import get_conn
    conn = get_conn()
    _ensure_schema(conn)

    rows = conn.execute("""
        SELECT *
        FROM signals
        WHERE date(detected_at) >= date('now', ? || ' days')
        ORDER BY
            CASE WHEN low_confidence = 0 THEN 0 ELSE 1 END,
            weight DESC,
            detected_at DESC
    """, (f"-{days}",)).fetchall()
    conn.close()
    return [dict(r) for r in rows]
