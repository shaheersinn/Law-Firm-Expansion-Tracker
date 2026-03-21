"""
ml/feedback_loop.py
─────────────────────
The Self-Learning Feedback Loop — The System Gets Smarter Every Week

This is what separates a static rules engine from an adaptive intelligence system.

Every time you mark an outreach as:
  - "got a reply"         → positive signal
  - "got an interview"    → strong positive
  - "rejected / no reply" → negative signal
  - "position filled"     → neutral

The system:
  1. Records the outcome against the signal that triggered the outreach
  2. Computes which signal TYPES have the highest reply/interview conversion rates
  3. Reweights SIGNAL_WEIGHTS in config based on observed conversion rates
  4. Adjusts the predictive model's feature weights using Bayesian updating
  5. Sends a weekly "what's working" Telegram report

Over time, if SEDI insider clusters at mid-size firms consistently produce
interviews but Glassdoor signals produce nothing — the model learns this and
stops alerting on Glassdoor while boosting SEDI weight.

Commands:
  python main_v5.py --outcome "signal_id=42 result=interview"
  python main_v5.py --outcome "firm_id=burnet result=reply"
  python main_v5.py --train           # retrain weights from all outcomes
  python main_v5.py --what-works      # print conversion rate report
"""

import json, logging, math
from datetime import date, datetime, timedelta
from collections import defaultdict
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import numpy as np

from database.db import get_conn
from alerts.notifier import send_telegram

log = logging.getLogger(__name__)

# Outcome values for Bayesian updating
OUTCOME_VALUES = {
    "interview":   +3.0,
    "reply":       +1.5,
    "no_reply":    -0.5,
    "rejected":    -1.0,
    "filled":       0.0,
    "irrelevant":  -1.5,
}

# Starting prior: all signal types equally weighted at 1.0
SIGNAL_TYPES_ALL = [
    "canlii_appearance_spike", "canlii_new_large_file",
    "sedar_major_deal", "sedar_counsel_named",
    "linkedin_turnover_detected", "lsa_retention_gap",
    "biglaw_spillage_predicted", "gravity_spillage_predicted",
    "breaking_deal_announcement", "breaking_ccaa_filing",
    "asc_enforcement_emergency", "tsxv_rto_announced",
    "partner_appearance_spike", "judicial_appointment_void",
    "registry_deal_structure", "sedi_insider_cluster",
    "sec_edgar_filing", "fiscal_pressure_incoming",
    "macro_ma_wave_incoming", "aer_proceeding_upcoming",
    "glassdoor_overwork_spike", "teampage_departure_detected",
    "placement_under_match",
]


def _init_ml_db():
    conn = get_conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS outreach_outcomes (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id      INTEGER,
            firm_id        TEXT NOT NULL,
            signal_type    TEXT,
            practice_area  TEXT,
            firm_tier      TEXT,
            outcome        TEXT NOT NULL,
            outcome_value  REAL,
            recorded_at    TEXT DEFAULT (date('now')),
            notes          TEXT
        )""")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS learned_weights (
            signal_type    TEXT NOT NULL,
            base_weight    REAL NOT NULL DEFAULT 1.0,
            learned_weight REAL NOT NULL DEFAULT 1.0,
            n_outcomes     INTEGER DEFAULT 0,
            n_positive     INTEGER DEFAULT 0,
            conversion_rate REAL DEFAULT 0.0,
            last_updated   TEXT DEFAULT (date('now')),
            PRIMARY KEY (signal_type)
        )""")
    # Seed initial weights
    for st in SIGNAL_TYPES_ALL:
        conn.execute("""
            INSERT OR IGNORE INTO learned_weights
                (signal_type, base_weight, learned_weight)
            VALUES (?, 1.0, 1.0)
        """, (st,))
    conn.commit()
    conn.close()


def record_outcome(firm_id: str, outcome: str,
                   signal_id: int | None = None,
                   signal_type: str | None = None,
                   notes: str = ""):
    """
    Record a real-world outcome against a signal or firm.
    Called by the user via CLI or Telegram bot command.
    """
    _init_ml_db()

    # Look up signal if not provided
    if signal_id and not signal_type:
        conn = get_conn()
        row  = conn.execute(
            "SELECT signal_type, practice_area, firm_id FROM signals WHERE id=?",
            (signal_id,)
        ).fetchone()
        conn.close()
        if row:
            signal_type   = row["signal_type"]
            firm_id       = row["firm_id"]

    # Lookup firm tier
    from config_calgary import FIRM_BY_ID
    firm      = FIRM_BY_ID.get(firm_id, {})
    firm_tier = firm.get("tier", "?")

    outcome_val = OUTCOME_VALUES.get(outcome, 0.0)

    conn = get_conn()
    conn.execute("""
        INSERT INTO outreach_outcomes
            (signal_id, firm_id, signal_type, firm_tier, outcome, outcome_value, notes)
        VALUES (?,?,?,?,?,?,?)
    """, (signal_id, firm_id, signal_type, firm_tier,
          outcome, outcome_val, notes))
    conn.commit()
    conn.close()

    log.info("[Feedback] Recorded outcome=%s for firm=%s signal=%s", outcome, firm_id, signal_type)

    # Immediately retrain weights
    retrain_weights()


def retrain_weights():
    """
    Bayesian weight update:
    learned_weight = base_weight × (1 + smoothed_conversion_lift)

    conversion_lift = (observed_conversion_rate - prior_rate) / prior_rate

    Uses Laplace smoothing with α=1 (add-one smoothing) to avoid
    overfitting on small samples.
    """
    _init_ml_db()
    conn = get_conn()

    # Get all outcomes grouped by signal_type
    rows = conn.execute("""
        SELECT signal_type,
               count(*) as n_total,
               sum(CASE WHEN outcome_value > 0 THEN 1 ELSE 0 END) as n_positive,
               avg(outcome_value) as avg_value
        FROM outreach_outcomes
        WHERE signal_type IS NOT NULL
        GROUP BY signal_type
    """).fetchall()

    prior_rate = 0.20   # assume 20% baseline conversion without signal intelligence

    for row in rows:
        st       = row["signal_type"]
        n        = row["n_total"]
        n_pos    = row["n_positive"]
        avg_val  = row["avg_value"] or 0.0

        # Laplace-smoothed conversion rate
        smoothed_rate = (n_pos + 1) / (n + 2)

        # Lift over prior
        lift = (smoothed_rate - prior_rate) / prior_rate

        # Bayesian weight update: bounded between 0.2× and 3× base
        base_w   = _get_base_weight(st)
        new_w    = max(base_w * 0.2, min(base_w * 3.0, base_w * (1 + lift)))

        conn.execute("""
            UPDATE learned_weights SET
                learned_weight = ?,
                n_outcomes = ?,
                n_positive = ?,
                conversion_rate = ?,
                last_updated = date('now')
            WHERE signal_type = ?
        """, (round(new_w, 3), n, n_pos, round(smoothed_rate, 4), st))

        log.info("[Feedback] %-35s  base=%.1f  learned=%.2f  CR=%.0f%%  n=%d",
                 st, base_w, new_w, smoothed_rate*100, n)

    conn.commit()
    conn.close()

    # Write updated weights to file for config import
    _export_learned_weights()


def _get_base_weight(signal_type: str) -> float:
    """Get the original base weight for a signal type."""
    from config_calgary import SIGNAL_WEIGHTS
    return SIGNAL_WEIGHTS.get(signal_type, 1.0)


def get_effective_weight(signal_type: str) -> float:
    """Returns the current learned weight for a signal type."""
    _init_ml_db()
    conn = get_conn()
    row  = conn.execute(
        "SELECT learned_weight FROM learned_weights WHERE signal_type=?",
        (signal_type,)
    ).fetchone()
    conn.close()
    if row:
        return float(row["learned_weight"])
    return _get_base_weight(signal_type)


def _export_learned_weights():
    """Write learned weights to reports/learned_weights.json for inspection."""
    import pathlib
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM learned_weights ORDER BY learned_weight DESC"
    ).fetchall()
    conn.close()
    data = [dict(r) for r in rows]
    pathlib.Path("reports").mkdir(exist_ok=True)
    with open("reports/learned_weights.json", "w") as f:
        json.dump(data, f, indent=2)


def conversion_rate_report() -> str:
    """
    Returns a formatted string showing which signals are producing results.
    Sent as weekly Telegram report.
    """
    _init_ml_db()
    conn = get_conn()
    rows = conn.execute("""
        SELECT signal_type, n_outcomes, n_positive,
               conversion_rate, learned_weight
        FROM learned_weights
        WHERE n_outcomes > 0
        ORDER BY conversion_rate DESC
    """).fetchall()
    conn.close()

    if not rows:
        return "No outcome data yet. Use --outcome to record results."

    lines = ["📊 <b>SIGNAL CONVERSION RATES</b> (learned weights)\n"]
    for r in rows:
        bar = "█" * int(r["conversion_rate"] * 10) + "░" * (10 - int(r["conversion_rate"] * 10))
        lines.append(
            f"<code>{r['signal_type'][:32]:<32}</code>  "
            f"CR={r['conversion_rate']:.0%}  "
            f"w={r['learned_weight']:.2f}  "
            f"n={r['n_outcomes']}"
        )

    lines.append(f"\nTotal outcomes recorded: {sum(r['n_outcomes'] for r in rows)}")
    return "\n".join(lines)


def weekly_learning_report():
    """Send weekly Telegram report on what's working."""
    report = conversion_rate_report()
    send_telegram(report)
    log.info("[Feedback] Weekly learning report sent.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    _init_ml_db()
    print(conversion_rate_report())
