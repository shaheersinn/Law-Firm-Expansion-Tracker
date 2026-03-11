"""
Signal Confidence Scorer + Firm Trajectory Tracker
====================================================
Two model enhancements that work together:

1. ConfidenceScorer
   Assigns a 0–100 confidence band to each expansion alert based on:
   - Source diversity (how many independent scrapers contributed signals)
   - Signal velocity (did signals arrive in a burst or trickle?)
   - Historical accuracy (how often did past alerts for this firm confirm?)
   - Cross-signal corroboration (do different signal types agree?)
   - Recency weighting (recent signals count more)

2. FirmTrajectoryTracker
   Maintains a rolling 8-week window of weekly scores per firm.
   Detects:
   - ACCELERATING: score growing week-over-week (3+ consecutive increases)
   - DECELERATING: score falling after a peak (possible false alarm)
   - BREAKOUT: score this week exceeds the 8-week 90th percentile
   - SUSTAINED: high score for 3+ consecutive weeks (very reliable signal)
   - QUIET: no signals for 2+ weeks (firm may be in planning phase)

These feed into the Notifier to add context to Telegram alerts.
"""

import logging
from datetime import datetime, timezone, timedelta

logger = logging.getLogger("learning.confidence")


class ConfidenceScorer:
    """
    Score each expansion alert with a 0–100 confidence value.
    Higher = more likely a true expansion event.
    """

    # How many independent scraper types support this alert?
    SOURCE_DIVERSITY_WEIGHTS = {
        1: 0,    # single source = low confidence
        2: 20,
        3: 35,
        4: 50,
        5: 60,
        6: 70,
    }

    # Signal type reliability (from historical feedback data)
    TYPE_BASE_RELIABILITY = {
        "lateral_hire":    0.90,
        "bar_leadership":  0.85,
        "ranking":         0.80,
        "practice_page":   0.75,
        "job_posting":     0.70,
        "press_release":   0.65,
        "publication":     0.55,
        "recruit_posting": 0.60,
        "court_record":    0.75,
        "website_snapshot":0.30,
        "bar_speaking":    0.65,
        "bar_sponsorship": 0.60,
    }

    def __init__(self, db):
        self._db = db

    def score_alert(self, alert: dict, contributing_signals: list[dict]) -> dict:
        """
        Return enriched alert dict with confidence_score (0-100) and confidence_band.
        contributing_signals: list of signals that contributed to this alert.
        """
        score = 0.0

        # ── Factor 1: Source diversity (0–70 pts) ─────────────────────
        unique_types = {s["signal_type"] for s in contributing_signals}
        diversity_pts = self.SOURCE_DIVERSITY_WEIGHTS.get(
            min(len(unique_types), 6), 70
        )
        score += diversity_pts

        # ── Factor 2: Average signal type reliability (0–20 pts) ──────
        if contributing_signals:
            avg_reliability = sum(
                self.TYPE_BASE_RELIABILITY.get(s["signal_type"], 0.5)
                for s in contributing_signals
            ) / len(contributing_signals)
            score += avg_reliability * 20

        # ── Factor 3: Historical accuracy for this firm (−10 to +10) ──
        firm_id = alert["firm_id"]
        hist_pts = self._firm_history_pts(firm_id)
        score += hist_pts

        # ── Factor 4: Signal velocity burst bonus (0–10 pts) ──────────
        # Signals clustering in last 48h = correlated event
        recent_48h = sum(1 for s in contributing_signals if self._is_recent_48h(s))
        velocity_pts = min(recent_48h * 2, 10)
        score += velocity_pts

        # ── Clamp and classify ─────────────────────────────────────────
        confidence = int(min(max(score, 0), 100))
        band = self._band(confidence)

        return {
            **alert,
            "confidence_score": confidence,
            "confidence_band": band,
            "contributing_types": sorted(unique_types),
            "source_count": len(unique_types),
        }

    def _firm_history_pts(self, firm_id: str) -> float:
        """Return −10 to +10 based on past alert confirmation rate for this firm."""
        try:
            cur = self._db.conn.execute("""
                SELECT
                  SUM(CASE WHEN outcome='confirmed'      THEN 1 ELSE 0 END) AS c,
                  SUM(CASE WHEN outcome='false_positive' THEN 1 ELSE 0 END) AS fp
                FROM signal_feedback sf
                JOIN signals s ON s.id = sf.signal_id
                WHERE s.firm_id = ?
            """, (firm_id,))
            row = cur.fetchone()
            if not row or (row["c"] + row["fp"]) < 5:
                return 0.0   # insufficient history
            rate = row["c"] / (row["c"] + row["fp"])
            return (rate - 0.5) * 20   # maps 0%→-10, 50%→0, 100%→+10
        except Exception:
            return 0.0

    def _is_recent_48h(self, signal: dict) -> bool:
        try:
            seen = signal.get("seen_at", "")
            if not seen:
                return False
            dt = datetime.fromisoformat(seen.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return (datetime.now(timezone.utc) - dt).total_seconds() < 48 * 3600
        except Exception:
            return False

    @staticmethod
    def _band(score: int) -> str:
        if score >= 80:
            return "HIGH"
        elif score >= 55:
            return "MEDIUM"
        else:
            return "LOW"


class FirmTrajectoryTracker:
    """
    Track weekly expansion scores over rolling 8-week window.
    Detect trend patterns: ACCELERATING / SUSTAINED / BREAKOUT / DECELERATING.
    """

    def __init__(self, db):
        self._db = db
        self._ensure_table()

    def _ensure_table(self):
        self._db.conn.executescript("""
            CREATE TABLE IF NOT EXISTS firm_trajectory (
                firm_id       TEXT NOT NULL,
                week_start    TEXT NOT NULL,
                weekly_score  REAL DEFAULT 0,
                alert_count   INTEGER DEFAULT 0,
                PRIMARY KEY (firm_id, week_start)
            );
        """)
        self._db.conn.commit()

    def update_week(self, firm_id: str, score: float, alert_count: int = 1):
        """Record this week's score for the firm."""
        week_start = self._week_start()
        self._db.conn.execute("""
            INSERT INTO firm_trajectory (firm_id, week_start, weekly_score, alert_count)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (firm_id, week_start) DO UPDATE SET
                weekly_score = MAX(weekly_score, excluded.weekly_score),
                alert_count  = alert_count + excluded.alert_count
        """, (firm_id, week_start, score, alert_count))
        self._db.conn.commit()

    def get_trajectory(self, firm_id: str) -> dict:
        """Return trajectory label and supporting data for a firm."""
        rows = self._db.conn.execute("""
            SELECT week_start, weekly_score, alert_count
            FROM firm_trajectory
            WHERE firm_id = ?
            ORDER BY week_start DESC
            LIMIT 8
        """, (firm_id,)).fetchall()

        if not rows:
            return {"label": "NEW", "weeks": 0, "scores": []}

        scores = [r["weekly_score"] for r in rows]
        weeks  = len(scores)

        if weeks < 2:
            return {"label": "NEW", "weeks": weeks, "scores": scores}

        # BREAKOUT: this week significantly exceeds recent history
        this_week = scores[0]
        baseline  = sum(scores[1:]) / len(scores[1:]) if len(scores) > 1 else 0
        if this_week > baseline * 2.0 and this_week >= 3.5:
            return {"label": "BREAKOUT", "weeks": weeks, "scores": scores,
                    "this_week": this_week, "baseline": round(baseline, 1)}

        # ACCELERATING: last 3 weeks each higher than previous
        if weeks >= 3 and scores[0] > scores[1] > scores[2]:
            pct = round((scores[0] / scores[2] - 1) * 100) if scores[2] > 0 else 100
            return {"label": "ACCELERATING", "weeks": weeks, "scores": scores,
                    "growth_pct": pct}

        # SUSTAINED: high score for 3+ consecutive weeks
        high_threshold = 3.5
        if weeks >= 3 and all(s >= high_threshold for s in scores[:3]):
            return {"label": "SUSTAINED", "weeks": min(weeks, sum(1 for s in scores if s >= high_threshold)),
                    "scores": scores}

        # DECELERATING: was high, now dropping
        if weeks >= 3 and scores[1] > scores[0] and scores[2] > scores[1]:
            return {"label": "DECELERATING", "weeks": weeks, "scores": scores}

        # QUIET: no meaningful activity recently
        if scores[0] < 1.0 and (weeks < 2 or scores[1] < 1.0):
            return {"label": "QUIET", "weeks": weeks, "scores": scores}

        return {"label": "ACTIVE", "weeks": weeks, "scores": scores}

    @staticmethod
    def _week_start() -> str:
        today = datetime.now(timezone.utc).date()
        monday = today - timedelta(days=today.weekday())
        return monday.isoformat()


# ── Trajectory emoji labels for Telegram ──────────────────────────────────
TRAJECTORY_EMOJI = {
    "BREAKOUT":      "🚀",
    "ACCELERATING":  "📈",
    "SUSTAINED":     "🔁",
    "DECELERATING":  "📉",
    "ACTIVE":        "✅",
    "QUIET":         "🔇",
    "NEW":           "🆕",
}

TRAJECTORY_LABEL = {
    "BREAKOUT":      "Breakout — activity far above baseline",
    "ACCELERATING":  "Accelerating — rising 3 weeks in a row",
    "SUSTAINED":     "Sustained — strong for multiple weeks",
    "DECELERATING":  "Decelerating — may be cooling off",
    "ACTIVE":        "Active",
    "QUIET":         "Quiet — no recent signals",
    "NEW":           "New firm tracked",
}


def format_trajectory(traj: dict) -> str:
    """Return a compact string for the Telegram digest."""
    label = traj.get("label", "ACTIVE")
    emoji = TRAJECTORY_EMOJI.get(label, "")
    text  = TRAJECTORY_LABEL.get(label, label)
    if label == "ACCELERATING" and "growth_pct" in traj:
        text += f" (+{traj['growth_pct']}%)"
    elif label == "BREAKOUT" and "this_week" in traj:
        text += f" ({traj['this_week']:.1f}× vs {traj['baseline']} avg)"
    return f"{emoji} {text}"
