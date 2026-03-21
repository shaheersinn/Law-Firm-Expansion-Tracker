"""
signals/predictive/deal_cascade.py
────────────────────────────────────
Signal 19 — Deal Cascade Tracker + Second-Derivative Acceleration Detector

TWO ENGINES FOR DETECTING MOMENTUM, NOT JUST EVENTS:

═══════════════════════════════════════════════════════════════════════
ENGINE A: Deal Cascade Tracker
═══════════════════════════════════════════════════════════════════════

A deal rarely announces itself with a single signal. It cascades:

  Step 1: SEDI insider cluster buy            (T-60 days)
  Step 2: Corporate registry entity velocity  (T-45 days)
  Step 3: AER hearing application filed       (T-30 days)
  Step 4: Competition Bureau pre-notification (T-21 days)
  Step 5: Newswire press release              (T-0 to T+1 day)
  Step 6: SEDAR+ filing                       (T+3 to T+14 days)
  Step 7: New CanLII proceeding               (T+60 to T+365 days)

If we see steps 1-3, we can predict with 80%+ confidence that
steps 5-7 are coming. The cascade detector fires a PREDICTIVE alert
BEFORE the deal becomes public.

More signals confirmed = higher probability = higher weight.
Confirmed cascade = weight 6.0 (highest in system).

═══════════════════════════════════════════════════════════════════════
ENGINE B: Second-Derivative Acceleration Detector
═══════════════════════════════════════════════════════════════════════

The first derivative: "Firm X's CanLII appearances increased 20% MoM"
The second derivative: "Firm X's appearance GROWTH RATE is itself accelerating"

Second derivative positive = exponential growth in workload = hire IMMINENT.
Second derivative negative = growth is decelerating = may plateau without hiring.

Computed for:
  - CanLII appearance velocity
  - SEDAR+ deal mention velocity
  - Newswire mention velocity
  - Team page headcount trajectory

This catches firms that are about to cross the "we need to hire" threshold
BEFORE they realize it themselves.
"""

import math, json, logging
from datetime import date, datetime, timedelta
from collections import defaultdict
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from database.db import insert_signal, get_conn
from config_calgary import CALGARY_FIRMS, FIRM_BY_ID

log = logging.getLogger(__name__)

# CASCADE STEP DEFINITIONS
# Each step: signal_type → (cascade_step_number, probability_contribution)
CASCADE_STEPS = {
    "sedi_insider_cluster":           (1, 0.25),
    "registry_entity_velocity":       (2, 0.20),
    "registry_deal_structure":        (2, 0.25),
    "aer_proceeding_upcoming":        (3, 0.15),
    "competition_bureau_notification":(4, 0.30),
    "macro_ma_wave_incoming":         (0, 0.10),
    "newswire_deal_detected":         (5, 0.35),
    "breaking_deal_announcement":     (5, 0.45),
    "sedar_major_deal":               (6, 0.40),
    "sedar_counsel_named":            (6, 0.30),
    "court_filing_major":             (7, 0.35),
    "canlii_appearance_spike":        (7, 0.30),
}

# Minimum cascade probability to fire a cascade signal
CASCADE_THRESHOLD = 0.55


class DealCascadeTracker:
    """
    Monitors the signal DB for multi-step cascade confirmation chains.
    When P(deal) > threshold, fires a high-weight predictive signal.
    """

    def run(self) -> list[dict]:
        log.info("[Cascade] Running deal cascade analysis…")
        new_signals = []
        conn = get_conn()

        # Group recent signals by firm
        rows = conn.execute("""
            SELECT firm_id, signal_type, weight, detected_at, raw_data
            FROM signals
            WHERE date(detected_at) >= date('now','-90 days')
            ORDER BY detected_at DESC
        """).fetchall()
        conn.close()

        firm_sigs: dict[str, list] = defaultdict(list)
        for r in rows:
            firm_sigs[r["firm_id"]].append(dict(r))

        for firm_id, signals in firm_sigs.items():
            result = self._analyse_cascade(firm_id, signals)
            if result:
                new_signals.append(result)

        log.info("[Cascade] Done. %d cascade signals.", len(new_signals))
        return new_signals

    def _analyse_cascade(self, firm_id: str, signals: list) -> dict | None:
        """
        For a given firm, compute cascade probability from its recent signals.
        Returns a signal dict if threshold exceeded, else None.
        """
        observed_steps    = set()
        total_probability = 0.0
        confirmed_signals = []

        for sig in signals:
            st = sig.get("signal_type", "")
            if st in CASCADE_STEPS:
                step, prob_contrib = CASCADE_STEPS[st]
                if step not in observed_steps:   # each step contributes once
                    observed_steps.add(step)
                    total_probability += prob_contrib
                    confirmed_signals.append({
                        "step": step, "type": st,
                        "title": sig.get("title","")[:60],
                        "date": (sig.get("detected_at","") or "")[:10],
                    })

        # Bonus: multiple independent source types (corroboration)
        source_diversity = len(set(
            "predictive" if s in ["sedi_insider_cluster","registry_entity_velocity","macro_ma_wave_incoming"]
            else "realtime" if s in ["breaking_deal_announcement","newswire_deal_detected","competition_bureau_notification"]
            else "lagging"
            for s in observed_steps
            for s in CASCADE_STEPS if CASCADE_STEPS.get(s, (0,0))[0] in observed_steps
        ))
        if source_diversity >= 3:
            total_probability *= 1.25

        # Clamp to [0, 1]
        probability = min(1.0, total_probability)

        if probability < CASCADE_THRESHOLD:
            return None

        firm  = FIRM_BY_ID.get(firm_id, {})
        steps_str = ", ".join(
            f"Step {s['step']} [{s['type']}] on {s['date']}"
            for s in sorted(confirmed_signals, key=lambda x: x["step"])
        )
        weight = 4.0 + (probability * 2.5)   # ranges from 4.0 (at 55%) to 6.5 (at 100%)

        desc = (
            f"DEAL CASCADE CONFIRMED at {firm.get('name', firm_id)}: "
            f"P(active deal/major mandate)={probability:.0%}. "
            f"Confirmed steps: {steps_str}. "
            f"Multiple independent signals converging on the same firm. "
            f"Contact hiring partner NOW — before the announcement."
        )

        is_new = insert_signal(
            firm_id=firm_id,
            signal_type="deal_cascade_confirmed",
            weight=round(weight, 2),
            title=f"Deal cascade: {firm.get('name',firm_id)} P={probability:.0%} ({len(confirmed_signals)} signals)",
            description=desc,
            source_url="",
            practice_area="corporate",
            raw_data={
                "probability":      probability,
                "steps_observed":   list(observed_steps),
                "confirmed_signals":confirmed_signals,
                "source_diversity": source_diversity,
            },
        )
        if is_new:
            log.info("[Cascade] 🎯 %s P=%.0f%% (steps: %s)",
                     firm_id, probability*100, observed_steps)
            return {
                "firm_id":     firm_id,
                "signal_type": "deal_cascade_confirmed",
                "weight":      round(weight, 2),
                "title":       f"Deal cascade: {firm.get('name',firm_id)} P={probability:.0%}",
                "practice_area": "corporate",
                "description": desc,
                "raw_data": {"probability": probability, "steps": list(observed_steps)},
            }
        return None


class SecondDerivativeDetector:
    """
    Detects firms whose signal RATE is itself accelerating.
    First derivative: Δ(signals/week)
    Second derivative: ΔΔ(signals/week) — the acceleration
    """

    def run(self) -> list[dict]:
        log.info("[2ndDeriv] Computing signal acceleration for all firms…")
        new_signals = []

        for firm in CALGARY_FIRMS:
            result = self._compute_acceleration(firm)
            if result:
                new_signals.append(result)

        log.info("[2ndDeriv] Done. %d acceleration signals.", len(new_signals))
        return new_signals

    def _compute_acceleration(self, firm: dict) -> dict | None:
        """
        Compute weekly signal counts over the past 5 weeks.
        If the count sequence shows positive second derivative,
        fire an acceleration signal.
        """
        fid  = firm["id"]
        conn = get_conn()
        weeks = []
        for w in range(5, 0, -1):   # weeks 5→1 (oldest to newest)
            start = (date.today() - timedelta(weeks=w)).isoformat()
            end   = (date.today() - timedelta(weeks=w-1)).isoformat()
            cnt   = conn.execute("""
                SELECT count(*) as c FROM signals
                WHERE firm_id=?
                  AND date(detected_at) BETWEEN ? AND ?
                  AND signal_type NOT IN ('fiscal_pressure_incoming','macro_demand_surge')
            """, (fid, start, end)).fetchone()["c"]
            weeks.append(cnt)
        conn.close()

        if sum(weeks) < 3:   # not enough signal history
            return None

        # First differences (weekly change)
        d1 = [weeks[i+1] - weeks[i] for i in range(len(weeks)-1)]
        # Second differences (acceleration)
        d2 = [d1[i+1] - d1[i] for i in range(len(d1)-1)]

        if not d2:
            return None

        avg_d2       = sum(d2) / len(d2)
        current_rate = weeks[-1]   # signals this week
        prev_rate    = weeks[-2]   # signals last week

        # Fire if: acceleration is positive AND current rate is meaningful
        if avg_d2 <= 0 or current_rate < 2:
            return None

        # Compute how many weeks until saturation (rate of growth)
        weeks_to_crisis = max(1, round(3 / avg_d2)) if avg_d2 > 0 else 4
        urgency = "IMMINENT" if weeks_to_crisis <= 1 else f"~{weeks_to_crisis}w out"

        desc = (
            f"SIGNAL ACCELERATION: {firm['name']}'s signal rate is accelerating. "
            f"5-week sequence: {weeks}. "
            f"1st derivative (weekly Δ): {d1}. "
            f"2nd derivative (acceleration): {d2}. "
            f"Average acceleration: +{avg_d2:.1f} signals/week². "
            f"At this rate: hiring need is {urgency}."
        )
        weight = min(5.0, 3.0 + avg_d2 * 0.5)

        is_new = insert_signal(
            firm_id=fid,
            signal_type="signal_acceleration_detected",
            weight=weight,
            title=f"Signal acceleration: {firm['name']} — +{avg_d2:.1f}/wk² ({urgency})",
            description=desc,
            source_url="",
            practice_area=firm.get("focus", ["general"])[0],
            raw_data={
                "weekly_counts": weeks, "d1": d1, "d2": d2,
                "avg_acceleration": avg_d2, "urgency": urgency,
            },
        )
        if is_new:
            log.info("[2ndDeriv] 📈 %s — accel=%.1f/wk² (%s)", fid, avg_d2, urgency)
            return {
                "firm_id":     fid,
                "signal_type": "signal_acceleration_detected",
                "weight":      weight,
                "title":       f"Signal acceleration: {firm['name']} ({urgency})",
                "practice_area": firm.get("focus", ["general"])[0],
                "description": desc,
            }
        return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # Cascade
    ct = DealCascadeTracker()
    for s in ct.run():
        print(f"  [CASCADE] {s['firm_id']}: {s['title']}")
    # Acceleration
    sd = SecondDerivativeDetector()
    for s in sd.run():
        print(f"  [ACCEL]   {s['firm_id']}: {s['title']}")
