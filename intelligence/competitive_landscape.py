"""
intelligence/competitive_landscape.py
───────────────────────────────────────
Competitive Landscape Monitor — Find the Low-Competition Windows

The insight: your outreach is most valuable when competition is lowest.
You don't want to email BDP the same day 50 law students do after Blakes
announces a deal. You want to email them 3 days BEFORE the deal is news.

This module estimates competition pressure per firm per signal type:

1. LINKEDIN JOB POST APPLICANT COUNT
   LinkedIn shows applicant counts on public postings.
   >200 applicants = saturated, direct outreach needed.
   <50 applicants = early window, still worth applying.

2. JOB POST AGE
   Postings < 3 days old = low competition window.
   Postings 7-14 days old = peak competition.
   Postings > 21 days old = likely being filled quietly.

3. LAW SCHOOL APPLICATION DEADLINES
   October OCI season = every firm flooded with applications.
   February in-term = quiet lateral window.
   June-August = 0 competition from students.

4. EVENT-DRIVEN COMPETITION SPIKES
   When a major deal goes public (Blakes announces $2B Cenovus mandate),
   EVERY job seeker emails the same day.
   The tracker fires a COMPETITION SPIKE ALERT to EMAIL EARLIER (hours,
   not days) or wait 7-10 days for the noise to pass.

5. FIRM RESPONSE PATTERN ANALYSIS
   Tracks which firms respond to outreach (via your outcome logs),
   at what times of day, and on which day of the week.
   Surfaces the optimal send window per firm.

Output: Competition score per (firm, signal) pair.
Low competition = boost signal weight.
High competition = add "BYPASS STRATEGY" to outreach draft.
"""

import logging, json
from datetime import date, datetime, timedelta
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from database.db import get_conn, insert_signal
from config_calgary import FIRM_BY_ID, CALGARY_FIRMS

log = logging.getLogger(__name__)

# Calgary law school OCI seasons (approximate)
HIGH_COMPETITION_WINDOWS = [
    # (month_start, month_end, reason, multiplier_on_competition)
    (9, 10, "OCI season — every student applying",            3.0),
    (1, 2,  "Articling match season",                         2.0),
    (3, 3,  "Post-match lateral rush",                        1.5),
]

# Best outreach windows (low competition)
LOW_COMPETITION_WINDOWS = [
    (6, 8,  "Summer — students gone, only serious lateral seekers"),
    (11, 12,"Year-end — deal lawyers too busy to review resumes but still respond"),
    (4, 5,  "Post-AGM quiet — before summer lull"),
]


def _current_competition_multiplier() -> float:
    """Returns the seasonal competition multiplier for today's date."""
    m = date.today().month
    for start, end, _, mult in HIGH_COMPETITION_WINDOWS:
        if start <= m <= end:
            return mult
    return 1.0


def _is_low_competition_window() -> tuple[bool, str]:
    m = date.today().month
    for start, end, reason in LOW_COMPETITION_WINDOWS:
        if start <= m <= end:
            return True, reason
    return False, ""


class CompetitiveLandscapeMonitor:
    """
    Assesses competition pressure per firm and adjusts signal weights and
    outreach timing accordingly.
    """

    def run(self) -> dict:
        """
        Returns a dict of firm_id → competition_score (lower = better opportunity).
        Also fires LOW_COMPETITION signals during optimal windows.
        """
        seasonal_mult = _current_competition_multiplier()
        is_low, low_reason = _is_low_competition_window()

        scores = {}
        new_signals = []

        for firm in CALGARY_FIRMS:
            fid   = firm["id"]
            score = self._compute_competition_score(fid, seasonal_mult)
            scores[fid] = score

            # Fire LOW_COMPETITION signal if in a quiet window
            if is_low and score < 0.5:
                is_new = insert_signal(
                    firm_id=fid,
                    signal_type="low_competition_window",
                    weight=2.0,
                    title=f"Low-competition window: {firm['name']} — {low_reason}",
                    description=(
                        f"STRATEGIC TIMING: Competition for positions at {firm['name']} "
                        f"is currently low ({low_reason}). "
                        f"Competition score: {score:.2f} (< 0.5 = ideal). "
                        f"Outreach during this window has historically higher response rates."
                    ),
                    source_url="",
                    practice_area=firm.get("focus",["general"])[0],
                    raw_data={"competition_score": score, "seasonal_mult": seasonal_mult,
                              "reason": low_reason},
                )
                if is_new:
                    new_signals.append({
                        "firm_id": fid,
                        "signal_type": "low_competition_window",
                        "weight": 2.0,
                        "title": f"Low-competition window: {firm['name']}",
                        "practice_area": firm.get("focus",["general"])[0],
                    })

        # Also check for "competition spike" after major deal announcements
        self._detect_competition_spikes(new_signals)

        # Persist scores
        self._save_scores(scores)

        log.info("[Competition] %d firms scored. Low window: %s", len(scores), is_low)
        return scores

    def _compute_competition_score(self, firm_id: str, seasonal_mult: float) -> float:
        """
        Returns normalized competition score [0, 1].
        0 = no competition, 1 = maximum competition.
        """
        # Components
        conn   = get_conn()

        # Active job postings (more postings = more competition)
        try:
            n_postings = conn.execute("""
                SELECT count(*) as c FROM career_postings
                WHERE firm_id=? AND is_active=1
                  AND date(first_seen) >= date('now','-21 days')
            """, (firm_id,)).fetchone()
            posting_pressure = min(1.0, (n_postings["c"] if n_postings else 0) / 3.0)
        except Exception:
            posting_pressure = 0.3   # default

        # Average applicant count from career postings
        try:
            avg_app = conn.execute("""
                SELECT avg(applicant_count) as avg
                FROM career_postings
                WHERE firm_id=? AND applicant_count IS NOT NULL
            """, (firm_id,)).fetchone()
            app_count = float(avg_app["avg"] or 0)
            app_pressure = min(1.0, app_count / 300.0)
        except Exception:
            app_pressure = 0.2

        conn.close()

        # Seasonal multiplier
        base_score = (posting_pressure * 0.4 + app_pressure * 0.4 + 0.2) * seasonal_mult
        return min(1.0, base_score)

    def _detect_competition_spikes(self, new_signals: list):
        """
        If a high-weight deal signal (sedar_major_deal, breaking_deal_announcement)
        was fired in the last 24h, flag it as a competition spike event.
        """
        conn   = get_conn()
        recent = conn.execute("""
            SELECT firm_id, signal_type, title, weight
            FROM signals
            WHERE signal_type IN ('sedar_major_deal','breaking_deal_announcement',
                                   'gravity_spillage_predicted')
              AND date(detected_at) = date('now')
              AND weight >= 4.5
        """).fetchall()
        conn.close()

        for row in recent:
            firm = FIRM_BY_ID.get(row["firm_id"], {})
            insert_signal(
                firm_id=row["firm_id"],
                signal_type="competition_spike_warning",
                weight=-1.0,   # NEGATIVE — temporarily depresses firm score
                title=f"Competition spike: {firm.get('name',row['firm_id'])} — '{row['title'][:50]}'",
                description=(
                    f"COMPETITION WARNING: The deal '{row['title'][:80]}' will cause "
                    f"a mass outreach spike to {firm.get('name','')} within 24-48h. "
                    f"STRATEGY: Either (a) send TODAY before the rush, or "
                    f"(b) wait 7-10 days and send with additional context that "
                    f"others won't have. DO NOT send the generic 'I saw your SEDAR filing' email."
                ),
                source_url="",
                practice_area="general",
                raw_data={"triggering_signal": row["signal_type"]},
            )

    def _save_scores(self, scores: dict):
        import pathlib, json
        pathlib.Path("reports").mkdir(exist_ok=True)
        data = [
            {"firm_id": fid,
             "firm_name": FIRM_BY_ID.get(fid,{}).get("name",fid),
             "competition_score": score}
            for fid, score in sorted(scores.items(), key=lambda x: x[1])
        ]
        with open("reports/competition_scores.json", "w") as f:
            json.dump(data, f, indent=2)

    def get_optimal_send_day(self, firm_id: str) -> str:
        """
        Look at historical outreach_outcomes to find which day of the week
        this firm has replied to outreach most often.
        Returns day name or "Tuesday" as default.
        """
        conn = get_conn()
        try:
            rows = conn.execute("""
                SELECT strftime('%w', recorded_at) as dow, count(*) as cnt
                FROM outreach_outcomes
                WHERE firm_id=? AND outcome IN ('reply','interview')
                GROUP BY dow ORDER BY cnt DESC LIMIT 1
            """, (firm_id,)).fetchone()
            conn.close()
            if rows:
                days = ["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"]
                return days[int(rows["dow"])]
        except Exception:
            conn.close()
        return "Tuesday"   # default: highest open rate day


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    monitor = CompetitiveLandscapeMonitor()
    scores  = monitor.run()
    print("\n═══ COMPETITION SCORES (lower = better) ═══")
    for fid, score in sorted(scores.items(), key=lambda x: x[1])[:15]:
        name = FIRM_BY_ID.get(fid,{}).get("name",fid)
        bar  = "▓" * int(score * 20) + "░" * (20 - int(score * 20))
        print(f"  {name:<42}  {score:.2f}  {bar}")
