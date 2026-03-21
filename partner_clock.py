"""
signals/deep/partner_clock.py
───────────────────────────────
Signal 14 — Partner Pressure Clock

Tracks individual named partners at target firms, not just the firm as a whole.
When a SPECIFIC partner is drowning, they personally reach out for junior help —
and a tailored "I saw your name on [file]" email is 10x more effective.

Four sub-signals:

A) COURT APPEARANCE FREQUENCY per partner
   Parse counsel names from CanLII decisions. Track each partner's personal
   30-day appearance count. A partner who normally appears 3x/month and is
   now appearing 12x/month is in crisis mode — they personally need help.

B) PARTNER RETIREMENT PROXIMITY
   Partners at most Calgary firms retire at 65-70 (mandatory or de facto).
   When a named partner is approaching this window:
   - They start transitioning files to associates (creating demand)
   - The firm needs someone to absorb their practice (often a junior)
   - This is a 1-3 year PREDICTIVE window
   Cross-reference CanLII first appearances (estimated call year) with
   partner bios (graduation year if listed) to estimate retirement proximity.

C) NEW EQUITY PARTNER ELEVATION
   When an associate becomes a partner (often announced via firm news or
   LinkedIn), their OLD associate position is now vacant. They also now need
   their own associate. One promotion = two vacancies.

D) JUDICIAL APPOINTMENT SIGNAL
   When a senior partner is appointed to the bench (announced via
   Order in Council, Alberta Gazette, or Canadian judicial appointments feed),
   the firm loses its anchor practitioner overnight.
   Source: https://www.fja-cmf.gc.ca/appointments-nominations/rss-en.xml

This module builds a named-lawyer graph in the DB for deep analytics.
"""

import re, time, logging, json
from datetime import date, datetime, timedelta
from collections import defaultdict
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import requests, feedparser
from bs4 import BeautifulSoup

from database.db import insert_signal, get_conn
from config_calgary import CALGARY_FIRMS, FIRM_BY_ID

log = logging.getLogger(__name__)

FJA_RSS = "https://www.fja-cmf.gc.ca/appointments-nominations/rss-en.xml"
ALBERTA_GAZETTE_URL = "https://www.alberta.ca/alberta-gazette.aspx"

# ── DB initialisation ──────────────────────────────────────────────────────────

def _init_partner_db():
    conn = get_conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS partner_appearances (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            firm_id         TEXT NOT NULL,
            partner_name    TEXT NOT NULL,
            case_id         TEXT,
            appearance_date TEXT,
            court           TEXT,
            recorded_at     TEXT DEFAULT (datetime('now')),
            UNIQUE(partner_name, case_id)
        )""")
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_pa_partner
        ON partner_appearances(partner_name, appearance_date)""")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS lawyer_profiles (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            firm_id         TEXT NOT NULL,
            full_name       TEXT NOT NULL UNIQUE,
            role            TEXT,            -- "partner", "associate", "counsel"
            estimated_call_year INTEGER,
            estimated_birth_year INTEGER,
            call_source     TEXT,            -- how we estimated
            first_seen      TEXT DEFAULT (date('now')),
            last_seen       TEXT DEFAULT (date('now')),
            is_active       INTEGER DEFAULT 1,
            notes           TEXT
        )""")
    conn.commit()
    conn.close()


# ── Counsel name extraction from CanLII text ───────────────────────────────────

# Patterns: "J. Smith for the applicant" / "Jane Smith of Blakes" / "Counsel: John Smith"
COUNSEL_NAME_RE = re.compile(
    r"(?:(?:Counsel|represented by|for the (?:applicant|respondent|appellant|defendant|plaintiff))[:,]?\s*)"
    r"([A-Z][a-z]+(?:\s[A-Z]\.?)?\s[A-Z][a-z]+(?:\s[A-Z][a-z]+)?)",
    re.MULTILINE,
)

# "of [Firm Name]" — to attribute names to firms
OF_FIRM_RE = re.compile(
    r"([A-Z][a-z]+(?:\s[A-Z]\.?)?\s[A-Z][a-z]+)\s+of\s+([A-Za-z,&\s]+LLP|[A-Za-z,&\s]+Law)",
    re.MULTILINE,
)

# Seniority indicators in CanLII text
QC_KC_RE   = re.compile(r"\b(Q\.?C\.?|K\.?C\.?)\b")
SENIOR_RE  = re.compile(r"\b(senior partner|managing partner|chair|co.chair)\b", re.IGNORECASE)


def extract_counsel_names(counsel_text: str, firm_aliases: list) -> list[str]:
    """Extract individual lawyer names from CanLII counsel metadata."""
    names = []
    # Try direct pattern
    for m in COUNSEL_NAME_RE.finditer(counsel_text):
        names.append(m.group(1).strip())
    # Try "of firm" pattern
    for m in OF_FIRM_RE.finditer(counsel_text):
        candidate_firm = m.group(2).strip()
        if any(alias.lower() in candidate_firm.lower() for alias in firm_aliases):
            names.append(m.group(1).strip())
    # Fallback: simple name extraction (capitalized pairs)
    if not names:
        for tok in re.findall(r"\b([A-Z][a-z]+ [A-Z][a-z]+)\b", counsel_text):
            names.append(tok)
    return list(set(n for n in names if len(n.split()) >= 2))


# ── Appearance frequency analysis ─────────────────────────────────────────────

class PartnerPressureClock:

    def __init__(self):
        self.new_signals: list[dict] = []
        _init_partner_db()

    def run(self) -> list[dict]:
        log.info("[PartnerClock] Running all sub-signals…")
        self._analyse_appearance_frequency()
        self._scan_judicial_appointments()
        self._detect_retirement_proximity()
        log.info("[PartnerClock] Done. %d signals.", len(self.new_signals))
        return self.new_signals

    # ── A: Appearance frequency ────────────────────────────────────────────────

    def _analyse_appearance_frequency(self):
        """
        For each lawyer in partner_appearances, compute 30-day count vs
        prior baseline. Fire if z-score ≥ 1.5.
        """
        import numpy as np
        conn   = get_conn()

        # Get all partners with enough history
        partners = conn.execute("""
            SELECT partner_name, firm_id, count(*) as total
            FROM partner_appearances
            GROUP BY partner_name, firm_id
            HAVING total >= 5
        """).fetchall()

        for row in partners:
            name    = row["partner_name"]
            firm_id = row["firm_id"]

            # 30-day count
            recent = conn.execute("""
                SELECT count(*) as c FROM partner_appearances
                WHERE partner_name=?
                  AND date(appearance_date) >= date('now','-90 days')
            """, (name,)).fetchone()["c"]

            # Baseline: 4 prior 30-day windows
            baseline = []
            for i in range(1, 5):
                window_start = (date.today() - timedelta(days=30*i+30)).isoformat()
                window_end   = (date.today() - timedelta(days=30*i)).isoformat()
                cnt = conn.execute("""
                    SELECT count(*) as c FROM partner_appearances
                    WHERE partner_name=?
                      AND date(appearance_date) BETWEEN ? AND ?
                """, (name, window_start, window_end)).fetchone()["c"]
                baseline.append(cnt)

            if len(baseline) < 2 or max(baseline) == 0:
                continue

            arr  = np.array(baseline, dtype=float)
            mu   = arr.mean()
            sd   = arr.std()
            z    = (recent - mu) / sd if sd > 0 else 0.0

            if z >= 1.5:
                firm = FIRM_BY_ID.get(firm_id, {"name": firm_id})
                desc = (
                    f"PARTNER OVERLOAD: {name} at {firm['name']} appeared {recent}× "
                    f"in the past 30 days (z={z:.2f}, baseline avg={mu:.1f}). "
                    f"This partner personally needs junior support. "
                    f"An email referencing their recent files is highly targeted."
                )
                is_new = insert_signal(
                    firm_id=firm_id,
                    signal_type="partner_appearance_spike",
                    weight=4.5,
                    title=f"Partner overload: {name} ({firm['name']}) — {recent} appearances (z={z:.2f})",
                    description=desc,
                    source_url="https://www.canlii.org/en/ab/abqb/",
                    practice_area="litigation",
                    raw_data={"partner": name, "recent_30": recent, "z": z, "baseline_avg": mu},
                )
                if is_new:
                    self.new_signals.append({
                        "firm_id": firm_id,
                        "signal_type": "partner_appearance_spike",
                        "weight": 4.5,
                        "title": f"Partner overload: {name} — {recent} appearances (z={z:.2f})",
                        "practice_area": "litigation",
                        "description": desc,
                    })
                    log.info("[PartnerClock] 🔴 %s @ %s — z=%.2f", name, firm_id, z)

        conn.close()

    # ── B: Judicial appointment scanner ───────────────────────────────────────

    def _scan_judicial_appointments(self):
        """
        Monitor Federal Judicial Affairs RSS for new bench appointments.
        Cross-check against our lawyer profiles to detect when a known
        partner at a target firm is appointed.
        """
        try:
            feed = feedparser.parse(FJA_RSS)
        except Exception as e:
            log.debug("[PartnerClock] FJA RSS error: %s", e); return

        conn     = get_conn()
        profiles = conn.execute(
            "SELECT full_name, firm_id FROM lawyer_profiles WHERE role='partner' AND is_active=1"
        ).fetchall()
        known_partners = {row["full_name"].lower(): row["firm_id"] for row in profiles}
        conn.close()

        for entry in feed.entries:
            title   = getattr(entry, "title",   "")
            link    = getattr(entry, "link",    "")
            summary = getattr(entry, "summary", "")
            combined = f"{title} {summary}"

            if "Alberta" not in combined and "Calgary" not in combined:
                continue

            # Try to match a known partner's name
            for partner_name_lower, firm_id in known_partners.items():
                parts = partner_name_lower.split()
                if len(parts) >= 2 and parts[-1] in combined.lower():
                    firm = FIRM_BY_ID.get(firm_id, {})
                    desc = (
                        f"JUDICIAL APPOINTMENT: {title}. "
                        f"If this is {partner_name_lower.title()} from {firm.get('name','')}, "
                        f"the firm has lost a senior practitioner overnight. "
                        f"Succession planning will require immediate associate intake."
                    )
                    is_new = insert_signal(
                        firm_id=firm_id,
                        signal_type="judicial_appointment_void",
                        weight=5.0,
                        title=f"Judicial appointment: {title[:60]}",
                        description=desc,
                        source_url=link,
                        practice_area="litigation",
                        raw_data={"appointment_title": title, "partner": partner_name_lower},
                    )
                    if is_new:
                        self.new_signals.append({
                            "firm_id": firm_id,
                            "signal_type": "judicial_appointment_void",
                            "weight": 5.0,
                            "title": f"Judicial appointment creates anchor-lawyer void: {title[:50]}",
                            "practice_area": "litigation",
                        })

        # Also: catch any Alberta appointment even if partner not yet profiled
        for entry in feed.entries:
            title   = getattr(entry, "title", "")
            link    = getattr(entry, "link",  "")
            if "Alberta" in title or "Calgary" in title:
                # Fire broad signal against litigation boutiques
                for firm in CALGARY_FIRMS:
                    if "litigation" in firm.get("focus", []) and firm["tier"] == "mid":
                        insert_signal(
                            firm_id=firm["id"],
                            signal_type="judicial_appointment_market",
                            weight=2.5,
                            title=f"Alberta bench appointment: {title[:60]}",
                            description=(
                                f"New judge appointed in Alberta: {title}. "
                                f"Appointing a litigator to the bench creates a gap "
                                f"at their firm and realigns opposing counsel relationships."
                            ),
                            source_url=link,
                            practice_area="litigation",
                        )

    # ── C: Retirement proximity ────────────────────────────────────────────────

    def _detect_retirement_proximity(self):
        """
        Scan lawyer profiles for estimated call years. Partners called in
        the 1980s-1990s are likely approaching 65-70. Fire an early signal.
        """
        conn = get_conn()
        rows = conn.execute("""
            SELECT full_name, firm_id, estimated_call_year
            FROM lawyer_profiles
            WHERE role='partner' AND is_active=1
              AND estimated_call_year IS NOT NULL
              AND estimated_call_year <= strftime('%Y','now') - 28
        """).fetchall()   # 28+ years called ≈ approaching retirement age
        conn.close()

        for row in rows:
            years_called = date.today().year - row["estimated_call_year"]
            firm         = FIRM_BY_ID.get(row["firm_id"], {})
            urgency      = "high" if years_called >= 35 else "medium"

            is_new = insert_signal(
                firm_id=row["firm_id"],
                signal_type="partner_retirement_proximity",
                weight=3.0 if urgency == "high" else 2.0,
                title=f"Retirement proximity: {row['full_name']} ({firm.get('name','')} — called ~{row['estimated_call_year']})",
                description=(
                    f"{row['full_name']} has been called approximately {years_called} years "
                    f"({urgency} retirement proximity). "
                    f"As senior partners transition out, firms need juniors to absorb "
                    f"file transfers and maintain client relationships. "
                    f"1-3 year predictive window."
                ),
                source_url="",
                practice_area=firm.get("focus", ["general"])[0] if firm else "general",
                raw_data={"years_called": years_called, "call_year": row["estimated_call_year"]},
            )
            if is_new:
                self.new_signals.append({
                    "firm_id": row["firm_id"],
                    "signal_type": "partner_retirement_proximity",
                    "weight": 3.0 if urgency == "high" else 2.0,
                    "title": f"Retirement proximity: {row['full_name']} (~{years_called}yr call)",
                    "practice_area": firm.get("focus", ["general"])[0] if firm else "general",
                })

    # ── Partner profile builder (called separately to bootstrap) ──────────────

    def build_partner_profiles_from_canlii(self):
        """
        Read the canlii_appearances table and extract individual counsel names
        to populate lawyer_profiles. Run once to bootstrap, then incrementally.
        """
        conn = get_conn()
        rows = conn.execute("""
            SELECT firm_id, counsel_raw, citation, decision_date
            FROM canlii_appearances
            WHERE counsel_raw IS NOT NULL AND counsel_raw != ''
            ORDER BY decision_date DESC
        """).fetchall()

        for row in rows:
            firm   = FIRM_BY_ID.get(row["firm_id"], {})
            aliases = [firm.get("name","")] + firm.get("aliases",[])
            names   = extract_counsel_names(row["counsel_raw"], aliases)

            for name in names:
                try:
                    conn.execute("""
                        INSERT OR IGNORE INTO lawyer_profiles
                            (firm_id, full_name, role, first_seen, last_seen)
                        VALUES (?, ?, 'unknown', date('now'), date('now'))
                    """, (row["firm_id"], name))
                    # Record the appearance
                    conn.execute("""
                        INSERT OR IGNORE INTO partner_appearances
                            (firm_id, partner_name, case_id, appearance_date, court)
                        VALUES (?, ?, ?, ?, 'ABQB')
                    """, (row["firm_id"], name,
                          f"{row['firm_id']}_{name[:8]}_{row['decision_date']}",
                          row["decision_date"]))
                except Exception:
                    pass

        conn.commit()
        conn.close()
        log.info("[PartnerClock] Partner profiles built from CanLII data.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    clock = PartnerPressureClock()
    clock.build_partner_profiles_from_canlii()
    for s in clock.run():
        print(f"  [{s['signal_type']}] {s['firm_id']}: {s['title']}")
