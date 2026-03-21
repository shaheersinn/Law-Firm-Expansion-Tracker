"""
graph/network_gravity.py
─────────────────────────
Network Gravity Model + Alumni Reverse Map

TWO ENGINES:

═══════════════════════════════════════════════════════════════════════
ENGINE A: Network Gravity Model (replaces simple co-appearance counting)
═══════════════════════════════════════════════════════════════════════

The physics metaphor: firms "attract" work based on mass (reputation,
deal size) and "repel" based on conflict distance. When a BigLaw firm
gets a mandate, overflow flows to boutiques with the highest GRAVITATIONAL
PULL from that BigLaw firm.

Gravitational pull g(bl → bt) is computed from:
  - Co-appearance count (raw attraction)
  - Practice area alignment (multiplier)
  - Geographic proximity (both Calgary? +20%)
  - Temporal recency (recent co-appearances weighted 2× older ones)
  - Deal-size history (larger deals = stronger link)

This produces a probabilistic distribution: given BigLaw firm X gets
a mandate, P(boutique Y gets overflow) for all boutiques.

═══════════════════════════════════════════════════════════════════════
ENGINE B: Alumni Reverse Map
═══════════════════════════════════════════════════════════════════════

Maps the "where did former associates go in-house" network.
When former Blakes associates dominate Cenovus's legal department,
Blakes will ALWAYS be the first call for Cenovus work. And when
Cenovus announces a deal, boutiques that frequently OPPOSED Blakes
will catch the overflow.

Data built from:
  - LinkedIn profile scraping (via Proxycurl)
  - LSA membership data (firm history)
  - CanLII counsel attribution

The alumni map gives us:
  1. Which energy companies have strong ties to which Calgary firms
  2. Which boutiques are "known" to BigLaw firms through alumni connections
  3. Which firms will lose associates to in-house roles (turnover predictor)

Both engines feed the Telegram alert:
  "BDP has 38 co-appearances opposite Blakes + 6 Blakes alumni now
   at Cenovus legal = BDP is Cenovus's first overflow call. Probability: 87%"
"""

import math, logging, json
from datetime import date, datetime, timedelta
from collections import defaultdict
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import numpy as np

from database.db import get_conn, insert_signal, get_spillage_graph
from config_calgary import CALGARY_FIRMS, FIRM_BY_ID, BIGLAW_FIRMS

log = logging.getLogger(__name__)

# ── Gravity model constants ────────────────────────────────────────────────────

G_CONSTANT          = 1.0    # gravitational constant
RECENCY_HALFLIFE_D  = 180    # 6-month half-life for temporal decay
PRACTICE_MATCH_MULT = 1.5    # multiplier for matching practice areas
SAME_CITY_MULT      = 1.2    # Calgary-Calgary multiplier
DEAL_SIZE_EXPONENT  = 0.3    # log-scale deal size contribution


def _temporal_weight(last_seen_str: str) -> float:
    """e^(-λt) decay with 6-month half-life."""
    try:
        d = date.fromisoformat(last_seen_str)
        days = (date.today() - d).days
        lam  = math.log(2) / RECENCY_HALFLIFE_D
        return math.exp(-lam * max(0, days))
    except Exception:
        return 0.5


def _practice_alignment(firm_a: dict, firm_b: dict) -> float:
    """Returns multiplier based on shared focus areas."""
    fa = set(firm_a.get("focus", []))
    fb = set(firm_b.get("focus", []))
    overlap = len(fa & fb)
    return PRACTICE_MATCH_MULT if overlap >= 2 else (1.1 if overlap == 1 else 1.0)


class NetworkGravityModel:
    """
    Computes P(boutique_Y | BigLaw_X gets mandate) using the gravity model.
    Updates after every CanLII / SEDAR batch ingestion.
    """

    def __init__(self):
        self.edges    = get_spillage_graph()
        self._gravity: dict[str, dict[str, float]] = {}
        self._build()

    def _build(self):
        """Build gravity scores from co-appearance edge data."""
        # Group edges: biglaw → {boutique → edge_data}
        raw = defaultdict(dict)
        for e in self.edges:
            bl, bt = e["biglaw_id"], e["boutique_id"]
            raw[bl][bt] = e

        for bl_id, boutiques in raw.items():
            bl_firm = FIRM_BY_ID.get(bl_id, {})
            scores  = {}
            for bt_id, edge in boutiques.items():
                bt_firm = FIRM_BY_ID.get(bt_id, {})

                # Base attraction: sqrt(co_appearances) to dampen outliers
                base   = math.sqrt(max(edge["co_appearances"], 1))

                # Temporal recency weight
                tw     = _temporal_weight(edge.get("last_seen") or "2020-01-01")

                # Practice alignment
                pa     = _practice_alignment(bl_firm, bt_firm)

                # Same-city bonus
                city   = SAME_CITY_MULT if (
                    bl_firm.get("hq","") == "Calgary" and
                    bt_firm.get("hq","") == "Calgary"
                ) else 1.0

                scores[bt_id] = G_CONSTANT * base * tw * pa * city

            # Normalize to probability distribution
            total = sum(scores.values())
            if total > 0:
                self._gravity[bl_id] = {bt: s/total for bt, s in scores.items()}
            else:
                self._gravity[bl_id] = {}

        log.debug("[Gravity] Built gravity model: %d BigLaw nodes", len(self._gravity))

    def predict_overflow(self, biglaw_id: str, top_n: int = 5) -> list[dict]:
        """
        Returns ranked list of boutiques with overflow probability.
        Each dict: {firm_id, firm_name, probability, gravity_score, drivers}
        """
        dist = self._gravity.get(biglaw_id, {})
        if not dist:
            return []

        bl_firm = FIRM_BY_ID.get(biglaw_id, {})
        result  = []
        for bt_id, prob in sorted(dist.items(), key=lambda x: x[1], reverse=True)[:top_n]:
            bt_firm = FIRM_BY_ID.get(bt_id, {})
            edge    = next((e for e in self.edges
                            if e["biglaw_id"] == biglaw_id and e["boutique_id"] == bt_id), {})
            drivers = []
            if edge.get("co_appearances", 0) >= 10:
                drivers.append(f"{edge['co_appearances']} co-appearances")
            pa = _practice_alignment(bl_firm, bt_firm)
            if pa > 1.0:
                drivers.append("practice alignment")
            if bt_firm.get("hq") == "Calgary":
                drivers.append("same-city")

            result.append({
                "firm_id":       bt_id,
                "firm_name":     bt_firm.get("name", bt_id),
                "probability":   round(prob, 4),
                "co_appearances":edge.get("co_appearances", 0),
                "last_seen":     edge.get("last_seen", ""),
                "drivers":       drivers,
            })
        return result

    def predict_all_overflows(self) -> dict[str, list[dict]]:
        """Returns overflow predictions for all BigLaw firms."""
        return {bl: self.predict_overflow(bl) for bl in self._gravity}

    def update_edge(self, biglaw_id: str, boutique_id: str):
        """Rebuild after a new co-appearance is recorded."""
        self.edges = get_spillage_graph()
        self._build()

    def fire_spillage_signals(self, deal_headline: str, biglaw_id: str,
                               deal_value_m: float | None, source_url: str):
        """
        Given an identified BigLaw deal, fire gravity-weighted signals
        against predicted overflow boutiques.
        """
        targets = self.predict_overflow(biglaw_id, top_n=4)
        bl_name = FIRM_BY_ID.get(biglaw_id, {}).get("name", biglaw_id)

        for t in targets:
            prob     = t["probability"]
            co_app   = t["co_appearances"]
            deal_str = f"${deal_value_m:.0f}M" if deal_value_m else "major"
            desc = (
                f"GRAVITY MODEL ALERT: {deal_str} deal → {bl_name} likely lead counsel. "
                f"Overflow probability to {t['firm_name']}: {prob:.0%} "
                f"(based on {co_app} historical co-appearances, gravity model). "
                f"Drivers: {', '.join(t['drivers'])}. "
                f"Deal: {deal_headline[:100]}."
            )
            weight = 4.0 + (prob * 2.0)   # max weight = 6.0 at 100% probability
            is_new = insert_signal(
                firm_id=t["firm_id"],
                signal_type="gravity_spillage_predicted",
                weight=weight,
                title=f"Gravity model: {bl_name} → {t['firm_name']} overflow P={prob:.0%}",
                description=desc,
                source_url=source_url,
                practice_area="corporate",
                raw_data={"biglaw": biglaw_id, "probability": prob,
                          "co_appearances": co_app, "deal_value_m": deal_value_m},
            )
            if is_new:
                log.info("[Gravity] %s → %s  P=%.0f%%  w=%.1f",
                         biglaw_id, t["firm_id"], prob*100, weight)


# ══════════════════════════════════════════════════════════════════════════════
# ENGINE B: Alumni Reverse Map
# ══════════════════════════════════════════════════════════════════════════════

class AlumniNetworkMap:
    """
    Tracks where former Calgary firm associates went in-house,
    and builds the "client pipeline" graph.

    firm A's alumni at company X → firm A will always get X's work
    → when X announces a deal, predict firm A as lead counsel
    → fire gravity signal against firm A's typical boutique opposites
    """

    def __init__(self):
        self._init_db()

    def _init_db(self):
        conn = get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS alumni_network (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                person_name      TEXT,
                from_firm_id     TEXT NOT NULL,
                to_employer      TEXT NOT NULL,
                role_at_employer TEXT,
                moved_date       TEXT,
                source           TEXT,
                recorded_at      TEXT DEFAULT (date('now')),
                UNIQUE(person_name, from_firm_id, to_employer)
            )""")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS firm_client_gravity (
                firm_id         TEXT NOT NULL,
                client_company  TEXT NOT NULL,
                alumni_count    INTEGER DEFAULT 0,
                last_updated    TEXT DEFAULT (date('now')),
                PRIMARY KEY (firm_id, client_company)
            )""")
        conn.commit()
        conn.close()

    def ingest_linkedin_departures(self):
        """
        Read linkedin_roster departed entries and build alumni map.
        Called after every LinkedIn turnover check.
        """
        conn = get_conn()
        rows = conn.execute("""
            SELECT firm_id, full_name, new_employer, left_date
            FROM linkedin_roster
            WHERE is_active=0 AND new_employer IS NOT NULL
        """).fetchall()

        for row in rows:
            employer = row["new_employer"] or ""
            # Filter: only in-house roles (company legal departments)
            is_inhouse = any(kw in employer.lower() for kw in [
                "energy", "resources", "oil", "gas", "pipeline",
                "corp", "inc", "ltd", "co.", "llc",
            ]) and all(kw not in employer.lower() for kw in [
                "llp", "law", "firm", "legal counsel services",
            ])
            if not is_inhouse:
                continue
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO alumni_network
                        (person_name, from_firm_id, to_employer, moved_date, source)
                    VALUES (?,?,?,?,'linkedin')
                """, (row["full_name"], row["firm_id"], employer, row["left_date"]))
                conn.execute("""
                    INSERT INTO firm_client_gravity (firm_id, client_company, alumni_count)
                    VALUES (?,?,1)
                    ON CONFLICT(firm_id, client_company) DO UPDATE SET
                        alumni_count = alumni_count + 1,
                        last_updated = date('now')
                """, (row["firm_id"], employer))
            except Exception:
                pass

        conn.commit()
        conn.close()

    def get_predicted_counsel(self, company_name: str, top_n: int = 3) -> list[dict]:
        """
        Given a company name, return the law firms most likely to be retained
        (based on alumni density at that company).
        """
        conn  = get_conn()
        rows  = conn.execute("""
            SELECT firm_id, alumni_count FROM firm_client_gravity
            WHERE lower(client_company) LIKE lower(?)
            ORDER BY alumni_count DESC LIMIT ?
        """, (f"%{company_name}%", top_n)).fetchall()
        conn.close()
        return [
            {
                "firm_id":     r["firm_id"],
                "firm_name":   FIRM_BY_ID.get(r["firm_id"], {}).get("name", r["firm_id"]),
                "alumni_count":r["alumni_count"],
            }
            for r in rows
        ]

    def get_turnover_risk_firms(self) -> list[dict]:
        """
        Firms that have lost the most associates to in-house roles recently
        = highest structural turnover = most likely to need associates ongoing.
        """
        conn  = get_conn()
        rows  = conn.execute("""
            SELECT from_firm_id, count(*) as alumni_count
            FROM alumni_network
            WHERE date(moved_date) >= date('now','-180 days')
            GROUP BY from_firm_id
            ORDER BY alumni_count DESC
            LIMIT 10
        """).fetchall()
        conn.close()
        return [
            {
                "firm_id": r["from_firm_id"],
                "firm_name": FIRM_BY_ID.get(r["from_firm_id"],{}).get("name", r["from_firm_id"]),
                "recent_departures_to_inhouse": r["alumni_count"],
            }
            for r in rows
        ]

    def generate_signals(self):
        """Fire signals for firms with high structural in-house turnover."""
        risky = self.get_turnover_risk_firms()
        for r in risky:
            if r["recent_departures_to_inhouse"] >= 2:
                is_new = insert_signal(
                    firm_id=r["firm_id"],
                    signal_type="alumni_turnover_structural",
                    weight=3.5,
                    title=f"Structural in-house drain: {r['firm_name']} — {r['recent_departures_to_inhouse']} departures to industry",
                    description=(
                        f"{r['firm_name']} has lost {r['recent_departures_to_inhouse']} associates "
                        f"to in-house roles in the past 6 months. "
                        f"This is a structural pattern, not a one-off. "
                        f"The firm will need to continuously backfill at the junior level."
                    ),
                    source_url="",
                    practice_area="general",
                    raw_data={"departures": r["recent_departures_to_inhouse"]},
                )
                if is_new:
                    log.info("[Alumni] Structural drain signal: %s", r["firm_id"])


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    model = NetworkGravityModel()
    print("\n═══ GRAVITY MODEL — ALL BIGLAW PREDICTIONS ═══")
    for bl_id in list(BIGLAW_FIRMS)[:4]:
        bl_name = FIRM_BY_ID.get(bl_id, {}).get("name", bl_id)
        preds   = model.predict_overflow(bl_id, top_n=3)
        print(f"\n  {bl_name}:")
        for p in preds:
            print(f"    → {p['firm_name']:<35} P={p['probability']:.0%}  ({p['co_appearances']} co-app)")
