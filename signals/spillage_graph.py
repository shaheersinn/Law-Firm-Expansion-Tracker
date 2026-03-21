"""
signals/spillage_graph.py
──────────────────────────
Strategy 5 — BigLaw "Spillage Graph" Exploit

Maps which Calgary boutiques most consistently appear opposite BigLaw firms
in CanLII cases and SEDAR+ transactions. When a massive deal involving a
BigLaw firm is announced, the graph predicts which boutique will catch the
conflict overflow — and fires a SAME-DAY alert.

Two components:
  A) Graph builder  — reads historical co-appearance data from the DB
  B) Deal monitor   — watches Google News RSS + SEDAR+ for mega-deals;
                      cross-references the graph to predict spillage recipients

Creative extras:
  • "Conflict radar" — tracks which Calgary BigLaw offices have recently
    acted for energy majors (Cenovus, Suncor, TC Energy, ARC, etc.).
    If Blakes was counsel for Cenovus last month, and Cenovus announces a
    new $2B acquisition, Blakes likely can't act for the counterparty.
  • Betweenness centrality ranks boutiques by how "indispensable" they are
    to the BigLaw overflow network → highest-centrality boutiques are
    the most hire-hungry when deal flow spikes.
"""

import json
import logging
import math
import re
import time
from collections import defaultdict
from datetime import datetime, timedelta

import feedparser
import requests

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config_calgary import (
    CALGARY_FIRMS, FIRM_ALIASES, BIGLAW_FIRMS,
    SIGNAL_WEIGHTS, FIRM_BY_ID,
)
from database.db import (
    get_spillage_graph, insert_signal, get_conn,
)

log = logging.getLogger(__name__)

# ─── Constants ───────────────────────────────────────────────────────────────

GOOGLE_NEWS_RSS = "https://news.google.com/rss/search?q={query}&hl=en-CA&gl=CA&ceid=CA:en"
SEDAR_RSS       = "https://www.sedarplus.ca/landingPage/rss/filings.rss"

# Deal size thresholds (CAD millions)
MEGA_DEAL_THRESHOLD  = 1_000   # $1B+
LARGE_DEAL_THRESHOLD = 250     # $250M+

# Energy companies most likely to trigger conflict overflow at Calgary BigLaw
CALGARY_ENERGY_MAJORS = [
    "Cenovus Energy", "Suncor Energy", "Canadian Natural Resources", "CNQ",
    "TC Energy", "Enbridge", "ARC Resources", "Tourmaline Oil", "Vermilion Energy",
    "Pembina Pipeline", "Whitecap Resources", "Crescent Point Energy",
    "Baytex Energy", "MEG Energy", "CNRL", "Imperial Oil",
    "TransAlta", "Capital Power", "Keyera",
]

MEGA_DEAL_KEYWORDS = re.compile(
    r"\b(billion|merger|acquisition|M&A|takeover|arrangement|"
    r"transaction|deal|CCAA|restructur|going.private|privatization|"
    r"IPO|initial public offering|spin.off)\b",
    re.IGNORECASE,
)

DOLLAR_RE = re.compile(
    r"\$\s*(\d[\d,\.]*)\s*(billion|million|B|M)\b",
    re.IGNORECASE,
)


# ─── Graph Analysis ──────────────────────────────────────────────────────────

class SpillageGraphAnalyzer:
    """
    Reads the co-appearance edge table and computes:
      • Per-BigLaw → sorted list of boutique partners by co-appearance count
      • Betweenness centrality (simple proxy version)
      • "Conflict radar" — which BigLaw firms recently acted for each energy major
    """

    def __init__(self):
        self.edges   = get_spillage_graph()      # list of {biglaw_id, boutique_id, ...}
        self.graph   = defaultdict(dict)         # biglaw_id → {boutique_id: count}
        self._build()

    def _build(self):
        for edge in self.edges:
            bl = edge["biglaw_id"]
            bt = edge["boutique_id"]
            self.graph[bl][bt] = edge["co_appearances"]

    def top_boutiques_for(self, biglaw_id: str, top_n: int = 5) -> list[tuple[str, int]]:
        """Returns [(boutique_id, co_appearances)] sorted descending."""
        partners = self.graph.get(biglaw_id, {})
        return sorted(partners.items(), key=lambda x: x[1], reverse=True)[:top_n]

    def most_vulnerable_boutiques(self, top_n: int = 10) -> list[dict]:
        """
        Returns the boutiques with the highest total co-appearances across all
        BigLaw firms — these will catch the most overflow when deal flow spikes.
        """
        totals = defaultdict(int)
        for bl, boutiques in self.graph.items():
            for bt, cnt in boutiques.items():
                totals[bt] += cnt

        ranked = sorted(totals.items(), key=lambda x: x[1], reverse=True)[:top_n]
        result = []
        for firm_id, total in ranked:
            firm = FIRM_BY_ID.get(firm_id, {"name": firm_id, "tier": "?"})
            result.append({
                "firm_id":      firm_id,
                "firm_name":    firm.get("name", firm_id),
                "tier":         firm.get("tier", "?"),
                "total_co_app": total,
                "biglaw_links": list(self.graph.keys()),
            })
        return result

    def betweenness_centrality(self) -> dict[str, float]:
        """
        Simple betweenness proxy:
        centrality(boutique) = number of distinct BigLaw firms it co-appears with.
        High centrality → indispensable to the overflow network → most hire-hungry.
        """
        centrality = defaultdict(int)
        for bl, boutiques in self.graph.items():
            for bt in boutiques:
                centrality[bt] += 1
        max_val = max(centrality.values(), default=1)
        return {k: v / max_val for k, v in centrality.items()}

    def predict_spillage_targets(self, biglaw_id: str) -> list[dict]:
        """
        Given a BigLaw firm about to get a big mandate, return the boutiques
        most likely to catch their conflict overflow.
        """
        centrality = self.betweenness_centrality()
        tops       = self.top_boutiques_for(biglaw_id, top_n=5)
        result     = []
        for boutique_id, co_app in tops:
            firm   = FIRM_BY_ID.get(boutique_id, {"name": boutique_id})
            result.append({
                "firm_id":      boutique_id,
                "firm_name":    firm.get("name", boutique_id),
                "co_appearances": co_app,
                "centrality":   centrality.get(boutique_id, 0),
                "confidence":   min(1.0, (co_app / 10) * centrality.get(boutique_id, 0.1)),
            })
        return sorted(result, key=lambda x: x["confidence"], reverse=True)


# ─── News & Deal Monitor ─────────────────────────────────────────────────────

class DealMonitor:
    """
    Watches Google News RSS and SEDAR+ for massive Calgary deals.
    On detection, cross-references the spillage graph and fires alerts.
    """

    def __init__(self):
        self.analyzer    = SpillageGraphAnalyzer()
        self.new_signals: list[dict] = []
        self._seen_links: set[str]  = set()

    def run(self):
        log.info("[SpillageGraph] Running deal monitor")
        self._scan_google_news()
        self._scan_sedar_rss()
        log.info("[SpillageGraph] Done. %d signals.", len(self.new_signals))
        return self.new_signals

    # ── Google News ──────────────────────────────────────────────────────────

    def _scan_google_news(self):
        queries = [
            "Calgary energy merger acquisition billion",
            "Alberta M&A deal announcement 2025 2026",
            "Calgary corporate transaction CCAA restructuring",
            "Alberta oil gas takeover billion",
        ]
        for q in queries:
            url  = GOOGLE_NEWS_RSS.format(query=requests.utils.quote(q))
            feed = feedparser.parse(url)
            for entry in feed.entries:
                self._process_news_item(entry)
            time.sleep(1)

    def _process_news_item(self, entry):
        link    = getattr(entry, "link", "")
        title   = getattr(entry, "title", "")
        summary = getattr(entry, "summary", "")

        if link in self._seen_links:
            return
        self._seen_links.add(link)

        combined = f"{title} {summary}"
        if not MEGA_DEAL_KEYWORDS.search(combined):
            return

        deal_value = self._parse_deal_value(combined)
        if deal_value and deal_value < LARGE_DEAL_THRESHOLD:
            return   # too small to generate overflow

        energy_co = self._find_energy_company(combined)
        biglaw_id  = self._find_biglaw_conflict(energy_co) if energy_co else None

        log.info("[SpillageGraph] Deal found: %s | $%.0fM | energy_co=%s | biglaw=%s",
                 title[:60], deal_value or 0, energy_co, biglaw_id)

        self._fire_spillage_signal(
            headline=title,
            deal_value=deal_value,
            energy_company=energy_co,
            biglaw_id=biglaw_id,
            source_url=link,
            is_mega=(deal_value or 0) >= MEGA_DEAL_THRESHOLD,
        )

    # ── SEDAR+ RSS ───────────────────────────────────────────────────────────

    def _scan_sedar_rss(self):
        try:
            feed = feedparser.parse(SEDAR_RSS)
        except Exception as e:
            log.error("[SpillageGraph] SEDAR RSS error: %s", e)
            return

        for entry in feed.entries:
            title   = getattr(entry, "title", "")
            link    = getattr(entry, "link", "")
            summary = getattr(entry, "summary", "")
            combined = f"{title} {summary}"

            if not MEGA_DEAL_KEYWORDS.search(combined):
                continue

            deal_value = self._parse_deal_value(combined)
            self._fire_spillage_signal(
                headline=title,
                deal_value=deal_value,
                energy_company=self._find_energy_company(combined),
                biglaw_id=None,
                source_url=link,
                is_mega=(deal_value or 0) >= MEGA_DEAL_THRESHOLD,
            )

    # ── Signal generation ────────────────────────────────────────────────────

    def _fire_spillage_signal(self, headline: str, deal_value: float | None,
                               energy_company: str | None, biglaw_id: str | None,
                               source_url: str, is_mega: bool):
        """
        Given a deal, predict which boutiques will catch overflow and fire signals.
        """
        weight_base = (SIGNAL_WEIGHTS["biglaw_spillage_predicted"]
                       if is_mega else SIGNAL_WEIGHTS["sedar_major_deal"])

        # If we know the BigLaw firm, use the graph; otherwise alert all high-centrality boutiques
        if biglaw_id:
            targets = self.analyzer.predict_spillage_targets(biglaw_id)
        else:
            targets = self.analyzer.most_vulnerable_boutiques(top_n=3)

        for target in targets[:3]:   # top 3 predicted overflow recipients
            firm_id   = target["firm_id"]
            firm_name = target.get("firm_name", firm_id)
            deal_str  = f"${deal_value:.0f}M" if deal_value else "multi-$M"
            bl_name   = FIRM_BY_ID.get(biglaw_id, {}).get("name", biglaw_id or "BigLaw") if biglaw_id else "BigLaw"
            co_app    = target.get("co_appearances", target.get("total_co_app", 0))

            msg = (f"SPILLAGE ALERT: {deal_str} deal announced ({headline[:80]}). "
                   f"{bl_name} likely acting for one side — conflict creates overflow. "
                   f"{firm_name} has appeared opposite {bl_name} {co_app}× historically. "
                   f"Email {firm_name} hiring partner TODAY before they post the job.")

            self.new_signals.append({
                "firm_id":     firm_id,
                "signal_type": "biglaw_spillage_predicted",
                "weight":      weight_base * target.get("confidence", 0.5),
                "title":       f"BigLaw spillage → {firm_name}: {deal_str} deal",
                "description": msg,
                "source_url":  source_url,
                "raw_data":    {
                    "headline":        headline[:120],
                    "deal_value_m":    deal_value,
                    "energy_company":  energy_company,
                    "biglaw_id":       biglaw_id,
                    "confidence":      target.get("confidence"),
                },
            })
            insert_signal(
                firm_id=firm_id,
                signal_type="biglaw_spillage_predicted",
                weight=weight_base * target.get("confidence", 0.5),
                title=f"BigLaw spillage → {firm_name}: {deal_str} deal",
                description=msg,
                source_url=source_url,
                raw_data={
                    "headline": headline[:120],
                    "deal_value_m": deal_value,
                    "biglaw_id": biglaw_id,
                },
            )
            log.info("[SpillageGraph] Signal → %s (weight=%.2f)",
                     firm_id, weight_base * target.get("confidence", 0.5))

    # ── Helpers ──────────────────────────────────────────────────────────────

    @staticmethod
    def _parse_deal_value(text: str) -> float | None:
        values = []
        for m in DOLLAR_RE.finditer(text):
            num  = float(m.group(1).replace(",", ""))
            unit = m.group(2).lower()
            if unit in ("billion", "b"):
                num *= 1000
            values.append(num)
        return max(values) if values else None

    @staticmethod
    def _find_energy_company(text: str) -> str | None:
        for co in CALGARY_ENERGY_MAJORS:
            if co.lower() in text.lower():
                return co
        return None

    def _find_biglaw_conflict(self, energy_company: str | None) -> str | None:
        """
        Crude heuristic: look up which BigLaw firm most recently appeared
        for this energy company in CanLII or SEDAR.
        Returns the BigLaw firm_id most likely to have the conflict.
        """
        if not energy_company:
            return None

        conn = get_conn()
        # Check if any BigLaw firm recently appeared on a case involving this company
        rows = conn.execute("""
            SELECT firm_id, count(*) as cnt
            FROM canlii_appearances
            WHERE firm_id IN ({})
              AND (case_title LIKE ? OR counsel_raw LIKE ?)
              AND date(decision_date) >= date('now', '-90 days')
            GROUP BY firm_id
            ORDER BY cnt DESC
            LIMIT 1
        """.format(",".join("?" * len(BIGLAW_FIRMS))),
            list(BIGLAW_FIRMS) + [f"%{energy_company}%", f"%{energy_company}%"]
        ).fetchone()
        conn.close()

        return dict(rows)["firm_id"] if rows else None


# ─── Conflict Radar ──────────────────────────────────────────────────────────

class ConflictRadar:
    """
    Tracks which Calgary BigLaw offices have RECENTLY acted for each energy major.
    If Blakes was Cenovus counsel in the last 90 days, and Cenovus announces a
    new deal, Blakes almost certainly can't act for the other side.
    Returns the predicted "conflict owner" so we know who will overflow.
    """

    def get_recent_counsel_for(self, energy_company: str, days_back: int = 90) -> list[str]:
        """Returns list of BigLaw firm_ids that recently acted for this company."""
        conn = get_conn()
        rows = conn.execute("""
            SELECT DISTINCT ca.firm_id
            FROM canlii_appearances ca
            WHERE ca.firm_id IN ({})
              AND (ca.case_title LIKE ? OR ca.counsel_raw LIKE ?)
              AND date(ca.decision_date) >= date('now', ? || ' days')
        """.format(",".join("?" * len(BIGLAW_FIRMS))),
            list(BIGLAW_FIRMS) + [
                f"%{energy_company}%", f"%{energy_company}%", f"-{days_back}"
            ]
        ).fetchall()
        conn.close()
        return [r["firm_id"] for r in rows]

    def radar_report(self) -> list[dict]:
        """Full conflict radar: for each energy major, which BigLaw has conflict?"""
        radar = []
        for company in CALGARY_ENERGY_MAJORS:
            counsel = self.get_recent_counsel_for(company)
            if counsel:
                radar.append({
                    "energy_company": company,
                    "biglaw_with_conflict": counsel,
                    "free_to_act": [f for f in BIGLAW_FIRMS if f not in counsel],
                })
        return radar


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    monitor = DealMonitor()
    signals = monitor.run()
    for s in signals:
        print(f"  [{s['signal_type']}] {s['firm_id']}: {s['title']}")

    print("\n─── Spillage Graph Top Boutiques ───")
    graph = SpillageGraphAnalyzer()
    for b in graph.most_vulnerable_boutiques(top_n=10):
        print(f"  {b['firm_name']}: {b['total_co_app']} co-appearances")
