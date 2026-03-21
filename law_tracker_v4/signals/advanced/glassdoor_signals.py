"""
signals/advanced/glassdoor_signals.py
───────────────────────────────────────
Signal 11 — Glassdoor / Indeed Workload Sentiment Monitor

Scrapes public Glassdoor and Indeed reviews for the 30 target firms.
Applies keyword NLP to detect:
  • Overwork signals: "understaffed", "too much work", "billing pressure", "burnout"
  • Turnover risk: "looking for other jobs", "high turnover", "people leaving"
  • Hiring signals: "growing fast", "lots of new clients", "expanding"
  • Dysfunction signals: "poor management", "toxic" → suggests people leaving

Glassdoor reviews are public. Indeed company reviews are public.

Scoring:
  • Overwork sentiment spike → high likelihood of imminent vacancy (people burning out)
  • Recent "left the firm" trend → open chairs
  • "Growing" + "busy" → expansion mode → need juniors

NOTE: This is public review data. We do not log in or scrape private pages.
Uses the unofficial Glassdoor public review endpoint (no auth required for
public employer pages).
"""

import re, time, logging, json
from datetime import datetime, date, timedelta
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import requests
from bs4 import BeautifulSoup

from database.db import insert_signal, get_conn
from config_calgary import CALGARY_FIRMS, FIRM_BY_ID

log = logging.getLogger(__name__)

INDEED_BASE     = "https://www.indeed.com/cmp/{slug}/reviews"
GLASSDOOR_BASE  = "https://www.glassdoor.ca/Reviews/{slug}-reviews-SRCH_KE0,{n}.htm"

# Sentiment word lists
OVERWORK_TOKENS = [
    "understaffed", "overworked", "burnout", "burn out", "too much work",
    "no work-life balance", "billing pressure", "unrealistic billable",
    "too many files", "resource constrained", "stretched thin",
    "need more people", "short staffed", "short-staffed",
]
TURNOVER_TOKENS = [
    "high turnover", "people leaving", "revolving door", "hard to retain",
    "brain drain", "associates leave", "attrition", "quit",
    "left the firm", "mass exodus",
]
GROWTH_TOKENS = [
    "growing fast", "lots of new clients", "expanding practice",
    "busy", "deal flow", "lots of work", "high demand", "booming",
    "exciting growth", "new practice area",
]
DYSFUNCTION_TOKENS = [
    "toxic", "poor management", "hostile", "disorganized",
    "no mentorship", "sink or swim", "thrown in the deep end",
]


def _score_text(text: str) -> dict:
    """
    Returns sentiment scores across four dimensions.
    Higher score = stronger signal.
    """
    text_lower = text.lower()
    return {
        "overwork":    sum(1 for t in OVERWORK_TOKENS    if t in text_lower),
        "turnover":    sum(1 for t in TURNOVER_TOKENS    if t in text_lower),
        "growth":      sum(1 for t in GROWTH_TOKENS      if t in text_lower),
        "dysfunction": sum(1 for t in DYSFUNCTION_TOKENS if t in text_lower),
    }


# Glassdoor slugs for Calgary firms (approximate — verify manually)
GLASSDOOR_SLUGS = {
    "mccarthy":      "McCarthy-Tetrault",
    "blakes":        "Blake-Cassels-Graydon",
    "bennett_jones": "Bennett-Jones",
    "norton_rose":   "Norton-Rose-Fulbright-Canada",
    "osler":         "Osler-Hoskin-Harcourt",
    "burnet":        "Burnet-Duckworth-Palmer",
    "field_law":     "Field-Law",
    "miller_thomson":"Miller-Thomson",
    "gowling":       "Gowling-WLG",
    "borden_ladner": "Borden-Ladner-Gervais",
    "fmc_law":       "Fasken-Martineau",
    "stikeman":      "Stikeman-Elliott",
    "parlee_mclaws": "Parlee-McLaws",
    "dentons":       "Dentons",
}


class GlassdoorSentimentMonitor:
    """
    Pulls recent public Glassdoor reviews and scores them for workload signals.
    Fires alerts when overwork or turnover vocabulary spikes.
    """

    def __init__(self):
        self.new_signals: list[dict] = []
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent":       "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                                "AppleWebKit/537.36 (KHTML, like Gecko) "
                                "Chrome/122.0.0.0 Safari/537.36",
            "Accept-Language":  "en-CA,en;q=0.9",
        })
        self._init_db()

    def _init_db(self):
        conn = get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS glassdoor_snapshots (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_id       TEXT NOT NULL,
                snapped_at    TEXT DEFAULT (datetime('now')),
                overwork_score  INTEGER DEFAULT 0,
                turnover_score  INTEGER DEFAULT 0,
                growth_score    INTEGER DEFAULT 0,
                review_count    INTEGER DEFAULT 0,
                sample_quotes   TEXT
            )""")
        conn.commit()
        conn.close()

    def run(self) -> list[dict]:
        log.info("[Glassdoor] Scanning %d firm pages…", len(GLASSDOOR_SLUGS))
        for firm_id, slug in GLASSDOOR_SLUGS.items():
            self._scan_firm(firm_id, slug)
            time.sleep(3.0)   # polite delay
        log.info("[Glassdoor] Done. %d signals.", len(self.new_signals))
        return self.new_signals

    def _scan_firm(self, firm_id: str, slug: str):
        firm  = FIRM_BY_ID.get(firm_id, {})
        url   = GLASSDOOR_BASE.format(slug=slug, n=len(slug))
        try:
            resp  = self.session.get(url, timeout=15)
            if resp.status_code != 200:
                log.debug("[Glassdoor] %s returned %d", firm_id, resp.status_code)
                return
            soup  = BeautifulSoup(resp.text, "lxml")
        except Exception as e:
            log.debug("[Glassdoor] Fetch failed %s: %s", firm_id, e)
            return

        # Extract review text blocks
        review_blocks = soup.select(
            ".review-text, .reviewBodyCell, .mt-md, [class*='review'], "
            "[data-test='review-text'], .pros, .cons"
        )
        if not review_blocks:
            log.debug("[Glassdoor] No review blocks found for %s", firm_id)
            return

        all_text      = " ".join(b.get_text(" ", strip=True) for b in review_blocks)
        review_count  = len(review_blocks)
        scores        = _score_text(all_text)

        log.info("[Glassdoor] %s: overwork=%d turnover=%d growth=%d (%d blocks)",
                 firm_id, scores["overwork"], scores["turnover"],
                 scores["growth"], review_count)

        # Load previous snapshot for comparison
        prev = self._load_prev(firm_id)
        self._save_snapshot(firm_id, scores, review_count, all_text[:400])

        overwork_delta  = scores["overwork"]  - (prev["overwork_score"]  if prev else 0)
        turnover_delta  = scores["turnover"]  - (prev["turnover_score"]  if prev else 0)

        # ── Fire signals ──────────────────────────────────────────────────────
        if scores["overwork"] >= 3 or overwork_delta >= 2:
            self._fire(firm_id, firm, "glassdoor_overwork_spike",
                       weight=3.5,
                       title=f"Glassdoor: '{firm.get('name',firm_id)}' — overwork vocabulary spike ({scores['overwork']} mentions)",
                       desc=(
                           f"Recent Glassdoor reviews for {firm.get('name',firm_id)} contain "
                           f"{scores['overwork']} overwork-related terms ('understaffed', 'billing pressure', etc.). "
                           f"Associates burning out → vacancies imminent."
                       ),
                       pa="general", url=url)

        if scores["turnover"] >= 2 or turnover_delta >= 1:
            self._fire(firm_id, firm, "glassdoor_turnover_risk",
                       weight=3.5,
                       title=f"Glassdoor: '{firm.get('name',firm_id)}' — high turnover vocabulary",
                       desc=(
                           f"Glassdoor reviews mention {scores['turnover']} turnover signals "
                           f"('revolving door', 'people leaving', 'high attrition'). "
                           f"Structural vacancy pattern — chairs will open regularly."
                       ),
                       pa="general", url=url)

        if scores["growth"] >= 3:
            self._fire(firm_id, firm, "glassdoor_growth_signal",
                       weight=2.5,
                       title=f"Glassdoor: '{firm.get('name',firm_id)}' — active growth vocabulary",
                       desc=(
                           f"Glassdoor reviews mention {scores['growth']} growth terms "
                           f"('deal flow', 'booming', 'lots of work'). Firm in expansion mode."
                       ),
                       pa="general", url=url)

    def _fire(self, firm_id, firm, sig_type, weight, title, desc, pa, url):
        is_new = insert_signal(
            firm_id=firm_id, signal_type=sig_type, weight=weight,
            title=title, description=desc, source_url=url, practice_area=pa,
        )
        if is_new:
            self.new_signals.append({
                "firm_id": firm_id, "signal_type": sig_type,
                "weight": weight, "title": title, "practice_area": pa,
            })

    def _load_prev(self, firm_id: str) -> dict | None:
        conn = get_conn()
        row  = conn.execute("""
            SELECT * FROM glassdoor_snapshots WHERE firm_id=?
            ORDER BY snapped_at DESC LIMIT 1
        """, (firm_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def _save_snapshot(self, firm_id, scores, count, quotes):
        conn = get_conn()
        conn.execute("""
            INSERT INTO glassdoor_snapshots
                (firm_id, overwork_score, turnover_score, growth_score,
                 review_count, sample_quotes)
            VALUES (?,?,?,?,?,?)
        """, (firm_id, scores["overwork"], scores["turnover"],
              scores["growth"], count, quotes))
        conn.commit()
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    mon = GlassdoorSentimentMonitor()
    sigs = mon.run()
    for s in sigs:
        print(f"  [{s['signal_type']}] {s['firm_id']}: {s['title']}")
