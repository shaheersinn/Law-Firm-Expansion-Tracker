"""
signals/advanced/website_headcount.py
───────────────────────────────────────
Signal 9 — Website Headcount Delta

Scrapes each firm's "Our Lawyers" / "Our Team" page weekly.
Tracks:
  1. Total headcount (number of named lawyers)
  2. Practice area breakdowns (if navigable by category)
  3. Page content hash (any change = something happened)
  4. New partner bios (lateral hire confirmation before press release)
  5. Missing lawyer bios (departure confirmation)

Why this matters:
  • If a firm's team page drops from 42 to 39 lawyers → 3 departures → 3 vacancies
  • If it jumps from 39 to 43 → they're expanding → still might need you
  • A new partner bio with no announcement = lateral hire they haven't publicised
  • A brand-new practice area page = they're building in that area = junior demand

Technique: HTML hash + lawyer name set diffing.
No API needed — 100% public pages.
"""

import re, time, hashlib, logging, json
from datetime import date
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import requests
from bs4 import BeautifulSoup

from database.db import insert_signal, get_conn
from config_calgary import CALGARY_FIRMS, FIRM_BY_ID

log = logging.getLogger(__name__)

# Known "team page" URL patterns for each firm
# Override per-firm in config_calgary.py if needed
TEAM_PAGE_SUFFIXES = [
    "/our-lawyers", "/our-people", "/our-team", "/lawyers",
    "/team", "/people", "/attorneys", "/professionals",
    "/en/our-lawyers", "/professionals/lawyers",
]

# Name extraction patterns — most firm pages use structured markup
NAME_SELECTORS = [
    "h2.lawyer-name", "h3.lawyer-name", ".person-name", ".attorney-name",
    "[class*='name']", "h2", "h3", ".staff-name", ".team-member-name",
    "[itemprop='name']",
]

# Practice area page signals
PRACTICE_PAGE_KEYWORDS = re.compile(
    r"\b(new practice|expanding our|recently launched|now offering|"
    r"proud to announce|growing team|joining the practice)\b",
    re.IGNORECASE,
)


class TeamPageScraper:
    """
    Maintains a rolling snapshot of each firm's public team page.
    Fires signals on headcount drops (vacancies) and jumps (expansion).
    """

    def __init__(self):
        self.new_signals: list[dict] = []
        self.session = requests.Session()
        self.session.headers["User-Agent"] = (
            "Mozilla/5.0 (compatible; LawFirmTracker/3.0; research)"
        )
        self._init_db()

    def _init_db(self):
        conn = get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS team_page_snapshots (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_id      TEXT NOT NULL,
                snapped_at   TEXT NOT NULL DEFAULT (datetime('now')),
                url          TEXT,
                page_hash    TEXT,
                lawyer_count INTEGER,
                lawyer_names TEXT,  -- JSON list
                UNIQUE(firm_id, page_hash)
            )""")
        conn.commit()
        conn.close()

    def run(self) -> list[dict]:
        log.info("[TeamPage] Scanning %d firm team pages…", len(CALGARY_FIRMS))
        for firm in CALGARY_FIRMS:
            self._scan_firm(firm)
            time.sleep(2.5)   # polite crawl delay
        log.info("[TeamPage] Done. %d signals.", len(self.new_signals))
        return self.new_signals

    # ── Per-firm scan ──────────────────────────────────────────────────────────

    def _scan_firm(self, firm: dict):
        base_url  = firm.get("website", "")
        if not base_url:
            return

        team_url  = self._find_team_url(base_url)
        if not team_url:
            log.debug("[TeamPage] No team URL found for %s", firm["id"])
            return

        try:
            resp = self.session.get(team_url, timeout=15)
            resp.raise_for_status()
        except Exception as e:
            log.debug("[TeamPage] Fetch failed %s: %s", team_url, e)
            return

        soup        = BeautifulSoup(resp.text, "lxml")
        names       = self._extract_names(soup)
        page_hash   = hashlib.sha256(resp.text.encode()).hexdigest()[:20]
        lawyer_count = len(names)

        previous    = self._load_snapshot(firm["id"])

        if previous:
            prev_hash  = previous["page_hash"]
            prev_count = previous["lawyer_count"]
            prev_names = set(json.loads(previous["lawyer_names"] or "[]"))

            if page_hash == prev_hash:
                log.debug("[TeamPage] %s — no change", firm["id"])
                return   # nothing changed

            # ── Diff analysis ───────────────────────────────────────────────
            current_names = set(names)
            departed      = prev_names - current_names
            new_hires     = current_names - prev_names
            delta         = lawyer_count - prev_count

            log.info("[TeamPage] %s: %+d lawyers | +%d new | -%d departed",
                     firm["name"], delta, len(new_hires), len(departed))

            if len(departed) >= 1:
                self._fire_departure(firm, departed, lawyer_count, team_url)

            if len(new_hires) >= 1:
                self._fire_new_hire(firm, new_hires, lawyer_count, team_url)

            if delta <= -3:
                self._fire_headcount_drop(firm, delta, lawyer_count, team_url)

            if delta >= 3:
                self._fire_headcount_jump(firm, delta, team_url)

        # Save snapshot
        self._save_snapshot(firm["id"], team_url, page_hash, lawyer_count, list(names))

    # ── Signal generators ──────────────────────────────────────────────────────

    def _fire_departure(self, firm: dict, departed: set, total: int, url: str):
        names_str = ", ".join(list(departed)[:5])
        desc = (
            f"Team page diff: {len(departed)} lawyer(s) no longer listed at "
            f"{firm['name']} (total now: {total}). "
            f"Removed: {names_str}. "
            f"Possible departures — unadvertised vacancy signal."
        )
        for name in departed:
            is_new = insert_signal(
                firm_id=firm["id"],
                signal_type="teampage_departure_detected",
                weight=4.0,
                title=f"Team page: {name} no longer listed at {firm['name']}",
                description=desc,
                source_url=url,
                practice_area=firm.get("focus", ["general"])[0],
                raw_data={"departed": list(departed), "new_total": total},
            )
            if is_new:
                self.new_signals.append({
                    "firm_id": firm["id"],
                    "signal_type": "teampage_departure_detected",
                    "weight": 4.0,
                    "title": f"Team page: {name} no longer listed at {firm['name']}",
                    "practice_area": firm.get("focus", ["general"])[0],
                })

    def _fire_new_hire(self, firm: dict, new_hires: set, total: int, url: str):
        names_str = ", ".join(list(new_hires)[:5])
        is_new = insert_signal(
            firm_id=firm["id"],
            signal_type="teampage_new_hire_detected",
            weight=2.5,
            title=f"Team page: {len(new_hires)} new name(s) at {firm['name']}",
            description=(
                f"New lawyers added to {firm['name']} team page (not yet press-released): "
                f"{names_str}. Total lawyers now: {total}. "
                f"Firm is growing — may need additional juniors."
            ),
            source_url=url,
            practice_area=firm.get("focus", ["general"])[0],
            raw_data={"new_hires": list(new_hires), "new_total": total},
        )
        if is_new:
            self.new_signals.append({
                "firm_id": firm["id"],
                "signal_type": "teampage_new_hire_detected",
                "weight": 2.5,
                "title": f"Team page: {len(new_hires)} new name(s) at {firm['name']}",
                "practice_area": firm.get("focus", ["general"])[0],
            })

    def _fire_headcount_drop(self, firm: dict, delta: int, total: int, url: str):
        insert_signal(
            firm_id=firm["id"],
            signal_type="teampage_headcount_drop",
            weight=4.5,
            title=f"Headcount DROP at {firm['name']}: {delta:+d} ({total} total)",
            description=(
                f"{firm['name']} team page shows {abs(delta)} fewer lawyers. "
                f"Significant capacity loss — firm needs to fill chairs urgently."
            ),
            source_url=url,
            practice_area=firm.get("focus", ["general"])[0],
            raw_data={"delta": delta, "total": total},
        )

    def _fire_headcount_jump(self, firm: dict, delta: int, url: str):
        insert_signal(
            firm_id=firm["id"],
            signal_type="teampage_headcount_jump",
            weight=2.0,
            title=f"Headcount JUMP at {firm['name']}: +{delta}",
            description=(
                f"{firm['name']} team page shows {delta} new lawyers. "
                f"Firm is actively growing — may signal continued junior hiring."
            ),
            source_url=url,
            practice_area=firm.get("focus", ["general"])[0],
            raw_data={"delta": delta},
        )

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _find_team_url(self, base_url: str) -> str | None:
        base = base_url.rstrip("/")
        for suffix in TEAM_PAGE_SUFFIXES:
            url = base + suffix
            try:
                r = self.session.head(url, timeout=8, allow_redirects=True)
                if r.status_code == 200:
                    return url
            except Exception:
                pass
            time.sleep(0.3)
        return None

    def _extract_names(self, soup: BeautifulSoup) -> list[str]:
        """Try multiple selectors to extract lawyer names."""
        names = []
        for sel in NAME_SELECTORS:
            try:
                found = soup.select(sel)
                if len(found) >= 3:
                    names = [el.get_text(strip=True) for el in found
                             if 3 < len(el.get_text(strip=True)) < 60]
                    if names:
                        break
            except Exception:
                pass

        # Fallback: count all <a> links containing "lawyer" or "/people/" in href
        if not names:
            for a in soup.find_all("a", href=True):
                if any(k in a["href"].lower() for k in ["/lawyer", "/people/", "/attorney"]):
                    txt = a.get_text(strip=True)
                    if 3 < len(txt) < 60 and " " in txt:
                        names.append(txt)
        return list(set(names))

    def _load_snapshot(self, firm_id: str) -> dict | None:
        conn = get_conn()
        row  = conn.execute("""
            SELECT * FROM team_page_snapshots
            WHERE firm_id = ?
            ORDER BY snapped_at DESC LIMIT 1
        """, (firm_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def _save_snapshot(self, firm_id: str, url: str, page_hash: str,
                       count: int, names: list):
        conn = get_conn()
        conn.execute("""
            INSERT OR IGNORE INTO team_page_snapshots
                (firm_id, url, page_hash, lawyer_count, lawyer_names)
            VALUES (?, ?, ?, ?, ?)
        """, (firm_id, url, page_hash, count, json.dumps(names)))
        conn.commit()
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    scraper = TeamPageScraper()
    sigs = scraper.run()
    for s in sigs:
        print(f"  [{s['signal_type']}] {s['firm_id']}: {s['title']}")
