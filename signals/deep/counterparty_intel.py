"""
signals/deep/counterparty_intel.py
────────────────────────────────────
Counterparty Intelligence — Full Counsel Team Extraction

Previous SEDAR monitoring detected when a firm was named as counsel.
This goes further: it extracts ALL counsel teams from the deal —
issuer counsel, underwriter counsel, target counsel, acquiror counsel —
because EVERY team on a deal needs juniors.

Also extracts:
  - Lead underwriters (investment banks) → their legal counsel
  - Independent legal counsel (ILC) for special committees
  - Trustees (in debt offerings) → Osler / Stikeman trust team
  - Accountants/auditors → their outside legal counsel

A $2B deal often involves 4-6 distinct law firms, each with different
junior needs. Previous versions only caught the most prominent.

Additionally implements:
  POST-CLOSE INTEGRATION SIGNAL
  When a deal that was announced 30-90 days ago is now "closed" (see
  press release on CNW/GlobeNewswire or SEDAR Material Change report),
  the post-close integration work begins IMMEDIATELY:
  - Regulatory filings (NEB, CER, AER, ASC)
  - Name changes, amalgamations, property transfers
  - Employment contracts, benefit plan transitions
  - Tax reorganizations

  The post-close phase creates a 30-90 day surge of junior work at
  EVERY firm that was involved in the deal.
"""

import re, logging, hashlib, json, io
from datetime import date, datetime, timedelta
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import requests, feedparser

from database.db import insert_signal, get_conn
from config_calgary import CALGARY_FIRMS, FIRM_BY_ID, FIRM_ALIASES

log = logging.getLogger(__name__)

# Counsel role labels in deal documents
COUNSEL_ROLE_RE = re.compile(
    r"""
    (?:
        (?P<role>
            issuer(?:'s)? counsel|company(?:'s)? counsel|counsel to the issuer|
            underwriter(?:s')? counsel|counsel to the underwriter|
            target(?:'s)? counsel|acquiror(?:'s)? counsel|purchaser(?:'s)? counsel|
            vendor(?:'s)? counsel|seller(?:'s)? counsel|
            independent legal counsel|ILC|special committee counsel|
            trustee(?:'s)? counsel|indenture trustee|
            lender(?:s')? counsel|agent(?:'s)? counsel|administrative agent
        )
        [:\s,]+
        (?P<firm>[A-Z][A-Za-z\s,&]+?(?:LLP|LLC|PC|Corp\.?|Law))
    )
    """,
    re.VERBOSE | re.IGNORECASE,
)

# "Closing" keywords — deal is now complete
CLOSING_RE = re.compile(
    r"\b(closes|closing|has closed|has completed|transaction closed|"
    r"acquisition closed|merger completed|completion of the|"
    r"successful closing|effective date)\b",
    re.IGNORECASE,
)

DOLLAR_RE = re.compile(r"\$\s*([\d,\.]+)\s*(billion|million|B|M)\b", re.IGNORECASE)


def _parse_value(text: str) -> float | None:
    for m in DOLLAR_RE.finditer(text):
        num  = float(m.group(1).replace(",",""))
        unit = m.group(2).lower()
        if unit in ("billion","b"): num *= 1000
        return num
    return None


def _map_firm_name_to_id(firm_name: str) -> str | None:
    """Map a free-text firm name to a firm_id using aliases."""
    fn_lower = firm_name.lower().strip()
    # Try exact alias match
    if fn_lower in FIRM_ALIASES:
        return FIRM_ALIASES[fn_lower]
    # Try substring match
    for alias, fid in FIRM_ALIASES.items():
        if len(alias) >= 6 and alias in fn_lower:
            return fid
    return None


class CounterpartyIntelExtractor:
    """
    Deep PDF parser for SEDAR+ transaction documents.
    Extracts every counsel team and fires signals for all of them.
    """

    def __init__(self):
        self.new_signals: list[dict] = []
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "LawTracker/5.0 (research; admin@example.com)"
        self._init_db()

    def _init_db(self):
        conn = get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS deal_counsel_teams (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                deal_hash       TEXT NOT NULL,
                filing_url      TEXT,
                deal_name       TEXT,
                deal_value_m    REAL,
                counsel_firm_id TEXT NOT NULL,
                counsel_role    TEXT,
                is_calgary_firm INTEGER DEFAULT 0,
                deal_stage      TEXT DEFAULT 'announced',
                recorded_at     TEXT DEFAULT (date('now')),
                UNIQUE(deal_hash, counsel_firm_id, counsel_role)
            )""")
        conn.commit()
        conn.close()

    def run(self) -> list[dict]:
        log.info("[Counterparty] Scanning SEDAR+ for full counsel team extraction…")
        self._poll_sedar_rss()
        self._detect_post_close_surges()
        log.info("[Counterparty] Done. %d signals.", len(self.new_signals))
        return self.new_signals

    # ── SEDAR polling ──────────────────────────────────────────────────────────

    def _poll_sedar_rss(self):
        from config_calgary import SEDAR_RSS_URL
        try:
            feed = feedparser.parse(SEDAR_RSS_URL)
        except Exception as e:
            log.debug("[Counterparty] SEDAR RSS error: %s", e); return

        for entry in feed.entries:
            title   = getattr(entry, "title",   "")
            link    = getattr(entry, "link",    "")
            summary = getattr(entry, "summary", "")
            combined = f"{title} {summary}"

            # Only process deal documents
            if not re.search(
                r"(prospectus|circular|arrangement|take-over|private placement|"
                r"business acquisition|M&A|amalgamation)", combined, re.I
            ):
                continue

            # Try to get the full PDF for deep parsing
            if link.endswith(".pdf"):
                self._parse_sedar_pdf(link, combined, title)
            else:
                # Parse the HTML summary for basic counsel extraction
                self._parse_text_for_counsel(combined, link, title)

    def _parse_sedar_pdf(self, url: str, combined_text: str, title: str):
        """Download and parse SEDAR PDF for full counsel team extraction."""
        try:
            import pdfplumber
            resp = self.session.get(url, timeout=25)
            resp.raise_for_status()
            with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
                # First 20 pages usually contain counsel list
                text = " ".join(
                    page.extract_text() or "" for page in pdf.pages[:20]
                )
            self._parse_text_for_counsel(text, url, title)
        except Exception as e:
            log.debug("[Counterparty] PDF parse failed: %s", e)

    def _parse_text_for_counsel(self, text: str, url: str, title: str):
        """Extract all counsel roles from document text."""
        deal_hash   = hashlib.md5(url.encode()).hexdigest()[:16]
        deal_value  = _parse_value(text)
        is_closing  = bool(CLOSING_RE.search(text))

        # Extract all counsel role-firm pairs
        found_teams = []
        for m in COUNSEL_ROLE_RE.finditer(text):
            role      = m.group("role").strip()
            firm_name = m.group("firm").strip().rstrip(",;.")
            firm_id   = _map_firm_name_to_id(firm_name)
            if firm_id:
                found_teams.append({"firm_id": firm_id, "role": role,
                                    "firm_name": firm_name})

        if not found_teams:
            return

        # Deduplicate by firm_id+role
        seen = set()
        unique_teams = []
        for t in found_teams:
            key = f"{t['firm_id']}|{t['role']}"
            if key not in seen:
                seen.add(key)
                unique_teams.append(t)

        log.info("[Counterparty] %s — %d counsel teams found: %s",
                 title[:50], len(unique_teams),
                 ", ".join(f"{t['firm_id']}({t['role'][:20]})" for t in unique_teams[:4]))

        # Store in deal_counsel_teams
        conn = get_conn()
        for t in unique_teams:
            is_calgary = int(bool(FIRM_BY_ID.get(t["firm_id"])))
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO deal_counsel_teams
                        (deal_hash, filing_url, deal_name, deal_value_m,
                         counsel_firm_id, counsel_role, is_calgary_firm, deal_stage)
                    VALUES (?,?,?,?,?,?,?,?)
                """, (deal_hash, url, title[:100], deal_value,
                      t["firm_id"], t["role"], is_calgary,
                      "closed" if is_closing else "announced"))
            except Exception:
                pass
        conn.commit()
        conn.close()

        # Fire signals for each Calgary firm on the deal
        for t in unique_teams:
            firm = FIRM_BY_ID.get(t["firm_id"])
            if not firm:
                continue

            all_roles = ", ".join(f"{x['firm_name']} ({x['role']})" for x in unique_teams)

            # Weight: closing deals = highest (post-close surge)
            if is_closing:
                weight   = 5.0
                sig_type = "deal_post_close_surge"
                extra    = (
                    f"DEAL CLOSING DETECTED — post-close integration surge beginning now. "
                    f"Expect 30-90 days of heavy junior work: regulatory filings, "
                    f"amalgamations, property transfers, employment transitions."
                )
            else:
                weight   = 4.0 + (0.5 if (deal_value or 0) >= 500 else 0)
                sig_type = "counterparty_counsel_team"
                extra    = f"All counsel teams on this deal: {all_roles[:200]}."

            desc = (
                f"[SEDAR+ Counterparty Intel] {firm['name']} acting as {t['role']} "
                f"on: {title[:80]}. "
                f"Deal value: {'$'+str(deal_value)+'M' if deal_value else 'undisclosed'}. "
                f"{extra}"
            )

            is_new = insert_signal(
                firm_id=t["firm_id"],
                signal_type=sig_type,
                weight=weight,
                title=f"[{sig_type}] {firm['name']}: {t['role']} — {title[:50]}",
                description=desc,
                source_url=url,
                practice_area="corporate",
                raw_data={
                    "role":        t["role"],
                    "deal_hash":   deal_hash,
                    "deal_value":  deal_value,
                    "is_closing":  is_closing,
                    "all_teams":   [x["firm_id"] for x in unique_teams],
                },
            )
            if is_new:
                self.new_signals.append({
                    "firm_id": t["firm_id"],
                    "signal_type": sig_type,
                    "weight": weight,
                    "title": f"{firm['name']}: {t['role'][:30]} on {title[:40]}",
                    "practice_area": "corporate",
                    "description": desc,
                })

    # ── Post-close surge detection ─────────────────────────────────────────────

    def _detect_post_close_surges(self):
        """
        Find deals that were announced 30-90 days ago and are now showing
        'closing' signals. Fire post-close surge for ALL firms on the deal.
        """
        conn    = get_conn()
        pending = conn.execute("""
            SELECT DISTINCT deal_hash, deal_name, deal_value_m, filing_url
            FROM deal_counsel_teams
            WHERE deal_stage='announced'
              AND date(recorded_at) BETWEEN date('now','-90 days') AND date('now','-30 days')
        """).fetchall()
        conn.close()

        for deal in pending:
            # Check CNW/Newswire for closing announcement
            self._check_deal_closed(dict(deal))

    def _check_deal_closed(self, deal: dict):
        """
        Search Google News for deal name + "closed" / "completed".
        If found, promote the deal to 'closed' and fire surge signals.
        """
        import feedparser as fp
        query = f"{deal['deal_name'][:50]} transaction closed completed"
        url   = f"https://news.google.com/rss/search?q={requests.utils.quote(query)}&hl=en-CA"
        try:
            feed = fp.parse(url)
            for entry in feed.entries[:5]:
                title = getattr(entry, "title", "")
                if CLOSING_RE.search(title):
                    self._promote_deal_to_closed(deal["deal_hash"])
                    break
        except Exception:
            pass

    def _promote_deal_to_closed(self, deal_hash: str):
        """Mark all teams on this deal as 'closed' and fire post-close signals."""
        conn  = get_conn()
        teams = conn.execute("""
            SELECT counsel_firm_id, counsel_role, deal_name, deal_value_m, filing_url
            FROM deal_counsel_teams
            WHERE deal_hash=? AND deal_stage='announced'
        """, (deal_hash,)).fetchall()

        conn.execute(
            "UPDATE deal_counsel_teams SET deal_stage='closed' WHERE deal_hash=?",
            (deal_hash,)
        )
        conn.commit()
        conn.close()

        for row in teams:
            firm = FIRM_BY_ID.get(row["counsel_firm_id"])
            if not firm:
                continue
            insert_signal(
                firm_id=row["counsel_firm_id"],
                signal_type="deal_post_close_surge",
                weight=5.0,
                title=f"POST-CLOSE SURGE: {firm['name']} — {row['deal_name'][:50]}",
                description=(
                    f"Deal has closed: {row['deal_name']}. "
                    f"{firm['name']} acting as {row['counsel_role']}. "
                    f"Post-close integration surge beginning — 30-90 days of heavy "
                    f"junior work starting immediately."
                ),
                source_url=row["filing_url"] or "",
                practice_area="corporate",
            )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    extractor = CounterpartyIntelExtractor()
    for s in extractor.run():
        print(f"  [{s['signal_type']}] {s['firm_id']}: {s['title']}")
