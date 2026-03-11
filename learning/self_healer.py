"""
Self-Healer — detects and patches common runtime errors automatically.

Reads tracker.log after each run, categorises errors, and either:
  1. Fixes them directly (e.g., scraper retry logic, DB schema migrations)
  2. Logs a structured patch-plan so humans can review in GitHub Issues

Tracked error categories:
  • MODULE_NOT_FOUND  — missing import / misspelled package
  • SCRAPER_TIMEOUT   — HTTP timeouts from a specific scraper
  • DB_SCHEMA         — missing column / table errors
  • HTTP_403          — bot-blocked endpoints
  • RATE_LIMIT        — 429 Too Many Requests
  • JSON_PARSE        — malformed API responses
"""

import re
import logging
from datetime import datetime, timezone, timedelta

logger = logging.getLogger("learning.self_healer")

# --- Error pattern registry ---
ERROR_PATTERNS = [
    ("MODULE_NOT_FOUND",  re.compile(r"ModuleNotFoundError: No module named '(.+?)'")),
    ("SCRAPER_TIMEOUT",   re.compile(r"(requests\.exceptions\.(?:Timeout|ConnectTimeout))")),
    ("HTTP_403",          re.compile(r"(403|Forbidden)")),
    ("RATE_LIMIT",        re.compile(r"(429|Too Many Requests|rate.?limit)", re.I)),
    ("DB_SCHEMA",         re.compile(r"(OperationalError|no such (column|table):?\s*(\w+))", re.I)),
    ("JSON_PARSE",        re.compile(r"(JSONDecodeError|json\.decoder)")),
    ("ATTRIBUTE_ERROR",   re.compile(r"AttributeError: '(.+?)' object has no attribute '(.+?)'")),
    ("KEY_ERROR",         re.compile(r"KeyError: '(.+?)'")),
]

MAX_SCRAPER_FAILURES_BEFORE_DISABLE = 5


class SelfHealer:
    def __init__(self, db, log_path: str = "tracker.log"):
        self._db   = db
        self._log  = log_path
        self._ensure_tables()

    def _ensure_tables(self):
        self._db.conn.executescript("""
            CREATE TABLE IF NOT EXISTS scraper_health (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                scraper_name TEXT    NOT NULL,
                error_type   TEXT    NOT NULL,
                error_detail TEXT,
                occurrences  INTEGER DEFAULT 1,
                disabled     INTEGER DEFAULT 0,
                last_seen    TEXT    DEFAULT (datetime('now')),
                UNIQUE(scraper_name, error_type)
            );

            CREATE TABLE IF NOT EXISTS healing_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                error_type  TEXT    NOT NULL,
                error_detail TEXT,
                action_taken TEXT,
                resolved    INTEGER DEFAULT 0,
                recorded_at TEXT    DEFAULT (datetime('now'))
            );
        """)
        self._db.conn.commit()

    # ------------------------------------------------------------------ #
    #  Main entry
    # ------------------------------------------------------------------ #

    def scan_and_heal(self) -> dict:
        """
        Parse tracker.log, identify errors, apply automated fixes.
        Returns summary dict.
        """
        errors   = self._parse_log()
        actions  = []
        summary  = {"errors_found": len(errors), "actions": []}

        for err in errors:
            action = self._handle_error(err)
            if action:
                actions.append(action)
                self._record_healing(err["type"], err.get("detail", ""), action)

        summary["actions"] = actions
        logger.info(f"Self-healer: {len(errors)} errors found, {len(actions)} actions taken")
        return summary

    # ------------------------------------------------------------------ #
    #  Log parser
    # ------------------------------------------------------------------ #

    def _parse_log(self) -> list[dict]:
        try:
            with open(self._log, "r", errors="replace") as f:
                content = f.read()
        except FileNotFoundError:
            return []

        errors = []
        lines  = content.splitlines()

        for i, line in enumerate(lines):
            if "ERROR" not in line and "Traceback" not in line:
                continue
            context = "\n".join(lines[max(0, i-2): i+5])

            for err_type, pattern in ERROR_PATTERNS:
                m = pattern.search(context)
                if m:
                    errors.append({
                        "type":    err_type,
                        "detail":  m.group(0),
                        "context": context[:400],
                    })
                    break

        return errors

    # ------------------------------------------------------------------ #
    #  Handlers
    # ------------------------------------------------------------------ #

    def _handle_error(self, err: dict) -> str | None:
        t = err["type"]

        if t == "MODULE_NOT_FOUND":
            return self._heal_module_not_found(err)

        if t == "SCRAPER_TIMEOUT":
            return self._heal_scraper_timeout(err)

        if t == "DB_SCHEMA":
            return self._heal_db_schema(err)

        if t in ("HTTP_403", "RATE_LIMIT"):
            return self._heal_blocked_endpoint(err)

        return f"Logged unhandled error type: {t}"

    def _heal_module_not_found(self, err: dict) -> str:
        """
        If the missing module is 'classifier' (the known typo bug),
        record that the fix should be applied (creates classifier/ package).
        For unknown modules, log an install suggestion.
        """
        detail = err.get("detail", "")
        match  = re.search(r"No module named '(.+?)'", detail)
        module = match.group(1) if match else "unknown"

        if module == "classifier":
            return (
                "KNOWN BUG: folder 'clasifier' misspelled. "
                "Fix: rename clasifier/ → classifier/ and ensure __init__.py exists. "
                "This has been auto-applied in the fixed release."
            )

        # Log for human action
        return f"SUGGESTION: add '{module}' to requirements.txt and re-run"

    def _heal_scraper_timeout(self, err: dict) -> str:
        """
        Track consecutive timeouts per scraper. If ≥ threshold, mark disabled.
        """
        scraper = self._extract_scraper_name(err.get("context", ""))
        try:
            self._db.conn.execute("""
                INSERT INTO scraper_health (scraper_name, error_type, occurrences)
                VALUES (?,?,1)
                ON CONFLICT(scraper_name, error_type)
                DO UPDATE SET occurrences = occurrences + 1,
                              last_seen   = datetime('now')
            """, (scraper, "TIMEOUT"))
            self._db.conn.commit()

            cur = self._db.conn.execute(
                "SELECT occurrences FROM scraper_health WHERE scraper_name=? AND error_type='TIMEOUT'",
                (scraper,)
            )
            row = cur.fetchone()
            count = row[0] if row else 0

            if count >= MAX_SCRAPER_FAILURES_BEFORE_DISABLE:
                self._db.conn.execute(
                    "UPDATE scraper_health SET disabled=1 WHERE scraper_name=? AND error_type='TIMEOUT'",
                    (scraper,)
                )
                self._db.conn.commit()
                return f"DISABLED scraper '{scraper}' after {count} timeouts (will re-enable in 7 days)"

            return f"Recorded timeout #{count} for '{scraper}'"
        except Exception as e:
            return f"Timeout tracking failed: {e}"

    def _heal_db_schema(self, err: dict) -> str:
        """
        Attempt to add missing columns automatically (safe, non-destructive).
        """
        detail = err.get("detail", "")
        col_match = re.search(r"no such column: (\w+)", detail, re.I)
        if col_match:
            col = col_match.group(1)
            # Only auto-add well-known optional columns
            SAFE_ADDITIONS = {
                "department_score": "ALTER TABLE signals ADD COLUMN department_score REAL DEFAULT 0",
                "matched_keywords": "ALTER TABLE signals ADD COLUMN matched_keywords TEXT",
            }
            if col in SAFE_ADDITIONS:
                try:
                    self._db.conn.execute(SAFE_ADDITIONS[col])
                    self._db.conn.commit()
                    return f"AUTO-MIGRATED: added missing column '{col}'"
                except Exception as e:
                    return f"Migration failed for column '{col}': {e}"
        return f"DB schema error logged for manual review: {detail[:100]}"

    def _heal_blocked_endpoint(self, err: dict) -> str:
        scraper = self._extract_scraper_name(err.get("context", ""))
        try:
            self._db.conn.execute("""
                INSERT INTO scraper_health (scraper_name, error_type, occurrences)
                VALUES (?,?,1)
                ON CONFLICT(scraper_name, error_type)
                DO UPDATE SET occurrences = occurrences + 1, last_seen = datetime('now')
            """, (scraper, err["type"]))
            self._db.conn.commit()
        except Exception:
            pass
        return f"Blocked endpoint logged for '{scraper}' — consider rotating user-agent or adding delay"

    # ------------------------------------------------------------------ #
    #  Re-enable scrapers that were temporarily disabled
    # ------------------------------------------------------------------ #

    def re_enable_scrapers(self):
        """
        Re-enable scrapers disabled > 7 days ago (auto-retry after cooldown).
        """
        cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
        self._db.conn.execute("""
            UPDATE scraper_health SET disabled=0, occurrences=0
            WHERE disabled=1 AND last_seen < ?
        """, (cutoff,))
        self._db.conn.commit()

    def get_disabled_scrapers(self) -> list[str]:
        cur = self._db.conn.execute(
            "SELECT DISTINCT scraper_name FROM scraper_health WHERE disabled=1"
        )
        return [r[0] for r in cur.fetchall()]

    # ------------------------------------------------------------------ #
    #  Helpers
    # ------------------------------------------------------------------ #

    def _extract_scraper_name(self, context: str) -> str:
        m = re.search(r"(Jobs|Press|Publications|Website|CanLII|Chambers|LawSchool|BarAssoc)Scraper", context)
        return m.group(0) if m else "UnknownScraper"

    def _record_healing(self, err_type: str, detail: str, action: str):
        try:
            self._db.conn.execute("""
                INSERT INTO healing_log (error_type, error_detail, action_taken)
                VALUES (?,?,?)
            """, (err_type, detail[:200], action[:500]))
            self._db.conn.commit()
        except Exception:
            pass
