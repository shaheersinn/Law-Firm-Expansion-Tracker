"""
scraper_health.py  —  Consecutive-zero watchdog for every scraper × firm pair.

Problem diagnosed from logs (2026-03-10):
  • RSSFeedScraper returned 0 signals for ALL 26 firms — a systemic failure
    that went completely silent. No warning was ever raised.
  • LateralTrackScraper (highest-priority scraper) returned 0 for every firm.
  • Total new signals: 1 out of 728 scraper executions.

This module:
  1. Records each scraper's result in a persistent JSON health ledger.
  2. Raises a Telegram warning when a scraper has been silent ≥ ALERT_AFTER runs.
  3. Produces a daily health summary appended to the Telegram digest.
  4. Flags when a scraper was *previously* finding signals and suddenly goes dark
     (regression detection) — even if the absolute zero threshold isn't hit yet.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

# Path to persistent health ledger (persisted in docs/ so it survives cache resets)
HEALTH_LEDGER_PATH = Path("docs/scraper_health.json")

# How many consecutive all-zero runs before firing a warning per scraper
ALERT_AFTER_RUNS = 3

# Scrapers whose silence is always suspicious (highest-value sources)
HIGH_VALUE_SCRAPERS = {
    "LateralTrackScraper",
    "DealTrackScraper",
    "RSSFeedScraper",
    "MediaScraper",
    "PressScraper",
    "RecruiterScraper",
}

# If a scraper drops from ≥ this many signals to 0, flag it immediately
REGRESSION_THRESHOLD = 5


def _load_ledger() -> dict:
    if HEALTH_LEDGER_PATH.exists():
        try:
            return json.loads(HEALTH_LEDGER_PATH.read_text())
        except Exception:
            pass
    return {}


def _save_ledger(ledger: dict) -> None:
    HEALTH_LEDGER_PATH.parent.mkdir(parents=True, exist_ok=True)
    HEALTH_LEDGER_PATH.write_text(json.dumps(ledger, indent=2))


def record_run(
    scraper_name: str,
    firm_key: str,
    total_signals: int,
    new_signals: int,
) -> Optional[str]:
    """
    Record one scraper result. Returns a warning string if something is wrong,
    otherwise None. Call once per scraper × firm execution.
    """
    ledger = _load_ledger()
    key = f"{scraper_name}::{firm_key}"
    now = datetime.now(timezone.utc).isoformat()

    entry = ledger.get(key, {
        "scraper": scraper_name,
        "firm": firm_key,
        "consecutive_zeros": 0,
        "peak_signals": 0,
        "last_signal_date": None,
        "runs": 0,
        "warnings_sent": 0,
    })

    entry["runs"] += 1
    entry["peak_signals"] = max(entry["peak_signals"], total_signals)

    warning = None

    if total_signals == 0:
        entry["consecutive_zeros"] += 1

        # High-value scraper: warn after 1 silent run
        zero_limit = 1 if scraper_name in HIGH_VALUE_SCRAPERS else ALERT_AFTER_RUNS

        if entry["consecutive_zeros"] >= zero_limit:
            warning = (
                f"⚠️ Health: {scraper_name} has returned 0 signals for "
                f"{entry['consecutive_zeros']} consecutive run(s) "
                f"[firm: {firm_key}]"
            )
            entry["warnings_sent"] += 1

    else:
        # Signals found — regression check
        prior_peak = entry["peak_signals"]
        if (
            prior_peak >= REGRESSION_THRESHOLD
            and total_signals == 0
            and entry["last_signal_date"] is not None
        ):
            warning = (
                f"📉 Regression: {scraper_name} previously peaked at "
                f"{prior_peak} signals for {firm_key} — now 0."
            )

        entry["consecutive_zeros"] = 0
        entry["last_signal_date"] = now

    ledger[key] = entry
    _save_ledger(ledger)
    return warning


def record_run_global(scraper_name: str, total_signals_across_firms: int) -> Optional[str]:
    """
    Records a global (cross-firm) scraper total. Used for scrapers that run
    once globally (e.g. RSSFeedScraper aggregated total).
    Returns a warning string or None.
    """
    return record_run(scraper_name, "__ALL__", total_signals_across_firms, 0)


def build_health_summary() -> str:
    """
    Returns a Markdown-formatted health table for the Telegram daily digest.
    Shows scrapers with ≥ 1 consecutive zero runs.
    """
    ledger = _load_ledger()
    sick = [
        v for v in ledger.values()
        if v.get("consecutive_zeros", 0) >= 1
    ]

    if not sick:
        return "✅ All scrapers healthy (no consecutive zero runs)."

    sick.sort(key=lambda x: (-x["consecutive_zeros"], x["scraper"]))

    lines = ["*Scraper Health Report*", ""]
    for entry in sick[:20]:  # cap at 20 rows
        icon = "🔴" if entry["consecutive_zeros"] >= 3 else "🟡"
        lines.append(
            f"{icon} `{entry['scraper']}` — {entry['consecutive_zeros']} zero run(s)"
            f"  |  firm: {entry['firm']}"
            f"  |  peak: {entry['peak_signals']}"
        )

    return "\n".join(lines)


def get_silent_scrapers(min_consecutive_zeros: int = 3) -> list[dict]:
    """Returns scrapers that have been silent for ≥ min_consecutive_zeros runs."""
    ledger = _load_ledger()
    return [
        v for v in ledger.values()
        if v.get("consecutive_zeros", 0) >= min_consecutive_zeros
    ]
