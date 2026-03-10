"""
Configuration loader — reads from environment variables.
All secrets live in GitHub Secrets; never hard-coded here.
"""

import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    # ── Telegram ──────────────────────────────────────────────────────────
    TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID: str   = os.getenv("TELEGRAM_CHAT_ID", "")

    # ── Dashboard ─────────────────────────────────────────────────────────
    DASHBOARD_URL: str = os.getenv("DASHBOARD_URL", "")

    # ── Behaviour ─────────────────────────────────────────────────────────
    INSTANT_ALERT_ON_LATERAL: bool = (
        os.getenv("INSTANT_ALERT_ON_LATERAL", "true").lower() == "true"
    )

    # ── Database ──────────────────────────────────────────────────────────
    DB_PATH: str = os.getenv("DB_PATH", "law_firm_tracker.db")

    # ── HTTP ──────────────────────────────────────────────────────────────
    REQUEST_TIMEOUT: int  = int(os.getenv("REQUEST_TIMEOUT", "20"))
    MIN_DELAY_SECS: float = float(os.getenv("MIN_DELAY_SECS", "1.0"))
    MAX_DELAY_SECS: float = float(os.getenv("MAX_DELAY_SECS", "3.0"))

    # ── Signal filtering ──────────────────────────────────────────────────
    # How many days back to accept signals (default 21 days)
    # Override via GitHub Secret: SIGNAL_LOOKBACK_DAYS=14
    SIGNAL_LOOKBACK_DAYS: int = int(os.getenv("SIGNAL_LOOKBACK_DAYS", "21"))

    # ── RSS ───────────────────────────────────────────────────────────────
    # Max entries to process per feed per firm per run
    RSS_MAX_ENTRIES: int = int(os.getenv("RSS_MAX_ENTRIES", "30"))
    # Comma-separated list of feed names to skip (useful for debugging)
    RSS_SKIP_FEEDS: list[str] = [
        s.strip()
        for s in os.getenv("RSS_SKIP_FEEDS", "").split(",")
        if s.strip()
    ]
