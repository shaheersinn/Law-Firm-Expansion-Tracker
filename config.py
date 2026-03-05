"""
Configuration — all values loaded from environment variables.
Copy .env.example → .env and fill in your values.
"""

import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    # Telegram
    TELEGRAM_BOT_TOKEN: str  = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID: str    = os.environ.get("TELEGRAM_CHAT_ID", "")

    # Database
    DB_PATH: str             = os.environ.get("DB_PATH", "law_firm_tracker.db")

    # Dashboard
    DASHBOARD_URL: str       = os.environ.get("DASHBOARD_URL", "")

    # Behaviour flags
    INSTANT_ALERT_ON_LATERAL: bool = (
        os.environ.get("INSTANT_ALERT_ON_LATERAL", "true").lower() == "true"
    )

    # Scraper tuning
    REQUEST_DELAY_MIN: float = float(os.environ.get("REQUEST_DELAY_MIN", "1.5"))
    REQUEST_DELAY_MAX: float = float(os.environ.get("REQUEST_DELAY_MAX", "4.0"))
    REQUEST_TIMEOUT:   int   = int(os.environ.get("REQUEST_TIMEOUT",   "20"))
    MAX_RETRIES:       int   = int(os.environ.get("MAX_RETRIES",       "3"))
