"""
Configuration loader — reads from environment variables.
All secrets live in GitHub Secrets; never hard-coded here.
"""

import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    # Telegram
    TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID: str   = os.getenv("TELEGRAM_CHAT_ID", "")

    # Behaviour
    INSTANT_ALERT_ON_LATERAL: bool = os.getenv("INSTANT_ALERT_ON_LATERAL", "true").lower() == "true"

    # Database
    DB_PATH: str = os.getenv("DB_PATH", "law_firm_tracker.db")

    # HTTP
    REQUEST_TIMEOUT: int    = int(os.getenv("REQUEST_TIMEOUT", "20"))
    MIN_DELAY_SECS: float   = float(os.getenv("MIN_DELAY_SECS", "1.0"))
    MAX_DELAY_SECS: float   = float(os.getenv("MAX_DELAY_SECS", "3.0"))
