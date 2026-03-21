"""
config_calgary.py
─────────────────
Master configuration for the Calgary-focused enhanced tracker.
Edit this file to add/remove target firms or tune signal weights.
"""

# ─── API KEYS (load from environment) ───────────────────────────────────────
import os

CANLII_API_KEY     = os.getenv("CANLII_API_KEY", "")
SEDAR_API_KEY      = os.getenv("SEDAR_API_KEY", "")
PROXYCURL_API_KEY  = os.getenv("PROXYCURL_API_KEY", "")
SENDGRID_API_KEY   = os.getenv("SENDGRID_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")
OPENAI_API_KEY     = os.getenv("OPENAI_API_KEY", "")  # for outreach generation

ALERT_EMAIL_FROM   = os.getenv("ALERT_EMAIL_FROM", "tracker@example.com")
ALERT_EMAIL_TO     = os.getenv("ALERT_EMAIL_TO", "you@example.com")

# ─── CANLII SETTINGS ─────────────────────────────────────────────────────────
# Alberta Court of King's Bench database ID on CanLII
CANLII_ABQB_DB      = "abqb"
CANLII_ABCA_DB      = "abca"   # Alberta Court of Appeal (bonus signal)
CANLII_BASE_URL     = "https://api.canlii.org/v1"
CANLII_RATE_LIMIT_S = 1.0      # seconds between requests (respect ToS)
CANLII_LOOKBACK_DAYS = 35      # fetch this many days for the 30-day MA window

# ─── SEDAR+ SETTINGS ─────────────────────────────────────────────────────────
SEDAR_RSS_URL  = "https://www.sedarplus.ca/landingPage/rss/filings.rss"
SEDAR_BASE_URL = "https://www.sedarplus.ca"
# Document types that carry legal counsel names
SEDAR_COUNSEL_DOC_TYPES = [
    "prospectus", "circular", "AIF", "material change",
    "M&A", "business acquisition", "private placement",
    "take-over bid", "issuer bid",
]

# ─── LSA SETTINGS ────────────────────────────────────────────────────────────
LSA_SEARCH_URL = "https://www.lawsociety.ab.ca/lawyer-lookup/"
LSA_SCRAPE_DELAY_S = 2.0

# ─── SPIKE DETECTION ─────────────────────────────────────────────────────────
ZSCORE_ALERT_THRESHOLD   = 1.5   # z-score to flag a spike
APPEARANCE_MA_DAYS       = 30    # rolling window for CanLII appearance MA
TURNOVER_CHECK_WEEKS     = 1     # LinkedIn cron frequency
HIREBACK_CHECK_DAYS_POST = 90    # days after articling end to re-check LSA

# ─── 30 TARGET CALGARY FIRMS ─────────────────────────────────────────────────
# For each firm: canonical name, common aliases (for PDF parsing), LinkedIn slug,
# key hiring contacts (if known), and practice focus tags.
CALGARY_FIRMS = [
    {
        "id": "mccarthy",
        "name": "McCarthy Tétrault LLP",
        "aliases": ["McCarthy Tétrault", "McCarthy", "MT"],
        "linkedin_slug": "mccarthy-tetrault",
        "hq": "Calgary",
        "tier": "big",
        "focus": ["corporate", "energy", "litigation"],
        "hiring_partner_title": "Recruiting Manager",
    },
    {
        "id": "blakes",
        "name": "Blake, Cassels & Graydon LLP",
        "aliases": ["Blakes", "Blake Cassels", "Blake, Cassels"],
        "linkedin_slug": "blake-cassels-graydon-llp",
        "hq": "Calgary",
        "tier": "big",
        "focus": ["corporate", "energy", "M&A"],
        "hiring_partner_title": "Articling Recruitment",
    },
    {
        "id": "bennett_jones",
        "name": "Bennett Jones LLP",
        "aliases": ["Bennett Jones", "BJ"],
        "linkedin_slug": "bennett-jones-llp",
        "hq": "Calgary",
        "tier": "big",
        "focus": ["energy", "corporate", "litigation"],
        "hiring_partner_title": "Hiring Partner",
    },
    {
        "id": "norton_rose",
        "name": "Norton Rose Fulbright Canada LLP",
        "aliases": ["Norton Rose Fulbright", "Norton Rose", "NRF"],
        "linkedin_slug": "norton-rose-fulbright",
        "hq": "Calgary",
        "tier": "big",
        "focus": ["energy", "banking", "corporate"],
        "hiring_partner_title": "Recruiting Coordinator",
    },
    {
        "id": "osler",
        "name": "Osler, Hoskin & Harcourt LLP",
        "aliases": ["Osler", "Osler Hoskin"],
        "linkedin_slug": "osler-hoskin-harcourt",
        "hq": "Calgary",
        "tier": "big",
        "focus": ["corporate", "M&A", "securities"],
        "hiring_partner_title": "Student Program Coordinator",
    },
    {
        "id": "torys",
        "name": "Torys LLP",
        "aliases": ["Torys"],
        "linkedin_slug": "torys-llp",
        "hq": "Calgary",
        "tier": "big",
        "focus": ["corporate", "energy", "M&A"],
        "hiring_partner_title": "Recruiting Manager",
    },
    {
        "id": "stikeman",
        "name": "Stikeman Elliott LLP",
        "aliases": ["Stikeman Elliott", "Stikeman"],
        "linkedin_slug": "stikeman-elliott",
        "hq": "Calgary",
        "tier": "big",
        "focus": ["corporate", "tax", "M&A"],
        "hiring_partner_title": "Hiring Partner",
    },
    {
        "id": "burnet",
        "name": "Burnet, Duckworth & Palmer LLP",
        "aliases": ["BDP", "Burnet Duckworth", "Burnet"],
        "linkedin_slug": "burnet-duckworth-palmer",
        "hq": "Calgary",
        "tier": "mid",
        "focus": ["energy", "corporate", "litigation"],
        "hiring_partner_title": "Managing Partner",
    },
    {
        "id": "field_law",
        "name": "Field Law",
        "aliases": ["Field LLP", "Field Law"],
        "linkedin_slug": "field-law",
        "hq": "Calgary",
        "tier": "mid",
        "focus": ["energy", "real estate", "labour"],
        "hiring_partner_title": "Hiring Partner",
    },
    {
        "id": "witten",
        "name": "Witten LLP",
        "aliases": ["Witten"],
        "linkedin_slug": "witten-llp",
        "hq": "Calgary",
        "tier": "mid",
        "focus": ["corporate", "real estate"],
        "hiring_partner_title": "Managing Partner",
    },
    {
        "id": "dentons",
        "name": "Dentons Canada LLP",
        "aliases": ["Dentons"],
        "linkedin_slug": "dentons",
        "hq": "Calgary",
        "tier": "big",
        "focus": ["energy", "corporate", "litigation"],
        "hiring_partner_title": "Talent Acquisition",
    },
    {
        "id": "gowling",
        "name": "Gowling WLG (Canada) LLP",
        "aliases": ["Gowling", "Gowling WLG", "GWL"],
        "linkedin_slug": "gowling-wlg",
        "hq": "Calgary",
        "tier": "big",
        "focus": ["IP", "energy", "corporate"],
        "hiring_partner_title": "Recruiting Manager",
    },
    {
        "id": "borden_ladner",
        "name": "Borden Ladner Gervais LLP",
        "aliases": ["BLG", "Borden Ladner"],
        "linkedin_slug": "borden-ladner-gervais-llp",
        "hq": "Calgary",
        "tier": "big",
        "focus": ["energy", "corporate", "litigation"],
        "hiring_partner_title": "Recruiting Manager",
    },
    {
        "id": "cassels",
        "name": "Cassels Brock & Blackwell LLP",
        "aliases": ["Cassels", "Cassels Brock"],
        "linkedin_slug": "cassels-brock-blackwell-llp",
        "hq": "Calgary",
        "tier": "mid",
        "focus": ["mining", "securities", "corporate"],
        "hiring_partner_title": "Hiring Partner",
    },
    {
        "id": "macleod_dixon",
        "name": "Macleod Dixon LLP",
        "aliases": ["Macleod Dixon"],
        "linkedin_slug": "macleod-dixon",
        "hq": "Calgary",
        "tier": "mid",
        "focus": ["energy", "corporate"],
        "hiring_partner_title": "Managing Partner",
    },
    {
        "id": "buss_law",
        "name": "Buss Law",
        "aliases": ["Buss Law", "BussLaw"],
        "linkedin_slug": "buss-law",
        "hq": "Calgary",
        "tier": "boutique",
        "focus": ["litigation", "employment"],
        "hiring_partner_title": "Principal",
    },
    {
        "id": "reynolds_mirth",
        "name": "Reynolds Mirth Richards & Farmer LLP",
        "aliases": ["RMRF", "Reynolds Mirth"],
        "linkedin_slug": "reynolds-mirth-richards-farmer",
        "hq": "Edmonton",          # regional competitor worth tracking
        "tier": "mid",
        "focus": ["litigation", "employment", "construction"],
        "hiring_partner_title": "Managing Partner",
    },
    {
        "id": "parlee_mclaws",
        "name": "Parlee McLaws LLP",
        "aliases": ["Parlee McLaws", "Parlee"],
        "linkedin_slug": "parlee-mclaws",
        "hq": "Calgary",
        "tier": "mid",
        "focus": ["litigation", "energy", "employment"],
        "hiring_partner_title": "Managing Partner",
    },
    {
        "id": "miller_thomson",
        "name": "Miller Thomson LLP",
        "aliases": ["Miller Thomson"],
        "linkedin_slug": "miller-thomson-llp",
        "hq": "Calgary",
        "tier": "mid",
        "focus": ["corporate", "employment", "real estate"],
        "hiring_partner_title": "Talent Acquisition",
    },
    {
        "id": "ds_simon",
        "name": "DS Simon Law",
        "aliases": ["DS Simon"],
        "linkedin_slug": "ds-simon-law",
        "hq": "Calgary",
        "tier": "boutique",
        "focus": ["securities", "corporate"],
        "hiring_partner_title": "Partner",
    },
    {
        "id": "fmc_law",
        "name": "Fasken Martineau DuMoulin LLP",
        "aliases": ["Fasken", "FMD"],
        "linkedin_slug": "fasken",
        "hq": "Calgary",
        "tier": "big",
        "focus": ["mining", "energy", "labour"],
        "hiring_partner_title": "Recruiting Manager",
    },
    {
        "id": "oyen_wiggs",
        "name": "Oyen Wiggs Green & Mutala LLP",
        "aliases": ["Oyen Wiggs"],
        "linkedin_slug": "oyen-wiggs",
        "hq": "Calgary",
        "tier": "boutique",
        "focus": ["IP", "patents"],
        "hiring_partner_title": "Managing Partner",
    },
    {
        "id": "taylor_janis",
        "name": "Taylor Janis LLP",
        "aliases": ["Taylor Janis"],
        "linkedin_slug": "taylor-janis-llp",
        "hq": "Calgary",
        "tier": "boutique",
        "focus": ["employment", "litigation"],
        "hiring_partner_title": "Partner",
    },
    {
        "id": "kcr_law",
        "name": "KCR Law Group",
        "aliases": ["KCR Law", "KCR"],
        "linkedin_slug": "kcr-law-group",
        "hq": "Calgary",
        "tier": "boutique",
        "focus": ["corporate", "real estate"],
        "hiring_partner_title": "Managing Partner",
    },
    {
        "id": "golden_sun",
        "name": "Golden Sun Law",
        "aliases": ["Golden Sun"],
        "linkedin_slug": "golden-sun-law",
        "hq": "Calgary",
        "tier": "boutique",
        "focus": ["immigration", "corporate"],
        "hiring_partner_title": "Partner",
    },
    {
        "id": "hamilton_law",
        "name": "Hamilton Cahoon LLP",
        "aliases": ["Hamilton Cahoon", "Hamilton"],
        "linkedin_slug": "hamilton-cahoon",
        "hq": "Calgary",
        "tier": "mid",
        "focus": ["corporate", "securities", "M&A"],
        "hiring_partner_title": "Managing Partner",
    },
    {
        "id": "walsh_law",
        "name": "Walsh LLP",
        "aliases": ["Walsh LLP", "Walsh"],
        "linkedin_slug": "walsh-llp",
        "hq": "Calgary",
        "tier": "mid",
        "focus": ["corporate", "energy"],
        "hiring_partner_title": "Partner",
    },
    {
        "id": "carters",
        "name": "Carters Professional Corporation",
        "aliases": ["Carters", "Carters PC"],
        "linkedin_slug": "carters-professional-corporation",
        "hq": "Calgary",
        "tier": "boutique",
        "focus": ["charity", "not-for-profit"],
        "hiring_partner_title": "Managing Partner",
    },
    {
        "id": "lapointe",
        "name": "Lapointe Rosenstein Marchand Melançon LLP",
        "aliases": ["Lapointe Rosenstein", "LRMM"],
        "linkedin_slug": "lapointe-rosenstein",
        "hq": "Calgary",
        "tier": "mid",
        "focus": ["corporate", "IP", "litigation"],
        "hiring_partner_title": "Partner",
    },
    {
        "id": "vbk",
        "name": "Vogel LLP",
        "aliases": ["Vogel LLP", "Vogel"],
        "linkedin_slug": "vogel-llp",
        "hq": "Calgary",
        "tier": "boutique",
        "focus": ["corporate", "employment"],
        "hiring_partner_title": "Managing Partner",
    },
]

# Convenience lookup dicts
FIRM_BY_ID    = {f["id"]: f for f in CALGARY_FIRMS}
FIRM_ALIASES  = {}  # alias -> firm_id
for _f in CALGARY_FIRMS:
    for _alias in _f["aliases"] + [_f["name"]]:
        FIRM_ALIASES[_alias.lower()] = _f["id"]

# ─── BIG-LAW "LEAD COUNSEL" SET ──────────────────────────────────────────────
# These are the BigLaw firms whose overflow we want to detect
BIGLAW_FIRMS = {
    "blakes", "mccarthy", "osler", "torys", "stikeman",
    "borden_ladner", "dentons", "norton_rose", "gowling", "fmc_law"
}

# ─── SIGNAL WEIGHTS ──────────────────────────────────────────────────────────
SIGNAL_WEIGHTS = {
    # Follow the Work
    "canlii_appearance_spike":   4.0,
    "canlii_new_large_file":     3.5,
    "biglaw_spillage_predicted":  5.0,

    # Follow the Money
    "sedar_counsel_named":       4.5,
    "sedar_major_deal":          5.0,

    # Empty Chair
    "linkedin_turnover_detected": 4.5,
    "linkedin_new_vacancy":       3.5,

    # Hireback Vacuum
    "lsa_student_not_retained":  4.0,
    "lsa_retention_gap":         5.0,

    # Supporting signals (inherited from base tracker)
    "job_posting":               2.5,
    "lateral_hire":              3.0,
    "ranking":                   2.5,
    "press_release":             1.5,
}

# ─── ARTICLING CYCLE ─────────────────────────────────────────────────────────
# Alberta articling runs Sept → Aug; adjust year as needed
ARTICLING_START_MONTH = 9   # September
ARTICLING_END_MONTH   = 8   # August
ARTICLING_YEAR        = 2025

# ─── DATABASE ────────────────────────────────────────────────────────────────
DB_PATH = "tracker.db"   # SQLite; swap for Postgres by changing connection string

# ─── OUTPUT ──────────────────────────────────────────────────────────────────
REPORT_OUTPUT_DIR = "reports"
DASHBOARD_OUTPUT  = "docs/index.html"

# ─── V3 ADDITIONAL SIGNAL WEIGHTS ────────────────────────────────────────────
SIGNAL_WEIGHTS.update({
    # Partner Clock
    "partner_clock":              4.2,
    # Regulatory Wave
    "aer_hearing_load":           3.8,
    "regulatory_wave":            3.2,
    "asc_enforcement_defence":    4.0,
    "competition_merger_filing":  4.5,
    # Cross-border & Lateral
    "sec_crossborder_filing":     5.0,
    "lateral_magnet":             3.8,
    "competitive_hire_gap":       4.0,
    # Intelligence layer
    "pressure_index_alert":       5.5,
    "practice_velocity_spike":    3.5,
    "dual_departure_crisis":      6.0,    # Highest weight in the system
    "oci_pipeline_prediction":    3.2,
})

# ─── URGENCY MAP (for dashboard + alerts) ────────────────────────────────────
URGENCY_MAP = {
    # Tier 0 — same hour
    "dual_departure_crisis":      "same-hour",
    # Tier 1 — today
    "sedar_major_deal":           "today",
    "biglaw_spillage_predicted":  "today",
    "linkedin_turnover_detected": "today",
    "sec_crossborder_filing":     "today",
    "pressure_index_alert":       "today",
    "asc_enforcement_defence":    "today",
    "competition_merger_filing":  "today",
    # Tier 2 — this week
    "canlii_appearance_spike":    "week",
    "canlii_new_large_file":      "week",
    "sedar_counsel_named":        "week",
    "partner_clock":              "week",
    "aer_hearing_load":           "week",
    "lateral_magnet":             "week",
    "competitive_hire_gap":       "week",
    "practice_velocity_spike":    "week",
    # Tier 3 — within 3 days
    "lsa_retention_gap":          "3days",
    "lsa_student_not_retained":   "3days",
    "regulatory_wave":            "3days",
    # Tier 4 — this month
    "oci_pipeline_prediction":    "month",
    "job_posting":                "month",
    "lateral_hire":               "month",
    "ranking":                    "month",
}

# News URLs per firm (add to relevant firms in CALGARY_FIRMS above)
# These are populated below as a separate dict for easier maintenance
FIRM_NEWS_URLS = {
    "mccarthy":      "https://www.mccarthy.ca/en/news",
    "blakes":        "https://www.blakes.com/news",
    "bennett_jones": "https://www.bennettjones.com/news-search",
    "norton_rose":   "https://www.nortonrosefulbright.com/en-ca/news",
    "osler":         "https://www.osler.com/en/news-and-insights",
    "torys":         "https://www.torys.com/insights",
    "stikeman":      "https://www.stikeman.com/en-ca/news",
    "dentons":       "https://www.dentons.com/en/news",
    "burnet":        "https://www.bdplaw.com/news",
    "field_law":     "https://www.fieldlaw.com/news",
    "borden_ladner": "https://www.blg.com/en/insights/news",
    "gowling":       "https://gowlingwlg.com/en/canada/insights-resources/",
    "fmc_law":       "https://www.fasken.com/en/news",
    "miller_thomson":"https://www.millerthomson.com/en/publications/news/",
    "parlee_mclaws": "https://www.parlee.com/news",
    "cassels":       "https://www.cassels.com/news",
}
