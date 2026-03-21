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
# ── Master lookback window ─────────────────────────────────────────────────────
# All scrapers read SIGNAL_LOOKBACK_DAYS so recency is controlled in one place.
# Default: 90 days (3 months). Override via env: SIGNAL_LOOKBACK_DAYS=90
import os as _os
SIGNAL_LOOKBACK_DAYS = int(_os.getenv("SIGNAL_LOOKBACK_DAYS", "90"))
CANLII_LOOKBACK_DAYS = SIGNAL_LOOKBACK_DAYS  # was hardcoded 35

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
APPEARANCE_MA_DAYS       = 90    # rolling window for CanLII appearance MA (was 30)
TURNOVER_CHECK_WEEKS     = 1     # LinkedIn cron frequency
HIREBACK_CHECK_DAYS_POST = 90    # days after articling end to re-check LSA

# ─── 30 TARGET CALGARY FIRMS ─────────────────────────────────────────────────
# For each firm: canonical name, common aliases (for PDF parsing), LinkedIn slug,
# key hiring contacts (if known), and practice focus tags.
CALGARY_FIRMS = [
    # ── National BigLaw (Calgary offices) ────────────────────────────────────
    {
        "id": "mccarthy",
        "name": "McCarthy Tétrault LLP",
        "aliases": ["McCarthy Tétrault", "McCarthy", "MT"],
        "linkedin_slug": "mccarthy-tetrault",
        "website": "https://www.mccarthy.ca",
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
        "website": "https://www.blakes.com",
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
        "website": "https://www.bennettjones.com",
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
        "website": "https://www.nortonrosefulbright.com/en-ca",
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
        "website": "https://www.osler.com",
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
        "website": "https://www.torys.com",
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
        "website": "https://www.stikeman.com",
        "hq": "Calgary",
        "tier": "big",
        "focus": ["corporate", "tax", "M&A"],
        "hiring_partner_title": "Hiring Partner",
    },
    {
        "id": "dentons",
        "name": "Dentons Canada LLP",
        "aliases": ["Dentons"],
        "linkedin_slug": "dentons",
        "website": "https://www.dentons.com",
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
        "website": "https://gowlingwlg.com",
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
        "website": "https://www.blg.com",
        "hq": "Calgary",
        "tier": "big",
        "focus": ["energy", "corporate", "litigation"],
        "hiring_partner_title": "Recruiting Manager",
    },
    {
        "id": "fmc_law",
        "name": "Fasken Martineau DuMoulin LLP",
        "aliases": ["Fasken", "FMD"],
        "linkedin_slug": "fasken",
        "website": "https://www.fasken.com",
        "hq": "Calgary",
        "tier": "big",
        "focus": ["mining", "energy", "labour"],
        "hiring_partner_title": "Recruiting Manager",
    },
    # ── Mid-tier Calgary firms ────────────────────────────────────────────────
    {
        "id": "burnet",
        "name": "Burnet, Duckworth & Palmer LLP",
        "aliases": ["BDP", "Burnet Duckworth", "Burnet"],
        "linkedin_slug": "burnet-duckworth-palmer",
        "website": "https://www.bdplaw.com",
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
        "website": "https://www.fieldlaw.com",
        "hq": "Calgary",
        "tier": "mid",
        "focus": ["energy", "real estate", "labour"],
        "hiring_partner_title": "Hiring Partner",
    },
    {
        "id": "cassels",
        "name": "Cassels Brock & Blackwell LLP",
        "aliases": ["Cassels", "Cassels Brock"],
        "linkedin_slug": "cassels-brock-blackwell-llp",
        "website": "https://www.cassels.com",
        "hq": "Calgary",
        "tier": "mid",
        "focus": ["mining", "securities", "corporate"],
        "hiring_partner_title": "Hiring Partner",
    },
    {
        "id": "miller_thomson",
        "name": "Miller Thomson LLP",
        "aliases": ["Miller Thomson"],
        "linkedin_slug": "miller-thomson-llp",
        "website": "https://www.millerthomson.com",
        "hq": "Calgary",
        "tier": "mid",
        "focus": ["corporate", "employment", "real estate"],
        "hiring_partner_title": "Talent Acquisition",
    },
    {
        "id": "parlee_mclaws",
        "name": "Parlee McLaws LLP",
        "aliases": ["Parlee McLaws", "Parlee"],
        "linkedin_slug": "parlee-mclaws",
        "website": "https://www.parlee.com",
        "hq": "Calgary",
        "tier": "mid",
        "focus": ["litigation", "energy", "employment"],
        "hiring_partner_title": "Managing Partner",
    },
    {
        "id": "hamilton_law",
        "name": "Hamilton Cahoon LLP",
        "aliases": ["Hamilton Cahoon", "Hamilton"],
        "linkedin_slug": "hamilton-cahoon",
        "website": "https://www.hamiltoncahoon.com",
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
        "website": "https://www.walshlaw.ca",
        "hq": "Calgary",
        "tier": "mid",
        "focus": ["corporate", "energy"],
        "hiring_partner_title": "Partner",
    },
    {
        "id": "witten",
        "name": "Witten LLP",
        "aliases": ["Witten"],
        "linkedin_slug": "witten-llp",
        "website": "https://www.wittenlaw.com",
        "hq": "Calgary",
        "tier": "mid",
        "focus": ["corporate", "real estate"],
        "hiring_partner_title": "Managing Partner",
    },
    {
        "id": "macleod_dixon",
        "name": "Macleod Dixon LLP",
        "aliases": ["Macleod Dixon"],
        "linkedin_slug": "macleod-dixon",
        "website": "https://www.macleoddixon.com",
        "hq": "Calgary",
        "tier": "mid",
        "focus": ["energy", "corporate"],
        "hiring_partner_title": "Managing Partner",
    },
    {
        "id": "reynolds_mirth",
        "name": "Reynolds Mirth Richards & Farmer LLP",
        "aliases": ["RMRF", "Reynolds Mirth"],
        "linkedin_slug": "reynolds-mirth-richards-farmer",
        "website": "https://www.rmrf.ca",
        "hq": "Edmonton",
        "tier": "mid",
        "focus": ["litigation", "employment", "construction"],
        "hiring_partner_title": "Managing Partner",
    },
    {
        "id": "lapointe",
        "name": "Lapointe Rosenstein Marchand Melançon LLP",
        "aliases": ["Lapointe Rosenstein", "LRMM"],
        "linkedin_slug": "lapointe-rosenstein",
        "website": "https://www.lrmm.com",
        "hq": "Calgary",
        "tier": "mid",
        "focus": ["corporate", "IP", "litigation"],
        "hiring_partner_title": "Partner",
    },
    # ── 5 Top Calgary firms previously missing ────────────────────────────────
    {
        "id": "duncan_craig",
        "name": "Duncan Craig LLP",
        "aliases": ["Duncan Craig"],
        "linkedin_slug": "duncan-craig-llp",
        "website": "https://www.duncancraig.com",
        "hq": "Calgary",
        "tier": "mid",
        "focus": ["litigation", "corporate", "employment"],
        "hiring_partner_title": "Managing Partner",
    },
    {
        "id": "mclennan_ross",
        "name": "McLennan Ross LLP",
        "aliases": ["McLennan Ross", "McRoss"],
        "linkedin_slug": "mclennan-ross-llp",
        "website": "https://www.mross.com",
        "hq": "Calgary",
        "tier": "mid",
        "focus": ["litigation", "energy", "insurance"],
        "hiring_partner_title": "Managing Partner",
    },
    {
        "id": "carscallen",
        "name": "Carscallen LLP",
        "aliases": ["Carscallen"],
        "linkedin_slug": "carscallen-llp",
        "website": "https://www.carscallen.com",
        "hq": "Calgary",
        "tier": "mid",
        "focus": ["corporate", "energy", "real estate"],
        "hiring_partner_title": "Managing Partner",
    },
    {
        "id": "jss_barristers",
        "name": "JSS Barristers",
        "aliases": ["JSS Barristers", "JSS"],
        "linkedin_slug": "jss-barristers",
        "website": "https://www.jssbarristers.ca",
        "hq": "Calgary",
        "tier": "boutique",
        "focus": ["litigation", "appeals", "regulatory"],
        "hiring_partner_title": "Partner",
    },
    {
        "id": "bryan_company",
        "name": "Bryan & Company LLP",
        "aliases": ["Bryan & Company", "Bryan Company"],
        "linkedin_slug": "bryan-company-llp",
        "website": "https://www.bryancompany.com",
        "hq": "Calgary",
        "tier": "mid",
        "focus": ["corporate", "real estate", "energy"],
        "hiring_partner_title": "Managing Partner",
    },
    # ── Boutique specialists ──────────────────────────────────────────────────
    {
        "id": "buss_law",
        "name": "Buss Law",
        "aliases": ["Buss Law", "BussLaw"],
        "linkedin_slug": "buss-law",
        "website": "https://www.busslaw.ca",
        "hq": "Calgary",
        "tier": "boutique",
        "focus": ["litigation", "employment"],
        "hiring_partner_title": "Principal",
    },
    {
        "id": "ds_simon",
        "name": "DS Simon Law",
        "aliases": ["DS Simon"],
        "linkedin_slug": "ds-simon-law",
        "website": "https://www.dssimonlaw.com",
        "hq": "Calgary",
        "tier": "boutique",
        "focus": ["securities", "corporate"],
        "hiring_partner_title": "Partner",
    },
    {
        "id": "oyen_wiggs",
        "name": "Oyen Wiggs Green & Mutala LLP",
        "aliases": ["Oyen Wiggs"],
        "linkedin_slug": "oyen-wiggs",
        "website": "https://www.patentable.com",
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
        "website": "https://www.taylorjanis.com",
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
        "website": "https://www.kcrlaw.ca",
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
        "website": "https://www.goldensunlaw.com",
        "hq": "Calgary",
        "tier": "boutique",
        "focus": ["immigration", "corporate"],
        "hiring_partner_title": "Partner",
    },
    {
        "id": "carters",
        "name": "Carters Professional Corporation",
        "aliases": ["Carters", "Carters PC"],
        "linkedin_slug": "carters-professional-corporation",
        "website": "https://www.carters.ca",
        "hq": "Calgary",
        "tier": "boutique",
        "focus": ["charity", "not-for-profit"],
        "hiring_partner_title": "Managing Partner",
    },
    {
        "id": "vbk",
        "name": "Vogel LLP",
        "aliases": ["Vogel LLP", "Vogel"],
        "linkedin_slug": "vogel-llp",
        "website": "https://www.vogellaw.com",
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
    "borden_ladner", "dentons", "norton_rose", "gowling", "fmc_law",
    "bennett_jones",  # Calgary-headquartered national firm — treated as BigLaw
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
