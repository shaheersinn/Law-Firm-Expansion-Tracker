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
        "short": "McCarthy",
        "aliases": ["McCarthy Tétrault", "McCarthy", "MT"],
        "alt_names": ["McCarthy Tétrault", "McCarthy", "MT", "McCarthy Tetrault"],
        "linkedin_slug": "mccarthy-tetrault",
        "website": "https://www.mccarthy.ca",
        "careers_url": "https://www.mccarthy.ca/en/careers",
        "news_url": "https://www.mccarthy.ca/en/insights/news",
        "hq": "Calgary",
        "tier": "big",
        "focus": ["corporate", "energy", "litigation"],
        "hiring_partner_title": "Recruiting Manager",
    },
    {
        "id": "blakes",
        "name": "Blake, Cassels & Graydon LLP",
        "aliases": ["Blakes", "Blake Cassels", "Blake, Cassels"],
        "short": "Blakes",
        "alt_names": ["Blakes", "Blake Cassels", "Blake, Cassels & Graydon", "Blake Cassels Graydon"],
        "linkedin_slug": "blake-cassels-graydon-llp",
        "website": "https://www.blakes.com",
        "careers_url": "https://www.blakes.com/careers",
        "news_url": "https://www.blakes.com/insights/news",
        "hq": "Calgary",
        "tier": "big",
        "focus": ["corporate", "energy", "M&A"],
        "hiring_partner_title": "Articling Recruitment",
    },
    {
        "id": "bennett_jones",
        "name": "Bennett Jones LLP",
        "aliases": ["Bennett Jones", "BJ"],
        "short": "Bennett Jones",
        "alt_names": ["Bennett Jones", "BJ", "Bennett Jones LLP"],
        "linkedin_slug": "bennett-jones-llp",
        "website": "https://www.bennettjones.com",
        "careers_url": "https://www.bennettjones.com/careers",
        "news_url": "https://www.bennettjones.com/insights",
        "hq": "Calgary",
        "tier": "big",
        "focus": ["energy", "corporate", "litigation"],
        "hiring_partner_title": "Hiring Partner",
    },
    {
        "id": "norton_rose",
        "name": "Norton Rose Fulbright Canada LLP",
        "aliases": ["Norton Rose Fulbright", "Norton Rose", "NRF"],
        "short": "NRF",
        "alt_names": ["Norton Rose Fulbright", "Norton Rose", "NRF", "Norton Rose Fulbright Canada"],
        "linkedin_slug": "norton-rose-fulbright",
        "website": "https://www.nortonrosefulbright.com/en-ca",
        "careers_url": "https://www.nortonrosefulbright.com/en-ca/careers",
        "news_url": "https://www.nortonrosefulbright.com/en-ca/knowledge",
        "hq": "Calgary",
        "tier": "big",
        "focus": ["energy", "banking", "corporate"],
        "hiring_partner_title": "Recruiting Coordinator",
    },
    {
        "id": "osler",
        "name": "Osler, Hoskin & Harcourt LLP",
        "aliases": ["Osler", "Osler Hoskin"],
        "short": "Osler",
        "alt_names": ["Osler", "Osler Hoskin", "Osler Hoskin & Harcourt"],
        "linkedin_slug": "osler-hoskin-harcourt",
        "website": "https://www.osler.com",
        "careers_url": "https://www.osler.com/en/careers",
        "news_url": "https://www.osler.com/en/insights",
        "hq": "Calgary",
        "tier": "big",
        "focus": ["corporate", "M&A", "securities"],
        "hiring_partner_title": "Student Program Coordinator",
    },
    {
        "id": "torys",
        "name": "Torys LLP",
        "aliases": ["Torys"],
        "short": "Torys",
        "alt_names": ["Torys", "Torys LLP"],
        "linkedin_slug": "torys-llp",
        "website": "https://www.torys.com",
        "careers_url": "https://www.torys.com/careers",
        "news_url": "https://www.torys.com/insights",
        "hq": "Calgary",
        "tier": "big",
        "focus": ["corporate", "energy", "M&A"],
        "hiring_partner_title": "Recruiting Manager",
    },
    {
        "id": "stikeman",
        "name": "Stikeman Elliott LLP",
        "aliases": ["Stikeman Elliott", "Stikeman"],
        "short": "Stikeman",
        "alt_names": ["Stikeman Elliott", "Stikeman", "Stikeman Elliott LLP"],
        "linkedin_slug": "stikeman-elliott",
        "website": "https://www.stikeman.com",
        "careers_url": "https://www.stikeman.com/en-ca/careers",
        "news_url": "https://www.stikeman.com/en-ca/kh",
        "hq": "Calgary",
        "tier": "big",
        "focus": ["corporate", "tax", "M&A"],
        "hiring_partner_title": "Hiring Partner",
    },
    {
        "id": "dentons",
        "name": "Dentons Canada LLP",
        "aliases": ["Dentons"],
        "short": "Dentons",
        "alt_names": ["Dentons", "Dentons Canada", "Dentons Canada LLP"],
        "linkedin_slug": "dentons",
        "website": "https://www.dentons.com",
        "careers_url": "https://www.dentons.com/en/careers",
        "news_url": "https://www.dentons.com/en/insights",
        "hq": "Calgary",
        "tier": "big",
        "focus": ["energy", "corporate", "litigation"],
        "hiring_partner_title": "Talent Acquisition",
    },
    {
        "id": "gowling",
        "name": "Gowling WLG (Canada) LLP",
        "aliases": ["Gowling", "Gowling WLG", "GWL"],
        "short": "Gowling",
        "alt_names": ["Gowling", "Gowling WLG", "GWL", "Gowling WLG Canada"],
        "linkedin_slug": "gowling-wlg",
        "website": "https://gowlingwlg.com",
        "careers_url": "https://gowlingwlg.com/en/careers/",
        "news_url": "https://gowlingwlg.com/en/insights/",
        "hq": "Calgary",
        "tier": "big",
        "focus": ["IP", "energy", "corporate"],
        "hiring_partner_title": "Recruiting Manager",
    },
    {
        "id": "borden_ladner",
        "name": "Borden Ladner Gervais LLP",
        "aliases": ["BLG", "Borden Ladner"],
        "short": "BLG",
        "alt_names": ["BLG", "Borden Ladner", "Borden Ladner Gervais", "Borden Ladner Gervais LLP"],
        "linkedin_slug": "borden-ladner-gervais-llp",
        "website": "https://www.blg.com",
        "careers_url": "https://www.blg.com/en/careers",
        "news_url": "https://www.blg.com/en/insights",
        "hq": "Calgary",
        "tier": "big",
        "focus": ["energy", "corporate", "litigation"],
        "hiring_partner_title": "Recruiting Manager",
    },
    {
        "id": "fmc_law",
        "name": "Fasken Martineau DuMoulin LLP",
        "aliases": ["Fasken", "FMD"],
        "short": "Fasken",
        "alt_names": ["Fasken", "FMD", "Fasken Martineau", "Fasken Martineau DuMoulin"],
        "linkedin_slug": "fasken",
        "website": "https://www.fasken.com",
        "careers_url": "https://www.fasken.com/en/careers",
        "news_url": "https://www.fasken.com/en/knowledge",
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
        "short": "BD&P",
        "alt_names": ["BDP", "Burnet Duckworth", "Burnet", "Burnet Duckworth & Palmer"],
        "linkedin_slug": "burnet-duckworth-palmer",
        "website": "https://www.bdplaw.com",
        "careers_url": "https://www.bdplaw.com/careers/",
        "news_url": "https://www.bdplaw.com/news/",
        "hq": "Calgary",
        "tier": "mid",
        "focus": ["energy", "corporate", "litigation"],
        "hiring_partner_title": "Managing Partner",
    },
    {
        "id": "field_law",
        "name": "Field Law",
        "aliases": ["Field LLP", "Field Law"],
        "short": "Field Law",
        "alt_names": ["Field LLP", "Field Law", "Field Law LLP"],
        "linkedin_slug": "field-law",
        "website": "https://www.fieldlaw.com",
        "careers_url": "https://www.fieldlaw.com/careers",
        "news_url": "https://www.fieldlaw.com/insights",
        "hq": "Calgary",
        "tier": "mid",
        "focus": ["energy", "real estate", "labour"],
        "hiring_partner_title": "Hiring Partner",
    },
    {
        "id": "cassels",
        "name": "Cassels Brock & Blackwell LLP",
        "aliases": ["Cassels", "Cassels Brock"],
        "short": "Cassels",
        "alt_names": ["Cassels", "Cassels Brock", "Cassels Brock & Blackwell"],
        "linkedin_slug": "cassels-brock-blackwell-llp",
        "website": "https://www.cassels.com",
        "careers_url": "https://www.cassels.com/careers/",
        "news_url": "https://www.cassels.com/insights/",
        "hq": "Calgary",
        "tier": "mid",
        "focus": ["mining", "securities", "corporate"],
        "hiring_partner_title": "Hiring Partner",
    },
    {
        "id": "miller_thomson",
        "name": "Miller Thomson LLP",
        "aliases": ["Miller Thomson"],
        "short": "Miller Thomson",
        "alt_names": ["Miller Thomson", "Miller Thomson LLP"],
        "linkedin_slug": "miller-thomson-llp",
        "website": "https://www.millerthomson.com",
        "careers_url": "https://www.millerthomson.com/careers/",
        "news_url": "https://www.millerthomson.com/insights/",
        "hq": "Calgary",
        "tier": "mid",
        "focus": ["corporate", "employment", "real estate"],
        "hiring_partner_title": "Talent Acquisition",
    },
    {
        "id": "parlee_mclaws",
        "name": "Parlee McLaws LLP",
        "aliases": ["Parlee McLaws", "Parlee"],
        "short": "Parlee McLaws",
        "alt_names": ["Parlee McLaws", "Parlee", "Parlee McLaws LLP"],
        "linkedin_slug": "parlee-mclaws",
        "website": "https://www.parlee.com",
        "careers_url": "https://www.parlee.com/careers/",
        "news_url": "https://www.parlee.com/news/",
        "hq": "Calgary",
        "tier": "mid",
        "focus": ["litigation", "energy", "employment"],
        "hiring_partner_title": "Managing Partner",
    },
    {
        "id": "hamilton_law",
        "name": "Hamilton Cahoon LLP",
        "aliases": ["Hamilton Cahoon", "Hamilton"],
        "short": "Hamilton Cahoon",
        "alt_names": ["Hamilton Cahoon", "Hamilton", "Hamilton Cahoon LLP"],
        "linkedin_slug": "hamilton-cahoon",
        "website": "https://www.hamiltoncahoon.com",
        "careers_url": "https://www.hamiltoncahoon.com/careers/",
        "news_url": "https://www.hamiltoncahoon.com/news/",
        "hq": "Calgary",
        "tier": "mid",
        "focus": ["corporate", "securities", "M&A"],
        "hiring_partner_title": "Managing Partner",
    },
    {
        "id": "walsh_law",
        "name": "Walsh LLP",
        "aliases": ["Walsh LLP", "Walsh"],
        "short": "Walsh",
        "alt_names": ["Walsh LLP", "Walsh", "Walsh Law"],
        "linkedin_slug": "walsh-llp",
        "website": "https://www.walshlaw.ca",
        "careers_url": "https://www.walshlaw.ca/careers/",
        "news_url": "https://www.walshlaw.ca/news/",
        "hq": "Calgary",
        "tier": "mid",
        "focus": ["corporate", "energy"],
        "hiring_partner_title": "Partner",
    },
    {
        "id": "witten",
        "name": "Witten LLP",
        "aliases": ["Witten"],
        "short": "Witten",
        "alt_names": ["Witten", "Witten LLP"],
        "linkedin_slug": "witten-llp",
        "website": "https://www.wittenlaw.com",
        "careers_url": "https://www.wittenlaw.com/careers/",
        "news_url": "https://www.wittenlaw.com/news/",
        "hq": "Calgary",
        "tier": "mid",
        "focus": ["corporate", "real estate"],
        "hiring_partner_title": "Managing Partner",
    },
    {
        "id": "macleod_dixon",
        "name": "Macleod Dixon LLP",
        "aliases": ["Macleod Dixon"],
        "short": "Macleod Dixon",
        "alt_names": ["Macleod Dixon", "Macleod Dixon LLP"],
        "linkedin_slug": "macleod-dixon",
        "website": "https://www.macleoddixon.com",
        "careers_url": "https://www.macleoddixon.com/careers/",
        "news_url": "https://www.macleoddixon.com/news/",
        "hq": "Calgary",
        "tier": "mid",
        "focus": ["energy", "corporate"],
        "hiring_partner_title": "Managing Partner",
    },
    {
        "id": "reynolds_mirth",
        "name": "Reynolds Mirth Richards & Farmer LLP",
        "aliases": ["RMRF", "Reynolds Mirth"],
        "short": "RMRF",
        "alt_names": ["RMRF", "Reynolds Mirth", "Reynolds Mirth Richards & Farmer"],
        "linkedin_slug": "reynolds-mirth-richards-farmer",
        "website": "https://www.rmrf.ca",
        "careers_url": "https://www.rmrf.ca/careers/",
        "news_url": "https://www.rmrf.ca/news/",
        "hq": "Edmonton",
        "tier": "mid",
        "focus": ["litigation", "employment", "construction"],
        "hiring_partner_title": "Managing Partner",
    },
    {
        "id": "lapointe",
        "name": "Lapointe Rosenstein Marchand Melançon LLP",
        "aliases": ["Lapointe Rosenstein", "LRMM"],
        "short": "LRMM",
        "alt_names": ["Lapointe Rosenstein", "LRMM", "Lapointe Rosenstein Marchand Melancon"],
        "linkedin_slug": "lapointe-rosenstein",
        "website": "https://www.lrmm.com",
        "careers_url": "https://www.lrmm.com/careers/",
        "news_url": "https://www.lrmm.com/publications/",
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
        "short": "Duncan Craig",
        "alt_names": ["Duncan Craig", "Duncan Craig LLP"],
        "linkedin_slug": "duncan-craig-llp",
        "website": "https://www.duncancraig.com",
        "careers_url": "https://www.duncancraig.com/careers/",
        "news_url": "https://www.duncancraig.com/news/",
        "hq": "Calgary",
        "tier": "mid",
        "focus": ["litigation", "corporate", "employment"],
        "hiring_partner_title": "Managing Partner",
    },
    {
        "id": "mclennan_ross",
        "name": "McLennan Ross LLP",
        "aliases": ["McLennan Ross", "McRoss"],
        "short": "McLennan Ross",
        "alt_names": ["McLennan Ross", "McRoss", "McLennan Ross LLP"],
        "linkedin_slug": "mclennan-ross-llp",
        "website": "https://www.mross.com",
        "careers_url": "https://www.mross.com/careers/",
        "news_url": "https://www.mross.com/news/",
        "hq": "Calgary",
        "tier": "mid",
        "focus": ["litigation", "energy", "insurance"],
        "hiring_partner_title": "Managing Partner",
    },
    {
        "id": "carscallen",
        "name": "Carscallen LLP",
        "aliases": ["Carscallen"],
        "short": "Carscallen",
        "alt_names": ["Carscallen", "Carscallen LLP"],
        "linkedin_slug": "carscallen-llp",
        "website": "https://www.carscallen.com",
        "careers_url": "https://www.carscallen.com/careers/",
        "news_url": "https://www.carscallen.com/news/",
        "hq": "Calgary",
        "tier": "mid",
        "focus": ["corporate", "energy", "real estate"],
        "hiring_partner_title": "Managing Partner",
    },
    {
        "id": "jss_barristers",
        "name": "JSS Barristers",
        "aliases": ["JSS Barristers", "JSS"],
        "short": "JSS Barristers",
        "alt_names": ["JSS Barristers", "JSS"],
        "linkedin_slug": "jss-barristers",
        "website": "https://www.jssbarristers.ca",
        "careers_url": "https://www.jssbarristers.ca/careers/",
        "news_url": "https://www.jssbarristers.ca/news/",
        "hq": "Calgary",
        "tier": "boutique",
        "focus": ["litigation", "appeals", "regulatory"],
        "hiring_partner_title": "Partner",
    },
    {
        "id": "bryan_company",
        "name": "Bryan & Company LLP",
        "aliases": ["Bryan & Company", "Bryan Company"],
        "short": "Bryan & Company",
        "alt_names": ["Bryan & Company", "Bryan Company", "Bryan & Company LLP"],
        "linkedin_slug": "bryan-company-llp",
        "website": "https://www.bryancompany.com",
        "careers_url": "https://www.bryancompany.com/careers/",
        "news_url": "https://www.bryancompany.com/news/",
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
        "short": "Buss Law",
        "alt_names": ["Buss Law", "BussLaw"],
        "linkedin_slug": "buss-law",
        "website": "https://www.busslaw.ca",
        "careers_url": "https://www.busslaw.ca/careers/",
        "news_url": "https://www.busslaw.ca/news/",
        "hq": "Calgary",
        "tier": "boutique",
        "focus": ["litigation", "employment"],
        "hiring_partner_title": "Principal",
    },
    {
        "id": "ds_simon",
        "name": "DS Simon Law",
        "aliases": ["DS Simon"],
        "short": "DS Simon",
        "alt_names": ["DS Simon", "DS Simon Law"],
        "linkedin_slug": "ds-simon-law",
        "website": "https://www.dssimonlaw.com",
        "careers_url": "https://www.dssimonlaw.com/careers/",
        "news_url": "https://www.dssimonlaw.com/news/",
        "hq": "Calgary",
        "tier": "boutique",
        "focus": ["securities", "corporate"],
        "hiring_partner_title": "Partner",
    },
    {
        "id": "oyen_wiggs",
        "name": "Oyen Wiggs Green & Mutala LLP",
        "aliases": ["Oyen Wiggs"],
        "short": "Oyen Wiggs",
        "alt_names": ["Oyen Wiggs", "Oyen Wiggs Green & Mutala"],
        "linkedin_slug": "oyen-wiggs",
        "website": "https://www.patentable.com",
        "careers_url": "https://www.patentable.com/careers/",
        "news_url": "https://www.patentable.com/news/",
        "hq": "Calgary",
        "tier": "boutique",
        "focus": ["IP", "patents"],
        "hiring_partner_title": "Managing Partner",
    },
    {
        "id": "taylor_janis",
        "name": "Taylor Janis LLP",
        "aliases": ["Taylor Janis"],
        "short": "Taylor Janis",
        "alt_names": ["Taylor Janis", "Taylor Janis LLP"],
        "linkedin_slug": "taylor-janis-llp",
        "website": "https://www.taylorjanis.com",
        "careers_url": "https://www.taylorjanis.com/careers/",
        "news_url": "https://www.taylorjanis.com/blog/",
        "hq": "Calgary",
        "tier": "boutique",
        "focus": ["employment", "litigation"],
        "hiring_partner_title": "Partner",
    },
    {
        "id": "kcr_law",
        "name": "KCR Law Group",
        "aliases": ["KCR Law", "KCR"],
        "short": "KCR Law",
        "alt_names": ["KCR Law", "KCR", "KCR Law Group"],
        "linkedin_slug": "kcr-law-group",
        "website": "https://www.kcrlaw.ca",
        "careers_url": "https://www.kcrlaw.ca/careers/",
        "news_url": "https://www.kcrlaw.ca/news/",
        "hq": "Calgary",
        "tier": "boutique",
        "focus": ["corporate", "real estate"],
        "hiring_partner_title": "Managing Partner",
    },
    {
        "id": "golden_sun",
        "name": "Golden Sun Law",
        "aliases": ["Golden Sun"],
        "short": "Golden Sun",
        "alt_names": ["Golden Sun", "Golden Sun Law"],
        "linkedin_slug": "golden-sun-law",
        "website": "https://www.goldensunlaw.com",
        "careers_url": "https://www.goldensunlaw.com/careers/",
        "news_url": "https://www.goldensunlaw.com/news/",
        "hq": "Calgary",
        "tier": "boutique",
        "focus": ["immigration", "corporate"],
        "hiring_partner_title": "Partner",
    },
    {
        "id": "carters",
        "name": "Carters Professional Corporation",
        "aliases": ["Carters", "Carters PC"],
        "short": "Carters",
        "alt_names": ["Carters", "Carters PC", "Carters Professional Corporation"],
        "linkedin_slug": "carters-professional-corporation",
        "website": "https://www.carters.ca",
        "careers_url": "https://www.carters.ca/careers/",
        "news_url": "https://www.carters.ca/pub/",
        "hq": "Calgary",
        "tier": "boutique",
        "focus": ["charity", "not-for-profit"],
        "hiring_partner_title": "Managing Partner",
    },
    {
        "id": "vbk",
        "name": "Vogel LLP",
        "aliases": ["Vogel LLP", "Vogel"],
        "short": "Vogel",
        "alt_names": ["Vogel LLP", "Vogel", "Vogel Law"],
        "linkedin_slug": "vogel-llp",
        "website": "https://www.vogellaw.com",
        "careers_url": "https://www.vogellaw.com/careers/",
        "news_url": "https://www.vogellaw.com/news/",
        "hq": "Calgary",
        "tier": "boutique",
        "focus": ["corporate", "employment"],
        "hiring_partner_title": "Managing Partner",
    },
]

# Convenience lookup dicts
FIRM_BY_ID    = {f["id"]: f for f in CALGARY_FIRMS}
# Pseudo-firm for market-wide macro signals (macro_correlator fires these with firm_id="market")
FIRM_BY_ID["market"] = {
    "id": "market", "name": "Calgary Energy Legal Market", "tier": "market",
    "focus": ["energy", "corporate"], "aliases": [], "hq": "Calgary",
}
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
