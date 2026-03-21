"""
pipeline/orchestrator_v5.py  — All 21 strategies + ML layers
"""
import logging, time, sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

log = logging.getLogger(__name__)

BI_HOURLY = [
    ("CanLII Litigation",         "signals.canlii_litigation",       "CanLIILitigationTracker"),
    ("SEDAR+ Corporate",          "signals.sedar_corporate",          "SEDARPlusMonitor"),
    ("BigLaw Spillage + Gravity", "signals.spillage_graph",           "DealMonitor"),
    ("Macro Correlator",          "signals.advanced.macro_correlator","MacroCorrelator"),
    ("AER/AUC Hearings",          "signals.advanced.aer_hearings",    "AERHearingMonitor"),
    ("Newswire Monitor",          "signals.advanced.newswire_monitor","NewswireMonitor"),
    ("ASC Enforcement + TSXV",    "signals.deep.asc_enforcement",     "ASCEnforcementMonitor"),
    ("SEC EDGAR Cross-Border",    "signals.deep.fiscal_calendar",     "CrossBorderIntelligence"),
    ("Counterparty Intel",        "signals.deep.counterparty_intel",  "CounterpartyIntelExtractor"),
    ("Fiscal Calendar Predictor", "signals.deep.fiscal_calendar",     "FiscalCalendarPredictor"),
]
DAILY = [
    ("Website Headcount",         "signals.advanced.website_headcount","TeamPageScraper"),
    ("SEDI Insider Cluster",      "signals.advanced.sedi_monitor",    "SEDIMonitor"),
    ("Glassdoor NLP",             "signals.advanced.glassdoor_signals","GlassdoorSentimentMonitor"),
    ("Law School Placement",      "signals.advanced.law_school_placement","LawSchoolPlacementMonitor"),
    ("Partner Clock",             "signals.deep.partner_clock",       "PartnerPressureClock"),
    ("Corporate Registry",        "signals.deep.corporate_registry",  "CorporateRegistryMonitor"),
    ("Career Semantic Monitor",   "signals.deep.career_semantic",     "CareerSemanticMonitor"),
    ("Dark Pipeline",             "signals.deep.dark_pipeline",       "DarkPipelineMonitor"),
]
WEEKLY  = [("LinkedIn Turnover", "signals.linkedin_turnover",        "LinkedInTurnoverTracker", "check_departures")]
SEASONAL = [("LSA Hireback",     "signals.lsa_hireback",             "HirebackVacuumTracker")]


def _run(strats):
    total = 0
    for item in strats:
        name, mod, cls = item[0], item[1], item[2]
        method = item[3] if len(item) > 3 else "run"
        t0 = time.time()
        try:
            m    = __import__(mod, fromlist=[cls])
            inst = getattr(m, cls)()
            sigs = getattr(inst, method)() or []
            log.info("  ✅  %-35s %2d signals  %.1fs", name, len(sigs), time.time()-t0)
            total += len(sigs)
        except Exception as e:
            log.error("  ❌  %-35s FAILED: %s", name, e)
    return total


def run_full_pipeline_v5(mode: str = "bi-hourly"):
    from database.db import init_db
    init_db()

    sets = {
        "bi-hourly": BI_HOURLY,
        "daily":     DAILY,
        "weekly":    WEEKLY,
        "seasonal":  SEASONAL,
        "full":      BI_HOURLY + DAILY + WEEKLY + SEASONAL,
    }
    strats = sets.get(mode, BI_HOURLY)
    log.info("═"*60)
    log.info("  CALGARY LAW TRACKER v5  [%s]  21 strategies", mode.upper())
    log.info("═"*60)

    n = _run(strats)
    log.info("Total new signals: %d", n)

    from alerts.notifier import AlertDispatcher
    AlertDispatcher().dispatch_unalerted()

    # Run ML layers on daily+ modes
    if mode in ("daily","full"):
        try:
            from ml.demand_forecast import DemandForecaster
            DemandForecaster().run()
        except Exception as e:
            log.error("[Forecast] %s", e)
        try:
            from intelligence.competitive_landscape import CompetitiveLandscapeMonitor
            CompetitiveLandscapeMonitor().run()
        except Exception as e:
            log.error("[Competition] %s", e)
        try:
            from graph.network_gravity import AlumniNetworkMap
            AlumniNetworkMap().ingest_linkedin_departures()
            AlumniNetworkMap().generate_signals()
        except Exception as e:
            log.error("[Alumni] %s", e)

    from dashboard.generator import generate_dashboard
    generate_dashboard()
    log.info("✅  v5 pipeline complete.  mode=%s", mode)
