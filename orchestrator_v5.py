"""
pipeline/orchestrator_v5.py  — All 24 strategies + ML layers  (v5.2)
═══════════════════════════════════════════════════════════════════════
BUG FIXES applied (v5.1 → v5.2):

  v5.1 fixes (carried forward):
    1. SEC EDGAR Cross-Border pointed at fiscal_calendar.CrossBorderIntelligence;
       replaced with dedicated signals.cross_border_intel module (two classes).
    2. Four orphaned signal classes (SECEdgarCrossBorderTracker,
       LateralMagnetTracker, NewCourtFilingMonitor, CompetitionBureauMonitor)
       were implemented but never scheduled — all four now wired in.

  v5.2 additions:
    3. Added "deep" mode — the primary production run mode. It runs all 24
       strategies then chains the full ML/intelligence pipeline:
         · CanLII expanded to 3 Alberta courts (ABQB + ABCA + ABPC)
         · ML demand forecaster
         · Competitive landscape monitor
         · Alumni network map (LinkedIn departure signals)
         · Predictive demand model
         · Scored firm leaderboard (exported to reports/)
         · Outreach plan generation
         · A/B optimizer report
         · Decision engine briefing (if ANTHROPIC_API_KEY present)
         · Full dashboard regeneration
       Expected runtime in GitHub Actions: 35–55 minutes.

    4. main.py shim now maps MODE=collect → --run deep so the legacy
       "Collect & Alert" workflow actually scrapes instead of printing help.
═══════════════════════════════════════════════════════════════════════
"""
import logging, time, sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

log = logging.getLogger(__name__)

# ── BI-HOURLY: fast, low-overhead — runs every 2 hours ──────────────────────
BI_HOURLY = [
    ("CanLII Litigation",          "signals.canlii_litigation",          "CanLIILitigationTracker"),
    ("SEDAR+ Corporate",           "signals.sedar_corporate",            "SEDARPlusMonitor"),
    ("BigLaw Spillage + Gravity",  "signals.spillage_graph",             "DealMonitor"),
    ("Macro Correlator",           "signals.advanced.macro_correlator",  "MacroCorrelator"),
    ("AER/AUC Hearings",           "signals.advanced.aer_hearings",      "AERHearingMonitor"),
    ("Newswire Monitor",           "signals.advanced.newswire_monitor",  "NewswireMonitor"),
    ("ASC Enforcement + TSXV",     "signals.deep.asc_enforcement",       "ASCEnforcementMonitor"),
    ("SEC EDGAR Cross-Border",     "signals.cross_border_intel",         "SECEdgarCrossBorderTracker"),
    ("Lateral Magnet Tracker",     "signals.cross_border_intel",         "LateralMagnetTracker"),
    ("Counterparty Intel",         "signals.deep.counterparty_intel",    "CounterpartyIntelExtractor"),
    ("Fiscal Calendar Predictor",  "signals.deep.fiscal_calendar",       "FiscalCalendarPredictor"),
    ("New Court Filings",          "signals.deep.new_court_filings",     "NewCourtFilingMonitor"),
]

# ── DAILY: scraping-heavy — runs once per day at 06:00 UTC ──────────────────
DAILY = [
    ("Website Headcount",          "signals.advanced.website_headcount",    "TeamPageScraper"),
    ("SEDI Insider Cluster",       "signals.advanced.sedi_monitor",         "SEDIMonitor"),
    ("Glassdoor NLP",              "signals.advanced.glassdoor_signals",    "GlassdoorSentimentMonitor"),
    ("Law School Placement",       "signals.advanced.law_school_placement", "LawSchoolPlacementMonitor"),
    ("Partner Clock",              "signals.deep.partner_clock",            "PartnerPressureClock"),
    ("Corporate Registry",         "signals.deep.corporate_registry",       "CorporateRegistryMonitor"),
    ("Career Semantic Monitor",    "signals.deep.career_semantic",          "CareerSemanticMonitor"),
    ("Dark Pipeline",              "signals.deep.dark_pipeline",            "DarkPipelineMonitor"),
    ("Competition Bureau Monitor", "signals.deep.competition_bureau",       "CompetitionBureauMonitor"),
]

# ── WEEKLY / SEASONAL ────────────────────────────────────────────────────────
WEEKLY   = [("LinkedIn Turnover", "signals.linkedin_turnover", "LinkedInTurnoverTracker", "check_departures")]
SEASONAL = [("LSA Hireback",      "signals.lsa_hireback",      "HirebackVacuumTracker")]

# ── DEEP extras: CanLII Appeal Court + Provincial Court ─────────────────────
# In deep mode we run CanLII a second time against two additional databases
# (Alberta Court of Appeal + Alberta Provincial Court) to maximise coverage.
_CANLII_DEEP_EXTRA = [
    ("CanLII Appeal Court",   "signals.canlii_litigation", "CanLIILitigationTracker", "run_abca"),
    ("CanLII Provincial Ct.", "signals.canlii_litigation", "CanLIILitigationTracker", "run_abpc"),
]


def _run(strats: list, *, label: str = "") -> int:
    """Execute a list of (name, module, class[, method]) strategy tuples."""
    total = 0
    for item in strats:
        name, mod, cls = item[0], item[1], item[2]
        method = item[3] if len(item) > 3 else "run"
        t0 = time.time()
        try:
            m    = __import__(mod, fromlist=[cls])
            inst = getattr(m, cls)()
            sigs = getattr(inst, method)() or []
            log.info("  ✅  %-42s %2d signals  %.1fs", name, len(sigs), time.time()-t0)
            total += len(sigs)
        except Exception as e:
            log.error("  ❌  %-42s FAILED: %s", name, e)
    return total


def _phase(title: str, fn, *args, **kwargs):
    """Run a post-pipeline phase with header/footer logging and error isolation."""
    log.info("")
    log.info("── %s ──", title)
    t0 = time.time()
    try:
        fn(*args, **kwargs)
        log.info("   done  %.1fs", time.time() - t0)
    except Exception as e:
        log.error("   FAILED: %s", e)


def run_full_pipeline_v5(mode: str = "bi-hourly"):
    """
    Main entry-point called by main_v5.py --run <mode>.

    Modes
    ─────
    bi-hourly  Fast pass: 12 BI_HOURLY strategies + alerts
    daily      Scraping pass: 9 DAILY strategies + ML layers
    weekly     LinkedIn turnover check only
    seasonal   LSA hireback check only
    full       All 24 strategies + ML layers (no deep extras)
    deep       All 24 strategies + CanLII extra courts + full ML/intel
               pipeline. Expected runtime 35–55 min. This is what
               main.py (legacy shim) dispatches when MODE=collect.
    """
    from database.db import init_db
    init_db()

    total_registered = len(BI_HOURLY) + len(DAILY) + len(WEEKLY) + len(SEASONAL)
    log.info("═" * 65)
    log.info("  CALGARY LAW TRACKER v5  [%s]  %d strategies registered",
             mode.upper(), total_registered)
    log.info("═" * 65)

    t_start = time.time()

    # ── Signal collection phase ───────────────────────────────────────────
    sets = {
        "bi-hourly": BI_HOURLY,
        "daily":     DAILY,
        "weekly":    WEEKLY,
        "seasonal":  SEASONAL,
        "full":      BI_HOURLY + DAILY + WEEKLY + SEASONAL,
        "deep":      BI_HOURLY + DAILY + WEEKLY + SEASONAL,
    }
    n = _run(sets.get(mode, BI_HOURLY))

    # Deep mode: extra CanLII databases (Appeal Court + Provincial Court)
    if mode == "deep":
        log.info("")
        log.info("── Deep mode: expanding CanLII to 3 Alberta courts ──")
        # run_abca / run_abpc — check if these method aliases exist; if not,
        # fall back to calling run() with the db arg directly.
        from signals.canlii_litigation import CanLIILitigationTracker
        try:
            inst = CanLIILitigationTracker()
            if hasattr(inst, "run_abca"):
                n += len(inst.run_abca() or [])
                log.info("  ✅  CanLII Appeal Court (abca)")
            else:
                from config_calgary import CANLII_ABCA_DB
                sigs = inst.run(databases=[CANLII_ABCA_DB]) or []
                n += len(sigs)
                log.info("  ✅  CanLII Appeal Court (abca)  %d signals", len(sigs))
        except Exception as e:
            log.error("  ❌  CanLII Appeal Court: %s", e)

        try:
            inst2 = CanLIILitigationTracker()
            if hasattr(inst2, "run_abpc"):
                n += len(inst2.run_abpc() or [])
                log.info("  ✅  CanLII Provincial Court (abpc)")
            else:
                sigs2 = inst2.run(databases=["abpc"]) or []
                n += len(sigs2)
                log.info("  ✅  CanLII Provincial Court (abpc)  %d signals", len(sigs2))
        except Exception as e:
            log.error("  ❌  CanLII Provincial Court: %s", e)

    log.info("")
    log.info("Signal collection complete.  new signals: %d  elapsed: %.0fs",
             n, time.time() - t_start)

    # ── Alerts dispatch ───────────────────────────────────────────────────
    _phase("Alerts dispatch", _dispatch_alerts)

    # ── ML + intelligence layers (daily / full / deep) ────────────────────
    if mode in ("daily", "full", "deep"):
        _phase("ML demand forecast",        _run_forecast)
        _phase("Competitive landscape",     _run_competition)
        _phase("Alumni network map",        _run_alumni)

    # ── Deep-only intelligence layers ─────────────────────────────────────
    if mode == "deep":
        _phase("Predictive demand model",   _run_predict)
        _phase("Firm leaderboard export",   _run_leaderboard)
        _phase("Outreach plan generation",  _run_outreach)
        _phase("A/B performance report",    _run_ab_report)
        if os.getenv("ANTHROPIC_API_KEY"):
            _phase("Morning decision engine", _run_decision)

    # ── Dashboard (always) ────────────────────────────────────────────────
    _phase("Dashboard regeneration", _run_dashboard)

    elapsed = time.time() - t_start
    log.info("")
    log.info("═" * 65)
    log.info("  ✅  v5 pipeline complete.  mode=%-10s  total=%.0fs (%.1f min)",
             mode, elapsed, elapsed / 60)
    log.info("═" * 65)


# ── Post-pipeline helpers (each isolated so one failure doesn't abort the rest)

def _dispatch_alerts():
    from alerts.notifier import AlertDispatcher
    AlertDispatcher().dispatch_unalerted()

def _run_forecast():
    from ml.demand_forecast import DemandForecaster
    DemandForecaster().run()

def _run_competition():
    from intelligence.competitive_landscape import CompetitiveLandscapeMonitor
    CompetitiveLandscapeMonitor().run()

def _run_alumni():
    from graph.network_gravity import AlumniNetworkMap
    m = AlumniNetworkMap()
    m.ingest_linkedin_departures()
    m.generate_signals()

def _run_predict():
    from predictive.demand_model import DemandPredictor
    m = DemandPredictor()
    preds = m.predict_all()
    m.print_predictions(preds)
    m.save_predictions(preds)

def _run_leaderboard():
    from scoring.aggregator import compute_firm_scores, export_leaderboard_json
    scores = compute_firm_scores()
    export_leaderboard_json(scores)
    log.info("   leaderboard saved to reports/leaderboard.json")

def _run_outreach():
    from outreach.generator import generate_weekly_outreach_plan
    plan = generate_weekly_outreach_plan(10)
    log.info("   outreach plan: %d firms", len(plan))

def _run_ab_report():
    from intelligence.adaptive.ab_optimizer import generate_ab_report
    report = generate_ab_report()
    log.info("   A/B report:\n%s", report[:500])

def _run_decision():
    from intelligence.decision_engine import run_daily_decision_engine
    run_daily_decision_engine(send_telegram_briefing=True, send_outreach_drafts=False)

def _run_dashboard():
    from dashboard.generator import generate_dashboard
    generate_dashboard()
