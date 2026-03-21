"""
pipeline/orchestrator_v5.py  —  single daily run, all strategies  (v5.3)
═══════════════════════════════════════════════════════════════════════
Changes in v5.3:
  • Removed BI_HOURLY / DAILY / WEEKLY / SEASONAL split — everything
    runs once per day in a single "daily" execution.
  • "deep" mode now runs ALL signal strategies + all 54 per-firm
    web scrapers + full ML/intelligence pipeline.
  • Added 4 previously missing v5 strategies:
      PartnerClockTracker     (signals.partner_clock)
      DealCascadeTracker      (signals.predictive.deal_cascade)
      JobDescriptionAnalyzer  (signals.predictive.jd_nlp_analyzer)
      RegulatoryWaveTracker   (signals.regulatory_wave)
  • Default mode is "daily" (same as "deep" — one word, one schedule).
═══════════════════════════════════════════════════════════════════════
"""
import logging, time, sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
log = logging.getLogger(__name__)

# ── All v5 signal strategies — run once daily ────────────────────────────────
ALL_STRATEGIES = [
    # CanLII courts
    ("CanLII Litigation (ABQB)",       "signals.canlii_litigation",         "CanLIILitigationTracker"),
    ("CanLII Appeal Court (ABCA)",     "signals.canlii_litigation",         "CanLIILitigationTracker",  "run_abca"),
    # SEDAR+
    ("SEDAR+ Corporate",               "signals.sedar_corporate",           "SEDARPlusMonitor"),
    # BigLaw spillage
    ("BigLaw Spillage + Gravity",      "signals.spillage_graph",            "DealMonitor"),
    # Macro / commodities
    ("Macro Correlator",               "signals.advanced.macro_correlator", "MacroCorrelator"),
    # Regulatory hearings
    ("AER/AUC Hearings",               "signals.advanced.aer_hearings",     "AERHearingMonitor"),
    # Newswire
    ("Newswire Monitor",               "signals.advanced.newswire_monitor", "NewswireMonitor"),
    # ASC enforcement
    ("ASC Enforcement + TSXV",         "signals.deep.asc_enforcement",      "ASCEnforcementMonitor"),
    # SEC / cross-border
    ("SEC EDGAR Cross-Border",         "signals.cross_border_intel",        "SECEdgarCrossBorderTracker"),
    ("Lateral Magnet Tracker",         "signals.cross_border_intel",        "LateralMagnetTracker"),
    # Counterparty
    ("Counterparty Intel",             "signals.deep.counterparty_intel",   "CounterpartyIntelExtractor"),
    # Fiscal calendar + EDGAR
    ("Fiscal Calendar Predictor",      "signals.deep.fiscal_calendar",      "FiscalCalendarPredictor"),
    # New court filings
    ("New Court Filings",              "signals.deep.new_court_filings",    "NewCourtFilingMonitor"),
    # Website headcount delta
    ("Website Headcount Delta",        "signals.advanced.website_headcount","TeamPageScraper"),
    # SEDI insider cluster
    ("SEDI Insider Cluster",           "signals.advanced.sedi_monitor",     "SEDIMonitor"),
    # Glassdoor NLP
    ("Glassdoor NLP",                  "signals.advanced.glassdoor_signals","GlassdoorSentimentMonitor"),
    # Law school placement
    ("Law School Placement",           "signals.advanced.law_school_placement","LawSchoolPlacementMonitor"),
    # Partner clocks
    ("Partner Clock (deep)",           "signals.deep.partner_clock",        "PartnerPressureClock"),
    ("Partner Clock (root)",           "signals.partner_clock",             "PartnerClockTracker"),
    # Corporate registry
    ("Corporate Registry",             "signals.deep.corporate_registry",   "CorporateRegistryMonitor"),
    # Career semantics
    ("Career Semantic Monitor",        "signals.deep.career_semantic",      "CareerSemanticMonitor"),
    # Dark pipeline
    ("Dark Pipeline",                  "signals.deep.dark_pipeline",        "DarkPipelineMonitor"),
    # Competition bureau
    ("Competition Bureau",             "signals.deep.competition_bureau",   "CompetitionBureauMonitor"),
    # LinkedIn turnover
    ("LinkedIn Turnover",              "signals.linkedin_turnover",         "LinkedInTurnoverTracker",   "check_departures"),
    # LSA hireback
    ("LSA Hireback",                   "signals.lsa_hireback",              "HirebackVacuumTracker"),
    # Predictive strategies (new in v5.3)
    ("Regulatory Wave",                "signals.regulatory_wave",           "RegulatoryWaveTracker"),
    ("Deal Cascade",                   "signals.predictive.deal_cascade",   "DealCascadeTracker"),
    ("JD NLP Analyzer",                "signals.predictive.jd_nlp_analyzer","JobDescriptionAnalyzer"),
]


def _run_strategies(strats: list) -> int:
    total = 0
    for item in strats:
        name, mod_path, cls_name = item[0], item[1], item[2]
        method = item[3] if len(item) > 3 else "run"
        t0 = time.time()
        try:
            mod  = __import__(mod_path, fromlist=[cls_name])
            inst = getattr(mod, cls_name)()
            if not hasattr(inst, method):
                method = "run"
            sigs = getattr(inst, method)() or []
            log.info("  OK  %-42s %2d signals  %.1fs", name, len(sigs), time.time()-t0)
            total += len(sigs)
        except Exception as e:
            log.error("  ERR %-42s FAILED: %s", name, e)
    return total


def _phase(title: str, fn, *args, **kwargs):
    log.info("")
    log.info("-- %s --", title)
    t0 = time.time()
    try:
        fn(*args, **kwargs)
        log.info("   done  %.1fs", time.time() - t0)
    except Exception as e:
        log.error("   FAILED: %s", e)


def run_full_pipeline_v5(mode: str = "daily"):
    """
    Single entry-point — runs everything once.

    Mode "daily" (and "deep") is identical: all strategies + all scrapers +
    full ML/intelligence pipeline.  All other mode names are accepted for
    backwards-compatibility but also run the full set.
    """
    from database.db import init_db
    init_db()

    t_start = time.time()
    n_strats = len(ALL_STRATEGIES)

    log.info("=" * 65)
    log.info("  CALGARY LAW TRACKER v5  [DAILY]  %d strategies + 54 firm scrapers", n_strats)
    log.info("=" * 65)

    # ── Phase 1: All v5 signal strategies ────────────────────────────────────
    log.info("")
    log.info("PHASE 1 — v5 signal strategies (%d)", n_strats)
    n = _run_strategies(ALL_STRATEGIES)
    log.info("Phase 1 complete — %d signals", n)

    # ── Phase 2: CanLII Provincial Court (extra DB) ───────────────────────────
    log.info("")
    log.info("PHASE 2 — CanLII Provincial Court (ABPC)")
    try:
        from signals.canlii_litigation import CanLIILitigationTracker
        inst = CanLIILitigationTracker()
        sigs = inst.run(databases=["abpc"]) or []
        log.info("  OK  CanLII Provincial Court (abpc)  %d signals", len(sigs))
        n += len(sigs)
    except Exception as e:
        log.error("  ERR CanLII ABPC: %s", e)

    # ── Phase 3: Signal verification (double-check accuracy) ─────────────────
    _phase("Signal accuracy verification", _run_verifier)

    # ── Phase 4: Dispatch alerts ──────────────────────────────────────────────
    _phase("Alerts dispatch", _dispatch_alerts)

    # ── Phase 4: v3 per-firm scraper loop (54 scrapers x 35 firms) ───────────
    _phase("v3 per-firm scraper loop (54 scrapers x 35 firms)", _run_firm_scrapers)

    # ── Phase 5: ML layers ───────────────────────────────────────────────────
    _phase("ML demand forecast",        _run_forecast)
    _phase("Competitive landscape",     _run_competition)
    _phase("Alumni network map",        _run_alumni)
    _phase("Predictive demand model",   _run_predict)

    # ── Phase 6: Scoring & intelligence ──────────────────────────────────────
    _phase("Firm leaderboard export",   _run_leaderboard)
    _phase("Outreach plan",             _run_outreach)
    _phase("A/B report",                _run_ab_report)
    if os.getenv("ANTHROPIC_API_KEY"):
        _phase("Decision engine",       _run_decision)

    # ── Phase 7: Dashboard ───────────────────────────────────────────────────
    _phase("Dashboard regeneration",    _run_dashboard)

    elapsed = time.time() - t_start
    log.info("")
    log.info("=" * 65)
    log.info("  DAILY RUN COMPLETE  total=%.0fs (%.1f min)", elapsed, elapsed / 60)
    log.info("=" * 65)


# ── Phase helpers ─────────────────────────────────────────────────────────────

def _run_verifier():
    from database.signal_verifier import verify_recent_signals
    verify_recent_signals(days=1)

def _dispatch_alerts():
    from alerts.notifier import AlertDispatcher
    AlertDispatcher().dispatch_unalerted()

def _run_firm_scrapers():
    from pipeline.firm_scrapers import run_firm_scrapers
    run_firm_scrapers()

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
    export_leaderboard_json(compute_firm_scores())

def _run_outreach():
    from outreach.generator import generate_weekly_outreach_plan
    generate_weekly_outreach_plan(10)

def _run_ab_report():
    from intelligence.adaptive.ab_optimizer import generate_ab_report
    log.info(generate_ab_report()[:300])

def _run_decision():
    from intelligence.decision_engine import run_daily_decision_engine
    run_daily_decision_engine(send_telegram_briefing=True, send_outreach_drafts=False)

def _run_dashboard():
    from dashboard.generator import generate_dashboard
    generate_dashboard()
