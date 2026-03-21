"""
main_v4.py — Calgary Law Tracker v4
─────────────────────────────────────
16 signal strategies · Network gravity model · Alumni reverse map
Predictive P30/P60/P90 model · Intelligence briefs · Decision engine
Autonomous outreach with complete email drafts in Telegram
Three-touch follow-up system · Fiscal calendar predictor
Partner pressure clock · Corporate registry · SEC EDGAR cross-border intel
"""

import argparse, logging, sys
from database.db import init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("main_v4")

BI_HOURLY = [
    ("CanLII Litigation",    lambda: __import__("signals.canlii_litigation", fromlist=["CanLIILitigationTracker"]).CanLIILitigationTracker().run()),
    ("SEDAR+ Corporate",     lambda: __import__("signals.sedar_corporate",   fromlist=["SEDARPlusMonitor"]).SEDARPlusMonitor().run()),
    ("BigLaw Spillage",      lambda: __import__("signals.spillage_graph",    fromlist=["DealMonitor"]).DealMonitor().run()),
    ("Macro Correlator",     lambda: __import__("signals.advanced.macro_correlator", fromlist=["MacroCorrelator"]).MacroCorrelator().run()),
    ("AER/AUC Hearings",     lambda: __import__("signals.advanced.aer_hearings",     fromlist=["AERHearingMonitor"]).AERHearingMonitor().run()),
    ("Newswire Monitor",     lambda: __import__("signals.advanced.newswire_monitor", fromlist=["NewswireMonitor"]).NewswireMonitor().run()),
    ("ASC + TSXV",           lambda: __import__("signals.deep.asc_enforcement",      fromlist=["ASCEnforcementMonitor"]).ASCEnforcementMonitor().run()),
    ("SEC EDGAR Cross-Border",lambda:__import__("signals.deep.fiscal_calendar",      fromlist=["CrossBorderIntelligence"]).CrossBorderIntelligence().run()),
]

DAILY_STRATS = [
    ("Website Headcount",    lambda: __import__("signals.advanced.website_headcount",  fromlist=["TeamPageScraper"]).TeamPageScraper().run()),
    ("SEDI Insider Cluster", lambda: __import__("signals.advanced.sedi_monitor",       fromlist=["SEDIMonitor"]).SEDIMonitor().run()),
    ("Glassdoor NLP",        lambda: __import__("signals.advanced.glassdoor_signals",  fromlist=["GlassdoorSentimentMonitor"]).GlassdoorSentimentMonitor().run()),
    ("Law School Placement", lambda: __import__("signals.advanced.law_school_placement",fromlist=["LawSchoolPlacementMonitor"]).LawSchoolPlacementMonitor().run()),
    ("Partner Clock",        lambda: __import__("signals.deep.partner_clock",          fromlist=["PartnerPressureClock"]).PartnerPressureClock().run()),
    ("Corporate Registry",   lambda: __import__("signals.deep.corporate_registry",     fromlist=["CorporateRegistryMonitor"]).CorporateRegistryMonitor().run()),
]

WEEKLY_STRATS = [
    ("LinkedIn Turnover",    lambda: __import__("signals.linkedin_turnover", fromlist=["LinkedInTurnoverTracker"]).LinkedInTurnoverTracker().check_departures()),
]

SEASONAL_STRATS = [
    ("LSA Hireback",         lambda: __import__("signals.lsa_hireback",      fromlist=["HirebackVacuumTracker"]).HirebackVacuumTracker().run()),
]


def _run_strats(strats, mode):
    import time
    log.info("── Running %d strategies [%s] ──", len(strats), mode)
    total = 0
    for name, fn in strats:
        t0 = time.time()
        try:
            sigs = fn() or []
            log.info("  ✅  %-30s %2d signals  %.1fs", name, len(sigs), time.time()-t0)
            total += len(sigs)
        except Exception as e:
            log.error("  ❌  %-30s FAILED: %s", name, e)
    return total


def main():
    parser = argparse.ArgumentParser(description="Calgary Law Tracker v4 — 16 strategies")
    parser.add_argument("--run",       type=str, help="bi-hourly|daily|weekly|seasonal|full")
    parser.add_argument("--decision",  action="store_true", help="Morning decision engine + outreach")
    parser.add_argument("--brief",     type=str, help="firm_id for intelligence brief")
    parser.add_argument("--brief-all", action="store_true", dest="brief_all")
    parser.add_argument("--predict",   action="store_true")
    parser.add_argument("--leaderboard",action="store_true")
    parser.add_argument("--outreach",  action="store_true")
    parser.add_argument("--digest",    action="store_true")
    parser.add_argument("--graph",     action="store_true")
    parser.add_argument("--dashboard", action="store_true")
    parser.add_argument("--init-db",   action="store_true", dest="init_db")
    parser.add_argument("--gravity",   action="store_true", help="Print gravity model predictions")
    parser.add_argument("--background",type=str, default="")
    args = parser.parse_args()

    init_db()

    if args.init_db:
        log.info("DB initialised.")

    elif args.run:
        sets = {
            "bi-hourly": BI_HOURLY,
            "daily":     DAILY_STRATS,
            "weekly":    WEEKLY_STRATS,
            "seasonal":  SEASONAL_STRATS,
            "full":      BI_HOURLY + DAILY_STRATS + WEEKLY_STRATS + SEASONAL_STRATS,
        }
        strats = sets.get(args.run, BI_HOURLY)
        n = _run_strats(strats, args.run)
        log.info("Total new signals: %d", n)

        from alerts.notifier import AlertDispatcher
        AlertDispatcher().dispatch_unalerted()

        from dashboard.generator import generate_dashboard
        generate_dashboard()

    elif args.decision:
        from intelligence.decision_engine import run_daily_decision_engine
        run_daily_decision_engine(send_telegram_briefing=True, send_outreach_drafts=True)

    elif args.brief:
        from intelligence.brief_generator import generate_firm_brief, print_brief
        print_brief(generate_firm_brief(args.brief, your_background=args.background))

    elif args.brief_all:
        from intelligence.brief_generator import generate_top_opportunities_report
        print(generate_top_opportunities_report(top_n=5, your_background=args.background))

    elif args.predict:
        from predictive.demand_model import DemandPredictor
        m = DemandPredictor()
        p = m.predict_all()
        m.print_predictions(p)
        m.save_predictions(p)

    elif args.leaderboard:
        from scoring.aggregator import compute_firm_scores, print_leaderboard
        print_leaderboard(compute_firm_scores())

    elif args.gravity:
        from graph.network_gravity import NetworkGravityModel
        from config_calgary import FIRM_BY_ID, BIGLAW_FIRMS
        model = NetworkGravityModel()
        print("\n═══ GRAVITY MODEL ═══")
        for bl in BIGLAW_FIRMS:
            name = FIRM_BY_ID.get(bl,{}).get("name",bl)
            print(f"\n  {name}:")
            for p in model.predict_overflow(bl, top_n=4):
                print(f"    → {p['firm_name']:<38} P={p['probability']:.0%}  {p['co_appearances']} co-app")

    elif args.digest:
        from scoring.aggregator import compute_firm_scores
        from outreach.generator import generate_weekly_outreach_plan
        from alerts.notifier import AlertDispatcher
        AlertDispatcher().send_weekly_digest(compute_firm_scores(), generate_weekly_outreach_plan(5))

    elif args.outreach:
        from outreach.generator import generate_weekly_outreach_plan, print_outreach_plan
        print_outreach_plan(generate_weekly_outreach_plan(10))

    elif args.dashboard:
        from dashboard.generator import generate_dashboard
        generate_dashboard(); log.info("Dashboard regenerated.")

    elif args.graph:
        from signals.spillage_graph import SpillageGraphAnalyzer, ConflictRadar
        from config_calgary import FIRM_BY_ID
        g = SpillageGraphAnalyzer()
        print("\n─── Top Boutiques ───")
        for b in g.most_vulnerable_boutiques(10):
            print(f"  {b['firm_name']:42s}  {b['total_co_app']} co-appearances")

    else:
        parser.print_help()

if __name__ == "__main__":
    main()
