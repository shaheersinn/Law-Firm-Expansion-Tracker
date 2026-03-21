"""
main_enhanced.py  (v3 — full intelligence platform)

12 signal sources · 3 intelligence layers · Pressure model · Velocity index
"""
import argparse, logging, sys
from datetime import datetime
from database.db import init_db

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s — %(message)s",
    datefmt="%H:%M:%S")
log = logging.getLogger("main")

def run_all():
    log.info("═"*60)
    log.info("  CALGARY LAW INTELLIGENCE PLATFORM v3  —  %s",
             datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"))
    log.info("═"*60)

    # ── 5 core strategies ────────────────────────────────────────────────────
    from signals.canlii_litigation  import CanLIILitigationTracker
    from signals.sedar_corporate    import SEDARPlusMonitor
    from signals.linkedin_turnover  import LinkedInTurnoverTracker
    from signals.lsa_hireback       import HirebackVacuumTracker
    from signals.spillage_graph     import DealMonitor

    # ── 3 new signal modules ─────────────────────────────────────────────────
    from signals.partner_clock      import PartnerClockTracker
    from signals.regulatory_wave    import RegulatoryWaveTracker
    from signals.cross_border_intel import SECEdgarCrossBorderTracker, LateralMagnetTracker

    # ── 3 intelligence layers ────────────────────────────────────────────────
    from intelligence.firm_pressure   import FirmPressureAnalyzer
    from intelligence.practice_velocity import (
        PracticeVelocityTracker, DualDepartureDetector, OCIPipelineTracker
    )

    # ── Alerts + Dashboard ───────────────────────────────────────────────────
    from alerts.notifier       import AlertDispatcher
    from dashboard.generator   import generate_dashboard

    log.info("\n[1/11] CanLII Litigation (Follow the Work)")
    CanLIILitigationTracker().run()

    log.info("\n[2/11] SEDAR+ Corporate (Follow the Money)")
    SEDARPlusMonitor().run()

    log.info("\n[3/11] LinkedIn Turnover (Empty Chair)")
    LinkedInTurnoverTracker().check_departures()

    log.info("\n[4/11] LSA Directory (Hireback Vacuum)")
    HirebackVacuumTracker().run()

    log.info("\n[5/11] BigLaw Spillage Graph + Deal Monitor")
    DealMonitor().run()

    log.info("\n[6/11] Partner Clock (Promotion Detector)")
    PartnerClockTracker().run()

    log.info("\n[7/11] Regulatory Wave (AER/Gazette/ASC/Competition)")
    RegulatoryWaveTracker().run()

    log.info("\n[8/11] SEC EDGAR Cross-Border (Alberta 40-F filings)")
    SECEdgarCrossBorderTracker().run()

    log.info("\n[9/11] Lateral Magnet + Competitive Hire Gap")
    LateralMagnetTracker().run()

    log.info("\n[10/11] Intelligence Layers (Pressure + Velocity + Dual Departure + OCI)")
    pressure_results, p_sigs = FirmPressureAnalyzer().run()
    v_sigs = PracticeVelocityTracker().run()
    dd_sigs = DualDepartureDetector().run()
    oci_sigs = OCIPipelineTracker().run()
    total_intel = len(p_sigs)+len(v_sigs)+len(dd_sigs)+len(oci_sigs)
    log.info("[Intel] %d new intelligence-layer signals", total_intel)

    log.info("\n[11/11] Alerts + Dashboard")
    sent = AlertDispatcher().dispatch_unalerted()
    log.info("[Alerts] %d Telegram message(s) sent (new signals only)", sent)
    generate_dashboard()
    log.info("\n✅  Run complete.")

def main():
    parser = argparse.ArgumentParser(description="Calgary Law Intelligence Platform v3")
    parser.add_argument("--all",         action="store_true")
    parser.add_argument("--strategy",    type=str,
        help="canlii|sedar|linkedin|lsa|spillage|partner|regulatory|edgar|lateral|pressure|velocity|dual|oci")
    parser.add_argument("--digest",      action="store_true")
    parser.add_argument("--outreach",    action="store_true")
    parser.add_argument("--leaderboard", action="store_true")
    parser.add_argument("--dashboard",   action="store_true")
    parser.add_argument("--build-roster",action="store_true",dest="build_roster")
    parser.add_argument("--graph",       action="store_true")
    parser.add_argument("--pressure",    action="store_true")
    parser.add_argument("--init-db",     action="store_true",dest="init_db")
    args = parser.parse_args()

    init_db()

    if args.init_db:
        log.info("DB initialised.")
    elif args.all:
        run_all()
    elif args.strategy:
        dispatch_strategy(args.strategy)
    elif args.digest:
        from scoring.aggregator import compute_firm_scores
        from outreach.generator import generate_weekly_outreach_plan
        from alerts.notifier    import AlertDispatcher
        AlertDispatcher().send_weekly_digest(
            compute_firm_scores(), generate_weekly_outreach_plan(5))
    elif args.outreach:
        from outreach.generator import generate_weekly_outreach_plan, print_outreach_plan
        print_outreach_plan(generate_weekly_outreach_plan(10))
    elif args.leaderboard:
        from scoring.aggregator import compute_firm_scores, print_leaderboard
        print_leaderboard(compute_firm_scores())
    elif args.dashboard:
        from dashboard.generator import generate_dashboard
        generate_dashboard()
        log.info("Dashboard regenerated.")
    elif args.build_roster:
        from signals.linkedin_turnover import LinkedInTurnoverTracker
        LinkedInTurnoverTracker().build_roster()
    elif args.pressure:
        from intelligence.firm_pressure import FirmPressureAnalyzer
        results, _ = FirmPressureAnalyzer().run()
        print("\n─── FIRM PRESSURE INDEX ───")
        for r in results[:10]:
            bar = "█"*int(r["pressure_index"]*10) + "░"*max(0,10-int(r["pressure_index"]*10))
            print(f"  {r['firm_name']:38s}  [{bar}]  {r['pressure_index']:.2f}×  {r['status']}")
    elif args.graph:
        from signals.spillage_graph import SpillageGraphAnalyzer
        g = SpillageGraphAnalyzer()
        print("\n─── TOP BOUTIQUES (by co-appearances) ───")
        for b in g.most_vulnerable_boutiques(10):
            print(f"  {b['firm_name']:42s}  {b['total_co_app']} co-appearances")
    else:
        parser.print_help()

def dispatch_strategy(name: str):
    from alerts.notifier     import AlertDispatcher
    from dashboard.generator import generate_dashboard
    strategies = {
        "canlii":     lambda: __import__("signals.canlii_litigation",fromlist=["CanLIILitigationTracker"]).CanLIILitigationTracker().run(),
        "sedar":      lambda: __import__("signals.sedar_corporate",   fromlist=["SEDARPlusMonitor"]).SEDARPlusMonitor().run(),
        "linkedin":   lambda: __import__("signals.linkedin_turnover", fromlist=["LinkedInTurnoverTracker"]).LinkedInTurnoverTracker().check_departures(),
        "lsa":        lambda: __import__("signals.lsa_hireback",      fromlist=["HirebackVacuumTracker"]).HirebackVacuumTracker().run(),
        "spillage":   lambda: __import__("signals.spillage_graph",    fromlist=["DealMonitor"]).DealMonitor().run(),
        "partner":    lambda: __import__("signals.partner_clock",     fromlist=["PartnerClockTracker"]).PartnerClockTracker().run(),
        "regulatory": lambda: __import__("signals.regulatory_wave",   fromlist=["RegulatoryWaveTracker"]).RegulatoryWaveTracker().run(),
        "edgar":      lambda: __import__("signals.cross_border_intel",fromlist=["SECEdgarCrossBorderTracker"]).SECEdgarCrossBorderTracker().run(),
        "lateral":    lambda: __import__("signals.cross_border_intel",fromlist=["LateralMagnetTracker"]).LateralMagnetTracker().run(),
        "pressure":   lambda: __import__("intelligence.firm_pressure",fromlist=["FirmPressureAnalyzer"]).FirmPressureAnalyzer().run(),
        "velocity":   lambda: __import__("intelligence.practice_velocity",fromlist=["PracticeVelocityTracker"]).PracticeVelocityTracker().run(),
        "dual":       lambda: __import__("intelligence.practice_velocity",fromlist=["DualDepartureDetector"]).DualDepartureDetector().run(),
        "oci":        lambda: __import__("intelligence.practice_velocity",fromlist=["OCIPipelineTracker"]).OCIPipelineTracker().run(),
    }
    fn = strategies.get(name)
    if not fn:
        log.error("Unknown strategy: %s. Options: %s", name, list(strategies.keys()))
        sys.exit(1)
    fn()
    AlertDispatcher().dispatch_unalerted()
    generate_dashboard()

if __name__ == "__main__":
    main()
