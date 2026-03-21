"""
main_v3.py — Calgary Law Tracker v3
─────────────────────────────────────
Usage:
  python main_v3.py --run bi-hourly          # every 2h: fast strategies
  python main_v3.py --run daily              # once/day: scraping-heavy
  python main_v3.py --run weekly             # LinkedIn check
  python main_v3.py --run seasonal           # LSA hireback (Sept/Oct)
  python main_v3.py --run full               # everything

  python main_v3.py --brief bennett_jones    # intelligence brief for one firm
  python main_v3.py --brief-all              # comparative brief: top 5 firms
  python main_v3.py --predict                # predictive demand model P30/P60/P90
  python main_v3.py --leaderboard            # signal-weighted firm scores
  python main_v3.py --outreach               # personalized email drafts
  python main_v3.py --digest                 # send weekly email digest
  python main_v3.py --graph                  # spillage graph analysis
  python main_v3.py --dashboard              # regenerate dashboard only
  python main_v3.py --init-db                # initialise database
"""

import argparse, logging, sys
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("main_v3")


def main():
    parser = argparse.ArgumentParser(description="Calgary Law Tracker v3")
    parser.add_argument("--run",       type=str, help="bi-hourly|daily|weekly|seasonal|full")
    parser.add_argument("--brief",     type=str, help="firm_id for intelligence brief")
    parser.add_argument("--brief-all", action="store_true", dest="brief_all")
    parser.add_argument("--predict",   action="store_true")
    parser.add_argument("--leaderboard",action="store_true")
    parser.add_argument("--outreach",  action="store_true")
    parser.add_argument("--digest",    action="store_true")
    parser.add_argument("--graph",     action="store_true")
    parser.add_argument("--dashboard", action="store_true")
    parser.add_argument("--init-db",   action="store_true", dest="init_db")
    parser.add_argument("--background",type=str, default="",
                        help="Your background for personalised briefs")
    args = parser.parse_args()

    from database.db import init_db
    init_db()

    if args.init_db:
        log.info("DB initialised.")

    elif args.run:
        from pipeline.orchestrator import run_full_pipeline
        run_full_pipeline(mode=args.run, run_predictions=True)

    elif args.brief:
        from intelligence.brief_generator import generate_firm_brief, print_brief
        result = generate_firm_brief(args.brief, your_background=args.background)
        print_brief(result)

    elif args.brief_all:
        from intelligence.brief_generator import generate_top_opportunities_report
        report = generate_top_opportunities_report(top_n=5, your_background=args.background)
        print(report)

    elif args.predict:
        from predictive.demand_model import DemandPredictor
        model = DemandPredictor()
        preds = model.predict_all()
        model.print_predictions(preds)
        model.save_predictions(preds)

    elif args.leaderboard:
        from scoring.aggregator import compute_firm_scores, print_leaderboard
        print_leaderboard(compute_firm_scores())

    elif args.outreach:
        from outreach.generator import generate_weekly_outreach_plan, print_outreach_plan
        print_outreach_plan(generate_weekly_outreach_plan(top_n=10))

    elif args.digest:
        from scoring.aggregator import compute_firm_scores
        from outreach.generator import generate_weekly_outreach_plan
        from alerts.notifier import AlertDispatcher
        AlertDispatcher().send_weekly_digest(
            compute_firm_scores(), generate_weekly_outreach_plan(5)
        )

    elif args.graph:
        from signals.spillage_graph import SpillageGraphAnalyzer, ConflictRadar
        from config_calgary import FIRM_BY_ID
        graph = SpillageGraphAnalyzer()
        print("\n═══ SPILLAGE GRAPH — TOP OVERFLOW BOUTIQUES ═══")
        for b in graph.most_vulnerable_boutiques(top_n=10):
            print(f"  {b['firm_name']:42s}  {b['total_co_app']} co-appearances")
        print("\n═══ BETWEENNESS CENTRALITY ═══")
        for fid, score in sorted(graph.betweenness_centrality().items(),
                                  key=lambda x: x[1], reverse=True)[:10]:
            print(f"  {FIRM_BY_ID.get(fid,{}).get('name',fid):42s}  {score:.2f}")
        print("\n═══ CONFLICT RADAR ═══")
        for item in ConflictRadar().radar_report():
            bl = [FIRM_BY_ID.get(f,{}).get("name",f) for f in item["biglaw_with_conflict"]]
            print(f"  {item['energy_company']:30s}  ← {', '.join(bl)}")

    elif args.dashboard:
        from dashboard.generator import generate_dashboard
        generate_dashboard()
        log.info("Dashboard regenerated.")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
