"""
main_v5.py — Calgary Law Tracker v5
═══════════════════════════════════════════════════════════════════════
21 signal strategies  ·  Network gravity + alumni map  ·  Feedback loop
Time-series demand forecasting  ·  Competitive landscape intelligence
Counterparty team extraction  ·  Dark pipeline monitor
Reply coaching  ·  Interview prep briefs  ·  Self-learning weights
═══════════════════════════════════════════════════════════════════════
"""
import argparse, logging, sys
from database.db import init_db

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s — %(message)s",
    datefmt="%H:%M:%S")
log = logging.getLogger("main_v5")

def main():
    p = argparse.ArgumentParser(description="Calgary Law Tracker v5")
    p.add_argument("--run",        type=str, help="bi-hourly|daily|weekly|seasonal|full")
    p.add_argument("--decision",   action="store_true", help="Morning decision engine")
    p.add_argument("--brief",      type=str, help="firm_id")
    p.add_argument("--brief-all",  action="store_true", dest="brief_all")
    p.add_argument("--predict",    action="store_true")
    p.add_argument("--forecast",   action="store_true", help="Run demand forecaster")
    p.add_argument("--competition",action="store_true", help="Competition landscape")
    p.add_argument("--train",      action="store_true", help="Retrain learned weights from outcomes")
    p.add_argument("--what-works", action="store_true", dest="what_works", help="Conversion rate report")
    p.add_argument("--outcome",    type=str, help="Record outcome: 'firm=burnet result=reply'")
    p.add_argument("--reply",      type=str, help="firm_id for reply coaching (prompts for reply text)")
    p.add_argument("--interview-prep", type=str, dest="interview_prep", help="firm_id for interview brief")
    p.add_argument("--leaderboard",action="store_true")
    p.add_argument("--outreach",   action="store_true")
    p.add_argument("--digest",     action="store_true")
    p.add_argument("--graph",      action="store_true")
    p.add_argument("--gravity",    action="store_true")
    p.add_argument("--dashboard",  action="store_true")
    p.add_argument("--verify-signals", action="store_true", dest="verify_signals", help="Run the multi-agent scraped-data verifier")
    p.add_argument("--init-db",    action="store_true", dest="init_db")
    # BUG FIX: --ab-report was called in tracker.yml weekly-digest job but
    # was never registered as an argparse argument — argparse silently printed
    # help and exited 0, so no A/B report was ever generated.
    p.add_argument("--ab-report",  action="store_true", dest="ab_report",
                   help="Print A/B outreach performance report")
    p.add_argument("--background", type=str, default="")
    args = p.parse_args()

    init_db()

    if args.init_db:
        log.info("DB initialised.")

    elif args.run:
        from pipeline.orchestrator_v5 import run_full_pipeline_v5
        run_full_pipeline_v5(mode=args.run)

    elif args.decision:
        from intelligence.decision_engine import run_daily_decision_engine
        run_daily_decision_engine(send_telegram_briefing=True, send_outreach_drafts=True)

    elif args.brief:
        from intelligence.brief_generator import generate_firm_brief, print_brief
        print_brief(generate_firm_brief(args.brief, your_background=args.background))

    elif args.brief_all:
        from intelligence.brief_generator import generate_top_opportunities_report
        print(generate_top_opportunities_report(5, args.background))

    elif args.predict:
        from predictive.demand_model import DemandPredictor
        m = DemandPredictor(); p2 = m.predict_all()
        m.print_predictions(p2); m.save_predictions(p2)

    elif args.forecast:
        from ml.demand_forecast import DemandForecaster
        f  = DemandForecaster(); sigs = f.run()
        fc = [s for s in sigs]  # sigs already printed internally

    elif args.competition:
        from intelligence.competitive_landscape import CompetitiveLandscapeMonitor
        CompetitiveLandscapeMonitor().run()

    elif args.train:
        from ml.feedback_loop import retrain_weights
        retrain_weights(); log.info("Weights retrained.")

    elif args.what_works:
        from ml.feedback_loop import conversion_rate_report
        print(conversion_rate_report())

    elif args.outcome:
        # Parse "firm=burnet result=interview signal=42"
        parts = dict(kv.split("=") for kv in args.outcome.split() if "=" in kv)
        from ml.feedback_loop import record_outcome
        record_outcome(
            firm_id=parts.get("firm",""),
            outcome=parts.get("result","no_reply"),
            signal_id=int(parts["signal"]) if "signal" in parts else None,
        )
        log.info("Outcome recorded.")

    elif args.reply:
        reply_text = input(f"Paste their reply to {args.reply}:\n> ")
        from intelligence.reply_coach import ReplyCoach
        coach = ReplyCoach()
        coach.process_reply(args.reply, reply_text, your_background=args.background)

    elif args.interview_prep:
        from intelligence.reply_coach import ReplyCoach
        brief = ReplyCoach().generate_interview_brief(args.interview_prep, args.background)
        print(brief)

    elif args.leaderboard:
        from scoring.aggregator import compute_firm_scores, print_leaderboard
        print_leaderboard(compute_firm_scores())

    elif args.gravity:
        from graph.network_gravity import NetworkGravityModel
        from config_calgary import FIRM_BY_ID, BIGLAW_FIRMS
        g = NetworkGravityModel()
        for bl in BIGLAW_FIRMS:
            n = FIRM_BY_ID.get(bl,{}).get("name",bl)
            print(f"\n  {n}:")
            for p in g.predict_overflow(bl, 4):
                print(f"    → {p['firm_name']:<38} P={p['probability']:.0%}")

    elif args.dashboard:
        from dashboard.generator import generate_dashboard
        generate_dashboard(); log.info("Dashboard regenerated.")

    elif args.verify_signals:
        from database.signal_verifier import verify_recent_signals
        verify_recent_signals(days=90)
        log.info("Signal verification complete.")

    elif args.digest:
        from scoring.aggregator import compute_firm_scores
        from outreach.generator import generate_weekly_outreach_plan
        from alerts.notifier import AlertDispatcher
        AlertDispatcher().send_weekly_digest(compute_firm_scores(), generate_weekly_outreach_plan(5))

    elif args.outreach:
        from outreach.generator import generate_weekly_outreach_plan, print_outreach_plan
        print_outreach_plan(generate_weekly_outreach_plan(10))

    # BUG FIX: wired --ab-report to the actual generate_ab_report() function
    # that lives in intelligence/adaptive/ab_optimizer.py
    elif args.ab_report:
        from intelligence.adaptive.ab_optimizer import generate_ab_report
        print(generate_ab_report())

    else:
        p.print_help()

if __name__ == "__main__":
    main()
