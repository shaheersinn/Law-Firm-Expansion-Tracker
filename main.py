"""
Law Firm Expansion Tracker — main entry point.

Run modes:
  python main.py                → full collection + analysis + digest
  python main.py --digest       → send weekly digest from existing DB only
  python main.py --firm osler   → run for a single firm (testing)
  python main.py --dashboard    → regenerate dashboard HTML only
  python main.py --evolve       → run self-learning weight evolution cycle
"""

import logging
import sys
import argparse
from datetime import datetime, timezone

from config import Config
from firms import FIRMS, FIRMS_BY_ID
from database.db import Database
from scrapers.rss          import RSSFeedScraper
from scrapers.press        import PressScraper
from scrapers.jobs         import JobsScraper
from scrapers.publications import PublicationsScraper
from scrapers.website      import WebsiteScraper
from scrapers.canlii       import CanLIIScraper
from scrapers.chambers     import ChambersScraper
from scrapers.awards       import AwardsScraper
from scrapers.lawschool    import LawSchoolScraper
from scrapers.barassoc     import BarAssociationScraper
from scrapers.sedar        import SedarScraper
from scrapers.govtrack     import GovTrackScraper
from scrapers.lobbyist     import LobbyistScraper
from scrapers.conference   import ConferenceScraper
from scrapers.linkedin     import LinkedInScraper
from analysis.signals      import ExpansionAnalyzer
from alerts.notifier       import Notifier

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("tracker.log"),
    ],
)
logger = logging.getLogger("main")

# All 15 scrapers, ordered by signal quality (highest first)
ALL_SCRAPERS = [
    RSSFeedScraper(),
    LinkedInScraper(),
    PressScraper(),
    ChambersScraper(),
    AwardsScraper(),
    BarAssociationScraper(),
    GovTrackScraper(),
    SedarScraper(),
    LobbyistScraper(),
    JobsScraper(),
    LawSchoolScraper(),
    ConferenceScraper(),
    PublicationsScraper(),
    WebsiteScraper(),
    CanLIIScraper(),
]


def run(firms_to_run: list = None, digest_only: bool = False, gen_dashboard: bool = False):
    logger.info("=" * 70)
    logger.info(f"Law Firm Expansion Tracker — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    logger.info(f"Scrapers: {len(ALL_SCRAPERS)} | Firms: {len(firms_to_run or FIRMS)}")
    logger.info("=" * 70)

    config   = Config()
    db       = Database(config.DB_PATH)
    notifier = Notifier(config)
    analyzer = ExpansionAnalyzer(db)

    target_firms = firms_to_run or FIRMS

    if digest_only:
        logger.info("Digest-only mode — skipping scraping")
        _send_digest(db, analyzer, notifier)
        if gen_dashboard:
            _generate_dashboard(db)
        db.close()
        return

    # ------------------------------------------------------------------ #
    #  COLLECTION PHASE
    # ------------------------------------------------------------------ #

    all_new_signals = []

    for firm in target_firms:
        logger.info(f"\n{'─' * 50}")
        logger.info(f"Processing: {firm['name']}")
        firm_new = 0

        for scraper in ALL_SCRAPERS:
            try:
                signals = scraper.fetch(firm)
                for signal in signals:
                    if db.is_new_signal(signal):
                        db.save_signal(signal)
                        all_new_signals.append(signal)
                        firm_new += 1

                        # Update website hash for change detection
                        if signal["signal_type"] == "website_snapshot":
                            db.save_website_hash(
                                firm["id"], signal["url"], signal.get("body", "")
                            )

                logger.info(f"  {scraper.name:<28} {len(signals):>3} signals  ({firm_new} new total)")

            except Exception as e:
                logger.error(f"  {scraper.name} failed for {firm['short']}: {e}", exc_info=True)

    logger.info(f"\nTotal new signals collected: {len(all_new_signals)}")

    # ------------------------------------------------------------------ #
    #  ANALYSIS PHASE
    # ------------------------------------------------------------------ #

    weekly_signals = db.get_signals_this_week()
    expansion_alerts  = analyzer.analyze(weekly_signals)
    website_changes   = analyzer.detect_website_changes(all_new_signals)

    for alert in expansion_alerts:
        db.save_weekly_score(
            firm_id=alert["firm_id"],
            firm_name=alert["firm_name"],
            department=alert["department"],
            score=alert["expansion_score"],
            signal_count=alert["signal_count"],
            breakdown=alert["signal_breakdown"],
        )

    logger.info(f"Expansion alerts: {len(expansion_alerts)}")
    logger.info(f"Website changes:  {len(website_changes)}")

    # ------------------------------------------------------------------ #
    #  NOTIFICATION
    # ------------------------------------------------------------------ #

    _send_digest(db, analyzer, notifier, new_signals=all_new_signals)

    # ------------------------------------------------------------------ #
    #  DASHBOARD
    # ------------------------------------------------------------------ #

    if gen_dashboard:
        _generate_dashboard(db)

    db.close()
    logger.info("\nDone.\n")


def _send_digest(
    db: Database,
    analyzer: ExpansionAnalyzer,
    notifier: Notifier,
    new_signals: list = None,
):
    weekly_signals   = db.get_signals_this_week()
    expansion_alerts = analyzer.analyze(weekly_signals)
    website_changes  = analyzer.detect_website_changes([])

    new_alerts = [
        a for a in expansion_alerts
        if not db.was_alert_sent(a["firm_id"], a["department"])
    ]

    notifier.send_combined_digest(new_alerts, website_changes, new_signals=new_signals or [])

    for a in new_alerts:
        db.mark_alert_sent(a["firm_id"], a["department"], a["expansion_score"])

    logger.info(
        f"Digest sent — {len(new_alerts)} alert(s), "
        f"{len(new_signals or [])} new signal(s)"
    )


def _generate_dashboard(db: Database):
    try:
        from dashboard.generator import DashboardGenerator
        DashboardGenerator(db).generate()
    except Exception as e:
        logger.error(f"Dashboard generation failed: {e}", exc_info=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Canadian Law Firm Expansion Tracker")
    parser.add_argument("--digest",    action="store_true", help="Send digest from existing data only")
    parser.add_argument("--dashboard", action="store_true", help="Generate / refresh dashboard HTML")
    parser.add_argument("--evolve",    action="store_true", help="Run self-learning evolution cycle")
    parser.add_argument("--firm",      type=str,            help="Single firm ID, e.g. osler")
    args = parser.parse_args()

    if args.evolve:
        from learning.evolution import run_evolution
        run_evolution()
        sys.exit(0)

    target = None
    if args.firm:
        firm = FIRMS_BY_ID.get(args.firm)
        if not firm:
            logger.error(f"Unknown firm ID: {args.firm}. Available: {list(FIRMS_BY_ID.keys())}")
            sys.exit(1)
        target = [firm]

    run(
        firms_to_run=target,
        digest_only=args.digest,
        gen_dashboard=args.dashboard,
    )
