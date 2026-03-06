"""
Law Firm Expansion Tracker — main entry point.

Run modes:
  python main.py            → full collection + analysis + weekly digest
  python main.py --digest   → send weekly digest from existing DB data only
  python main.py --firm osler → run for a single firm (testing)
"""

import logging
import sys
import argparse
from datetime import datetime, timezone

from config import Config
from firms import FIRMS, FIRMS_BY_ID
from database.db import Database
from scrapers.jobs import JobsScraper
from scrapers.press import PressScraper
from scrapers.publications import PublicationsScraper
from scrapers.website import WebsiteScraper
from scrapers.canlii import CanLIIScraper
from scrapers.chambers import ChambersScraper
from scrapers.lawschool import LawSchoolScraper
from scrapers.barassoc import BarAssociationScraper
from analysis.signals import ExpansionAnalyzer
from alerts.notifier import Notifier

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("tracker.log"),
    ],
)
logger = logging.getLogger("main")


def run(firms_to_run: list = None, digest_only: bool = False):
    logger.info("=" * 70)
    logger.info(f"Law Firm Expansion Tracker — {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}")
    logger.info("=" * 70)

    config = Config()
    db = Database(config.DB_PATH)
    notifier = Notifier(config)
    analyzer = ExpansionAnalyzer(db)

    target_firms = firms_to_run or FIRMS

    if digest_only:
        logger.info("Digest-only mode — skipping scraping")
        _send_digest(db, analyzer, notifier)
        db.close()
        return

    # ------------------------------------------------------------------ #
    #  COLLECTION PHASE
    # ------------------------------------------------------------------ #

    scrapers = [
        JobsScraper(),
        PressScraper(),
        PublicationsScraper(),
        WebsiteScraper(),
        CanLIIScraper(),
        ChambersScraper(),
        LawSchoolScraper(),
        BarAssociationScraper(),
    ]

    all_new_signals = []
    instant_alerts_sent = set()

    for firm in target_firms:
        logger.info(f"\n{'─'*50}")
        logger.info(f"Processing: {firm['name']}")

        for scraper in scrapers:
            try:
                signals = scraper.fetch(firm)
                new_count = 0

                for signal in signals:
                    if db.is_new_signal(signal):
                        db.save_signal(signal)
                        all_new_signals.append(signal)
                        new_count += 1

                        # Instant alert for top-tier signals
                        HIGH_VALUE_TYPES = {"lateral_hire", "bar_leadership", "ranking"}
                        if (config.INSTANT_ALERT_ON_LATERAL
                                and signal["signal_type"] in HIGH_VALUE_TYPES
                                and signal["department"]
                                and f"{firm['id']}_{signal['department']}" not in instant_alerts_sent):
                            notifier.send_new_signal_alert(signal, signal["department"])
                            instant_alerts_sent.add(f"{firm['id']}_{signal['department']}")

                        # Save website hash for change detection
                        if signal["signal_type"] == "website_snapshot":
                            db.save_website_hash(firm["id"], signal["url"], signal["body"])

                logger.info(f"  {scraper.name}: {new_count} new signal(s)")

            except Exception as e:
                logger.error(f"  {scraper.name} failed for {firm['short']}: {e}", exc_info=True)

    logger.info(f"\nTotal new signals collected: {len(all_new_signals)}")

    # ------------------------------------------------------------------ #
    #  ANALYSIS PHASE
    # ------------------------------------------------------------------ #

    # Get all signals for this week (including previously collected)
    weekly_signals = db.get_signals_this_week()
    expansion_alerts = analyzer.analyze(weekly_signals)
    website_changes = analyzer.detect_website_changes(all_new_signals)

    # Save weekly scores to DB
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
    logger.info(f"Website changes: {len(website_changes)}")

    # ------------------------------------------------------------------ #
    #  NOTIFICATION PHASE — only send digest on weekly run (Sundays)
    # ------------------------------------------------------------------ #

    is_sunday = datetime.now(timezone.utc).weekday() == 6
    force_digest = "--digest" in sys.argv

    if is_sunday or force_digest:
        _send_digest(db, analyzer, notifier)
    else:
        logger.info("Not Sunday — skipping weekly digest (instant alerts already sent above)")

    db.close()
    logger.info("\nDone.\n")


def _send_digest(db: Database, analyzer: ExpansionAnalyzer, notifier: Notifier):
    weekly_signals = db.get_signals_this_week()
    expansion_alerts = analyzer.analyze(weekly_signals)
    website_changes = analyzer.detect_website_changes([])  # changes already logged

    # Filter out alerts already sent this week
    new_alerts = [
        a for a in expansion_alerts
        if not db.was_alert_sent(a["firm_id"], a["department"])
    ]

    if new_alerts or website_changes:
        notifier.send_weekly_digest(new_alerts, website_changes)
        for a in new_alerts:
            db.mark_alert_sent(a["firm_id"], a["department"], a["expansion_score"])
        logger.info(f"Weekly digest sent: {len(new_alerts)} alert(s)")
    else:
        notifier.send_weekly_digest([], [])
        logger.info("Weekly digest sent (no new signals this week)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--digest",  action="store_true", help="Send digest from existing data without scraping")
    parser.add_argument("--dashboard", action="store_true", help="Generate dashboard")
    parser.add_argument("--evolve",  action="store_true", help="Run daily self-learning evolution cycle")
    parser.add_argument("--firm",    type=str, help="Run for a single firm ID only (e.g. osler)")
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

    run(firms_to_run=target, digest_only=args.digest)
