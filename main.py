"""
Law Firm Expansion Tracker v3 — 28 scrapers.
"""

import logging
import sys
import argparse
import concurrent.futures
from datetime import datetime, timezone

from config import Config
from firms import FIRMS, FIRMS_BY_ID
from database.db import Database

from scrapers.lateral_tracker    import LateralTrackScraper
from scrapers.deal_tracker       import DealTrackScraper
from scrapers.media              import MediaScraper
from scrapers.office_tracker     import OfficeTracker
from scrapers.recruiter          import RecruiterScraper
from scrapers.google_news        import GoogleNewsScraper
from scrapers.press              import PressScraper
from scrapers.publications       import PublicationsScraper
from scrapers.website            import WebsiteScraper
from scrapers.chambers           import ChambersScraper
from scrapers.awards             import AwardsScraper
from scrapers.barassoc           import BarAssociationScraper
from scrapers.jobs               import JobsScraper
from scrapers.lawschool          import LawSchoolScraper
from scrapers.rss                import RSSFeedScraper
from scrapers.govtrack           import GovTrackScraper
from scrapers.sedar              import SedarScraper
from scrapers.conference         import ConferenceScraper
from scrapers.lobbyist           import LobbyistScraper
from scrapers.canlii             import CanLIIScraper
from scrapers.linkedin           import LinkedInScraper
# ── 7 New scrapers ───────────────────────────────────────────────────────────
from scrapers.podcast            import PodcastScraper
from scrapers.alumni             import AlumniTrackScraper
from scrapers.thought_leader     import ThoughtLeaderScraper
from scrapers.diversity          import DiversityScraper
from scrapers.cipo_scraper       import CIPOScraper
from scrapers.event_scraper      import EventScraper
from scrapers.signal_cross_ref   import SignalCrossRefScraper

from analysis.signals  import ExpansionAnalyzer
from alerts.notifier   import Notifier

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("tracker.log"),
    ],
)
logger = logging.getLogger("main")

SCRAPER_TIMEOUT = 90


def _build_scrapers(db):
    return [
        LateralTrackScraper(),
        DealTrackScraper(),
        MediaScraper(),
        OfficeTracker(),
        RecruiterScraper(),
        GoogleNewsScraper(),
        PressScraper(),
        PublicationsScraper(),
        WebsiteScraper(),
        ChambersScraper(),
        AwardsScraper(),
        BarAssociationScraper(),
        JobsScraper(),
        LawSchoolScraper(),
        RSSFeedScraper(),
        GovTrackScraper(),
        SedarScraper(),
        ConferenceScraper(),
        LobbyistScraper(),
        CanLIIScraper(),
        LinkedInScraper(),
        # ── 7 new ───────────────────────────────────
        PodcastScraper(),
        AlumniTrackScraper(),
        ThoughtLeaderScraper(),
        DiversityScraper(),
        CIPOScraper(),
        EventScraper(),
        SignalCrossRefScraper(db=db),  # receives db reference
    ]


def run(firms_to_run=None, digest_only=False):
    logger.info("=" * 70)
    logger.info(
        f"Law Firm Expansion Tracker v3  —  "
        f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    )
    config   = Config()
    db       = Database(config.DB_PATH)
    scrapers = _build_scrapers(db)
    notifier = Notifier(config)
    analyzer = ExpansionAnalyzer(db)
    target   = firms_to_run or FIRMS

    logger.info(f"Scrapers: {len(scrapers)}  |  Firms: {len(target)}")
    logger.info("=" * 70)

    if digest_only:
        logger.info("Digest-only mode")
        alerts  = analyzer.analyze(db.get_signals_this_week())
        changes = analyzer.detect_website_changes([])
        _send_digest(db, notifier, alerts, changes, [])
        _generate_dashboard(db)
        db.close()
        return

    all_new: list[dict] = []

    for firm in target:
        logger.info(f"\n{'─'*50}")
        logger.info(f"Processing: {firm['name']}")

        for scraper in scrapers:
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                    fut = ex.submit(scraper.fetch, firm)
                    try:
                        signals = fut.result(timeout=SCRAPER_TIMEOUT)
                    except concurrent.futures.TimeoutError:
                        logger.warning(f"  {scraper.name} timed out — skip")
                        continue

                new_count = 0
                for sig in (signals or []):
                    if db.is_new_signal(sig):
                        db.save_signal(sig)
                        all_new.append(sig)
                        new_count += 1
                        if sig.get("signal_type") == "website_snapshot":
                            db.save_website_hash(firm["id"], sig["url"], sig.get("body", ""))

                logger.info(f"  {scraper.name:<36} {len(signals or []):>3} signals  ({new_count} new)")

            except Exception as e:
                logger.error(f"  {scraper.name} [{firm['short']}]: {e}", exc_info=False)

    logger.info(f"\nTotal new signals this run: {len(all_new)}")

    weekly  = db.get_signals_this_week()
    alerts  = analyzer.analyze(weekly)
    changes = analyzer.detect_website_changes(all_new)

    for a in alerts:
        db.save_weekly_score(
            firm_id=a["firm_id"], firm_name=a["firm_name"],
            department=a["department"], score=a["expansion_score"],
            signal_count=a["signal_count"], breakdown=a["signal_breakdown"],
        )

    logger.info(f"Expansion alerts: {len(alerts)}")
    _send_digest(db, notifier, alerts, changes, all_new)
    _generate_dashboard(db)
    db.close()
    logger.info("\nDone.\n")


def _send_digest(db, notifier, alerts, changes, new_signals):
    new_alerts = [a for a in alerts if not db.was_alert_sent(a["firm_id"], a["department"])]
    notifier.send_combined_digest(new_alerts, changes, new_signals=new_signals)
    for a in new_alerts:
        db.mark_alert_sent(a["firm_id"], a["department"], a["expansion_score"])
    logger.info(f"Digest: {len(new_alerts)} new alerts")


def _generate_dashboard(db):
    try:
        from dashboard.generate import generate_dashboard
        generate_dashboard(db_path=db.db_path)
        logger.info("Dashboard → docs/index.html")
    except Exception as e:
        logger.error(f"Dashboard: {e}", exc_info=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--digest",    action="store_true")
    parser.add_argument("--dashboard", action="store_true")
    parser.add_argument("--evolve",    action="store_true")
    parser.add_argument("--firm",      type=str)
    args = parser.parse_args()

    if args.evolve:
        from learning.evolution import run_evolution
        run_evolution(); sys.exit(0)

    if args.dashboard:
        cfg = Config(); db = Database(cfg.DB_PATH)
        _generate_dashboard(db); db.close(); sys.exit(0)

    target = None
    if args.firm:
        firm = FIRMS_BY_ID.get(args.firm)
        if not firm:
            print(f"Unknown firm: {args.firm}")
            sys.exit(1)
        target = [firm]

    run(firms_to_run=target, digest_only=args.digest)
