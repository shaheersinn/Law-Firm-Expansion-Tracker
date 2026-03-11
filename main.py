"""
Law Firm Expansion Tracker — main entry point.

Run modes:
  python main.py                → full collection + analysis + digest
  python main.py --digest       → send weekly digest from existing DB only
  python main.py --firm osler   → run for a single firm (testing)
  python main.py --evolve       → run self-learning evolution cycle
  python main.py --dashboard    → regenerate dashboard from existing data
"""

import logging
import sys
import argparse
from datetime import datetime, timezone

from config import Config
from firms import FIRMS, FIRMS_BY_ID
from database.db import Database
from scrapers.lateral_track import LateralTrackScraper
from scrapers.deal_track import DealTrackScraper
from scrapers.media import MediaScraper
from scrapers.office_tracker import OfficeTracker
from scrapers.recruiter import RecruiterScraper
from scrapers.google_news import GoogleNewsScraper
from scrapers.press import PressScraper
from scrapers.publications import PublicationsScraper
from scrapers.website import WebsiteScraper
from scrapers.chambers import ChambersScraper
from scrapers.awards import AwardsScraper
from scrapers.barassoc import BarAssociationScraper
from scrapers.jobs import JobsScraper
from scrapers.lawschool import LawSchoolScraper
from scrapers.rss import RSSFeedScraper
from scrapers.govtrack import GovTrackScraper
from scrapers.sedar import SedarScraper
from scrapers.conference import ConferenceScraper
from scrapers.lobbyist import LobbyistScraper
from scrapers.canlii import CanLIIScraper
from scrapers.linkedin import LinkedInScraper
from scrapers.podcast import PodcastScraper
from scrapers.alumni_track import AlumniTrackScraper
from scrapers.thought_leader import ThoughtLeaderScraper
from scrapers.diversity import DiversityScraper
from scrapers.cipo import CIPOScraper
from scrapers.event import EventScraper
from scrapers.signal_crossref import SignalCrossRefScraper
from analysis.signals import ExpansionAnalyzer
from alerts.notifier import Notifier
from dashboard.generator import DashboardGenerator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("tracker.log"),
    ],
)
logger = logging.getLogger("main")

# Scraper order matters: lateral/media first, crossref last
SCRAPER_CLASSES = [
    LateralTrackScraper,
    DealTrackScraper,
    MediaScraper,
    OfficeTracker,
    RecruiterScraper,
    GoogleNewsScraper,
    PressScraper,
    PublicationsScraper,
    WebsiteScraper,
    ChambersScraper,
    AwardsScraper,
    BarAssociationScraper,
    JobsScraper,
    LawSchoolScraper,
    RSSFeedScraper,
    GovTrackScraper,
    SedarScraper,
    ConferenceScraper,
    LobbyistScraper,
    CanLIIScraper,
    LinkedInScraper,
    PodcastScraper,
    AlumniTrackScraper,
    ThoughtLeaderScraper,
    DiversityScraper,
    CIPOScraper,
    EventScraper,
    # SignalCrossRefScraper added per-firm below (needs run context)
]


def run(firms_to_run: list | None = None, digest_only: bool = False):
    logger.info("=" * 70)
    logger.info(f"Law Firm Expansion Tracker v3  —  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    logger.info(f"Scrapers: {len(SCRAPER_CLASSES) + 1}  |  Firms: {len(firms_to_run or FIRMS)}")
    logger.info("=" * 70)

    config = Config()
    db     = Database(config.DB_PATH)
    notifier  = Notifier(config)
    analyzer  = ExpansionAnalyzer(db)
    dashboard = DashboardGenerator(db)

    target_firms = firms_to_run or FIRMS

    if digest_only:
        logger.info("Digest-only mode — skipping scraping")
        _send_digest(db, analyzer, notifier, dashboard)
        db.close()
        return

    # ------------------------------------------------------------------ #
    #  COLLECTION PHASE
    # ------------------------------------------------------------------ #

    scrapers = [cls() for cls in SCRAPER_CLASSES]
    all_new_signals: list[dict] = []

    # Health tracking: scraper_name → total signals found across all firms
    scraper_totals: dict[str, int] = {s.name: 0 for s in scrapers}
    scraper_totals["SignalCrossRefScraper"] = 0

    for firm in target_firms:
        logger.info(f"\n{'─' * 50}")
        logger.info(f"Processing: {firm['name']}")

        firm_run_signals: list[dict] = []

        for scraper in scrapers:
            try:
                fetched = scraper.fetch(firm)
                new_count = 0
                for signal in fetched:
                    firm_run_signals.append(signal)
                    if db.is_new_signal(signal):
                        db.save_signal(signal)
                        all_new_signals.append(signal)
                        new_count += 1
                        if signal["signal_type"] == "website_snapshot":
                            db.save_website_hash(firm["id"], signal["url"], signal.get("body", ""))

                scraper_totals[scraper.name] = scraper_totals.get(scraper.name, 0) + len(fetched)
                logger.info(f"  {scraper.name:<38} {len(fetched)} signals  ({new_count} new)")
            except Exception as e:
                logger.error(f"  {scraper.name} failed for {firm['short']}: {e}", exc_info=True)

        # Cross-ref scraper gets the full run context for this firm
        try:
            crossref = SignalCrossRefScraper(current_run_signals=firm_run_signals)
            fetched = crossref.fetch(firm)
            new_count = 0
            for signal in fetched:
                if db.is_new_signal(signal):
                    db.save_signal(signal)
                    all_new_signals.append(signal)
                    new_count += 1
            scraper_totals["SignalCrossRefScraper"] += len(fetched)
            logger.info(f"  {'SignalCrossRefScraper':<38} {len(fetched)} signals  ({new_count} new)")
        except Exception as e:
            logger.error(f"  SignalCrossRefScraper failed for {firm['short']}: {e}", exc_info=True)

    logger.info(f"\nTotal new signals this run: {len(all_new_signals)}")

    # ── Scraper health summary ───────────────────────────────────────────
    silent_scrapers = [name for name, total in scraper_totals.items() if total == 0]
    if silent_scrapers:
        logger.warning(
            f"SCRAPER HEALTH: {len(silent_scrapers)} scrapers returned 0 signals "
            f"across all firms: {', '.join(silent_scrapers)}"
        )
    else:
        active_count = sum(1 for t in scraper_totals.values() if t > 0)
        logger.info(f"SCRAPER HEALTH: {active_count}/{len(scraper_totals)} scrapers active")

    # ── Zero-signal run detection — Telegram alert ───────────────────────
    if len(all_new_signals) == 0:
        logger.warning(
            "⚠️  ZERO NEW SIGNALS this run. Possible causes: "
            "all items already in DB (21-day dedup), scrapers blocked, "
            "or classifier thresholds too strict."
        )
        if hasattr(notifier, "send_health_alert"):
            notifier.send_health_alert(
                "⚠️ Zero new signals this run",
                details=(
                    f"Silent scrapers: {len(silent_scrapers)}/{len(scraper_totals)}\n"
                    f"Firms processed: {len(target_firms)}\n"
                    f"Check: dedup window, scraper logs, network access."
                ),
            )
        else:
            logger.warning("Notifier.send_health_alert not available — skipping Telegram health alert")

    # ------------------------------------------------------------------ #
    #  ANALYSIS PHASE
    # ------------------------------------------------------------------ #

    weekly_signals  = db.get_signals_this_week()
    expansion_alerts = analyzer.analyze(weekly_signals)
    website_changes  = analyzer.detect_website_changes(all_new_signals)

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

    # ------------------------------------------------------------------ #
    #  NOTIFICATION + DASHBOARD
    # ------------------------------------------------------------------ #

    _send_digest(db, analyzer, notifier, dashboard, new_signals=all_new_signals)
    try:
        from learning.evolution import run_evolution
        learning_report = run_evolution(force=True, db_path=config.DB_PATH)
        if learning_report:
            feedback = learning_report.get("feedback_summary", {})
            logger.info(
                "Self-training complete: confirmed=%s false_positive=%s keywords_updated=%s",
                feedback.get("confirmed", 0),
                feedback.get("false_positive", 0),
                learning_report.get("keywords_updated", 0),
            )
    except Exception as exc:
        logger.warning(f"Self-training step failed: {exc}")
    db.close()
    logger.info("\nDone.\n")


def _send_digest(
    db: Database,
    analyzer: ExpansionAnalyzer,
    notifier: Notifier,
    dashboard: DashboardGenerator,
    new_signals: list | None = None,
):
    weekly_signals   = db.get_signals_this_week()
    expansion_alerts = analyzer.analyze(weekly_signals)
    # BUG FIX: was passing [] instead of the actual new_signals list,
    # so website change alerts were never generated during normal runs.
    website_changes  = analyzer.detect_website_changes(new_signals or [])

    new_alerts = [
        a for a in expansion_alerts
        if not db.was_alert_sent(a["firm_id"], a["department"])
    ]

    notifier.send_combined_digest(new_alerts, website_changes, new_signals=new_signals or [])
    for a in new_alerts:
        db.mark_alert_sent(a["firm_id"], a["department"], a["expansion_score"])

    dashboard.generate()

    logger.info(
        f"Digest: {len(new_alerts)} new alerts  |  "
        f"Weekly signals in DB: {len(weekly_signals)}  |  "
        f"Website changes: {len(website_changes)}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--digest",    action="store_true")
    parser.add_argument("--dashboard", action="store_true")
    parser.add_argument("--evolve",    action="store_true")
    parser.add_argument("--firm",      type=str)
    args = parser.parse_args()

    if args.evolve:
        from learning.evolution import run_evolution
        run_evolution()
        sys.exit(0)

    if args.dashboard:
        config = Config()
        db = Database(config.DB_PATH)
        DashboardGenerator(db).generate()
        db.close()
        sys.exit(0)

    target = None
    if args.firm:
        firm = FIRMS_BY_ID.get(args.firm)
        if not firm:
            logger.error(f"Unknown firm ID: {args.firm}. Available: {list(FIRMS_BY_ID.keys())}")
            sys.exit(1)
        target = [firm]

    run(firms_to_run=target, digest_only=args.digest)
