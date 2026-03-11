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
# 20 new scrapers
from scrapers.merger_news import MergerNewsScraper
from scrapers.osc_track import OSCTrackScraper
from scrapers.partner_promote import PartnerPromoteScraper
from scrapers.inhouse_move import InhouseMoveScraper
from scrapers.pro_bono import ProBonoScraper
from scrapers.legal_tech import LegalTechScraper
from scrapers.bench_appt import BenchApptScraper
from scrapers.law360 import Law360CanadaScraper
from scrapers.precedent_rank import PrecedentRankScraper
from scrapers.cba_section import CBASectionScraper
from scrapers.foreign_office import ForeignOfficeScraper
from scrapers.practice_launch import PracticeLaunchScraper
from scrapers.counsel_move import CounselMoveScraper
from scrapers.lexpert_rank import LexpertRankScraper
from scrapers.bnn_track import BNNTrackScraper
from scrapers.capital_markets_monitor import CapitalMarketsMonitor
from scrapers.insolvency_monitor import InsolvencyMonitorScraper
from scrapers.competition_track import CompetitionTrackScraper
from scrapers.regulatory_track import RegulatoryTrackScraper
from scrapers.private_equity_track import PrivateEquityTrackScraper
from scrapers.esg_award import ESGAwardScraper
from scrapers.infrastructure_track import InfrastructureTrackScraper
from scrapers.immigration_track import ImmigrationTrackScraper
from scrapers.healthcare_law_track import HealthcareLawTrackScraper
from scrapers.tax_law_track import TaxLawTrackScraper
from scrapers.employment_law_track import EmploymentLawTrackScraper
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
    # ── High-conviction lateral / deal signals ───────────────────────
    LateralTrackScraper,
    MergerNewsScraper,
    PartnerPromoteScraper,
    DealTrackScraper,
    CapitalMarketsMonitor,
    PrivateEquityTrackScraper,
    InsolvencyMonitorScraper,
    # ── Media & news ─────────────────────────────────────────────────
    MediaScraper,
    Law360CanadaScraper,
    BNNTrackScraper,
    # ── Firm intelligence ────────────────────────────────────────────
    OfficeTracker,
    ForeignOfficeScraper,
    PracticeLaunchScraper,
    RecruiterScraper,
    JobsScraper,
    GoogleNewsScraper,
    # ── Rankings & awards ────────────────────────────────────────────
    ChambersScraper,
    AwardsScraper,
    LexpertRankScraper,
    PrecedentRankScraper,
    ESGAwardScraper,
    # ── Bar, bench & association ─────────────────────────────────────
    BarAssociationScraper,
    CBASectionScraper,
    BenchApptScraper,
    # ── Publications & thought leadership ───────────────────────────
    PressScraper,
    PublicationsScraper,
    ThoughtLeaderScraper,
    PodcastScraper,
    # ── Practice-area specialist scrapers ────────────────────────────
    CompetitionTrackScraper,
    RegulatoryTrackScraper,
    OSCTrackScraper,
    TaxLawTrackScraper,
    EmploymentLawTrackScraper,
    InfrastructureTrackScraper,
    HealthcareLawTrackScraper,
    ImmigrationTrackScraper,
    LegalTechScraper,
    ProBonoScraper,
    # ── People movements ─────────────────────────────────────────────
    InhouseMoveScraper,
    CounselMoveScraper,
    AlumniTrackScraper,
    # ── Government / regulatory filings ─────────────────────────────
    GovTrackScraper,
    SedarScraper,
    LobbyistScraper,
    CIPOScraper,
    CanLIIScraper,
    # ── Events & conferences ─────────────────────────────────────────
    ConferenceScraper,
    EventScraper,
    # ── Feed aggregators ─────────────────────────────────────────────
    RSSFeedScraper,
    LawSchoolScraper,
    LinkedInScraper,
    DiversityScraper,
    # ── Website snapshots ────────────────────────────────────────────
    WebsiteScraper,
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
            logger.info(f"  {'SignalCrossRefScraper':<38} {len(fetched)} signals  ({new_count} new)")
        except Exception as e:
            logger.error(f"  SignalCrossRefScraper failed for {firm['short']}: {e}", exc_info=True)

    logger.info(f"\nTotal new signals this run: {len(all_new_signals)}")

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
    website_changes  = analyzer.detect_website_changes([])

    new_alerts = [
        a for a in expansion_alerts
        if not db.was_alert_sent(a["firm_id"], a["department"])
    ]

    notifier.send_combined_digest(new_alerts, website_changes, new_signals=new_signals or [])
    for a in new_alerts:
        db.mark_alert_sent(a["firm_id"], a["department"], a["expansion_score"])

    dashboard.generate()

    logger.info(
        f"Digest: {len(new_alerts)} new alerts"
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
