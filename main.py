"""
Law Firm Expansion Tracker — main entry point.

14 scrapers × 26 firms = comprehensive expansion intelligence.

Sources: firm websites, Indeed, LinkedIn, CanLII, Chambers, Legal 500,
         bar associations, law schools, SEDAR+, government proceedings,
         RSS feeds, lobbyist registry, conferences, awards directories.

Run modes:
  python main.py                → full collection + digest if Sunday
  python main.py --digest       → send digest from DB without scraping
  python main.py --firm osler   → single-firm test run
  python main.py --dashboard    → regenerate dashboard only
"""

import logging, os, sys, argparse
from datetime import datetime, timezone

from config import Config
from firms import FIRMS, FIRMS_BY_ID
from database.db import Database
from scrapers.jobs         import JobsScraper
from scrapers.press        import PressScraper
from scrapers.publications import PublicationsScraper
from scrapers.website      import WebsiteScraper
from scrapers.canlii       import CanLIIScraper
from scrapers.chambers     import ChambersScraper
from scrapers.lawschool    import LawSchoolScraper
from scrapers.barassoc     import BarAssociationScraper
from scrapers.sedar        import SedarScraper
from scrapers.govtrack     import GovTrackScraper
from scrapers.linkedin     import LinkedInScraper
from scrapers.awards       import AwardsScraper
from scrapers.conferences  import ConferenceScraper
from scrapers.rss          import RSSFeedScraper
from scrapers.lobbyist     import LobbyistScraper
from analysis.signals      import ExpansionAnalyzer
from alerts.notifier       import Notifier

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("tracker.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("main")

SCRAPERS = [
    RSSFeedScraper(),          # Fastest — check news feeds first
    PressScraper(),            # Firm news pages + media
    LinkedInScraper(),         # Company feed + Google-cached profiles
    JobsScraper(),             # Indeed, LinkedIn jobs, firm careers
    PublicationsScraper(),     # Insights, Lexology, Mondaq
    WebsiteScraper(),          # Practice pages + snapshots
    CanLIIScraper(),           # Court records
    SedarScraper(),            # SEDAR+ securities filings
    GovTrackScraper(),         # Federal/provincial regulatory proceedings
    LobbyistScraper(),         # Federal lobbyist registry
    ChambersScraper(),         # Chambers Canada + Legal 500
    AwardsScraper(),           # Best Lawyers, Benchmark, Lexpert, etc.
    LawSchoolScraper(),        # Student recruitment signals
    BarAssociationScraper(),   # CBA, OBA, section leadership
    ConferenceScraper(),       # Conference speaking + sponsorship
]

HIGH_VALUE_TYPES = {"lateral_hire", "bar_leadership", "ranking"}


def _dashboard(db, config, notifier):
    try:
        from dashboard.generator import generate
        repo_url = "{}/{}".format(
            os.environ.get("GITHUB_SERVER_URL", "https://github.com"),
            os.environ.get("GITHUB_REPOSITORY", "")
        )
        url = generate(db, output_path="docs/index.html", repo_url=repo_url)
        logger.info(f"Dashboard → {url}")
        if url and not notifier.dashboard_url:
            notifier.dashboard_url = url
        return url
    except Exception as e:
        logger.error(f"Dashboard failed: {e}", exc_info=True)
        return ""


def _digest(db, analyzer, notifier):
    weekly  = db.get_signals_this_week()
    alerts  = analyzer.analyze(weekly)
    changes = analyzer.detect_website_changes([])
    new     = [a for a in alerts if not db.was_alert_sent(a["firm_id"], a["department"])]
    notifier.send_weekly_digest(new or [], changes or [])
    for a in new:
        db.mark_alert_sent(a["firm_id"], a["department"], a["expansion_score"])
    logger.info(f"Digest: {len(new)} alert(s), {len(changes)} website change(s)")


def run(firms_to_run=None, digest_only=False, dash_only=False):
    logger.info("=" * 70)
    logger.info(f"Law Firm Tracker — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    logger.info(f"{len(SCRAPERS)} scrapers × {len(firms_to_run or FIRMS)} firms")
    logger.info("=" * 70)

    config   = Config()
    db       = Database(config.DB_PATH)
    notifier = Notifier(config)
    analyzer = ExpansionAnalyzer(db)
    targets  = firms_to_run or FIRMS

    if dash_only:
        _dashboard(db, config, notifier)
        db.close(); return

    if digest_only:
        _dashboard(db, config, notifier)
        _digest(db, analyzer, notifier)
        db.close(); return

    # ── COLLECTION ─────────────────────────────────────────────────────
    all_new, instant_sent = [], set()

    for firm in targets:
        logger.info(f"\n{'─'*55}\n{firm['name']}")
        for scraper in SCRAPERS:
            try:
                new_count = 0
                for sig in scraper.fetch(firm):
                    if db.is_new_signal(sig):
                        db.save_signal(sig)
                        all_new.append(sig)
                        new_count += 1
                        key = f"{firm['id']}_{sig.get('department','')}"
                        if (config.INSTANT_ALERT_ON_LATERAL
                                and sig["signal_type"] in HIGH_VALUE_TYPES
                                and sig.get("department")
                                and key not in instant_sent):
                            notifier.send_new_signal_alert(sig, sig["department"])
                            instant_sent.add(key)
                        if sig["signal_type"] == "website_snapshot":
                            db.save_website_hash(firm["id"], sig["url"], sig["body"])
                if new_count:
                    logger.info(f"  {scraper.name}: {new_count} new")
            except Exception as e:
                logger.error(f"  {scraper.name} [{firm['short']}]: {e}", exc_info=True)

    logger.info(f"\nCollection complete — {len(all_new)} new signals total")

    # ── ANALYSIS ───────────────────────────────────────────────────────
    weekly   = db.get_signals_this_week()
    alerts   = analyzer.analyze(weekly)
    changes  = analyzer.detect_website_changes(all_new)

    for a in alerts:
        db.save_weekly_score(
            firm_id=a["firm_id"], firm_name=a["firm_name"],
            department=a["department"], score=a["expansion_score"],
            signal_count=a["signal_count"], breakdown=a["signal_breakdown"],
        )

    logger.info(f"Expansion alerts: {len(alerts)} | Website changes: {len(changes)}")

    # ── DASHBOARD + DIGEST ─────────────────────────────────────────────
    _dashboard(db, config, notifier)

    if datetime.now(timezone.utc).weekday() == 6 or "--digest" in sys.argv:
        _digest(db, analyzer, notifier)
    else:
        logger.info("Weekday — instant alerts sent; digest fires Sunday")

    db.close()
    logger.info("\nDone.\n")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--digest",    action="store_true")
    p.add_argument("--dashboard", action="store_true")
    p.add_argument("--firm",      type=str)
    args = p.parse_args()

    target = None
    if args.firm:
        f = FIRMS_BY_ID.get(args.firm.lower())
        if not f:
            logger.error(f"Unknown firm '{args.firm}'. Valid: {sorted(FIRMS_BY_ID.keys())}")
            sys.exit(1)
        target = [f]

    run(firms_to_run=target, digest_only=args.digest, dash_only=args.dashboard)
