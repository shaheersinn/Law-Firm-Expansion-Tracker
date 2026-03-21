"""
pipeline/firm_scrapers.py  — comprehensive per-firm scraper loop  (v2)
Runs ALL 54 web scrapers from scrapers/ against every firm once per day.
"""
import logging, time, sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
log = logging.getLogger(__name__)

DEPT_TO_PRACTICE = {
    "Corporate/M&A": "corporate", "Litigation": "litigation",
    "Energy": "energy", "Employment": "employment",
    "Real Estate": "real_estate", "Tax": "tax", "IP": "ip",
    "Securities": "securities", "Restructuring": "restructuring",
    "Regulatory": "regulatory", "Banking": "corporate",
    "Competition": "corporate", "Immigration": "regulatory",
    "Environmental": "energy", "Technology": "ip", "Privacy": "regulatory",
    "Health": "regulatory", "Infrastructure": "corporate",
    "Pro Bono": "general", "ESG": "general",
}

FIRM_SCRAPERS = [
    # Lateral & personnel (highest value)
    ("LateralTrackScraper",       "scrapers.lateral_tracker",       "LateralTrackScraper"),
    ("LateralTrack2Scraper",      "scrapers.lateral_track",         "LateralTrackScraper"),
    ("CounselMoveScraper",        "scrapers.counsel_move",          "CounselMoveScraper"),
    ("PartnerPromoteScraper",     "scrapers.partner_promote",       "PartnerPromoteScraper"),
    ("InhouseMoveScraper",        "scrapers.inhouse_move",          "InhouseMoveScraper"),
    # Jobs & careers
    ("JobsScraper",               "scrapers.jobs",                  "JobsScraper"),
    ("RecruiterScraper",          "scrapers.recruiter",             "RecruiterScraper"),
    ("LawSchoolScraper",          "scrapers.lawschool",             "LawSchoolScraper"),
    # Deals & M&A
    ("DealTrackScraper",          "scrapers.deal_tracker",          "DealTrackScraper"),
    ("MergerNewsScraper",         "scrapers.merger_news",           "MergerNewsScraper"),
    ("PrivateEquityTrackScraper", "scrapers.private_equity_track",  "PrivateEquityTrackScraper"),
    ("CapitalMarketsMonitor",     "scrapers.capital_markets_monitor","CapitalMarketsMonitor"),
    # Rankings & awards
    ("ChambersScraper",           "scrapers.chambers",              "ChambersScraper"),
    ("LexpertRankScraper",        "scrapers.lexpert_rank",          "LexpertRankScraper"),
    ("PrecedentRankScraper",      "scrapers.precedent_rank",        "PrecedentRankScraper"),
    ("AwardsScraper",             "scrapers.awards",                "AwardsScraper"),
    ("ESGAwardScraper",           "scrapers.esg_award",             "ESGAwardScraper"),
    ("Law360CanadaScraper",       "scrapers.law360",                "Law360CanadaScraper"),
    # Practice & office signals
    ("PracticeLaunchScraper",     "scrapers.practice_launch",       "PracticeLaunchScraper"),
    ("OfficeTracker",             "scrapers.office_tracker",        "OfficeTracker"),
    ("ForeignOfficeScraper",      "scrapers.foreign_office",        "ForeignOfficeScraper"),
    ("LegalTechScraper",          "scrapers.legal_tech",            "LegalTechScraper"),
    # Regulatory & sector
    ("RegulatoryTrackScraper",    "scrapers.regulatory_track",      "RegulatoryTrackScraper"),
    ("OSCTrackScraper",           "scrapers.osc_track",             "OSCTrackScraper"),
    ("CompetitionTrackScraper",   "scrapers.competition_track",     "CompetitionTrackScraper"),
    ("TaxLawTrackScraper",        "scrapers.tax_law_track",         "TaxLawTrackScraper"),
    ("EmploymentLawTrackScraper", "scrapers.employment_law_track",  "EmploymentLawTrackScraper"),
    ("InsolvencyMonitorScraper",  "scrapers.insolvency_monitor",    "InsolvencyMonitorScraper"),
    ("HealthcareLawTrackScraper", "scrapers.healthcare_law_track",  "HealthcareLawTrackScraper"),
    ("ImmigrationTrackScraper",   "scrapers.immigration_track",     "ImmigrationTrackScraper"),
    ("InfrastructureTrackScraper","scrapers.infrastructure_track",  "InfrastructureTrackScraper"),
    ("ProBonoScraper",            "scrapers.pro_bono",              "ProBonoScraper"),
    # Government / official
    ("GovTrackScraper",           "scrapers.govtrack",              "GovTrackScraper"),
    ("BenchApptScraper",          "scrapers.bench_appt",            "BenchApptScraper"),
    ("LobbyistScraper",           "scrapers.lobbyist",              "LobbyistScraper"),
    ("SedarScraper",              "scrapers.sedar",                 "SedarScraper"),
    # Media & news
    ("MediaScraper",              "scrapers.media",                 "MediaScraper"),
    ("GoogleNewsScraper",         "scrapers.google_news",           "GoogleNewsScraper"),
    ("PressScraper",              "scrapers.press",                 "PressScraper"),
    ("PublicationsScraper",       "scrapers.publications",          "PublicationsScraper"),
    ("BNNTrackScraper",           "scrapers.bnn_track",             "BNNTrackScraper"),
    ("RSSNewsScraper",            "scrapers.rss_news",              "RSSNewsScraper"),
    ("RSSFeedScraper",            "scrapers.rss",                   "RSSFeedScraper"),
    ("CBASectionScraper",         "scrapers.cba_section",           "CBASectionScraper"),
    # Alumni, events, thought-leadership
    ("AlumniTrackScraper",        "scrapers.alumni_track",          "AlumniTrackScraper"),
    ("ThoughtLeaderScraper",      "scrapers.thought_leader",        "ThoughtLeaderScraper"),
    ("DiversityScraper",          "scrapers.diversity",             "DiversityScraper"),
    ("ConferenceScraper",         "scrapers.conference",            "ConferenceScraper"),
    ("EventScraper",              "scrapers.event",                 "EventScraper"),
    # Website, CIPO, CanLII, social, cross-ref
    ("WebsiteScraper",            "scrapers.website",               "WebsiteScraper"),
    ("CIPOScraper",               "scrapers.cipo",                  "CIPOScraper"),
    ("CanLIIScraper",             "scrapers.canlii",                "CanLIIScraper"),
    ("LinkedInScraper",           "scrapers.linkedin",              "LinkedInScraper"),
    ("PodcastScraper",            "scrapers.podcast",               "PodcastScraper"),
    ("BarAssociationScraper",     "scrapers.barassoc",              "BarAssociationScraper"),
    ("SignalCrossRefScraper",     "scrapers.signal_crossref",       "SignalCrossRefScraper"),
]


def _load_scraper_instances():
    instances = []
    seen_cls = set()
    seen_mod = set()
    for log_name, mod_path, cls_name in FIRM_SCRAPERS:
        key = f"{mod_path}.{cls_name}"
        if key in seen_mod:
            continue
        seen_mod.add(key)
        try:
            mod  = __import__(mod_path, fromlist=[cls_name])
            cls  = getattr(mod, cls_name)
            instances.append((log_name, cls()))
        except Exception as e:
            log.debug("[FirmScrapers] Could not load %s: %s", log_name, e)
    log.info("[FirmScrapers] Loaded %d / %d scrapers", len(instances), len(FIRM_SCRAPERS))
    return instances


def _v3_to_v5(sig: dict) -> dict:
    dept     = sig.get("department") or "Corporate/M&A"
    practice = DEPT_TO_PRACTICE.get(dept, "general")
    raw_w    = float(sig.get("department_score") or 1.0)
    weight   = round(max(1.0, min(5.0, 1.0 + raw_w * 0.4)), 2)
    return dict(
        firm_id      = sig["firm_id"],
        signal_type  = sig.get("signal_type", "web_signal"),
        weight       = weight,
        title        = (sig.get("title") or "")[:200],
        description  = (sig.get("body")  or "")[:800],
        source_url   = sig.get("url", ""),
        practice_area= practice,
        raw_data     = {
            "source":           sig.get("source", ""),
            "matched_keywords": sig.get("matched_keywords", []),
            "department":       dept,
            "department_score": raw_w,
            "published_at":     sig.get("published_at", ""),
        },
    )


def run_firm_scrapers() -> int:
    """
    Run all scrapers for every firm in CALGARY_FIRMS.
    Logs in v3 style: 'Processing: Firm Name' then per-scraper counts.
    Returns count of NEW signals inserted into DB.
    """
    from config_calgary import CALGARY_FIRMS
    from database.db import insert_signal

    t_total   = time.time()
    instances = _load_scraper_instances()
    if not instances:
        log.error("[FirmScrapers] No scrapers loaded — abort")
        return 0

    grand_total = 0
    grand_new   = 0
    scraper_hit: dict = {name: 0 for name, _ in instances}

    log.info("=" * 70)
    log.info("  FIRM SCRAPER LOOP  —  %d firms  x  %d scrapers",
             len(CALGARY_FIRMS), len(instances))
    log.info("=" * 70)

    for firm in CALGARY_FIRMS:
        if not firm.get("website"):
            continue

        firm_v3 = {
            "id":           firm["id"],
            "name":         firm["name"],
            "short":        firm.get("short", firm["name"].split()[0]),
            "alt_names":    firm.get("alt_names", firm.get("aliases", [])),
            "website":      firm.get("website", ""),
            "careers_url":  firm.get("careers_url", ""),
            "news_url":     firm.get("news_url", ""),
            "linkedin_slug":firm.get("linkedin_slug", ""),
            "tier":         firm.get("tier", "mid"),
            "hq":           firm.get("hq", "Calgary"),
        }

        log.info("")
        log.info("--------------------------------------------------")
        log.info("Processing: %s", firm["name"])

        for scraper_name, scraper in instances:
            n_total = 0
            n_new   = 0
            try:
                raw = scraper.fetch(firm_v3) or []
                for s in raw:
                    n_total += 1
                    if insert_signal(**_v3_to_v5(s)):
                        n_new += 1
            except Exception as e:
                log.debug("[FirmScrapers] %s/%s: %s", scraper_name, firm["id"], e)

            log.info("  %-42s %d signals  (%d new)", scraper_name, n_total, n_new)
            grand_total += n_total
            grand_new   += n_new
            if n_total > 0:
                scraper_hit[scraper_name] = scraper_hit.get(scraper_name, 0) + 1

    # Lateral boost (module-level function, not per-firm BaseScraper)
    try:
        from scrapers.lateral_boost import run_lateral_boost
        boosts = run_lateral_boost() or []
        boost_new = 0
        for b in boosts:
            sig = {
                "firm_id":         getattr(b, "firm_id", ""),
                "signal_type":     "lateral_hire",
                "title":           getattr(b, "title", ""),
                "body":            getattr(b, "description", ""),
                "url":             getattr(b, "url", ""),
                "department":      getattr(b, "department", "Corporate/M&A"),
                "department_score":getattr(b, "confidence", 3.0),
                "matched_keywords":getattr(b, "matched_keywords", []),
                "source":          "LateralBoostScraper",
            }
            if sig["firm_id"] and insert_signal(**_v3_to_v5(sig)):
                boost_new += 1
        if boosts:
            log.info("  %-42s %d signals  (%d new)", "LateralBoostScraper", len(boosts), boost_new)
            grand_new += boost_new
    except Exception as e:
        log.debug("[FirmScrapers] LateralBoost: %s", e)

    always_zero = [n for n, cnt in scraper_hit.items() if cnt == 0]
    if always_zero:
        log.warning("SCRAPER HEALTH: %d scrapers returned 0 signals across all firms: %s",
                    len(always_zero), ", ".join(always_zero))

    elapsed = time.time() - t_total
    log.info("")
    log.info("Total new signals this run: %d", grand_new)
    log.info("Firm loop: total=%d  new=%d  elapsed=%.0fs (%.1f min)",
             grand_total, grand_new, elapsed, elapsed / 60)
    return grand_new
