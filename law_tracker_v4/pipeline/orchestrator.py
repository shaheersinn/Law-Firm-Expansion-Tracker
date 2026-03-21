"""
pipeline/orchestrator.py
─────────────────────────
Master pipeline runner for all 12 signal strategies.

Strategy inventory:
  ORIGINAL (5):
    1. CanLII Litigation Spike        (signals/canlii_litigation.py)
    2. SEDAR+ Corporate/Securities    (signals/sedar_corporate.py)
    3. LinkedIn Empty Chair           (signals/linkedin_turnover.py)
    4. LSA Hireback Vacuum            (signals/lsa_hireback.py)
    5. BigLaw Spillage Graph          (signals/spillage_graph.py)

  ADVANCED (7):
    6.  Macro Correlator              (signals/advanced/macro_correlator.py)
    7.  AER/AUC Hearing Calendar      (signals/advanced/aer_hearings.py)
    8.  Newswire Monitor              (signals/advanced/newswire_monitor.py)
    9.  Website Headcount Delta       (signals/advanced/website_headcount.py)
    10. SEDI Insider Cluster          (signals/advanced/sedi_monitor.py)
    11. Glassdoor Workload Sentiment  (signals/advanced/glassdoor_signals.py)
    12. Law School Placement Intel    (signals/advanced/law_school_placement.py)

  INTELLIGENCE:
    - Predictive Demand Model (P30/P60/P90)
    - Intelligence Brief Generator (Claude-powered)
    - Outreach Plan
    - Telegram alerts (new signals only)
    - Dashboard regeneration
"""

import logging, sys, os, time
from datetime import datetime
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from database.db import init_db
from alerts.notifier import AlertDispatcher
from dashboard.generator import generate_dashboard

log = logging.getLogger(__name__)


def run_strategy_1():
    from signals.canlii_litigation import CanLIILitigationTracker
    return CanLIILitigationTracker().run()

def run_strategy_2():
    from signals.sedar_corporate import SEDARPlusMonitor
    return SEDARPlusMonitor().run()

def run_strategy_3():
    from signals.linkedin_turnover import LinkedInTurnoverTracker
    return LinkedInTurnoverTracker().check_departures()

def run_strategy_4():
    from signals.lsa_hireback import HirebackVacuumTracker
    return HirebackVacuumTracker().run()

def run_strategy_5():
    from signals.spillage_graph import DealMonitor
    return DealMonitor().run()

def run_strategy_6():
    from signals.advanced.macro_correlator import MacroCorrelator
    return MacroCorrelator().run()

def run_strategy_7():
    from signals.advanced.aer_hearings import AERHearingMonitor
    return AERHearingMonitor().run()

def run_strategy_8():
    from signals.advanced.newswire_monitor import NewswireMonitor
    return NewswireMonitor().run()

def run_strategy_9():
    from signals.advanced.website_headcount import TeamPageScraper
    return TeamPageScraper().run()

def run_strategy_10():
    from signals.advanced.sedi_monitor import SEDIMonitor
    return SEDIMonitor().run()

def run_strategy_11():
    from signals.advanced.glassdoor_signals import GlassdoorSentimentMonitor
    return GlassdoorSentimentMonitor().run()

def run_strategy_12():
    from signals.advanced.law_school_placement import LawSchoolPlacementMonitor
    return LawSchoolPlacementMonitor().run()


# Strategies grouped by run frequency
# BI-HOURLY: fast, free APIs — run every 2h
BI_HOURLY_STRATEGIES = {
    1:  ("CanLII Litigation Spike",       run_strategy_1),
    2:  ("SEDAR+ Corporate",              run_strategy_2),
    5:  ("BigLaw Spillage + Deal Monitor",run_strategy_5),
    6:  ("Macro Correlator",              run_strategy_6),
    7:  ("AER/AUC Hearing Calendar",      run_strategy_7),
    8:  ("Newswire Monitor",              run_strategy_8),
}

# DAILY: scraping-heavy — run once per day
DAILY_STRATEGIES = {
    9:  ("Website Headcount Delta",       run_strategy_9),
    10: ("SEDI Insider Cluster",          run_strategy_10),
    11: ("Glassdoor Workload Sentiment",  run_strategy_11),
    12: ("Law School Placement Intel",    run_strategy_12),
}

# WEEKLY (Sunday only)
WEEKLY_STRATEGIES = {
    3:  ("LinkedIn Empty Chair",          run_strategy_3),
}

# SEASONAL (Sept/Oct only)
SEASONAL_STRATEGIES = {
    4:  ("LSA Hireback Vacuum",           run_strategy_4),
}


def run_all_strategies(mode: str = "bi-hourly") -> dict:
    """
    Run strategies by mode.
    mode: "bi-hourly" | "daily" | "weekly" | "seasonal" | "full"
    """
    strategy_sets = {
        "bi-hourly": BI_HOURLY_STRATEGIES,
        "daily":     DAILY_STRATEGIES,
        "weekly":    WEEKLY_STRATEGIES,
        "seasonal":  SEASONAL_STRATEGIES,
        "full":      {**BI_HOURLY_STRATEGIES, **DAILY_STRATEGIES,
                      **WEEKLY_STRATEGIES, **SEASONAL_STRATEGIES},
    }

    to_run   = strategy_sets.get(mode, BI_HOURLY_STRATEGIES)
    results  = {}
    total_new = 0

    log.info("═" * 68)
    log.info("  CALGARY LAW TRACKER v3  —  mode=%s  —  %s",
             mode.upper(), datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"))
    log.info("  12 strategies · Predictive model · Intelligence briefs")
    log.info("═" * 68)

    for num, (name, fn) in sorted(to_run.items()):
        log.info("\n[%d/12] %s", num, name)
        t0 = time.time()
        try:
            sigs = fn() or []
            elapsed = time.time() - t0
            log.info("  ✅  %d new signals  (%.1fs)", len(sigs), elapsed)
            total_new += len(sigs)
            results[num] = {"name": name, "signals": len(sigs), "error": None}
        except Exception as e:
            log.error("  ❌  Strategy %d failed: %s", num, e)
            results[num] = {"name": name, "signals": 0, "error": str(e)}

    return results


def run_predictive_model() -> list:
    from predictive.demand_model import DemandPredictor
    model = DemandPredictor()
    preds = model.predict_all()
    model.print_predictions(preds, top_n=10)
    model.save_predictions(preds)
    return preds


def run_full_pipeline(mode: str = "bi-hourly", run_predictions: bool = True):
    init_db()

    log.info("\n[Pipeline] Running strategies (mode=%s)…", mode)
    strategy_results = run_all_strategies(mode)

    log.info("\n[Pipeline] Dispatching new Telegram alerts…")
    sent = AlertDispatcher().dispatch_unalerted()
    log.info("[Pipeline] %d alert(s) sent.", sent)

    if run_predictions:
        log.info("\n[Pipeline] Running predictive demand model…")
        try:
            run_predictive_model()
        except Exception as e:
            log.error("[Pipeline] Predictive model error: %s", e)

    log.info("\n[Pipeline] Regenerating dashboard…")
    try:
        generate_dashboard()
    except Exception as e:
        log.error("[Pipeline] Dashboard error: %s", e)

    log.info("\n✅  Pipeline complete.  mode=%s", mode)
    return strategy_results


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )
    mode = sys.argv[1] if len(sys.argv) > 1 else "bi-hourly"
    run_full_pipeline(mode)
