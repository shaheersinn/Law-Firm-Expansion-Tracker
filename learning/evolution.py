"""
Evolution Orchestrator v2.

Key upgrade: adaptive cadence controlled by LearningSchedule.
  • First 48 hours: runs every hour (bootstrap, alpha=0.40)
  • After 48 hours: runs once per day (stable, alpha=0.15)

Full pipeline:
  1. LearningSchedule.should_run()    — skip if too soon
  2. SelfHealer                       — parse log, auto-fix errors
  3. FeedbackEngine (v2)              — infer signal outcomes (5 rules)
  4. KeywordLearnerV2                 — update weights with momentum + confidence
  5. SignalWeightAdapter              — update signal-type weights
  6. AnomalyDetector                  — flag unusual signal patterns
  7. LearningSchedule.record_run()    — advance the schedule
  8. EvolutionLogger                  — write docs/learning_report.json
"""

import json
import logging
import os
from datetime import datetime, timezone

from database.db import Database
from config import Config
from learning.schedule import LearningSchedule
from learning.feedback_v2 import FeedbackEngine
from learning.keyword_learner_v2 import KeywordLearnerV2
from learning.self_healer import SelfHealer

logger = logging.getLogger("learning.evolution")


# ====================================================================== #
#  Signal Weight Adapter (uses schedule alpha)
# ====================================================================== #

class SignalWeightAdapter:
    BASE_WEIGHTS = {
        "lateral_hire": 3.0, "practice_page": 2.5, "job_posting": 2.0,
        "press_release": 1.5, "publication": 1.0, "attorney_profile": 1.0,
        "website_snapshot": 0.0, "bar_leadership": 3.5, "ranking": 3.0,
        "court_record": 2.5, "recruit_posting": 2.0, "bar_speaking": 1.5,
        "bar_sponsorship": 1.0, "bar_mention": 0.5,
    }
    MIN_WEIGHT = 0.1
    MAX_WEIGHT = 5.0
    MIN_SAMPLES = 8

    def __init__(self, db, schedule=None):
        self._db = db
        self._schedule = schedule
        self._ensure_table()

    def _ensure_table(self):
        self._db.conn.executescript("""
            CREATE TABLE IF NOT EXISTS signal_type_weights (
                signal_type TEXT PRIMARY KEY,
                weight      REAL NOT NULL,
                samples     INTEGER DEFAULT 0,
                updated_at  TEXT DEFAULT (datetime('now'))
            );
        """)
        self._db.conn.commit()

    def update(self) -> int:
        alpha = self._schedule.current_alpha() if self._schedule else 0.15
        try:
            cur = self._db.conn.execute("""
                SELECT s.signal_type,
                       SUM(CASE WHEN f.outcome='confirmed'      THEN COALESCE(f.recency_weight,1) ELSE 0 END) AS w_conf,
                       SUM(CASE WHEN f.outcome='false_positive' THEN COALESCE(f.recency_weight,1) ELSE 0 END) AS w_fp
                FROM signals s
                JOIN signal_feedback f ON f.signal_id = s.id
                GROUP BY s.signal_type
            """)
            rows = cur.fetchall()
        except Exception:
            return 0

        updated = 0
        for signal_type, w_conf, w_fp in rows:
            total = (w_conf or 0) + (w_fp or 0)
            if total < self.MIN_SAMPLES:
                continue
            hit_rate   = (w_conf or 0) / total
            base       = self.BASE_WEIGHTS.get(signal_type, 1.0)
            target     = base * (0.3 + 1.7 * hit_rate)
            row = self._db.conn.execute(
                "SELECT weight FROM signal_type_weights WHERE signal_type=?", (signal_type,)
            ).fetchone()
            prev = row[0] if row else base
            new_w = round(max(self.MIN_WEIGHT, min(self.MAX_WEIGHT,
                           alpha * target + (1 - alpha) * prev)), 4)
            self._db.conn.execute("""
                INSERT INTO signal_type_weights (signal_type, weight, samples)
                VALUES (?,?,?)
                ON CONFLICT(signal_type)
                DO UPDATE SET weight=excluded.weight, samples=excluded.samples,
                              updated_at=datetime('now')
            """, (signal_type, new_w, int(total)))
            updated += 1
        self._db.conn.commit()
        logger.info(f"[SignalWeights] {updated} types updated (alpha={alpha})")
        return updated

    def get_current_weights(self) -> dict:
        weights = dict(self.BASE_WEIGHTS)
        try:
            cur = self._db.conn.execute("SELECT signal_type, weight FROM signal_type_weights")
            for st, w in cur.fetchall():
                weights[st] = w
        except Exception:
            pass
        return weights


# ====================================================================== #
#  Anomaly Detector
# ====================================================================== #

class AnomalyDetector:
    def __init__(self, db):
        self._db = db
        self._ensure_table()

    def _ensure_table(self):
        self._db.conn.executescript("""
            CREATE TABLE IF NOT EXISTS anomalies (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                anomaly_type TEXT NOT NULL,
                description  TEXT,
                severity     TEXT DEFAULT 'info',
                detected_at  TEXT DEFAULT (datetime('now'))
            );
        """)
        self._db.conn.commit()

    def detect(self) -> list[dict]:
        anomalies = []
        anomalies.extend(self._check_signal_burst())
        anomalies.extend(self._check_weight_instability())
        anomalies.extend(self._check_silent_firms())
        for a in anomalies:
            try:
                self._db.conn.execute("""
                    INSERT INTO anomalies (anomaly_type, description, severity)
                    VALUES (?,?,?)
                """, (a["type"], a["desc"], a["severity"]))
            except Exception:
                pass
        try:
            self._db.conn.commit()
        except Exception:
            pass
        if anomalies:
            logger.info(f"[Anomaly] {len(anomalies)} anomalies detected")
        return anomalies

    def _check_signal_burst(self) -> list[dict]:
        results = []
        try:
            cur = self._db.conn.execute("""
                SELECT firm_name,
                       SUM(CASE WHEN seen_at >= datetime('now','-1 day') THEN 1 ELSE 0 END) as today,
                       COUNT(*)/7.0 as daily_avg
                FROM signals
                WHERE seen_at >= datetime('now','-7 days')
                GROUP BY firm_id
                HAVING today > daily_avg*5 AND today >= 5
            """)
            for firm, today, avg in cur.fetchall():
                results.append({"type": "SIGNAL_BURST",
                                 "desc": f"{firm}: {today} signals today vs {avg:.1f} avg",
                                 "severity": "warning"})
        except Exception:
            pass
        return results

    def _check_weight_instability(self) -> list[dict]:
        results = []
        try:
            cur = self._db.conn.execute("""
                SELECT department, keyword, multiplier, momentum
                FROM keyword_weights WHERE ABS(momentum) > 0.5
            """)
            for dept, kw, mult, mom in cur.fetchall():
                results.append({"type": "WEIGHT_INSTABILITY",
                                 "desc": f"[{dept}] '{kw}' oscillating: mult={mult}, mom={mom}",
                                 "severity": "info"})
        except Exception:
            pass
        return results

    def _check_silent_firms(self) -> list[dict]:
        results = []
        try:
            cur = self._db.conn.execute("""
                SELECT DISTINCT firm_name FROM signals
                WHERE firm_id NOT IN (
                    SELECT DISTINCT firm_id FROM signals
                    WHERE seen_at >= datetime('now','-14 days')
                ) LIMIT 5
            """)
            for (firm,) in cur.fetchall():
                results.append({"type": "SILENT_FIRM",
                                 "desc": f"{firm} silent for 14+ days",
                                 "severity": "info"})
        except Exception:
            pass
        return results


# ====================================================================== #
#  Evolution Logger
# ====================================================================== #

class EvolutionLogger:
    def __init__(self, db, docs_dir="docs"):
        self._db = db
        self._docs = docs_dir
        os.makedirs(docs_dir, exist_ok=True)

    def write_report(self, schedule_stats, healer_summary,
                     feedback_counts, keywords_updated,
                     signal_weights, anomalies) -> dict:
        confirmed, false_pos = feedback_counts

        try:
            r = self._db.conn.execute("""
                SELECT COUNT(*), AVG(multiplier), MIN(multiplier), MAX(multiplier),
                       AVG(confidence), AVG(ABS(momentum))
                FROM keyword_weights
            """).fetchone()
            kw_stats = {"total": r[0] or 0, "avg_mult": round(r[1] or 1.0, 3),
                        "min_mult": round(r[2] or 1.0, 3), "max_mult": round(r[3] or 1.0, 3),
                        "avg_confidence": round(r[4] or 0.0, 3),
                        "avg_momentum": round(r[5] or 0.0, 3)}
        except Exception:
            kw_stats = {}

        try:
            candidates = [{"dept": r[0], "ngram": r[1], "freq": r[2]}
                          for r in self._db.conn.execute("""
                SELECT department, ngram, frequency FROM keyword_candidates
                ORDER BY frequency DESC LIMIT 20
            """).fetchall()]
        except Exception:
            candidates = []

        try:
            heal_log = [{"error": r[0], "action": r[1], "at": r[2]}
                        for r in self._db.conn.execute("""
                SELECT error_type, action_taken, recorded_at FROM healing_log
                ORDER BY id DESC LIMIT 10
            """).fetchall()]
        except Exception:
            heal_log = []

        report = {
            "generated_at":          datetime.now(timezone.utc).isoformat() + "Z",
            "learning_schedule":     schedule_stats,
            "feedback_this_run":     {"confirmed": confirmed, "false_positives": false_pos},
            "keyword_weights":       kw_stats,
            "keywords_updated":      keywords_updated,
            "signal_type_weights":   signal_weights,
            "discovered_candidates": candidates,
            "healer_summary":        healer_summary,
            "healing_log_recent":    heal_log,
            "anomalies":             [a["desc"] for a in anomalies],
        }

        path = os.path.join(self._docs, "learning_report.json")
        with open(path, "w") as f:
            json.dump(report, f, indent=2)

        hist = os.path.join(self._docs, "learning_history.jsonl")
        with open(hist, "a") as f:
            f.write(json.dumps({
                "ts": report["generated_at"], "phase": schedule_stats.get("phase"),
                "confirmed": confirmed, "false_pos": false_pos,
                "kw_updated": keywords_updated,
            }) + "\n")

        logger.info(f"Report written → {path}")
        return report


# ====================================================================== #
#  Main entry point
# ====================================================================== #

def run_evolution(log_path: str = "tracker.log", force: bool = False):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    config = Config()
    db     = Database(config.DB_PATH)
    schedule = LearningSchedule(db)

    if not force and not schedule.should_run():
        db.close()
        return None

    logger.info("=" * 60)
    logger.info(f"Evolution v2 — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    logger.info(f"Phase: {schedule.current_phase()} | Alpha: {schedule.current_alpha()}")
    logger.info("=" * 60)

    healer = SelfHealer(db, log_path=log_path)
    healer.re_enable_scrapers()
    healer_summary = healer.scan_and_heal()

    feedback = FeedbackEngine(db, schedule)
    confirmed, false_pos = feedback.infer_feedback_from_db()
    cooccurrence = feedback.get_cooccurrence()

    learner = KeywordLearnerV2(db, schedule)
    keywords_updated = learner.update_weights(cooccurrence)
    learner.discover_new_keywords()
    learner.penalise_cross_dept_noise()

    adapter = SignalWeightAdapter(db, schedule)
    adapter.update()
    signal_weights = adapter.get_current_weights()

    detector = AnomalyDetector(db)
    anomalies = detector.detect()

    schedule.record_run(confirmed, false_pos)
    schedule_stats = schedule.get_stats()

    evo_logger = EvolutionLogger(db)
    report = evo_logger.write_report(
        schedule_stats, healer_summary,
        (confirmed, false_pos), keywords_updated,
        signal_weights, anomalies
    )

    db.close()

    logger.info(f"Done. Phase={schedule_stats['phase']}, "
                f"Run#{schedule_stats['run_count']}, "
                f"Confirmed={confirmed}, FP={false_pos}, KW={keywords_updated}")
    return report
