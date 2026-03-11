import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

from classifier.department import DepartmentClassifier
from database.db import Database
from learning.evolution import run_evolution


class LearningLoopTests(unittest.TestCase):
    def test_classifier_has_fallback_result(self):
        classifier = DepartmentClassifier()

        classified = classifier.classify_with_fallback(
            "Our team advises on privacy breaches and cybersecurity readiness.",
            title="Privacy breach response update",
        )
        fallback = classifier.classify_with_fallback("", title="")

        self.assertEqual(classified["department"], "Data Privacy")
        self.assertGreater(classified["score"], 0)
        self.assertEqual(fallback["department"], "General")

    def test_evolution_trains_from_saved_run_output(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "tracker.db")
            db = Database(db_path)
            now = datetime.now(timezone.utc)

            for idx in range(3):
                db.save_signal({
                    "firm_id": "firm-1",
                    "firm_name": "Example LLP",
                    "signal_type": "lateral_hire" if idx == 2 else "press_release",
                    "title": f"Privacy expansion signal {idx}",
                    "body": "privacy cybersecurity data breach response team growth",
                    "url": f"https://example.test/signal-{idx}",
                    "department": "Data Privacy",
                    "department_score": 1.8,
                    "matched_keywords": ["privacy", "cybersecurity"],
                    "seen_at": (now - timedelta(hours=idx)).isoformat(),
                })

            report = run_evolution(force=True, db_path=db_path)

            conn = sqlite3.connect(db_path)
            feedback_count = conn.execute(
                "SELECT COUNT(*) FROM signal_feedback"
            ).fetchone()[0]
            weight_rows = conn.execute(
                "SELECT COUNT(*) FROM keyword_weights WHERE department='Data Privacy'"
            ).fetchone()[0]
            seen_at = conn.execute(
                "SELECT seen_at FROM signals ORDER BY id LIMIT 1"
            ).fetchone()[0]
            conn.close()
            db.close()

            self.assertIsNotNone(report)
            self.assertGreater(report["feedback_summary"]["confirmed"], 0)
            self.assertGreater(report["keywords_updated"], 0)
            self.assertGreater(feedback_count, 0)
            self.assertGreater(weight_rows, 0)
            self.assertTrue(seen_at)


if __name__ == "__main__":
    unittest.main()
