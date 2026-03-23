import sqlite3
import unittest

from database.signal_verifier import ORCHESTRATOR, compute_confidence


class SignalVerifierTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute(
            """
            CREATE TABLE signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_id TEXT,
                signal_type TEXT,
                title TEXT,
                description TEXT,
                source_url TEXT,
                raw_data TEXT,
                weight REAL,
                detected_at TEXT,
                dedup_hash TEXT
            )
            """
        )

    def tearDown(self):
        self.conn.close()

    def test_high_quality_signal_scores_as_verified(self):
        signal = {
            "firm_id": "bennett_jones",
            "signal_type": "sedar_major_deal",
            "title": "Bennett Jones named on Calgary prospectus for $500 million financing",
            "description": "SEDAR+ filing lists Bennett Jones LLP as counsel on a Calgary energy financing.",
            "source_url": "https://www.sedarplus.ca/calgary-financing",
            "raw_data": {"issuer": "Example Energy"},
            "weight": 4.8,
            "detected_at": "2026-03-22T12:00:00+00:00",
        }
        result = ORCHESTRATOR.verify(signal, conn=self.conn)
        self.assertEqual(result.verdict, "verified")
        self.assertGreaterEqual(result.confidence_score, 0.78)

    def test_sparse_unknown_signal_is_rejected(self):
        signal = {
            "firm_id": "mystery_firm",
            "signal_type": "web_signal",
            "title": "test signal",
            "description": "",
            "source_url": "",
            "raw_data": {},
            "weight": 9.0,
            "detected_at": "2024-01-01T00:00:00+00:00",
        }
        result = ORCHESTRATOR.verify(signal, conn=self.conn)
        self.assertEqual(result.verdict, "rejected")
        self.assertLess(result.confidence_score, 0.45)

    def test_corroboration_improves_confidence(self):
        baseline = compute_confidence(
            firm_id="burnet",
            signal_type="job_posting",
            title="BDP posts Calgary corporate associate role",
            description="Burnet Duckworth & Palmer posted a corporate associate role in Calgary.",
            weight=2.0,
            source_url="https://www.bdplaw.com/careers/corporate-associate",
            detected_at="2026-03-22T09:00:00+00:00",
            raw_data={},
            conn=self.conn,
        )
        self.conn.execute(
            """
            INSERT INTO signals (firm_id, signal_type, title, description, source_url, raw_data, weight, detected_at, dedup_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "burnet",
                "job_posting",
                "BDP posts Calgary corporate associate role",
                "Burnet Duckworth & Palmer posted a corporate associate role in Calgary.",
                "https://www.bdplaw.com/careers/corporate-associate",
                "{}",
                2.0,
                "2026-03-22T08:00:00+00:00",
                "abc123",
            ),
        )
        self.conn.execute(
            """
            INSERT INTO signals (firm_id, signal_type, title, description, source_url, raw_data, weight, detected_at, dedup_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "burnet",
                "job_posting",
                "Burnet posts Calgary corporate associate role",
                "Independent careers page scrape confirms the same Calgary opening.",
                "https://careers.example.ca/burnet-corporate-associate",
                "{}",
                2.0,
                "2026-03-22T10:00:00+00:00",
                "abc124",
            ),
        )
        corroborated = compute_confidence(
            firm_id="burnet",
            signal_type="job_posting",
            title="BDP posts Calgary corporate associate role",
            description="Burnet Duckworth & Palmer posted a corporate associate role in Calgary.",
            weight=2.0,
            source_url="https://www.bdplaw.com/careers/corporate-associate",
            detected_at="2026-03-22T09:00:00+00:00",
            raw_data={},
            conn=self.conn,
        )
        self.assertGreater(corroborated, baseline)


if __name__ == "__main__":
    unittest.main()
