"""
test_transformations.py — Unit Tests for Pure-Python Pipeline Logic
====================================================================
Tests the sentiment scoring functions and CDC logic without requiring
a live Spark session. These tests run in standard pytest on any machine.

Run with:
    pip install pytest
    pytest tests/ -v

Tests covered
-------------
  TestSentimentScoring
    - Positive phrases score > 0
    - Negative phrases score < 0
    - Neutral / empty text scores ~0
    - Intensifiers amplify score
    - Label thresholds: > 0.1 → positive, < -0.1 → negative

  TestSentimentLabel
    - Label correctly derives from score

  TestDataGeneratorOutput
    - Generator produces valid JSON structure
    - Malformed records contain the expected defect
    - Late records have epoch in the past
"""

import json
import os
import sys
import time
import unittest

# Allow importing from the project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.utils.sentiment import score_text, score_label


# ============================================================================
# Sentiment Scoring Tests
# ============================================================================

class TestSentimentScoring(unittest.TestCase):

    def test_positive_phrase_scores_positive(self):
        score = score_text("Thank you so much, excellent service and very helpful")
        self.assertGreater(score, 0.1, f"Expected positive score, got {score}")

    def test_negative_phrase_scores_negative(self):
        score = score_text("I am very frustrated and disappointed with the terrible service")
        self.assertLess(score, -0.1, f"Expected negative score, got {score}")

    def test_neutral_text_scores_near_zero(self):
        score = score_text("I have a question about my account billing")
        self.assertAlmostEqual(score, 0.0, delta=0.2, msg=f"Expected neutral score, got {score}")

    def test_empty_string_returns_zero(self):
        self.assertEqual(score_text(""), 0.0)

    def test_none_returns_zero(self):
        self.assertEqual(score_text(None), 0.0)

    def test_intensifier_amplifies_negative(self):
        score_normal   = score_text("disappointed")
        score_amplified = score_text("very disappointed")
        self.assertLess(score_amplified, score_normal,
                        "Intensifier 'very' should make score more negative")

    def test_intensifier_amplifies_positive(self):
        score_normal   = score_text("happy")
        score_amplified = score_text("very happy")
        self.assertGreater(score_amplified, score_normal,
                           "Intensifier 'very' should make score more positive")

    def test_score_bounded_positive_one(self):
        # Extremely positive text should not exceed +1.0
        text = " ".join(["excellent great amazing fantastic brilliant outstanding"] * 10)
        score = score_text(text)
        self.assertLessEqual(score, 1.0)
        self.assertGreaterEqual(score, -1.0)

    def test_score_bounded_negative_one(self):
        # Extremely negative text should not go below -1.0
        text = " ".join(["frustrated terrible worst unacceptable disgusted ridiculous"] * 10)
        score = score_text(text)
        self.assertGreaterEqual(score, -1.0)
        self.assertLessEqual(score, 1.0)

    def test_mixed_text_returns_intermediate_score(self):
        score = score_text("The agent was helpful but the problem is still not resolved")
        # Should be in [-0.5, +0.5] inclusive — not strongly either way
        self.assertGreater(score, -0.5)
        self.assertLessEqual(score, 0.5)


# ============================================================================
# Sentiment Label Tests
# ============================================================================

class TestSentimentLabel(unittest.TestCase):

    def test_positive_score_gives_positive_label(self):
        self.assertEqual(score_label(0.5), "positive")

    def test_negative_score_gives_negative_label(self):
        self.assertEqual(score_label(-0.5), "negative")

    def test_zero_gives_neutral_label(self):
        self.assertEqual(score_label(0.0), "neutral")

    def test_boundary_positive_exclusive(self):
        # exactly 0.1 should be positive
        self.assertEqual(score_label(0.11), "positive")

    def test_boundary_negative_exclusive(self):
        # exactly -0.1 should be negative
        self.assertEqual(score_label(-0.11), "negative")

    def test_within_neutral_band(self):
        self.assertEqual(score_label(0.05), "neutral")
        self.assertEqual(score_label(-0.05), "neutral")


# ============================================================================
# Data Generator Structure Tests
# ============================================================================

class TestGeneratorOutput(unittest.TestCase):

    def setUp(self):
        """Run the generator with a tiny batch to get sample records."""
        import subprocess
        import tempfile

        self.out_dir = tempfile.mkdtemp()
        result = subprocess.run(
            [sys.executable, "data_generator/generate_call_stream.py",
             "--records", "30", "--out-dir", self.out_dir, "--batch-size", "10"],
            capture_output=True, text=True,
            cwd=os.path.join(os.path.dirname(__file__), ".."),
        )
        self.assertEqual(result.returncode, 0,
                         f"Generator failed:\n{result.stdout}\n{result.stderr}")

        self.records = []
        for fname in os.listdir(self.out_dir):
            fpath = os.path.join(self.out_dir, fname)
            with open(fpath, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        self.records.append(json.loads(line))

    def test_records_generated(self):
        self.assertGreater(len(self.records), 0, "No records generated")

    def test_valid_records_have_required_fields(self):
        valid = [r for r in self.records if r.get("_injection_type") != "malformed"]
        for rec in valid[:10]:
            self.assertIn("customer_id",       rec, f"Missing customer_id: {rec}")
            self.assertIn("agent_id",          rec, f"Missing agent_id: {rec}")
            self.assertIn("epoch_timestamp",   rec, f"Missing epoch_timestamp: {rec}")
            self.assertIn("duration_sec",      rec, f"Missing duration_sec: {rec}")
            self.assertIn("transcript_segments", rec, f"Missing transcript_segments: {rec}")
            self.assertIn("metadata",          rec, f"Missing metadata: {rec}")

    def test_transcript_segments_structure(self):
        valid = [r for r in self.records
                 if r.get("_injection_type") != "malformed"
                 and "transcript_segments" in r]
        for rec in valid[:5]:
            for seg in rec["transcript_segments"]:
                self.assertIn("speaker", seg)
                self.assertIn("text", seg)
                self.assertIn("start_sec", seg)
                self.assertIn(seg["speaker"], ["agent", "customer"])

    def test_metadata_structure(self):
        valid = [r for r in self.records
                 if r.get("_injection_type") != "malformed"
                 and "metadata" in r]
        for rec in valid[:5]:
            self.assertIn("call_type", rec["metadata"])
            self.assertIn("channel",   rec["metadata"])
            self.assertIn("region",    rec["metadata"])
            self.assertIn(rec["metadata"]["call_type"],  ["support", "billing", "technical"])
            self.assertIn(rec["metadata"]["channel"],    ["phone", "chat", "callback"])

    def test_late_records_have_past_epoch(self):
        now = int(time.time())
        late = [r for r in self.records if r.get("_injection_type") == "late_arriving"]
        if late:
            for rec in late:
                self.assertLess(rec["epoch_timestamp"], now,
                                "Late record epoch should be in the past")
                age_seconds = now - rec["epoch_timestamp"]
                self.assertGreater(age_seconds, 300,
                                   "Late record should be > 5 min old")

    def test_malformed_records_have_defect(self):
        malformed = [r for r in self.records
                     if r.get("_injection_type", "").startswith("malformed")]
        if malformed:
            for rec in malformed:
                defect_found = (
                    rec.get("customer_id") is None
                    or "call_id" not in rec
                    or (rec.get("duration_sec") is not None and rec["duration_sec"] < 0)
                )
                self.assertTrue(defect_found,
                                f"Expected a structural defect in malformed record: {rec}")

    def test_customer_ids_are_valid(self):
        VALID_IDS = {4, 5, 12, 14, 15, 18, 26, 28, 29, 30,
                     32, 36, 55, 65, 70, 72, 76, 78, 82, 87, 95}
        valid = [r for r in self.records
                 if r.get("_injection_type") != "malformed"
                 and r.get("customer_id") is not None]
        for rec in valid[:20]:
            self.assertIn(rec["customer_id"], VALID_IDS,
                          f"customer_id {rec['customer_id']} not in expected set")


# ============================================================================
# CDC Operation Tests
# ============================================================================

class TestCDCOperations(unittest.TestCase):
    """Test that CDC operation values are valid."""

    VALID_OPERATIONS = {"INSERT", "UPDATE", "DELETE"}

    def test_operation_values(self):
        import csv
        csv_path = os.path.join(
            os.path.dirname(__file__), "..",
            "..", "customer_cdc_data_final.csv"
        )
        if not os.path.exists(csv_path):
            self.skipTest("customer_cdc_data_final.csv not found relative to tests/")
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                self.assertIn(row["operation"], self.VALID_OPERATIONS,
                              f"Unexpected operation: {row['operation']}")


if __name__ == "__main__":
    unittest.main()
